const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
app.use(express.static('public'));

// ---------- Firebase (اختياري: حفظ دائم للمستخدمين + إشعارات المكالمات) ----------
let db = null, messaging = null;
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  try {
    const admin = require('firebase-admin');
    admin.initializeApp({
      credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)),
    });
    db = admin.firestore();
    messaging = admin.messaging();
    console.log('Firebase enabled');
  } catch (e) {
    console.error('Firebase init failed:', e.message);
  }
}

const mem = new Map(); // بديل مؤقت لو Firebase مو مفعّل
const sha = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');
const lc = (s) => String(s || '').trim().toLowerCase();

async function getUser(name) {
  if (db) {
    const d = await db.collection('users').doc(name).get();
    return d.exists ? d.data() : null;
  }
  return mem.get(name) || null;
}
async function saveUser(name, data) {
  if (db) await db.collection('users').doc(name).set(data, { merge: true });
  else mem.set(name, { ...(mem.get(name) || {}), ...data });
}

// ---------- TURN credentials (Metered) ----------
app.get('/ice', async (req, res) => {
  const fallback = [{ urls: 'stun:stun.l.google.com:19302' }];
  const { METERED_DOMAIN, METERED_API_KEY } = process.env;
  if (!METERED_DOMAIN || !METERED_API_KEY) return res.json(fallback);
  try {
    const r = await fetch(`https://${METERED_DOMAIN}/api/v1/turn/credentials?apiKey=${METERED_API_KEY}`);
    const list = await r.json();
    res.json(Array.isArray(list) && list.length ? list : fallback);
  } catch (e) {
    res.json(fallback);
  }
});

// ---------- حالة المكالمات ----------
const online = new Map();  // username -> socket.id
const pending = new Map(); // callee -> { sender, offer, video, ice[], t, delivered }

function deliver(name) {
  const p = pending.get(name);
  const id = online.get(name);
  const recipient = id && io.sockets.sockets.get(id);
  if (!p || !recipient || recipient.inForeground === false || p.delivered) return;
  if (Date.now() - p.t > 60000) return pending.delete(name);
  recipient.emit('incoming', { from: p.from, sender: p.from, offer: p.offer, video: p.video });
  p.ice.forEach(c => recipient.emit('ice', { from: p.from, sender: p.from, candidate: c }));
  p.delivered = true;
  p.ice = [];
}

function clearPair(a, b) {
  const pa = pending.get(a);
  if (pa && pa.from === b) pending.delete(a);
  const pb = pending.get(b);
  if (pb && pb.from === a) pending.delete(b);
}

setInterval(() => {
  const now = Date.now();
  for (const [k, p] of pending) {
    if ((!p.delivered && now - p.t > 60000) || now - p.t > 600000) pending.delete(k);
  }
}, 30000);

io.on('connection', (socket) => {
  const relay = (to, event, data) => {
    const id = online.get(lc(to));
    const sender = socket.username;
    if (id) io.to(id).emit(event, { sender, from: sender, ...data });
  };
  socket.on('register', async (payload, cb) => {
    if (typeof cb !== 'function') return;
    try {
      const name = lc(payload && payload.name);
      const secret = payload && payload.secret;
      if (!/^[a-z0-9_]{3,20}$/.test(name) || !secret) return cb({ ok: false, error: 'invalid' });
      const hash = sha(secret);
      const user = await getUser(name);
      if (user && user.secret !== hash) return cb({ ok: false, error: 'taken' });
      if (!user) await saveUser(name, { secret: hash });
      online.set(name, socket.id);
      socket.username = name;
      socket.inForeground = payload.active !== false;
      cb({ ok: true, name });
      if (socket.inForeground) deliver(name);
    } catch (e) {
      console.error(e);
      cb({ ok: false, error: 'server' });
    }
  });

  socket.on('app-state', (active) => {
    if (!socket.username || typeof active !== 'boolean') return;
    socket.inForeground = active;
    if (active) deliver(socket.username);
  });

  socket.on('renegotiate-offer', ({ to, offer } = {}) => {
    if (!socket.username || !offer) return;
    relay(to, 'renegotiate-offer', { offer });
  });

  socket.on('renegotiate-answer', ({ to, answer } = {}) => {
    if (!socket.username || !answer) return;
    relay(to, 'renegotiate-answer', { answer });
  });

  socket.on('screen-share-state', ({ to, sharing } = {}) => {
    if (!socket.username || typeof sharing !== 'boolean') return;
    relay(to, 'screen-share-state', { sharing });
  });

  socket.on('token', (t) => {
    if (socket.username && typeof t === 'string') {
      saveUser(socket.username, { fcmToken: t }).catch(console.error);
    }
  });

  socket.on('call', async (payload, cb) => {
    if (typeof cb !== 'function') return;
    try {
      const to = lc(payload && payload.to);
      const from = socket.username;
      if (!from || !to || to === from) return cb({ ok: false, error: 'invalid' });

      // نسجل المكالمة فوراً عشان ما نضيع أي ICE candidates توصل قبل ما نخلص
      pending.set(to, {
        from,
        offer: payload.offer,
        video: payload.video === true,
        ice: [],
        t: Date.now(),
        delivered: false,
      });

      const user = await getUser(to);
      if (!user) {
        pending.delete(to);
        return cb({ ok: false, error: 'notfound' });
      }
      const recipientId = online.get(to);
      const recipient = recipientId && io.sockets.sockets.get(recipientId);
      const isForeground = recipient && recipient.inForeground !== false;
      if (isForeground) {
        deliver(to);
      }
      let pushed = false;
      if (!isForeground && user.fcmToken && messaging) {
        try {
          await messaging.send({
            token: user.fcmToken,
            notification: { title: '📞 مكالمة واردة', body: '@' + from + ' يتصل بك' },
            android: { priority: 'high', ttl: 30000, notification: { channelId: 'calls_v2', sound: 'ringtone' } },
            data: { type: 'incoming_call', sender: from, video: payload.video === true ? 'true' : 'false' },
          });
          pushed = true;
        } catch (e) {
          console.error('push failed:', e.message);
        }
      }
      if (isForeground || pushed) return cb({ ok: true, pushed });
      pending.delete(to);
      cb({ ok: false, error: 'offline' });
    } catch (e) {
      console.error(e);
      cb({ ok: false, error: 'server' });
    }
  });

  socket.on('answer', ({ to, answer } = {}) => {
    to = lc(to);
    clearPair(socket.username, to);
    relay(to, 'answer', { answer });
  });

  socket.on('ice', ({ to, candidate } = {}) => {
    to = lc(to);
    const p = pending.get(to);
    if (p && p.from === socket.username && !p.delivered) p.ice.push(candidate);
    else relay(to, 'ice', { candidate });
  });

  socket.on('reject', ({ to } = {}) => {
    to = lc(to);
    clearPair(socket.username, to);
    relay(to, 'rejected');
  });

  socket.on('hangup', ({ to } = {}) => {
    to = lc(to);
    clearPair(socket.username, to);
    relay(to, 'hangup');
  });

  socket.on('disconnect', () => {
    const name = socket.username;
    if (!name) return;
    if (online.get(name) === socket.id) online.delete(name);
    for (const [callee, p] of pending) {
      if (p.from === name) {
        pending.delete(callee);
        const id = online.get(callee);
        if (p.delivered && id) io.to(id).emit('hangup', { from: name, sender: name });
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server running on port ' + PORT));
