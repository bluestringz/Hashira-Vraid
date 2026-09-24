const express = require('express'), http = require('http'), { WebSocketServer } = require('ws');
const crypto = require('crypto'), fs = require('fs'), path = require('path');

const FILE = path.join(process.env.DATA_DIR || __dirname, 'data.json');
const ALL = ['channels', 'kick', 'private'];
let db = {
  users: {}, banned: {}, sessions: {}, msgs: {},
  roles: {
    default: { name: 'Default', color: '#8a8a94', perms: [] },
    verified: { name: 'Verified', color: '#2f9e6b', perms: ['private'] },
    senior: { name: 'Senior', color: '#e11d2e', perms: ['channels', 'kick', 'private'] }
  },
  channels: [
    { id: 'open', name: 'OPEN CHAT', type: 'text', open: true },
    { id: 'verified-chat', name: 'Verified Chat', type: 'text', open: false },
    { id: 'raid-room', name: 'Raid Room', type: 'voice', open: false }
  ]
};
try { db = { ...db, ...JSON.parse(fs.readFileSync(FILE, 'utf8')) }; } catch {}
const save = () => { try { fs.writeFileSync(FILE, JSON.stringify(db)); } catch (e) { console.log('save failed:', e.message); } };

const hash = (p, s = crypto.randomBytes(16).toString('hex')) => s + ':' + crypto.scryptSync(p, s, 32).toString('hex');
const same = (p, h) => { const a = Buffer.from(hash(p, h.split(':')[0])), b = Buffer.from(h); return a.length === b.length && crypto.timingSafeEqual(a, b); };
const U = k => (Object.hasOwn(db.users, k) ? db.users[k] : null);

// Admin account (first account). Set ADMIN_USER / ADMIN_PASS in Render to override.
const AU = (process.env.ADMIN_USER || 'admin').toLowerCase();
if (!U(AU) || process.env.ADMIN_PASS) {
  db.users[AU] = { ...(U(AU) || { name: AU }), role: 'admin', pass: hash(process.env.ADMIN_PASS || '@Qaz123qaz') };
  save();
}

const app = express();
app.use(express.json({ limit: '10kb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/av/:k', (req, res) => {
  const u = U(req.params.k);
  if (!u || !u.avatar) return res.sendStatus(404);
  res.type('jpeg').set('Cache-Control', 'public,max-age=86400').send(Buffer.from(u.avatar.split(',')[1], 'base64'));
});

app.post('/api/auth', (req, res) => {
  const { mode, username, password } = req.body || {};
  const name = String(username || '').trim(), k = name.toLowerCase(), pw = String(password || '');
  const bad = e => res.status(400).json({ error: e });
  if (!/^[\w.-]{3,20}$/.test(name)) return bad('Username: 3-20 letters, numbers, . _ -');
  if (mode === 'register') {
    if (pw.length < 6) return bad('Password: at least 6 characters');
    if (U(k)) return bad('Username is already taken');
    db.users[k] = { name, pass: hash(pw), role: 'default' };
  } else if (!U(k) || !same(pw, U(k).pass)) return bad('Wrong username or password');
  if (db.banned[k]) return bad('This account is banned');
  const token = crypto.randomBytes(24).toString('hex');
  db.sessions[token] = k; save();
  res.json({ token });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 200000 });
const send = (w, o) => w.readyState === 1 && w.send(JSON.stringify(o));
const live = () => [...wss.clients].filter(w => w.key);
const P = k => (db.users[k].role === 'admin' ? ALL : (db.roles[db.users[k].role] || db.roles.default).perms);
const chan = id => db.channels.find(c => c.id === id);
const can = (k, c) => c && (c.open || P(k).includes('private'));

function push() {
  live().forEach(w => { if (w.voice && !can(w.key, chan(w.voice))) { w.voice = null; send(w, { t: 'kicked' }); } });
  const voice = {}, vs = {};
  live().forEach(w => {
    if (!w.voice) return;
    (voice[w.voice] = voice[w.voice] || []).push(w.key);
    if (w.m || w.d) vs[w.key] = { m: !!w.m, d: !!w.d };
  });
  const users = Object.entries(db.users).map(([key, u]) => ({
    key, name: u.name, role: u.role, av: u.av || 0, banned: !!db.banned[key], on: live().some(w => w.key === key)
  }));
  const roles = { admin: { name: 'Admin', color: '#b8860b', perms: ALL }, ...db.roles };
  live().forEach(w => send(w, {
    t: 'state', me: { key: w.key, role: db.users[w.key].role, perms: P(w.key) },
    roles, users, voice, vs, channels: db.channels.filter(c => can(w.key, c))
  }));
}

wss.on('connection', (w, req) => {
  const k = db.sessions[new URL(req.url, 'http://x').searchParams.get('token')];
  if (!k || !U(k)) return w.close(4001);
  if (db.banned[k]) return w.close(4003);
  w.key = k; w.voice = null; push();
  w.on('message', raw => { try { handle(w, JSON.parse(raw)); } catch (e) { console.log(e.message); } });
  w.on('close', () => { w.key = null; w.voice = null; push(); });
});

function handle(w, m) {
  const k = w.key, u = db.users[k], adm = u.role === 'admin', has = p => P(k).includes(p);
  const okTarget = t => t && t !== k && U(t) && U(t).role !== 'admin' &&
    (adm || !(db.roles[U(t).role] || { perms: [] }).perms.includes('kick'));
  const done = () => { save(); push(); };
  switch (m.t) {
    case 'open': { const c = chan(m.ch); if (can(k, c)) send(w, { t: 'history', ch: c.id, msgs: db.msgs[c.id] || [] }); break; }
    case 'chat': {
      const c = chan(m.ch), text = String(m.text || '').trim().slice(0, 500);
      if (!can(k, c) || !text) break;
      const msg = { key: k, name: u.name, text, ts: Date.now() };
      db.msgs[c.id] = (db.msgs[c.id] || []).concat(msg).slice(-100); save();
      live().forEach(x => can(x.key, c) && send(x, { t: 'chat', ch: c.id, msg }));
      break;
    }
    case 'join': {
      const c = chan(m.ch);
      if (!can(k, c) || c.type !== 'voice') return send(w, { t: 'denied' });
      w.voice = c.id;
      send(w, { t: 'peers', peers: live().filter(x => x !== w && x.voice === c.id).map(x => x.key) });
      push(); break;
    }
    case 'leave': w.voice = null; push(); break;
    case 'sig': { const p = live().find(x => x.key === m.to && x.voice && x.voice === w.voice); p && send(p, { t: 'sig', from: k, data: m.data }); break; }
    case 'vs': w.m = !!m.m; w.d = !!m.d; push(); break;
    case 'setname': {
      const name = String(m.name || '').trim().replace(/\s+/g, ' '), l = name.toLowerCase();
      if (!/^[\w .-]{3,20}$/.test(name)) return send(w, { t: 'error', msg: 'Name: 3-20 letters, numbers, spaces, . _ -' });
      if (Object.entries(db.users).some(([key, x]) => key !== k && (key === l || x.name.toLowerCase() === l))) return send(w, { t: 'error', msg: 'That name is already taken' });
      u.name = name; done(); break;
    }
    case 'avatar':
      if (typeof m.data === 'string' && m.data.startsWith('data:image/jpeg;base64,') && m.data.length < 100000) { u.avatar = m.data; u.av = Date.now(); done(); }
      break;
    case 'addch': {
      const name = String(m.name || '').trim().slice(0, 30);
      if (!has('channels') || !name) break;
      db.channels.push({ id: crypto.randomBytes(4).toString('hex'), name, type: m.type === 'voice' ? 'voice' : 'text', open: !!m.open });
      done(); break;
    }
    case 'renamech': {
      const c = chan(m.id), name = String(m.name || '').trim().slice(0, 30);
      if (has('channels') && c && name) { c.name = name; done(); }
      break;
    }
    case 'delch': if (adm && m.id !== 'open') { db.channels = db.channels.filter(c => c.id !== m.id); done(); } break;
    case 'kick': case 'ban': {
      if (!has('kick') || !okTarget(m.key)) break;
      if (m.t === 'ban') { db.banned[m.key] = 1; for (const t in db.sessions) if (db.sessions[t] === m.key) delete db.sessions[t]; }
      live().filter(x => x.key === m.key).forEach(x => x.close(m.t === 'ban' ? 4003 : 4002));
      done(); break;
    }
    case 'unban': if (adm && db.banned[m.key]) { delete db.banned[m.key]; done(); } break;
    case 'setrole':
      if (adm && U(m.key) && U(m.key).role !== 'admin' && Object.hasOwn(db.roles, m.role)) { U(m.key).role = m.role; done(); }
      break;
    case 'role': {
      const name = String(m.name || '').trim().slice(0, 20);
      if (!adm || !name) break;
      const id = m.id && Object.hasOwn(db.roles, m.id) ? m.id : crypto.randomBytes(3).toString('hex');
      db.roles[id] = { name, color: /^#[0-9a-f]{6}$/i.test(m.color) ? m.color : '#888888', perms: (m.perms || []).filter(p => ALL.includes(p)) };
      done(); break;
    }
    case 'delrole':
      if (adm && m.id !== 'default' && Object.hasOwn(db.roles, m.id)) {
        delete db.roles[m.id];
        Object.values(db.users).forEach(x => { if (x.role === m.id) x.role = 'default'; });
        done();
      }
      break;
  }
}

server.listen(process.env.PORT || 3000, () => console.log('Hashira VRaid running'));
