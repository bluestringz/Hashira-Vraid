const express = require('express'), http = require('http'), { WebSocketServer } = require('ws');
const crypto = require('crypto'), fs = require('fs'), path = require('path');
const { Pool } = require('pg');

const FILE = path.join(process.env.DATA_DIR || __dirname, 'data.json');
const ALL = ['channels', 'kick', 'private', 'assign', 'roles', 'voicemod', 'disconnect', 'move'];
const STAFF = ['channels', 'kick', 'assign', 'roles', 'voicemod', 'disconnect', 'move'];
const srv = {}; // moderator mute/deafen per user key: { m, d }. Kept in memory until removed or the server restarts.
let db = {
  users: {}, banned: {}, sessions: {}, msgs: {},
  roles: {
    default: { name: 'Default', color: '#8a8a94', perms: [] },
    verified: { name: 'Verified', color: '#2f9e6b', perms: ['private'] },
    senior: { name: 'Senior', color: '#e11d2e', perms: ['channels', 'kick', 'private', 'assign', 'move'] }
  },
  cats: [{ id: 'text', name: 'Text Channels' }, { id: 'voice', name: 'Voice Channels' }],
  channels: [
    { id: 'open', name: 'OPEN CHAT', type: 'text', open: true, cat: 'text' },
    { id: 'verified-chat', name: 'Verified Chat', type: 'text', open: false, cat: 'text' },
    { id: 'raid-room', name: 'Raid Room', type: 'voice', open: false, cat: 'voice' }
  ]
};

// ---- Storage ----
// If DATABASE_URL is set, everything is stored in Postgres (survives redeploys and restarts).
// Otherwise it falls back to data.json (local testing, or a Render persistent disk via DATA_DIR).
const pool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.DATABASE_SSL === 'off' ? false : { rejectUnauthorized: false } })
  : null;
if (pool) pool.on('error', e => console.log('db pool error:', e.message));
let ready = false, timer = null, writing = Promise.resolve();

async function load() {
  let saved = null;
  if (pool) {
    await pool.query('CREATE TABLE IF NOT EXISTS store (id INT PRIMARY KEY, data JSONB NOT NULL)');
    const r = await pool.query('SELECT data FROM store WHERE id = 1');
    if (r.rows[0]) saved = r.rows[0].data;
  } else {
    try { saved = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch {}
  }
  if (saved) db = { ...db, ...saved };
  ready = true;
}
function flush() {
  clearTimeout(timer); timer = null;
  if (!ready) return Promise.resolve();   // never overwrite stored data before it was loaded
  const json = JSON.stringify(db);
  if (!pool) { try { fs.writeFileSync(FILE, json); } catch (e) { console.log('save failed:', e.message); } return Promise.resolve(); }
  writing = writing
    .then(() => pool.query('INSERT INTO store (id, data) VALUES (1, $1::jsonb) ON CONFLICT (id) DO UPDATE SET data = $1::jsonb', [json]))
    .catch(e => console.log('save failed:', e.message));
  return writing;
}
const save = () => { if (!timer) timer = setTimeout(flush, 300); };

const hash = (p, s = crypto.randomBytes(16).toString('hex')) => s + ':' + crypto.scryptSync(p, s, 32).toString('hex');
const same = (p, h) => { const a = Buffer.from(hash(p, h.split(':')[0])), b = Buffer.from(h); return a.length === b.length && crypto.timingSafeEqual(a, b); };
const U = k => (Object.hasOwn(db.users, k) ? db.users[k] : null);

// Admin account (first account). Set ADMIN_USER / ADMIN_PASS in Render to override.
const AU = (process.env.ADMIN_USER || 'admin').toLowerCase();
function seedAdmin() {
  db.roles.default = { name: 'Default', color: '#8a8a94', perms: [] };   // permanent: every new account starts here
  if (!db.mig1) {   // one time: existing Senior role gets the new "move members" permission
    if (db.roles.senior && !db.roles.senior.perms.includes('move')) db.roles.senior.perms.push('move');
    db.mig1 = 1;
  }
  // Users used to have one `role`; now they have a list of extra roles (Default is implicit for everyone).
  for (const u of Object.values(db.users)) {
    if (!Array.isArray(u.roles)) u.roles = u.role && u.role !== 'default' ? [u.role] : [];
    delete u.role;
  }
  db.channels.forEach(c => { if (!Array.isArray(c.roles)) c.roles = []; });
  for (const [id, r] of Object.entries(db.roles)) {   // repair any role saved in an older/odd shape so it can never break the member list
    if (!r || typeof r !== 'object') { delete db.roles[id]; continue; }
    r.name = String(r.name || id); r.perms = Array.isArray(r.perms) ? r.perms.filter(p => ALL.includes(p)) : [];
    if (!/^#[0-9a-f]{6}$/i.test(r.color || '')) r.color = '#888888';
  }   // roles allowed into a channel (empty = the older open/private rule)
  save();
  if (!U(AU) || process.env.ADMIN_PASS) {
    db.users[AU] = { ...(U(AU) || { name: AU }), roles: ['admin'], pass: hash(process.env.ADMIN_PASS || '@Qaz123qaz') };
    save();
  }
  if (U(AU) && !U(AU).roles.includes('admin')) { U(AU).roles.push('admin'); save(); }
}

const app = express();
app.use(express.json({ limit: '10kb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/av/:k', (req, res) => {
  const u = U(req.params.k);
  if (!u || !u.avatar) return res.sendStatus(404);
  res.type('jpeg').set('Cache-Control', 'public,max-age=86400').send(Buffer.from(u.avatar.split(',')[1], 'base64'));
});

// ICE servers for voice/video. STUN is always there. A TURN relay is added when configured, so people on strict
// networks (mobile data, some routers, Brave's privacy settings) can still connect:
//   Cloudflare Realtime TURN:  CF_TURN_KEY_ID + CF_TURN_API_TOKEN   (short-lived credentials are minted here, the key never reaches the browser)
//   any other TURN server:     TURN_URLS (comma separated) + TURN_USERNAME + TURN_CREDENTIAL
let iceCache = { at: 0, servers: null, relay: false };
async function getIce() {
  const now = Date.now();
  if (iceCache.servers && now - iceCache.at < 6 * 3600e3) return iceCache;
  let servers = [{ urls: ['stun:stun.cloudflare.com:3478', 'stun:stun.l.google.com:19302'] }], relay = false, ok = true;
  try {
    if (process.env.CF_TURN_KEY_ID && process.env.CF_TURN_API_TOKEN) {
      const r = await fetch('https://rtc.live.cloudflare.com/v1/turn/keys/' + encodeURIComponent(process.env.CF_TURN_KEY_ID) + '/credentials/generate-ice-servers', {
        method: 'POST',
        headers: { Authorization: 'Bearer ' + process.env.CF_TURN_API_TOKEN, 'Content-Type': 'application/json' },
        body: JSON.stringify({ ttl: 86400 }),
        signal: AbortSignal.timeout(5000)
      });
      const j = r.ok ? await r.json() : null;
      if (j && Array.isArray(j.iceServers) && j.iceServers.length) { servers = j.iceServers; relay = true; }
      else { ok = false; console.log('TURN credentials request failed:', r.status); }
    } else if (process.env.TURN_URLS) {
      servers.push({ urls: process.env.TURN_URLS.split(',').map(x => x.trim()).filter(Boolean), username: process.env.TURN_USERNAME || '', credential: process.env.TURN_CREDENTIAL || '' });
      relay = true;
    }
  } catch (e) { ok = false; console.log('TURN setup failed:', e.message); }
  iceCache = { at: ok ? now : now - 6 * 3600e3 + 60e3, servers, relay };   // on a failure, try again in a minute
  return iceCache;
}
app.get('/api/ice', async (req, res) => {
  try {
    if (!db.sessions[(req.headers.authorization || '').replace(/^Bearer /, '')]) return res.sendStatus(401);
    const c = await getIce();
    res.json({ iceServers: c.servers, relay: c.relay });
  } catch (e) { console.error('[ice]', e); res.status(500).json({ error: 'ice failed' }); }
});

app.post('/api/auth', (req, res) => {
  const { mode, username, password } = req.body || {};
  const name = String(username || '').trim(), k = name.toLowerCase(), pw = String(password || '');
  const bad = e => res.status(400).json({ error: e });
  if (!/^[\w.-]{3,20}$/.test(name)) return bad('Username: 3-20 letters, numbers, . _ -');
  if (mode === 'register') {
    if (pw.length < 6) return bad('Password: at least 6 characters');
    if (U(k)) return bad('Username is already taken');
    db.users[k] = { name, pass: hash(pw), roles: [] };
  } else if (!U(k) || !same(pw, U(k).pass)) return bad('Wrong username or password');
  if (db.banned[k]) return bad('This account is banned');
  // A new login signs out every other device/browser of this account.
  for (const t in db.sessions) if (db.sessions[t] === k) delete db.sessions[t];
  dropSockets(k);
  const token = crypto.randomBytes(24).toString('hex');
  db.sessions[token] = k; save();
  res.json({ token });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 200000 });
const send = (w, o) => w.readyState === 1 && w.send(JSON.stringify(o));
const live = () => [...wss.clients].filter(w => w.key);
const isAdm = k => db.users[k].roles.includes('admin');
const eff = k => (isAdm(k) ? ['admin'] : ['default', ...db.users[k].roles.filter(r => Object.hasOwn(db.roles, r))]);   // every role the user has (Default always)
const perms = id => (db.roles[id] && Array.isArray(db.roles[id].perms) ? db.roles[id].perms : []);
const P = k => (isAdm(k) ? ALL : [...new Set(eff(k).flatMap(perms))]);   // permissions add up across roles
const roleName = id => String(id === 'admin' ? 'Admin' : (db.roles[id] && db.roles[id].name) || id);
const rank = id => (id === 'admin' ? 1e3 : id === 'default' ? -1 : perms(id).length);
const primary = k => eff(k).slice().sort((a, b) => rank(b) - rank(a) || roleName(a).localeCompare(roleName(b)))[0];   // the role shown in lists and tags
const chan = id => db.channels.find(c => c.id === id);
// Who can see/enter a channel: admins always; if the channel lists roles, only people with one of them; otherwise open, or the "private" permission.
const can = (k, c) => {
  if (!c) return false;
  if (isAdm(k)) return true;
  if (c.roles && c.roles.length) return c.roles.some(r => eff(k).includes(r));
  return c.open || P(k).includes('private');
};
const cleanRoles = a => [...new Set((Array.isArray(a) ? a : []).filter(r => typeof r === 'string' && r !== 'admin' && r !== 'default' && Object.hasOwn(db.roles, r)))];

// Move a socket in/out of a voice channel and tell the others in that channel (used for the join/leave sounds).
function setVoice(w, ch, why) {
  const old = w.voice;
  if (old === ch) return;
  w.voice = ch;
  if (ch) console.log('[voice]', new Date().toISOString(), w.key, 'joined', ch);
  else if (old) console.log('[voice]', new Date().toISOString(), w.key, 'left', old, '-', why || 'unknown');   // shows up in the Render logs
  if (!ch) { w.cam = null; w.scr = null; }
  live().forEach(x => {
    if (x === w) return;
    if (old && x.voice === old) send(x, { t: 'vev', ev: 'leave', key: w.key });
    if (ch && x.voice === ch) send(x, { t: 'vev', ev: 'join', key: w.key });
  });
}

// One session per account: sign out every other socket of this user (code 4004 = logged in elsewhere).
function dropSockets(k) {
  const old = live().filter(x => x.key === k);
  old.forEach(x => { setVoice(x, null, 'signed in somewhere else'); x.key = null; x.close(4004); });
  if (old.length) push();
}

function push() {
  live().forEach(w => { if (w.voice && !can(w.key, chan(w.voice))) { setVoice(w, null, 'no access to the channel'); send(w, { t: 'kicked' }); } });
  const voice = {}, vs = {};
  live().forEach(w => {
    if (!w.voice) return;
    (voice[w.voice] = voice[w.voice] || []).push(w.key);
    if (w.m || w.d || w.cam || w.scr) vs[w.key] = { m: !!w.m, d: !!w.d, cam: w.cam || null, scr: w.scr || null };
  });
  const users = Object.entries(db.users).map(([key, u]) => ({
    key, name: u.name, role: primary(key), roles: u.roles, av: u.av || 0, banned: !!db.banned[key], on: live().some(w => w.key === key)
  }));
  const roles = { admin: { name: 'Admin', color: '#b8860b', perms: ALL }, ...db.roles };
  live().forEach(w => {
    try {
      send(w, {
        t: 'state', me: { key: w.key, role: primary(w.key), roles: db.users[w.key].roles, perms: P(w.key), sv: srv[w.key] || {} },
        roles, users, voice, vs, sv: srv, cats: db.cats, channels: db.channels.filter(c => can(w.key, c))
      });
    } catch (e) { console.error('[push] state for', w.key, 'failed:', e); }   // one bad account must never stop everyone else's update
  });
}

// Heartbeat: browsers answer pings by themselves. It keeps proxies from cutting quiet connections and
// removes half-dead sockets quickly, so nobody stays "online" or "in voice" after their connection is gone.
const heartbeat = setInterval(() => {
  wss.clients.forEach(w => { if (w.isAlive === false) return w.terminate(); w.isAlive = false; try { w.ping(); } catch {} });
}, 25000);
wss.on('close', () => clearInterval(heartbeat));

wss.on('connection', (w, req) => {
  w.isAlive = true;
  w.on('pong', () => { w.isAlive = true; });
  w.on('error', e => console.log('[ws] error', e.message));   // without this listener a socket error can crash the whole server
  w.on('close', code => {
    try {
      const k = w.key;
      if (!k) return;
      setVoice(w, null, 'connection closed (' + code + ')');
      w.key = null;
      console.log('[ws] closed', k, code);
      push();
    } catch (e) { console.error('[ws] close handler failed:', e); }
  });
  try {
    const k = db.sessions[new URL(req.url, 'http://x').searchParams.get('token')];
    if (!k || !U(k)) return w.close(4001);
    if (db.banned[k]) return w.close(4003);
    dropSockets(k);   // newest connection wins, so the same account can't be online twice
    w.key = k; w.voice = null; push();
    w.on('message', raw => {
      w.isAlive = true;
      let m; try { m = JSON.parse(raw); } catch { return; }
      try { handle(w, m); } catch (e) { console.error('[msg]', w.key, m && m.t, 'failed:', e); }
    });
  } catch (e) { console.error('[ws] connection failed:', e); try { w.close(1011); } catch {} }
});

function handle(w, m) {
  const k = w.key, u = db.users[k], adm = isAdm(k), has = p => P(k).includes(p);
  const okTarget = t => t && t !== k && U(t) && !isAdm(t) && (adm || !P(t).some(p => STAFF.includes(p)));
  const done = () => { save(); push(); };
  switch (m.t) {
    case 'open': { const c = chan(m.ch); if (can(k, c)) send(w, { t: 'history', ch: c.id, msgs: db.msgs[c.id] || [] }); break; }
    case 'chat': {
      const c = chan(m.ch), text = String(m.text || '').trim().slice(0, 500);
      if (!can(k, c) || !text) break;
      const msg = { id: crypto.randomBytes(4).toString('hex'), key: k, name: u.name, text, ts: Date.now() };
      db.msgs[c.id] = (db.msgs[c.id] || []).concat(msg).slice(-100); save();
      live().forEach(x => can(x.key, c) && send(x, { t: 'chat', ch: c.id, msg }));
      break;
    }
    case 'join': {
      const c = chan(m.ch);
      if (!can(k, c) || c.type !== 'voice') return send(w, { t: 'denied' });
      setVoice(w, c.id);
      send(w, { t: 'peers', peers: live().filter(x => x !== w && x.voice === c.id).map(x => x.key) });
      push(); break;
    }
    case 'ping': break;   // client keep-alive
    case 'leave': setVoice(w, null, 'left by request'); push(); break;
    case 'sig': { const p = live().find(x => x.key === m.to && x.voice && x.voice === w.voice); p && send(p, { t: 'sig', from: k, data: m.data }); break; }
    case 'vs':
      w.m = !!m.m; w.d = !!m.d;
      w.cam = typeof m.cam === 'string' ? m.cam.slice(0, 80) : null;   // ids of the camera / screen-share streams, so others know which video is which
      w.scr = typeof m.scr === 'string' ? m.scr.slice(0, 80) : null;
      push(); break;
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
      const roles = cleanRoles(m.roles);
      db.channels.push({ id: crypto.randomBytes(4).toString('hex'), name, type: m.type === 'voice' ? 'voice' : 'text', open: !roles.length && !!m.open, roles, cat: db.cats.some(x => x.id === m.cat) ? m.cat : '' });
      done(); break;
    }
    case 'chaccess': {   // who can see and enter one channel
      const c = chan(m.id);
      if (!has('channels') || !c) break;
      if (c.id === 'open') return send(w, { t: 'error', msg: 'OPEN CHAT must stay open to everyone' });
      c.roles = cleanRoles(m.roles); c.open = !c.roles.length && !!m.open;
      done(); break;
    }
    case 'renamech': {
      const c = chan(m.id), name = String(m.name || '').trim().slice(0, 30);
      if (has('channels') && c && name) { c.name = name; done(); }
      break;
    }
    case 'delch':
      if (has('channels') && m.id !== 'open' && chan(m.id)) { db.channels = db.channels.filter(c => c.id !== m.id); delete db.msgs[m.id]; done(); }
      break;
    case 'addcat': {
      const name = String(m.name || '').trim().slice(0, 30);
      if (!has('channels') || !name) break;
      db.cats.push({ id: crypto.randomBytes(4).toString('hex'), name }); done(); break;
    }
    case 'renamecat': {
      const c = db.cats.find(x => x.id === m.id), name = String(m.name || '').trim().slice(0, 30);
      if (has('channels') && c && name) { c.name = name; done(); }
      break;
    }
    case 'delcat':
      if (has('channels') && db.cats.some(x => x.id === m.id)) {
        db.cats = db.cats.filter(x => x.id !== m.id);
        db.channels.forEach(c => { if (c.cat === m.id) c.cat = ''; });
        done();
      }
      break;
    case 'move': {
      if (!has('channels')) break;
      if (m.kind === 'ch') {
        const c = chan(m.id);
        if (!c || (m.cat && !db.cats.some(x => x.id === m.cat))) break;
        db.channels = db.channels.filter(x => x !== c);
        c.cat = m.cat || '';
        const i = db.channels.findIndex(x => x.id === m.before);
        if (i < 0) db.channels.push(c); else db.channels.splice(i, 0, c);
      } else if (m.kind === 'cat') {
        const c = db.cats.find(x => x.id === m.id);
        if (!c) break;
        db.cats = db.cats.filter(x => x !== c);
        const i = db.cats.findIndex(x => x.id === m.before);
        if (i < 0) db.cats.push(c); else db.cats.splice(i, 0, c);
      }
      done(); break;
    }
    case 'delmsg': {
      const c = chan(m.ch);
      if (!adm || !c) break;
      db.msgs[c.id] = (db.msgs[c.id] || []).filter(x => (x.id || x.ts) !== m.id); save();
      live().forEach(x => can(x.key, c) && send(x, { t: 'del', ch: c.id, id: m.id }));
      break;
    }
    case 'clear': {
      const c = chan(m.ch);
      if (!adm || !c) break;
      db.msgs[c.id] = []; save();
      live().forEach(x => can(x.key, c) && send(x, { t: 'clear', ch: c.id }));
      break;
    }
    case 'kick': case 'ban': {
      if (!has('kick') || !okTarget(m.key)) break;
      if (m.t === 'ban') { db.banned[m.key] = 1; for (const t in db.sessions) if (db.sessions[t] === m.key) delete db.sessions[t]; }
      live().filter(x => x.key === m.key).forEach(x => x.close(m.t === 'ban' ? 4003 : 4002));
      done(); break;
    }
    case 'unban': if (adm && db.banned[m.key]) { delete db.banned[m.key]; done(); } break;
    case 'togglerole': {   // add or remove one role; a person can have as many as needed
      const t = U(m.key), rid = String(m.role), r = Object.hasOwn(db.roles, rid) ? db.roles[rid] : null;
      if (!t || !r || rid === 'default') break;
      // admin: any role on anyone but admins. Others with "assign": only non-staff roles, only on non-staff people.
      if (!(adm ? !isAdm(m.key) : has('assign') && okTarget(m.key) && !r.perms.some(p => STAFF.includes(p)))) break;
      const set = new Set(t.roles);
      if (m.on) set.add(rid); else set.delete(rid);
      t.roles = [...set]; done(); break;
    }
    case 'role': {
      const name = String(m.name || '').trim().slice(0, 20), mine = P(k);
      if (!has('roles') || !name) break;
      if (m.id === 'default') return send(w, { t: 'error', msg: 'The Default role is permanent and cannot be changed' });
      const perms = (m.perms || []).filter(p => ALL.includes(p));
      // Non-admins can only hand out permissions they have, and only edit roles that stay within their own level.
      if (!perms.every(p => mine.includes(p))) return send(w, { t: 'error', msg: 'You can only give permissions you have yourself' });
      const old = m.id && Object.hasOwn(db.roles, m.id) ? db.roles[m.id] : null;
      if (old && !old.perms.every(p => mine.includes(p))) return send(w, { t: 'error', msg: 'You cannot edit a role that has permissions you do not have' });
      const id = old ? m.id : crypto.randomBytes(3).toString('hex');
      db.roles[id] = { name, color: /^#[0-9a-f]{6}$/i.test(m.color) ? m.color : '#888888', perms };
      done(); break;
    }
    case 'delrole': {
      const old = Object.hasOwn(db.roles, m.id) ? db.roles[m.id] : null;
      if (!has('roles') || !old || m.id === 'default' || !old.perms.every(p => P(k).includes(p))) break;
      delete db.roles[m.id];
      Object.values(db.users).forEach(x => { x.roles = x.roles.filter(r => r !== m.id); });
      db.channels.forEach(c => { c.roles = (c.roles || []).filter(r => r !== m.id); });
      done(); break;
    }
    case 'smute': case 'sdeaf': {
      if (!has('voicemod') || !okTarget(m.key)) break;
      const f = m.t === 'smute' ? 'm' : 'd', on = !!m.on;
      if (on && !live().some(x => x.key === m.key && x.voice)) break;   // only people currently in voice
      const s = srv[m.key] = srv[m.key] || {};
      s[f] = on;
      if (!s.m && !s.d) delete srv[m.key];
      push(); break;
    }
    case 'vmove': {
      if (!has('move') || !okTarget(m.key)) break;
      const c = chan(m.ch), t = live().find(x => x.key === m.key && x.voice);
      if (!c || c.type !== 'voice' || !t || t.voice === c.id) break;
      if (!can(m.key, c)) return send(w, { t: 'error', msg: 'That member cannot access that channel' });
      setVoice(t, c.id);
      send(t, { t: 'moved', ch: c.id });
      send(t, { t: 'peers', peers: live().filter(x => x !== t && x.voice === c.id).map(x => x.key) });
      push(); break;
    }
    case 'vkick': {
      if (!has('disconnect') || !okTarget(m.key)) break;
      live().filter(x => x.key === m.key && x.voice).forEach(x => { setVoice(x, null, 'disconnected by a moderator'); send(x, { t: 'kicked', by: 1 }); });
      push(); break;
    }
  }
}

// A stray error must never take the whole server down (that would drop everybody out of voice at once).
process.on('uncaughtException', e => console.error('[uncaught]', e));
process.on('unhandledRejection', e => console.error('[unhandled]', e));

async function boot() {
  try { await load(); }
  catch (e) { console.error('Could not load saved data, not starting (it would overwrite it):', e.message); process.exit(1); }
  seedAdmin();
  server.listen(process.env.PORT || 3000, () => console.log('Hashira VRaid running (' + (pool ? 'Postgres' : 'data.json') + ')'));
}
boot();
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, async () => { await flush(); process.exit(0); });
