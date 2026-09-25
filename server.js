const express = require('express'), http = require('http'), { WebSocketServer } = require('ws');
const crypto = require('crypto'), fs = require('fs'), path = require('path');
const { Pool } = require('pg');

const FILE = path.join(process.env.DATA_DIR || __dirname, 'data.json');
const ALL = ['channels', 'kick', 'private', 'assign', 'roles', 'voicemod', 'disconnect', 'move', 'nick', 'bracket'];
const STAFF = ['channels', 'kick', 'assign', 'roles', 'voicemod', 'disconnect', 'move'];
const srv = {}; // moderator mute/deafen per user key: { m, d }. Kept in memory until removed or the server restarts.
let db = {
  users: {}, banned: {}, sessions: {}, msgs: {},
  roles: {
    default: { name: 'Default', color: '#8a8a94', perms: [] },
    verified: { name: 'Verified', color: '#2f9e6b', perms: ['private'] },
    senior: { name: 'Senior', color: '#e11d2e', perms: ['channels', 'kick', 'private', 'assign', 'move', 'nick', 'bracket'] }
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
    // Moving to a new database (e.g. a new Render region): set IMPORT_DATABASE_URL to the OLD database's External URL.
    // On the first start with an empty new database, everything is copied over once. Remove the variable afterwards.
    if (!saved && process.env.IMPORT_DATABASE_URL) {
      const old = new Pool({ connectionString: process.env.IMPORT_DATABASE_URL, ssl: process.env.DATABASE_SSL === 'off' ? false : { rejectUnauthorized: false } });
      try {
        const o = await old.query('SELECT data FROM store WHERE id = 1');
        if (o.rows[0]) {
          saved = o.rows[0].data;
          await pool.query('INSERT INTO store (id, data) VALUES (1, $1::jsonb) ON CONFLICT (id) DO NOTHING', [JSON.stringify(saved)]);
          console.log('[import] copied all data from the old database (' + Object.keys(saved.users || {}).length + ' accounts). You can now remove IMPORT_DATABASE_URL.');
        } else console.log('[import] the old database has no data');
      } catch (e) { console.error('[import] could not read the old database:', e.message); throw e; }   // stop, so an empty app never starts by mistake
      finally { old.end().catch(() => {}); }
    }
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
  if (!db.mig3) {   // one time: existing Senior role can edit the tournament bracket
    if (db.roles.senior && !db.roles.senior.perms.includes('bracket')) db.roles.senior.perms.push('bracket');
    db.mig3 = 1;
  }
  if (!db.mig2) {   // one time: existing Senior role gets the new "change members' nicknames" permission
    if (db.roles.senior && !db.roles.senior.perms.includes('nick')) db.roles.senior.perms.push('nick');
    db.mig2 = 1;
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
  // ADMIN_PASS is applied when the admin account is first made, and again only when the value in Render is changed,
  // so a password the admin changes inside the app is not undone by the next redeploy.
  const envSig = process.env.ADMIN_PASS ? crypto.createHash('sha256').update(process.env.ADMIN_PASS).digest('hex') : '';
  if (!U(AU) || (envSig && db.adminEnv !== envSig)) {
    db.users[AU] = { ...(U(AU) || { name: AU }), roles: ['admin'], pass: hash(process.env.ADMIN_PASS || '@Qaz123qaz') };
    db.adminEnv = envSig;
    save();
  }
  if (U(AU) && !U(AU).roles.includes('admin')) { U(AU).roles.push('admin'); save(); }
  migrateBracket();
}

const app = express();
app.use(express.json({ limit: '10kb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ---- Appearance (admin): background picture, logo, and the colour of the main (red) buttons ----
// Pictures are kept as files next to data.json (the persistent disk), not inside it, so saving chat stays fast.
const THEME_DIR = process.env.DATA_DIR || __dirname;
const themeFile = w => path.join(THEME_DIR, 'theme-' + w);
const THEME_DEFAULT = { bg: path.join(__dirname, 'public', 'bg.jpg'), logo: path.join(__dirname, 'public', 'logo.png'), bracket: path.join(__dirname, 'public', 'bracket.jpg') };
const themeOut = () => ({ btn: (db.theme && db.theme.btn) || '', v: (db.theme && db.theme.v) || 0 });
app.get('/theme/:w(bg|logo|bracket)', (req, res) => {
  const w = req.params.w, t = db.theme || {};
  res.set('Cache-Control', 'no-cache');
  if (t[w + 'Type'] && fs.existsSync(themeFile(w))) return res.type(t[w + 'Type']).sendFile(themeFile(w));
  res.sendFile(THEME_DEFAULT[w]);
});
app.get('/api/theme', (req, res) => res.json(themeOut()));   // for the login screen (before signing in)
const adminFromReq = (req, w) => { const k = db.sessions[(req.headers.authorization || '').replace(/^Bearer /, '')]; return k && U(k) && (isAdm(k) || (w === 'bracket' && P(k).includes('bracket'))) ? k : null; };   // the bracket picture: also people with the bracket permission
app.post('/api/theme/:w(bg|logo|bracket)', express.raw({ type: 'image/*', limit: '8mb' }), (req, res) => {
  if (!adminFromReq(req, req.params.w)) return res.sendStatus(403);
  const type = String(req.headers['content-type'] || '');
  if (!/^image\/(png|jpeg|webp|gif)$/.test(type) || !Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'Use a PNG, JPG, WebP or GIF picture' });
  try { fs.writeFileSync(themeFile(req.params.w), req.body); } catch (e) { return res.status(500).json({ error: 'Could not save the picture' }); }
  db.theme = db.theme || {}; db.theme[req.params.w + 'Type'] = type; db.theme.v = Date.now();
  console.log('[theme] admin changed the', req.params.w); save(); push(); res.json({ ok: true });
});
app.delete('/api/theme/:w(bg|logo|bracket)', (req, res) => {   // back to the original picture
  if (!adminFromReq(req, req.params.w)) return res.sendStatus(403);
  try { fs.unlinkSync(themeFile(req.params.w)); } catch {}
  db.theme = db.theme || {}; delete db.theme[req.params.w + 'Type']; db.theme.v = Date.now();
  save(); push(); res.json({ ok: true });
});

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

// Change your own password. Needs the current one. Every other device of this account is signed out; this one stays in.
const tries = {};   // wrong current-password attempts per account, to slow down guessing
app.post('/api/password', (req, res) => {
  const t = (req.headers.authorization || '').replace(/^Bearer /, ''), k = db.sessions[t], u = k && U(k);
  if (!u) return res.sendStatus(401);
  const { current, password } = req.body || {}, pw = String(password || '');
  const tr = tries[k] || (tries[k] = { n: 0, until: 0 });
  if (Date.now() < tr.until) return res.status(429).json({ error: 'Too many wrong attempts. Try again in a few minutes.' });
  if (!same(String(current || ''), u.pass)) {
    if (++tr.n >= 5) { tr.n = 0; tr.until = Date.now() + 5 * 60e3; }
    return res.status(400).json({ error: 'Current password is wrong' });
  }
  delete tries[k];
  if (pw.length < 6) return res.status(400).json({ error: 'New password: at least 6 characters' });
  if (pw.length > 100) return res.status(400).json({ error: 'New password is too long' });
  u.pass = hash(pw);
  for (const x in db.sessions) if (db.sessions[x] === k && x !== t) delete db.sessions[x];
  live().filter(x => x.key === k && x.tok !== t).forEach(x => { setVoice(x, null, 'password changed'); x.key = null; x.close(4005); });
  console.log('[auth] password changed for', k);
  save(); push();
  res.json({ ok: true });
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
        roles, users, voice, vs, sv: srv, cats: db.cats, channels: db.channels.filter(c => can(w.key, c)), unread: unreadFor(w.key), dl: process.env.DESKTOP_APP_URL || '', bk: db.bk, theme: themeOut()
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
    const tok = new URL(req.url, 'http://x').searchParams.get('token'), k = db.sessions[tok];
    if (!k || !U(k)) return w.close(4001);
    if (db.banned[k]) return w.close(4003);
    dropSockets(k);   // newest connection wins, so the same account can't be online twice
    w.key = k; w.tok = tok; w.voice = null; push();
    w.on('message', (raw, isBin) => {
      w.isAlive = true;
      if (isBin) return relayAudio(w, raw);
      let m; try { m = JSON.parse(raw); } catch { return; }
      try { handle(w, m); } catch (e) { console.error('[msg]', w.key, m && m.t, 'failed:', e); }
    });
  } catch (e) { console.error('[ws] connection failed:', e); try { w.close(1011); } catch {} }
});

// Backup audio: when two people cannot get a direct call (and no TURN relay helps), their voice goes through this
// server instead. Frames are small compressed chunks; they are only passed to people in the same voice channel
// who were asked for with 'rset'. Each forwarded frame starts with [key length][sender key].
function relayAudio(w, raw) {
  if (!w.key || !w.voice || !w.rset || !w.rset.size || raw.length > 4000) return;
  const now = Date.now();
  if (now - (w.rwin || 0) > 1000) { w.rwin = now; w.rbytes = 0; }
  if ((w.rbytes += raw.length) > 48000) return;   // about 3x a normal voice stream per second: ignore anything above
  const kb = Buffer.from(w.key), out = Buffer.concat([Buffer.from([kb.length]), kb, raw]);
  live().forEach(x => { if (x !== w && w.rset.has(x.key) && x.voice === w.voice && x.bufferedAmount < 256e3) x.send(out, { binary: true }); });
}

// Unread counts per text channel, like Discord. Each account keeps the time it last viewed each channel.
// A channel seen for the first time counts as read, so new members don't start with a pile of old messages.
function unreadFor(k) {
  const u = U(k), out = {};
  if (!u) return out;
  const read = u.read || (u.read = {});
  let changed = false;
  db.channels.forEach(c => {
    if (c.type !== 'text' || !can(k, c)) return;
    if (read[c.id] == null) { read[c.id] = Date.now(); changed = true; return; }
    const n = (db.msgs[c.id] || []).filter(x => x.ts > read[c.id] && x.key !== k).length;
    if (n) out[c.id] = n;
  });
  if (changed) save();
  return out;
}
const markRead = (k, id) => { const u = U(k), c = chan(id); if (u && c && can(k, c)) { (u.read || (u.read = {}))[c.id] = Date.now(); save(); } };

// ---- Tournament bracket layout: boxes and straight lines on a 1600 x 900 canvas ----
const num = (v, lo, hi) => Math.max(lo, Math.min(hi, Math.round(Number(v) || 0)));
const hex = (v, d) => (/^#[0-9a-f]{6}$/i.test(String(v || '')) ? String(v) : d);
function cleanBk(d) {
  d = d || {};
  const slots = (Array.isArray(d.slots) ? d.slots : []).slice(0, 64).map(s => {
    const w = num(s.w, 60, 1600), h = num(s.h, 25, 900);
    return { id: String(s.id || crypto.randomBytes(4).toString('hex')).slice(0, 12), x: num(s.x, 0, 1600 - w), y: num(s.y, 0, 900 - h), w, h,
      shape: ['a', 'b', 'c', 'none'].includes(s.shape) ? s.shape : 'a', champ: !!s.champ, name: String(s.name || '').trim().slice(0, 40) };
  });
  const lines = (Array.isArray(d.lines) ? d.lines : []).slice(0, 200).map(l => ({
    id: String(l.id || crypto.randomBytes(4).toString('hex')).slice(0, 12),
    pts: (Array.isArray(l.pts) ? l.pts : []).slice(0, 60).map(p => [num(p && p[0], 0, 1600), num(p && p[1], 0, 900)])
  })).filter(l => l.pts.length >= 2);
  const c = d.colors || {};
  const colors = { box: hex(c.box, '#e11d2e'), line: hex(c.line, '#ffffff'), text: hex(c.text, '#ffffff'), champ: hex(c.champ, '#ffd166'), lw: num(c.lw || 4, 1, 14) };
  return { slots, lines, colors };
}
function migrateBracket() {   // one time: the old fixed bracket becomes an editable layout (same positions and names)
  if (db.bk) return;
  const OLD = { L1: [3.71, 44.10, 16.04, 5.31], L2: [3.71, 52.28, 16.04, 5.63], L3: [3.71, 61.42, 16.04, 5.74], L4: [3.71, 70.14, 16.04, 5.84],
    SL: [26.63, 58.66, 11.97, 5.10], F: [42.22, 62.91, 15.57, 5.10], SR: [61.16, 58.66, 12.15, 5.10],
    R1: [80.19, 44.10, 16.16, 5.31], R2: [80.19, 52.28, 16.16, 5.63], R3: [80.19, 61.42, 16.16, 5.74], R4: [80.19, 70.14, 16.16, 5.84] };
  const names = db.bracket || {};
  db.bk = cleanBk({ slots: Object.entries(OLD).map(([id, [x, y, w, h]]) => ({ id, x: x * 16, y: y * 9, w: w * 16, h: h * 9, shape: 'none', champ: id === 'F', name: names[id] || '' })), lines: [], colors: {} });
  save();
}

const REACTS = '👍 ❤️ 😂 🤣 😮 😢 😡 🔥 💯 👏 🙏 🎉 ⚔️ 🛡️ 🏹 🪄 💀 🩸 ⚡ 🐉 🥷 👑 🏆 🎯 ✅ ❌ 👀 🫡'.split(' ');   // emoji allowed for reactions (same list as the picker in the page)

function handle(w, m) {
  const k = w.key, u = db.users[k], adm = isAdm(k), has = p => P(k).includes(p), canBk = adm || has('bracket');
  const okTarget = t => t && t !== k && U(t) && !isAdm(t) && (adm || !P(t).some(p => STAFF.includes(p)));
  const done = () => { save(); push(); };
  switch (m.t) {
    case 'open': { const c = chan(m.ch); if (can(k, c)) { markRead(k, c.id); send(w, { t: 'history', ch: c.id, msgs: db.msgs[c.id] || [] }); } break; }
    case 'read': markRead(k, m.ch); break;   // saw new messages while the channel was open
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
    case 'rset': w.rset = new Set((Array.isArray(m.to) ? m.to : []).slice(0, 30).map(String)); break;   // who gets my backup audio
    case 'leave': setVoice(w, null, 'left by request'); push(); break;
    case 'sig': { const p = live().find(x => x.key === m.to && x.voice && x.voice === w.voice); p && send(p, { t: 'sig', from: k, data: m.data }); break; }
    case 'vs':
      w.m = !!m.m; w.d = !!m.d;
      w.cam = typeof m.cam === 'string' ? m.cam.slice(0, 80) : null;   // ids of the camera / screen-share streams, so others know which video is which
      w.scr = typeof m.scr === 'string' ? m.scr.slice(0, 80) : null;
      push(); break;
    case 'nick': {   // admin / Senior (anyone with the "nick" permission): change another member's display name
      const t = String(m.key || '');
      if (!(adm || P(k).includes('nick')) || !okTarget(t)) break;
      const name = String(m.name || '').trim().replace(/\s+/g, ' '), l = name.toLowerCase();
      if (!/^[\w .-]{3,20}$/.test(name)) return send(w, { t: 'error', msg: 'Name: 3-20 letters, numbers, spaces, . _ -' });
      if (Object.entries(db.users).some(([key, x]) => key !== t && (key === l || x.name.toLowerCase() === l))) return send(w, { t: 'error', msg: 'That name is already taken' });
      console.log('[nick]', k, 'renamed', t, 'from', U(t).name, 'to', name);
      U(t).name = name; done(); break;
    }
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
    case 'react': {   // add/remove my reaction on a message (same emoji again = remove)
      const c = chan(m.ch), e = String(m.e || '');
      if (!can(k, c) || !REACTS.includes(e)) break;
      const msg = (db.msgs[c.id] || []).find(x => (x.id || x.ts) === m.id);
      if (!msg) break;
      const re = msg.re || (msg.re = {}), who = re[e] || (re[e] = []);
      const i = who.indexOf(k);
      if (i >= 0) who.splice(i, 1); else { if (Object.keys(re).length >= 20 && !re[e].length) break; who.push(k); }
      if (!who.length) delete re[e];
      if (!Object.keys(re).length) delete msg.re;
      save();
      live().forEach(x => can(x.key, c) && send(x, { t: 'react', ch: c.id, id: m.id, re: msg.re || {} }));
      break;
    }
    case 'delmsg': {
      const c = chan(m.ch);
      if (!adm || !c) break;
      db.msgs[c.id] = (db.msgs[c.id] || []).filter(x => (x.id || x.ts) !== m.id); save();
      live().forEach(x => can(x.key, c) && send(x, { t: 'del', ch: c.id, id: m.id }));
      break;
    }
    case 'btncolor': {   // admin: colour of the main buttons (Login, Send, …); empty = back to the original red
      if (!adm) break;
      const c = String(m.color || '');
      if (c && !/^#[0-9a-f]{6}$/i.test(c)) break;
      db.theme = db.theme || {}; if (c) db.theme.btn = c; else delete db.theme.btn;
      done(); break;
    }
    case 'bklayout': {   // admin: the whole bracket layout (boxes, lines, colours) from the editor
      if (!canBk) break;
      db.bk = cleanBk(m.data); done(); break;
    }
    case 'bkname': {   // admin: team name in one box
      if (!canBk || !db.bk) break;
      const sl = db.bk.slots.find(x => x.id === String(m.id));
      if (sl) { sl.name = String(m.name || '').trim().slice(0, 40); done(); }
      break;
    }
    case 'bkclear': if (canBk && db.bk) { db.bk.slots.forEach(x => { x.name = ''; }); done(); } break;
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
    case 'deluser': {   // admin only: remove an account completely (its old chat messages stay, shown with the old name)
      const t = String(m.key || '');
      if (!adm || !U(t) || isAdm(t) || t === k) break;
      live().filter(x => x.key === t).forEach(x => { setVoice(x, null, 'account deleted'); x.key = null; x.close(4006); });
      delete db.users[t]; delete db.banned[t]; delete srv[t];
      for (const s in db.sessions) if (db.sessions[s] === t) delete db.sessions[s];
      console.log('[admin] deleted account', t);
      done(); break;
    }
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
      if (!old && !adm) return send(w, { t: 'error', msg: 'Only the admin can create new roles' });
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
  const fatal = e => { console.error('Server could not start:', e.message); process.exit(1); };   // e.g. port in use: exit so Render restarts it
  server.on('error', fatal); wss.on('error', fatal);
  server.listen(process.env.PORT || 3000, () => console.log('Hashira VRaid running (' + (pool ? 'Postgres' : 'data.json') + ')'));
}
boot();
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, async () => { await flush(); process.exit(0); });
