import { createServer } from 'node:http';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createHash, randomBytes } from 'node:crypto';
import { createReadStream, createWriteStream, existsSync, mkdirSync, unlinkSync } from 'node:fs';
import WebSocket from 'ws';

const PORT = Number(process.env.PORT ?? 4600);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = path.join(__dirname, 'dawri.db');
const TTS_CACHE_DIR = path.join(__dirname, 'tts-cache');

const db = new DatabaseSync(DB_PATH);

db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA foreign_keys = ON;

  CREATE TABLE IF NOT EXISTS clinics (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL DEFAULT '',
    specialty      TEXT NOT NULL DEFAULT '',
    city           TEXT NOT NULL DEFAULT '',
    address        TEXT NOT NULL DEFAULT '',
    phone          TEXT NOT NULL DEFAULT '',
    rating         REAL NOT NULL DEFAULT 4,
    reviews        INTEGER NOT NULL DEFAULT 0,
    fee            INTEGER NOT NULL DEFAULT 200,
    avg_minutes    INTEGER NOT NULL DEFAULT 15,
    certified      INTEGER NOT NULL DEFAULT 1,
    contracted     INTEGER NOT NULL DEFAULT 0,
    open_time      TEXT NOT NULL DEFAULT '08:00',
    always_open    INTEGER NOT NULL DEFAULT 0,
    color          TEXT NOT NULL DEFAULT '',
    is_open        INTEGER NOT NULL DEFAULT 1,
    commission     INTEGER NOT NULL DEFAULT 50,
    online_enabled INTEGER NOT NULL DEFAULT 1,
    max_online     INTEGER NOT NULL DEFAULT 30,
    activation_code TEXT,
    activated      INTEGER NOT NULL DEFAULT 0,
    password_hash  TEXT,
    password_salt  TEXT,
    is_paused      INTEGER NOT NULL DEFAULT 0,
    tagline        TEXT NOT NULL DEFAULT '',
    created_at     TEXT NOT NULL DEFAULT ''
  );

  CREATE TABLE IF NOT EXISTS bookings (
    id             TEXT PRIMARY KEY,
    clinic_id      TEXT NOT NULL,
    user_id        TEXT NOT NULL,
    patient_name   TEXT NOT NULL,
    whatsapp       TEXT NOT NULL DEFAULT '',
    disability     INTEGER NOT NULL DEFAULT 0,
    date           TEXT NOT NULL,
    queue_number   INTEGER NOT NULL,
    status         TEXT NOT NULL DEFAULT 'WAITING',
    fee            INTEGER NOT NULL DEFAULT 0,
    called_at      TEXT,
    finished_at    TEXT,
    payment_status TEXT NOT NULL DEFAULT 'UNPAID',
    created_at     TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_bookings_user  ON bookings(user_id);
  CREATE INDEX IF NOT EXISTS idx_bookings_queue ON bookings(clinic_id, date);

  CREATE TABLE IF NOT EXISTS counters (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS auth_credentials (
    phone      TEXT PRIMARY KEY,
    uid        TEXT NOT NULL,
    salt       TEXT NOT NULL,
    pass_hash  TEXT NOT NULL,
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
  );
`);

for (const stmt of [
  `ALTER TABLE clinics ADD COLUMN activation_code TEXT`,
  `ALTER TABLE clinics ADD COLUMN activated INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE clinics ADD COLUMN password_hash TEXT`,
  `ALTER TABLE clinics ADD COLUMN password_salt TEXT`,
  `ALTER TABLE clinics ADD COLUMN is_paused INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE clinics ADD COLUMN tagline TEXT NOT NULL DEFAULT ''`,
  `ALTER TABLE clinics ADD COLUMN created_at TEXT NOT NULL DEFAULT ''`,
]) {
  try { db.prepare(stmt).run(); } catch { /* موجود مسبقًا */ }
}

const get = (sql, ...params) => db.prepare(sql).get(...params);
const all = (sql, ...params) => db.prepare(sql).all(...params);
const run = (sql, ...params) => { db.prepare(sql).run(...params); };

class DomainError extends Error {}
const fail = (message) => { throw new DomainError(message); };

function tx(fn) {
  db.exec('BEGIN IMMEDIATE');
  try { const r = fn(); db.exec('COMMIT'); return r; }
  catch (e) { try { db.exec('ROLLBACK'); } catch {} throw e; }
}

const pad2 = (n) => String(n).padStart(2, '0');
function isoDate(offsetDays = 0) {
  const d = new Date(); d.setDate(d.getDate() + offsetDays);
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}
const nowIso = () => new Date().toISOString();
const genId = (p = '') => `${p}${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 9)}`;
const isDateStr = (s) => typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);

function hashPass(password, salt) {
  return createHash('sha256').update(`${salt}:${password}`).digest('hex');
}
function genCode() {
  return `${randomBytes(3).toString('hex').toUpperCase()}-${randomBytes(3).toString('hex').toUpperCase()}`;
}

const ADMIN_PIN = process.env.ADMIN_PIN || '0000';

const SETTINGS_KEY = 'clinic_settings';
const DEFAULT_SETTINGS = {
  name: 'عيادة النخبة الطبية', tagline: 'صحتك أولوية',
  address: 'شارع الاستقلال، الوادي', phone: '032741234',
  fee: 300, avgVisitMinutes: 10, maxQueuePerDay: 40,
  openTime: '08:00', closeTime: '17:00',
  secretaryPin: '2222', pin: '1234',
};

function getSettings() {
  const row = get(`SELECT v FROM meta WHERE k=?`, SETTINGS_KEY);
  if (!row) return DEFAULT_SETTINGS;
  try { return { ...DEFAULT_SETTINGS, ...JSON.parse(row.v) }; }
  catch { return DEFAULT_SETTINGS; }
}

function bootstrap() {
  if (!get(`SELECT k FROM meta WHERE k=?`, SETTINGS_KEY)) {
    run(`INSERT INTO meta(k,v) VALUES (?,?)`, SETTINGS_KEY, JSON.stringify(DEFAULT_SETTINGS));
  }
}
bootstrap();

function mapClinic(r) {
  return {
    id: r.id, nameAr: r.name, name: r.name, specialtyAr: r.specialty, specialty: r.specialty,
    city: r.city, address: r.address, phone: r.phone, rating: r.rating, reviews: r.reviews,
    commission: typeof r.commission === 'number' ? r.commission : 50,
    onlineEnabled: r.online_enabled !== 0, maxOnline: r.max_online ?? 30,
    avgVisitMinutes: r.avg_minutes, certified: r.certified === 1, contracted: r.contracted === 1,
    isOpen: r.is_open === 1, color: r.color,
    activated: r.activated === 1, isPaused: r.is_paused === 1,
    tagline: r.tagline || '', createdAt: r.created_at || '',
  };
}

function mapDoctor(r) {
  return {
    id: r.id, name: r.name, specialty: r.specialty, phone: r.phone || undefined,
    active: r.is_open === 1, createdAt: '',
  };
}

function mapBooking(r) {
  return {
    id: r.id, clinicId: r.clinic_id,
    clinicName: r.clinic_name ?? '', clinicSpecialty: r.clinic_specialty ?? '',
    userId: r.user_id, patientName: r.patient_name, whatsapp: r.whatsapp,
    disability: r.disability === 1, date: r.date, queueNumber: r.queue_number,
    status: r.status, fee: r.fee,
    paymentStatus: r.payment_status === 'PAID' ? 'PAID' : 'UNPAID',
    calledAt: r.called_at ?? undefined, finishedAt: r.finished_at ?? undefined,
    createdAt: r.created_at,
  };
}

function mapClinicBooking(r) {
  const b = mapBooking(r);
  return {
    id: b.id, date: b.date,
    doctorId: b.clinicId, doctorName: b.clinicName, doctorSpecialty: b.clinicSpecialty,
    patientName: b.patientName, patientPhone: b.whatsapp,
    queueNumber: b.queueNumber, status: b.status, fee: b.fee,
    paymentStatus: b.paymentStatus,
    calledAt: b.calledAt, finishedAt: b.finishedAt, createdAt: b.createdAt,
    updatedAt: r.finished_at ?? r.called_at ?? r.created_at,
  };
}

const BOOKING_SELECT = `
  SELECT b.*, c.name AS clinic_name, c.specialty AS clinic_specialty
  FROM bookings b JOIN clinics c ON c.id = b.clinic_id
`;

function nextQueueNumber(clinicId, date) {
  const maxRow = get(
    `SELECT MAX(queue_number) AS m FROM bookings WHERE clinic_id=? AND date=? AND status!='CANCELLED'`,
    clinicId, date,
  );
  const counterRow = get(`SELECT value FROM counters WHERE key=?`, `${clinicId}|${date}`);
  return Math.max(counterRow?.value ?? 0, maxRow?.m ?? 0) + 1;
}

function bumpCounter(key, value) {
  run(`INSERT INTO counters(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value);
}

function createWalkinTicket(body) {
  const settings = getSettings();
  const { doctorId, patientName, patientPhone } = body ?? {};
  const date = isoDate(0);
  if (!doctorId) fail('الطبيب غير محدد.');
  if (!patientName || !String(patientName).trim()) fail('أدخل اسم المريض.');
  if (!patientPhone || !String(patientPhone).trim()) fail('رقم هاتف المريض مطلوب.');

  return tx(() => {
    const provider = get(`SELECT * FROM clinics WHERE id=?`, doctorId);
    if (!provider) fail('الطبيب غير موجود.');
    if (provider.is_open !== 1) fail('العيادة مغلقة.');
    if (provider.is_paused === 1) fail('الطابور موقّف حاليًا.');

    const sameDay = all(`${BOOKING_SELECT} WHERE b.clinic_id=? AND b.date=?`, doctorId, date);
    const clash = sameDay.find(
      (b) => b.whatsapp === patientPhone && (b.status === 'WAITING' || b.status === 'CURRENT'),
    );
    if (clash) fail('لديك دور نشط بالفعل اليوم.');

    const n = nextQueueNumber(doctorId, date);
    bumpCounter(`${doctorId}|${date}`, n);

    const id = genId('bk-');
    run(
      `INSERT INTO bookings (id,clinic_id,user_id,patient_name,whatsapp,disability,date,queue_number,status,fee,payment_status,called_at,finished_at,created_at)
       VALUES (?,?,?,?,?,0,?,?,'WAITING',?,'UNPAID',NULL,NULL,?)`,
      id, doctorId, `guest-${patientPhone}`, String(patientName).trim(), String(patientPhone),
      date, n, provider.commission, nowIso(),
    );
    return mapClinicBooking(get(`${BOOKING_SELECT} WHERE b.id=?`, id));
  });
}

function callNextPatient(doctorId, date) {
  if (!doctorId || !isDateStr(date)) fail('معطيات الطابور غير مكتملة.');
  return tx(() => {
    const clinic = get(`SELECT * FROM clinics WHERE id=?`, doctorId);
    if (clinic && clinic.is_paused === 1) fail('الطابور موقّف حاليًا.');

    const current = get(
      `SELECT id FROM bookings WHERE clinic_id=? AND date=? AND status='CURRENT' LIMIT 1`, doctorId, date,
    );
    const next = get(
      `SELECT id FROM bookings WHERE clinic_id=? AND date=? AND status='WAITING'
       ORDER BY disability DESC, queue_number ASC LIMIT 1`, doctorId, date,
    );

    if (!next) {
      if (!current) fail('لا يوجد مرضى في الانتظار.');
      run(`UPDATE bookings SET status='COMPLETED', finished_at=?, payment_status='PAID' WHERE id=?`, nowIso(), current.id);
      return null;
    }

    const ts = nowIso();
    if (current) {
      run(`UPDATE bookings SET status='COMPLETED', finished_at=?, payment_status='PAID' WHERE id=?`, ts, current.id);
    }
    run(`UPDATE bookings SET status='CURRENT', called_at=? WHERE id=?`, ts, next.id);
    return mapClinicBooking(get(`${BOOKING_SELECT} WHERE b.id=?`, next.id));
  });
}

function promoteSpecific(id) {
  return tx(() => {
    const booking = get(`${BOOKING_SELECT} WHERE b.id=?`, id);
    if (!booking) fail('الحجز غير موجود.');
    if (booking.status === 'CURRENT') return mapClinicBooking(booking);
    if (booking.status !== 'WAITING') fail('لا يمكن استدعاء هذا الدور.');

    const ts = nowIso();
    run(`UPDATE bookings SET status='WAITING', called_at=NULL WHERE clinic_id=? AND date=? AND status='CURRENT' AND id!=?`,
      booking.clinic_id, booking.date, id);
    run(`UPDATE bookings SET status='CURRENT', called_at=? WHERE id=?`, ts, id);
    return mapClinicBooking(get(`${BOOKING_SELECT} WHERE b.id=?`, id));
  });
}

const ALLOWED_TRANSITIONS = { COMPLETED: ['WAITING', 'CURRENT'], NO_SHOW: ['WAITING', 'CURRENT'], CANCELLED: ['WAITING', 'CURRENT'] };

function setBookingStatus(id, status, ownerUid) {
  if (!ALLOWED_TRANSITIONS[status]) fail('حالة غير معروفة.');
  return tx(() => {
    const booking = get(`SELECT * FROM bookings WHERE id=?`, id);
    if (!booking) fail('الحجز غير موجود.');
    if (ownerUid !== undefined && booking.user_id !== ownerUid) fail('الحجز غير موجود.');
    if (!ALLOWED_TRANSITIONS[status].includes(booking.status)) fail('لا يمكن تنفيذ هذه العملية.');
    const ts = nowIso();
    const paid = status === 'COMPLETED' ? `, payment_status='PAID'` : '';
    const finished = (status === 'COMPLETED' || status === 'NO_SHOW') ? `, finished_at='${ts}'` : '';
    run(`UPDATE bookings SET status=?${paid}${finished} WHERE id=?`, status, id);
  });
}

function togglePayment(id) {
  return tx(() => {
    const booking = get(`SELECT * FROM bookings WHERE id=?`, id);
    if (!booking) fail('الحجز غير موجود.');
    const next = (booking.payment_status ?? 'UNPAID') === 'PAID' ? 'UNPAID' : 'PAID';
    run(`UPDATE bookings SET payment_status=? WHERE id=?`, next, id);
  });
}

/* ── TTS ── */

const TRUSTED_CLIENT_TOKEN = '6A5AA1D4EAFF4E9FB37E23D68491D6F4';
const CHROMIUM_FULL_VERSION = '143.0.3650.75';
const WINDOWS_FILE_TIME_EPOCH = 11644473600n;

function generateSecMsGecToken() {
  const ticks = BigInt(Math.floor(Date.now() / 1000 + Number(WINDOWS_FILE_TIME_EPOCH))) * 10000000n;
  const rounded = ticks - (ticks % 3000000000n);
  return createHash('sha256').update(`${rounded}${TRUSTED_CLIENT_TOKEN}`, 'ascii').digest('hex').toUpperCase();
}

const DEFAULT_TTS_VOICE = 'ar-DZ-IsmaelNeural';

function escapeXml(s) {
  return s.replace(/[<>&"']/g, (c) => c === '<' ? '&lt;' : c === '>' ? '&gt;' : c === '&' ? '&amp;' : c === '"' ? '&quot;' : '&apos;');
}

function edgeTtsToFile(text, audioPath, voice, lang) {
  return new Promise((resolve, reject) => {
    const url =
      `wss://speech.platform.bing.com/consumer/speech/synthesize/readaloud/edge/v1` +
      `?TrustedClientToken=${TRUSTED_CLIENT_TOKEN}` +
      `&Sec-MS-GEC=${generateSecMsGecToken()}` +
      `&Sec-MS-GEC-Version=1-${CHROMIUM_FULL_VERSION}`;
    const ws = new WebSocket(url, {
      host: 'speech.platform.bing.com',
      origin: 'chrome-extension://jdiccldimpdaibmpdkjnbmckianbfold',
      headers: {
        Pragma: 'no-cache', 'Cache-Control': 'no-cache',
        'User-Agent': `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${CHROMIUM_FULL_VERSION.split('.')[0]}.0.0.0 Safari/537.36 Edg/${CHROMIUM_FULL_VERSION.split('.')[0]}.0.0.0`,
        'Accept-Encoding': 'gzip, deflate, br', 'Accept-Language': 'en-US,en;q=0.9',
      },
    });

    const timer = setTimeout(() => { try { ws.close(); } catch {} reject(new Error('TTS timeout')); }, 10000);
    const fail = (err) => { clearTimeout(timer); reject(err); };
    ws.on('error', fail);
    ws.on('open', () => {
      ws.send(`Content-Type:application/json; charset=utf-8\r\nPath:speech.config\r\n\r\n` +
        JSON.stringify({ context: { synthesis: { audio: { metadataoptions: { sentenceBoundaryEnabled: 'false', wordBoundaryEnabled: 'false' }, outputFormat: 'audio-24khz-48kbitrate-mono-mp3' } } } }));
      const requestId = randomBytes(16).toString('hex');
      ws.send(`X-RequestId:${requestId}\r\nContent-Type:application/ssml+xml\r\nPath:ssml\r\n\r\n` +
        `<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="${lang}">` +
        `<voice name="${voice}"><prosody rate="10%" pitch="default">${escapeXml(text)}</prosody></voice></speak>`);
    });

    let stream = null; let bytes = 0;
    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        const sep = 'Path:audio\r\n'; const idx = data.indexOf(sep) + sep.length;
        const chunk = data.subarray(idx); bytes += chunk.length;
        if (!stream) stream = createWriteStream(audioPath);
        stream.write(chunk);
        return;
      }
      if (data.toString().includes('Path:turn.end')) {
        clearTimeout(timer);
        const done = () => { try { ws.close(); } catch {} resolve(bytes); };
        if (stream) stream.end(done); else done();
      }
    });
  });
}

const ttsInFlight = new Map();

async function handleTts(req, res, query) {
  const text = String(query.get('text') ?? '').trim().slice(0, 500);
  const voiceParam = String(query.get('voice') ?? '');
  const voice = /^[A-Za-z]+-[A-Za-z]+-[A-Za-z]+Neural$/.test(voiceParam) ? voiceParam : DEFAULT_TTS_VOICE;
  const lang = `${voice.split('-')[0]}-${voice.split('-')[1]}`;
  if (!text) return json(res, 400, { error: 'النص مطلوب.' });

  const hash = createHash('sha1').update(`${voice}|${text}`).digest('hex');
  const outFile = path.join(TTS_CACHE_DIR, `${hash}.mp3`);
  mkdirSync(TTS_CACHE_DIR, { recursive: true });

  if (!existsSync(outFile)) {
    if (ttsInFlight.has(hash)) await ttsInFlight.get(hash);
    else {
      const job = edgeTtsToFile(text, outFile, voice, lang)
        .catch((err) => { try { unlinkSync(outFile); } catch {} throw err; })
        .finally(() => ttsInFlight.delete(hash));
      ttsInFlight.set(hash, job);
      try { await job; }
      catch { return json(res, 502, { error: 'تعذر توليد الصوت.' }); }
    }
  }

  res.writeHead(200, { 'Content-Type': 'audio/mpeg', 'Cache-Control': 'public, max-age=86400' });
  createReadStream(outFile).pipe(res);
}

/* ── HTTP Handler ── */

const json = (res, code, payload) => {
  const body = JSON.stringify(payload);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(body);
};

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', (chunk) => { size += chunk.length; if (size > 1_000_000) { reject(new Error('too large')); req.destroy(); return; } chunks.push(chunk); });
    req.on('end', () => { if (chunks.length === 0) return resolve(undefined); try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); } catch { reject(new Error('invalid JSON')); } });
    req.on('error', reject);
  });
}

async function handle(req, res, pathname, query) {
  const seg = pathname.split('/').filter(Boolean);

  /* ── TTS ── */
  if (req.method === 'GET' && pathname === '/api/tts') return handleTts(req, res, query);

  /* ── Online Status ── */
  if (req.method === 'GET' && pathname === '/api/online-status') {
    const clinicId = query.get('clinicId') ?? '';
    const date = query.get('date') ?? isoDate(1);
    const c = get(`SELECT * FROM clinics WHERE id=?`, clinicId);
    if (!c) return json(res, 404, { error: 'العيادة غير موجودة.' });
    const max = typeof c.max_online === 'number' ? c.max_online : 30;
    const used = get(`SELECT COUNT(*) AS n FROM bookings WHERE clinic_id=? AND date=? AND user_id NOT LIKE 'guest-%' AND status != 'CANCELLED'`, clinicId, date)?.n ?? 0;
    return json(res, 200, { enabled: c.is_open === 1 && c.online_enabled === 1, used, max, remaining: max > 0 ? Math.max(0, max - used) : null });
  }

  /* ── System ── */
  if (req.method === 'GET' && pathname === '/api/health') return json(res, 200, { ok: true, db: DB_PATH });

  /* ══════════════════════════════════════════════
     لوحة الأدمين — إدارة العيادات
     ══════════════════════════════════════════════ */

  /* ── دخول الأدمين ── */
  if (pathname === '/api/admin/login' && req.method === 'POST') {
    const body = await readBody(req);
    const pin = String(body?.pin ?? '').trim();
    if (pin !== ADMIN_PIN) return json(res, 401, { error: 'رمز الأدمين غير صحيح.' });
    const token = genId('adm-');
    run(`INSERT OR REPLACE INTO meta(k,v) VALUES (?,?)`, `admin_token`, token);
    return json(res, 200, { ok: true, token });
  }

  /* middleware: تحقق من صلاحية الأدمين */
  const isAdmin = (req) => {
    const auth = req.headers.authorization || '';
    const token = auth.replace('Bearer ', '');
    const stored = get(`SELECT v FROM meta WHERE k=?`, 'admin_token');
    return stored && stored.v === token;
  };

  /* ── قائمة جميع العيادات (أدمين) ── */
  if (pathname === '/api/admin/clinics' && req.method === 'GET') {
    if (!isAdmin(req)) return json(res, 401, { error: 'غير مصرح.' });
    const clinics = all(`SELECT * FROM clinics ORDER BY created_at DESC`).map(mapClinic);
    return json(res, 200, clinics);
  }

  /* ── إضافة عيادة جديدة ── */
  if (pathname === '/api/admin/clinics' && req.method === 'POST') {
    if (!isAdmin(req)) return json(res, 401, { error: 'غير مصرح.' });
    const body = await readBody(req);
    const name = String(body?.name ?? '').trim();
    const specialty = String(body?.specialty ?? '').trim();
    if (!name) fail('اسم العيادة مطلوب.');
    if (!specialty) fail('التخصص مطلوب.');

    const id = genId('cl-');
    const code = genCode();
    const salt = randomBytes(16).toString('hex');
    const ts = nowIso();

    run(
      `INSERT INTO clinics (id, name, specialty, city, address, phone, fee, commission, color, activation_code, activated, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)`,
      id, name, specialty,
      String(body?.city ?? '').trim(),
      String(body?.address ?? '').trim(),
      String(body?.phone ?? '').trim(),
      Number(body?.fee) || 200,
      Number(body?.commission) || 50,
      String(body?.color ?? '').trim(),
      code,
      ts,
    );

    return json(res, 200, { ok: true, id, activationCode: code });
  }

  /* ── حذف عيادة ── */
  if (pathname === '/api/admin/clinics/delete' && req.method === 'POST') {
    if (!isAdmin(req)) return json(res, 401, { error: 'غير مصرح.' });
    const body = await readBody(req);
    const clinicId = String(body?.id ?? '').trim();
    if (!clinicId) fail('معرّف العيادة مطلوب.');
    const row = get(`SELECT id FROM clinics WHERE id=?`, clinicId);
    if (!row) return json(res, 404, { error: 'العيادة غير موجودة.' });
    run(`DELETE FROM bookings WHERE clinic_id=?`, clinicId);
    run(`DELETE FROM counters WHERE key LIKE ?`, `${clinicId}|%`);
    run(`DELETE FROM clinics WHERE id=?`, clinicId);
    return json(res, 200, { ok: true });
  }

  /* ══════════════════════════════════════════════
     تنشيط العيادة — كود لمرة واحدة
     ══════════════════════════════════════════════ */

  if (pathname === '/api/clinic/activate' && req.method === 'POST') {
    const body = await readBody(req);
    const code = String(body?.activationCode ?? '').trim().toUpperCase();
    const password = String(body?.password ?? '').trim();
    const clinicName = String(body?.clinicName ?? '').trim();
    const tagline = String(body?.tagline ?? '').trim();
    const address = String(body?.address ?? '').trim();
    const phone = String(body?.phone ?? '').trim();

    if (!code) fail('كود التفعيل مطلوب.');
    if (!password || password.length < 4) fail('كلمة المرور يجب أن تكون 4 أحرف على الأقل.');
    if (!clinicName) fail('اسم العيادة مطلوب.');

    const clinic = get(`SELECT * FROM clinics WHERE activation_code=?`, code);
    if (!clinic) fail('كود التفعيل غير صحيح.');
    if (clinic.activated === 1) fail('تم استخدام هذا الكود بالفعل.');

    const salt = randomBytes(16).toString('hex');
    const passHash = hashPass(password, salt);

    run(
      `UPDATE clinics SET activated=1, password_hash=?, password_salt=?, name=?, tagline=?, address=?, phone=? WHERE id=?`,
      passHash, salt, clinicName, tagline, address, phone, clinic.id,
    );

    const token = genId('c-');
    run(`INSERT OR REPLACE INTO meta(k,v) VALUES (?,?)`, `clinic_token_${clinic.id}`, token);

    return json(res, 200, { ok: true, clinicId: clinic.id, token, name: clinicName });
  }

  /* ── دخول العيادة ── */
  if (pathname === '/api/clinic/login' && req.method === 'POST') {
    const body = await readBody(req);
    const clinicId = String(body?.clinicId ?? '').trim();
    const password = String(body?.password ?? '').trim();
    if (!clinicId || !password) fail('البيانات غير مكتملة.');

    const clinic = get(`SELECT * FROM clinics WHERE id=?`, clinicId);
    if (!clinic) return json(res, 404, { error: 'العيادة غير موجودة.' });
    if (clinic.activated !== 1) return json(res, 403, { error: 'العيادة غير مفعّلة بعد.' });
    if (!clinic.password_hash || !clinic.password_salt) return json(res, 403, { error: 'لم يتم تعيين كلمة مرور.' });

    const hash = hashPass(password, clinic.password_salt);
    if (hash !== clinic.password_hash) return json(res, 401, { error: 'كلمة المرور غير صحيحة.' });

    const token = genId('c-');
    run(`INSERT OR REPLACE INTO meta(k,v) VALUES (?,?)`, `clinic_token_${clinic.id}`, token);

    return json(res, 200, { ok: true, token, name: clinic.name, tagline: clinic.tagline });
  }

  /* middleware: تحقق من صلاحية العيادة */
  const getClinicId = (req) => {
    const auth = req.headers.authorization || '';
    const token = auth.replace('Bearer ', '');
    for (const row of all(`SELECT k, v FROM meta WHERE k LIKE 'clinic_token_%'`)) {
      if (row.v === token) {
        return row.k.replace('clinic_token_', '');
      }
    }
    return null;
  };

  /* ══════════════════════════════════════════════
     لوحة العيادة
     ══════════════════════════════════════════════ */

  /* ── بيانات لوحة العيادة (مقيدة بالعيادة) ── */
  if (pathname === '/api/clinic/dashboard' && req.method === 'GET') {
    const clinicId = getClinicId(req);
    if (!clinicId) return json(res, 401, { error: 'غير مصرح.' });

    const date = query.get('date');
    if (!isDateStr(date)) fail('التاريخ غير صحيح.');

    const clinic = get(`SELECT * FROM clinics WHERE id=?`, clinicId);
    const bookings = all(`${BOOKING_SELECT} WHERE b.clinic_id=? AND b.date=? ORDER BY b.queue_number`, clinicId, date).map(mapClinicBooking);

    return json(res, 200, {
      clinic: clinic ? mapClinic(clinic) : null,
      bookings,
    });
  }

  /* ── إيقاف/استئناف الطابور ── */
  if (pathname === '/api/clinic/pause' && req.method === 'POST') {
    const clinicId = getClinicId(req);
    if (!clinicId) return json(res, 401, { error: 'غير مصرح.' });
    const body = await readBody(req);
    const paused = body?.paused === true;
    run(`UPDATE clinics SET is_paused=? WHERE id=?`, paused ? 1 : 0, clinicId);
    return json(res, 200, { ok: true, isPaused: paused });
  }

  /* ── استدعاء التالي (مقيد بالعيادة) ── */
  if (pathname === '/api/clinic/call-next' && req.method === 'POST') {
    const clinicId = getClinicId(req);
    if (!clinicId) return json(res, 401, { error: 'غير مصرح.' });
    const body = await readBody(req);
    const date = body?.date || isoDate(0);
    return json(res, 200, callNextPatient(clinicId, date));
  }

  /* ── إتمام حجز (مقيد) ── */
  if (pathname === '/api/clinic/complete' && req.method === 'POST') {
    const clinicId = getClinicId(req);
    if (!clinicId) return json(res, 401, { error: 'غير مصرح.' });
    const body = await readBody(req);
    const bookingId = String(body?.bookingId ?? '').trim();
    if (!bookingId) fail('معرّف الحجز مطلوب.');
    const booking = get(`SELECT * FROM bookings WHERE id=? AND clinic_id=?`, bookingId, clinicId);
    if (!booking) return json(res, 404, { error: 'الحجز غير موجود.' });
    setBookingStatus(bookingId, 'COMPLETED');
    return json(res, 200, { ok: true });
  }

  /* ── إضافة مريض يدوي (مقيد) ── */
  if (pathname === '/api/clinic/add-walkin' && req.method === 'POST') {
    const clinicId = getClinicId(req);
    if (!clinicId) return json(res, 401, { error: 'غير مصرح.' });
    const body = await readBody(req);
    return json(res, 200, createWalkinTicket({ ...body, doctorId: clinicId }));
  }

  /* ── معلومات العيادة ── */
  if (pathname === '/api/clinic/info' && req.method === 'GET') {
    const clinicId = getClinicId(req);
    if (!clinicId) return json(res, 401, { error: 'غير مصرح.' });
    const clinic = get(`SELECT * FROM clinics WHERE id=?`, clinicId);
    return json(res, 200, clinic ? mapClinic(clinic) : null);
  }

  /* ══════════════════════════════════════════════
     API قديمة (للتوافق مع تطبيق المريض)
     ══════════════════════════════════════════════ */

  /* ── العيادات (عام) ── */
  if (seg[1] === 'clinics' && !seg[1].startsWith('admin') && !seg[1].startsWith('clinic')) {
    if (req.method === 'GET' && seg.length === 2) {
      return json(res, 200, all(`SELECT * FROM clinics WHERE activated=1 AND is_open=1 ORDER BY rating DESC`).map(mapClinic));
    }
    if (req.method === 'GET' && seg.length === 3) {
      const row = get(`SELECT * FROM clinics WHERE id=?`, seg[2]);
      return json(res, 200, row ? mapClinic(row) : null);
    }
    if (req.method === 'PATCH' && seg.length === 3) {
      const body = await readBody(req);
      const row = get(`SELECT * FROM clinics WHERE id=?`, seg[2]);
      if (!row) return json(res, 404, { error: 'العيادة غير موجودة.' });
      const sets = []; const params = [];
      if (typeof body.isOpen === 'boolean') { sets.push('is_open=?'); params.push(body.isOpen ? 1 : 0); }
      if (typeof body.onlineEnabled === 'boolean') { sets.push('online_enabled=?'); params.push(body.onlineEnabled ? 1 : 0); }
      if (body.maxOnline !== undefined) { sets.push('max_online=?'); params.push(Math.max(0, Math.round(Number(body.maxOnline)))); }
      if (body.commission !== undefined) { sets.push('commission=?'); params.push(Math.max(0, Math.round(Number(body.commission)))); }
      for (const col of ['name', 'specialty', 'city', 'address', 'phone']) {
        if (typeof body[col] === 'string') { sets.push(`${col}=?`); params.push(body[col].trim()); }
      }
      if (sets.length > 0) { params.push(seg[2]); run(`UPDATE clinics SET ${sets.join(', ')} WHERE id=?`, ...params); }
      return json(res, 200, mapClinic(get(`SELECT * FROM clinics WHERE id=?`, seg[2])));
    }
  }

  /* ── الإعدادات ── */
  if (pathname === '/api/settings') {
    if (req.method === 'GET') return json(res, 200, getSettings());
    if (req.method === 'PUT') {
      const body = await readBody(req);
      run(`INSERT INTO meta(k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`, SETTINGS_KEY, JSON.stringify({ ...getSettings(), ...body }));
      return json(res, 200, { ok: true });
    }
  }

  /* ── الحجوزات ── */
  if (pathname === '/api/bookings' && req.method === 'POST') {
    const body = await readBody(req);
    const scopeId = getClinicId(req);
    return json(res, 200, createWalkinTicket({ ...body, doctorId: body.doctorId || body.clinicId || scopeId }));
  }

  if (pathname === '/api/walkin' && req.method === 'POST') {
    const body = await readBody(req);
    const scopeId = getClinicId(req);
    return json(res, 200, createWalkinTicket({ ...body, doctorId: body.doctorId || scopeId }));
  }

  if (pathname === '/api/bookings' && req.method === 'GET') {
    const scopeId = getClinicId(req);
    const clauses = []; const params = [];
    if (scopeId) { clauses.push('b.clinic_id=?'); params.push(scopeId); }
    if (query.get('date')) { clauses.push('b.date=?'); params.push(query.get('date')); }
    if (query.get('doctorId')) { clauses.push('b.clinic_id=?'); params.push(query.get('doctorId')); }
    if (query.get('phone')) { clauses.push('b.whatsapp=?'); params.push(query.get('phone')); }
    if (query.get('userId')) { clauses.push('b.user_id=?'); params.push(query.get('userId')); }
    const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
    const order = query.get('userId') || query.get('phone') ? 'ORDER BY b.created_at DESC' : 'ORDER BY b.queue_number ASC';
    const rows = all(`${BOOKING_SELECT} ${where} ${order}`, ...params);
    const mapper = query.get('view') === 'clinic' ? mapClinicBooking : mapBooking;
    return json(res, 200, rows.map(mapper));
  }

  if (seg[1] === 'bookings' && seg.length >= 3) {
    const id = seg[2];
    if (req.method === 'GET' && seg.length === 3) {
      const row = get(`${BOOKING_SELECT} WHERE b.id=?`, id);
      return json(res, 200, row ? mapBooking(row) : null);
    }
    if (req.method === 'PATCH' && seg.length === 4 && seg[3] === 'status') {
      const body = await readBody(req);
      setBookingStatus(id, body?.status, body?.userId);
      return json(res, 200, { ok: true });
    }
    if (req.method === 'POST' && seg.length === 4 && seg[3] === 'promote') {
      return json(res, 200, promoteSpecific(id));
    }
    if (req.method === 'POST' && seg.length === 4 && seg[3] === 'payment-toggle') {
      togglePayment(id);
      return json(res, 200, { ok: true });
    }
  }

  if (pathname === '/api/queue/call-next' && req.method === 'POST') {
    const body = await readBody(req);
    const scopeId = getClinicId(req);
    return json(res, 200, callNextPatient(body?.doctorId || scopeId, body?.date));
  }

  /* ── Dashboard (يُقيّد بالعيادة إذا كان التوكن موجودًا) ── */
  if (pathname === '/api/dashboard' && req.method === 'GET') {
    const date = query.get('date');
    if (!isDateStr(date)) fail('التاريخ غير صحيح.');
    const scopeClinicId = getClinicId(req);
    let doctors, bookings;
    if (scopeClinicId) {
      const cl = get(`SELECT * FROM clinics WHERE id=?`, scopeClinicId);
      doctors = cl ? [mapDoctor(cl)] : [];
      bookings = all(`${BOOKING_SELECT} WHERE b.clinic_id=? AND b.date=? ORDER BY b.queue_number`, scopeClinicId, date).map(mapClinicBooking);
    } else {
      doctors = all(`SELECT * FROM clinics WHERE activated=1 ORDER BY name`).map(mapDoctor);
      bookings = all(`${BOOKING_SELECT} WHERE b.date=? ORDER BY b.queue_number`, date).map(mapClinicBooking);
    }
    const settings = getSettings();
    if (scopeClinicId) {
      const cl = get(`SELECT * FROM clinics WHERE id=?`, scopeClinicId);
      if (cl) { settings.name = cl.name; settings.tagline = cl.tagline || ''; }
    }
    return json(res, 200, { settings, doctors, bookings });
  }

  if (seg[1] === 'ticket' && req.method === 'GET' && seg.length === 3) {
    const raw = get(`${BOOKING_SELECT} WHERE b.id=?`, seg[2]);
    if (!raw) return json(res, 200, null);
    const siblings = all(`${BOOKING_SELECT} WHERE b.clinic_id=? AND b.date=? ORDER BY b.queue_number`, raw.clinic_id, raw.date);
    const aheadCount = siblings.filter((b) => b.status === 'WAITING' && b.queue_number < raw.queue_number).length;
    const current = siblings.find((b) => b.status === 'CURRENT');
    return json(res, 200, { booking: query.get('view') === 'clinic' ? mapClinicBooking(raw) : mapBooking(raw), aheadCount, currentNumber: current?.queue_number ?? null, settings: getSettings() });
  }

  if (pathname === '/api/patients' && req.method === 'GET') {
    const date = query.get('date') ?? isoDate(0);
    const clinicId = query.get('clinicId');
    const scopeId = getClinicId(req);
    const finalClinicId = clinicId || scopeId;
    let sql = `${BOOKING_SELECT} WHERE b.date=? AND b.user_id NOT LIKE 'guest-%'`;
    const params = [date];
    if (finalClinicId) { sql += ` AND b.clinic_id=?`; params.push(finalClinicId); }
    sql += ` ORDER BY b.queue_number ASC`;
    return json(res, 200, all(sql, ...params).map(mapClinicBooking));
  }

  if (pathname === '/api/stats/week' && req.method === 'GET') {
    const result = [];
    for (let i = 6; i >= 0; i -= 1) {
      const date = isoDate(-i);
      const rows = all(`SELECT status, fee FROM bookings WHERE date=?`, date);
      const completed = rows.filter((r) => r.status === 'COMPLETED');
      result.push({ date, total: rows.length, completed: completed.length, revenue: completed.reduce((sum, r) => sum + r.fee, 0) });
    }
    return json(res, 200, result);
  }

  if (!pathname.startsWith('/api')) return undefined;
  return json(res, 404, { error: `مسار غير معروف: ${req.method} ${pathname}` });
}

/* ── Static files ── */

const LOCAL_DIST = path.join(__dirname, 'dist');
const STATIC_DIST = process.env.STATIC_DIR ?? (existsSync(LOCAL_DIST) ? LOCAL_DIST : path.join(__dirname, '..', 'clinic-app', 'dist'));
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.ico': 'image/x-icon',
  '.json': 'application/json; charset=utf-8', '.woff2': 'font/woff2',
  '.webmanifest': 'application/manifest+json',
};

function serveStatic(res, filePath) {
  const ext = path.extname(filePath).toLowerCase();
  res.writeHead(200, { 'Content-Type': MIME[ext] ?? 'application/octet-stream' });
  createReadStream(filePath).pipe(res);
}

function handleStatic(req, res, pathname) {
  if (req.method !== 'GET') return false;
  if (existsSync(STATIC_DIST)) {
    const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
    const target = path.normalize(path.join(STATIC_DIST, rel));
    if (target.startsWith(STATIC_DIST) && existsSync(target) && !target.includes('..')) {
      serveStatic(res, target); return true;
    }
    const fallback = path.join(STATIC_DIST, 'index.html');
    if (existsSync(fallback)) { serveStatic(res, fallback); return true; }
  }
  return false;
}

const server = createServer((req, res) => {
  const origin = req.headers.origin;
  if (origin) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.setHeader('Access-Control-Max-Age', '600');
  }
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  const url = new URL(req.url, `http://localhost:${PORT}`);
  handle(req, res, url.pathname, url.searchParams).catch((err) => {
    if (err instanceof DomainError) return json(res, 409, { error: err.message });
    console.error('[dawri-server]', err.message);
    return json(res, 500, { error: 'خطأ داخلي في السيرفر.' });
  }).catch(() => {}).then((served) => {
    if (!served && !res.writableEnded) {
      const handled = handleStatic(req, res, url.pathname);
      if (!handled && !res.writableEnded) {
        res.writeHead(404, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end('<h1>404</h1>');
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`[dawri-server] http://localhost:${PORT} — DB: ${DB_PATH}`);
});
