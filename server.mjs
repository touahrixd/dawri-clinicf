/* ============================================================================
   dawri-server — السيرفر المشترك لتطبيقَي «دوري»
   ---------------------------------------------------------------------------
   • صفر تبعيات خارجية: node:http + node:sqlite المدمج في Node 26
   • قاعدة بيانات SQLite حقيقية على القرص: dawri-server/dawri.db
   • تطبيق المريض (:5180) ولوحة العيادة (:5181) يتصلان هنا عبر REST API
   ========================================================================== */

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

  CREATE TABLE IF NOT EXISTS users (
    uid        TEXT PRIMARY KEY,
    name       TEXT NOT NULL DEFAULT '',
    email      TEXT NOT NULL DEFAULT '',
    phone      TEXT,
    provider   TEXT NOT NULL,
    photo_url  TEXT,
    address    TEXT NOT NULL DEFAULT '',
    whatsapp   TEXT NOT NULL DEFAULT '',
    lat        REAL,
    lng        REAL,
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS clinics (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    specialty   TEXT NOT NULL,
    city        TEXT NOT NULL DEFAULT '',
    address     TEXT NOT NULL DEFAULT '',
    phone       TEXT NOT NULL DEFAULT '',
    rating      REAL NOT NULL DEFAULT 4,
    reviews     INTEGER NOT NULL DEFAULT 0,
    fee         INTEGER NOT NULL DEFAULT 200,
    avg_minutes INTEGER NOT NULL DEFAULT 15,
    certified   INTEGER NOT NULL DEFAULT 1,
    contracted  INTEGER NOT NULL DEFAULT 0,
    open_time   TEXT NOT NULL DEFAULT '20:00',
    always_open INTEGER NOT NULL DEFAULT 0,
    color       TEXT NOT NULL DEFAULT ''
  );

  CREATE TABLE IF NOT EXISTS bookings (
    id           TEXT PRIMARY KEY,
    clinic_id    TEXT NOT NULL,
    user_id      TEXT NOT NULL,
    patient_name TEXT NOT NULL,
    whatsapp     TEXT NOT NULL DEFAULT '',
    disability   INTEGER NOT NULL DEFAULT 0,
    date         TEXT NOT NULL,
    queue_number INTEGER NOT NULL,
    status       TEXT NOT NULL DEFAULT 'WAITING',
    fee          INTEGER NOT NULL DEFAULT 0,
    called_at    TEXT,
    finished_at  TEXT,
    created_at   TEXT NOT NULL
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
  `ALTER TABLE clinics ADD COLUMN is_open INTEGER NOT NULL DEFAULT 1`,
  `ALTER TABLE clinics ADD COLUMN commission INTEGER NOT NULL DEFAULT 50`,
  `ALTER TABLE clinics ADD COLUMN online_enabled INTEGER NOT NULL DEFAULT 1`,
  `ALTER TABLE clinics ADD COLUMN max_online INTEGER NOT NULL DEFAULT 30`,
  `ALTER TABLE bookings ADD COLUMN payment_status TEXT NOT NULL DEFAULT 'UNPAID'`,
]) {
  try {
    db.prepare(stmt).run();
  } catch {
    /* العمود موجود مسبقًا */
  }
}

/* ─────────── أدوات مساعدة ─────────── */

const get = (sql, ...params) => db.prepare(sql).get(...params);
const all = (sql, ...params) => db.prepare(sql).all(...params);
const run = (sql, ...params) => { db.prepare(sql).run(...params); };

class DomainError extends Error {}
const fail = (message) => { throw new DomainError(message); };

/** معاملة ذرّية */
function tx(fn) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const result = fn();
    db.exec('COMMIT');
    return result;
  } catch (err) {
    try { db.exec('ROLLBACK'); } catch { /* لا معاملة مفتوحة */ }
    throw err;
  }
}

const pad2 = (n) => String(n).padStart(2, '0');

/** تاريخ محلي بصيغة YYYY-MM-DD مع إزاحة أيام */
function isoDate(offsetDays = 0) {
  const d = new Date();
  d.setDate(d.getDate() + offsetDays);
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

const nowIso = () => new Date().toISOString();
const genId = (prefix = '') =>
  `${prefix}${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 9)}`;

const isDateStr = (s) => typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);

/* ─────────── البذر الأولي (مطابق لتطبيق المريض) ─────────── */

const SETTINGS_KEY = 'clinic_settings';

export const DEFAULT_SETTINGS = {
  key: 'clinic',
  name: 'عيادة النخبة الطبية',
  tagline: 'صحتك أولوية — بدون انتظار طويل',
  address: 'شارع الاستقلال، حي النور، الوادي',
  phone: '032741234',
  fee: 300,
  avgVisitMinutes: 10,
  maxQueuePerDay: 40,
  openTime: '08:00',
  closeTime: '17:00',
  pin: '1234',
  secretaryPin: '2222',
};

const SEED_CLINICS = [
  /* [id,name,specialty,city,address,phone,rating,reviews,fee,avg_min,certified,contracted,color,is_open,commission] */
  ['cl-shifa', 'عيادة الشفاء الطبية', 'طب عام', 'الجزائر الوسطى',
   'شارع ديدوش مراد، برج المكتبات، الطابق الثالث', '0555123400',
   4.8, 214, 50, 12, 1, 1, 'from-emerald-500 to-teal-600', 1, 50],
  ['cl-nour', 'عيادة النور لطب الأسنان', 'طب الأسنان', 'بئر مراد رايس',
   'حي البدر، عمارة 12، بجانب الصيدلية المركزية', '0661234567',
   4.6, 98, 80, 20, 1, 1, 'from-sky-500 to-indigo-500', 1, 80],
  ['cl-basra', 'مركز البصرة الطبي للأطفال', 'طب الأطفال', 'حسين داي',
   'شارع العقيد لطفي، أمام المسرح البلدي', '0770998811',
   4.9, 341, 60, 15, 1, 0, 'from-rose-400 to-orange-400', 0, 60],
  ['cl-hayat', 'عيادة الحياة النسائية', 'نساء وتوليد', 'باب الوادي',
   'نهج طرابلس، عمارة 4، الطابق الأول', '0550776611',
   4.4, 76, 100, 18, 1, 0, 'from-fuchsia-500 to-purple-500', 0, 100],
];

function bootstrap() {
  for (const c of SEED_CLINICS) {
    run(
      `INSERT OR IGNORE INTO clinics
       (id,name,specialty,city,address,phone,rating,reviews,fee,avg_minutes,certified,contracted,color,is_open,commission)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      ...c,
    );
  }

  if (!get(`SELECT k FROM meta WHERE k=?`, SETTINGS_KEY)) {
    run(`INSERT INTO meta(k,v) VALUES (?,?)`, SETTINGS_KEY, JSON.stringify(DEFAULT_SETTINGS));
  }

  /* حشد انتظار تجريبي لعيادة الشفاء — يُعاد زرعه كل يوم جديد فقط */
  const today = isoDate(0);
  run(`DELETE FROM bookings WHERE user_id='seed' AND date != ?`, today);
  const crowd = get(
    `SELECT COUNT(*) AS n FROM bookings WHERE clinic_id=? AND date=? AND user_id='seed'`,
    'cl-shifa', today,
  );
  if ((crowd?.n ?? 0) === 0) {
    tx(() => {
      for (let i = 1; i <= 6; i += 1) {
        run(
          `INSERT OR IGNORE INTO bookings
           (id,clinic_id,user_id,patient_name,whatsapp,disability,date,queue_number,status,fee,created_at)
           VALUES (?,?,?,?,?,?,?,?,'WAITING',?,?)`,
          `seed-${today}-${i}`, 'cl-shifa', 'seed', `مرضى مسجلون ${i}`, '', 0,
          today, i, 50, nowIso(),
        );
      }
    });
  }
}
bootstrap();

/* ─────────── المُخطِّطات (Mappers) ─────────── */

function mapClinic(r) {
  return {
    id: r.id, nameAr: r.name, specialtyAr: r.specialty, city: r.city, address: r.address,
    phone: r.phone, rating: r.rating, reviews: r.reviews,
    commission: typeof r.commission === 'number' ? r.commission : 50,
    onlineEnabled: r.online_enabled !== 0,
    maxOnline: typeof r.max_online === 'number' ? r.max_online : 30,
    avgVisitMinutes: r.avg_minutes, certified: r.certified === 1, contracted: r.contracted === 1,
    isOpen: r.is_open === 1, color: r.color,
  };
}

function mapUser(r) {
  return {
    uid: r.uid, name: r.name, email: r.email,
    phone: r.phone ?? undefined, photoURL: r.photo_url ?? undefined,
    provider: r.provider,
    address: r.address || undefined, whatsapp: r.whatsapp || undefined,
    location: typeof r.lat === 'number' && typeof r.lng === 'number'
      ? { lat: r.lat, lng: r.lng } : undefined,
    createdAt: r.created_at,
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

/* نسخة الحجز الخاصة بلوحة العيادة (doctorId بدل clinicId) */
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

function mapDoctor(r) {
  return {
    id: r.id, name: r.name, specialty: r.specialty, phone: r.phone || undefined,
    active: r.is_open === 1, createdAt: '',
  };
}

const BOOKING_SELECT = `
  SELECT b.*, c.name AS clinic_name, c.specialty AS clinic_specialty
  FROM bookings b JOIN clinics c ON c.id = b.clinic_id
`;

/* ─────────── منطق الأعمال ─────────── */

function nextQueueNumber(clinicId, date) {
  const maxRow = get(
    `SELECT MAX(queue_number) AS m FROM bookings WHERE clinic_id=? AND date=? AND status!='CANCELLED'`,
    clinicId, date,
  );
  const counterRow = get(`SELECT value FROM counters WHERE key=?`, `${clinicId}|${date}`);
  return Math.max(counterRow?.value ?? 0, maxRow?.m ?? 0) + 1;
}

function bumpCounter(key, value) {
  run(
    `INSERT INTO counters(key,value) VALUES (?,?)
     ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
    key, value,
  );
}

/** حجز إلكتروني من تطبيق المريض (الموعد غدًا — يحدده العميل) */
function createAppBooking(body) {
  const { clinicId, date, userId, patientName, whatsapp, disability } = body ?? {};
  if (!clinicId || !isDateStr(date) || !userId) fail('بيانات الحجز غير مكتملة.');
  if (!patientName || !String(patientName).trim()) fail('أدخل اسم المريض.');
  if (!whatsapp || !String(whatsapp).trim()) fail('رقم الواتساب مطلوب.');

  return tx(() => {
    const clinic = get(`SELECT * FROM clinics WHERE id=?`, clinicId);
    if (!clinic) fail('العيادة غير موجودة.');
    if (clinic.is_open !== 1) {
      fail('العيادة مغلقة حاليًا — الحجز متاح عند فتحها من طرف العيادة.');
    }
    /* بوابة التسجيل الإلكتروني: يتحكم بها الممرض من اللوحة */
    if (clinic.online_enabled !== 1) {
      fail('التسجيل الإلكتروني مغلق حاليًا من إدارة العيادة — توجّه إلى العيادة مباشرة.');
    }
    /* سقف عدد التسجيلات الإلكترونية لهذا اليوم (0 = بلا حد) */
    const cap = typeof clinic.max_online === 'number' ? clinic.max_online : 30;
    if (cap > 0) {
      const used = get(
        `SELECT COUNT(*) AS n FROM bookings
         WHERE clinic_id=? AND date=? AND user_id NOT LIKE 'guest-%' AND status != 'CANCELLED'`,
        clinicId, date,
      )?.n ?? 0;
      if (used >= cap) {
        fail(`اكتمل العدد المسموح من التسجيلات الإلكترونية (${cap}) — حاول غدًا أو توجّه للعيادة.`);
      }
    }

    const dup = get(
      `SELECT id FROM bookings WHERE clinic_id=? AND date=? AND user_id=? AND status IN ('WAITING','CURRENT') LIMIT 1`,
      clinicId, date, userId,
    );
    if (dup) fail('لديك حجز نشط بنفس العيادة لهذا اليوم.');

    const n = nextQueueNumber(clinicId, date);
    bumpCounter(`${clinicId}|${date}`, n);

    const id = genId('bk-');
    run(
      `INSERT INTO bookings
       (id,clinic_id,user_id,patient_name,whatsapp,disability,date,queue_number,status,fee,payment_status,called_at,finished_at,created_at)
       VALUES (?,?,?,?,?,?,?,?,'WAITING',?,'UNPAID',NULL,NULL,?)`,
      id, clinicId, userId, String(patientName).trim(), String(whatsapp).trim(),
      disability ? 1 : 0, date, n, clinic.commission, nowIso(),
    );
    return mapBooking(get(`${BOOKING_SELECT} WHERE b.id=?`, id));
  });
}

/** تذكرة حضورية من لوحة العيادة (ليوم اليوم) */
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
    if (provider.is_open !== 1) fail('العيادة مغلقة حاليًا ولا تستقبل أدوارًا جديدة.');

    const sameDay = all(`${BOOKING_SELECT} WHERE b.clinic_id=? AND b.date=?`, doctorId, date);

    const clash = sameDay.find(
      (b) => b.whatsapp === patientPhone && (b.status === 'WAITING' || b.status === 'CURRENT'),
    );
    if (clash) fail('لديك دور نشط بالفعل مع هذا الطبيب اليوم. تابعيه من صفحة «حجوزاتي».');

    const taken = sameDay.filter((b) => b.status !== 'CANCELLED').length;
    if (taken >= settings.maxQueuePerDay) {
      fail(`اكتملت أدوار اليوم لهذا الطبيب (${settings.maxQueuePerDay} دورًا). يرجى المحاولة غدًا.`);
    }

    const n = nextQueueNumber(doctorId, date);
    bumpCounter(`${doctorId}|${date}`, n);

    const id = genId('bk-');
    run(
      `INSERT INTO bookings
       (id,clinic_id,user_id,patient_name,whatsapp,disability,date,queue_number,status,fee,payment_status,called_at,finished_at,created_at)
       VALUES (?,?,?,?,?,0,?,?,'WAITING',?,'UNPAID',NULL,NULL,?)`,
      id, doctorId, `guest-${patientPhone}`, String(patientName).trim(), String(patientPhone),
      date, n, provider.commission, nowIso(),
    );
    return mapClinicBooking(get(`${BOOKING_SELECT} WHERE b.id=?`, id));
  });
}

/** زر «التالي»: إنهاء الحالي (مع تسويّله مدفوعًا) ونداء التالي — أولوية الإعاقة */
function callNextPatient(doctorId, date) {
  if (!doctorId || !isDateStr(date)) fail('معطيات الطابور غير مكتملة.');
  return tx(() => {
    const current = get(
      `SELECT id FROM bookings WHERE clinic_id=? AND date=? AND status='CURRENT' LIMIT 1`,
      doctorId, date,
    );
    const next = get(
      `SELECT id FROM bookings WHERE clinic_id=? AND date=? AND status='WAITING'
       ORDER BY disability DESC, queue_number ASC LIMIT 1`,
      doctorId, date,
    );

    if (!next) {
      if (!current) fail('لا يوجد مرضى في الانتظار.');
      run(
        `UPDATE bookings SET status='COMPLETED', finished_at=?, payment_status='PAID' WHERE id=?`,
        nowIso(), current.id,
      );
      return null;
    }

    const ts = nowIso();
    if (current) {
      run(
        `UPDATE bookings SET status='COMPLETED', finished_at=?, payment_status='PAID' WHERE id=?`,
        ts, current.id,
      );
    }
    run(`UPDATE bookings SET status='CURRENT', called_at=? WHERE id=?`, ts, next.id);
    return mapClinicBooking(get(`${BOOKING_SELECT} WHERE b.id=?`, next.id));
  });
}

/** استدعاء دور محدد — يعيد الحالي السابق إلى الانتظار */
function promoteSpecific(id) {
  return tx(() => {
    const booking = get(`${BOOKING_SELECT} WHERE b.id=?`, id);
    if (!booking) fail('الحجز غير موجود.');
    if (booking.status === 'CURRENT') return mapClinicBooking(booking);
    if (booking.status !== 'WAITING') fail('لا يمكن استدعاء هذا الدور في وضعه الحالي.');

    const ts = nowIso();
    run(
      `UPDATE bookings SET status='WAITING', called_at=NULL
       WHERE clinic_id=? AND date=? AND status='CURRENT' AND id!=?`,
      booking.clinic_id, booking.date, id,
    );
    run(`UPDATE bookings SET status='CURRENT', called_at=? WHERE id=?`, ts, id);
    return mapClinicBooking(get(`${BOOKING_SELECT} WHERE b.id=?`, id));
  });
}

const ALLOWED_TRANSITIONS = {
  COMPLETED: ['WAITING', 'CURRENT'],
  NO_SHOW: ['WAITING', 'CURRENT'],
  /* يُسمح بإلغاء الحالي أيضًا (حالة إلغاء المريض من تطبيقه) */
  CANCELLED: ['WAITING', 'CURRENT'],
};

function setBookingStatus(id, status, ownerUid) {
  if (!ALLOWED_TRANSITIONS[status]) fail('حالة غير معروفة.');
  return tx(() => {
    const booking = get(`SELECT * FROM bookings WHERE id=?`, id);
    if (!booking) fail('الحجز غير موجود.');
    if (ownerUid !== undefined && booking.user_id !== ownerUid) fail('الحجز غير موجود.');
    if (!ALLOWED_TRANSITIONS[status].includes(booking.status)) {
      fail('لا يمكن تنفيذ هذه العملية على الحجز في وضعه الحالي.');
    }
    const ts = nowIso();
    const paid = status === 'COMPLETED' ? `, payment_status='PAID'` : '';
    const finished =
      status === 'COMPLETED' || status === 'NO_SHOW'
        ? `, finished_at='${ts}'`
        : '';
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

/* ─────────── الإعدادات ─────────── */

function getSettings() {
  const row = get(`SELECT v FROM meta WHERE k=?`, SETTINGS_KEY);
  if (!row) return DEFAULT_SETTINGS;
  try {
    return { ...DEFAULT_SETTINGS, ...JSON.parse(row.v) };
  } catch {
    return DEFAULT_SETTINGS;
  }
}

/* ─────────── خادم HTTP ─────────── */

const json = (res, code, payload) => {
  const body = JSON.stringify(payload);
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(body);
};

function readBody(req) {
  return new Promise((resolveBody, rejectBody) => {
    let size = 0;
    const chunks = [];
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > 1_000_000) {
        rejectBody(new Error('payload too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (chunks.length === 0) return resolveBody(undefined);
      try {
        resolveBody(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        rejectBody(new Error('invalid JSON'));
      }
    });
    req.on('error', rejectBody);
  });
}

/* ============================================================================
   النداء الصوتي العربي — توليد MP3 عبر خدمة Edge TTS
   (منطق منقول من مشروع node-edge-tts على GitHub — MIT — بصفر تبعيات)
   ========================================================================== */

const TRUSTED_CLIENT_TOKEN = '6A5AA1D4EAFF4E9FB37E23D68491D6F4';
const CHROMIUM_FULL_VERSION = '143.0.3650.75';
const WINDOWS_FILE_TIME_EPOCH = 11644473600n;

function generateSecMsGecToken() {
  const ticks = BigInt(Math.floor(Date.now() / 1000 + Number(WINDOWS_FILE_TIME_EPOCH))) * 10000000n;
  const rounded = ticks - (ticks % 3000000000n);
  return createHash('sha256').update(`${rounded}${TRUSTED_CLIENT_TOKEN}`, 'ascii').digest('hex').toUpperCase();
}

const DEFAULT_TTS_VOICE = 'ar-DZ-IsmaelNeural'; /* صوت عربي جزائري رجالي */

function escapeXml(unsafe) {
  return unsafe.replace(/[<>&"']/g, (c) =>
    c === '<' ? '&lt;' : c === '>' ? '&gt;' : c === '&' ? '&amp;' : c === '"' ? '&quot;' : '&apos;',
  );
}

/** توليد ملف صوتي لنص عربي وحفظه في المسار المحدد */
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
        Pragma: 'no-cache',
        'Cache-Control': 'no-cache',
        'User-Agent':
          `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ` +
          `(KHTML, like Gecko) Chrome/${CHROMIUM_FULL_VERSION.split('.')[0]}.0.0.0 Safari/537.36 Edg/${CHROMIUM_FULL_VERSION.split('.')[0]}.0.0.0`,
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
      },
    });

    const timer = setTimeout(() => {
      try { ws.close(); } catch { /* تجاهل */ }
      reject(new Error('TTS timeout'));
    }, 15000);

    const fail = (err) => { clearTimeout(timer); reject(err); };

    ws.on('error', fail);
    ws.on('open', () => {
      ws.send(
        `Content-Type:application/json; charset=utf-8\r\nPath:speech.config\r\n\r\n` +
        JSON.stringify({
          context: {
            synthesis: {
              audio: {
                metadataoptions: { sentenceBoundaryEnabled: 'false', wordBoundaryEnabled: 'true' },
                outputFormat: 'audio-24khz-48kbitrate-mono-mp3',
              },
            },
          },
        }),
      );
      const requestId = randomBytes(16).toString('hex');
      ws.send(
        `X-RequestId:${requestId}\r\nContent-Type:application/ssml+xml\r\nPath:ssml\r\n\r\n` +
        `<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xmlns:mstts="https://www.w3.org/2001/mstts" xml:lang="${lang}">` +
        `<voice name="${voice}"><prosody rate="default" pitch="default" volume="default">${escapeXml(text)}</prosody></voice></speak>`,
      );
    });

    let stream = null;
    let bytes = 0;
    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        const sep = 'Path:audio\r\n';
        const idx = data.indexOf(sep) + sep.length;
        const chunk = data.subarray(idx);
        bytes += chunk.length;
        if (!stream) stream = createWriteStream(audioPath);
        stream.write(chunk);
        return;
      }
      const message = data.toString();
      if (message.includes('Path:turn.end')) {
        clearTimeout(timer);
        const done = () => { try { ws.close(); } catch { /* تجاهل */ } resolve(bytes); };
        if (stream) stream.end(done);
        else done();
      }
    });
  });
}

const ttsInFlight = new Map(); /* منع التوليد المتزامن المكرر لنفس النص */

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
        .catch((err) => { try { unlinkSync(outFile); } catch { /* تجاهل */ } throw err; })
        .finally(() => ttsInFlight.delete(hash));
      ttsInFlight.set(hash, job);
      try { await job; }
      catch { return json(res, 502, { error: 'تعذر توليد الصوت حاليًا.' }); }
    }
  }

  res.writeHead(200, { 'Content-Type': 'audio/mpeg', 'Cache-Control': 'public, max-age=86400' });
  createReadStream(outFile).pipe(res);
}

async function handle(req, res, pathname, query) {
  const seg = pathname.split('/').filter(Boolean); // ['api', ...]

  /* ── النداء الصوتي ── */
  if (req.method === 'GET' && pathname === '/api/tts') return handleTts(req, res, query);

  /* ── حالة التسجيل الإلكتروني (تُستهلك في اللوحة والتطبيق) ── */
  if (req.method === 'GET' && pathname === '/api/online-status') {
    const clinicId = query.get('clinicId') ?? '';
    const date = query.get('date') ?? isoDate(1);
    const c = get(`SELECT * FROM clinics WHERE id=?`, clinicId);
    if (!c) return json(res, 404, { error: 'العيادة غير موجودة.' });
    const max = typeof c.max_online === 'number' ? c.max_online : 30;
    const used = get(
      `SELECT COUNT(*) AS n FROM bookings
       WHERE clinic_id=? AND date=? AND user_id NOT LIKE 'guest-%' AND status != 'CANCELLED'`,
      clinicId, date,
    )?.n ?? 0;
    return json(res, 200, {
      enabled: c.is_open === 1 && c.online_enabled === 1,
      used, max, remaining: max > 0 ? Math.max(0, max - used) : null,
    });
  }

  /* ── سجل مرضى الحجوزات الإلكترونية (حضور/دفع) ── */
  if (req.method === 'GET' && pathname === '/api/patients') {
    const date = query.get('date') ?? isoDate(0);
    const clinicId = query.get('clinicId');
    let sql =
      `${BOOKING_SELECT} WHERE b.date=? AND b.user_id NOT LIKE 'guest-%'`;
    const params = [date];
    if (clinicId) { sql += ` AND b.clinic_id=?`; params.push(clinicId); }
    sql += ` ORDER BY b.queue_number ASC`;
    return json(res, 200, all(sql, ...params).map(mapClinicBooking));
  }

  /* ── نظام ── */
  if (req.method === 'POST' && pathname === '/api/bootstrap') return json(res, 200, { ok: true });
  if (req.method === 'GET' && pathname === '/api/health') return json(res, 200, { ok: true, db: DB_PATH });

  /* ── العيادات (مقدمو الخدمة المشتركون) ── */
  if (seg[1] === 'clinics') {
    if (req.method === 'GET' && seg.length === 2) {
      return json(res, 200, all(`SELECT * FROM clinics ORDER BY rating DESC`).map(mapClinic));
    }
    if (req.method === 'GET' && seg.length === 3) {
      const row = get(`SELECT * FROM clinics WHERE id=?`, seg[2]);
      return json(res, 200, row ? mapClinic(row) : null);
    }
    if (req.method === 'PATCH' && seg.length === 3) {
      const body = await readBody(req);
      const row = get(`SELECT * FROM clinics WHERE id=?`, seg[2]);
      if (!row) return json(res, 404, { error: 'العيادة غير موجودة.' });

      const sets = [];
      const params = [];
      if (typeof body.isOpen === 'boolean') { sets.push('is_open=?'); params.push(body.isOpen ? 1 : 0); }
      if (typeof body.active === 'boolean') { sets.push('is_open=?'); params.push(body.active ? 1 : 0); }
      if (typeof body.onlineEnabled === 'boolean') {
        sets.push('online_enabled=?'); params.push(body.onlineEnabled ? 1 : 0);
      }
      if (body.maxOnline !== undefined) {
        const v = Math.max(0, Math.round(Number(body.maxOnline)));
        if (!Number.isFinite(v)) fail('قيمة عدد التسجيلات غير صحيحة.');
        sets.push('max_online=?'); params.push(v);
      }
      if (body.commission !== undefined) {
        const v = Math.max(0, Math.round(Number(body.commission)));
        if (!Number.isFinite(v)) fail('قيمة العمولة غير صحيحة.');
        sets.push('commission=?'); params.push(v);
      }
      for (const col of ['name', 'specialty', 'city', 'address', 'phone']) {
        if (typeof body[col] === 'string') { sets.push(`${col}=?`); params.push(body[col].trim()); }
      }
      if (sets.length > 0) {
        params.push(seg[2]);
        run(`UPDATE clinics SET ${sets.join(', ')} WHERE id=?`, ...params);
      }
      return json(res, 200, mapClinic(get(`SELECT * FROM clinics WHERE id=?`, seg[2])));
    }
    if (req.method === 'DELETE' && seg.length === 3) {
      const used = get(`SELECT COUNT(*) AS n FROM bookings WHERE clinic_id=?`, seg[2]);
      if ((used?.n ?? 0) > 0) {
        fail('لا يمكن حذف طبيب له حجوزات سابقة — أوقف نشاطه بدلًا من حذفه.');
      }
      const row = get(`SELECT id FROM clinics WHERE id=?`, seg[2]);
      if (!row) return json(res, 404, { error: 'الطبيب غير موجود.' });
      run(`DELETE FROM clinics WHERE id=?`, seg[2]);
      return json(res, 200, { ok: true });
    }
  }

  /* ── الإعدادات ── */
  if (pathname === '/api/settings') {
    if (req.method === 'GET') return json(res, 200, getSettings());
    if (req.method === 'PUT') {
      const body = await readBody(req);
      run(
        `INSERT INTO meta(k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`,
        SETTINGS_KEY, JSON.stringify({ ...getSettings(), ...body }),
      );
      return json(res, 200, { ok: true });
    }
  }

  /* ── الحجوزات ── */
  if (pathname === '/api/bookings' && req.method === 'POST') {
    const body = await readBody(req);
    return json(res, 200, createAppBooking(body));
  }

  if (pathname === '/api/walkin' && req.method === 'POST') {
    const body = await readBody(req);
    return json(res, 200, createWalkinTicket(body));
  }

  if (pathname === '/api/bookings' && req.method === 'GET') {
    const clauses = [];
    const params = [];
    if (query.get('date')) { clauses.push('b.date=?'); params.push(query.get('date')); }
    if (query.get('doctorId')) { clauses.push('b.clinic_id=?'); params.push(query.get('doctorId')); }
    if (query.get('phone')) { clauses.push('b.whatsapp=?'); params.push(query.get('phone')); }
    if (query.get('userId')) { clauses.push('b.user_id=?'); params.push(query.get('userId')); }
    const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
    const order = query.get('userId') || query.get('phone')
      ? 'ORDER BY b.created_at DESC'
      : 'ORDER BY b.queue_number ASC';
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
    return json(res, 200, callNextPatient(body?.doctorId, body?.date));
  }

  /* ── عروض لوحة العيادة ── */
  if (pathname === '/api/dashboard' && req.method === 'GET') {
    const date = query.get('date');
    if (!isDateStr(date)) fail('التاريخ غير صحيح.');
    const [settings, doctors, bookings] = [
      getSettings(),
      all(`SELECT * FROM clinics ORDER BY name`).map(mapDoctor),
      all(`${BOOKING_SELECT} WHERE b.date=? ORDER BY b.queue_number`, date).map(mapClinicBooking),
    ];
    return json(res, 200, { settings, doctors, bookings });
  }

  if (seg[1] === 'ticket' && req.method === 'GET' && seg.length === 3) {
    const raw = get(`${BOOKING_SELECT} WHERE b.id=?`, seg[2]);
    if (!raw) return json(res, 200, null);
    const siblings = all(
      `${BOOKING_SELECT} WHERE b.clinic_id=? AND b.date=? ORDER BY b.queue_number`,
      raw.clinic_id, raw.date,
    );
    const aheadCount = siblings.filter(
      (b) => b.status === 'WAITING' && b.queue_number < raw.queue_number,
    ).length;
    const current = siblings.find((b) => b.status === 'CURRENT');
    return json(res, 200, {
      booking: query.get('view') === 'clinic' ? mapClinicBooking(raw) : mapBooking(raw),
      aheadCount,
      currentNumber: current?.queue_number ?? null,
      settings: getSettings(),
    });
  }

  if (pathname === '/api/stats/week' && req.method === 'GET') {
    const result = [];
    for (let i = 6; i >= 0; i -= 1) {
      const date = isoDate(-i);
      const rows = all(`SELECT status, fee FROM bookings WHERE date=?`, date);
      const completed = rows.filter((r) => r.status === 'COMPLETED');
      result.push({
        date,
        total: rows.length,
        completed: completed.length,
        revenue: completed.reduce((sum, r) => sum + r.fee, 0),
      });
    }
    return json(res, 200, result);
  }

  if (pathname === '/api/admin/reset' && req.method === 'POST') {
    tx(() => {
      run(`DELETE FROM bookings`);
      run(`DELETE FROM counters`);
      run(`DELETE FROM meta`);
      run(`DELETE FROM auth_credentials`);
      run(`DELETE FROM users`);
    });
    return json(res, 200, { ok: true });
  }

  /* ── المستخدمون ── */
  if (pathname === '/api/users/google' && req.method === 'POST') {
    const p = await readBody(req);
    if (!p?.sub) fail('معرّف Google مفقود.');
    const uid = `gg-${p.sub}`;
    const existing = get(`SELECT * FROM users WHERE uid=?`, uid);
    if (!existing) {
      run(
        `INSERT INTO users (uid,name,email,phone,provider,photo_url,address,whatsapp,created_at)
         VALUES (?,?,?,NULL,'google',?,'','',?)`,
        uid, (p.name ?? '').trim() || 'مستخدم Google', (p.email ?? '').trim(),
        p.picture ?? null, nowIso(),
      );
    } else if ((p.name ?? '').trim() && p.name.trim() !== existing.name) {
      run(`UPDATE users SET name=?, photo_url=COALESCE(?, photo_url) WHERE uid=?`,
        p.name.trim(), p.picture ?? null, uid);
    }
    return json(res, 200, mapUser(get(`SELECT * FROM users WHERE uid=?`, uid)));
  }

  if (seg[1] === 'users' && seg.length === 3) {
    const uid = seg[2];
    if (req.method === 'GET') {
      const row = get(`SELECT * FROM users WHERE uid=?`, uid);
      return json(res, 200, row ? mapUser(row) : null);
    }
    if (req.method === 'PATCH') {
      const base = get(`SELECT * FROM users WHERE uid=?`, uid);
      if (!base) return json(res, 404, { error: 'الحساب غير موجود.' });
      const patch = (await readBody(req)) ?? {};
      const merge = (a, b, fallback) => patch[b] !== undefined ? patch[b] : (a ?? fallback);
      run(
        `UPDATE users SET name=?, email=?, address=?, whatsapp=?, lat=?, lng=?, photo_url=? WHERE uid=?`,
        merge(base.name, 'name', ''), merge(base.email, 'email', ''),
        merge(base.address, 'address', ''), merge(base.whatsapp, 'whatsapp', ''),
        patch.location?.lat ?? base.lat, patch.location?.lng ?? base.lng,
        merge(base.photo_url, 'photoURL', null), uid,
      );
      return json(res, 200, mapUser(get(`SELECT * FROM users WHERE uid=?`, uid)));
    }
  }

  /* المسارات غير-API تُترك لخدمة الملفات الثابتة (SPA) */
  if (!pathname.startsWith('/api')) return undefined;
  return json(res, 404, { error: `مسار غير معروف: ${req.method} ${pathname}` });
}

/* ── خدمة واجهة اللوحة المبنية (dist) من نفس السيرفر — للنشر بدون Node modules ── */

/* خدمة واجهة اللوحة: dist ملاصق للسيرفر (الاستضافة) أو مشروع clinic-app (التطوير) */
const LOCAL_DIST = path.join(__dirname, 'dist');
const STATIC_DIST =
  process.env.STATIC_DIR ??
  (existsSync(LOCAL_DIST) ? LOCAL_DIST : path.join(__dirname, '..', 'clinic-app', 'dist'));
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.ico': 'image/x-icon',
  '.json': 'application/json; charset=utf-8',
  '.woff2': 'font/woff2',
  '.webmanifest': 'application/manifest+json',
};

function serveStatic(res, filePath) {
  const ext = path.extname(filePath).toLowerCase();
  res.writeHead(200, { 'Content-Type': MIME[ext] ?? 'application/octet-stream' });
  createReadStream(filePath).pipe(res);
}

function handleStatic(req, res, pathname) {
  if (req.method !== 'GET' || !existsSync(STATIC_DIST)) return false;
  const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const target = path.normalize(path.join(STATIC_DIST, rel));
  if (target.startsWith(STATIC_DIST) && existsSync(target) && !target.includes('..')) {
    serveStatic(res, target);
    return true;
  }
  /* SPA fallback */
  const index = path.join(STATIC_DIST, 'index.html');
  if (existsSync(index)) {
    serveStatic(res, index);
    return true;
  }
  return false;
}

const server = createServer((req, res) => {
  const origin = req.headers.origin;
  if (origin) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.setHeader('Access-Control-Max-Age', '600');
  }
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://localhost:${PORT}`);
  handle(req, res, url.pathname, url.searchParams).catch((err) => {
    if (err instanceof DomainError) return json(res, 409, { error: err.message });
    const msg = err instanceof Error ? err.message : String(err);
    console.error('[dawri-server]', msg);
    return json(res, 500, { error: 'خطأ داخلي في السيرفر.' });
  }).catch(() => { /* تجاهل */ }).then((served) => {
    if (!served && !res.writableEnded) handleStatic(req, res, url.pathname);
  });
});

server.listen(PORT, () => {
  console.log(`[dawri-server] API جاهز على http://localhost:${PORT} — قاعدة البيانات: ${DB_PATH}`);
});
