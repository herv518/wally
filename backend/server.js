/*
Run:
  XAI_API_KEY=your_key_here node backend/server.js

Optional:
  PORT=8080 node backend/server.js
  (or create backend/.env with XAI_API_KEY=your_key_here)
  Optional model pinning:
  XAI_MODEL=grok-3-mini PORT=8080 node backend/server.js

Purpose:
  - Serves frontend/index.html (fallback: frontend/widget.html) on http://localhost:8080
  - Provides POST /api/xai/turn for stable xAI voice turns
*/

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { spawn } = require('child_process');
const { WebSocket, WebSocketServer } = require('ws');
let ffmpegStatic = null;
try {
  ffmpegStatic = require('ffmpeg-static');
} catch (_) {}

function loadEnvFile(filePath) {
  let raw = '';
  try {
    raw = fs.readFileSync(filePath, 'utf8');
  } catch (_) {
    return;
  }
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const idx = trimmed.indexOf('=');
    if (idx <= 0) continue;
    const key = trimmed.slice(0, idx).trim();
    const value = trimmed.slice(idx + 1).trim();
    if (key && process.env[key] == null) process.env[key] = value;
  }
}
loadEnvFile(path.join(__dirname, '.env'));

const ROOT = path.resolve(__dirname, '..');
const FRONTEND_ROOT = path.join(ROOT, 'frontend');
const MD_ROOT = path.join(ROOT, 'wally', 'md');
const INVENTORY_FILE = process.env.WALLY_INVENTORY_FILE || path.resolve(ROOT, '..', 'public', 'data', 'wally_inventory.json');
const PORT = Number(process.env.PORT || 8080);
const HOST = String(process.env.HOST || '0.0.0.0').trim() || '0.0.0.0';
const MAX_JSON_BYTES = 30 * 1024 * 1024;
const FFMPEG_BIN = process.env.FFMPEG_BIN || ffmpegStatic || '/opt/homebrew/bin/ffmpeg';
const WALLY_SMOKE_MODE = String(process.env.WALLY_SMOKE_MODE || '').trim() === '1';
const WALLY_SMOKE_DELAY_MS = Math.max(0, Number(process.env.WALLY_SMOKE_DELAY_MS || 0));
const ALLOWED_ORIGINS = String(process.env.ALLOWED_ORIGINS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const WALLY_UNKNOWN_REPLY = 'Wally kennt dieses Auto nicht. Piep! Frag mich zu den Fahrzeugen in unserer Liste!';
const WALLY_NO_AUDIO_REPLY = 'Ich habe gerade kein nutzbares Audiosignal vom Mikrofon bekommen. Bitte Mikrofon-Freigabe pruefen und dann nochmal klar sprechen.';
const WALLY_ROBOT_SYSTEM = [
  'Du bist Wally, digitaler Fahrzeugberater von Rolf Automobile GmbH.',
  'Du antwortest nur auf Deutsch, klar und natuerlich in 1 bis 3 kurzen Saetzen.',
  'Starte direkt mit der Antwort auf die letzte Frage. Keine Vorstellung deiner Rolle.',
  'Wiederhole nie die gleiche Antwort zweimal hintereinander.',
  'Sprich fluessig wie in einem normalen Gespraech, nicht steif oder formularhaft.',
  'Sprich normal: keine Selbstdarstellung, kein Rollen-Text, kein JSON.',
  'Nutze niemals ein Schluessel-Wert-Format wie "ID=...", "Modell=..." oder "==".',
  'Sprich Nutzer neutral an und nutze keinen festen Personennamen, ausser der Name wurde im Chat explizit genannt.',
  'Wenn der Nutzer nach einem Modell fragt (z.B. "Habt ihr einen Golf?"), suche in der Fahrzeugliste und antworte klar mit "Ja, gefunden" oder "Nein, aktuell nicht".',
  'Wenn der Nutzer nach einer Automarke fragt, antworte genauso klar mit "Ja, aktuell..." oder "Nein, aktuell nicht" und nenne bei Treffer kurz 1 bis 2 Beispiele.',
  'Wenn ein aktuelles Fahrzeug markiert ist, nutze dieses zuerst fuer Detailfragen.',
  'Nenne niemals konkrete Preise oder Euro-Betraege. Wenn nach Preis gefragt wird, sage kurz, dass du hier keine Preise nennst.',
  'Priorisiere Ausstattung, Extras und Zustand statt Preisangaben.',
  'Nenne ID, Baujahr, Kilometer und PS nur dann vollstaendig, wenn es zur Frage passt oder explizit verlangt ist.',
  'Wenn ein Feld fehlt, nutze "unbekannt".',
  `Wenn die Frage nicht zu diesen Fahrzeugen passt oder das Auto nicht existiert, antworte exakt: "${WALLY_UNKNOWN_REPLY}"`,
  'Sage niemals "ich bin eine KI", "als KI" oder "ich bin Grok".'
].join('\n');
const MAX_MD_CONTEXT_CHARS = 12000;
const MAX_HISTORY_TURNS = 8;
const MAX_CATALOG_CONTEXT_CHARS = 7000;
const MAX_VEHICLE_PROFILES = 200;
const MAX_VEHICLE_TEXT_CHARS = 900;
const MAX_EQUIPMENT_TEXT_CHARS = 1400;
const XAI_STREAM_AUDIO = String(process.env.XAI_STREAM_AUDIO || '1').trim() !== '0';
const XAI_MODEL = String(process.env.XAI_MODEL || '').trim();
const KEEPALIVE_MIN_MS = 5 * 60 * 1000;
const KEEPALIVE_MAX_MS = 10 * 60 * 1000;
const KEEPALIVE_INTERVAL_MS = Math.max(
  KEEPALIVE_MIN_MS,
  Math.min(KEEPALIVE_MAX_MS, Number(process.env.WALLY_KEEPALIVE_INTERVAL_MS || (7 * 60 * 1000)))
);
const KEEPALIVE_ENABLED = String(process.env.WALLY_KEEPALIVE_ENABLED || '1').trim() !== '0';
const KEEPALIVE_URL = String(process.env.WALLY_KEEPALIVE_URL || '').trim();
const DIAG_MAX_ENTRIES = Math.max(20, Math.min(500, Number(process.env.WALLY_DIAG_MAX_ENTRIES || 120)));
const XAI_TURN_TIMEOUT_MS = Math.max(9000, Math.min(30000, Number(process.env.WALLY_XAI_TURN_TIMEOUT_MS || 16000)));
const WALLY_FAST_TRANSCRIPT_MODE = String(process.env.WALLY_FAST_TRANSCRIPT_MODE || '0').trim() === '1';
const REALTIME_SESSION_IDLE_MS = Math.max(15000, Math.min(10 * 60 * 1000, Number(process.env.WALLY_REALTIME_IDLE_MS || 2 * 60 * 1000)));
const MAX_TURN_AUDIO_BASE64_CHARS = Math.max(120000, Math.min(8 * 1024 * 1024, Number(process.env.WALLY_MAX_TURN_AUDIO_BASE64_CHARS || 2 * 1024 * 1024)));
const MAX_TURN_PCM_BASE64_CHARS = Math.max(120000, Math.min(24 * 1024 * 1024, Number(process.env.WALLY_MAX_TURN_PCM_BASE64_CHARS || 6 * 1024 * 1024)));
const MAX_REALTIME_CHUNK_BASE64_CHARS = Math.max(12000, Math.min(2 * 1024 * 1024, Number(process.env.WALLY_MAX_REALTIME_CHUNK_BASE64_CHARS || 512 * 1024)));
let inventoryCacheMtime = 0;
let inventoryCacheProfiles = [];
const turnDiagnostics = [];
const realtimeSessions = new Map();

const BRAND_ALIAS_LOOKUP = {
  vw: 'Volkswagen',
  volkswagen: 'Volkswagen',
  audi: 'Audi',
  bmw: 'BMW',
  seat: 'Seat',
  cupra: 'Cupra',
  skoda: 'Skoda',
  mazda: 'Mazda',
  fiat: 'Fiat',
  opel: 'Opel',
  ford: 'Ford',
  renault: 'Renault',
  peugeot: 'Peugeot',
  citroen: 'Citroen',
  mercedes: 'Mercedes-Benz',
  mercedesbenz: 'Mercedes-Benz',
  mb: 'Mercedes-Benz',
  hyundai: 'Hyundai',
  kia: 'Kia',
  toyota: 'Toyota',
  nissan: 'Nissan',
  dacia: 'Dacia',
  honda: 'Honda',
  volvo: 'Volvo',
  porsche: 'Porsche',
  mini: 'MINI',
  landrover: 'Land Rover',
  alfaromeo: 'Alfa Romeo'
};

function isOriginAllowed(origin) {
  if (!origin) return true;
  if (!ALLOWED_ORIGINS.length) return true;
  return ALLOWED_ORIGINS.includes(origin);
}

function corsHeaders(origin) {
  if (!origin) return {};
  if (!ALLOWED_ORIGINS.length) {
    return { 'Access-Control-Allow-Origin': '*', 'Vary': 'Origin' };
  }
  if (!ALLOWED_ORIGINS.includes(origin)) return {};
  return { 'Access-Control-Allow-Origin': origin, 'Vary': 'Origin' };
}

function sendJson(res, status, payload, origin = '') {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    ...corsHeaders(origin),
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body)
  });
  res.end(body);
}

function listMdFiles(dir) {
  let entries = [];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch (_) {
    return [];
  }
  const out = [];
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...listMdFiles(full));
      continue;
    }
    if (!entry.isFile()) continue;
    if (!entry.name.toLowerCase().endsWith('.md')) continue;
    out.push(full);
  }
  return out;
}

function loadMdContext() {
  const files = listMdFiles(MD_ROOT).sort();
  if (!files.length) return '';
  const sections = [];
  for (const filePath of files) {
    let raw = '';
    try {
      raw = fs.readFileSync(filePath, 'utf8');
    } catch (_) {
      continue;
    }
    const rel = path.relative(MD_ROOT, filePath);
    const text = raw.trim();
    if (!text) continue;
    sections.push(`# ${rel}\n${text}`);
  }
  return sections.join('\n\n---\n\n').slice(0, MAX_MD_CONTEXT_CHARS);
}

const STATIC_MD_CONTEXT = loadMdContext();

function readJson(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_JSON_BYTES) {
        reject(new Error('Payload too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      try {
        const raw = Buffer.concat(chunks).toString('utf8');
        resolve(raw ? JSON.parse(raw) : {});
      } catch (err) {
        reject(new Error('Invalid JSON'));
      }
    });
    req.on('error', reject);
  });
}

function contentTypeFor(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.html') return 'text/html; charset=utf-8';
  if (ext === '.css') return 'text/css; charset=utf-8';
  if (ext === '.js') return 'application/javascript; charset=utf-8';
  if (ext === '.json') return 'application/json; charset=utf-8';
  if (ext === '.png') return 'image/png';
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.svg') return 'image/svg+xml';
  if (ext === '.ico') return 'image/x-icon';
  return 'application/octet-stream';
}

function normalizeContent(value) {
  if (typeof value === 'string') return value.trim();
  if (Array.isArray(value)) {
    return value
      .map((part) => {
        if (typeof part === 'string') return part.trim();
        if (part && typeof part.text === 'string') return part.text.trim();
        return '';
      })
      .filter(Boolean)
      .join(' ')
      .trim();
  }
  if (value && typeof value.text === 'string') return value.text.trim();
  return '';
}

function oneLine(value, maxLen = 160) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, maxLen);
}

function normalizeField(value, fallback = 'unbekannt') {
  const v = oneLine(value, 120);
  return v || fallback;
}

function normalizeFreeTextKey(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function isVehicleNarrativeNoiseLine(value) {
  const key = normalizeFreeTextKey(value);
  if (!key) return true;
  if (/^(id|url|title|status)\b/.test(key)) return true;
  if (key === 'pragestelle' || key === 'praegestelle') return true;
  if (key === 'aufbereitung') return true;
  if (key === 'aktuelles') return true;
  if (key === 'bewertung') return true;
  if (key === 'anfahrt') return true;
  if (key === 'fahrzeugsuche') return true;
  if (key === 'privatsphare einstellungen' || key === 'privatsphaere einstellungen') return true;
  if (key.startsWith('es besteht die moglichkeit')) return true;
  if (key.startsWith('gerne nehmen wir')) return true;
  if (key.startsWith('ihre ansprechpartner')) return true;
  if (key.startsWith('wir sprechen')) return true;
  return false;
}

function sanitizeVehicleNarrative(value, maxItems = 42) {
  const raw = String(value || '');
  if (!raw) return '';
  const lines = raw.split(/\r?\n/);
  const out = [];
  const seen = new Set();
  for (const lineRaw of lines) {
    const line = String(lineRaw || '').replace(/^\s*[-*•]+\s*/, '').trim();
    if (!line) continue;
    if (isVehicleNarrativeNoiseLine(line)) continue;
    const key = normalizeFreeTextKey(line);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(line);
    if (out.length >= maxItems) break;
  }
  if (!out.length) return '';
  return out.join('; ');
}

function normalizeLongField(value, maxLen = 900) {
  const cleaned = sanitizeVehicleNarrative(value);
  const src = cleaned || String(value || '');
  return src
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, Math.max(0, maxLen));
}

function normalizeBrandKey(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '');
}

function canonicalBrand(value) {
  const key = normalizeBrandKey(value);
  return key ? (BRAND_ALIAS_LOOKUP[key] || '') : '';
}

function splitWords(value) {
  return String(value || '')
    .split(/[^A-Za-z0-9]+/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function fallbackBrandLabel(token) {
  const raw = oneLine(token, 60);
  if (!raw) return '';
  if (raw.toUpperCase() === raw && raw.length <= 5) return raw;
  return raw[0].toUpperCase() + raw.slice(1);
}

function extractBrandFromModel(model, fallbackTitle = '') {
  for (const source of [model, fallbackTitle]) {
    const words = splitWords(source);
    if (!words.length) continue;
    if (words.length >= 2) {
      const two = canonicalBrand(`${words[0]} ${words[1]}`);
      if (two) return two;
    }
    const one = canonicalBrand(words[0]);
    if (one) return one;
  }
  for (const source of [model, fallbackTitle]) {
    const words = splitWords(source);
    if (!words.length) continue;
    const probe = normalizeBrandKey(words[0]);
    if (!probe || probe === 'unbekannt' || probe === 'unknown' || probe === 'na') continue;
    return fallbackBrandLabel(words[0]);
  }
  return '';
}

function normalizeBrandField(value, model, title) {
  const raw = oneLine(value, 80);
  const known = canonicalBrand(raw);
  if (known) return known;
  if (raw) return raw;
  const extracted = extractBrandFromModel(model, title);
  return extracted || 'unbekannt';
}

function normalizeVehicleProfile(profile) {
  const model = normalizeField(profile?.model);
  const title = normalizeField(profile?.title || profile?.name || profile?.vehicle || profile?.label, '');
  const vehicleText = normalizeLongField(
    profile?.vehicleText || profile?.vehicle_text || profile?.voice_text || profile?.description || profile?.text,
    MAX_VEHICLE_TEXT_CHARS
  );
  const equipmentText = normalizeLongField(
    profile?.equipmentText || profile?.equipment_text,
    MAX_EQUIPMENT_TEXT_CHARS
  );
  return {
    id: normalizeField(profile?.id),
    brand: normalizeBrandField(profile?.brand || profile?.make || profile?.manufacturer || profile?.marke, model, title),
    model,
    title,
    price: normalizeField(profile?.price),
    year: normalizeField(profile?.year),
    km: normalizeField(profile?.km),
    ps: normalizeField(profile?.ps),
    fuel: normalizeField(profile?.fuel, ''),
    link: normalizeField(profile?.link || profile?.url || profile?.detailUrl, ''),
    vehicleText,
    equipmentText
  };
}

function normalizeVehicleId(value) {
  return oneLine(value, 120).toLowerCase().replace(/\s+/g, '');
}

function toArray(value) {
  if (Array.isArray(value)) return value;
  if (value && Array.isArray(value.items)) return value.items;
  if (value && Array.isArray(value.data)) return value.data;
  if (value && typeof value === 'object') return [value];
  return [];
}

function loadInventoryProfiles() {
  try {
    const stat = fs.statSync(INVENTORY_FILE);
    if (inventoryCacheProfiles.length > 0 && stat.mtimeMs === inventoryCacheMtime) {
      return inventoryCacheProfiles;
    }
    const raw = JSON.parse(fs.readFileSync(INVENTORY_FILE, 'utf8'));
    const profiles = toArray(raw)
      .slice(0, MAX_VEHICLE_PROFILES * 3)
      .map(normalizeVehicleProfile)
      .filter((p) => p.id !== 'unbekannt' || p.model !== 'unbekannt');
    inventoryCacheMtime = stat.mtimeMs;
    inventoryCacheProfiles = profiles;
    return profiles;
  } catch (_) {
    return [];
  }
}

function mergeVehicleProfiles(primary, fallback) {
  const merged = [];
  const seen = new Set();

  const addProfile = (profile) => {
    const id = normalizeField(profile?.id);
    const model = normalizeField(profile?.model);
    if (id === 'unbekannt' && model === 'unbekannt') return;
    const key = `${id}|${model}`;
    if (seen.has(key)) return;
    seen.add(key);
    merged.push(profile);
  };

  for (const item of primary || []) addProfile(item);
  for (const item of fallback || []) addProfile(item);
  return merged;
}

function buildVehicleProfilesContext(profiles) {
  if (!Array.isArray(profiles) || !profiles.length) return '';
  return profiles
    .slice(0, MAX_VEHICLE_PROFILES)
    .map((p, idx) => {
      const head = `Fahrzeug ${idx + 1}: ID ${p.id}; Marke ${p.brand}; Modell ${p.model}; Baujahr ${p.year}; Kilometer ${p.km}; PS ${p.ps}`;
      const tail = [p.fuel ? `Kraftstoff ${p.fuel}` : '', p.link ? `Link ${p.link}` : ''].filter(Boolean).join('; ');
      return tail ? `${head} | ${tail}` : head;
    })
    .join('\n');
}

function findVehicleById(profiles, vehicleId) {
  if (!Array.isArray(profiles) || !profiles.length) return null;
  const needle = normalizeVehicleId(vehicleId);
  if (!needle) return null;
  const exact = profiles.find((p) => normalizeVehicleId(p?.id) === needle);
  if (exact) return exact;
  return profiles.find((p) => {
    const id = normalizeVehicleId(p?.id);
    if (!id) return false;
    return id.includes(needle) || needle.includes(id);
  }) || null;
}

function buildFocusedVehicleContext(vehicle) {
  if (!vehicle) return '';
  const head = `Aktuell geoeffnetes Fahrzeug: ID ${vehicle.id}, Marke ${vehicle.brand}, Modell ${vehicle.model}, Baujahr ${vehicle.year}, Kilometer ${vehicle.km}, PS ${vehicle.ps}.`;
  const tail = [vehicle.fuel ? `Kraftstoff ${vehicle.fuel}.` : '', vehicle.link ? `Link ${vehicle.link}.` : '']
    .filter(Boolean)
    .join(' ');
  const details = [
    vehicle.vehicleText ? `Kurzbeschreibung: ${vehicle.vehicleText}` : '',
    vehicle.equipmentText ? `Ausstattung/Fakten: ${vehicle.equipmentText}` : ''
  ]
    .filter(Boolean)
    .join(' ');
  return [head, tail, details].filter(Boolean).join(' ');
}

function buildVehicleSystemPrompt(vehicleProfilesContext, catalogContext, focusedVehicleContext, options = {}) {
  const singleVehicleMode = !!options.singleVehicleMode;
  const focusedVehicle = options.focusedVehicle || null;
  const currentVehicleId = oneLine(options.currentVehicleId, 120);
  const chunks = [WALLY_ROBOT_SYSTEM];
  if (singleVehicleMode && focusedVehicle) {
    chunks.push(
      `QR-Einzelfahrzeug-Modus aktiv: Berate ausschliesslich zum Fahrzeug ID ${focusedVehicle.id} (${focusedVehicle.model}). Wenn der Nutzer nach anderen Autos oder Marken fragt, sage klar, dass dieser QR-Code nur dieses Fahrzeug zeigt.`
    );
  } else if (singleVehicleMode && currentVehicleId) {
    chunks.push(
      `QR-Einzelfahrzeug-Modus aktiv: Berate ausschliesslich zum Fahrzeug mit ID ${currentVehicleId}. Wenn der Nutzer nach anderen Autos oder Marken fragt, sage klar, dass dieser QR-Code nur dieses Fahrzeug zeigt.`
    );
  }
  if (focusedVehicleContext) {
    chunks.push(focusedVehicleContext);
  }
  if (vehicleProfilesContext) {
    chunks.push(`Aktuelle Fahrzeugliste:\n${vehicleProfilesContext}`);
  } else if (catalogContext) {
    chunks.push(`Aktuelle Fahrzeugliste (Rohkontext):\n${catalogContext}`);
  }
  return chunks.filter(Boolean).join('\n\n');
}

function buildTurnContext(body = {}) {
  const catalogContext = typeof body.catalogContext === 'string'
    ? body.catalogContext.slice(0, MAX_CATALOG_CONTEXT_CHARS)
    : '';
  const currentVehicleId = typeof body.currentVehicleId === 'string' ? body.currentVehicleId.slice(0, 120) : '';
  const singleVehicleMode = !!body.singleVehicleMode || !!currentVehicleId;
  const currentVehiclePayload = body.currentVehicle && typeof body.currentVehicle === 'object'
    ? normalizeVehicleProfile(body.currentVehicle)
    : null;
  const hasCurrentVehiclePayload = !!currentVehiclePayload
    && (currentVehiclePayload.id !== 'unbekannt' || currentVehiclePayload.model !== 'unbekannt');
  const rawProfiles = Array.isArray(body.vehicleProfiles) ? body.vehicleProfiles : [];
  const payloadProfiles = rawProfiles
    .map(normalizeVehicleProfile)
    .filter((p) => p.id !== 'unbekannt' || p.model !== 'unbekannt');
  if (hasCurrentVehiclePayload) payloadProfiles.unshift(currentVehiclePayload);
  const fileProfiles = loadInventoryProfiles();
  const mergedVehicleProfiles = mergeVehicleProfiles(payloadProfiles, fileProfiles).slice(0, MAX_VEHICLE_PROFILES);
  const fileFocusedVehicle = findVehicleById(mergedVehicleProfiles, currentVehicleId);
  const inferredFocusedVehicle = singleVehicleMode && currentVehicleId
    ? normalizeVehicleProfile({
      id: currentVehicleId,
      model: `Fahrzeug ${currentVehicleId}`
    })
    : null;
  const focusedVehicle = hasCurrentVehiclePayload
    ? currentVehiclePayload
    : (fileFocusedVehicle || inferredFocusedVehicle);
  const vehicleProfiles = singleVehicleMode && focusedVehicle
    ? [focusedVehicle]
    : mergedVehicleProfiles;
  const vehicleProfilesContext = buildVehicleProfilesContext(vehicleProfiles);
  const focusedVehicleContext = buildFocusedVehicleContext(focusedVehicle);
  const mergedInstructions = buildVehicleSystemPrompt(
    vehicleProfilesContext,
    catalogContext,
    focusedVehicleContext,
    { singleVehicleMode, focusedVehicle, currentVehicleId }
  );
  const historyTurns = Array.isArray(body.history) ? body.history : [];
  return {
    catalogContext,
    currentVehicleId,
    singleVehicleMode,
    currentVehiclePayload,
    focusedVehicle,
    vehicleProfiles,
    mergedInstructions,
    historyTurns
  };
}

function buildSessionInstructions(baseInstructions, history) {
  const base = (baseInstructions || WALLY_ROBOT_SYSTEM).trim();
  const turns = Array.isArray(history)
    ? history
        .slice(-MAX_HISTORY_TURNS)
        .map((m) => {
          const role = m?.role === 'assistant' ? 'ASSISTANT' : (m?.role === 'user' ? 'USER' : '');
          const text = normalizeContent(m?.content);
          if (!role || !text) return '';
          return `${role}: ${text.slice(0, 260)}`;
        })
        .filter(Boolean)
    : [];
  const mdSection = STATIC_MD_CONTEXT
    ? `Interner Team-Kontext aus Markdown:\n${STATIC_MD_CONTEXT}`
    : '';
  if (!turns.length) {
    return [base, mdSection].filter(Boolean).join('\n\n');
  }
  return [base, mdSection, `Bisheriger Verlauf:\n${turns.join('\n')}`].filter(Boolean).join('\n\n');
}

function extractTextFromResponseDone(msg) {
  if (typeof msg?.response?.output_text === 'string' && msg.response.output_text.trim()) {
    return msg.response.output_text.trim();
  }
  const output = msg?.response?.output;
  if (!Array.isArray(output)) return '';
  const parts = [];
  for (const item of output) {
    if (Array.isArray(item?.content)) {
      for (const c of item.content) {
        if (typeof c?.text === 'string' && c.text.trim()) parts.push(c.text.trim());
        else if (typeof c?.transcript === 'string' && c.transcript.trim()) parts.push(c.transcript.trim());
      }
    }
  }
  return parts.join(' ').trim();
}

function cleanRealtimeText(value) {
  return String(value || '')
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeBase64Payload(value, maxChars = 0) {
  if (typeof value !== 'string') return '';
  const compact = value.replace(/\s+/g, '').trim();
  if (!compact) return '';
  if (maxChars > 0 && compact.length > maxChars) return '';
  if (!/^[A-Za-z0-9+/=]+$/.test(compact)) return '';
  if (compact.length % 4 === 1) return '';
  return compact;
}

const MODEL_QUERY_STOP_WORDS = new Set([
  'habt', 'hast', 'gibt', 'gib', 'suche', 'suchst', 'finden', 'finde', 'zeige', 'zeigen',
  'auto', 'autos', 'wagen', 'fahrzeug', 'fahrzeuge',
  'marke', 'marken', 'automarke', 'automarken',
  'preis', 'kosten', 'kostet', 'kilometer', 'km', 'ps', 'leistung', 'baujahr', 'jahr',
  'ich', 'du', 'ihr', 'wir', 'uns', 'mir', 'mich',
  'der', 'die', 'das', 'dem', 'den', 'des',
  'ein', 'eine', 'einen', 'einem', 'einer',
  'und', 'oder', 'mit', 'ohne', 'fuer', 'für', 'bitte', 'mal', 'aktuell', 'hier'
]);

function sanitizeAssistantText(value) {
  let text = cleanRealtimeText(value)
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/`+/g, ' ')
    .replace(/\b([A-Za-zÄÖÜäöüß]+)\s*=\s*/g, '$1 ')
    .replace(/={1,}/g, ' ')
    .replace(/[{}[\]<>]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (/\b(preis|euro|€)\b/i.test(text)) {
    text = text
      .replace(/[0-9]{1,3}(?:[.\s][0-9]{3})*(?:,\d{2})?\s*(?:€|euro)\b/gi, 'Preis auf Anfrage')
      .replace(/\b(kostet|preis[:\s])/gi, 'Preis');
  }
  if (text.length > 360) {
    text = text.slice(0, 360).replace(/\s+\S*$/, '').trim();
  }
  return text;
}

function isLikelyTechnicalNoise(value) {
  const text = String(value || '').trim();
  if (!text) return true;
  const letters = (text.match(/[A-Za-zÄÖÜäöüß]/g) || []).length;
  const symbols = (text.match(/[=<>[\]{}|`]/g) || []).length;
  if (!letters) return true;
  if (symbols > letters * 0.25) return true;
  if (/^[-=+_*|`~:;,.0-9\s]+$/.test(text)) return true;
  return false;
}

function queryTokens(question) {
  return String(question || '')
    .toLowerCase()
    .replace(/[^a-z0-9äöüß]+/g, ' ')
    .split(/\s+/)
    .map((w) => w.trim())
    .filter((w) => w.length >= 3 && !MODEL_QUERY_STOP_WORDS.has(w));
}

function questionWords(question) {
  return String(question || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .split(/\s+/)
    .map((w) => w.trim())
    .filter(Boolean);
}

function normalizeTextWithSpaces(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function vehicleBrand(vehicle) {
  if (!vehicle || typeof vehicle !== 'object') return 'unbekannt';
  const raw = oneLine(vehicle.brand, 80);
  if (raw && raw !== 'unbekannt') return normalizeBrandField(raw, vehicle.model, vehicle.title);
  return normalizeBrandField('', vehicle.model, vehicle.title);
}

function uniqueBrandsFromProfiles(profiles) {
  if (!Array.isArray(profiles) || !profiles.length) return [];
  const byKey = new Map();
  for (const profile of profiles) {
    const brand = vehicleBrand(profile);
    const key = normalizeBrandKey(brand);
    if (!key || key === 'unbekannt' || key === 'unknown' || key === 'na') continue;
    if (!byKey.has(key)) byKey.set(key, brand);
  }
  return Array.from(byKey.values()).sort((a, b) => a.localeCompare(b, 'de', { sensitivity: 'base' }));
}

function extractRequestedBrands(question, profiles) {
  const words = questionWords(question);
  if (!words.length) return [];

  const requested = new Map();
  for (let i = 0; i < words.length; i += 1) {
    const one = canonicalBrand(words[i]);
    if (one) requested.set(normalizeBrandKey(one), one);
    if (i + 1 < words.length) {
      const two = canonicalBrand(`${words[i]} ${words[i + 1]}`);
      if (two) requested.set(normalizeBrandKey(two), two);
    }
  }

  const normalizedQuestion = ` ${words.join(' ')} `;
  for (const brand of uniqueBrandsFromProfiles(profiles)) {
    const probe = normalizeTextWithSpaces(brand);
    if (!probe) continue;
    if (normalizedQuestion.includes(` ${probe} `)) {
      requested.set(normalizeBrandKey(brand), brand);
    }
  }

  return Array.from(requested.values());
}

function formatBrandList(brands) {
  const list = (Array.isArray(brands) ? brands : []).filter(Boolean);
  if (!list.length) return '';
  if (list.length === 1) return list[0];
  if (list.length === 2) return `${list[0]} und ${list[1]}`;
  return `${list.slice(0, -1).join(', ')} und ${list[list.length - 1]}`;
}

function matchVehiclesByBrandQuestion(question, profiles) {
  if (!Array.isArray(profiles) || !profiles.length) return { requestedBrands: [], hits: [] };
  const requestedBrands = extractRequestedBrands(question, profiles);
  if (!requestedBrands.length) return { requestedBrands: [], hits: [] };
  const requestedKeys = new Set(requestedBrands.map((b) => normalizeBrandKey(b)).filter(Boolean));
  const hits = profiles.filter((vehicle) => requestedKeys.has(normalizeBrandKey(vehicleBrand(vehicle))));
  return { requestedBrands, hits };
}

function matchVehiclesByModelQuestion(question, profiles) {
  if (!Array.isArray(profiles) || !profiles.length) return [];
  const tokens = queryTokens(question);
  if (!tokens.length) return [];
  return profiles
    .map((vehicle) => {
      const model = String(vehicle?.model || '').toLowerCase();
      const score = tokens.reduce((acc, token) => (model.includes(token) ? acc + 1 : acc), 0);
      return { vehicle, score };
    })
    .filter((row) => row.score > 0)
    .sort((a, b) => b.score - a.score)
    .map((row) => row.vehicle);
}

function formatVehicleSummary(vehicle, includeCore = true) {
  const v = normalizeVehicleProfile(vehicle || {});
  if (!includeCore) {
    return `${v.model} (ID ${v.id})`;
  }
  return `${v.model} (ID ${v.id}), Baujahr ${v.year}, ${v.km} km, ${v.ps} PS`;
}

function extractRequestedModelWord(question) {
  const src = String(question || '').toLowerCase();
  const articleMatch = src.match(/\b(?:einen|eine|ein|den|die|das)\s+([a-z0-9äöüß-]{3,})\b/i);
  if (articleMatch && articleMatch[1] && !MODEL_QUERY_STOP_WORDS.has(articleMatch[1])) {
    return articleMatch[1];
  }
  const searchVerbMatch = src.match(
    /\b(?:habt ihr|hast du|gibt es|suche|suchst|finde|finden)\b(?:\s+(?:einen|eine|ein|den|die|das))?\s+([a-z0-9äöüß-]{3,})\b/i
  );
  if (searchVerbMatch && searchVerbMatch[1] && !MODEL_QUERY_STOP_WORDS.has(searchVerbMatch[1])) {
    return searchVerbMatch[1];
  }
  return '';
}

function buildFocusedVehicleReply(question, vehicle) {
  const q = String(question || '').toLowerCase();
  const v = normalizeVehicleProfile(vehicle || {});
  const equipment = oneLine(v.equipmentText || v.vehicleText, 260);
  if (/\b(preis|kosten|kostet|teuer)\b/.test(q)) {
    if (equipment) {
      return `Zum Preis gebe ich hier keine Angabe. Bei ${v.model} (ID ${v.id}) sind wichtige Extras: ${equipment}.`;
    }
    return `Zum Preis gebe ich hier keine Angabe. Ich beschreibe dir gern die Ausstattung von ${v.model} mit ID ${v.id}.`;
  }
  if (/\b(ausstattung|extras|extra|features|paket|pakete|komfort|sicherheit|infotainment|sitze|leder|navi|navigation)\b/.test(q)) {
    if (equipment) {
      return `Die Ausstattung von ${v.model} (ID ${v.id}) umfasst unter anderem: ${equipment}.`;
    }
    return `Zur Ausstattung von ${v.model} (ID ${v.id}) habe ich aktuell nur Basisdaten. Frag mich gern nach konkreten Extras.`;
  }
  if (/\b(kilometer|km|laufleistung)\b/.test(q)) {
    return `Das aktuell geoeffnete Fahrzeug ${v.model} hat ${v.km} Kilometer.`;
  }
  if (/\b(ps|leistung)\b/.test(q)) {
    return `${v.model} hat ${v.ps} PS.`;
  }
  if (/\b(baujahr|jahr|erstzulassung|ez)\b/.test(q)) {
    return `${v.model} ist aus dem Baujahr ${v.year}.`;
  }
  if (equipment) {
    return `Aktuell geoeffnet ist ${v.model} mit ID ${v.id}. Zur Ausstattung: ${equipment}.`;
  }
  return `Aktuell geoeffnet ist ${formatVehicleSummary(v, true)}.`;
}

function buildSingleVehicleScopeReply(vehicle, vehicleId = '') {
  const fallbackId = normalizeField(vehicleId, 'unbekannt');
  if (!vehicle) {
    return `Dieser QR-Code gehoert zum Fahrzeug mit ID ${fallbackId}. Ich beantworte dir dazu alle Fragen zu Ausstattung, Zustand und Probefahrt.`;
  }
  const v = normalizeVehicleProfile(vehicle || {});
  const id = v.id !== 'unbekannt' ? v.id : fallbackId;
  const equipmentTeaser = oneLine(v.equipmentText || v.vehicleText, 180);
  if (v.model && v.model !== 'unbekannt') {
    if (equipmentTeaser) {
      return `Dieser QR-Code gehoert zu ${v.model} mit ID ${id}. Wichtige Punkte zur Ausstattung: ${equipmentTeaser}.`;
    }
    return `Dieser QR-Code gehoert zum Fahrzeug ${v.model} mit ID ${id}. Ich beantworte dir dazu alle Fragen zu Ausstattung, Zustand und Probefahrt.`;
  }
  return `Dieser QR-Code gehoert zum Fahrzeug mit ID ${id}. Ich beantworte dir dazu alle Fragen zu Ausstattung, Zustand und Probefahrt.`;
}

function buildBrandOverviewReply(profiles) {
  const brands = uniqueBrandsFromProfiles(profiles);
  if (!brands.length) {
    return 'Aktuell habe ich keine Marken in der Fahrzeugliste gefunden.';
  }
  if (brands.length <= 10) {
    return `Aktuell haben wir Fahrzeuge von ${formatBrandList(brands)}.`;
  }
  return `Aktuell haben wir Fahrzeuge von ${formatBrandList(brands.slice(0, 10))} und weiteren Marken.`;
}

function buildBrandSearchReply(question, profiles) {
  const { requestedBrands, hits } = matchVehiclesByBrandQuestion(question, profiles);
  if (!requestedBrands.length) return '';

  if (!hits.length) {
    if (requestedBrands.length === 1) {
      return `Nein, aktuell habe ich keine Fahrzeuge von ${requestedBrands[0]} in unserer Liste.`;
    }
    return `Nein, aktuell habe ich keine Fahrzeuge der Marken ${formatBrandList(requestedBrands)} in unserer Liste.`;
  }

  if (requestedBrands.length === 1) {
    const requestedKey = normalizeBrandKey(requestedBrands[0]);
    const brandHits = hits.filter((vehicle) => normalizeBrandKey(vehicleBrand(vehicle)) === requestedKey);
    if (!brandHits.length) {
      return `Nein, aktuell habe ich keine Fahrzeuge von ${requestedBrands[0]} in unserer Liste.`;
    }
    if (brandHits.length === 1) {
      return `Ja, aktuell habe ich ein Fahrzeug von ${requestedBrands[0]}: ${formatVehicleSummary(brandHits[0], true)}.`;
    }
    const first = formatVehicleSummary(brandHits[0], false);
    const second = formatVehicleSummary(brandHits[1], false);
    return `Ja, aktuell habe ich ${brandHits.length} Fahrzeuge von ${requestedBrands[0]}, zum Beispiel ${first} und ${second}.`;
  }

  if (hits.length === 1) {
    return `Ja, aktuell habe ich ein passendes Fahrzeug der Marken ${formatBrandList(requestedBrands)}: ${formatVehicleSummary(hits[0], true)}.`;
  }
  const first = formatVehicleSummary(hits[0], false);
  const second = formatVehicleSummary(hits[1], false);
  return `Ja, aktuell habe ich passende Fahrzeuge der Marken ${formatBrandList(requestedBrands)}, zum Beispiel ${first} und ${second}.`;
}

function buildModelSearchReply(question, profiles) {
  if (!Array.isArray(profiles) || !profiles.length) {
    return 'Ich habe gerade keine Fahrzeugliste geladen. Frag mich bitte zum aktuell geoeffneten Fahrzeug oder versuche es gleich nochmal.';
  }
  const brandReply = buildBrandSearchReply(question, profiles);
  if (brandReply) return brandReply;

  const hits = matchVehiclesByModelQuestion(question, profiles);
  if (!hits.length) {
    const wanted = extractRequestedModelWord(question);
    if (wanted) {
      return `Aktuell habe ich das Modell ${wanted} in unserer Liste nicht gefunden.`;
    }
    return 'Aktuell habe ich kein passendes Fahrzeug in unserer Liste gefunden.';
  }
  if (hits.length === 1) {
    return `Ja, ich habe einen gefunden: ${formatVehicleSummary(hits[0], true)}.`;
  }
  const first = formatVehicleSummary(hits[0], false);
  const second = formatVehicleSummary(hits[1], false);
  return `Ja, ich habe passende Fahrzeuge gefunden: ${first} und ${second}.`;
}

function buildGreetingReply() {
  return 'Hallo, gern. Frag mich direkt zum Fahrzeug hier oder zu den wichtigsten Extras.';
}

function buildThanksReply() {
  return 'Gern. Wenn du magst, gehe ich direkt auf Ausstattung, Zustand oder Verfuegbarkeit ein.';
}

function buildDeterministicReply({
  transcript,
  focusedVehicle,
  vehicleProfiles,
  singleVehicleMode = false,
  currentVehicleId = ''
}) {
  const question = cleanRealtimeText(transcript);
  if (!question) return '';
  const q = question.toLowerCase();
  const requestedBrands = extractRequestedBrands(question, vehicleProfiles);
  const requestedModelWord = extractRequestedModelWord(question);
  const asksModelSearchVerb = /\b(habt ihr|hast du|gibt es|suche|suchst|finde|finden)\b/.test(q);
  const hasVehicleSearchKeyword = /\b(auto|autos|wagen|fahrzeug|fahrzeuge|modell|modelle|marke|marken|bestand|liste|id)\b/.test(q);
  const hasModelHit = matchVehiclesByModelQuestion(question, vehicleProfiles).length > 0;
  const isGreeting = /\b(hallo|hi|hey|moin|servus|guten tag|guten morgen|guten abend|gruezi|gruss gott)\b/.test(q);
  const isThanks = /\b(danke|dankeschoen|vielen dank|merci)\b/.test(q);
  const asksModelSearch = asksModelSearchVerb
    && (
      hasVehicleSearchKeyword
      || requestedBrands.length > 0
      || !!requestedModelWord
      || hasModelHit
    );
  const asksBrandOverview = /\b(automarke|automarken|marke|marken)\b/.test(q)
    && /\b(welche|was|habt ihr|gibt es|fuehrt ihr)\b/.test(q);
  const asksActiveVehicle = /\b(dieses|diesem|dieser|das auto|hier|aufgerufen|aktuell(?:es|en)? fahrzeug)\b/.test(q);
  const asksVehicleFacts = /\b(preis|kosten|kilometer|km|ps|leistung|baujahr|jahr|erstzulassung|ez|welches auto)\b/.test(q);
  const asksEquipment = /\b(ausstattung|extras?|extra|features?|paket|pakete|komfort|sicherheit|assistenz|navi|navigation|carplay|android auto|zustand)\b/.test(q);
  const hasVehicleIntent = asksModelSearch || asksBrandOverview || asksActiveVehicle || asksVehicleFacts || asksEquipment || requestedBrands.length > 0;

  if (isGreeting && !hasVehicleIntent) {
    return buildGreetingReply();
  }
  if (isThanks && !hasVehicleIntent) {
    return buildThanksReply();
  }

  if (singleVehicleMode && focusedVehicle && asksModelSearch && asksActiveVehicle) {
    return `Ja, hier am QR-Code geht es um ${formatVehicleSummary(focusedVehicle, true)}.`;
  }
  if (singleVehicleMode && focusedVehicle && (asksActiveVehicle || asksVehicleFacts || asksEquipment)) {
    return buildFocusedVehicleReply(question, focusedVehicle);
  }
  if (singleVehicleMode && (asksBrandOverview || asksModelSearch || requestedBrands.length > 0)) {
    return buildSingleVehicleScopeReply(focusedVehicle, currentVehicleId);
  }
  if (asksBrandOverview && !requestedBrands.length) {
    return buildBrandOverviewReply(vehicleProfiles);
  }
  if (asksModelSearch || requestedBrands.length) {
    return buildModelSearchReply(question, vehicleProfiles);
  }
  if (focusedVehicle && (asksActiveVehicle || asksVehicleFacts || asksEquipment)) {
    return buildFocusedVehicleReply(question, focusedVehicle);
  }
  return '';
}

function asksIdentityQuestion(question) {
  const q = cleanRealtimeText(question).toLowerCase();
  if (!q) return false;
  return /\b(wer bist|wie heisst|wie heißt|was bist|stell dich vor|vorstellen)\b/.test(q);
}

function looksLikeIdentityMonologue(text) {
  const t = cleanRealtimeText(text).toLowerCase();
  if (!t) return false;
  return /\b(ich bin wally|digitaler fahrzeugberater|wie kann ich dir helfen)\b/.test(t);
}

function extractLatestAssistantReply(history) {
  const turns = Array.isArray(history) ? history : [];
  for (let i = turns.length - 1; i >= 0; i -= 1) {
    const turn = turns[i] || {};
    if (String(turn.role || '').toLowerCase() !== 'assistant') continue;
    const content = normalizeContent(turn.content);
    if (content) return content;
  }
  return '';
}

function normalizeReplyKey(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function avoidImmediateRepeat(candidate, history, focusedVehicle) {
  const nextKey = normalizeReplyKey(candidate);
  if (!nextKey) return candidate;
  const lastAssistant = extractLatestAssistantReply(history);
  const prevKey = normalizeReplyKey(lastAssistant);
  if (!prevKey || prevKey !== nextKey) return candidate;
  const v = focusedVehicle ? normalizeVehicleProfile(focusedVehicle) : null;
  if (v && v.model !== 'unbekannt') {
    const equipment = oneLine(v.equipmentText || v.vehicleText, 180);
    if (equipment) {
      return `Kurz anders formuliert: Bei ${v.model} (ID ${v.id}) sind unter anderem ${equipment}.`;
    }
    return `Kurz anders formuliert: Aktuell geht es um ${v.model} mit ID ${v.id}.`;
  }
  return 'Kurz anders formuliert: Frag mich bitte direkt nach Marke, Modell oder Ausstattung.';
}

function stabilizeReply({
  modelText,
  transcript,
  focusedVehicle,
  vehicleProfiles,
  history = [],
  singleVehicleMode = false,
  currentVehicleId = ''
}) {
  const deterministic = buildDeterministicReply({
    transcript,
    focusedVehicle,
    vehicleProfiles,
    singleVehicleMode,
    currentVehicleId
  });
  const sanitizedModel = sanitizeAssistantText(modelText);
  let candidate = deterministic || sanitizedModel;
  if (candidate && looksLikeIdentityMonologue(candidate) && !asksIdentityQuestion(transcript)) {
    candidate = deterministic || 'Frag mich bitte direkt nach einem Modell oder nach Ausstattung, Baujahr, Kilometer und PS.';
  }
  if (!candidate || isLikelyTechnicalNoise(candidate)) {
    if (singleVehicleMode) return buildSingleVehicleScopeReply(focusedVehicle, currentVehicleId);
    return deterministic || WALLY_UNKNOWN_REPLY;
  }
  return avoidImmediateRepeat(candidate, history, focusedVehicle);
}

function waitMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms || 0)));
}

function nowNs() {
  return process.hrtime.bigint();
}

function elapsedMs(startNs) {
  if (!startNs) return 0;
  return Number((nowNs() - startNs) / 1000000n);
}

function clampMs(value, fallback = 0) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.max(0, Math.min(15000, Math.round(num)));
}

function pushTurnDiagnostic(entry) {
  turnDiagnostics.push({
    ts: new Date().toISOString(),
    status: entry.status || 'ok',
    turnId: String(entry.turnId || '').slice(0, 80),
    recordMs: clampMs(entry.recordMs, 0),
    decodeMs: clampMs(entry.decodeMs, 0),
    xaiMs: clampMs(entry.xaiMs, 0),
    totalMs: clampMs(entry.totalMs, 0),
    error: entry.error ? String(entry.error).slice(0, 180) : ''
  });
  while (turnDiagnostics.length > DIAG_MAX_ENTRIES) turnDiagnostics.shift();
}

function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const idx = Math.max(0, Math.min(sorted.length - 1, Math.round((sorted.length - 1) * p)));
  return sorted[idx];
}

function summarizeField(items, field) {
  const vals = items.map((x) => Number(x[field] || 0)).filter((v) => Number.isFinite(v) && v >= 0);
  if (!vals.length) return { avg: 0, p50: 0, p95: 0, min: 0, max: 0 };
  const sum = vals.reduce((a, b) => a + b, 0);
  return {
    avg: Math.round(sum / vals.length),
    p50: percentile(vals, 0.5),
    p95: percentile(vals, 0.95),
    min: Math.min(...vals),
    max: Math.max(...vals)
  };
}

function buildDiagnosticsSnapshot(limit = 30) {
  const safeLimit = Math.max(1, Math.min(200, Number(limit) || 30));
  const latest = turnDiagnostics.slice(-safeLimit);
  const okOnly = turnDiagnostics.filter((x) => x.status === 'ok');
  return {
    count: turnDiagnostics.length,
    okCount: okOnly.length,
    errorCount: turnDiagnostics.length - okOnly.length,
    summary: {
      recordMs: summarizeField(okOnly, 'recordMs'),
      decodeMs: summarizeField(okOnly, 'decodeMs'),
      xaiMs: summarizeField(okOnly, 'xaiMs'),
      totalMs: summarizeField(okOnly, 'totalMs')
    },
    latest
  };
}

function resolveKeepaliveUrl() {
  if (KEEPALIVE_URL) return KEEPALIVE_URL;
  const localHost = (HOST === '0.0.0.0' || HOST === '::') ? '127.0.0.1' : HOST;
  return `http://${localHost}:${PORT}/api/health`;
}

function pingUrl(urlString, timeoutMs = 5000) {
  return new Promise((resolve) => {
    let target = null;
    try {
      target = new URL(urlString);
    } catch (_) {
      resolve(0);
      return;
    }
    const client = target.protocol === 'https:' ? https : http;
    const req = client.request(target, { method: 'GET', timeout: timeoutMs }, (resp) => {
      resp.resume();
      resolve(Number(resp.statusCode || 0));
    });
    req.on('error', () => resolve(0));
    req.on('timeout', () => {
      req.destroy();
      resolve(0);
    });
    req.end();
  });
}

function startKeepaliveLoop() {
  if (!KEEPALIVE_ENABLED) return;
  const target = resolveKeepaliveUrl();
  if (!target) return;

  const tick = async () => {
    const status = await pingUrl(target, 5000);
    if (!status || status >= 400) {
      console.warn(`[keepalive] ping ${target} -> ${status || 'ERR'}`);
    }
  };

  setTimeout(() => {
    tick().catch(() => {});
  }, 1200);
  const timer = setInterval(() => {
    tick().catch(() => {});
  }, KEEPALIVE_INTERVAL_MS);
  if (typeof timer.unref === 'function') timer.unref();
  console.log(`[keepalive] enabled target=${target} interval=${Math.round(KEEPALIVE_INTERVAL_MS / 1000)}s`);
}

function serveStatic(req, res, pathname) {
  let clean = pathname;
  if (clean === '/') {
    const defaultPage = fs.existsSync(path.join(FRONTEND_ROOT, 'index.html')) ? '/index.html' : '/widget.html';
    clean = defaultPage;
  }
  const target = path.normalize(path.join(FRONTEND_ROOT, clean.replace(/^\/+/, '')));
  if (!target.startsWith(FRONTEND_ROOT)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.stat(target, (err, stat) => {
    if (err || !stat.isFile()) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    const stream = fs.createReadStream(target);
    res.writeHead(200, { 'Content-Type': contentTypeFor(target) });
    stream.pipe(res);
    stream.on('error', () => {
      if (!res.headersSent) res.writeHead(500);
      res.end('Read error');
    });
  });
}

function execFile(bin, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let stderr = '';
    child.stderr.on('data', (d) => {
      stderr += String(d);
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${bin} failed (${code}): ${stderr.slice(0, 400)}`));
    });
  });
}

async function convertWebmToPcm16(webmBuffer) {
  const id = crypto.randomBytes(6).toString('hex');
  const inPath = path.join(os.tmpdir(), `xai_in_${id}.webm`);
  const outPath = path.join(os.tmpdir(), `xai_out_${id}.pcm`);
  await fs.promises.writeFile(inPath, webmBuffer);
  try {
    const bins = [FFMPEG_BIN, 'ffmpeg'];
    let converted = false;
    let lastErr = null;
    for (const bin of bins) {
      try {
        await execFile(bin, [
          '-hide_banner',
          '-loglevel', 'error',
          '-y',
          '-i', inPath,
          '-ac', '1',
          '-ar', '16000',
          '-f', 's16le',
          outPath
        ]);
        converted = true;
        break;
      } catch (err) {
        lastErr = err;
      }
    }
    if (!converted) throw lastErr || new Error('ffmpeg conversion failed');
    const pcm = await fs.promises.readFile(outPath);
    return pcm;
  } finally {
    fs.promises.unlink(inPath).catch(() => {});
    fs.promises.unlink(outPath).catch(() => {});
  }
}

function runXaiRealtimeTurn({ apiKey, pcmBase64, history, instructions, signal, onTranscript }) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket('wss://api.x.ai/v1/realtime', {
      headers: {
        Authorization: `Bearer ${apiKey}`
      }
    });

    let done = false;
    let text = '';
    let audioTranscriptText = '';
    let transcript = '';
    const audioChunks = [];
    const timer = setTimeout(() => {
      if (done) return;
      done = true;
      try { ws.close(); } catch (_) {}
      reject(new Error(`xAI realtime timeout after ${XAI_TURN_TIMEOUT_MS}ms`));
    }, XAI_TURN_TIMEOUT_MS);

    const onAbort = () => {
      const abortErr = new Error('client aborted');
      abortErr.name = 'AbortError';
      finish(abortErr);
    };

    const finish = (err, payload) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      if (signal && typeof signal.removeEventListener === 'function') {
        signal.removeEventListener('abort', onAbort);
      }
      try { ws.close(); } catch (_) {}
      if (err) reject(err);
      else resolve(payload);
    };

    if (signal && typeof signal.addEventListener === 'function') {
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener('abort', onAbort, { once: true });
    }

    ws.on('open', () => {
      const sessionInstructions = buildSessionInstructions(instructions, history);
      const sessionPayload = {
        voice: 'Ara',
        instructions: sessionInstructions,
        // Manual turn flow keeps behavior deterministic for buffered chunks.
        turn_detection: { type: null },
        audio: {
          input: { format: { type: 'audio/pcm', rate: 16000 } },
          output: { format: { type: 'audio/pcm', rate: 24000 } }
        }
      };
      if (XAI_MODEL) sessionPayload.model = XAI_MODEL;
      ws.send(JSON.stringify({
        type: 'session.update',
        session: sessionPayload
      }));

      // Keep turn creation minimal and deterministic for stability:
      // send one committed audio turn, then request response.

      ws.send(JSON.stringify({ type: 'input_audio_buffer.append', audio: pcmBase64 }));
      ws.send(JSON.stringify({ type: 'input_audio_buffer.commit' }));
      const responsePayload = {
        modalities: XAI_STREAM_AUDIO ? ['text', 'audio'] : ['text']
      };
      if (XAI_MODEL) responsePayload.model = XAI_MODEL;
      ws.send(JSON.stringify({
        type: 'response.create',
        response: responsePayload
      }));
    });

    ws.on('message', (raw) => {
      let msg = null;
      try {
        msg = JSON.parse(raw.toString());
      } catch (_) {
        return;
      }
      const type = msg?.type || '';

      if (type === 'error') {
        const e = msg?.error || {};
        const detail = [
          e.message,
          e.type ? `type=${e.type}` : '',
          e.code ? `code=${e.code}` : ''
        ].filter(Boolean).join(' | ');
        finish(new Error(detail || 'xAI realtime error'));
        return;
      }

      if (type === 'conversation.item.input_audio_transcription.completed' && typeof msg.transcript === 'string') {
        transcript = msg.transcript.trim();
        if (typeof onTranscript === 'function' && transcript) {
          try {
            const quickText = cleanRealtimeText(onTranscript(transcript));
            if (quickText) {
              finish(null, {
                text: quickText,
                transcript,
                audioBase64: '',
                source: 'local-transcript'
              });
              return;
            }
          } catch (_) {}
        }
        return;
      }
      if ((type === 'response.output_text.delta' || type === 'response.text.delta') && typeof msg.delta === 'string') {
        text += msg.delta;
        return;
      }
      if ((type === 'response.output_audio_transcript.delta' || type === 'response.audio_transcript.delta') && typeof msg.delta === 'string') {
        audioTranscriptText += msg.delta;
        return;
      }
      if ((type === 'response.output_audio_transcript.done' || type === 'response.audio_transcript.done')
        && typeof msg.transcript === 'string'
        && !audioTranscriptText.trim()) {
        audioTranscriptText = msg.transcript.trim();
        return;
      }
      if ((type === 'response.output_audio.delta' || type === 'response.audio.delta') && typeof msg.delta === 'string') {
        try {
          audioChunks.push(Buffer.from(msg.delta, 'base64'));
        } catch (_) {}
        return;
      }
      if (type === 'response.done') {
        const textFromDelta = cleanRealtimeText(text);
        const textFromDone = cleanRealtimeText(extractTextFromResponseDone(msg));
        const textFromAudioTranscript = cleanRealtimeText(audioTranscriptText);
        const rawText = textFromDelta || textFromDone || textFromAudioTranscript;
        const safeText = rawText || WALLY_UNKNOWN_REPLY;
        const audio = XAI_STREAM_AUDIO && audioChunks.length ? Buffer.concat(audioChunks).toString('base64') : '';
        finish(null, {
          text: safeText,
          transcript,
          audioBase64: audio
        });
      }
    });

    ws.on('error', (err) => finish(err));
    ws.on('close', () => {
      if (!done) finish(new Error('xAI realtime closed before completion'));
    });
  });
}

function wsSendJson(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  try {
    ws.send(JSON.stringify(payload));
    return true;
  } catch (_) {
    return false;
  }
}

function makeRealtimeSessionId() {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  return crypto.randomBytes(16).toString('hex');
}

function makeRealtimeSessionToken() {
  return crypto.randomBytes(18).toString('hex');
}

function clampHistoryTurns(history) {
  const turns = Array.isArray(history) ? history : [];
  return turns.slice(-MAX_HISTORY_TURNS * 2);
}

function resetRealtimeTurnBuffers(session) {
  session.textBuffer = '';
  session.audioTranscriptBuffer = '';
  session.audioChunks = [];
  session.lastUserTranscript = '';
}

function scheduleRealtimeSessionCleanup(session) {
  if (!session || session.closed) return;
  if (session.cleanupTimer) clearTimeout(session.cleanupTimer);
  session.cleanupTimer = setTimeout(() => {
    if (session.closed) return;
    const idleMs = Date.now() - session.lastActivity;
    if (session.clients.size > 0 || idleMs < REALTIME_SESSION_IDLE_MS) {
      scheduleRealtimeSessionCleanup(session);
      return;
    }
    closeRealtimeSession(session, 'idle-timeout');
  }, REALTIME_SESSION_IDLE_MS + 250);
  if (typeof session.cleanupTimer.unref === 'function') session.cleanupTimer.unref();
}

function touchRealtimeSession(session) {
  if (!session || session.closed) return;
  session.lastActivity = Date.now();
  scheduleRealtimeSessionCleanup(session);
}

function broadcastRealtime(session, payload) {
  if (!session || !session.clients.size) return;
  for (const client of Array.from(session.clients)) {
    if (!client || client.readyState !== WebSocket.OPEN) {
      session.clients.delete(client);
      continue;
    }
    try {
      client.send(JSON.stringify(payload));
    } catch (_) {
      session.clients.delete(client);
    }
  }
}

function closeRealtimeSession(session, reason = 'closed') {
  if (!session || session.closed) return;
  session.closed = true;
  session.ready = false;
  if (session.cleanupTimer) clearTimeout(session.cleanupTimer);
  realtimeSessions.delete(session.id);

  const upstream = session.upstream;
  session.upstream = null;
  if (upstream && (upstream.readyState === WebSocket.OPEN || upstream.readyState === WebSocket.CONNECTING)) {
    try { upstream.close(); } catch (_) {}
  }

  for (const client of Array.from(session.clients)) {
    wsSendJson(client, {
      type: 'session.closed',
      sessionId: session.id,
      reason
    });
    if (client.readyState === WebSocket.OPEN) {
      try { client.close(1011, reason); } catch (_) {}
    }
  }
  session.clients.clear();
}

function buildRealtimeSessionPayload(session) {
  const payload = {
    voice: 'Ara',
    instructions: buildSessionInstructions(session.instructions, session.history),
    turn_detection: { type: 'server_vad' },
    audio: {
      input: { format: { type: 'audio/pcm', rate: 16000 } },
      output: { format: { type: 'audio/pcm', rate: 24000 } }
    }
  };
  if (XAI_MODEL) payload.model = XAI_MODEL;
  return payload;
}

function sendRealtimeSessionUpdate(session) {
  if (!session || session.closed || !session.upstream || session.upstream.readyState !== WebSocket.OPEN) return false;
  try {
    session.upstream.send(JSON.stringify({
      type: 'session.update',
      session: buildRealtimeSessionPayload(session)
    }));
    return true;
  } catch (_) {
    return false;
  }
}

function applyRealtimeSessionContext(session, payload = {}) {
  if (!session || session.closed) return;
  const mergedPayload = { ...payload };
  if (!Array.isArray(mergedPayload.history)) mergedPayload.history = session.history;
  if (!mergedPayload.currentVehicleId && session.currentVehicleId) mergedPayload.currentVehicleId = session.currentVehicleId;
  if (!mergedPayload.currentVehicle && session.focusedVehicle) mergedPayload.currentVehicle = session.focusedVehicle;
  if (!Array.isArray(mergedPayload.vehicleProfiles) && Array.isArray(session.vehicleProfiles) && session.vehicleProfiles.length) {
    mergedPayload.vehicleProfiles = session.vehicleProfiles;
  }

  const context = buildTurnContext(mergedPayload);
  session.history = clampHistoryTurns(context.historyTurns);
  session.focusedVehicle = context.focusedVehicle;
  session.vehicleProfiles = context.vehicleProfiles;
  session.singleVehicleMode = context.singleVehicleMode;
  session.currentVehicleId = context.currentVehicleId;
  session.instructions = context.mergedInstructions;
  sendRealtimeSessionUpdate(session);
}

function buildRealtimeErrorDetail(msg) {
  const e = msg?.error || {};
  return [
    e.message,
    e.type ? `type=${e.type}` : '',
    e.code ? `code=${e.code}` : ''
  ].filter(Boolean).join(' | ');
}

function safeDecodeAudioChunks(chunks) {
  if (!Array.isArray(chunks) || !chunks.length) return '';
  const buffers = [];
  for (const part of chunks) {
    if (typeof part !== 'string' || !part) continue;
    try {
      buffers.push(Buffer.from(part, 'base64'));
    } catch (_) {}
  }
  if (!buffers.length) return '';
  return Buffer.concat(buffers).toString('base64');
}

function handleRealtimeUpstreamMessage(session, msg) {
  if (!session || session.closed || !msg || typeof msg !== 'object') return;
  touchRealtimeSession(session);
  const type = String(msg.type || '');
  const turnId = session.activeTurnId || '';

  if (type === 'error') {
    const detail = buildRealtimeErrorDetail(msg) || 'xAI realtime error';
    broadcastRealtime(session, { type: 'session.error', sessionId: session.id, error: detail, turnId });
    return;
  }

  if (type === 'conversation.item.input_audio_transcription.completed' && typeof msg.transcript === 'string') {
    const transcript = cleanRealtimeText(msg.transcript);
    session.lastUserTranscript = transcript;
    if (transcript) session.history.push({ role: 'user', content: transcript });
    session.history = clampHistoryTurns(session.history);
    broadcastRealtime(session, { type: 'input.transcript', sessionId: session.id, turnId, transcript });
    return;
  }

  if (type === 'input_audio_buffer.speech_started') {
    if (session.upstream && session.upstream.readyState === WebSocket.OPEN) {
      try { session.upstream.send(JSON.stringify({ type: 'response.cancel' })); } catch (_) {}
    }
    broadcastRealtime(session, { type: 'input_audio_buffer.speech_started', sessionId: session.id, turnId });
    return;
  }

  if ((type === 'response.output_text.delta' || type === 'response.text.delta') && typeof msg.delta === 'string') {
    session.textBuffer += msg.delta;
    broadcastRealtime(session, { type: 'response.text.delta', sessionId: session.id, turnId, delta: msg.delta });
    return;
  }

  if ((type === 'response.output_audio.delta' || type === 'response.audio.delta') && typeof msg.delta === 'string') {
    session.audioChunks.push(msg.delta);
    broadcastRealtime(session, { type: 'response.audio.delta', sessionId: session.id, turnId, delta: msg.delta });
    return;
  }

  if ((type === 'response.output_audio_transcript.delta' || type === 'response.audio_transcript.delta') && typeof msg.delta === 'string') {
    session.audioTranscriptBuffer += msg.delta;
    broadcastRealtime(session, { type: 'response.audio_transcript.delta', sessionId: session.id, turnId, delta: msg.delta });
    return;
  }

  if ((type === 'response.output_audio_transcript.done' || type === 'response.audio_transcript.done')
    && typeof msg.transcript === 'string' && !session.audioTranscriptBuffer.trim()) {
    session.audioTranscriptBuffer = cleanRealtimeText(msg.transcript);
    return;
  }

  if (type === 'response.done') {
    const textFromDelta = cleanRealtimeText(session.textBuffer);
    const textFromDone = cleanRealtimeText(extractTextFromResponseDone(msg));
    const textFromAudioTranscript = cleanRealtimeText(session.audioTranscriptBuffer);
    const modelText = textFromDelta || textFromDone || textFromAudioTranscript;
    const transcript = cleanRealtimeText(session.lastUserTranscript);
    const stableText = stabilizeReply({
      modelText: modelText || WALLY_UNKNOWN_REPLY,
      transcript,
      focusedVehicle: session.focusedVehicle,
      vehicleProfiles: session.vehicleProfiles,
      history: session.history,
      singleVehicleMode: session.singleVehicleMode,
      currentVehicleId: session.currentVehicleId
    });
    session.history.push({ role: 'assistant', content: stableText });
    session.history = clampHistoryTurns(session.history);
    const audioBase64 = XAI_STREAM_AUDIO ? safeDecodeAudioChunks(session.audioChunks) : '';
    broadcastRealtime(session, {
      type: 'response.done',
      sessionId: session.id,
      turnId,
      text: stableText,
      transcript,
      audioBase64
    });
    session.activeTurnId = '';
    resetRealtimeTurnBuffers(session);
  }
}

function createRealtimeSession({ apiKey, initPayload = {} }) {
  const context = buildTurnContext(initPayload);
  const session = {
    id: makeRealtimeSessionId(),
    token: makeRealtimeSessionToken(),
    closed: false,
    ready: false,
    createdAt: Date.now(),
    lastActivity: Date.now(),
    cleanupTimer: null,
    clients: new Set(),
    upstream: null,
    readyPromise: null,
    activeTurnId: '',
    textBuffer: '',
    audioTranscriptBuffer: '',
    audioChunks: [],
    lastUserTranscript: '',
    history: clampHistoryTurns(context.historyTurns),
    focusedVehicle: context.focusedVehicle,
    vehicleProfiles: context.vehicleProfiles,
    singleVehicleMode: context.singleVehicleMode,
    currentVehicleId: context.currentVehicleId,
    instructions: context.mergedInstructions
  };

  realtimeSessions.set(session.id, session);
  touchRealtimeSession(session);

  session.readyPromise = new Promise((resolve, reject) => {
    let settled = false;
    const settleReady = (err) => {
      if (settled) return;
      settled = true;
      if (err) reject(err);
      else resolve();
    };

    const upstream = new WebSocket('wss://api.x.ai/v1/realtime', {
      headers: { Authorization: `Bearer ${apiKey}` }
    });
    session.upstream = upstream;

    upstream.on('open', () => {
      if (session.closed) {
        settleReady(new Error('session closed'));
        return;
      }
      session.ready = true;
      resetRealtimeTurnBuffers(session);
      sendRealtimeSessionUpdate(session);
      touchRealtimeSession(session);
      settleReady(null);
      broadcastRealtime(session, { type: 'session.ready', sessionId: session.id });
    });

    upstream.on('message', (raw) => {
      let msg = null;
      try {
        msg = JSON.parse(raw.toString());
      } catch (_) {
        return;
      }
      handleRealtimeUpstreamMessage(session, msg);
    });

    upstream.on('error', (err) => {
      const detail = err?.message || 'upstream websocket error';
      broadcastRealtime(session, { type: 'session.error', sessionId: session.id, error: detail, turnId: session.activeTurnId || '' });
      if (!session.ready) settleReady(new Error(detail));
      closeRealtimeSession(session, 'upstream-error');
    });

    upstream.on('close', () => {
      if (!session.closed) {
        broadcastRealtime(session, { type: 'session.error', sessionId: session.id, error: 'xAI realtime closed', turnId: session.activeTurnId || '' });
      }
      if (!session.ready) settleReady(new Error('xAI realtime closed before ready'));
      closeRealtimeSession(session, 'upstream-closed');
    });
  });

  return session;
}

function sendRealtimeResponseCreate(session) {
  if (!session || session.closed || !session.upstream || session.upstream.readyState !== WebSocket.OPEN) return false;
  const responsePayload = {
    modalities: XAI_STREAM_AUDIO ? ['text', 'audio'] : ['text']
  };
  if (XAI_MODEL) responsePayload.model = XAI_MODEL;
  try {
    session.upstream.send(JSON.stringify({ type: 'response.create', response: responsePayload }));
    return true;
  } catch (_) {
    return false;
  }
}

function handleRealtimeClientPayload(session, client, payload) {
  if (!session || session.closed || !client) return;
  if (!payload || typeof payload !== 'object') {
    wsSendJson(client, { type: 'session.error', sessionId: session.id, error: 'Invalid message payload.' });
    return;
  }
  touchRealtimeSession(session);
  const type = String(payload.type || '');

  if (type === 'session.ping') {
    wsSendJson(client, { type: 'session.pong', sessionId: session.id, ts: Date.now() });
    return;
  }

  if (type === 'session.init' || type === 'session.update') {
    applyRealtimeSessionContext(session, payload);
    wsSendJson(client, { type: 'session.context.updated', sessionId: session.id });
    return;
  }

  if (!session.ready || !session.upstream || session.upstream.readyState !== WebSocket.OPEN) {
    wsSendJson(client, { type: 'session.error', sessionId: session.id, error: 'Realtime session not ready yet.' });
    return;
  }

  if (type === 'input_audio') {
    const audio = normalizeBase64Payload(payload.audio, MAX_REALTIME_CHUNK_BASE64_CHARS);
    if (!audio) return;
    try {
      session.upstream.send(JSON.stringify({ type: 'input_audio_buffer.append', audio }));
    } catch (_) {
      wsSendJson(client, { type: 'session.error', sessionId: session.id, error: 'Failed to append input audio.' });
    }
    return;
  }

  if (type === 'commit') {
    const turnId = oneLine(payload.turnId || '', 80) || `${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
    session.activeTurnId = turnId;
    resetRealtimeTurnBuffers(session);
    try {
      session.upstream.send(JSON.stringify({ type: 'input_audio_buffer.commit' }));
      if (!sendRealtimeResponseCreate(session)) {
        wsSendJson(client, { type: 'session.error', sessionId: session.id, turnId, error: 'Failed to start response.' });
        return;
      }
      broadcastRealtime(session, { type: 'turn.started', sessionId: session.id, turnId });
    } catch (_) {
      wsSendJson(client, { type: 'session.error', sessionId: session.id, turnId, error: 'Failed to commit audio turn.' });
    }
    return;
  }

  if (type === 'response.cancel') {
    try {
      session.upstream.send(JSON.stringify({ type: 'response.cancel' }));
    } catch (_) {}
    return;
  }

  wsSendJson(client, { type: 'session.error', sessionId: session.id, error: `Unsupported message type: ${type || 'unknown'}` });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  const origin = String(req.headers.origin || '').trim();

  if (url.pathname.startsWith('/api/') && !isOriginAllowed(origin)) {
    sendJson(res, 403, { error: 'Origin not allowed.' }, origin);
    return;
  }

  if (req.method === 'OPTIONS' && url.pathname.startsWith('/api/')) {
    res.writeHead(204, {
      ...corsHeaders(origin),
      'Access-Control-Allow-Methods': 'POST, GET, HEAD, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Max-Age': '86400'
    });
    res.end();
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/xai/turn') {
    const reqStartNs = nowNs();
    let diagTurnId = '';
    let diagRecordMs = 0;
    let diagDecodeMs = 0;
    let diagXaiMs = 0;
    let diagCommitted = false;
    const commitDiag = (status, error = '') => {
      if (diagCommitted) return;
      diagCommitted = true;
      pushTurnDiagnostic({
        status,
        turnId: diagTurnId,
        recordMs: diagRecordMs,
        decodeMs: diagDecodeMs,
        xaiMs: diagXaiMs,
        totalMs: elapsedMs(reqStartNs),
        error
      });
    };

    const turnAbortController = new AbortController();
    const abortTurn = () => {
      if (!turnAbortController.signal.aborted) turnAbortController.abort();
    };
    const onReqAborted = () => abortTurn();
    const onResClose = () => {
      if (!res.writableEnded) abortTurn();
    };
    req.on('aborted', onReqAborted);
    res.on('close', onResClose);
    const canRespond = () => !turnAbortController.signal.aborted && !res.writableEnded && !res.destroyed;
    try {
      const body = await readJson(req);
      const key = process.env.XAI_API_KEY || process.env.OPENAI_API_KEY;
      if (!key) {
        if (canRespond()) sendJson(res, 400, { error: 'Missing API key (set XAI_API_KEY in backend env).' }, origin);
        return;
      }
      const turnId = typeof body.turnId === 'string' ? body.turnId.slice(0, 80) : '';
      diagTurnId = turnId;
      diagRecordMs = clampMs(body.captureMs, 0);
      const debugTranscript = typeof body.debugTranscript === 'string' ? body.debugTranscript.slice(0, 280) : '';
      const pcmBase64Input = normalizeBase64Payload(body.pcmBase64, MAX_TURN_PCM_BASE64_CHARS);
      const audioBase64 = normalizeBase64Payload(body.audioBase64, MAX_TURN_AUDIO_BASE64_CHARS);
      if (!pcmBase64Input && !audioBase64) {
        if (canRespond()) {
          sendJson(res, 200, {
            text: WALLY_NO_AUDIO_REPLY,
            transcript: '',
            audioBase64: '',
            turnId,
            noAudio: true
          }, origin);
        }
        commitDiag('error', 'missing audio payload');
        return;
      }
      const context = buildTurnContext(body);
      const {
        currentVehicleId,
        singleVehicleMode,
        focusedVehicle,
        vehicleProfiles,
        mergedInstructions,
        historyTurns
      } = context;

      if (WALLY_SMOKE_MODE) {
        if (WALLY_SMOKE_DELAY_MS > 0) await waitMs(WALLY_SMOKE_DELAY_MS);
        const transcript = cleanRealtimeText(debugTranscript || 'Smoke-Test Anfrage');
        const syntheticRaw = focusedVehicle
          ? `ID==${focusedVehicle.id} Modell==${focusedVehicle.model}`
          : 'ID==unbekannt Modell==unbekannt ==';
        const text = stabilizeReply({
          modelText: syntheticRaw,
          transcript,
          focusedVehicle,
          vehicleProfiles,
          history: historyTurns,
          singleVehicleMode,
          currentVehicleId
        });
        if (canRespond()) {
          sendJson(res, 200, {
            text,
            transcript,
            audioBase64: '',
            turnId
          }, origin);
        }
        commitDiag('ok');
        return;
      }

      let pcmBase64 = pcmBase64Input;
      if (!pcmBase64) {
        const decodeStartNs = nowNs();
        const webmBuffer = Buffer.from(audioBase64, 'base64');
        const pcm = await convertWebmToPcm16(webmBuffer);
        diagDecodeMs = elapsedMs(decodeStartNs);
        pcmBase64 = pcm.toString('base64');
      }
      const quickReplyFromTranscript = WALLY_FAST_TRANSCRIPT_MODE
        ? (transcript) => {
          const deterministic = buildDeterministicReply({
            transcript,
            focusedVehicle,
            vehicleProfiles,
            singleVehicleMode,
            currentVehicleId
          });
          if (!deterministic) return '';
          return stabilizeReply({
            modelText: deterministic,
            transcript,
            focusedVehicle,
            vehicleProfiles,
            history: historyTurns,
            singleVehicleMode,
            currentVehicleId
          });
        }
        : null;
      const xaiStartNs = nowNs();
      const result = await runXaiRealtimeTurn({
        apiKey: key,
        pcmBase64,
        history: historyTurns,
        instructions: mergedInstructions,
        signal: turnAbortController.signal,
        onTranscript: quickReplyFromTranscript
      });
      diagXaiMs = elapsedMs(xaiStartNs);
      if (!canRespond()) return;
      const stabilizationTranscript = result.transcript || cleanRealtimeText(debugTranscript);
      result.text = stabilizeReply({
        modelText: result.text,
        transcript: stabilizationTranscript,
        focusedVehicle,
        vehicleProfiles,
        history: historyTurns,
        singleVehicleMode,
        currentVehicleId
      });
      result.turnId = turnId;
      if (canRespond()) sendJson(res, 200, result, origin);
      commitDiag('ok');
    } catch (err) {
      if (err?.name === 'AbortError' || turnAbortController.signal.aborted) {
        commitDiag('aborted', err?.message || 'aborted');
        return;
      }
      console.error('[xai-turn] error:', err?.message || err);
      if (canRespond()) sendJson(res, 500, { error: err?.message || 'Server error' }, origin);
      commitDiag('error', err?.message || 'server error');
    } finally {
      req.removeListener('aborted', onReqAborted);
      res.removeListener('close', onResClose);
    }
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/health') {
    const hasXaiKey = !!String(process.env.XAI_API_KEY || '').trim();
    const hasOpenAiKey = !!String(process.env.OPENAI_API_KEY || '').trim();
    const diag = buildDiagnosticsSnapshot(1);
    const lastTurn = diag.latest.length ? diag.latest[diag.latest.length - 1] : null;
    sendJson(res, 200, {
      ok: true,
      service: 'wally-backend',
      ts: new Date().toISOString(),
      uptimeSec: Math.round(process.uptime()),
      apiKeyConfigured: hasXaiKey || hasOpenAiKey,
      apiProvider: hasXaiKey ? 'xai' : (hasOpenAiKey ? 'openai-fallback' : 'none'),
      xaiModel: XAI_MODEL || 'default',
      xaiStreamAudio: XAI_STREAM_AUDIO,
      realtimeSessions: realtimeSessions.size,
      diag: {
        count: diag.count,
        okCount: diag.okCount,
        errorCount: diag.errorCount,
        p95TotalMs: diag.summary.totalMs.p95,
        p95XaiMs: diag.summary.xaiMs.p95,
        lastTurn: lastTurn ? {
          ts: lastTurn.ts,
          status: lastTurn.status,
          totalMs: lastTurn.totalMs,
          recordMs: lastTurn.recordMs,
          decodeMs: lastTurn.decodeMs,
          xaiMs: lastTurn.xaiMs
        } : null
      }
    }, origin);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/diag') {
    const limit = Number(url.searchParams.get('limit') || 30);
    sendJson(res, 200, {
      ok: true,
      service: 'wally-backend',
      ...buildDiagnosticsSnapshot(limit)
    }, origin);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/inventory') {
    const vehicleProfiles = loadInventoryProfiles();
    sendJson(res, 200, {
      ok: true,
      source: INVENTORY_FILE,
      count: vehicleProfiles.length,
      sample: vehicleProfiles.slice(0, 5)
    }, origin);
    return;
  }

  if (req.method === 'GET' || req.method === 'HEAD') {
    serveStatic(req, res, url.pathname);
    return;
  }

  res.writeHead(405);
  res.end('Method not allowed');
});

const realtimeWss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  let url = null;
  try {
    url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  } catch (_) {
    socket.destroy();
    return;
  }
  if (url.pathname !== '/api/xai/realtime') {
    socket.destroy();
    return;
  }
  const origin = String(req.headers.origin || '').trim();
  if (!isOriginAllowed(origin)) {
    try {
      socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
    } catch (_) {}
    socket.destroy();
    return;
  }
  realtimeWss.handleUpgrade(req, socket, head, (ws) => {
    realtimeWss.emit('connection', ws, req);
  });
});

realtimeWss.on('connection', (clientWs, req) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (url.pathname !== '/api/xai/realtime') {
    try { clientWs.close(1008, 'Invalid path'); } catch (_) {}
    return;
  }

  const apiKey = process.env.XAI_API_KEY || process.env.OPENAI_API_KEY;
  if (!apiKey) {
    wsSendJson(clientWs, { type: 'session.error', error: 'Missing API key (set XAI_API_KEY in backend env).' });
    try { clientWs.close(1011, 'Missing API key'); } catch (_) {}
    return;
  }

  const requestedSessionId = oneLine(url.searchParams.get('sessionId') || '', 120);
  const requestedToken = oneLine(url.searchParams.get('token') || '', 180);
  let session = null;

  if (requestedSessionId) {
    const existing = realtimeSessions.get(requestedSessionId);
    if (existing) {
      if (!requestedToken || requestedToken !== existing.token) {
        wsSendJson(clientWs, { type: 'session.error', error: 'Unauthorized realtime session token.' });
        try { clientWs.close(1008, 'Unauthorized'); } catch (_) {}
        return;
      }
      session = existing;
    }
  }

  if (!session) {
    session = createRealtimeSession({ apiKey, initPayload: {} });
  }

  session.clients.add(clientWs);
  touchRealtimeSession(session);

  wsSendJson(clientWs, {
    type: 'session.created',
    sessionId: session.id,
    token: session.token,
    resumed: !!requestedSessionId && requestedSessionId === session.id
  });

  if (session.ready) {
    wsSendJson(clientWs, { type: 'session.ready', sessionId: session.id });
  } else if (session.readyPromise) {
    session.readyPromise
      .then(() => {
        if (clientWs.readyState === WebSocket.OPEN) {
          wsSendJson(clientWs, { type: 'session.ready', sessionId: session.id });
        }
      })
      .catch((err) => {
        wsSendJson(clientWs, {
          type: 'session.error',
          sessionId: session.id,
          error: err?.message || 'Failed to initialize realtime session'
        });
      });
  }

  clientWs.on('message', (raw) => {
    let payload = null;
    try {
      payload = JSON.parse(raw.toString());
    } catch (_) {
      wsSendJson(clientWs, { type: 'session.error', sessionId: session.id, error: 'Invalid JSON message.' });
      return;
    }
    handleRealtimeClientPayload(session, clientWs, payload);
  });

  clientWs.on('close', () => {
    session.clients.delete(clientWs);
    touchRealtimeSession(session);
  });

  clientWs.on('error', () => {
    session.clients.delete(clientWs);
    touchRealtimeSession(session);
  });
});

server.listen(PORT, HOST, () => {
  console.log(`WALLY server running: http://${HOST}:${PORT}`);
  startKeepaliveLoop();
});
