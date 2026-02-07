/*
Run:
  XAI_API_KEY=your_key_here node backend/server.js

Optional:
  PORT=8080 node backend/server.js
  (or create backend/.env with XAI_API_KEY=your_key_here)

Purpose:
  - Serves frontend/widget.html on http://localhost:8080
  - Provides POST /api/xai/turn for stable xAI voice turns
*/

const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { spawn } = require('child_process');
const { WebSocket } = require('ws');
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
const PORT = Number(process.env.PORT || 8080);
const MAX_JSON_BYTES = 30 * 1024 * 1024;
const FFMPEG_BIN = process.env.FFMPEG_BIN || ffmpegStatic || '/opt/homebrew/bin/ffmpeg';
const ALLOWED_ORIGINS = String(process.env.ALLOWED_ORIGINS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const WALLY_SALES_SYSTEM = [
  'Du bist WALLY, ein Spezialagent fuer Autoverkauf und Flottenberatung in Deutschland.',
  'Antworte auf Deutsch, kurz, klar, umsetzbar.',
  'Prioritaet: Bedarf analysieren, passende Fahrzeuge empfehlen, Finanzierungsoptionen erklaeren,',
  'Einwaende behandeln, Abschluss naechsten Schritt definieren.',
  'Arbeite loesungsorientiert wie ein starker Autoverkaeufer-Coach.',
  'Nenne wenn sinnvoll konkrete Fragen, damit der Verkauf schneller zum Abschluss kommt.',
  'Keine Floskeln, keine Wiederholungen, maximal 2-4 kurze Saetze.'
].join(' ');
const MAX_MD_CONTEXT_CHARS = 12000;
const MAX_HISTORY_TURNS = 8;
const MAX_CATALOG_CONTEXT_CHARS = 7000;

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

function buildSessionInstructions(baseInstructions, history) {
  const base = (baseInstructions || 'Du bist WALLY. Antworte kurz, klar und auf Deutsch.').trim();
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
  const antiRepeat =
    'WICHTIG: Wiederhole nicht wortgleich die letzte Assistant-Antwort. Variiere Formulierung und liefere pro Antwort einen neuen konkreten Schritt. Wenn Audio unklar ist, sag das kurz und bitte um Wiederholung.';
  const mdSection = STATIC_MD_CONTEXT
    ? `Verbindliche WALLY-Konfiguration aus Markdown:\n${STATIC_MD_CONTEXT}`
    : '';
  if (!turns.length) {
    return [base, mdSection, antiRepeat].filter(Boolean).join('\n\n');
  }
  return [base, mdSection, `Bisheriger Verlauf:\n${turns.join('\n')}`, antiRepeat].filter(Boolean).join('\n\n');
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

function normalizedForCompare(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9aeiouäöüß\s]/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function lastAssistantText(history) {
  if (!Array.isArray(history)) return '';
  for (let i = history.length - 1; i >= 0; i--) {
    if (history[i]?.role !== 'assistant') continue;
    const t = normalizeContent(history[i]?.content);
    if (t) return t;
  }
  return '';
}

function isNearDuplicateReply(current, previous) {
  const a = normalizedForCompare(current);
  const b = normalizedForCompare(previous);
  if (!a || !b) return false;
  if (a === b) return true;
  if (a.length >= 24 && b.length >= 24 && (a.includes(b) || b.includes(a))) return true;
  return false;
}

function makeFallbackSalesReply(transcript) {
  const heard = normalizeContent(transcript);
  if (heard) {
    return `Verstanden: ${heard.slice(0, 90)}. Fuer den naechsten Verkaufsschritt: ist es Privatkunde oder Gewerbe/Fleet und welches Budget ist gesetzt?`;
  }
  return 'Ich habe dich akustisch nicht klar verstanden. Sag bitte in einem Satz Kunde, Fahrzeugwunsch und Budget.';
}

function serveStatic(req, res, pathname) {
  let clean = pathname;
  if (clean === '/') clean = '/widget.html';
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
          '-ar', '24000',
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

function runXaiRealtimeTurn({ apiKey, pcmBase64, history, instructions }) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket('wss://api.x.ai/v1/realtime', {
      headers: {
        Authorization: `Bearer ${apiKey}`
      }
    });

    let done = false;
    let text = '';
    let transcript = '';
    const audioChunks = [];
    const timer = setTimeout(() => {
      if (done) return;
      done = true;
      try { ws.close(); } catch (_) {}
      reject(new Error('xAI realtime timeout'));
    }, 24000);

    const finish = (err, payload) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      try { ws.close(); } catch (_) {}
      if (err) reject(err);
      else resolve(payload);
    };

    ws.on('open', () => {
      const sessionInstructions = buildSessionInstructions(instructions, history);
      ws.send(JSON.stringify({
        type: 'session.update',
        session: {
          voice: 'Ara',
          instructions: sessionInstructions,
          // Manual turn flow keeps behavior deterministic for buffered chunks.
          turn_detection: { type: null },
          audio: {
            input: { format: { type: 'audio/pcm', rate: 24000 } },
            output: { format: { type: 'audio/pcm', rate: 24000 } }
          }
        }
      }));

      // Keep turn creation minimal and deterministic for stability:
      // send one committed audio turn, then request response.

      ws.send(JSON.stringify({ type: 'input_audio_buffer.append', audio: pcmBase64 }));
      ws.send(JSON.stringify({ type: 'input_audio_buffer.commit' }));
      ws.send(JSON.stringify({
        type: 'response.create',
        response: {
          modalities: ['text', 'audio']
        }
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
        return;
      }
      if ((type === 'response.output_text.delta' || type === 'response.text.delta') && typeof msg.delta === 'string') {
        text += msg.delta;
        return;
      }
      if (type === 'response.output_audio_transcript.delta' && typeof msg.delta === 'string') {
        text += msg.delta;
        return;
      }
      if (type === 'response.output_audio_transcript.done' && typeof msg.transcript === 'string' && !text.trim()) {
        text = msg.transcript.trim();
        return;
      }
      if (type === 'response.output_audio.delta' && typeof msg.delta === 'string') {
        try {
          audioChunks.push(Buffer.from(msg.delta, 'base64'));
        } catch (_) {}
        return;
      }
      if (type === 'response.done') {
        const rawText = text.trim() || extractTextFromResponseDone(msg);
        const prevAssistant = lastAssistantText(history);
        let audio = audioChunks.length ? Buffer.concat(audioChunks).toString('base64') : '';
        let safeText = rawText || makeFallbackSalesReply(transcript);
        if (isNearDuplicateReply(safeText, prevAssistant)) {
          safeText = makeFallbackSalesReply(transcript);
          // Audio would no longer match the rewritten text.
          audio = '';
        }
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
    try {
      const body = await readJson(req);
      const key = process.env.XAI_API_KEY || process.env.OPENAI_API_KEY;
      if (!key) {
        sendJson(res, 400, { error: 'Missing API key (set XAI_API_KEY in backend env).' }, origin);
        return;
      }
      const audioBase64 = String(body.audioBase64 || '').trim();
      if (!audioBase64) {
        sendJson(res, 400, { error: 'Missing audioBase64.' }, origin);
        return;
      }
      const catalogContext = typeof body.catalogContext === 'string'
        ? body.catalogContext.slice(0, MAX_CATALOG_CONTEXT_CHARS)
        : '';
      const mergedInstructions = catalogContext
        ? `${WALLY_SALES_SYSTEM}\n\nAktuelle Fahrzeugdaten aus Key2Drive (verwende diese Daten bevorzugt):\n${catalogContext}`
        : WALLY_SALES_SYSTEM;

      const webmBuffer = Buffer.from(audioBase64, 'base64');
      const pcm = await convertWebmToPcm16(webmBuffer);
      const result = await runXaiRealtimeTurn({
        apiKey: key,
        pcmBase64: pcm.toString('base64'),
        history: Array.isArray(body.history) ? body.history : [],
        instructions: mergedInstructions
      });
      sendJson(res, 200, result, origin);
    } catch (err) {
      console.error('[xai-turn] error:', err?.message || err);
      sendJson(res, 500, { error: err?.message || 'Server error' }, origin);
    }
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/health') {
    sendJson(res, 200, {
      ok: true,
      service: 'wally-backend',
      ts: new Date().toISOString()
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

server.listen(PORT, () => {
  console.log(`WALLY server running: http://localhost:${PORT}`);
});
