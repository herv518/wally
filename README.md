# WALLY

WALLY ist jetzt in zwei klar getrennte Teile aufgeteilt:

- `frontend/`: schlankes Voice-Widget mit nur dem WALLY-Kopf (`frontend/widget.html`)
- `backend/`: Node.js API/Proxy fuer xAI Realtime Voice (`backend/server.js`)

Der bestehende Python-Agent bleibt unveraendert in `wally/`.

## Struktur

- `frontend/widget.html` -> Widget-UI (WALLY-Kopf, Start/Stop, API-Key)
- `backend/server.js` -> statische Auslieferung des Frontends + `POST /api/xai/turn`
- `backend/package.json` -> Backend-Abhaengigkeiten (`ws`) und Startskript
- `server.js` -> Root-Entry, delegiert an `backend/server.js`
- `wally/` -> CLI-Agent mit Markdown-Steuerung und lokalen Tools

## Quickstart Widget + Backend

```bash
cd ~/Desktop/WALLY
cd backend
npm install
XAI_API_KEY=dein_key npm start
```

Alternativ per Datei:

```bash
cd ~/Desktop/WALLY/backend
printf "XAI_API_KEY=dein_key\n" > .env
npm start
```

Dann im Browser:

```text
http://localhost:8080
```

## API

- `POST /api/xai/turn`
- Erwartet JSON mit `audioBase64` (webm), optional `apiKey`, `history`, `instructions`
- Antwort: `{ text, transcript, audioBase64 }`

## Python-CLI (optional, separater Teil)

Der lokale CLI-Agent liegt unter `wally/` und nutzt:

- Markdown-Konfig in `wally/md/`
- Tools in `wally/tools/`
- Indexing via `wally/scripts/index_project.py`

## Sicherheit

- Empfohlen: API-Key nur im Backend setzen (`XAI_API_KEY`), nicht im Frontend-Code.
- Optionaler Widget-Key bleibt nur in der Browser-Session (Override).
- Python-Agent bleibt standardmaessig read-only (`READ_ONLY=1`).
