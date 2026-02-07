# SECURITY 🔒 (WALLY)

## Grundprinzip
WALLY ist **dein lokaler Assistent**. Standard: **read-only**.

## Empfohlen
- `READ_ONLY=1` lassen, bis du WALLY wirklich vertraust.
- Projektzugriff einschränken über `PROJECT_ROOT`.
- Backups nutzen (Time Machine / Git).

## Approval-Flow
Wenn du später Schreibzugriff willst:
- WALLY erzeugt einen “Patch/Plan”
- du bestätigst
- erst dann wird geschrieben

## Offline
- BASE_URL muss localhost sein (oder LAN-only)
- macOS Firewall + optional LuLu/Little Snitch → nur localhost erlauben
