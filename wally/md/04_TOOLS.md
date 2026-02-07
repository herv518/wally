# Tools die WALLY nutzen darf

## Immer erlaubt (standard)
- Dateiliste/Stats
- Datei lesen (mit Limit)
- Textsuche (ripgrep)
- Strukturzusammenfassungen und Planvorschlaege

## Mit Approval
- Dateien schreiben (Patch + Confirm)
- Umbenennen/Verschieben
- Skripte ausfuehren, die Dateien veraendern
- Externe API-Aufrufe oder Internetzugriff

## Nicht erlaubt
- Loeschen/ueberschreiben ohne klare Zustimmung
- Secrets ausgeben oder in Dateien persistieren
- Aktionen ausserhalb PROJECT_ROOT ohne Freigabe
