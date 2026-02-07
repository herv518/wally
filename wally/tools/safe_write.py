from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

def propose_write(path: str, new_content: str) -> Dict[str, Any]:
    p = Path(path).expanduser()
    old = ""
    if p.exists():
        old = p.read_text(encoding="utf-8", errors="ignore")
    return {
        "action": "write",
        "path": str(p),
        "old_preview": old[:2000],
        "new_preview": new_content[:2000],
        "note": "Preview only. Use apply_write() after approval."
    }

def apply_write(path: str, new_content: str, read_only: bool=True) -> Dict[str, Any]:
    if read_only:
        return {"error": "READ_ONLY=1. Schreibaktion blockiert. Setze READ_ONLY=0 und bestätige erneut."}
    p = Path(path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(new_content, encoding="utf-8")
    return {"ok": True, "path": str(p), "bytes": len(new_content.encode('utf-8'))}
