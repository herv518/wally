from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

def list_indexed_files(index_path: str, ext_filter: Optional[str]=None, min_size_bytes: int=0, limit: int=50) -> Dict[str, Any]:
    p = Path(index_path)
    if not p.exists():
        return {"error": f"Index nicht gefunden: {p}"}
    results: List[Dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            if ext_filter and rec.get("ext") != ext_filter.lower():
                continue
            if rec.get("size", 0) < min_size_bytes:
                continue
            results.append(rec)
            if len(results) >= limit:
                break
    return {"count": len(results), "results": results}

def read_text_file(path: str, max_chars: int=14000) -> Dict[str, Any]:
    p = Path(path).expanduser()
    if not p.exists():
        return {"error": f"Datei nicht gefunden: {p}"}
    text = p.read_text(encoding="utf-8", errors="ignore")
    if len(text) > max_chars:
        text = text[:max_chars] + "\n\n[...gekürzt...]"
    return {"path": str(p), "content": text}

def file_stats(path: str) -> Dict[str, Any]:
    p = Path(path).expanduser()
    if not p.exists():
        return {"error": f"Datei nicht gefunden: {p}"}
    st = p.stat()
    return {"path": str(p), "size": st.st_size, "mtime": int(st.st_mtime), "suffix": p.suffix.lower()}
