from __future__ import annotations
from pathlib import Path
from typing import Dict

def load_md_bundle(md_root: str) -> Dict[str, str]:
    root = Path(md_root)
    bundle: Dict[str, str] = {}
    for p in sorted(root.rglob("*.md")):
        rel = str(p.relative_to(root))
        bundle[rel] = p.read_text(encoding="utf-8", errors="ignore")
    return bundle
