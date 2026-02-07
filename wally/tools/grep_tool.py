from __future__ import annotations
import subprocess
from typing import Any, Dict

def ripgrep_search(pattern: str, root: str, max_results: int=200) -> Dict[str, Any]:
    cmd = ["rg", "--line-number", "--no-heading", "--smart-case", "--max-count", str(max_results), pattern, root]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        return {"error": "ripgrep (rg) nicht installiert. Installiere: brew install ripgrep"}
    out = proc.stdout.strip().splitlines()
    return {"pattern": pattern, "root": root, "matches": out[:max_results], "exit_code": proc.returncode}
