#!/usr/bin/env python3
import os, json, time
from pathlib import Path
from dotenv import load_dotenv
import yaml

def is_excluded(path: Path, exclude_dirs: set[str]) -> bool:
    parts = set(path.parts)
    return any(d in parts for d in exclude_dirs)

def main():
    load_dotenv()
    root = Path(os.environ.get("PROJECT_ROOT", "")).expanduser()
    if not root.exists():
        raise SystemExit(f"PROJECT_ROOT existiert nicht: {root}")

    cfg = yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8"))
    include_ext = set(cfg["index"]["include_ext"])
    exclude_dirs = set(cfg["index"]["exclude_dirs"])
    max_bytes = int(cfg["index"]["max_file_mb"]) * 1024 * 1024

    out = Path("data/index.jsonl")
    out.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    n = 0
    skipped = 0
    with out.open("w", encoding="utf-8") as f:
        for p in root.rglob("*"):
            if p.is_dir():
                continue
            if is_excluded(p, exclude_dirs):
                continue
            if p.suffix.lower() not in include_ext:
                continue
            try:
                st = p.stat()
            except OSError:
                skipped += 1
                continue
            if st.st_size > max_bytes:
                skipped += 1
                continue
            rec = {"path": str(p), "size": st.st_size, "mtime": int(st.st_mtime), "ext": p.suffix.lower()}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1

    dt = time.time() - t0
    print(f"✅ Index geschrieben: {out} ({n} Dateien, {skipped} übersprungen) in {dt:.1f}s")

if __name__ == "__main__":
    main()
