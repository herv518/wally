#!/usr/bin/env python3
import os, json
from pathlib import Path
from typing import Dict, List, Any

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
import yaml

from openai import OpenAI

from tools.md_loader import load_md_bundle
from tools.fs_tools import list_indexed_files, read_text_file, file_stats
from tools.grep_tool import ripgrep_search
from tools.safe_write import propose_write, apply_write

console = Console()

def load_cfg() -> dict:
    return yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8"))

def build_system_prompt(md_bundle: Dict[str, str]) -> str:
    parts = ["WALLY ist ein lokaler, persönlicher Assistant. Er nutzt folgende Markdown-Konfiguration:"]
    for k, v in md_bundle.items():
        parts.append(f"\n---\n# {k}\n{v.strip()}\n")
    parts.append("\nWICHTIG: Halte dich an 02_RULES.md. Standard: read-only. Erst planen, dann ausführen.")
    return "\n".join(parts)

def llm_client() -> OpenAI:
    return OpenAI(base_url=os.environ["BASE_URL"], api_key=os.environ.get("OPENAI_API_KEY", "local"))

def chat(messages: List[Dict[str, str]], model: str, timeout: int) -> str:
    client = llm_client()
    resp = client.chat.completions.create(model=model, messages=messages, timeout=timeout)
    return resp.choices[0].message.content or ""

def main():
    load_dotenv()
    cfg = load_cfg()

    model = os.environ.get("MODEL", "local-model")
    timeout = int(os.environ.get("TIMEOUT", "120"))
    project_root = os.environ.get("PROJECT_ROOT", str(Path.home()))
    read_only = os.environ.get("READ_ONLY", "1").strip() != "0"

    md_bundle = load_md_bundle("md")
    system_prompt = build_system_prompt(md_bundle)

    history: List[Dict[str, str]] = [{"role":"system","content":system_prompt}]

    console.print(Panel.fit(
        f"WALLY 🤖✨ (local)\nPROJECT_ROOT: {project_root}\nREAD_ONLY: {read_only}\nTippe 'help' für Beispiele, 'quit' zum Beenden.",
        title="Ready"
    ))

    index_path = Path("data/index.jsonl")

    while True:
        user = Prompt.ask("\n[bold]du[/bold]")
        if user.strip().lower() in {"quit","exit"}:
            break
        if user.strip().lower() == "help":
            console.print(Panel(
                "- 'index': Index erstellen (oder scripts/index_project.py nutzen)\n"
                "- 'liste': Liste Dateien aus Index\n"
                "- 'grep': Textsuche in PROJECT_ROOT\n"
                "- 'read': Datei lesen\n"
                "- 'stats': Datei-Stats\n"
                "- 'write-propose': Schreibvorschlag (Preview)\n"
                "- 'write-apply': Schreibaktion (nur wenn READ_ONLY=0)\n",
                title="Commands"
            ))
            continue

        low = user.strip().lower()
        tool_result: Any = None

        try:
            if low == "index":
                tool_result = {"hint":"Bitte ausführen: python scripts/index_project.py"}
            elif low == "liste":
                if not index_path.exists():
                    tool_result = {"error":"Kein Index gefunden. Erst: python scripts/index_project.py"}
                else:
                    ext = Prompt.ask("Filter extension? (.html/.css/leer)", default="")
                    min_kb = int(Prompt.ask("Min size KB?", default="0"))
                    tool_result = list_indexed_files(str(index_path), ext_filter=ext.strip() or None, min_size_bytes=min_kb*1024, limit=50)
            elif low == "grep":
                pattern = Prompt.ask("Pattern (regex ok)")
                tool_result = ripgrep_search(pattern=pattern, root=project_root, max_results=200)
            elif low == "read":
                path = Prompt.ask("Dateipfad")
                tool_result = read_text_file(path, max_chars=cfg["assistant"]["max_context_chars"])
            elif low == "stats":
                path = Prompt.ask("Dateipfad")
                tool_result = file_stats(path)
            elif low == "write-propose":
                path = Prompt.ask("Ziel-Dateipfad")
                console.print("Paste neuen Inhalt. Ende mit einer Zeile nur: EOF")
                lines = []
                while True:
                    line = input()
                    if line.strip() == "EOF":
                        break
                    lines.append(line)
                tool_result = propose_write(path, "\n".join(lines))
            elif low == "write-apply":
                path = Prompt.ask("Ziel-Dateipfad")
                console.print("Paste neuen Inhalt. Ende mit einer Zeile nur: EOF")
                lines = []
                while True:
                    line = input()
                    if line.strip() == "EOF":
                        break
                    lines.append(line)
                tool_result = apply_write(path, "\n".join(lines), read_only=read_only)
            else:
                tool_result = None

        except Exception as e:
            tool_result = {"error": str(e)}

        history.append({"role":"user","content":user})
        if tool_result is not None:
            history.append({"role":"user","content":"[TOOL_RESULT]\n"+json.dumps(tool_result, ensure_ascii=False)[:cfg["assistant"]["tool_result_char_limit"]]})

        answer = chat(history, model=model, timeout=timeout)
        console.print(Panel(answer, title="WALLY"))

if __name__ == "__main__":
    main()
