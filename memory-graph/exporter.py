# memory-graph/exporter.py
# HyperSpace AGI v1.02 -- Memory Graph Exporter + LLM Titler
# Legge memory dal CP, genera titoli via LLM (qwen2:0.5b default),
# scrive .md nel vault Obsidian con frontmatter YAML e wikilinks.
#
# Env vars:
#   CP_URL, VAULT_PATH, EXPORT_INTERVAL
#   TITLER_ENABLED    true/false (default: true)
#   TITLER_URL        URL Ollama o LM Studio (default: http://host.docker.internal:1234)
#   TITLER_MODEL      modello da usare (default: qwen2:0.5b)
#   TITLER_BACKEND    ollama | lmstudio (default: ollama)
#
# API:
#   GET  /status
#   GET  /export
#   POST /export/force

import os, time, threading, json, re, hashlib
from datetime import datetime
from flask import Flask, jsonify
import requests

app = Flask(__name__)

CP_URL           = os.getenv("CP_URL", "http://control-plane:8085")
VAULT_PATH       = os.getenv("VAULT_PATH", "/vault")
EXPORT_INTERVAL  = int(os.getenv("EXPORT_INTERVAL", "30"))
TITLER_ENABLED   = os.getenv("TITLER_ENABLED", "true").lower() == "true"
TITLER_URL       = os.getenv("TITLER_URL", "http://host.docker.internal:11434")
TITLER_MODEL     = os.getenv("TITLER_MODEL", "qwen2:0.5b")
TITLER_BACKEND   = os.getenv("TITLER_BACKEND", "ollama")  # ollama | lmstudio

TITLE_CACHE_PATH = os.path.join(VAULT_PATH, ".title_cache.json")
_title_cache: dict = {}

state = {
    "last_export":   None,
    "last_count":    0,
    "last_error":    None,
    "total_exports": 0,
    "titles_generated": 0,
    "running":       False,
}


# ── Titolatore LLM ────────────────────────────────────────────────────────────

def _load_title_cache():
    global _title_cache
    try:
        if os.path.exists(TITLE_CACHE_PATH):
            with open(TITLE_CACHE_PATH, "r") as f:
                _title_cache = json.load(f)
    except Exception:
        _title_cache = {}


def _save_title_cache():
    try:
        os.makedirs(VAULT_PATH, exist_ok=True)
        with open(TITLE_CACHE_PATH, "w") as f:
            json.dump(_title_cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _entry_hash(entry: dict) -> str:
    key = str(entry.get("ts", "")) + str(entry.get("content", ""))[:100]
    return hashlib.md5(key.encode()).hexdigest()[:12]


def _call_llm_title(etype: str, content: str) -> str | None:
    prompt = (
        f"Genera un titolo di massimo 6 parole per questa memoria di un agente IA.\n"
        f"Rispondi SOLO con il titolo, niente altro.\n\n"
        f"Tipo: {etype}\n"
        f"Contenuto: {content[:300]}"
    )
    try:
        if TITLER_BACKEND == "lmstudio":
            r = requests.post(
                f"{TITLER_URL}/v1/chat/completions",
                json={
                    "model": TITLER_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 20,
                    "temperature": 0.3,
                },
                timeout=8,
            )
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"].strip().strip('"')
        else:  # ollama
            r = requests.post(
                f"{TITLER_URL}/api/generate",
                json={
                    "model": TITLER_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"num_predict": 20, "temperature": 0.3},
                },
                timeout=8,
            )
            r.raise_for_status()
            return r.json().get("response", "").strip().strip('"')
    except Exception:
        return None


def _get_title(entry: dict) -> str:
    etype   = entry.get("type") or entry.get("event_type") or "memory"
    content = str(entry.get("content") or entry.get("summary") or entry.get("detail") or "")
    ts      = (entry.get("ts") or "")[:16]

    if not TITLER_ENABLED or not content.strip():
        return f"[{etype}] {ts}"

    h = _entry_hash(entry)
    if h in _title_cache:
        return _title_cache[h]

    title = _call_llm_title(etype, content)
    if not title or len(title) > 80:
        title = f"[{etype}] {ts}"

    _title_cache[h] = title
    state["titles_generated"] += 1
    _save_title_cache()
    return title


# ── Helpers ───────────────────────────────────────────────────────────────────

def _slugify(text: str) -> str:
    text = str(text).lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_-]+', '-', text)
    return text[:80]


def _entry_to_md(entry: dict) -> tuple[str, str]:
    ts       = entry.get("ts") or entry.get("timestamp", "")
    etype    = entry.get("type") or entry.get("event_type") or "memory"
    content  = str(entry.get("content") or entry.get("summary") or entry.get("detail") or "")
    node_id  = entry.get("node_id") or entry.get("sourceNode") or entry.get("source") or ""
    model    = entry.get("model", "")
    task_id  = entry.get("task_id", "")
    priority = entry.get("priority", 3)
    status   = entry.get("status", "active")

    title    = _get_title(entry)
    ts_slug  = _slugify(ts[:19]) if ts else _slugify(str(time.time()))
    filename = f"{etype}_{ts_slug}.md"

    links = []
    if node_id:
        links.append(f"[[node/{node_id[:16]}]]")
    if model:
        links.append(f"[[model/{model}]]")
    if task_id:
        links.append(f"[[task/{task_id}]]")

    tags = [etype]
    if node_id:
        tags.append(f"node-{_slugify(node_id[:12])}")
    if status:
        tags.append(status)

    frontmatter = (
        f"---\n"
        f"title: \"{title}\"\n"
        f"type: {etype}\n"
        f"ts: {ts}\n"
        f"node: {node_id}\n"
        f"model: {model}\n"
        f"task_id: {task_id}\n"
        f"priority: {priority}\n"
        f"status: {status}\n"
        f"tags: [{', '.join(tags)}]\n"
        f"---\n"
    )

    links_section = "\n".join(links)
    md = (
        f"{frontmatter}\n"
        f"# {title}\n\n"
        f"> `{etype}` — {ts[:19]} — `{node_id[:16]}`\n\n"
        f"{content}\n\n"
        + (f"## Links\n{links_section}\n" if links else "")
    )
    return filename, md


def _write_index(entries: list, vault: str):
    lines = [
        "# 🧠 HyperSpace AGI — Memory Index",
        "",
        f"_Last updated: {datetime.utcnow().isoformat(timespec='seconds')}Z_",
        f"_Total entries: {len(entries)} | Titler: {'ON' if TITLER_ENABLED else 'OFF'} ({TITLER_MODEL})_",
        "",
        "| ts | type | node | title |",
        "|---|---|---|---|",
    ]
    for e in sorted(entries, key=lambda x: x.get("ts") or "", reverse=True)[:200]:
        ts    = (e.get("ts") or "")[:19]
        etype = e.get("type") or e.get("event_type") or "memory"
        node  = (e.get("node_id") or e.get("source") or "")[:16]
        h     = _entry_hash(e)
        title = _title_cache.get(h, f"[{etype}] {ts}").replace("|", "-")
        lines.append(f"| {ts} | {etype} | {node} | {title} |")

    with open(os.path.join(vault, "HyperSpace-Index.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ── Export ────────────────────────────────────────────────────────────────────

def do_export() -> dict:
    try:
        r = requests.get(f"{CP_URL}/memory", params={"limit": 500}, timeout=10)
        r.raise_for_status()
        entries = r.json().get("entries", [])
    except Exception as e:
        state["last_error"] = str(e)
        return {"ok": False, "error": str(e)}

    os.makedirs(VAULT_PATH, exist_ok=True)
    _load_title_cache()

    written = skipped = 0
    for entry in entries:
        etype   = entry.get("type") or entry.get("event_type") or "memory"
        sub_dir = os.path.join(VAULT_PATH, etype)
        os.makedirs(sub_dir, exist_ok=True)
        filename, md = _entry_to_md(entry)
        fpath = os.path.join(sub_dir, filename)
        if not os.path.exists(fpath):
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(md)
            written += 1
        else:
            skipped += 1

    _write_index(entries, VAULT_PATH)

    state.update({
        "last_export":  datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "last_count":   len(entries),
        "last_error":   None,
        "total_exports": state["total_exports"] + 1,
    })
    return {"ok": True, "total": len(entries), "written": written, "skipped": skipped}


def export_loop():
    state["running"] = True
    time.sleep(5)
    while True:
        do_export()
        time.sleep(EXPORT_INTERVAL)


# ── API ───────────────────────────────────────────────────────────────────────

@app.route("/status")
def status():
    return jsonify({
        "service": "memory-graph", "version": "1.02",
        "vault": VAULT_PATH, "cp_url": CP_URL,
        "export_interval": EXPORT_INTERVAL,
        "titler": {"enabled": TITLER_ENABLED, "model": TITLER_MODEL,
                   "backend": TITLER_BACKEND, "url": TITLER_URL},
        **state,
    })


@app.route("/export", methods=["GET"])
@app.route("/export/force", methods=["POST"])
def force_export():
    result = do_export()
    return jsonify(result), 200 if result["ok"] else 500


if __name__ == "__main__":
    threading.Thread(target=export_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8090, debug=False)
