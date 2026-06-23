# memory-graph/exporter.py
# HyperSpace AGI v1.02 -- Memory Graph Exporter
# Legge memory.json.gz dal Control Plane (/memory)
# e scrive file .md nel vault Obsidian montato su /vault
# Ogni entry diventa una nota con tag, wikilinks e frontmatter YAML.
# API:
#   GET  /status        stato exporter + ultimo export
#   GET  /export        esegue export immediato e ritorna stats
#   POST /export/force  idem via POST

import os, time, threading, json, re
from datetime import datetime
from flask import Flask, jsonify
import requests

app = Flask(__name__)

CP_URL          = os.getenv("CP_URL", "http://control-plane:8085")
VAULT_PATH      = os.getenv("VAULT_PATH", "/vault")
EXPORT_INTERVAL = int(os.getenv("EXPORT_INTERVAL", "30"))

state = {
    "last_export":    None,
    "last_count":     0,
    "last_error":     None,
    "total_exports":  0,
    "running":        False,
}


def _slugify(text: str) -> str:
    text = str(text).lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_-]+', '-', text)
    return text[:80]


def _entry_to_md(entry: dict) -> tuple[str, str]:
    """Ritorna (filename, markdown_content) per una memory entry."""
    ts         = entry.get("ts") or entry.get("timestamp", "")
    etype      = entry.get("type") or entry.get("event_type") or "memory"
    content    = str(entry.get("content") or entry.get("summary") or entry.get("detail") or "")
    node_id    = entry.get("node_id") or entry.get("sourceNode") or entry.get("source") or ""
    model      = entry.get("model", "")
    task_id    = entry.get("task_id", "")
    priority   = entry.get("priority", 3)
    status     = entry.get("status", "active")

    # filename: tipo + timestamp slug
    ts_slug  = _slugify(ts[:19]) if ts else _slugify(str(time.time()))
    filename = f"{etype}_{ts_slug}.md"

    # wikilinks: node, model, task
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
        f"# [{etype}] {ts[:19]}\n\n"
        f"{content}\n\n"
        + (f"## Links\n{links_section}\n" if links else "")
    )
    return filename, md


def _write_index(entries: list, vault: str):
    """Scrive HyperSpace-Index.md con tabella riassuntiva di tutte le entry."""
    lines = [
        "# HyperSpace AGI — Memory Index",
        "",
        f"_Last updated: {datetime.utcnow().isoformat(timespec='seconds')}Z_",
        f"_Total entries: {len(entries)}_",
        "",
        "| ts | type | node | summary |",
        "|---|---|---|---|",
    ]
    for e in sorted(entries, key=lambda x: x.get("ts") or "", reverse=True)[:200]:
        ts      = (e.get("ts") or "")[:19]
        etype   = e.get("type") or e.get("event_type") or "memory"
        node    = (e.get("node_id") or e.get("source") or "")[:16]
        summary = str(e.get("content") or e.get("summary") or "")[:60].replace("|", "-")
        lines.append(f"| {ts} | {etype} | {node} | {summary} |")

    index_path = os.path.join(vault, "HyperSpace-Index.md")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def do_export() -> dict:
    """Scarica la memoria dal CP e scrive i file nel vault. Ritorna stats."""
    try:
        r = requests.get(f"{CP_URL}/memory", params={"limit": 500}, timeout=10)
        r.raise_for_status()
        entries = r.json().get("entries", [])
    except Exception as e:
        state["last_error"] = str(e)
        return {"ok": False, "error": str(e)}

    os.makedirs(VAULT_PATH, exist_ok=True)

    # Crea sottocartelle per tipo
    written = 0
    skipped = 0
    for entry in entries:
        etype    = entry.get("type") or entry.get("event_type") or "memory"
        sub_dir  = os.path.join(VAULT_PATH, etype)
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

    state["last_export"]  = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    state["last_count"]   = len(entries)
    state["last_error"]   = None
    state["total_exports"] += 1

    return {
        "ok":      True,
        "total":   len(entries),
        "written": written,
        "skipped": skipped,
        "vault":   VAULT_PATH,
    }


def export_loop():
    state["running"] = True
    time.sleep(5)  # attendi CP
    while True:
        do_export()
        time.sleep(EXPORT_INTERVAL)


# API
@app.route("/status")
def status():
    return jsonify({
        "service":         "memory-graph",
        "version":         "1.02",
        "vault":           VAULT_PATH,
        "cp_url":          CP_URL,
        "export_interval": EXPORT_INTERVAL,
        **state,
    })


@app.route("/export", methods=["GET"])
@app.route("/export/force", methods=["POST"])
def force_export():
    result = do_export()
    return jsonify(result), 200 if result["ok"] else 500


if __name__ == "__main__":
    t = threading.Thread(target=export_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=8090, debug=False)
