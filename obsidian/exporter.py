#!/usr/bin/env python3
# obsidian/exporter.py
# HyperSpace AGI v1.02 — Memory Graph Exporter
# Reads memory.json.gz from CP and writes Obsidian-compatible .md notes into the vault.
# Runs as a sidecar container. Vault is shared via Docker volume.

import os, gzip, json, time, hashlib, re
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify
import threading

CP_URL        = os.getenv("CP_URL", "http://control-plane:8085")
VAULT_DIR     = Path(os.getenv("VAULT_DIR", "/vault"))
EXPORT_EVERY  = int(os.getenv("EXPORT_EVERY", "60"))   # secondi tra ogni export
MEMORY_LIMIT  = int(os.getenv("MEMORY_LIMIT", "200"))
PORT          = int(os.getenv("EXPORTER_PORT", "8090"))

app = Flask(__name__)
_stats = {"last_run": None, "notes_written": 0, "total_entries": 0, "errors": []}


def _sanitize_filename(s: str) -> str:
    """Rende sicuro un nome file rimuovendo caratteri non validi."""
    s = re.sub(r'[\\/:*?"<>|]', '_', s)
    return s[:80].strip()


def _entry_to_slug(entry: dict) -> str:
    """ID univoco per ogni entry basato su ts + content hash."""
    ts      = entry.get("ts") or entry.get("timestamp", "unknown")
    content = str(entry.get("content") or entry.get("summary") or entry.get("detail") or "")
    h       = hashlib.md5(content.encode()).hexdigest()[:8]
    date    = ts[:10] if len(ts) >= 10 else "nodate"
    return f"{date}_{h}"


def _entry_type_folder(entry: dict) -> str:
    """Mappa type → sottocartella del vault."""
    t = str(entry.get("type") or entry.get("event_type") or "misc").lower()
    mapping = {
        "webui_prompt":      "prompts",
        "webui_response":    "responses",
        "memory_sync":       "sync",
        "connection_test":   "mesh",
        "inter_node_message":"mesh",
        "mesh_event":        "mesh",
        "vault_note":        "vault",
        "system":            "system",
    }
    return mapping.get(t, "misc")


def _wikilinks_for(entry: dict, all_entries: list) -> list:
    """Trova entry correlate per task_id, node_id, model."""
    links = []
    task_id = entry.get("task_id", "")
    node_id = (entry.get("node_id") or entry.get("sourceNode") or "")[:12]
    model   = entry.get("model", "")
    for other in all_entries:
        if other is entry:
            continue
        other_slug = _entry_to_slug(other)
        # collega se stesso task_id
        if task_id and other.get("task_id") == task_id:
            links.append(other_slug)
        # collega stesso nodo (max 3)
        elif node_id and (other.get("node_id") or other.get("sourceNode") or "")[:12] == node_id:
            links.append(other_slug)
            if len(links) >= 3:
                break
    return list(dict.fromkeys(links))[:5]  # dedup, max 5


def _write_note(entry: dict, all_entries: list) -> Path:
    """Scrive un file .md nel vault per questa entry."""
    slug    = _entry_to_slug(entry)
    folder  = _entry_type_folder(entry)
    out_dir = VAULT_DIR / folder
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{slug}.md"

    ts      = entry.get("ts") or entry.get("timestamp", "")
    etype   = str(entry.get("type") or entry.get("event_type") or "misc")
    content = str(entry.get("content") or entry.get("summary") or entry.get("detail") or "")
    node_id = (entry.get("node_id") or entry.get("sourceNode") or "unknown")[:16]
    model   = entry.get("model", "")
    task_id = entry.get("task_id", "")
    status  = entry.get("status", "active")
    source  = entry.get("source", "")
    priority = entry.get("priority", 3)

    links = _wikilinks_for(entry, all_entries)
    wikilinks_block = "\n".join([f"- [[{l}]]" for l in links]) if links else "_nessun collegamento_"
    tags = [etype, folder, node_id.replace(" ", "-")]
    if model:
        tags.append(model.replace(":", "-"))

    md = f"""---
ts: {ts}
type: {etype}
node_id: {node_id}
model: {model}
task_id: {task_id}
status: {status}
source: {source}
priority: {priority}
tags: [{', '.join(tags)}]
---

# {_sanitize_filename(content[:60] or slug)}

**Tipo:** `{etype}`  
**Nodo:** `{node_id}`  
**Modello:** `{model or 'n/a'}`  
**Task:** `{task_id or 'n/a'}`  
**Timestamp:** `{ts}`  
**Status:** `{status}`  
**Priority:** {priority}  

## Contenuto

{content}

## Relazioni

{wikilinks_block}

---
*Generato da HyperSpace AGI memory-exporter — {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC*
"""
    path.write_text(md, encoding="utf-8")
    return path


def _write_index(all_entries: list, written: int):
    """Scrive il nodo indice principale MOC (Map of Content)."""
    index_path = VAULT_DIR / "INDEX.md"
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')

    # raggruppa per tipo
    by_type: dict = {}
    for e in all_entries:
        t = _entry_type_folder(e)
        by_type.setdefault(t, []).append(e)

    sections = []
    for folder, entries in sorted(by_type.items()):
        items = []
        for e in entries[:20]:  # max 20 per sezione nell'indice
            slug = _entry_to_slug(e)
            ts   = (e.get("ts") or "")[:19]
            snip = str(e.get("content") or "")[:60].replace("\n", " ")
            items.append(f"- [[{folder}/{slug}]] — `{ts}` {snip}")
        sections.append(f"### {folder.upper()} ({len(entries)})\n" + "\n".join(items))

    md = f"""---
tags: [index, moc, hyperspace-agi]
---

# HyperSpace AGI — Memory Map

> Generato automaticamente da `memory-exporter`  
> Ultimo aggiornamento: **{now}**  
> Entry totali: **{len(all_entries)}** | Note scritte: **{written}**

## Navigazione

- [[prompts/]] — Prompt WebUI
- [[responses/]] — Risposte Ollama
- [[mesh/]] — Traffico inter-nodo
- [[sync/]] — Memory sync
- [[vault/]] — Note Obsidian OMEGA
- [[system/]] — Eventi di sistema

## Entry recenti

{chr(10).join(sections)}
"""
    index_path.write_text(md, encoding="utf-8")


def export_loop():
    import requests as req
    while True:
        try:
            r = req.get(f"{CP_URL}/memory", params={"limit": MEMORY_LIMIT}, timeout=10)
            if r.status_code == 200:
                entries = r.json().get("entries", [])
                written = 0
                for entry in entries:
                    try:
                        _write_note(entry, entries)
                        written += 1
                    except Exception as e:
                        _stats["errors"].append(str(e))
                _write_index(entries, written)
                _stats["last_run"]      = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                _stats["notes_written"] = written
                _stats["total_entries"] = len(entries)
                _stats["errors"]        = _stats["errors"][-10:]  # keep last 10 errors
                print(f"[EXPORTER] {written} notes written to vault | {len(entries)} entries total")
            else:
                print(f"[EXPORTER] CP returned {r.status_code}")
        except Exception as e:
            print(f"[EXPORTER] Error: {e}")
            _stats["errors"].append(str(e))
        time.sleep(EXPORT_EVERY)


@app.route('/health')
def health():
    return jsonify({"status": "ok", "vault": str(VAULT_DIR), **_stats})


@app.route('/stats')
def stats():
    return jsonify(_stats)


if __name__ == '__main__':
    VAULT_DIR.mkdir(parents=True, exist_ok=True)
    t = threading.Thread(target=export_loop, daemon=True)
    t.start()
    print(f"[EXPORTER] Started — vault={VAULT_DIR} cp={CP_URL} every={EXPORT_EVERY}s port={PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=False)
