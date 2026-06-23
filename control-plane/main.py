# control-plane/main.py
# HyperSpace AGI v1.02 — Control Plane + Dashboard
# feat: memory sync inter-nodo nell'heartbeat + smart task routing (tier/vram/load)
# feat: memory compression — gzip + TTL/max-entries pruning
# feat: OMEGA Obsidian bridge — /health + /mcp JSON-RPC 2.0
# fix: DB reload al boot, status recovery, endpoint dedup

from flask import Flask, request, jsonify, send_from_directory, Response, stream_with_context
import os, threading, time, requests, json, uuid, random, gzip
from datetime import datetime, timedelta, timezone
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, ".."))

import shared.db as db

app = Flask(__name__)

# CONFIG
NODE_ENDPOINTS     = [e.strip() for e in os.getenv("NODE_ENDPOINTS", "node:8084").split(",") if e.strip()]
OLLAMA_URL         = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434")
DEFAULT_MODEL      = os.getenv("OLLAMA_MODEL", "phi3")
INFERENCE_BACKEND  = os.getenv("INFERENCE_BACKEND", "ollama")
REGISTRY_URL       = os.getenv("REGISTRY_URL", "http://registry:8086")
_AUTHORITY_URL     = os.getenv("AUTHORITY_URL", "http://authority:8080")
_AUTHORITY_ENABLED = os.getenv("AUTHORITY_ENABLED", "false").lower() == "true"

# MEMORY CONFIG
MEMORY_FILE_GZ     = os.path.join(BASE_DIR, "memory.json.gz")
MEMORY_TTL_DAYS    = int(os.getenv("MEMORY_TTL_DAYS", "7"))
MEMORY_MAX_ENTRIES = int(os.getenv("MEMORY_MAX_ENTRIES", "200"))

# STATE
tasks: dict = {}
_nodes_by_id: dict  = {}
_known_endpoints: set = set()

_synced_memory_keys: set = set()

hb_state = {
    "cycle": 0, "last_tick": None, "last_conn": None,
    "last_dream": None, "last_chat": None, "last_memory_sync": None,
    "nodes_seen": [], "running": False,
}

advanced_config = {
    "ollama":     {"url": OLLAMA_URL, "defaultModel": DEFAULT_MODEL},
    "mesh":       {"nodeEndpoints": NODE_ENDPOINTS, "heartbeatEvery": 15},
    "_authority": {"serverUrl": _AUTHORITY_URL, "enabled": _AUTHORITY_ENABLED},
    "security":   {"sharedSecret": "", "secretRotatedAt": None},
}

db.init_db()

# HELPERS
def _normalize_endpoint(ep: str) -> str:
    """Bug 2 fix: aggiunge http:// se mancante, idempotente."""
    ep = ep.strip().rstrip("/")
    if not ep:
        return ep
    if ep.startswith("http://") or ep.startswith("https://"):
        return ep
    return f"http://{ep}"

def _ep_to_url(ep: str) -> str:
    """Alias di _normalize_endpoint per compatibilità."""
    return _normalize_endpoint(ep)

def _is_public_ep(ep):
    if ep.startswith("https://"): return True
    if ep.startswith("http://"):
        host = ep.split("//")[1].split(":")[0].split("/")[0]
        return "." in host and host not in ("localhost",)
    host = ep.split(":")[0]
    return "." in host

def _best_endpoint(node_info):
    ep = _normalize_endpoint(node_info.get("endpoint", ""))
    if ep.startswith("https://"): return ep
    public = _normalize_endpoint(node_info.get("public_endpoint", ""))
    if public and public.startswith("https://"): return public
    return ep

def _node_list():
    return list(_nodes_by_id.values())

# ── Bug 3 fix: ricarica nodi dal DB all'avvio ─────────────────────────────────
def _load_nodes_from_db():
    """
    Ricarica _nodes_by_id e _known_endpoints dal DB SQLite all'avvio.
    Senza questo, ogni restart del control-plane svuota la mesh in RAM
    e tutti i task falliscono finché i nodi non riannunciano (max 15s).
    """
    nodes = db.get_all_nodes()
    for n in nodes:
        nid = n.get("node_id", "")
        ep  = _normalize_endpoint(n.get("endpoint", ""))
        if not nid:
            continue
        n["endpoint"] = ep
        _nodes_by_id[nid] = n
        if ep:
            _known_endpoints.add(ep)
    # Aggiungi anche gli endpoint statici da config
    for ep in NODE_ENDPOINTS:
        _known_endpoints.add(_normalize_endpoint(ep))
    print(f"[CP] Loaded {len(_nodes_by_id)} nodes from DB, {len(_known_endpoints)} known endpoints")

# ── MEMORY: gzip + TTL/max-entries pruning ────────────────────────────────────

def _load_memory() -> list:
    if not os.path.exists(MEMORY_FILE_GZ):
        return []
    try:
        with gzip.open(MEMORY_FILE_GZ, "rt", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []

def _save_memory(entries: list) -> None:
    pruned = _prune_memory(entries)
    with gzip.open(MEMORY_FILE_GZ, "wt", encoding="utf-8") as f:
        json.dump(pruned, f, ensure_ascii=False)

def _prune_memory(entries: list) -> list:
    cutoff = datetime.now(timezone.utc) - timedelta(days=MEMORY_TTL_DAYS)
    fresh = []
    for e in entries:
        ts_str = e.get("ts") or e.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if ts >= cutoff:
                fresh.append(e)
        except Exception:
            fresh.append(e)

    def _ts_key(e):
        return e.get("ts") or e.get("timestamp", "")

    fresh.sort(key=_ts_key, reverse=True)
    return fresh[:MEMORY_MAX_ENTRIES]

# ── OMEGA BRIDGE — helpers ─────────────────────────────────────────────────────

def _omega_format_memories(entries: list) -> list:
    out = []
    for e in entries:
        out.append({
            "content":      str(e.get("content") or e.get("summary") or e.get("detail") or ""),
            "event_type":   str(e.get("type") or e.get("event_type") or "memory"),
            "created_at":   str(e.get("ts") or e.get("timestamp") or ""),
            "project":      e.get("node_id") or e.get("sourceNode") or None,
            "priority":     int(e.get("priority", 3)),
            "access_count": int(e.get("access_count", 0)),
            "status":       str(e.get("status") or "active"),
        })
    return out

def _omega_query(args: dict) -> str:
    query      = str(args.get("query", "")).lower()
    limit      = int(args.get("limit", 10))
    event_type = str(args.get("event_type", "")).lower()
    mode       = str(args.get("mode", "semantic"))
    entries    = _load_memory()
    results    = []
    for e in entries:
        content = str(e.get("content") or e.get("summary") or e.get("detail") or "").lower()
        etype   = str(e.get("type") or e.get("event_type") or "memory").lower()
        if event_type and event_type not in etype:
            continue
        if mode == "browse" or not query:
            results.append(e)
        else:
            if query in content:
                results.append(e)
    results = results[:limit]
    if not results:
        return "No memories found matching the query."
    lines = []
    for m in _omega_format_memories(results):
        lines.append(
            f"[{m['event_type']}] {m['created_at']}\n"
            f"{m['content'][:300]}\n"
            f"project: {m['project'] or 'hyperspace-agi'}\n---"
        )
    return "\n".join(lines)

def _omega_store(args: dict) -> str:
    content = str(args.get("content", "")).strip()
    if not content:
        return "Error: content is required."
    metadata   = args.get("metadata") or {}
    event_type = str(args.get("event_type") or metadata.get("event_type") or "vault_note")
    entry = {
        "ts":           datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "type":         event_type,
        "content":      content,
        "source":       str(metadata.get("source", "obsidian-vault")),
        "plugin":       str(metadata.get("plugin", "omega-memory")),
        "status":       "active",
        "priority":     int(metadata.get("priority", 3)),
        "access_count": 0,
    }
    entries = _load_memory()
    entries.append(entry)
    _save_memory(entries)
    push_log('memory_sync', 'OMEGA store: vault note ingested',
             detail=f'chars={len(content)} event_type={event_type}', status='success')
    return f"Stored: {content[:80]}..."

def _omega_reflect(args: dict) -> str:
    action  = str(args.get("action", "contradictions"))
    entries = _load_memory()
    if action != "contradictions" or len(entries) < 2:
        return "No contradictions detected."
    from collections import defaultdict
    groups: dict = defaultdict(list)
    for e in entries:
        words = str(e.get("content") or "").lower().split()[:3]
        key   = " ".join(words)
        if key:
            groups[key].append(e)
    contradictions = []
    for key, group in groups.items():
        if len(group) < 2:
            continue
        types = {str(g.get("type") or g.get("event_type", "")) for g in group}
        if len(types) > 1:
            a, b = group[0], group[1]
            contradictions.append(
                f"Potential contradiction on topic '{key}':\n"
                f"  A [{a.get('type','?')}]: {str(a.get('content',''))[:150]}\n"
                f"  B [{b.get('type','?')}]: {str(b.get('content',''))[:150]}\n---"
            )
    if not contradictions:
        return "No contradictions detected."
    return f"Found {len(contradictions)} potential contradiction(s):\n\n" + "\n".join(contradictions[:10])

def _omega_stats(args: dict) -> str:
    entries      = _load_memory()
    size_bytes   = os.path.getsize(MEMORY_FILE_GZ) if os.path.exists(MEMORY_FILE_GZ) else 0
    nodes_active = len([n for n in _node_list() if n.get("status") == "active"])
    return (
        f"memories: {len(entries)}\n"
        f"max_entries: {MEMORY_MAX_ENTRIES}\n"
        f"ttl_days: {MEMORY_TTL_DAYS}\n"
        f"file_size_kb: {round(size_bytes / 1024, 2)}\n"
        f"mesh_nodes_active: {nodes_active}\n"
        f"engine: hyperspace-agi v1.02"
    )

# ── OMEGA BRIDGE — endpoints ──────────────────────────────────────────────────

@app.route('/health')
def omega_health():
    entries      = _load_memory()
    nodes_active = len([n for n in _node_list() if n.get("status") == "active"])
    return jsonify({
        "status":       "ok",
        "engine":       "hyperspace-agi",
        "version":      "1.02",
        "memories":     len(entries),
        "nodes_active": nodes_active,
        "ttl_days":     MEMORY_TTL_DAYS,
        "ts":           datetime.utcnow().isoformat(timespec="seconds") + "Z",
    })

@app.route('/mcp', methods=['POST'])
def omega_mcp():
    payload = request.get_json(force=True, silent=True) or {}
    rpc_id  = payload.get("id", 1)

    def _ok(text: str):
        return jsonify({
            "jsonrpc": "2.0",
            "result":  {"content": [{"type": "text", "text": text}]},
            "id":      rpc_id,
        })

    def _err(msg: str, code: int = -32600):
        return jsonify({
            "jsonrpc": "2.0",
            "error":   {"code": code, "message": msg},
            "id":      rpc_id,
        }), 400

    method = payload.get("method", "")
    if method != "tools/call":
        return _err(f"Unsupported method: {method}")
    params    = payload.get("params") or {}
    tool_name = params.get("name", "")
    arguments = params.get("arguments") or {}
    if tool_name == "omega_call":
        inner_tool = str(arguments.get("tool", ""))
        inner_args = arguments.get("args") or {}
    else:
        inner_tool = tool_name
        inner_args = arguments
    _TOOLS = {
        "omega_query":   _omega_query,
        "omega_store":   _omega_store,
        "omega_reflect": _omega_reflect,
        "omega_stats":   _omega_stats,
    }
    handler = _TOOLS.get(inner_tool)
    if not handler:
        return _err(f"Unknown tool: {inner_tool}", -32601)
    try:
        return _ok(handler(inner_args))
    except Exception as exc:
        return _err(str(exc), -32603)

# ── SMART TASK ROUTING ────────────────────────────────────────────────────────
_TIER_SCORE = {"root": 3, "hub": 2, "leaf": 1}

def _node_score(node: dict) -> float:
    tier_s   = _TIER_SCORE.get(node.get("tier", "leaf"), 1) / 3.0
    vram_s   = min(float(node.get("vram_gb", 0)), 24.0) / 24.0
    peers_s  = min(int(node.get("peers_active", 0)), 10) / 10.0
    uptime_s = min(int(node.get("uptime_s", 0)), 604800) / 604800.0
    return tier_s * 0.40 + vram_s * 0.30 + peers_s * 0.20 + uptime_s * 0.10

def _select_best_node(active_nodes: list) -> dict:
    if not active_nodes:
        return None
    return max(active_nodes, key=_node_score)

# ── MODELLI ───────────────────────────────────────────────────────────────────
def _fetch_models():
    url     = advanced_config["ollama"]["url"].rstrip("/")
    backend = INFERENCE_BACKEND
    errors  = []
    try:
        r = requests.get(f"{url}/api/tags", timeout=4)
        if r.status_code == 200:
            data = r.json()
            if "models" in data:
                return {"ok": True, "backend": "ollama", "url": url,
                        "models": [m["name"] for m in data["models"] if m.get("name")]}
    except Exception as e:
        errors.append(f"ollama-style: {e}")
    try:
        r = requests.get(f"{url}/v1/models", timeout=4)
        if r.status_code == 200:
            data = r.json()
            if "data" in data:
                return {"ok": True, "backend": "lmstudio", "url": url,
                        "models": [m["id"] for m in data["data"] if m.get("id")]}
    except Exception as e:
        errors.append(f"lmstudio-style: {e}")
    return {"ok": False, "url": url, "backend": backend, "models": [], "errors": errors}

# LOG
LOG_TYPES = {"connection_test", "inter_node_message", "dream", "node_chat", "system", "mesh_event", "memory_sync"}

def push_log(type_, summary, detail="", source="control-plane", target="", status="info", trace_id=""):
    entry = {
        "id":         str(uuid.uuid4()),
        "ts":         datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "type":       type_ if type_ in LOG_TYPES else "system",
        "sourceNode": source,
        "targetNode": target,
        "status":     status,
        "traceId":    trace_id or str(uuid.uuid4())[:8],
        "summary":    summary,
        "detail":     detail,
    }
    db.insert_log(entry)
    return entry

@app.route('/logs')
def get_logs():
    tf  = request.args.get('type', '')
    sf  = request.args.get('status', '')
    nf  = request.args.get('node', '')
    q   = request.args.get('q', '').lower()
    page     = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 100))
    rows  = db.query_logs(type_=tf, status=sf, node=nf, q=q, page=page, per_page=per_page)
    total = db.count_logs(type_=tf, status=sf, node=nf, q=q)
    return jsonify({"logs": rows, "total": total, "page": page, "per_page": per_page})

@app.route('/logs/export')
def export_logs():
    tf  = request.args.get('type', '')
    sf  = request.args.get('status', '')
    nf  = request.args.get('node', '')
    q   = request.args.get('q', '')
    fmt = request.args.get('format', 'json').lower()
    rows = db.export_logs(type_=tf, status=sf, node=nf, q=q)
    if fmt == 'csv':
        import io, csv
        out = io.StringIO()
        if rows:
            writer = csv.DictWriter(out, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        return Response(
            out.getvalue(), mimetype='text/csv',
            headers={'Content-Disposition': 'attachment; filename=hyperspace_logs.csv'},
        )
    return Response(
        json.dumps(rows, indent=2), mimetype='application/json',
        headers={'Content-Disposition': 'attachment; filename=hyperspace_logs.json'},
    )

@app.route('/logs/add', methods=['POST'])
def add_log():
    data  = request.get_json(force=True, silent=True) or {}
    entry = push_log(
        type_=data.get('type', 'system'),
        summary=data.get('summary', ''),
        detail=data.get('detail', ''),
        source=data.get('sourceNode', 'unknown'),
        target=data.get('targetNode', ''),
        status=data.get('status', 'info'),
        trace_id=data.get('traceId', ''),
    )
    return jsonify(entry), 201

@app.route('/logs/clear', methods=['POST'])
def clear_logs():
    db.clear_logs()
    return jsonify({"ok": True})

# MESH
@app.route('/mesh/announce', methods=['POST'])
def mesh_announce():
    """
    Bug 2+5 fix:
    - normalizza endpoint con _normalize_endpoint() → no duplicati in _known_endpoints
    - forza status='active' su qualsiasi announce valido, anche se il nodo
      era precedentemente 'unreachable' nel DB
    """
    data = request.get_json(force=True, silent=True) or {}
    ep   = _normalize_endpoint(data.get("endpoint", ""))
    nid  = data.get("node_id", "")
    if not ep or not nid:
        return jsonify({"ok": False, "error": "missing endpoint or node_id"}), 400

    existing = _nodes_by_id.get(nid)
    should_update = True
    if existing:
        existing_ep = _normalize_endpoint(existing.get("endpoint", ""))
        if existing_ep.startswith("https://") and not ep.startswith("https://"):
            should_update = False

    if should_update:
        # Bug 5 fix: qualsiasi announce valido resetta lo status ad 'active'
        info = {
            **data,
            "endpoint":  ep,
            "status":    "active",
            "last_seen": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
        _nodes_by_id[nid] = info
        _known_endpoints.add(ep)
        db.upsert_node(info)

    push_log('mesh_event', f'Node announced: {nid[:12]}',
             f'endpoint={ep} accepted={should_update}', source=nid[:12], status='success')
    return jsonify({"ok": True, "registered": ep, "accepted": should_update})

@app.route('/mesh/nodes')
def get_mesh_nodes():
    return jsonify(_node_list())

@app.route('/mesh/node/<path:endpoint>/status')
def get_node_status(endpoint):
    try:
        r = requests.get(f"{_ep_to_url(endpoint)}/status", timeout=3)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 503

@app.route('/mesh/node/<path:endpoint>/peers')
def get_node_peers(endpoint):
    try:
        r = requests.get(f"{_ep_to_url(endpoint)}/peers", timeout=3)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 503

@app.route('/mesh/topology')
def mesh_topology():
    nodes_out = []
    edges_out = []
    seen_edges = set()
    for nid, node in _nodes_by_id.items():
        nodes_out.append({
            "id":           nid,
            "tier":         node.get("tier", "leaf"),
            "endpoint":     node.get("endpoint", ""),
            "peers_active": node.get("peers_active", 0),
            "uptime_s":     node.get("uptime_s", 0),
            "version":      node.get("version", ""),
            "status":       node.get("status", "active"),
            "score":        round(_node_score(node), 3),
        })
        try:
            ep = _best_endpoint(node)
            r  = requests.get(f"{ep}/peers", timeout=2)
            for peer in r.json().get("peers", []):
                pid = peer.get("node_id", "")
                if not pid or pid == nid:
                    continue
                edge_key = tuple(sorted([nid, pid]))
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    edges_out.append({
                        "source": nid, "target": pid,
                        "active": peer.get("status", "active") == "active",
                    })
        except Exception:
            pass
    return jsonify({"nodes": nodes_out, "edges": edges_out})

@app.route('/mesh/node/<path:endpoint>/pull', methods=['POST'])
def node_pull_model(endpoint):
    data  = request.get_json(force=True, silent=True) or {}
    model = data.get("model", advanced_config["ollama"]["defaultModel"])
    def generate():
        try:
            with requests.post(
                f"{_ep_to_url(endpoint)}/ollama/pull",
                json={"model": model}, stream=True, timeout=600,
            ) as resp:
                for line in resp.iter_lines():
                    if line:
                        yield f"{line.decode()}\n\n"
            yield 'data: {"status":"done"}\n\n'
        except Exception as e:
            yield f'data: {{"error": "{e}"}}\n\n'
    return Response(
        stream_with_context(generate()), mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

@app.route('/hb/status')
def hb_status():
    return jsonify(dict(hb_state))

# REGISTRY PROXY
@app.route('/registry/nodes')
def registry_nodes():
    try:
        r = requests.get(f"{REGISTRY_URL}/nodes", timeout=5)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e), "registry_url": REGISTRY_URL}), 503

@app.route('/registry/health')
def registry_health():
    try:
        r = requests.get(f"{REGISTRY_URL}/health", timeout=3)
        return jsonify({"ok": r.status_code == 200, "status": r.status_code})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 503

# CONFIG
@app.route('/config/advanced')
def get_advanced_config():
    safe = json.loads(json.dumps(advanced_config))
    if safe["security"]["sharedSecret"]:
        safe["security"]["sharedSecret"] = "***"
    return jsonify(safe)

@app.route('/config/advanced', methods=['POST'])
def set_advanced_config():
    global OLLAMA_URL, DEFAULT_MODEL
    data   = request.get_json(force=True, silent=True) or {}
    sec    = data.get('security', {})
    mesh   = data.get('mesh', {})
    ollama = data.get('ollama', {})
    auth   = data.get('_authority', {})
    if 'sharedSecret' in sec and sec['sharedSecret'] not in ('', '***'):
        advanced_config['security']['sharedSecret']   = sec['sharedSecret']
        advanced_config['security']['secretRotatedAt'] = datetime.utcnow().isoformat()
    if 'url' in ollama:
        advanced_config['ollama']['url'] = ollama['url']
        OLLAMA_URL = ollama['url']
    if 'defaultModel' in ollama:
        advanced_config['ollama']['defaultModel'] = ollama['defaultModel']
        DEFAULT_MODEL = ollama['defaultModel']
    if 'nodeEndpoints' in mesh:
        advanced_config['mesh']['nodeEndpoints'] = mesh['nodeEndpoints']
        for ep in mesh['nodeEndpoints']:
            _known_endpoints.add(_normalize_endpoint(ep))
    if 'serverUrl' in auth:
        advanced_config['_authority']['serverUrl'] = auth['serverUrl']
    if 'enabled' in auth:
        advanced_config['_authority']['enabled'] = bool(auth['enabled'])
    push_log('system', 'Config updated', json.dumps(data, default=str))
    return jsonify({"ok": True})

@app.route('/config/secret/rotate', methods=['POST'])
def rotate_secret():
    new_secret = str(uuid.uuid4()).replace('-', '')
    advanced_config['security']['sharedSecret']   = new_secret
    advanced_config['security']['secretRotatedAt'] = datetime.utcnow().isoformat()
    push_log('system', 'Shared secret rotated', status='success')
    return jsonify({"ok": True, "secret": new_secret,
                    "rotatedAt": advanced_config['security']['secretRotatedAt']})

@app.route('/models')
def list_models():
    return jsonify(_fetch_models())

@app.route('/ollama/status')
def ollama_status():
    result = _fetch_models()
    return jsonify({"ok": result["ok"], "url": result["url"], "models": result["models"]})

# TASKS
@app.route('/task/create', methods=['POST'])
def create_task():
    data    = request.get_json(force=True, silent=True) or {}
    task_id = data.get('task_id') or str(uuid.uuid4())[:8]
    prompt  = data.get('prompt', '')
    model   = data.get('model', advanced_config['ollama']['defaultModel'])
    task = {
        "id": task_id, "status": "created", "node": None,
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "payload": {"prompt": prompt, "model": model},
    }
    tasks[task_id] = task
    db.insert_task(task)
    push_log('system', f'Task created: {task_id}', detail=f'prompt={prompt[:80]}')
    return jsonify({"message": "Task created", "task_id": task_id}), 201

@app.route('/task/assign', methods=['POST'])
def assign_task():
    data    = request.get_json(force=True, silent=True) or {}
    task_id = data.get('task_id')
    if not task_id or task_id not in tasks:
        return jsonify({"error": "Task not found"}), 404
    active = [n for n in _node_list() if n.get("status") == "active"]
    if not active:
        return jsonify({"error": "No active nodes"}), 503
    selected = _select_best_node(active)
    endpoint = _best_endpoint(selected)
    node_id  = selected["node_id"]
    score    = round(_node_score(selected), 3)
    task = tasks[task_id]
    task["status"]        = "assigned"
    task["node"]          = node_id
    task["endpoint"]      = endpoint
    task["routing_score"] = score
    db.update_task(task_id, "assigned", node_id=node_id, endpoint=endpoint)
    tid = str(uuid.uuid4())[:8]
    push_log('inter_node_message', f'Task {task_id} -> {node_id[:12]}',
             f'endpoint={endpoint} score={score} tier={selected.get("tier","?")} vram={selected.get("vram_gb","?")}GB',
             target=node_id[:12], status='pending', trace_id=tid)
    try:
        r = requests.post(f"{endpoint}/execute", json=task, timeout=120)
        task["result"]       = r.json()
        task["status"]       = "done"
        task["completed_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        db.update_task(task_id, "done", result=json.dumps(task["result"]))
        push_log('inter_node_message', f'Task {task_id} done',
                 json.dumps(task.get("result", {})),
                 source=node_id[:12], target='control-plane', status='success', trace_id=tid)
    except Exception as e:
        task["status"] = "failed"
        task["error"]  = str(e)
        db.update_task(task_id, "failed", error=str(e))
        push_log('inter_node_message', f'Task {task_id} failed',
                 str(e), source=node_id[:12], status='failed', trace_id=tid)
        return jsonify({"error": str(e)}), 500
    return jsonify({"message": "done", "task": task})

@app.route('/tasks')
def get_tasks():
    return jsonify(tasks)

# ── MEMORY ENDPOINTS ──────────────────────────────────────────────────────────

@app.route('/memory')
def get_memory():
    limit   = int(request.args.get("limit", MEMORY_MAX_ENTRIES))
    entries = _load_memory()
    return jsonify({"entries": entries[:limit], "total": len(entries)})

@app.route('/memory/push', methods=['POST'])
def push_memory():
    data  = request.get_json(force=True, silent=True) or {}
    entry = data.get("entry")
    if not entry or not isinstance(entry, dict):
        return jsonify({"ok": False, "error": "missing entry"}), 400
    entries     = _load_memory()
    ts_key      = entry.get("ts") or entry.get("timestamp", "")
    content_key = str(entry.get("content", ""))[:64]
    dedup_key   = f"{ts_key}:{content_key}"
    existing_keys = {
        f"{e.get('ts') or e.get('timestamp','')}:{str(e.get('content',''))[:64]}"
        for e in entries
    }
    if dedup_key not in existing_keys:
        entries.append(entry)
        _save_memory(entries)
    return jsonify({"ok": True})

@app.route('/memory/stats')
def memory_stats():
    entries    = _load_memory()
    size_bytes = os.path.getsize(MEMORY_FILE_GZ) if os.path.exists(MEMORY_FILE_GZ) else 0
    return jsonify({
        "entries":         len(entries),
        "max_entries":     MEMORY_MAX_ENTRIES,
        "ttl_days":        MEMORY_TTL_DAYS,
        "file_size_bytes": size_bytes,
        "file_size_kb":    round(size_bytes / 1024, 2),
        "file":            MEMORY_FILE_GZ,
    })

# ── MEMORY SYNC ───────────────────────────────────────────────────────────────
def _sync_memory_across_nodes():
    active_nodes = [n for n in _node_list() if n.get("status") == "active"]
    if len(active_nodes) < 2:
        return
    node_memories: dict = {}
    for node in active_nodes:
        ep  = _best_endpoint(node)
        nid = node.get("node_id", "")
        try:
            r = requests.get(f"{ep}/memory", params={"limit": 30}, timeout=4)
            if r.status_code == 200:
                node_memories[nid] = r.json().get("entries", [])
        except Exception:
            pass
    if not node_memories:
        return
    pushed_total  = 0
    local_entries = _load_memory()
    local_changed = False
    for src_nid, entries in node_memories.items():
        for entry in entries:
            ts  = entry.get("ts") or entry.get("timestamp", "")
            key = f"{src_nid}:{ts}"
            if key in _synced_memory_keys:
                continue
            _synced_memory_keys.add(key)
            content_key = str(entry.get("content", ""))[:64]
            dedup_key   = f"{ts}:{content_key}"
            existing_k  = {
                f"{e.get('ts') or e.get('timestamp','')}:{str(e.get('content',''))[:64]}"
                for e in local_entries
            }
            if dedup_key not in existing_k:
                local_entries.append(entry)
                local_changed = True
            for dst_node in active_nodes:
                dst_nid = dst_node.get("node_id", "")
                if dst_nid == src_nid:
                    continue
                dst_ep = _best_endpoint(dst_node)
                try:
                    requests.post(
                        f"{dst_ep}/memory/push",
                        json={"node_id": src_nid, "entry": entry},
                        timeout=4,
                    )
                    pushed_total += 1
                except Exception:
                    pass
    if local_changed:
        _save_memory(local_entries)
    if pushed_total > 0:
        push_log(
            'memory_sync',
            f'Memory sync: {pushed_total} entries propagate su {len(active_nodes)} nodi',
            detail=f'nodi={[n.get("node_id","")[:12] for n in active_nodes]}',
            status='success',
        )

# HEARTBEAT
DREAM_PHRASES = [
    "autonomous planning cycle initiated", "memory consolidation phase started",
    "sub-task decomposition in progress", "latent space exploration #{}",
    "tool-use reflection completed", "goal re-prioritization triggered",
    "associative memory update: {} new links", "dream cycle #{} - context window cleared",
]
CHAT_PHRASES = [
    ("can you handle a summarize task?", "yes, {} slots free"),
    ("what is your current model?", "running {}"),
    ("sync memory snapshot?", "snapshot ready - {} KB"),
    ("queue depth?", "depth {} - capacity normal"),
    ("ready for next task?", "ready, latency {}ms"),
]

def _poll_mesh_nodes():
    """
    Bug 5 fix: quando il ping torna OK su un nodo precedentemente 'unreachable',
    forza status='active' sia in RAM (_nodes_by_id) che nel DB.
    """
    for ep in list(_known_endpoints):
        try:
            r = requests.get(f"{ep}/status", timeout=3)
            if r.status_code == 200:
                info = r.json()
                nid  = info.get("node_id", "")
                info["endpoint"]  = ep
                info["status"]    = "active"   # Bug 5: forza active sul recovery
                info["last_seen"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                if nid:
                    existing = _nodes_by_id.get(nid)
                    if not existing or \
                       not _normalize_endpoint(existing.get("endpoint", "")).startswith("https://") or \
                       ep.startswith("https://"):
                        _nodes_by_id[nid] = info
                        db.upsert_node(info)   # Bug 5: sovrascrive 'unreachable' nel DB
                try:
                    rp = requests.get(f"{ep}/peers", timeout=2)
                    for peer in rp.json().get("peers", []):
                        pep = _normalize_endpoint(peer.get("endpoint", ""))
                        if pep and pep not in _known_endpoints:
                            _known_endpoints.add(pep)   # Bug 2: già normalizzato
                except Exception:
                    pass
        except Exception:
            for nid, n in _nodes_by_id.items():
                if _normalize_endpoint(n.get("endpoint", "")) == ep:
                    _nodes_by_id[nid]["status"] = "unreachable"
                    db.upsert_node({**_nodes_by_id[nid], "status": "unreachable"})

def heartbeat_loop():
    time.sleep(3)
    push_log('system', 'Control-plane v1.02 started',
             detail=f'nodes_loaded={len(_nodes_by_id)} endpoints={list(_known_endpoints)}',
             status='info')
    hb_state["running"] = True
    while True:
        cycle = hb_state["cycle"] + 1
        hb_state["cycle"]     = cycle
        hb_state["last_tick"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        _poll_mesh_nodes()
        hb_state["nodes_seen"] = [
            n.get("node_id", n.get("endpoint", "?"))[:12]
            for n in _node_list() if n.get("status") == "active"
        ]
        if cycle % 2 == 0:
            _sync_memory_across_nodes()
            hb_state["last_memory_sync"] = hb_state["last_tick"]
        for node in _node_list():
            if node.get("status") != "active":
                continue
            nid = node.get("node_id", node.get("endpoint", "unknown"))[:12]
            ep  = _best_endpoint(node)
            tid = str(uuid.uuid4())[:8]
            try:
                t0  = time.time()
                requests.get(f"{ep}/health", timeout=2)
                lat = int((time.time() - t0) * 1000)
                push_log('connection_test', f'HB#{cycle} ping OK -> {nid}',
                         f'latency: {lat}ms | endpoint: {ep} | score: {round(_node_score(node),3)}',
                         source='control-plane', target=nid, status='success', trace_id=tid)
                hb_state["last_conn"] = hb_state["last_tick"]
            except Exception as e:
                push_log('connection_test', f'HB#{cycle} ping FAILED -> {nid}',
                         str(e), source='control-plane', target=nid, status='failed', trace_id=tid)
        if cycle % 3 == 0:
            pool = [n.get("node_id", "node-sim")[:16] for n in _node_list()] or ["node-sim"]
            nid  = random.choice(pool)
            push_log('dream', f'{nid}: {random.choice(DREAM_PHRASES).format(random.randint(1, 99))}',
                     f'cycle={cycle}', source=nid, status='info')
            hb_state["last_dream"] = hb_state["last_tick"]
        if cycle % 5 == 0:
            real_ids  = [n.get("node_id", "")[:16] for n in _node_list() if n.get("node_id")]
            sim_pool  = ["node-sim-A", "node-sim-B", "node-sim-C"]
            pool      = list(dict.fromkeys(real_ids + sim_pool))
            if len(pool) < 2:
                pool = ["node-sim-A", "node-sim-B"]
            src, dst  = random.sample(pool, 2)
            q, a_tpl  = random.choice(CHAT_PHRASES)
            answer    = a_tpl.format(random.randint(1, 8))
            tid       = str(uuid.uuid4())[:8]
            push_log('node_chat', f'{src} -> {dst}: "{q}"', f'cycle={cycle}',
                     source=src, target=dst, status='info', trace_id=tid)
            push_log('node_chat', f'{dst} -> {src}: "{answer}"', 'reply',
                     source=dst, target=src, status='info', trace_id=tid)
            hb_state["last_chat"] = hb_state["last_tick"]
        time.sleep(15)

# DASHBOARD
@app.route('/')
def dashboard():
    return send_from_directory(BASE_DIR, 'dashboard.html')

@app.route('/dashboard')
def dashboard_alias():
    return send_from_directory(BASE_DIR, 'dashboard.html')

# STARTUP
# Bug 3 fix: _load_nodes_from_db() prima del thread heartbeat
if __name__ == '__main__':
    _load_nodes_from_db()
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=8085, debug=False)
else:
    _load_nodes_from_db()
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()
