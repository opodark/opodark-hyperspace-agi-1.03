from flask import Flask, request, jsonify, render_template_string
import os
import threading
import time
import requests
import json
import uuid
import random
from datetime import datetime

app = Flask(__name__)

# ----------------------------
# IN-MEMORY STATE
# ----------------------------
tasks = {}
event_logs = []
advanced_config = {
    "security": {
        "sharedSecret": "",
        "secretRotatedAt": None,
    },
    "authority": {
        "serverUrl": os.getenv("AUTHORITY_URL", "http://authority:8080"),
        "enabled": True,
        "authMode": "none",
    },
    "mesh": {
        "enabled": False,
        "mhtEnabled": False,
        "bootstrapPeers": [],
    },
    "ollama": {
        "url": os.getenv("OLLAMA_URL", "http://ollama:11434"),
        "defaultModel": os.getenv("OLLAMA_MODEL", "phi3"),
    },
}

AUTHORITY_URL = advanced_config["authority"]["serverUrl"]
OLLAMA_URL    = advanced_config["ollama"]["url"]
LOG_LIMIT = 500

hb_state = {
    "cycle":      0,
    "last_tick":  None,
    "last_conn":  None,
    "last_dream": None,
    "last_chat":  None,
    "nodes_seen": [],
    "running":    False,
}

# ----------------------------
# LOG HELPERS
# ----------------------------
LOG_TYPES = {
    "connection_test", "inter_node_message", "dream",
    "node_chat", "authority_event", "system"
}

def push_log(type_, summary, detail="",
             source="control-plane", target="",
             status="info", trace_id=""):
    global event_logs
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
    event_logs.append(entry)
    if len(event_logs) > LOG_LIMIT:
        event_logs = event_logs[-LOG_LIMIT:]
    return entry


# ----------------------------
# LOG API
# ----------------------------
@app.route('/logs', methods=['GET'])
def get_logs():
    type_filter   = request.args.get('type', '')
    status_filter = request.args.get('status', '')
    node_filter   = request.args.get('node', '')
    search        = request.args.get('q', '').lower()
    result = event_logs[:]
    if type_filter:
        result = [l for l in result if l['type'] == type_filter]
    if status_filter:
        result = [l for l in result if l['status'] == status_filter]
    if node_filter:
        result = [l for l in result if
                  node_filter in l['sourceNode'] or node_filter in l['targetNode']]
    if search:
        result = [l for l in result if
                  search in l['summary'].lower() or search in l['detail'].lower()]
    return jsonify(list(reversed(result[-200:])))

@app.route('/logs/add', methods=['POST'])
def add_log():
    data = request.get_json(force=True, silent=True) or {}
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
    global event_logs
    event_logs = []
    return jsonify({"ok": True})


# ----------------------------
# HEARTBEAT STATUS API
# ----------------------------
@app.route('/hb/status', methods=['GET'])
def hb_status():
    return jsonify(dict(hb_state))


# ----------------------------
# ADVANCED CONFIG API
# ----------------------------
@app.route('/config/advanced', methods=['GET'])
def get_advanced_config():
    safe = json.loads(json.dumps(advanced_config))
    if safe["security"]["sharedSecret"]:
        safe["security"]["sharedSecret"] = "***"
    return jsonify(safe)

@app.route('/config/advanced', methods=['POST'])
def set_advanced_config():
    global advanced_config, AUTHORITY_URL, OLLAMA_URL
    data   = request.get_json(force=True, silent=True) or {}
    sec    = data.get('security', {})
    auth   = data.get('authority', {})
    mesh   = data.get('mesh', {})
    ollama = data.get('ollama', {})

    if 'sharedSecret' in sec and sec['sharedSecret'] not in ('', '***'):
        advanced_config['security']['sharedSecret']   = sec['sharedSecret']
        advanced_config['security']['secretRotatedAt'] = datetime.utcnow().isoformat()
    if 'serverUrl' in auth:
        advanced_config['authority']['serverUrl'] = auth['serverUrl']
        AUTHORITY_URL = auth['serverUrl']
    if 'enabled'  in auth: advanced_config['authority']['enabled']  = bool(auth['enabled'])
    if 'authMode' in auth: advanced_config['authority']['authMode'] = auth['authMode']
    if 'mhtEnabled'     in mesh: advanced_config['mesh']['mhtEnabled']     = bool(mesh['mhtEnabled'])
    if 'bootstrapPeers' in mesh: advanced_config['mesh']['bootstrapPeers'] = mesh['bootstrapPeers']
    if 'enabled'        in mesh: advanced_config['mesh']['enabled']         = bool(mesh['enabled'])
    if 'url'          in ollama:
        advanced_config['ollama']['url'] = ollama['url']
        OLLAMA_URL = ollama['url']
    if 'defaultModel' in ollama:
        advanced_config['ollama']['defaultModel'] = ollama['defaultModel']

    push_log('authority_event', 'Advanced config updated',
             json.dumps(data, default=str), status='info')
    return jsonify({"ok": True})

@app.route('/config/authority/test', methods=['POST'])
def test_authority():
    url = advanced_config['authority']['serverUrl']
    try:
        r = requests.get(f"{url}/nodes", timeout=3)
        push_log('connection_test', f'Authority reachability OK ({url})',
                 f'HTTP {r.status_code}', target='authority', status='success')
        return jsonify({"ok": True, "status": r.status_code})
    except Exception as e:
        push_log('connection_test', f'Authority unreachable ({url})',
                 str(e), target='authority', status='failed')
        return jsonify({"ok": False, "error": str(e)}), 503

@app.route('/config/secret/rotate', methods=['POST'])
def rotate_secret():
    new_secret = str(uuid.uuid4()).replace('-', '')
    advanced_config['security']['sharedSecret']   = new_secret
    advanced_config['security']['secretRotatedAt'] = datetime.utcnow().isoformat()
    push_log('authority_event', 'Shared secret rotated', status='success')
    return jsonify({"ok": True, "secret": new_secret,
                    "rotatedAt": advanced_config['security']['secretRotatedAt']})


# ----------------------------
# OLLAMA STATUS API
# ----------------------------
@app.route('/ollama/status', methods=['GET'])
def ollama_status():
    url = advanced_config['ollama']['url']
    try:
        r = requests.get(f"{url}/api/tags", timeout=3)
        models = [m['name'] for m in r.json().get('models', [])]
        return jsonify({"ok": True, "url": url, "models": models})
    except Exception as e:
        return jsonify({"ok": False, "url": url, "error": str(e)})


# ----------------------------
# TASK API
# ----------------------------
@app.route('/task/create', methods=['POST'])
def create_task():
    data    = request.get_json(force=True, silent=True) or {}
    task_id = data.get('task_id') or request.form.get('task_id')
    prompt  = data.get('prompt', '')
    model   = data.get('model', advanced_config['ollama']['defaultModel'])
    if not task_id:
        return jsonify({"error": "Missing task_id"}), 400
    tasks[task_id] = {
        "id": task_id, "status": "created",
        "worker": None,
        "payload": {"prompt": prompt, "model": model},
    }
    push_log('system', f'Task created: {task_id}',
             detail=f'prompt={prompt[:80]}', status='info')
    return jsonify({"message": "Task created", "task_id": task_id}), 201

@app.route('/task/assign', methods=['POST'])
def assign_task():
    data    = request.get_json(force=True, silent=True) or {}
    task_id = data.get('task_id') or request.form.get('task_id')
    if not task_id:
        return jsonify({"error": "Missing task_id"}), 400
    if task_id not in tasks:
        return jsonify({"error": "Task not found"}), 404
    try:
        res   = requests.get(f"{AUTHORITY_URL}/nodes", timeout=3)
        nodes = res.json()
    except Exception as e:
        push_log('connection_test', 'Authority call failed during task assign',
                 str(e), target='authority', status='failed')
        return jsonify({"error": f"authority error: {str(e)}"}), 500
    node_list    = nodes.values() if isinstance(nodes, dict) else nodes
    active_nodes = [n for n in node_list if n.get("status") == "active"]
    if not active_nodes:
        return jsonify({"error": "No active nodes"}), 404
    selected  = active_nodes[0]
    worker_id = selected["node_id"]
    task      = tasks[task_id]
    task["status"] = "assigned"
    task["worker"] = worker_id
    tid = str(uuid.uuid4())[:8]
    push_log('inter_node_message', f'Task {task_id} dispatched to {worker_id}',
             json.dumps(task), source='control-plane', target=worker_id,
             status='pending', trace_id=tid)
    try:
        worker_url      = f"http://{worker_id}:8084/execute"
        worker_response = requests.post(worker_url, json=task, timeout=120)
        task["result"]  = worker_response.json()
        push_log('inter_node_message', f'Task {task_id} completed on {worker_id}',
                 json.dumps(task.get("result", {})),
                 source=worker_id, target='control-plane',
                 status='success', trace_id=tid)
    except Exception as e:
        push_log('inter_node_message', f'Execution failed on {worker_id}',
                 str(e), source=worker_id, target='control-plane',
                 status='failed', trace_id=tid)
        return jsonify({"error": f"execution failed: {str(e)}"}), 500
    return jsonify({"message": "Task assigned and executed", "task": task})

@app.route('/tasks')
def get_tasks():
    return jsonify(tasks)


# ----------------------------
# HEARTBEAT LOOP
# ciclo % 3 == 0  → dream su nodo casuale
# ciclo % 5 == 0  → node_chat tra due nodi
# ogni ciclo      → connection_test authority + ogni nodo attivo
# ----------------------------
DREAM_PHRASES = [
    "autonomous planning cycle initiated",
    "memory consolidation phase started",
    "sub-task decomposition in progress",
    "latent space exploration #{}",
    "tool-use reflection completed",
    "goal re-prioritization triggered",
    "associative memory update: {} new links",
    "dream cycle #{} — context window cleared",
    "semantic embedding refresh triggered",
    "long-term memory write #{} completed",
]

CHAT_PHRASES = [
    ("can you handle a summarize task?",  "yes, {} slots free"),
    ("what is your current model?",        "running {}"),
    ("sync memory snapshot?",              "snapshot ready — {} KB"),
    ("queue depth?",                       "depth {} — capacity normal"),
    ("ready for next task?",               "ready, latency {}ms"),
    ("resource usage?",                    "cpu {}% — within limits"),
    ("can you accept a classification task?", "affirmative, priority slot {} open"),
]

def _get_active_nodes():
    try:
        res  = requests.get(f"{AUTHORITY_URL}/nodes", timeout=2)
        nodes = res.json()
        lst  = list(nodes.values()) if isinstance(nodes, dict) else nodes
        return [n for n in lst if n.get("status") == "active"]
    except Exception:
        return []

def heartbeat_loop():
    global hb_state
    time.sleep(3)
    push_log('system', 'Control-plane v1.01 started',
             detail='port=8085 | authority=' + advanced_config['authority']['serverUrl'],
             status='info')
    hb_state["running"] = True

    while True:
        cycle = hb_state["cycle"] + 1
        hb_state["cycle"]     = cycle
        hb_state["last_tick"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        active = _get_active_nodes()
        hb_state["nodes_seen"] = [n.get("node_id", "?") for n in active]

        # --- connection_test: authority ---
        try:
            r = requests.get(f"{AUTHORITY_URL}/health", timeout=2)
            push_log('connection_test',
                     f'HB#{cycle} authority health OK',
                     f'HTTP {r.status_code} | nodes_active={len(active)}',
                     target='authority', status='success')
            hb_state["last_conn"] = hb_state["last_tick"]
        except Exception as e:
            push_log('connection_test',
                     f'HB#{cycle} authority unreachable',
                     str(e), target='authority', status='failed')

        # --- connection_test: ogni nodo attivo ---
        for node in active:
            nid  = node.get("node_id", "unknown")
            port = node.get("port", 8084)
            tid  = str(uuid.uuid4())[:8]
            try:
                t0 = time.time()
                requests.get(f"http://{nid}:{port}/status", timeout=2)
                lat = int((time.time() - t0) * 1000)
                push_log('connection_test',
                         f'HB#{cycle} ping OK → {nid}',
                         f'latency: {lat}ms | port: {port}',
                         source='control-plane', target=nid,
                         status='success', trace_id=tid)
                hb_state["last_conn"] = hb_state["last_tick"]
            except Exception as e:
                push_log('connection_test',
                         f'HB#{cycle} ping FAILED → {nid}',
                         str(e),
                         source='control-plane', target=nid,
                         status='failed', trace_id=tid)

        # --- ogni 3 cicli: dream ---
        if cycle % 3 == 0:
            if active:
                node   = random.choice(active)
                nid    = node.get("node_id", "node-unknown")
                phrase = random.choice(DREAM_PHRASES).format(random.randint(1, 99))
                push_log('dream',
                         f'{nid}: {phrase}',
                         f'cycle={cycle} | ts={hb_state["last_tick"]}',
                         source=nid, status='info')
            else:
                phrase = random.choice(DREAM_PHRASES).format(cycle)
                push_log('dream',
                         f'node-sim: {phrase}',
                         f'simulated — no active nodes at cycle {cycle}',
                         source='node-sim', status='info')
            hb_state["last_dream"] = hb_state["last_tick"]

        # --- ogni 5 cicli: node_chat ---
        if cycle % 5 == 0:
            nodes_pool = [n.get("node_id") for n in active] if active else ["node-alpha", "node-beta"]
            if len(nodes_pool) < 2:
                nodes_pool = nodes_pool + ["node-sim"]
            src, dst = random.sample(nodes_pool, 2)
            q, a_tpl = random.choice(CHAT_PHRASES)
            answer   = a_tpl.format(random.randint(1, 8))
            tid      = str(uuid.uuid4())[:8]
            push_log('node_chat',
                     f'{src} → {dst}: "{q}"',
                     f'context: negotiation | cycle={cycle}',
                     source=src, target=dst, status='info', trace_id=tid)
            push_log('node_chat',
                     f'{dst} → {src}: "{answer}"',
                     f'reply to trace {tid}',
                     source=dst, target=src, status='info', trace_id=tid)
            hb_state["last_chat"] = hb_state["last_tick"]

        time.sleep(10)


# ----------------------------
# DASHBOARD HTML
# ----------------------------
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="it" data-theme="dark">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>HyperSpace AGI v1.01</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root,[data-theme="dark"]{
  --bg:#0b0c0e;--surface:#131518;--surface2:#191c21;--surface3:#21252d;
  --border:#272b33;--divider:#1c1f26;
  --text:#c8cdd6;--text-muted:#636a78;--text-faint:#353a44;
  --primary:#4f98a3;--primary-h:#2d7d8a;--primary-bg:rgba(79,152,163,.12);
  --success:#6daa45;--success-bg:rgba(109,170,69,.12);
  --warning:#e8af34;--warning-bg:rgba(232,175,52,.10);
  --error:#dd6974;--error-bg:rgba(221,105,116,.12);
  --info:#5591c7;--info-bg:rgba(85,145,199,.12);
  --dream:#a86fdf;--dream-bg:rgba(168,111,223,.12);
  --chat:#fdab43;--chat-bg:rgba(253,171,67,.10);
  --font-mono:'JetBrains Mono',monospace;
  --font-body:'Inter',sans-serif;
  --radius:5px;--radius-lg:9px;--radius-xl:13px;
  --tr:160ms cubic-bezier(.16,1,.3,1);
}
[data-theme="light"]{
  --bg:#f0f1f4;--surface:#fff;--surface2:#f6f7fa;--surface3:#eaecf0;
  --border:#d8dbe2;--divider:#e4e6eb;
  --text:#191c22;--text-muted:#636a78;--text-faint:#b0b8c8;
  --primary:#016970;--primary-h:#014f55;--primary-bg:rgba(1,105,112,.08);
  --success:#3a6e1a;--success-bg:rgba(58,110,26,.08);
  --warning:#9a6800;--warning-bg:rgba(154,104,0,.08);
  --error:#a12c3a;--error-bg:rgba(161,44,58,.08);
  --info:#225f99;--info-bg:rgba(34,95,153,.08);
  --dream:#6b30b5;--dream-bg:rgba(107,48,181,.08);
  --chat:#b56200;--chat-bg:rgba(181,98,0,.08);
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html{font-size:13px;-webkit-font-smoothing:antialiased}
body{font-family:var(--font-body);background:var(--bg);color:var(--text);min-height:100vh;display:grid;grid-template-rows:auto auto 1fr}

/* ── HEADER ── */
header{
  background:var(--surface);border-bottom:1px solid var(--border);
  padding:0 20px;display:flex;align-items:center;gap:12px;
  height:50px;position:sticky;top:0;z-index:200;
}
.logo{display:flex;align-items:center;gap:9px;font-family:var(--font-mono);font-weight:700;font-size:.9rem;color:var(--primary);white-space:nowrap}
.vbadge{font-size:.58rem;font-weight:700;background:var(--primary-bg);color:var(--primary);padding:2px 6px;border-radius:99px;letter-spacing:.06em}
nav{display:flex;gap:1px;margin-left:12px}
nav button{background:none;border:none;cursor:pointer;padding:5px 13px;border-radius:var(--radius);font-size:.78rem;font-weight:500;color:var(--text-muted);transition:color var(--tr),background var(--tr);font-family:var(--font-body)}
nav button.active{background:var(--primary-bg);color:var(--primary)}
nav button:hover:not(.active){background:var(--surface3);color:var(--text)}
.hdr-right{margin-left:auto;display:flex;align-items:center;gap:10px}
.ollama-pill{display:flex;align-items:center;gap:5px;background:var(--surface2);border:1px solid var(--border);border-radius:99px;padding:3px 10px;font-size:.7rem;font-family:var(--font-mono);cursor:pointer;transition:border-color var(--tr)}
.ollama-pill:hover{border-color:var(--primary)}
.ollama-dot{width:6px;height:6px;border-radius:50%;background:var(--text-faint);flex-shrink:0}
.ollama-dot.ok{background:var(--success);box-shadow:0 0 4px var(--success)}
.ollama-dot.err{background:var(--error)}
.clock{font-family:var(--font-mono);font-size:.68rem;color:var(--text-muted)}
#themeBtn{background:none;border:1px solid var(--border);cursor:pointer;padding:5px 7px;border-radius:var(--radius);color:var(--text-muted);line-height:1;transition:border-color var(--tr),color var(--tr)}
#themeBtn:hover{border-color:var(--text-muted);color:var(--text)}

/* ── HB BAR ── */
#hbBar{
  background:var(--surface2);border-bottom:1px solid var(--border);
  padding:5px 20px;display:flex;gap:20px;align-items:center;flex-wrap:wrap;
  font-size:.65rem;font-family:var(--font-mono);color:var(--text-muted);
}
.hb-item{display:flex;align-items:center;gap:5px}
.hb-dot{width:5px;height:5px;border-radius:50%;background:var(--text-faint)}
.hb-dot.ok{background:var(--success)}.hb-dot.err{background:var(--error)}
.hb-label{color:var(--text-faint);text-transform:uppercase;letter-spacing:.06em;margin-right:2px}
.hb-val{color:var(--text)}
@keyframes hbpulse{0%,100%{opacity:1}50%{opacity:.25}}
.hb-live{animation:hbpulse 2s infinite;color:var(--success)}

/* ── LAYOUT ── */
main{padding:18px 20px;display:grid}
.panel{display:none;flex-direction:column;gap:14px;animation:fadein .18s ease}
.panel.active{display:flex}
@keyframes fadein{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
.sec-title{font-size:.65rem;font-weight:700;letter-spacing:.11em;text-transform:uppercase;color:var(--text-muted);padding-bottom:8px;border-bottom:1px solid var(--divider)}
.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:15px}
.card-title{font-size:.75rem;font-weight:600;color:var(--text-muted);margin-bottom:11px;display:flex;align-items:center;gap:7px}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}

/* ── BUTTONS ── */
.btn{border:none;cursor:pointer;padding:7px 15px;border-radius:var(--radius);font-size:.78rem;font-weight:500;transition:background var(--tr),color var(--tr);font-family:var(--font-body);white-space:nowrap}
.btn-primary{background:var(--primary);color:#fff}.btn-primary:hover{background:var(--primary-h)}
.btn-ghost{background:var(--surface3);color:var(--text)}.btn-ghost:hover{background:var(--border)}
.btn-danger{background:var(--error-bg);color:var(--error)}.btn-danger:hover{background:var(--error);color:#fff}
.btn-warn{background:var(--warning-bg);color:var(--warning)}.btn-warn:hover{background:var(--warning);color:#000}
.btn-success{background:var(--success-bg);color:var(--success)}.btn-success:hover{background:var(--success);color:#fff}
.btn-dream{background:var(--dream-bg);color:var(--dream)}.btn-dream:hover{background:var(--dream);color:#fff}
.btn-chat{background:var(--chat-bg);color:var(--chat)}.btn-chat:hover{background:var(--chat);color:#000}
.btn-sm{padding:4px 10px;font-size:.72rem}

/* ── INPUTS ── */
.inp{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:7px 11px;color:var(--text);font-size:.8rem;font-family:var(--font-body);transition:border-color var(--tr);width:100%}
.inp:focus{outline:none;border-color:var(--primary)}
.inp-mono{font-family:var(--font-mono);letter-spacing:.04em}
.sel{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:7px 11px;color:var(--text);font-size:.8rem;font-family:var(--font-body)}
.sel:focus{outline:none;border-color:var(--primary)}
.label{font-size:.68rem;font-weight:600;color:var(--text-muted);letter-spacing:.04em;text-transform:uppercase;margin-bottom:4px;display:block}
.hint{font-size:.65rem;color:var(--text-muted);margin-top:3px}
.fg{display:flex;flex-direction:column;gap:4px}

/* ── TASKS ── */
.task-form{display:grid;grid-template-columns:1fr 1fr;gap:10px}
@media(max-width:640px){.task-form{grid-template-columns:1fr}}
.task-out{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-lg);padding:14px;font-family:var(--font-mono);font-size:.72rem;color:var(--text);white-space:pre-wrap;max-height:340px;overflow-y:auto;min-height:60px}

/* ── LOG VIEWER ── */
.log-tabs{display:flex;gap:4px;flex-wrap:wrap}
.lt{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:4px 11px;cursor:pointer;font-size:.72rem;font-weight:600;color:var(--text-muted);transition:all var(--tr);font-family:var(--font-body);white-space:nowrap}
.lt:hover{background:var(--surface3);color:var(--text)}
.lt.active{background:var(--surface3);border-color:var(--text-muted);color:var(--text)}
.lt.active[data-type=""]{border-color:var(--primary);color:var(--primary)}
.lt.active[data-type="connection_test"]{border-color:var(--success);color:var(--success)}
.lt.active[data-type="inter_node_message"]{border-color:var(--info);color:var(--info)}
.lt.active[data-type="dream"]{border-color:var(--dream);color:var(--dream)}
.lt.active[data-type="node_chat"]{border-color:var(--chat);color:var(--chat)}
.lt.active[data-type="authority_event"]{border-color:var(--warning);color:var(--warning)}
.lt.active[data-type="system"]{border-color:var(--text-muted);color:var(--text)}
.filter-row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.filter-row .inp{font-size:.75rem;width:auto}
.log-wrap{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);overflow:hidden;font-family:var(--font-mono)}
.log-thead{display:grid;grid-template-columns:112px 120px 110px 100px 80px 1fr;font-size:.62rem;font-weight:700;color:var(--text-muted);letter-spacing:.07em;text-transform:uppercase;padding:7px 14px;border-bottom:1px solid var(--divider);background:var(--surface2)}
.log-body{max-height:460px;overflow-y:auto}
.log-row{display:grid;grid-template-columns:112px 120px 110px 100px 80px 1fr;padding:7px 14px;border-bottom:1px solid var(--divider);cursor:pointer;transition:background var(--tr);font-size:.72rem;align-items:start}
.log-row:last-child{border-bottom:none}
.log-row:hover{background:var(--surface3)}
.log-row .ts{color:var(--text-muted);font-size:.64rem;padding-top:2px;font-family:var(--font-mono)}
.tbadge{display:inline-flex;align-items:center;padding:2px 7px;border-radius:99px;font-size:.59rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em;white-space:nowrap}
.tb-connection_test{background:var(--success-bg);color:var(--success)}
.tb-inter_node_message{background:var(--info-bg);color:var(--info)}
.tb-dream{background:var(--dream-bg);color:var(--dream)}
.tb-node_chat{background:var(--chat-bg);color:var(--chat)}
.tb-authority_event{background:var(--warning-bg);color:var(--warning)}
.tb-system{background:var(--surface3);color:var(--text-muted)}
.sbadge{display:inline-flex;padding:2px 7px;border-radius:99px;font-size:.59rem;font-weight:700;text-transform:uppercase;white-space:nowrap}
.st-success{background:var(--success-bg);color:var(--success)}
.st-failed{background:var(--error-bg);color:var(--error)}
.st-warning{background:var(--warning-bg);color:var(--warning)}
.st-pending{background:var(--info-bg);color:var(--info)}
.st-info{background:var(--surface3);color:var(--text-muted)}
.log-row .summary{color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;padding-right:8px}
.log-row .nc{color:var(--text-muted);font-size:.67rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.detail-row{display:none;padding:8px 14px 12px 14px;background:var(--surface2);border-top:1px solid var(--divider);font-size:.68rem;color:var(--text-muted);white-space:pre-wrap;word-break:break-all;font-family:var(--font-mono)}
.detail-row.open{display:block}
.log-footer{display:flex;align-items:center;gap:10px;padding:6px 14px;background:var(--surface2);border-top:1px solid var(--divider);font-size:.68rem;color:var(--text-muted)}
.pulse{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--success);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.log-empty{padding:40px;text-align:center;color:var(--text-faint);font-size:.78rem}

/* ── DIAGNOSTICS ── */
.diag-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px}
.diag-out{margin-top:8px;padding:9px;border-radius:var(--radius);background:var(--surface2);font-family:var(--font-mono);font-size:.68rem;color:var(--text-muted);white-space:pre-wrap;min-height:44px;border:1px solid var(--divider);max-height:180px;overflow-y:auto}

/* ── ADVANCED SETUP ── */
.setup-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:680px){.setup-grid{grid-template-columns:1fr}}
.secret-row{display:flex;gap:7px;align-items:center}
.secret-row .inp{flex:1}
.mode-row{display:flex;gap:7px}
.mode-btn{flex:1;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:9px;cursor:pointer;text-align:center;font-size:.75rem;font-weight:600;color:var(--text-muted);transition:all var(--tr);font-family:var(--font-body)}
.mode-btn.active{background:var(--primary-bg);border-color:var(--primary);color:var(--primary)}
.setup-footer{display:flex;gap:8px;justify-content:flex-end;padding-top:10px;border-top:1px solid var(--divider)}
.mht-badge{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:99px;font-size:.6rem;font-weight:700;background:var(--dream-bg);color:var(--dream);margin-left:8px;white-space:nowrap}
.mesh-section{opacity:.35;pointer-events:none;transition:opacity .25s}
.mesh-section.on{opacity:1;pointer-events:all}
#saveMsg{font-size:.72rem;color:var(--success);text-align:right;min-height:18px;margin-top:4px}
.rot-at{font-size:.65rem;color:var(--text-muted);margin-left:auto}

/* ── OLLAMA MODAL ── */
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:300;align-items:center;justify-content:center}
.modal-overlay.open{display:flex}
.modal{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-xl);padding:22px;min-width:340px;max-width:480px;width:90%;box-shadow:0 20px 60px rgba(0,0,0,.4)}
.modal-title{font-size:.85rem;font-weight:700;margin-bottom:14px;display:flex;align-items:center;gap:8px}
.model-list{display:flex;flex-direction:column;gap:5px;max-height:220px;overflow-y:auto;margin:10px 0}
.model-item{display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:var(--radius);background:var(--surface2);font-family:var(--font-mono);font-size:.72rem;color:var(--text)}
.model-dot{width:5px;height:5px;border-radius:50%;background:var(--success);flex-shrink:0}
.modal-close{margin-left:auto;background:var(--surface3);border:none;cursor:pointer;padding:4px 10px;border-radius:var(--radius);color:var(--text-muted);font-size:.75rem}

::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:var(--surface)}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
</style>
</head>
<body>

<!-- HEADER -->
<header>
  <div class="logo">
    <svg width="26" height="26" viewBox="0 0 28 28" fill="none" aria-label="HyperSpace AGI">
      <polygon points="14,2 26,9 26,19 14,26 2,19 2,9" stroke="currentColor" stroke-width="1.5" fill="none"/>
      <circle cx="14" cy="14" r="3.5" fill="currentColor" opacity=".75"/>
      <line x1="14" y1="2" x2="14" y2="10.5" stroke="currentColor" stroke-width="1"/>
      <line x1="26" y1="9" x2="17.5" y2="13" stroke="currentColor" stroke-width="1"/>
      <line x1="26" y1="19" x2="17.5" y2="15" stroke="currentColor" stroke-width="1"/>
      <line x1="14" y1="26" x2="14" y2="17.5" stroke="currentColor" stroke-width="1"/>
      <line x1="2" y1="19" x2="10.5" y2="15" stroke="currentColor" stroke-width="1"/>
      <line x1="2" y1="9" x2="10.5" y2="13" stroke="currentColor" stroke-width="1"/>
    </svg>
    HyperSpace AGI <span class="vbadge">v1.01</span>
  </div>
  <nav>
    <button class="active" onclick="showPanel('tasks',this)">Tasks</button>
    <button onclick="showPanel('logs',this)">Log Viewer</button>
    <button onclick="showPanel('diag',this)">Diagnostics</button>
    <button onclick="showPanel('setup',this)">Advanced Setup</button>
  </nav>
  <div class="hdr-right">
    <div class="ollama-pill" onclick="openOllamaModal()" title="Ollama status">
      <span class="ollama-dot" id="ollamaDot"></span>
      <span id="ollamaLabel" style="font-size:.7rem;color:var(--text-muted)">Ollama</span>
    </div>
    <span class="clock" id="clock"></span>
    <button id="themeBtn" aria-label="Toggle theme">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>
    </button>
  </div>
</header>

<!-- HB STATUS BAR -->
<div id="hbBar">
  <div class="hb-item"><span class="hb-dot" id="hbDot"></span><span class="hb-label">HB</span><span class="hb-val" id="hbCycle">—</span></div>
  <div class="hb-item"><span class="hb-label">Tick</span><span class="hb-val" id="hbTick">—</span></div>
  <div class="hb-item"><span class="hb-label">Nodes</span><span class="hb-val" id="hbNodes">—</span></div>
  <div class="hb-item"><span class="hb-label">Last conn</span><span class="hb-val" id="hbConn">—</span></div>
  <div class="hb-item"><span class="hb-label">Last dream</span><span class="hb-val" id="hbDream">—</span></div>
  <div class="hb-item"><span class="hb-label">Last chat</span><span class="hb-val" id="hbChat">—</span></div>
  <span class="hb-live" style="margin-left:auto">&#9679; LIVE</span>
</div>

<!-- MAIN -->
<main>

  <!-- TASKS -->
  <div id="panel-tasks" class="panel active">
    <div class="sec-title">Task Management</div>
    <div class="card">
      <div class="card-title">&#x1F680; Create &amp; Execute Task</div>
      <div class="task-form">
        <div class="fg">
          <label class="label">Task ID</label>
          <input id="tId" class="inp inp-mono" placeholder="task-001"/>
        </div>
        <div class="fg">
          <label class="label">Model (Ollama)</label>
          <input id="tModel" class="inp inp-mono" placeholder="phi3" value="phi3"/>
        </div>
        <div class="fg" style="grid-column:span 2">
          <label class="label">Prompt</label>
          <textarea id="tPrompt" class="inp" rows="3" placeholder="Inserisci il prompt per il modello LLM…" style="resize:vertical"></textarea>
        </div>
      </div>
      <div class="row" style="margin-top:10px">
        <button class="btn btn-ghost" onclick="createTask()">Create</button>
        <button class="btn btn-primary" onclick="createAndAssign()">&#9654; Create &amp; Execute</button>
        <span style="font-size:.7rem;color:var(--text-muted);margin-left:4px" id="taskStatus"></span>
      </div>
    </div>
    <div class="task-out" id="taskOut">// Task output will appear here…</div>
  </div>

  <!-- LOG VIEWER -->
  <div id="panel-logs" class="panel">
    <div class="sec-title">Log Viewer</div>

    <!-- TAB BAR -->
    <div class="log-tabs">
      <button class="lt active" data-type="" onclick="setTab('',this)">All</button>
      <button class="lt" data-type="connection_test" onclick="setTab('connection_test',this)">&#x1F50C; Connection Tests</button>
      <button class="lt" data-type="inter_node_message" onclick="setTab('inter_node_message',this)">&#x1F4E1; Node Communication</button>
      <button class="lt" data-type="dream" onclick="setTab('dream',this)">&#x1F4AD; Dreams</button>
      <button class="lt" data-type="node_chat" onclick="setTab('node_chat',this)">&#x1F4AC; Node Chat</button>
      <button class="lt" data-type="authority_event" onclick="setTab('authority_event',this)">&#x1F511; Authority</button>
      <button class="lt" data-type="system" onclick="setTab('system',this)">&#x2699;&#xFE0F; System</button>
    </div>

    <!-- FILTERS -->
    <div class="filter-row">
      <input class="inp" id="fNode" placeholder="Filter node…" oninput="refreshLogs()" style="width:150px"/>
      <select class="sel" id="fStatus" onchange="refreshLogs()">
        <option value="">All statuses</option>
        <option>success</option><option>failed</option>
        <option>warning</option><option>pending</option><option>info</option>
      </select>
      <input class="inp" id="fQ" placeholder="&#x1F50D; Search…" oninput="refreshLogs()" style="flex:1;min-width:140px"/>
      <label style="display:flex;align-items:center;gap:5px;font-size:.72rem;color:var(--text-muted);cursor:pointer;white-space:nowrap">
        <input type="checkbox" id="autoScroll" checked> Auto-scroll
      </label>
      <button class="btn btn-danger btn-sm" onclick="clearLogs()">Clear</button>
    </div>

    <!-- TABLE -->
    <div class="log-wrap">
      <div class="log-thead">
        <span>Timestamp</span>
        <span>Type</span>
        <span>Source</span>
        <span>Target</span>
        <span>Status</span>
        <span>Summary</span>
      </div>
      <div class="log-body" id="logBody">
        <div class="log-empty">No events yet — logs appear here in real time.</div>
      </div>
      <div class="log-footer">
        <span class="pulse"></span>
        <span id="logCount">0 events</span>
        <span style="margin-left:auto" id="logLast">—</span>
      </div>
    </div>
  </div>

  <!-- DIAGNOSTICS -->
  <div id="panel-diag" class="panel">
    <div class="sec-title">Node Diagnostics</div>
    <div class="diag-grid">

      <div class="card">
        <div class="card-title">&#x1F50C; Authority Reachability</div>
        <p style="font-size:.72rem;color:var(--text-muted);margin-bottom:10px">Verifica connessione verso l'authority server.</p>
        <button class="btn btn-primary btn-sm" onclick="testAuth()">Run Test</button>
        <div class="diag-out" id="dAuth">—</div>
      </div>

      <div class="card">
        <div class="card-title">&#x1F4E1; Active Nodes</div>
        <p style="font-size:.72rem;color:var(--text-muted);margin-bottom:10px">Lista nodi attivi registrati sull'authority.</p>
        <button class="btn btn-ghost btn-sm" onclick="listNodes()">Refresh</button>
        <div class="diag-out" id="dNodes">—</div>
      </div>

      <div class="card">
        <div class="card-title">&#x1F916; Ollama Status</div>
        <p style="font-size:.72rem;color:var(--text-muted);margin-bottom:10px">Verifica Ollama e modelli disponibili.</p>
        <button class="btn btn-success btn-sm" onclick="checkOllama()">Check Ollama</button>
        <div class="diag-out" id="dOllama">—</div>
      </div>

      <div class="card">
        <div class="card-title">&#x1F4AD; Simulate Dream</div>
        <p style="font-size:.72rem;color:var(--text-muted);margin-bottom:8px">Inietta un evento dream nel log stream.</p>
        <div style="display:flex;gap:7px;flex-wrap:wrap;margin-bottom:8px">
          <input id="drNode" class="inp inp-mono" placeholder="node-id" style="width:120px"/>
          <input id="drText" class="inp" placeholder="Dream description…" style="flex:1;min-width:160px"/>
        </div>
        <button class="btn btn-dream btn-sm" onclick="sendDream()">Send Dream</button>
        <div class="diag-out" id="dDream">—</div>
      </div>

      <div class="card">
        <div class="card-title">&#x1F4AC; Simulate Node Chat</div>
        <p style="font-size:.72rem;color:var(--text-muted);margin-bottom:8px">Simula messaggi inter-nodo.</p>
        <div style="display:flex;gap:7px;flex-wrap:wrap;margin-bottom:8px">
          <input id="chFrom" class="inp inp-mono" placeholder="from" style="width:100px"/>
          <input id="chTo" class="inp inp-mono" placeholder="to" style="width:100px"/>
          <input id="chMsg" class="inp" placeholder="Message…" style="flex:1;min-width:160px"/>
        </div>
        <button class="btn btn-chat btn-sm" onclick="sendChat()">Send Chat</button>
        <div class="diag-out" id="dChat">—</div>
      </div>

      <div class="card">
        <div class="card-title">&#x1F4CA; Connection Test (multi-node)</div>
        <p style="font-size:.72rem;color:var(--text-muted);margin-bottom:8px">Ping tutti i nodi attivi, log latenza.</p>
        <button class="btn btn-ghost btn-sm" onclick="pingAllNodes()">Ping All Nodes</button>
        <div class="diag-out" id="dPing">—</div>
      </div>

      <div class="card">
        <div class="card-title">&#x2699;&#xFE0F; Heartbeat Status</div>
        <p style="font-size:.72rem;color:var(--text-muted);margin-bottom:8px">Stato del loop heartbeat interno.</p>
        <button class="btn btn-ghost btn-sm" onclick="checkHb()">Refresh HB</button>
        <div class="diag-out" id="dHb">—</div>
      </div>

    </div>
  </div>

  <!-- ADVANCED SETUP -->
  <div id="panel-setup" class="panel">
    <div class="sec-title">Advanced Setup</div>

    <!-- SECURITY -->
    <div class="card">
      <div class="card-title">
        &#x1F511; Security &mdash; Shared Secret
        <span class="rot-at" id="rsAt"></span>
      </div>
      <div class="fg">
        <label class="label">Shared Secret</label>
        <div class="secret-row">
          <input id="secVal" type="password" class="inp inp-mono" placeholder="Leave blank to keep current"/>
          <button class="btn btn-ghost btn-sm" onclick="toggleSec(this)">Show</button>
          <button class="btn btn-warn btn-sm" onclick="rotateSecret()">&#x21BB; Rotate</button>
        </div>
        <span class="hint">Autentica i nodi sulla rete. Ruotalo periodicamente o usa il bottone Rotate per generarne uno casuale.</span>
      </div>
    </div>

    <!-- AUTHORITY -->
    <div class="card">
      <div class="card-title">&#x1F3DB; Authority Server</div>
      <div class="setup-grid">
        <div class="fg">
          <label class="label">Server URL</label>
          <input id="aUrl" class="inp inp-mono" placeholder="http://authority:8080"/>
          <span class="hint">Endpoint REST dell'authority server.</span>
        </div>
        <div class="fg">
          <label class="label">Auth Mode</label>
          <select id="aMode" class="sel">
            <option value="none">None (dev / open)</option>
            <option value="token">Token</option>
            <option value="jwt">JWT</option>
            <option value="public-key">Public Key</option>
          </select>
          <span class="hint">Modalit&agrave; di autenticazione dei nodi.</span>
        </div>
        <div class="fg">
          <label class="label">Authority Status</label>
          <div class="mode-row" style="max-width:240px">
            <button class="mode-btn active" id="aOn" onclick="setAuthEnabled(true)">Enabled</button>
            <button class="mode-btn" id="aOff" onclick="setAuthEnabled(false)">Disabled</button>
          </div>
          <span class="hint">Disabilita solo in ambienti di test isolati.</span>
        </div>
        <div class="fg" style="align-self:end">
          <button class="btn btn-ghost btn-sm" onclick="testAuthSetup()">&#x1F50C; Test Connection</button>
          <div class="diag-out" id="sAuthTest" style="margin-top:6px;min-height:32px"></div>
        </div>
      </div>
    </div>

    <!-- OLLAMA -->
    <div class="card">
      <div class="card-title">&#x1F916; Ollama Configuration</div>
      <div class="setup-grid">
        <div class="fg">
          <label class="label">Ollama URL</label>
          <input id="oUrl" class="inp inp-mono" placeholder="http://ollama:11434"/>
          <span class="hint">Endpoint del servizio Ollama locale o su host.</span>
        </div>
        <div class="fg">
          <label class="label">Default Model</label>
          <input id="oModel" class="inp inp-mono" placeholder="phi3"/>
          <span class="hint">Modello Ollama di default per i task (es. phi3, llama3, mistral).</span>
        </div>
      </div>
    </div>

    <!-- NETWORK MODE -->
    <div class="card">
      <div class="card-title">
        &#x1F310; Network Mode
        <span class="mht-badge">&#x1F4A0; MHT &mdash; coming soon</span>
      </div>
      <div class="setup-grid">
        <div class="fg" style="grid-column:span 2">
          <label class="label">Operating Mode</label>
          <div class="mode-row" style="max-width:380px">
            <button class="mode-btn active" id="mAuth" onclick="setNetMode('authority')">Authority-managed</button>
            <button class="mode-btn" id="mMesh" onclick="setNetMode('mesh')">Pure Mesh (MHT)</button>
          </div>
          <span class="hint">Authority-managed: i nodi si registrano sull&apos;authority. Pure Mesh: coordinamento P2P con Modular Hash Tree (setup futuro).</span>
        </div>
        <div class="fg mesh-section" id="meshPeersSection">
          <label class="label">Bootstrap Peers</label>
          <textarea id="mPeers" class="inp" rows="3" placeholder="node-alpha:9000&#10;node-beta:9000" style="resize:vertical"></textarea>
          <span class="hint">Un peer per riga, formato host:port.</span>
        </div>
        <div class="fg mesh-section" id="meshMhtSection">
          <label class="label">MHT Routing</label>
          <div class="mode-row" style="max-width:240px">
            <button class="mode-btn" id="mhtOn" onclick="setMht(true)">Enabled</button>
            <button class="mode-btn active" id="mhtOff" onclick="setMht(false)">Disabled</button>
          </div>
          <span class="hint">Abilita Modular Hash Tree per mesh routing avanzato tra nodi.</span>
        </div>
      </div>
    </div>

    <!-- FOOTER -->
    <div class="setup-footer">
      <button class="btn btn-ghost" onclick="loadCfg()">&#x21BA; Reset</button>
      <button class="btn btn-primary" onclick="saveCfg()">&#x1F4BE; Save Configuration</button>
    </div>
    <div id="saveMsg"></div>
  </div>

</main>

<!-- OLLAMA MODAL -->
<div class="modal-overlay" id="ollamaModal" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <div class="modal-title">
      <span class="ollama-dot" id="modalDot"></span>
      &#x1F916; Ollama Status
      <button class="modal-close" onclick="closeModal()">&#x2715; Close</button>
    </div>
    <div id="modalUrl" style="font-size:.7rem;color:var(--text-muted);font-family:var(--font-mono);margin-bottom:8px"></div>
    <div class="model-list" id="modelList"></div>
    <div id="modalErr" style="font-size:.72rem;color:var(--error);display:none"></div>
    <button class="btn btn-ghost btn-sm" style="margin-top:8px" onclick="checkOllamaModal()">&#x21BA; Refresh</button>
  </div>
</div>

<script>
// ── THEME ──
(function(){
  const r=document.documentElement,btn=document.getElementById('themeBtn');
  let d='dark'; r.setAttribute('data-theme',d);
  const ic={
    dark:'<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>',
    light:'<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>'
  };
  btn.innerHTML=ic[d];
  btn.addEventListener('click',()=>{ d=d==='dark'?'light':'dark'; r.setAttribute('data-theme',d); btn.innerHTML=ic[d]; });
})();

// ── CLOCK ──
function tick(){ document.getElementById('clock').textContent=new Date().toISOString().replace('T',' ').slice(0,19)+' UTC'; }
setInterval(tick,1000); tick();

// ── NAV ──
function showPanel(name,btn){
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('nav button').forEach(b=>b.classList.remove('active'));
  document.getElementById('panel-'+name).classList.add('active');
  btn.classList.add('active');
  if(name==='logs')  refreshLogs();
  if(name==='setup') loadCfg();
  if(name==='diag')  checkOllamaDot();
}

// ── HB STATUS BAR ──
async function refreshHbBar(){
  try{
    const d=await(await fetch('/hb/status')).json();
    document.getElementById('hbDot').className='hb-dot '+(d.running?'ok':'err');
    document.getElementById('hbCycle').textContent='#'+d.cycle;
    document.getElementById('hbTick').textContent=d.last_tick?d.last_tick.slice(11,19):'—';
    document.getElementById('hbNodes').textContent=d.nodes_seen&&d.nodes_seen.length?d.nodes_seen.join(', '):'none';
    document.getElementById('hbConn').textContent=d.last_conn?d.last_conn.slice(11,19):'—';
    document.getElementById('hbDream').textContent=d.last_dream?d.last_dream.slice(11,19):'—';
    document.getElementById('hbChat').textContent=d.last_chat?d.last_chat.slice(11,19):'—';
  }catch(e){}
}
setInterval(refreshHbBar,5000); refreshHbBar();

// ── TASKS ──
async function createTask(){
  const id=document.getElementById('tId').value.trim();
  const prompt=document.getElementById('tPrompt').value.trim();
  const model=document.getElementById('tModel').value.trim()||'phi3';
  if(!id){alert('Inserisci un Task ID');return;}
  document.getElementById('taskStatus').textContent='Creating...';
  const r=await fetch('/task/create',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({task_id:id,prompt,model})});
  const d=await r.json();
  document.getElementById('taskStatus').textContent=d.message||JSON.stringify(d);
  refreshTasks();
}
async function createAndAssign(){
  await createTask();
  const id=document.getElementById('tId').value.trim();
  if(!id)return;
  document.getElementById('taskStatus').textContent='Assigning & executing...';
  const r=await fetch('/task/assign',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({task_id:id})});
  const d=await r.json();
  document.getElementById('taskOut').textContent=JSON.stringify(d,null,2);
  document.getElementById('taskStatus').textContent=d.message||JSON.stringify(d);
  refreshLogs();
}
async function refreshTasks(){
  const r=await fetch('/tasks');
  const d=await r.json();
  document.getElementById('taskOut').textContent=JSON.stringify(d,null,2);
}
setInterval(refreshTasks,4000); refreshTasks();

// ── LOG VIEWER ──
let curType='';
const stEmoji={success:'\u2705',failed:'\u274C',warning:'\u26A0\uFE0F',pending:'\u23F3',info:'\u2139\uFE0F'};

function setTab(type,btn){
  curType=type;
  document.querySelectorAll('.lt').forEach(t=>t.classList.remove('active'));
  btn.classList.add('active');
  refreshLogs();
}

async function refreshLogs(){
  const node=document.getElementById('fNode').value.trim();
  const status=document.getElementById('fStatus').value;
  const q=document.getElementById('fQ').value.trim();
  let url='/logs?';
  if(curType) url+=`type=${curType}&`;
  if(node)   url+=`node=${encodeURIComponent(node)}&`;
  if(status) url+=`status=${status}&`;
  if(q)      url+=`q=${encodeURIComponent(q)}&`;
  try{
    const logs=await(await fetch(url)).json();
    renderLogs(logs);
  }catch(e){}
}

function escH(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

function renderLogs(logs){
  const body=document.getElementById('logBody');
  document.getElementById('logCount').textContent=logs.length+' events';
  document.getElementById('logLast').textContent='Updated '+new Date().toISOString().slice(11,19)+' UTC';
  if(!logs.length){
    body.innerHTML='<div class="log-empty">No events match current filters.</div>';
    return;
  }
  body.innerHTML=logs.map((l,i)=>`
    <div class="log-row" onclick="toggleD('ld${i}')">
      <span class="ts">${escH(l.ts.replace('T',' ').slice(0,19))}</span>
      <span><span class="tbadge tb-${l.type}">${escH(l.type.replace(/_/g,'\u200B'))}</span></span>
      <span class="nc">${escH(l.sourceNode||'—')}</span>
      <span class="nc">${escH(l.targetNode||'—')}</span>
      <span><span class="sbadge st-${l.status}">${stEmoji[l.status]||''}&nbsp;${escH(l.status)}</span></span>
      <span class="summary">${escH(l.summary)}</span>
    </div>
    <div class="detail-row" id="ld${i}"><b>TraceID:</b> ${escH(l.traceId)} &nbsp;|&nbsp; <b>ID:</b> ${escH(l.id)}\n<b>Detail:</b>\n${escH(l.detail||'—')}</div>
  `).join('');
  if(document.getElementById('autoScroll').checked){
    body.scrollTop=body.scrollHeight;
  }
}

function toggleD(id){const e=document.getElementById(id);if(e)e.classList.toggle('open');}
async function clearLogs(){await fetch('/logs/clear',{method:'POST'});refreshLogs();}
setInterval(refreshLogs,5000);

// ── DIAGNOSTICS ──
async function testAuth(){
  document.getElementById('dAuth').textContent='Testing...';
  const d=await(await fetch('/config/authority/test',{method:'POST'})).json();
  document.getElementById('dAuth').textContent=JSON.stringify(d,null,2);
}
async function listNodes(){
  document.getElementById('dNodes').textContent='Loading...';
  try{
    const cfg=await(await fetch('/config/advanced')).json();
    const d=await(await fetch(cfg.authority.serverUrl+'/nodes')).json();
    document.getElementById('dNodes').textContent=JSON.stringify(d,null,2);
  }catch(e){document.getElementById('dNodes').textContent='Error: '+e.message;}
}
async function checkOllama(){
  document.getElementById('dOllama').textContent='Checking...';
  const d=await(await fetch('/ollama/status')).json();
  document.getElementById('dOllama').textContent=JSON.stringify(d,null,2);
}
async function checkHb(){
  const d=await(await fetch('/hb/status')).json();
  document.getElementById('dHb').textContent=JSON.stringify(d,null,2);
}
async function sendDream(){
  const node=document.getElementById('drNode').value.trim()||'node-unknown';
  const sum=document.getElementById('drText').value.trim()||'Autonomous task started';
  const r=await fetch('/logs/add',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({type:'dream',summary:sum,sourceNode:node,status:'info',detail:'Injected via Diagnostics panel'})});
  document.getElementById('dDream').textContent=JSON.stringify(await r.json(),null,2);
  if(document.getElementById('panel-logs').classList.contains('active')) refreshLogs();
}
async function sendChat(){
  const from=document.getElementById('chFrom').value.trim()||'node-a';
  const to=document.getElementById('chTo').value.trim()||'node-b';
  const msg=document.getElementById('chMsg').value.trim()||'hello';
  const r=await fetch('/logs/add',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({type:'node_chat',summary:`${from} \u2192 ${to}: \"${msg}\"`,sourceNode:from,targetNode:to,status:'info',detail:'Injected via Diagnostics panel'})});
  document.getElementById('dChat').textContent=JSON.stringify(await r.json(),null,2);
  if(document.getElementById('panel-logs').classList.contains('active')) refreshLogs();
}
async function pingAllNodes(){
  document.getElementById('dPing').textContent='Pinging...';
  try{
    const cfg=await(await fetch('/config/advanced')).json();
    const nodes=await(await fetch(cfg.authority.serverUrl+'/nodes')).json();
    const list=Object.values(nodes);
    if(!list.length){document.getElementById('dPing').textContent='No nodes registered.';return;}
    const results={};
    await Promise.all(list.map(async n=>{
      const start=Date.now();
      try{
        await fetch(`http://${n.node_id}:${n.port||8084}/status`,{signal:AbortSignal.timeout(3000)});
        const lat=Date.now()-start;
        results[n.node_id]={status:'ok',latency:lat+'ms'};
        await fetch('/logs/add',{method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({type:'connection_test',summary:`Ping OK \u2192 ${n.node_id}`,detail:`latency: ${lat}ms`,sourceNode:'control-plane',targetNode:n.node_id,status:'success'})});
      }catch(e){
        results[n.node_id]={status:'failed',error:e.message};
        await fetch('/logs/add',{method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({type:'connection_test',summary:`Ping FAILED \u2192 ${n.node_id}`,detail:e.message,sourceNode:'control-plane',targetNode:n.node_id,status:'failed'})});
      }
    }));
    document.getElementById('dPing').textContent=JSON.stringify(results,null,2);
    refreshLogs();
  }catch(e){document.getElementById('dPing').textContent='Error: '+e.message;}
}

// ── OLLAMA DOT ──
async function checkOllamaDot(){
  try{
    const d=await(await fetch('/ollama/status')).json();
    const dot=document.getElementById('ollamaDot');
    const lbl=document.getElementById('ollamaLabel');
    if(d.ok){
      dot.className='ollama-dot ok';
      lbl.textContent=`Ollama \u00B7 ${(d.models||[]).length} model${(d.models||[]).length!==1?'s':''}`;
      lbl.style.color='var(--success)';
    }else{
      dot.className='ollama-dot err';
      lbl.textContent='Ollama offline';
      lbl.style.color='var(--error)';
    }
  }catch(e){}
}
setInterval(checkOllamaDot,30000); checkOllamaDot();

// ── OLLAMA MODAL ──
function openOllamaModal(){document.getElementById('ollamaModal').classList.add('open');checkOllamaModal();}
function closeModal(){document.getElementById('ollamaModal').classList.remove('open');}
async function checkOllamaModal(){
  try{
    const d=await(await fetch('/ollama/status')).json();
    document.getElementById('modalUrl').textContent=d.url||'—';
    const dot=document.getElementById('modalDot');
    const list=document.getElementById('modelList');
    const err=document.getElementById('modalErr');
    if(d.ok){
      dot.className='ollama-dot ok'; err.style.display='none';
      if(!d.models||!d.models.length){
        list.innerHTML='<div style="color:var(--text-muted);font-size:.72rem;padding:8px">No models found.<br>Run: <code>docker exec hyperspace_ollama ollama pull phi3</code></div>';
      }else{
        list.innerHTML=d.models.map(m=>`<div class="model-item"><span class="model-dot"></span>${escH(m)}</div>`).join('');
      }
    }else{
      dot.className='ollama-dot err'; list.innerHTML='';
      err.textContent='Error: '+(d.error||'unreachable'); err.style.display='block';
    }
  }catch(e){}
}

// ── ADVANCED SETUP ──
let _authEnabled=true, _netMode='authority', _mhtEnabled=false;

async function loadCfg(){
  try{
    const c=await(await fetch('/config/advanced')).json();
    document.getElementById('secVal').value='';
    document.getElementById('rsAt').textContent=
      c.security.secretRotatedAt?'rotated '+c.security.secretRotatedAt.slice(0,10):'never rotated';
    document.getElementById('aUrl').value=c.authority.serverUrl||'';
    document.getElementById('aMode').value=c.authority.authMode||'none';
    setAuthEnabled(c.authority.enabled!==false);
    document.getElementById('oUrl').value=c.ollama?.url||'';
    document.getElementById('oModel').value=c.ollama?.defaultModel||'';
    document.getElementById('mPeers').value=(c.mesh.bootstrapPeers||[]).join('\n');
    setMht(!!c.mesh.mhtEnabled);
    setNetMode(c.mesh.enabled?'mesh':'authority');
  }catch(e){}
}

function setAuthEnabled(v){
  _authEnabled=v;
  document.getElementById('aOn').classList.toggle('active',v);
  document.getElementById('aOff').classList.toggle('active',!v);
}
function setNetMode(m){
  _netMode=m;
  document.getElementById('mAuth').classList.toggle('active',m==='authority');
  document.getElementById('mMesh').classList.toggle('active',m==='mesh');
  document.getElementById('meshPeersSection').classList.toggle('on',m==='mesh');
  document.getElementById('meshMhtSection').classList.toggle('on',m==='mesh');
}
function setMht(v){
  _mhtEnabled=v;
  document.getElementById('mhtOn').classList.toggle('active',v);
  document.getElementById('mhtOff').classList.toggle('active',!v);
}
function toggleSec(btn){
  const i=document.getElementById('secVal');
  const show=i.type==='password';
  i.type=show?'text':'password';
  btn.textContent=show?'Hide':'Show';
}
async function rotateSecret(){
  const d=await(await fetch('/config/secret/rotate',{method:'POST'})).json();
  if(d.ok){
    document.getElementById('secVal').value=d.secret;
    document.getElementById('secVal').type='text';
    document.getElementById('rsAt').textContent='rotated '+d.rotatedAt.slice(0,10);
    showMsg('\u2705 Secret ruotato: '+d.secret);
  }
}
async function saveCfg(){
  const peers=document.getElementById('mPeers').value.split('\n').map(s=>s.trim()).filter(Boolean);
  const payload={
    security:{sharedSecret:document.getElementById('secVal').value},
    authority:{
      serverUrl:document.getElementById('aUrl').value,
      authMode:document.getElementById('aMode').value,
      enabled:_authEnabled
    },
    ollama:{
      url:document.getElementById('oUrl').value,
      defaultModel:document.getElementById('oModel').value
    },
    mesh:{enabled:_netMode==='mesh',mhtEnabled:_mhtEnabled,bootstrapPeers:peers}
  };
  const d=await(await fetch('/config/advanced',{method:'POST',
    headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)})).json();
  if(d.ok) showMsg('\u2705 Configurazione salvata');
  else showMsg('\u274C Errore salvataggio');
}
async function testAuthSetup(){
  document.getElementById('sAuthTest').textContent='Testing...';
  const d=await(await fetch('/config/authority/test',{method:'POST'})).json();
  document.getElementById('sAuthTest').textContent=JSON.stringify(d,null,2);
}
function showMsg(m){
  const e=document.getElementById('saveMsg');
  e.textContent=m;
  setTimeout(()=>e.textContent='',4000);
}
</script>
</body>
</html>"""


@app.route('/dashboard')
def dashboard():
    return DASHBOARD_HTML


# ----------------------------
# MAIN
# ----------------------------
def main():
    print("[control-plane] v1.01 starting on :8085")
    hb = threading.Thread(target=heartbeat_loop, daemon=True)
    hb.start()
    app.run(host="0.0.0.0", port=8085)


if __name__ == "__main__":
    main()
