# control-plane/main.py
# HyperSpace AGI v0.2 — Control Plane + Dashboard

from flask import Flask, request, jsonify, Response
import os, threading, time, requests, json, uuid, random
from datetime import datetime

app = Flask(__name__)

# ── CONFIG ──────────────────────────────────────────────────────────────────
NODE_ENDPOINTS = [e.strip() for e in os.getenv("NODE_ENDPOINTS","node:8084").split(",") if e.strip()]
OLLAMA_URL    = os.getenv("OLLAMA_URL","http://host.docker.internal:11434")
DEFAULT_MODEL = os.getenv("OLLAMA_MODEL","phi3")
_AUTHORITY_URL     = os.getenv("AUTHORITY_URL","http://authority:8080")
_AUTHORITY_ENABLED = os.getenv("AUTHORITY_ENABLED","false").lower()=="true"
LOG_LIMIT = 500

# ── STATE ────────────────────────────────────────────────────────────────────
tasks: dict = {}
event_logs  = []
_nodes_by_id: dict  = {}
_known_endpoints: set = set(NODE_ENDPOINTS)

hb_state = {"cycle":0,"last_tick":None,"last_conn":None,
            "last_dream":None,"last_chat":None,
            "nodes_seen":[],"running":False}

advanced_config = {
    "ollama":     {"url":OLLAMA_URL,"defaultModel":DEFAULT_MODEL},
    "mesh":       {"nodeEndpoints":NODE_ENDPOINTS,"heartbeatEvery":15},
    "_authority": {"serverUrl":_AUTHORITY_URL,"enabled":_AUTHORITY_ENABLED},
    "security":   {"sharedSecret":"","secretRotatedAt":None},
}

# ── HELPERS ──────────────────────────────────────────────────────────────────
def _ep_to_url(ep):
    return ep.rstrip("/") if ep.startswith("http") else f"http://{ep}"

def _is_public_ep(ep):
    if ep.startswith("https://"): return True
    if ep.startswith("http://"):
        host=ep.split("//")[1].split(":")[0].split("/")[0]
        return "." in host and host not in ("localhost",)
    host=ep.split(":")[0]
    return "." in host

def _best_endpoint(node_info):
    ep=node_info.get("endpoint","")
    if ep.startswith("https://"): return ep
    public=node_info.get("public_endpoint","")
    if public and public.startswith("https://"): return public
    return ep

def _node_list():
    return list(_nodes_by_id.values())

# ── LOG ──────────────────────────────────────────────────────────────────────
LOG_TYPES = {"connection_test","inter_node_message","dream","node_chat","system","mesh_event"}

def push_log(type_, summary, detail="", source="control-plane", target="", status="info", trace_id=""):
    global event_logs
    entry = {
        "id": str(uuid.uuid4()),
        "ts": datetime.utcnow().isoformat(timespec="seconds")+"Z",
        "type": type_ if type_ in LOG_TYPES else "system",
        "sourceNode": source, "targetNode": target, "status": status,
        "traceId": trace_id or str(uuid.uuid4())[:8],
        "summary": summary, "detail": detail,
    }
    event_logs.append(entry)
    if len(event_logs)>LOG_LIMIT: event_logs=event_logs[-LOG_LIMIT:]
    return entry

@app.route('/logs')
def get_logs():
    tf=request.args.get('type',''); sf=request.args.get('status','')
    nf=request.args.get('node',''); q=request.args.get('q','').lower()
    r=event_logs[:]
    if tf: r=[l for l in r if l['type']==tf]
    if sf: r=[l for l in r if l['status']==sf]
    if nf: r=[l for l in r if nf in l['sourceNode'] or nf in l['targetNode']]
    if q:  r=[l for l in r if q in l['summary'].lower() or q in l['detail'].lower()]
    return jsonify(list(reversed(r[-200:])))

@app.route('/logs/add', methods=['POST'])
def add_log():
    data=request.get_json(force=True,silent=True) or {}
    entry=push_log(type_=data.get('type','system'),summary=data.get('summary',''),
        detail=data.get('detail',''),source=data.get('sourceNode','unknown'),
        target=data.get('targetNode',''),status=data.get('status','info'),
        trace_id=data.get('traceId',''))
    return jsonify(entry), 201

@app.route('/logs/clear', methods=['POST'])
def clear_logs():
    global event_logs; event_logs=[]
    return jsonify({"ok": True})

# ── MESH ──────────────────────────────────────────────────────────────────────
@app.route('/mesh/announce', methods=['POST'])
def mesh_announce():
    data=request.get_json(force=True,silent=True) or {}
    ep=data.get("endpoint","").strip().rstrip("/")
    nid=data.get("node_id","")
    if not ep or not nid:
        return jsonify({"ok":False,"error":"missing endpoint or node_id"}), 400
    existing=_nodes_by_id.get(nid)
    should_update=True
    if existing:
        existing_ep=existing.get("endpoint","")
        if existing_ep.startswith("https://") and not ep.startswith("https://"):
            should_update=False
    if should_update:
        _nodes_by_id[nid]={**data,"status":"active",
            "last_seen":datetime.utcnow().isoformat(timespec="seconds")+"Z"}
        _known_endpoints.add(ep)
    push_log('mesh_event',f'Node announced: {nid[:12]}',
        f'endpoint={ep} accepted={should_update}',source=nid[:12],status='success')
    return jsonify({"ok":True,"registered":ep,"accepted":should_update})

@app.route('/mesh/nodes')
def get_mesh_nodes(): return jsonify(_node_list())

@app.route('/mesh/node/<path:endpoint>/status')
def get_node_status(endpoint):
    try:
        r=requests.get(f"{_ep_to_url(endpoint)}/status",timeout=3)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error":str(e)}), 503

@app.route('/mesh/node/<path:endpoint>/peers')
def get_node_peers(endpoint):
    try:
        r=requests.get(f"{_ep_to_url(endpoint)}/peers",timeout=3)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error":str(e)}), 503

@app.route('/hb/status')
def hb_status(): return jsonify(dict(hb_state))

# ── CONFIG ────────────────────────────────────────────────────────────────────
@app.route('/config/advanced')
def get_advanced_config():
    safe=json.loads(json.dumps(advanced_config))
    if safe["security"]["sharedSecret"]: safe["security"]["sharedSecret"]="***"
    return jsonify(safe)

@app.route('/config/advanced', methods=['POST'])
def set_advanced_config():
    global OLLAMA_URL, DEFAULT_MODEL
    data=request.get_json(force=True,silent=True) or {}
    sec=data.get('security',{}); mesh=data.get('mesh',{})
    ollama=data.get('ollama',{}); auth=data.get('_authority',{})
    if 'sharedSecret' in sec and sec['sharedSecret'] not in ('','***'):
        advanced_config['security']['sharedSecret']=sec['sharedSecret']
        advanced_config['security']['secretRotatedAt']=datetime.utcnow().isoformat()
    if 'url' in ollama: advanced_config['ollama']['url']=ollama['url']; OLLAMA_URL=ollama['url']
    if 'defaultModel' in ollama:
        advanced_config['ollama']['defaultModel']=ollama['defaultModel']
        DEFAULT_MODEL=ollama['defaultModel']
    if 'nodeEndpoints' in mesh:
        advanced_config['mesh']['nodeEndpoints']=mesh['nodeEndpoints']
        for ep in mesh['nodeEndpoints']: _known_endpoints.add(ep)
    if 'serverUrl' in auth: advanced_config['_authority']['serverUrl']=auth['serverUrl']
    if 'enabled' in auth: advanced_config['_authority']['enabled']=bool(auth['enabled'])
    push_log('system','Config updated',json.dumps(data,default=str))
    return jsonify({"ok":True})

@app.route('/config/secret/rotate', methods=['POST'])
def rotate_secret():
    new_secret=str(uuid.uuid4()).replace('-','')
    advanced_config['security']['sharedSecret']=new_secret
    advanced_config['security']['secretRotatedAt']=datetime.utcnow().isoformat()
    push_log('system','Shared secret rotated',status='success')
    return jsonify({"ok":True,"secret":new_secret,
        "rotatedAt":advanced_config['security']['secretRotatedAt']})

@app.route('/ollama/status')
def ollama_status():
    url=advanced_config['ollama']['url']
    try:
        r=requests.get(f"{url}/api/tags",timeout=3)
        models=[m['name'] for m in r.json().get('models',[])]
        return jsonify({"ok":True,"url":url,"models":models})
    except Exception as e:
        return jsonify({"ok":False,"url":url,"error":str(e)})

# ── TASKS ─────────────────────────────────────────────────────────────────────
@app.route('/task/create', methods=['POST'])
def create_task():
    data=request.get_json(force=True,silent=True) or {}
    task_id=data.get('task_id') or str(uuid.uuid4())[:8]
    prompt=data.get('prompt','')
    model=data.get('model',advanced_config['ollama']['defaultModel'])
    tasks[task_id]={
        "id":task_id,"status":"created","node":None,
        "created_at":datetime.utcnow().isoformat(timespec="seconds")+"Z",
        "payload":{"prompt":prompt,"model":model}
    }
    push_log('system',f'Task created: {task_id}',detail=f'prompt={prompt[:80]}')
    return jsonify({"message":"Task created","task_id":task_id}), 201

@app.route('/task/assign', methods=['POST'])
def assign_task():
    data=request.get_json(force=True,silent=True) or {}
    task_id=data.get('task_id')
    if not task_id or task_id not in tasks:
        return jsonify({"error":"Task not found"}), 404
    active=[n for n in _node_list() if n.get("status")=="active"]
    if not active:
        return jsonify({"error":"No active nodes"}), 503
    public_nodes=[n for n in active if _best_endpoint(n).startswith("https://")]
    selected=public_nodes[0] if public_nodes else active[0]
    endpoint=_best_endpoint(selected)
    node_id=selected["node_id"]
    task=tasks[task_id]
    task["status"]="assigned"; task["node"]=node_id; task["endpoint"]=endpoint
    tid=str(uuid.uuid4())[:8]
    push_log('inter_node_message',f'Task {task_id} -> {node_id[:12]}',
        f'endpoint={endpoint}',target=node_id[:12],status='pending',trace_id=tid)
    try:
        r=requests.post(f"{_ep_to_url(endpoint)}/execute",json=task,timeout=120)
        task["result"]=r.json(); task["status"]="done"
        task["completed_at"]=datetime.utcnow().isoformat(timespec="seconds")+"Z"
        push_log('inter_node_message',f'Task {task_id} done',
            json.dumps(task.get("result",{})),source=node_id[:12],
            target='control-plane',status='success',trace_id=tid)
    except Exception as e:
        task["status"]="failed"; task["error"]=str(e)
        push_log('inter_node_message',f'Task {task_id} failed',
            str(e),source=node_id[:12],status='failed',trace_id=tid)
        return jsonify({"error":str(e)}), 500
    return jsonify({"message":"done","task":task})

@app.route('/tasks')
def get_tasks(): return jsonify(tasks)

# ── HEARTBEAT ─────────────────────────────────────────────────────────────────
DREAM_PHRASES=[
    "autonomous planning cycle initiated","memory consolidation phase started",
    "sub-task decomposition in progress","latent space exploration #{}",
    "tool-use reflection completed","goal re-prioritization triggered",
    "associative memory update: {} new links","dream cycle #{} - context window cleared",
]
CHAT_PHRASES=[
    ("can you handle a summarize task?","yes, {} slots free"),
    ("what is your current model?","running {}"),
    ("sync memory snapshot?","snapshot ready - {} KB"),
    ("queue depth?","depth {} - capacity normal"),
    ("ready for next task?","ready, latency {}ms"),
]

def _poll_mesh_nodes():
    for ep in list(_known_endpoints):
        if not _is_public_ep(ep):
            already_public=any(_is_public_ep(n.get("endpoint",""))
                for n in _nodes_by_id.values() if n.get("endpoint","")!=ep)
            if already_public: continue
        try:
            r=requests.get(f"{_ep_to_url(ep)}/status",timeout=3)
            if r.status_code==200:
                info=r.json(); nid=info.get("node_id","")
                info["endpoint"]=ep; info["status"]="active"
                info["last_seen"]=datetime.utcnow().isoformat(timespec="seconds")+"Z"
                if nid:
                    existing=_nodes_by_id.get(nid)
                    if not existing or not existing.get("endpoint","").startswith("https://") or ep.startswith("https://"):
                        _nodes_by_id[nid]=info
                try:
                    rp=requests.get(f"{_ep_to_url(ep)}/peers",timeout=2)
                    for peer in rp.json().get("peers",[]):
                        pep=peer.get("endpoint","")
                        if pep and pep not in _known_endpoints: _known_endpoints.add(pep)
                except Exception: pass
        except Exception:
            for nid,n in _nodes_by_id.items():
                if n.get("endpoint")==ep: _nodes_by_id[nid]["status"]="unreachable"

def heartbeat_loop():
    time.sleep(3)
    push_log('system','Control-plane v0.2 started',
        detail=f'nodes={list(_known_endpoints)}',status='info')
    hb_state["running"]=True
    while True:
        cycle=hb_state["cycle"]+1; hb_state["cycle"]=cycle
        hb_state["last_tick"]=datetime.utcnow().isoformat(timespec="seconds")+"Z"
        _poll_mesh_nodes()
        hb_state["nodes_seen"]=[n.get("node_id",n.get("endpoint","?"))[:12]
            for n in _node_list() if n.get("status")=="active"]
        for node in _node_list():
            if node.get("status")!="active": continue
            nid=node.get("node_id",node.get("endpoint","unknown"))[:12]
            ep=_best_endpoint(node); tid=str(uuid.uuid4())[:8]
            try:
                t0=time.time(); requests.get(f"{_ep_to_url(ep)}/health",timeout=2)
                lat=int((time.time()-t0)*1000)
                push_log('connection_test',f'HB#{cycle} ping OK -> {nid}',
                    f'latency: {lat}ms | endpoint: {ep}',
                    source='control-plane',target=nid,status='success',trace_id=tid)
                hb_state["last_conn"]=hb_state["last_tick"]
            except Exception as e:
                push_log('connection_test',f'HB#{cycle} ping FAILED -> {nid}',
                    str(e),source='control-plane',target=nid,status='failed',trace_id=tid)
        if cycle%3==0:
            pool=[n.get("node_id","node-sim")[:16] for n in _node_list()] or ["node-sim"]
            nid=random.choice(pool)
            push_log('dream',f'{nid}: {random.choice(DREAM_PHRASES).format(random.randint(1,99))}',
                f'cycle={cycle}',source=nid,status='info')
            hb_state["last_dream"]=hb_state["last_tick"]
        if cycle%5==0:
            pool=[n.get("node_id","")[:16] for n in _node_list()]
            if len(pool)<2: pool=(pool+["node-sim"])[:2]
            src,dst=random.sample(pool,2)
            q,a_tpl=random.choice(CHAT_PHRASES)
            answer=a_tpl.format(random.randint(1,8))
            tid=str(uuid.uuid4())[:8]
            push_log('node_chat',f'{src} -> {dst}: "{q}"',f'cycle={cycle}',
                source=src,target=dst,status='info',trace_id=tid)
            push_log('node_chat',f'{dst} -> {src}: "{answer}"',f'reply',
                source=dst,target=src,status='info',trace_id=tid)
            hb_state["last_chat"]=hb_state["last_tick"]
        time.sleep(15)

# ── DASHBOARD ─────────────────────────────────────────────────────────────────
# HTML served as a raw Response to avoid Python interpreting JS unicode escapes
# inside the triple-quoted string (which caused SyntaxError in the browser).
_DASHBOARD_HTML = (
    '<!DOCTYPE html>'
    '<html lang="it" data-theme="dark">'
    '<head>'
    '<meta charset="UTF-8"/>'
    '<meta name="viewport" content="width=device-width,initial-scale=1"/>'
    '<title>HyperSpace AGI v0.2</title>'
    '<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">'
    '<style>'
    ':root,[data-theme="dark"]{--bg:#0b0c0e;--surface:#131518;--surface2:#191c21;--surface3:#21252d;--border:#272b33;--divider:#1c1f26;--text:#c8cdd6;--text-muted:#636a78;--text-faint:#353a44;--primary:#4f98a3;--primary-h:#2d7d8a;--primary-bg:rgba(79,152,163,.12);--success:#6daa45;--success-bg:rgba(109,170,69,.12);--warning:#e8af34;--warning-bg:rgba(232,175,52,.10);--error:#dd6974;--error-bg:rgba(221,105,116,.12);--info:#5591c7;--info-bg:rgba(85,145,199,.12);--dream:#a86fdf;--dream-bg:rgba(168,111,223,.12);--chat:#fdab43;--chat-bg:rgba(253,171,67,.10);--root:#f06292;--hub:#4dd0e1;--leaf:#81c784;--font-mono:\'JetBrains Mono\',monospace;--font-body:\'Inter\',sans-serif;--radius:5px;--radius-lg:9px;--tr:160ms cubic-bezier(.16,1,.3,1)}'
    '[data-theme="light"]{--bg:#f0f1f4;--surface:#fff;--surface2:#f6f7fa;--surface3:#eaecf0;--border:#d8dbe2;--text:#191c22;--text-muted:#636a78;--text-faint:#b0b8c8;--primary:#016970;--primary-h:#014f55;--primary-bg:rgba(1,105,112,.08);--success:#3a6e1a;--success-bg:rgba(58,110,26,.08);--warning:#9a6800;--error:#a12c3a;--error-bg:rgba(161,44,58,.08);--info:#225f99;--info-bg:rgba(34,95,153,.08);--dream:#6b30b5;--dream-bg:rgba(107,48,181,.08);--chat:#b56200;--chat-bg:rgba(181,98,0,.08)}'
    '*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}'
    'html{font-size:13px;-webkit-font-smoothing:antialiased}'
    'body{font-family:var(--font-body);background:var(--bg);color:var(--text);min-height:100vh;display:grid;grid-template-rows:auto auto 1fr}'
    'header{background:var(--surface);border-bottom:1px solid var(--border);padding:0 20px;display:flex;align-items:center;gap:12px;height:50px;position:sticky;top:0;z-index:200}'
    '.logo{display:flex;align-items:center;gap:9px;font-family:var(--font-mono);font-weight:700;font-size:.9rem;color:var(--primary);white-space:nowrap}'
    '.vbadge{font-size:.58rem;font-weight:700;background:var(--primary-bg);color:var(--primary);padding:2px 6px;border-radius:99px}'
    'nav{display:flex;gap:1px;margin-left:12px}'
    'nav button{background:none;border:none;cursor:pointer;padding:5px 13px;border-radius:var(--radius);font-size:.78rem;font-weight:500;color:var(--text-muted);transition:color var(--tr),background var(--tr);font-family:var(--font-body)}'
    'nav button.active{background:var(--primary-bg);color:var(--primary)}'
    'nav button:hover:not(.active){background:var(--surface3);color:var(--text)}'
    '.hdr-right{margin-left:auto;display:flex;align-items:center;gap:10px}'
    '.ollama-pill{display:flex;align-items:center;gap:5px;background:var(--surface2);border:1px solid var(--border);border-radius:99px;padding:3px 10px;font-size:.7rem;font-family:var(--font-mono);cursor:pointer;transition:border-color var(--tr)}'
    '.ollama-pill:hover{border-color:var(--primary)}'
    '.ollama-dot{width:6px;height:6px;border-radius:50%;background:var(--text-faint);flex-shrink:0}'
    '.ollama-dot.ok{background:var(--success);box-shadow:0 0 4px var(--success)}'
    '.ollama-dot.err{background:var(--error)}'
    '.clock{font-family:var(--font-mono);font-size:.68rem;color:var(--text-muted)}'
    '#themeBtn{background:none;border:1px solid var(--border);cursor:pointer;padding:5px 7px;border-radius:var(--radius);color:var(--text-muted);line-height:1}'
    '#hbBar{background:var(--surface2);border-bottom:1px solid var(--border);padding:5px 20px;display:flex;gap:20px;align-items:center;flex-wrap:wrap;font-size:.65rem;font-family:var(--font-mono);color:var(--text-muted)}'
    '.hb-item{display:flex;align-items:center;gap:5px}'
    '.hb-dot{width:5px;height:5px;border-radius:50%;background:var(--text-faint)}'
    '.hb-dot.ok{background:var(--success)}.hb-dot.err{background:var(--error)}'
    '.hb-label{color:var(--text-faint);text-transform:uppercase;letter-spacing:.06em;margin-right:2px}'
    '.hb-val{color:var(--text)}'
    '@keyframes hbpulse{0%,100%{opacity:1}50%{opacity:.25}}'
    '.hb-live{animation:hbpulse 2s infinite;color:var(--success)}'
    'main{padding:18px 20px;display:grid}'
    '.panel{display:none;flex-direction:column;gap:14px;animation:fadein .18s ease}'
    '.panel.active{display:flex}'
    '@keyframes fadein{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}'
    '.sec-title{font-size:.65rem;font-weight:700;letter-spacing:.11em;text-transform:uppercase;color:var(--text-muted);padding-bottom:8px;border-bottom:1px solid var(--divider)}'
    '.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:15px}'
    '.card-title{font-size:.75rem;font-weight:600;color:var(--text-muted);margin-bottom:11px}'
    '.btn{border:none;cursor:pointer;padding:7px 15px;border-radius:var(--radius);font-size:.78rem;font-weight:500;transition:background var(--tr),color var(--tr);font-family:var(--font-body);white-space:nowrap}'
    '.btn-primary{background:var(--primary);color:#fff}.btn-primary:hover{background:var(--primary-h)}'
    '.btn-ghost{background:var(--surface3);color:var(--text)}.btn-ghost:hover{background:var(--border)}'
    '.btn-danger{background:var(--error-bg);color:var(--error)}.btn-danger:hover{background:var(--error);color:#fff}'
    '.btn-warn{background:var(--warning-bg);color:var(--warning)}'
    '.btn-success{background:var(--success-bg);color:var(--success)}'
    '.btn-dream{background:var(--dream-bg);color:var(--dream)}'
    '.btn-chat{background:var(--chat-bg);color:var(--chat)}'
    '.btn-sm{padding:4px 10px;font-size:.72rem}'
    '.inp{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:7px 11px;color:var(--text);font-size:.8rem;font-family:var(--font-body);transition:border-color var(--tr);width:100%}'
    '.inp:focus{outline:none;border-color:var(--primary)}'
    '.inp-mono{font-family:var(--font-mono)}'
    '.sel{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:7px 11px;color:var(--text);font-size:.8rem;font-family:var(--font-body)}'
    '.label{font-size:.68rem;font-weight:600;color:var(--text-muted);letter-spacing:.04em;text-transform:uppercase;margin-bottom:4px;display:block}'
    '.hint{font-size:.65rem;color:var(--text-muted);margin-top:3px}'
    '.fg{display:flex;flex-direction:column;gap:4px}'
    '.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}'
    '.nodes-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px}'
    '.node-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:14px;transition:border-color var(--tr)}'
    '.node-card:hover{border-color:var(--primary)}'
    '.nc-header{display:flex;align-items:center;gap:8px;margin-bottom:10px}'
    '.node-status-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}'
    '.node-status-dot.active{background:var(--success);box-shadow:0 0 6px var(--success)}'
    '.node-status-dot.unreachable{background:var(--error)}'
    '.node-status-dot.unknown{background:var(--text-faint)}'
    '.node-id{font-family:var(--font-mono);font-size:.72rem;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1}'
    '.tier-badge{display:inline-flex;padding:2px 8px;border-radius:99px;font-size:.6rem;font-weight:700;text-transform:uppercase;flex-shrink:0}'
    '.tier-root{background:rgba(240,98,146,.15);color:var(--root)}'
    '.tier-hub{background:rgba(77,208,225,.12);color:var(--hub)}'
    '.tier-leaf{background:rgba(129,199,132,.12);color:var(--leaf)}'
    '.node-meta{display:grid;grid-template-columns:1fr 1fr;gap:4px 12px;font-size:.68rem}'
    '.nm-label{color:var(--text-muted)}.nm-val{font-family:var(--font-mono);color:var(--text)}'
    '.node-peers{margin-top:8px;font-size:.65rem;color:var(--text-muted);border-top:1px solid var(--divider);padding-top:7px}'
    '.peer-tag{display:inline-flex;background:var(--surface2);border:1px solid var(--border);border-radius:3px;padding:1px 6px;font-family:var(--font-mono);font-size:.6rem;margin:2px 2px 0 0;color:var(--text-muted)}'
    '.nodes-empty{padding:40px;text-align:center;color:var(--text-faint);font-size:.8rem}'
    '.task-form{display:grid;grid-template-columns:1fr 1fr;gap:10px}'
    '@media(max-width:640px){.task-form{grid-template-columns:1fr}}'
    '.task-status-label{font-size:.75rem;color:var(--text-muted)}'
    '.task-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);overflow:hidden;transition:border-color var(--tr);margin-bottom:8px}'
    '.task-card:hover{border-color:var(--primary)}'
    '.task-header{display:flex;align-items:center;gap:10px;padding:12px 15px;cursor:pointer;background:var(--surface2);flex-wrap:wrap}'
    '.task-id{font-family:var(--font-mono);font-size:.78rem;font-weight:700;color:var(--primary)}'
    '.task-node{font-family:var(--font-mono);font-size:.68rem;color:var(--text-muted);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
    '.task-model-badge{background:var(--surface3);border:1px solid var(--border);border-radius:99px;padding:2px 9px;font-size:.65rem;font-family:var(--font-mono);color:var(--text-muted)}'
    '.task-status-badge{padding:3px 9px;border-radius:99px;font-size:.65rem;font-weight:700;text-transform:uppercase}'
    '.ts-done{background:var(--success-bg);color:var(--success)}'
    '.ts-failed{background:var(--error-bg);color:var(--error)}'
    '.ts-assigned,.ts-created{background:var(--info-bg);color:var(--info)}'
    '.task-ts{font-size:.63rem;color:var(--text-faint)}'
    '.task-body{display:none;padding:14px 15px;border-top:1px solid var(--divider)}'
    '.task-body.open{display:block}'
    '.task-section{margin-bottom:12px}'
    '.task-section-title{font-size:.62rem;font-weight:700;text-transform:uppercase;letter-spacing:.09em;color:var(--text-muted);margin-bottom:6px}'
    '.task-prompt-box{background:var(--surface2);border:1px solid var(--divider);border-radius:var(--radius);padding:10px 12px;font-size:.82rem;color:var(--text);line-height:1.6;white-space:pre-wrap}'
    '.task-response-box{background:var(--surface3);border:1px solid var(--divider);border-radius:var(--radius);padding:10px 12px;font-size:.82rem;color:var(--text);line-height:1.7;white-space:pre-wrap;max-height:320px;overflow-y:auto}'
    '.task-error-box{background:var(--error-bg);border:1px solid var(--error);border-radius:var(--radius);padding:10px 12px;font-size:.75rem;color:var(--error);font-family:var(--font-mono);white-space:pre-wrap}'
    '.tasks-empty{padding:40px;text-align:center;color:var(--text-faint);font-size:.8rem}'
    '.log-tabs{display:flex;gap:4px;flex-wrap:wrap}'
    '.lt{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:4px 11px;cursor:pointer;font-size:.72rem;font-weight:600;color:var(--text-muted);transition:all var(--tr);font-family:var(--font-body)}'
    '.lt:hover{background:var(--surface3);color:var(--text)}'
    '.lt.active{background:var(--surface3);border-color:var(--text-muted);color:var(--text)}'
    '.filter-row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}'
    '.log-wrap{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);overflow:hidden;font-family:var(--font-mono)}'
    '.log-thead{display:grid;grid-template-columns:112px 130px 110px 100px 80px 1fr;font-size:.62rem;font-weight:700;color:var(--text-muted);letter-spacing:.07em;text-transform:uppercase;padding:7px 14px;border-bottom:1px solid var(--divider);background:var(--surface2)}'
    '.log-body{max-height:460px;overflow-y:auto}'
    '.log-row{display:grid;grid-template-columns:112px 130px 110px 100px 80px 1fr;padding:7px 14px;border-bottom:1px solid var(--divider);cursor:pointer;transition:background var(--tr);font-size:.72rem;align-items:start}'
    '.log-row:last-child{border-bottom:none}'
    '.log-row:hover{background:var(--surface3)}'
    '.log-row .ts{color:var(--text-muted);font-size:.64rem;padding-top:2px}'
    '.tbadge{display:inline-flex;align-items:center;padding:2px 7px;border-radius:99px;font-size:.59rem;font-weight:700;text-transform:uppercase;white-space:nowrap}'
    '.tb-connection_test{background:var(--success-bg);color:var(--success)}'
    '.tb-inter_node_message{background:var(--info-bg);color:var(--info)}'
    '.tb-dream{background:var(--dream-bg);color:var(--dream)}'
    '.tb-node_chat{background:var(--chat-bg);color:var(--chat)}'
    '.tb-mesh_event{background:var(--warning-bg);color:var(--warning)}'
    '.tb-system{background:var(--surface3);color:var(--text-muted)}'
    '.sbadge{display:inline-flex;padding:2px 7px;border-radius:99px;font-size:.59rem;font-weight:700;text-transform:uppercase;white-space:nowrap}'
    '.st-success{background:var(--success-bg);color:var(--success)}'
    '.st-failed{background:var(--error-bg);color:var(--error)}'
    '.st-pending,.st-info{background:var(--info-bg);color:var(--info)}'
    '.log-row .summary{color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;padding-right:8px}'
    '.log-row .nc{color:var(--text-muted);font-size:.67rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}'
    '.detail-row{display:none;padding:8px 14px 12px;background:var(--surface2);border-top:1px solid var(--divider);font-size:.68rem;color:var(--text-muted);white-space:pre-wrap;word-break:break-all}'
    '.detail-row.open{display:block}'
    '.log-footer{display:flex;align-items:center;gap:10px;padding:6px 14px;background:var(--surface2);border-top:1px solid var(--divider);font-size:.68rem;color:var(--text-muted)}'
    '.pulse{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--success);animation:pulse 2s infinite}'
    '@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}'
    '.log-empty{padding:40px;text-align:center;color:var(--text-faint);font-size:.78rem}'
    '.diag-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px}'
    '.diag-out{margin-top:8px;padding:9px;border-radius:var(--radius);background:var(--surface2);font-family:var(--font-mono);font-size:.68rem;color:var(--text-muted);white-space:pre-wrap;min-height:44px;border:1px solid var(--divider);max-height:220px;overflow-y:auto}'
    '.setup-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}'
    '@media(max-width:680px){.setup-grid{grid-template-columns:1fr}}'
    '.setup-footer{display:flex;gap:8px;justify-content:flex-end;padding-top:10px;border-top:1px solid var(--divider)}'
    '#saveMsg{font-size:.72rem;color:var(--success);text-align:right;min-height:18px;margin-top:4px}'
    '.legacy-toggle{display:flex;align-items:center;gap:8px;cursor:pointer;font-size:.72rem;color:var(--text-muted);padding:8px 0;user-select:none}'
    '.legacy-body{display:none;padding-top:10px;border-top:1px solid var(--divider);margin-top:4px}'
    '.legacy-body.open{display:block}'
    '.legacy-warn{background:var(--warning-bg);border:1px solid var(--warning);border-radius:var(--radius);padding:7px 11px;font-size:.7rem;color:var(--warning);margin-bottom:10px}'
    '.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:300;align-items:center;justify-content:center}'
    '.modal-overlay.open{display:flex}'
    '.modal{background:var(--surface);border:1px solid var(--border);border-radius:13px;padding:22px;min-width:340px;max-width:480px;width:90%}'
    '.modal-title{font-size:.85rem;font-weight:700;margin-bottom:14px;display:flex;align-items:center;gap:8px}'
    '.model-list{display:flex;flex-direction:column;gap:5px;max-height:220px;overflow-y:auto;margin:10px 0}'
    '.model-item{display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:var(--radius);background:var(--surface2);font-family:var(--font-mono);font-size:.72rem}'
    '.model-dot{width:5px;height:5px;border-radius:50%;background:var(--success)}'
    '.modal-close{margin-left:auto;background:var(--surface3);border:none;cursor:pointer;padding:4px 10px;border-radius:var(--radius);color:var(--text-muted);font-size:.75rem}'
    '::-webkit-scrollbar{width:5px;height:5px}'
    '::-webkit-scrollbar-track{background:var(--surface)}'
    '::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}'
    '</style>'
    '</head>'
    '<body>'
    '<header>'
    '  <div class="logo">'
    '    <svg width="26" height="26" viewBox="0 0 28 28" fill="none"><polygon points="14,2 26,9 26,19 14,26 2,19 2,9" stroke="currentColor" stroke-width="1.5" fill="none"/><circle cx="14" cy="14" r="3.5" fill="currentColor" opacity=".75"/><line x1="14" y1="2" x2="14" y2="10.5" stroke="currentColor" stroke-width="1"/><line x1="26" y1="9" x2="17.5" y2="13" stroke="currentColor" stroke-width="1"/><line x1="26" y1="19" x2="17.5" y2="15" stroke="currentColor" stroke-width="1"/><line x1="14" y1="26" x2="14" y2="17.5" stroke="currentColor" stroke-width="1"/><line x1="2" y1="19" x2="10.5" y2="15" stroke="currentColor" stroke-width="1"/><line x1="2" y1="9" x2="10.5" y2="13" stroke="currentColor" stroke-width="1"/></svg>'
    '    HyperSpace AGI <span class="vbadge">v0.2</span>'
    '  </div>'
    '  <nav>'
    '    <button class="active" onclick="showPanel(\'nodes\',this)">&#127760; Mesh Nodes</button>'
    '    <button onclick="showPanel(\'tasks\',this)">&#128640; Tasks</button>'
    '    <button onclick="showPanel(\'logs\',this)">&#128203; Logs</button>'
    '    <button onclick="showPanel(\'diag\',this)">&#128295; Diagnostics</button>'
    '    <button onclick="showPanel(\'setup\',this)">&#9881;&#65039; Setup</button>'
    '  </nav>'
    '  <div class="hdr-right">'
    '    <div class="ollama-pill" onclick="openOllamaModal()"><span class="ollama-dot" id="ollamaDot"></span><span id="ollamaLabel">Ollama</span></div>'
    '    <span class="clock" id="clock"></span>'
    '    <button id="themeBtn"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg></button>'
    '  </div>'
    '</header>'
    '<div id="hbBar">'
    '  <div class="hb-item"><span class="hb-dot" id="hbDot"></span><span class="hb-label">HB</span><span class="hb-val" id="hbCycle">&#8212;</span></div>'
    '  <div class="hb-item"><span class="hb-label">Tick</span><span class="hb-val" id="hbTick">&#8212;</span></div>'
    '  <div class="hb-item"><span class="hb-label">Nodes</span><span class="hb-val" id="hbNodes">&#8212;</span></div>'
    '  <div class="hb-item"><span class="hb-label">Last conn</span><span class="hb-val" id="hbConn">&#8212;</span></div>'
    '  <div class="hb-item"><span class="hb-label">Last dream</span><span class="hb-val" id="hbDream">&#8212;</span></div>'
    '  <span class="hb-live" style="margin-left:auto">&#9679; LIVE</span>'
    '</div>'
    '<main>'
    '<div id="panel-nodes" class="panel active">'
    '  <div class="sec-title">Mesh Nodes &#8212; Live</div>'
    '  <div class="row"><button class="btn btn-ghost btn-sm" onclick="refreshNodes()">&#8634; Refresh</button><span style="font-size:.7rem;color:var(--text-muted)" id="nodesCount"></span></div>'
    '  <div class="nodes-grid" id="nodesGrid"><div class="nodes-empty">Loading nodes&#8230;</div></div>'
    '</div>'
    '<div id="panel-tasks" class="panel">'
    '  <div class="sec-title">Task History</div>'
    '  <div class="card">'
    '    <div class="card-title">&#128640; Nuovo Task</div>'
    '    <div class="task-form">'
    '      <div class="fg"><label class="label">Task ID</label><input id="tId" class="inp inp-mono" placeholder="task-001"/></div>'
    '      <div class="fg"><label class="label">Modello</label><input id="tModel" class="inp inp-mono" placeholder="phi3" value="phi3"/></div>'
    '      <div class="fg" style="grid-column:span 2"><label class="label">Prompt</label><textarea id="tPrompt" class="inp" rows="3" placeholder="Scrivi il prompt..." style="resize:vertical"></textarea></div>'
    '    </div>'
    '    <div class="row" style="margin-top:10px">'
    '      <button class="btn btn-primary" onclick="createAndAssign()">&#9654; Esegui</button>'
    '      <button class="btn btn-ghost" onclick="createTask()">Solo crea</button>'
    '      <span class="task-status-label" id="taskStatus"></span>'
    '    </div>'
    '  </div>'
    '  <div class="row" style="justify-content:space-between">'
    '    <span style="font-size:.7rem;color:var(--text-muted)" id="taskCount">0 tasks</span>'
    '    <button class="btn btn-ghost btn-sm" onclick="refreshTaskHistory()">&#8634; Refresh</button>'
    '  </div>'
    '  <div id="taskHistory"><div class="tasks-empty">Nessun task ancora.</div></div>'
    '</div>'
    '<div id="panel-logs" class="panel">'
    '  <div class="sec-title">Log Viewer</div>'
    '  <div class="log-tabs">'
    '    <button class="lt active" onclick="setTab(\'\',this)">All</button>'
    '    <button class="lt" onclick="setTab(\'connection_test\',this)">&#128268; Connection</button>'
    '    <button class="lt" onclick="setTab(\'inter_node_message\',this)">&#128225; Node Comm</button>'
    '    <button class="lt" onclick="setTab(\'dream\',this)">&#128173; Dreams</button>'
    '    <button class="lt" onclick="setTab(\'node_chat\',this)">&#128172; Chat</button>'
    '    <button class="lt" onclick="setTab(\'mesh_event\',this)">&#127760; Mesh</button>'
    '    <button class="lt" onclick="setTab(\'system\',this)">&#9881;&#65039; System</button>'
    '  </div>'
    '  <div class="filter-row">'
    '    <input class="inp" id="fNode" placeholder="Filter node..." oninput="refreshLogs()" style="width:150px"/>'
    '    <select class="sel" id="fStatus" onchange="refreshLogs()"><option value="">All</option><option>success</option><option>failed</option><option>warning</option><option>pending</option><option>info</option></select>'
    '    <input class="inp" id="fQ" placeholder="Search..." oninput="refreshLogs()" style="flex:1;min-width:140px"/>'
    '    <label style="display:flex;align-items:center;gap:5px;font-size:.72rem;color:var(--text-muted);cursor:pointer"><input type="checkbox" id="autoScroll" checked> Auto-scroll</label>'
    '    <button class="btn btn-danger btn-sm" onclick="clearLogs()">Clear</button>'
    '  </div>'
    '  <div class="log-wrap">'
    '    <div class="log-thead"><span>Timestamp</span><span>Type</span><span>Source</span><span>Target</span><span>Status</span><span>Summary</span></div>'
    '    <div class="log-body" id="logBody"><div class="log-empty">No events yet.</div></div>'
    '    <div class="log-footer"><span class="pulse"></span><span id="logCount">0 events</span><span style="margin-left:auto" id="logLast">&#8212;</span></div>'
    '  </div>'
    '</div>'
    '<div id="panel-diag" class="panel">'
    '  <div class="sec-title">Diagnostics</div>'
    '  <div class="diag-grid">'
    '    <div class="card"><div class="card-title">&#128225; Mesh Nodes Raw</div><button class="btn btn-ghost btn-sm" onclick="diagMeshNodes()">Fetch</button><div class="diag-out" id="dMesh">&#8212;</div></div>'
    '    <div class="card"><div class="card-title">&#129302; Ollama Status</div><button class="btn btn-success btn-sm" onclick="checkOllama()">Check</button><div class="diag-out" id="dOllama">&#8212;</div></div>'
    '    <div class="card"><div class="card-title">&#128173; Simulate Dream</div>'
    '      <div style="display:flex;gap:7px;flex-wrap:wrap;margin-bottom:8px"><input id="drNode" class="inp inp-mono" placeholder="node-id" style="width:120px"/><input id="drText" class="inp" placeholder="Dream text..." style="flex:1"/></div>'
    '      <button class="btn btn-dream btn-sm" onclick="sendDream()">Send Dream</button><div class="diag-out" id="dDream">&#8212;</div>'
    '    </div>'
    '    <div class="card"><div class="card-title">&#128172; Simulate Chat</div>'
    '      <div style="display:flex;gap:7px;flex-wrap:wrap;margin-bottom:8px"><input id="chFrom" class="inp inp-mono" placeholder="from" style="width:90px"/><input id="chTo" class="inp inp-mono" placeholder="to" style="width:90px"/><input id="chMsg" class="inp" placeholder="Message..." style="flex:1"/></div>'
    '      <button class="btn btn-chat btn-sm" onclick="sendChat()">Send Chat</button><div class="diag-out" id="dChat">&#8212;</div>'
    '    </div>'
    '    <div class="card"><div class="card-title">&#9881;&#65039; HB Status</div><button class="btn btn-ghost btn-sm" onclick="checkHb()">Refresh</button><div class="diag-out" id="dHb">&#8212;</div></div>'
    '    <div class="card"><div class="card-title">&#128202; Ping All Nodes</div><button class="btn btn-ghost btn-sm" onclick="pingAll()">Ping All</button><div class="diag-out" id="dPing">&#8212;</div></div>'
    '  </div>'
    '</div>'
    '<div id="panel-setup" class="panel">'
    '  <div class="sec-title">Setup</div>'
    '  <div class="card"><div class="card-title">&#129302; Ollama</div>'
    '    <div class="setup-grid">'
    '      <div class="fg"><label class="label">Ollama URL</label><input id="oUrl" class="inp inp-mono"/></div>'
    '      <div class="fg"><label class="label">Default Model</label><input id="oModel" class="inp inp-mono"/></div>'
    '    </div>'
    '  </div>'
    '  <div class="card"><div class="card-title">&#127760; Mesh Node Endpoints</div>'
    '    <div class="fg">'
    '      <label class="label">Node Endpoints (uno per riga)</label>'
    '      <textarea id="meshEps" class="inp" rows="4" style="resize:vertical"></textarea>'
    '      <span class="hint">Es: node:8084 oppure https://xxxx.ngrok-free.dev</span>'
    '    </div>'
    '  </div>'
    '  <div class="card"><div class="card-title">&#128273; Security</div>'
    '    <div class="fg"><label class="label">Shared Secret</label>'
    '      <div style="display:flex;gap:7px">'
    '        <input id="secVal" type="password" class="inp inp-mono" placeholder="Leave blank to keep"/>'
    '        <button class="btn btn-ghost btn-sm" onclick="toggleSec(this)">Show</button>'
    '        <button class="btn btn-warn btn-sm" onclick="rotateSecret()">&#8635; Rotate</button>'
    '      </div>'
    '      <span class="hint" id="rsAt"></span>'
    '    </div>'
    '  </div>'
    '  <div class="card">'
    '    <div class="legacy-toggle" onclick="toggleLegacy()">'
    '      <svg width="10" height="10" viewBox="0 0 10 10" fill="currentColor"><polygon points="2,1 9,5 2,9"/></svg>'
    '      <span>Legacy: Authority Server</span>'
    '    </div>'
    '    <div class="legacy-body" id="legacyBody">'
    '      <div class="legacy-warn">&#9888;&#65039; L\'authority e\' mantenuta per compatibilita\'.</div>'
    '      <div class="setup-grid">'
    '        <div class="fg"><label class="label">Authority URL</label><input id="aUrl" class="inp inp-mono"/></div>'
    '        <div class="fg" style="align-self:end"><button class="btn btn-ghost btn-sm" onclick="testAuthority()">&#128268; Test</button><div class="diag-out" id="sAuthTest" style="margin-top:6px;min-height:32px"></div></div>'
    '      </div>'
    '    </div>'
    '  </div>'
    '  <div class="setup-footer">'
    '    <button class="btn btn-ghost" onclick="loadCfg()">&#8634; Reset</button>'
    '    <button class="btn btn-primary" onclick="saveCfg()">&#128190; Save</button>'
    '  </div>'
    '  <div id="saveMsg"></div>'
    '</div>'
    '</main>'
    '<div class="modal-overlay" id="ollamaModal" onclick="if(event.target===this)closeModal()">'
    '  <div class="modal">'
    '    <div class="modal-title"><span class="ollama-dot" id="modalDot"></span>&#129302; Ollama<button class="modal-close" onclick="closeModal()">&#10005; Close</button></div>'
    '    <div id="modalUrl" style="font-size:.7rem;color:var(--text-muted);font-family:var(--font-mono);margin-bottom:8px"></div>'
    '    <div class="model-list" id="modelList"></div>'
    '    <div id="modalErr" style="font-size:.72rem;color:var(--error);display:none"></div>'
    '    <button class="btn btn-ghost btn-sm" style="margin-top:8px" onclick="checkOllamaModal()">&#8634; Refresh</button>'
    '  </div>'
    '</div>'
    '<script>'
    '(function(){var r=document.documentElement,btn=document.getElementById("themeBtn");var d="dark";r.setAttribute("data-theme",d);btn.addEventListener("click",function(){d=d==="dark"?"light":"dark";r.setAttribute("data-theme",d);});})();'
    'function tick(){document.getElementById("clock").textContent=new Date().toISOString().replace("T"," ").slice(0,19)+" UTC";}setInterval(tick,1000);tick();'
    'function showPanel(name,btn){'
    '  document.querySelectorAll(".panel").forEach(function(p){p.classList.remove("active");});'
    '  document.querySelectorAll("nav button").forEach(function(b){b.classList.remove("active");});'
    '  document.getElementById("panel-"+name).classList.add("active");'
    '  btn.classList.add("active");'
    '  if(name==="nodes")refreshNodes();'
    '  if(name==="tasks")refreshTaskHistory();'
    '  if(name==="logs")refreshLogs();'
    '  if(name==="setup")loadCfg();'
    '}'
    'async function refreshHbBar(){try{var d=await(await fetch("/hb/status")).json();document.getElementById("hbDot").className="hb-dot "+(d.running?"ok":"err");document.getElementById("hbCycle").textContent="#"+d.cycle;document.getElementById("hbTick").textContent=d.last_tick?d.last_tick.slice(11,19):"--";document.getElementById("hbNodes").textContent=d.nodes_seen&&d.nodes_seen.length?d.nodes_seen.join(", "):"none";document.getElementById("hbConn").textContent=d.last_conn?d.last_conn.slice(11,19):"--";document.getElementById("hbDream").textContent=d.last_dream?d.last_dream.slice(11,19):"--";}catch(e){}}'
    'setInterval(refreshHbBar,5000);refreshHbBar();'
    'function tierClass(t){return t==="root"?"tier-root":t==="hub"?"tier-hub":"tier-leaf";}'
    'function statusDotClass(s){return s==="active"?"active":s==="unreachable"?"unreachable":"unknown";}'
    'function formatUptime(s){if(s<60)return s+"s";if(s<3600)return Math.floor(s/60)+"m";return Math.floor(s/3600)+"h "+Math.floor((s%3600)/60)+"m";}'
    'async function refreshNodes(){try{var nodes=await(await fetch("/mesh/nodes")).json();var grid=document.getElementById("nodesGrid");document.getElementById("nodesCount").textContent=nodes.length+" node"+(nodes.length!==1?"s":"");if(!nodes.length){grid.innerHTML=\'<div class="nodes-empty">No nodes discovered yet.</div>\';return;}grid.innerHTML=nodes.map(function(n){var nid=n.node_id?n.node_id.slice(0,16)+"...":n.endpoint||"?";var tier=n.tier||"leaf";var uptime=n.uptime_s?formatUptime(n.uptime_s):"?";var caps=(n.capabilities||[]).join(", ")||"?";var ver=n.version||"?";var ep=n.endpoint||"";return\'<div class="node-card"><div class="nc-header"><span class="node-status-dot \'+statusDotClass(n.status)+\'"></span><span class="node-id" title="\'+escH(n.node_id||\'\')+\'">&#128187; \'+escH(nid)+\'</span><span class="tier-badge \'+tierClass(tier)+\'">\'+tier+\'</span></div><div class="node-meta"><span class="nm-label">Endpoint</span><span class="nm-val" title="\'+escH(ep)+\'">\'+escH(ep.replace("https://","").slice(0,30))+\'</span><span class="nm-label">Version</span><span class="nm-val">\'+escH(ver)+\'</span><span class="nm-label">Uptime</span><span class="nm-val">\'+uptime+\'</span><span class="nm-label">Peers</span><span class="nm-val">\'+( n.peers_active||0)+\' active</span><span class="nm-label">Caps</span><span class="nm-val">\'+escH(caps)+\'</span><span class="nm-label">VRAM</span><span class="nm-val">\'+( n.vram_gb||0)+\' GB</span></div><div class="node-peers"><span style="color:var(--text-faint);margin-right:4px">pubkey</span><span class="peer-tag" title="\'+escH(n.public_key||\'\')+\'">\'+escH((n.public_key||"").slice(0,20))+\'&hellip;</span></div></div>\';}).join("");}catch(e){document.getElementById("nodesGrid").innerHTML=\'<div class="nodes-empty">Error: \'+e.message+\'</div>\';}}'
    'setInterval(refreshNodes,15000);refreshNodes();'
    'function escH(s){return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}'
    'function statusBadge(s){var map={done:"ts-done",failed:"ts-failed",assigned:"ts-assigned",created:"ts-created"};var emoji={done:"&#10003;",failed:"&#10007;",assigned:"&#8987;",created:"&#9679;"};return\'<span class="task-status-badge \'+( map[s]||"ts-created")+\'">\'+( emoji[s]||"")+\' \'+s+\'</span>\';}'
    'function renderTaskCard(t){var id=t.id||"?";var node=(t.node||"").slice(0,16);var model=(t.payload&&t.payload.model)||"?";var prompt=(t.payload&&t.payload.prompt)||"";var status=t.status||"created";var resp=(t.result&&t.result.response)||t.error||"";var isError=status==="failed"||resp.toLowerCase().indexOf("[ollama error]")===0;var responseHTML=resp?(isError?\'<div class="task-error-box">\'+escH(resp)+\'</div>\':\'<div class="task-response-box">\'+escH(resp)+\'</div>\'):\'<div style="color:var(--text-faint);font-size:.75rem">Nessuna risposta.</div>\';var ts=(t.completed_at||t.created_at||"").slice(0,19).replace("T"," ");return\'<div class="task-card"><div class="task-header" onclick="toggleTaskBody(\\\'tb-\'+escH(id)+\'\\\')">\'+statusBadge(status)+\'<span class="task-id">#\'+escH(id)+\'</span><span class="task-model-badge">\'+escH(model)+\'</span><span class="task-node" title="\'+escH(t.node||"")+\'">\'+( node?"&#128187; "+node+"...":"")+\'</span><span class="task-ts">\'+escH(ts)+\'</span></div><div class="task-body" id="tb-\'+escH(id)+\'"><div class="task-section"><div class="task-section-title">Prompt</div><div class="task-prompt-box">\'+( escH(prompt)||"<em>vuoto</em>")+\'</div></div><div class="task-section"><div class="task-section-title">Risposta</div>\'+responseHTML+\'</div></div></div>\';}'
    'function toggleTaskBody(id){var el=document.getElementById(id);if(el)el.classList.toggle("open");}'
    'async function createTask(){var id=document.getElementById("tId").value.trim();var prompt=document.getElementById("tPrompt").value.trim();var model=document.getElementById("tModel").value.trim()||"phi3";if(!id){alert("Task ID obbligatorio");return;}document.getElementById("taskStatus").textContent="Creazione...";var r=await fetch("/task/create",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({task_id:id,prompt:prompt,model:model})});var d=await r.json();document.getElementById("taskStatus").textContent=d.message||"";refreshTaskHistory();}'
    'async function createAndAssign(){var id=document.getElementById("tId").value.trim();var prompt=document.getElementById("tPrompt").value.trim();var model=document.getElementById("tModel").value.trim()||"phi3";if(!id||!prompt){alert("Task ID e Prompt obbligatori");return;}document.getElementById("taskStatus").textContent="Esecuzione...";await fetch("/task/create",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({task_id:id,prompt:prompt,model:model})});var r=await fetch("/task/assign",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({task_id:id})});var d=await r.json();document.getElementById("taskStatus").textContent=d.error?"Errore: "+d.error:"Completato";refreshTaskHistory();}'
    'async function refreshTaskHistory(){var data=await(await fetch("/tasks")).json();var list=Object.values(data).reverse();document.getElementById("taskCount").textContent=list.length+" task"+(list.length!==1?"s":"");var hist=document.getElementById("taskHistory");if(!list.length){hist.innerHTML=\'<div class="tasks-empty">Nessun task ancora.</div>\';return;}hist.innerHTML=list.map(renderTaskCard).join("");var firstBody=hist.querySelector(".task-body");if(firstBody)firstBody.classList.add("open");}'
    'setInterval(refreshTaskHistory,5000);'
    'var curType="";'
    'function setTab(type,btn){curType=type;document.querySelectorAll(".lt").forEach(function(t){t.classList.remove("active");});btn.classList.add("active");refreshLogs();}'
    'async function refreshLogs(){var node=document.getElementById("fNode").value.trim();var status=document.getElementById("fStatus").value;var q=document.getElementById("fQ").value.trim();var url="/logs?";if(curType)url+="type="+curType+"&";if(node)url+="node="+encodeURIComponent(node)+"&";if(status)url+="status="+status+"&";if(q)url+="q="+encodeURIComponent(q)+"&";try{var logs=await(await fetch(url)).json();renderLogs(logs);}catch(e){}}'
    'function renderLogs(logs){var body=document.getElementById("logBody");document.getElementById("logCount").textContent=logs.length+" events";document.getElementById("logLast").textContent="Updated "+new Date().toISOString().slice(11,19)+" UTC";if(!logs.length){body.innerHTML=\'<div class="log-empty">No events.</div>\';return;}var stEmoji={success:"&#10003;",failed:"&#10007;",warning:"&#9888;",pending:"&#8987;",info:"&#9432;"};body.innerHTML=logs.map(function(l,i){return\'<div class="log-row" onclick="toggleD(\\\'ld\'+i+\'\\\')"><span class="ts">\'+escH(l.ts.replace("T"," ").slice(0,19))+\'</span><span><span class="tbadge tb-\'+l.type+\'">\'+escH(l.type.replace(/_/g," "))+\'</span></span><span class="nc">\'+escH(l.sourceNode||"--")+\'</span><span class="nc">\'+escH(l.targetNode||"--")+\'</span><span><span class="sbadge st-\'+l.status+\'">\'+( stEmoji[l.status]||"")+\' \'+escH(l.status)+\'</span></span><span class="summary">\'+escH(l.summary)+\'</span></div><div class="detail-row" id="ld\'+i+\'"><b>TraceID:</b> \'+escH(l.traceId)+\' | <b>ID:</b> \'+escH(l.id)+\' <b>Detail:</b> \'+escH(l.detail||"--")+\'</div>\';}).join("");if(document.getElementById("autoScroll").checked)body.scrollTop=body.scrollHeight;}'
    'function toggleD(id){var e=document.getElementById(id);if(e)e.classList.toggle("open");}'
    'async function clearLogs(){await fetch("/logs/clear",{method:"POST"});refreshLogs();}'
    'setInterval(refreshLogs,5000);'
    'async function diagMeshNodes(){document.getElementById("dMesh").textContent="...";var d=await(await fetch("/mesh/nodes")).json();document.getElementById("dMesh").textContent=JSON.stringify(d,null,2);}'
    'async function checkOllama(){document.getElementById("dOllama").textContent="...";var d=await(await fetch("/ollama/status")).json();document.getElementById("dOllama").textContent=JSON.stringify(d,null,2);}'
    'async function checkHb(){var d=await(await fetch("/hb/status")).json();document.getElementById("dHb").textContent=JSON.stringify(d,null,2);}'
    'async function sendDream(){var node=document.getElementById("drNode").value.trim()||"node-sim";var sum=document.getElementById("drText").value.trim()||"Autonomous cycle";var r=await fetch("/logs/add",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"dream",summary:sum,sourceNode:node,status:"info",detail:"Injected via Diagnostics"})});document.getElementById("dDream").textContent=JSON.stringify(await r.json(),null,2);}'
    'async function sendChat(){var from=document.getElementById("chFrom").value.trim()||"node-a";var to=document.getElementById("chTo").value.trim()||"node-b";var msg=document.getElementById("chMsg").value.trim()||"hello";var r=await fetch("/logs/add",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"node_chat",summary:from+" -> "+to+": "+msg,sourceNode:from,targetNode:to,status:"info",detail:"Injected"})});document.getElementById("dChat").textContent=JSON.stringify(await r.json(),null,2);}'
    'async function pingAll(){document.getElementById("dPing").textContent="Pinging...";var nodes=await(await fetch("/mesh/nodes")).json();if(!nodes.length){document.getElementById("dPing").textContent="No nodes.";return;}var res={};await Promise.all(nodes.map(async function(n){var ep=n.endpoint;var start=Date.now();try{await fetch("/mesh/node/"+encodeURIComponent(ep)+"/status",{signal:AbortSignal.timeout(3000)});res[ep]={status:"ok",latency:Date.now()-start+"ms"};}catch(e){res[ep]={status:"failed",error:e.message};}}));document.getElementById("dPing").textContent=JSON.stringify(res,null,2);}'
    'async function checkOllamaDot(){try{var d=await(await fetch("/ollama/status")).json();var dot=document.getElementById("ollamaDot"),lbl=document.getElementById("ollamaLabel");if(d.ok){dot.className="ollama-dot ok";lbl.textContent="Ollama - "+(d.models||[]).length+" models";lbl.style.color="var(--success)";}else{dot.className="ollama-dot err";lbl.textContent="Ollama offline";lbl.style.color="var(--error)";}}catch(e){}}'
    'setInterval(checkOllamaDot,30000);checkOllamaDot();'
    'function openOllamaModal(){document.getElementById("ollamaModal").classList.add("open");checkOllamaModal();}'
    'function closeModal(){document.getElementById("ollamaModal").classList.remove("open");}'
    'async function checkOllamaModal(){var d=await(await fetch("/ollama/status")).json();document.getElementById("modalUrl").textContent=d.url||"--";var dot=document.getElementById("modalDot"),list=document.getElementById("modelList"),err=document.getElementById("modalErr");if(d.ok){dot.className="ollama-dot ok";err.style.display="none";list.innerHTML=(!d.models||!d.models.length)?\'<div style="color:var(--text-muted);font-size:.72rem;padding:8px">No models loaded.</div>\':d.models.map(function(m){return\'<div class="model-item"><span class="model-dot"></span>\'+escH(m)+\'</div>\';}).join("");}else{dot.className="ollama-dot err";list.innerHTML="";err.textContent="Error: "+(d.error||"unreachable");err.style.display="block";}}'
    'async function loadCfg(){var c=await(await fetch("/config/advanced")).json();document.getElementById("oUrl").value=(c.ollama&&c.ollama.url)||"";document.getElementById("oModel").value=(c.ollama&&c.ollama.defaultModel)||"";document.getElementById("meshEps").value=(c.mesh&&c.mesh.nodeEndpoints||[]).join("\\n");document.getElementById("secVal").value="";document.getElementById("rsAt").textContent=(c.security&&c.security.secretRotatedAt)?"Last rotated: "+c.security.secretRotatedAt.slice(0,10):"";document.getElementById("aUrl").value=(c._authority&&c._authority.serverUrl)||"";}'
    'async function saveCfg(){var eps=document.getElementById("meshEps").value.split("\\n").map(function(s){return s.trim();}).filter(Boolean);var payload={ollama:{url:document.getElementById("oUrl").value,defaultModel:document.getElementById("oModel").value},mesh:{nodeEndpoints:eps},security:{sharedSecret:document.getElementById("secVal").value},_authority:{serverUrl:document.getElementById("aUrl").value}};var d=await(await fetch("/config/advanced",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)})).json();showMsg(d.ok?"Salvato":"Errore");}'
    'async function rotateSecret(){var d=await(await fetch("/config/secret/rotate",{method:"POST"})).json();if(d.ok){document.getElementById("secVal").value=d.secret;document.getElementById("secVal").type="text";showMsg("Secret ruotato");}}'
    'function toggleSec(btn){var i=document.getElementById("secVal");var show=i.type==="password";i.type=show?"text":"password";btn.textContent=show?"Hide":"Show";}'
    'function toggleLegacy(){document.getElementById("legacyBody").classList.toggle("open");}'
    'async function testAuthority(){document.getElementById("sAuthTest").textContent="Testing...";try{var cfg=await(await fetch("/config/advanced")).json();var r=await fetch(cfg._authority.serverUrl+"/health",{signal:AbortSignal.timeout(3000)});document.getElementById("sAuthTest").textContent="HTTP "+r.status+(r.ok?" OK":" ERROR");}catch(e){document.getElementById("sAuthTest").textContent="Error: "+e.message;}}'
    'function showMsg(m){var e=document.getElementById("saveMsg");e.textContent=m;setTimeout(function(){e.textContent="";},4000);}'
    '</script>'
    '</body></html>'
)

@app.route('/dashboard')
def dashboard():
    return Response(_DASHBOARD_HTML, mimetype='text/html; charset=utf-8')

def main():
    print("[control-plane] v0.2 starting on :8085")
    print(f"[control-plane] known endpoints: {_known_endpoints}")
    hb=threading.Thread(target=heartbeat_loop,daemon=True)
    hb.start()
    app.run(host="0.0.0.0",port=8085)

if __name__=="__main__":
    main()
