# control-plane/main.py
# HyperSpace AGI v0.2 — Control Plane + Dashboard

from flask import Flask, request, jsonify, send_from_directory
import os, threading, time, requests, json, uuid, random
from datetime import datetime

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# CONFIG
NODE_ENDPOINTS = [e.strip() for e in os.getenv("NODE_ENDPOINTS","node:8084").split(",") if e.strip()]
OLLAMA_URL    = os.getenv("OLLAMA_URL","http://host.docker.internal:11434")
DEFAULT_MODEL = os.getenv("OLLAMA_MODEL","phi3")
REGISTRY_URL       = os.getenv("REGISTRY_URL","http://registry:8086")
_AUTHORITY_URL     = os.getenv("AUTHORITY_URL","http://authority:8080")
_AUTHORITY_ENABLED = os.getenv("AUTHORITY_ENABLED","false").lower()=="true"
LOG_LIMIT = 500

# STATE
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

# HELPERS
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

# LOG
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

# MESH
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

# REGISTRY PROXY
@app.route('/registry/nodes')
def registry_nodes():
    try:
        r=requests.get(f"{REGISTRY_URL}/nodes",timeout=5)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error":str(e),"registry_url":REGISTRY_URL}), 503

@app.route('/registry/health')
def registry_health():
    try:
        r=requests.get(f"{REGISTRY_URL}/health",timeout=3)
        return jsonify({"ok":r.status_code==200,"status":r.status_code})
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}), 503

# CONFIG
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

# TASKS
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

# HEARTBEAT
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

# DASHBOARD — served from standalone dashboard.html file
@app.route('/')
def dashboard():
    return send_from_directory(BASE_DIR, 'dashboard.html')

@app.route('/dashboard')
def dashboard_alias():
    return send_from_directory(BASE_DIR, 'dashboard.html')

# STARTUP
if __name__ == '__main__':
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=8085, debug=False)
else:
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()
