# registry/registry.py
# HyperSpace AGI v1.02 — Public Registry
# feat: landing page pubblica, /dashboard nodi live, /nodes/active TTL-filtered

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from time import time
from threading import Lock
import uvicorn
import os

app = FastAPI(title="HyperSpace Registry", version="1.02")
lock = Lock()

TTL_SECONDS        = int(os.getenv("NODE_TTL", "300"))   # default 300s = 20 cicli heartbeat
REGISTRY_PUBLIC_URL = os.getenv("REGISTRY_PUBLIC_URL", "https://sanctuary-mower-plated.ngrok-free.dev")

class NodeRegistration(BaseModel):
    node_id: str
    public_address: str
    role: Optional[str] = "worker"
    metadata: Dict[str, str] = Field(default_factory=dict)

class NodeRecord(NodeRegistration):
    last_seen: float

nodes: Dict[str, NodeRecord] = {}


def prune_stale_nodes():
    now = time()
    stale = [nid for nid, rec in nodes.items() if now - rec.last_seen > TTL_SECONDS]
    for nid in stale:
        del nodes[nid]


def _active_nodes() -> list:
    with lock:
        prune_stale_nodes()
        return [
            {
                "node_id":        n.node_id,
                "public_address": n.public_address,
                "role":           n.role,
                "metadata":       n.metadata,
                "last_seen":      n.last_seen,
                "uptime_s":       int(n.metadata.get("uptime_s", "0")),
                "vram_gb":        float(n.metadata.get("vram_gb", "0")),
                "tier":           n.metadata.get("tier", "leaf"),
                "version":        n.metadata.get("version", ""),
            }
            for n in nodes.values()
        ]


# ── API ENDPOINTS ─────────────────────────────────────────────────────────────

@app.post("/register", summary="Registra o aggiorna un nodo")
def register(node: NodeRegistration):
    with lock:
        nodes[node.node_id] = NodeRecord(**node.model_dump(), last_seen=time())
    return {"ok": True, "node_id": node.node_id}


@app.post("/heartbeat", summary="Aggiorna last_seen del nodo")
def heartbeat(node_id: str):
    with lock:
        if node_id not in nodes:
            raise HTTPException(status_code=404, detail=f"Nodo '{node_id}' non trovato")
        nodes[node_id].last_seen = time()
    return {"ok": True, "node_id": node_id}


@app.delete("/nodes/{node_id}", summary="Deregistra un nodo")
def deregister(node_id: str):
    with lock:
        if node_id not in nodes:
            raise HTTPException(status_code=404, detail=f"Nodo '{node_id}' non trovato")
        del nodes[node_id]
    return {"ok": True, "node_id": node_id}


@app.get("/nodes", response_model=List[NodeRecord], summary="Lista nodi attivi")
def list_nodes():
    with lock:
        prune_stale_nodes()
        return list(nodes.values())


@app.get("/nodes/active", summary="Lista nodi attivi per auto-discovery (usata dai nodi al boot)")
def list_nodes_active():
    """Ritorna solo i nodi vivi con TTL — usato da node/main.py al boot per auto-discovery."""
    return {"nodes": _active_nodes(), "ttl_seconds": TTL_SECONDS}


@app.get("/health")
def health():
    with lock:
        prune_stale_nodes()
    return {"status": "ok", "nodes_count": len(nodes), "ttl_seconds": TTL_SECONDS}


# ── LANDING PAGE ──────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def landing():
    active = _active_nodes()
    node_count = len(active)
    badge_color = "#00c896" if node_count > 0 else "#ff4757"
    nodes_html = ""
    for n in active:
        tier_colors = {"root": "#a78bfa", "hub": "#38bdf8", "leaf": "#4ade80"}
        tier_color = tier_colors.get(n["tier"], "#94a3b8")
        vram = n["vram_gb"]
        vram_str = f"{vram:.0f} GB VRAM" if vram > 0 else "CPU only"
        uptime_h = n["uptime_s"] // 3600
        nodes_html += f"""
        <div class="node-card">
          <div class="node-header">
            <span class="dot" style="background:{badge_color}"></span>
            <span class="node-id">{n['node_id'][:16]}…</span>
            <span class="tier-badge" style="background:{tier_color}22;color:{tier_color};border:1px solid {tier_color}44">{n['tier'].upper()}</span>
          </div>
          <div class="node-meta">{vram_str} &nbsp;·&nbsp; up {uptime_h}h &nbsp;·&nbsp; v{n['version']}</div>
          <div class="node-ep">{n['public_address']}</div>
        </div>"""

    if not nodes_html:
        nodes_html = '<div class="empty">No active nodes yet — be the first to join!</div>'

    return HTMLResponse(content=f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>HyperSpace AGI — Mesh Registry</title>
  <link href="https://api.fontshare.com/v2/css?f[]=satoshi@400,500,700&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    :root {{
      --bg: #0d0d10; --surface: #15151a; --surface2: #1c1c23;
      --border: rgba(255,255,255,0.07); --text: #e2e2e8; --muted: #6b6b7a;
      --accent: #00c896; --accent2: #38bdf8;
    }}
    html {{ scroll-behavior: smooth; }}
    body {{ font-family: 'Satoshi', sans-serif; background: var(--bg); color: var(--text);
            min-height: 100dvh; line-height: 1.6; }}

    /* NAV */
    nav {{ display: flex; align-items: center; justify-content: space-between;
           padding: 1.25rem 2rem; border-bottom: 1px solid var(--border);
           position: sticky; top: 0; background: var(--bg); z-index: 10; }}
    .logo {{ display: flex; align-items: center; gap: .6rem; font-weight: 700; font-size: 1.1rem; }}
    .logo svg {{ color: var(--accent); }}
    .nav-links {{ display: flex; gap: 1.5rem; }}
    .nav-links a {{ color: var(--muted); text-decoration: none; font-size: .9rem;
                    transition: color .2s; }}
    .nav-links a:hover {{ color: var(--text); }}

    /* HERO */
    .hero {{ max-width: 860px; margin: 0 auto; padding: 5rem 2rem 3rem; text-align: center; }}
    .badge {{ display: inline-flex; align-items: center; gap: .4rem;
              background: rgba(0,200,150,.1); color: var(--accent);
              border: 1px solid rgba(0,200,150,.25); border-radius: 999px;
              font-size: .8rem; font-weight: 500; padding: .3rem .9rem;
              margin-bottom: 1.5rem; }}
    .badge .pulse {{ width: 7px; height: 7px; background: var(--accent);
                     border-radius: 50%; animation: pulse 2s infinite; }}
    @keyframes pulse {{ 0%,100%{{opacity:1;transform:scale(1)}} 50%{{opacity:.4;transform:scale(1.4)}} }}
    h1 {{ font-size: clamp(2.2rem, 5vw, 3.8rem); font-weight: 700; line-height: 1.15;
          margin-bottom: 1.2rem; }}
    h1 span {{ background: linear-gradient(135deg, var(--accent), var(--accent2));
               -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
    .hero p {{ color: var(--muted); font-size: 1.1rem; max-width: 560px; margin: 0 auto 2.5rem; }}
    .cta-row {{ display: flex; gap: 1rem; justify-content: center; flex-wrap: wrap; }}
    .btn {{ padding: .7rem 1.6rem; border-radius: 8px; font-size: .95rem;
             font-weight: 600; cursor: pointer; text-decoration: none;
             transition: all .2s; }}
    .btn-primary {{ background: var(--accent); color: #0d0d10; }}
    .btn-primary:hover {{ background: #00e0a8; transform: translateY(-1px); }}
    .btn-ghost {{ background: transparent; color: var(--text);
                  border: 1px solid var(--border); }}
    .btn-ghost:hover {{ border-color: var(--accent); color: var(--accent); }}

    /* STATS BAR */
    .stats-bar {{ display: flex; justify-content: center; gap: 3rem;
                  padding: 2rem; border-top: 1px solid var(--border);
                  border-bottom: 1px solid var(--border); }}
    .stat {{ text-align: center; }}
    .stat-value {{ font-size: 1.8rem; font-weight: 700; color: var(--accent); }}
    .stat-label {{ font-size: .8rem; color: var(--muted); text-transform: uppercase;
                   letter-spacing: .05em; }}

    /* NODES SECTION */
    .section {{ max-width: 960px; margin: 0 auto; padding: 3rem 2rem; }}
    .section-title {{ font-size: 1.2rem; font-weight: 600; margin-bottom: 1.5rem;
                      display: flex; align-items: center; gap: .6rem; }}
    .section-title .dot {{ width: 8px; height: 8px; background: var(--accent);
                           border-radius: 50%; }}
    .nodes-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(280px,1fr));
                   gap: 1rem; }}
    .node-card {{ background: var(--surface); border: 1px solid var(--border);
                  border-radius: 12px; padding: 1.2rem; transition: border-color .2s; }}
    .node-card:hover {{ border-color: rgba(0,200,150,.3); }}
    .node-header {{ display: flex; align-items: center; gap: .6rem; margin-bottom: .5rem; }}
    .dot {{ width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }}
    .node-id {{ font-weight: 600; font-size: .95rem; flex: 1; }}
    .tier-badge {{ font-size: .7rem; font-weight: 600; padding: .15rem .5rem;
                   border-radius: 999px; }}
    .node-meta {{ font-size: .82rem; color: var(--muted); margin-bottom: .3rem; }}
    .node-ep {{ font-size: .78rem; color: var(--accent2); word-break: break-all; }}
    .empty {{ color: var(--muted); font-size: .95rem; padding: 2rem;
              background: var(--surface); border-radius: 12px;
              border: 1px dashed var(--border); text-align: center; }}

    /* JOIN SECTION */
    .join {{ background: var(--surface); border: 1px solid var(--border);
             border-radius: 16px; padding: 2rem; margin-top: 1rem; }}
    .join h3 {{ font-size: 1.1rem; margin-bottom: 1rem; }}
    pre {{ background: var(--bg); border: 1px solid var(--border); border-radius: 8px;
           padding: 1.2rem; font-size: .82rem; overflow-x: auto; line-height: 1.7;
           color: #a3e7d4; }}
    .comment {{ color: #4a5568; }}

    /* FOOTER */
    footer {{ text-align: center; padding: 2rem; color: var(--muted); font-size: .82rem;
              border-top: 1px solid var(--border); margin-top: 3rem; }}
    footer a {{ color: var(--accent); text-decoration: none; }}

    @media (max-width: 640px) {{
      .stats-bar {{ gap: 1.5rem; flex-wrap: wrap; }}
      .cta-row {{ flex-direction: column; align-items: center; }}
    }}
  </style>
</head>
<body>

<nav>
  <div class="logo">
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
    </svg>
    HyperSpace AGI
  </div>
  <div class="nav-links">
    <a href="#nodes">Nodes</a>
    <a href="#join">Join</a>
    <a href="/dashboard">Dashboard</a>
    <a href="https://github.com/opodark/hyperspace-agi-1.02" target="_blank">GitHub</a>
  </div>
</nav>

<section class="hero">
  <div class="badge"><span class="pulse"></span> {node_count} node{'s' if node_count != 1 else ''} active</div>
  <h1>Decentralized <span>AI mesh</span><br>for everyone</h1>
  <p>Run local AI agents on consumer hardware. Join the HyperSpace mesh in minutes — no cloud, no cost, just your machine.</p>
  <div class="cta-row">
    <a class="btn btn-primary" href="#join">Join the mesh</a>
    <a class="btn btn-ghost" href="https://github.com/opodark/hyperspace-agi-1.02" target="_blank">View on GitHub</a>
  </div>
</section>

<div class="stats-bar">
  <div class="stat"><div class="stat-value" id="node-count">{node_count}</div><div class="stat-label">Active Nodes</div></div>
  <div class="stat"><div class="stat-value">P2P</div><div class="stat-label">Architecture</div></div>
  <div class="stat"><div class="stat-value">gzip</div><div class="stat-label">Memory Store</div></div>
  <div class="stat"><div class="stat-value">SLM</div><div class="stat-label">Local Inference</div></div>
</div>

<section class="section" id="nodes">
  <div class="section-title"><span class="dot"></span> Active Nodes</div>
  <div class="nodes-grid" id="nodes-grid">{nodes_html}</div>
</section>

<section class="section" id="join">
  <div class="section-title"><span class="dot"></span> Join the Mesh</div>
  <div class="join">
    <h3>3 steps to connect your machine</h3>
    <pre><span class="comment"># 1. Clone the repo</span>
git clone https://github.com/opodark/hyperspace-agi-1.02
cd hyperspace-agi-1.02

<span class="comment"># 2. Configure .env (minimal)</span>
cp .env.example .env
<span class="comment"># Edit .env — set PUBLIC_ENDPOINT to your ngrok/public URL</span>
PUBLIC_ENDPOINT=https://your-tunnel.ngrok-free.app
REGISTRY_URL={REGISTRY_PUBLIC_URL}

<span class="comment"># 3. Start</span>
docker compose up -d

<span class="comment"># You're in the mesh. Check:</span>
curl {REGISTRY_PUBLIC_URL}/nodes/active</pre>
  </div>
</section>

<footer>
  HyperSpace AGI v1.02 &nbsp;·&nbsp;
  Registry: <a href="{REGISTRY_PUBLIC_URL}">{REGISTRY_PUBLIC_URL}</a> &nbsp;·&nbsp;
  <a href="https://github.com/opodark/hyperspace-agi-1.02" target="_blank">GitHub</a>
</footer>

<script>
  // Auto-refresh nodi ogni 15s senza ricaricare la pagina
  async function refreshNodes() {{
    try {{
      const r = await fetch('/nodes/active');
      const data = await r.json();
      const nodes = data.nodes || [];
      document.getElementById('node-count').textContent = nodes.length;
      const grid = document.getElementById('nodes-grid');
      if (nodes.length === 0) {{
        grid.innerHTML = '<div class="empty">No active nodes yet — be the first to join!</div>';
        return;
      }}
      const tierColors = {{root:'#a78bfa',hub:'#38bdf8',leaf:'#4ade80'}};
      grid.innerHTML = nodes.map(n => {{
        const tc = tierColors[n.tier] || '#94a3b8';
        const vram = n.vram_gb > 0 ? `${{n.vram_gb.toFixed(0)}} GB VRAM` : 'CPU only';
        const uptime = Math.floor(n.uptime_s / 3600);
        return `
          <div class="node-card">
            <div class="node-header">
              <span class="dot" style="background:#00c896"></span>
              <span class="node-id">${{n.node_id.slice(0,16)}}…</span>
              <span class="tier-badge" style="background:${{tc}}22;color:${{tc}};border:1px solid ${{tc}}44">${{n.tier.toUpperCase()}}</span>
            </div>
            <div class="node-meta">${{vram}} · up ${{uptime}}h · v${{n.version}}</div>
            <div class="node-ep">${{n.public_address}}</div>
          </div>`;
      }}).join('');
    }} catch(e) {{}}
  }}
  setInterval(refreshNodes, 15000);
</script>

</body>
</html>""")


# ── DASHBOARD PUBBLICA ────────────────────────────────────────────────────────

@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    active = _active_nodes()
    rows = ""
    for n in active:
        uptime_h = n["uptime_s"] // 3600
        vram_str = f"{n['vram_gb']:.0f} GB" if n["vram_gb"] > 0 else "CPU"
        rows += f"<tr><td>{n['node_id'][:20]}…</td><td>{n['tier'].upper()}</td><td>{vram_str}</td><td>{uptime_h}h</td><td>{n['public_address']}</td><td>{n['version']}</td></tr>"
    if not rows:
        rows = '<tr><td colspan="6" style="text-align:center;color:#6b6b7a">No active nodes</td></tr>'
    return HTMLResponse(content=f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <title>HyperSpace — Node Dashboard</title>
  <link href="https://api.fontshare.com/v2/css?f[]=satoshi@400,500,700&display=swap" rel="stylesheet">
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:'Satoshi',sans-serif;background:#0d0d10;color:#e2e2e8;padding:2rem}}
    h1{{font-size:1.4rem;margin-bottom:.3rem}}p{{color:#6b6b7a;font-size:.85rem;margin-bottom:1.5rem}}
    table{{width:100%;border-collapse:collapse;background:#15151a;border-radius:12px;overflow:hidden}}
    th{{background:#1c1c23;padding:.7rem 1rem;text-align:left;font-size:.78rem;color:#6b6b7a;text-transform:uppercase;letter-spacing:.05em}}
    td{{padding:.75rem 1rem;border-top:1px solid rgba(255,255,255,.06);font-size:.88rem}}
    .refresh{{font-size:.75rem;color:#00c896;margin-top:.8rem}}
  </style>
</head>
<body>
  <h1>⬡ HyperSpace Node Dashboard</h1>
  <p>Auto-refresh every 10s &nbsp;·&nbsp; {len(active)} active nodes &nbsp;·&nbsp; TTL {TTL_SECONDS}s</p>
  <table>
    <thead><tr><th>Node ID</th><th>Tier</th><th>VRAM</th><th>Uptime</th><th>Endpoint</th><th>Version</th></tr></thead>
    <tbody id="tbody">{rows}</tbody>
  </table>
  <div class="refresh" id="ts">Last refresh: now</div>
  <script>
    async function refresh(){{
      const r=await fetch('/nodes/active');
      const {{nodes}}=await r.json();
      const tc={{root:'#a78bfa',hub:'#38bdf8',leaf:'#4ade80'}};
      document.getElementById('tbody').innerHTML=nodes.length
        ?nodes.map(n=>`<tr><td>${{n.node_id.slice(0,20)}}…</td><td>${{n.tier.toUpperCase()}}</td><td>${{n.vram_gb>0?n.vram_gb.toFixed(0)+' GB':'CPU'}}</td><td>${{Math.floor(n.uptime_s/3600)}}h</td><td>${{n.public_address}}</td><td>${{n.version}}</td></tr>`).join('')
        :'<tr><td colspan="6" style="text-align:center;color:#6b6b7a">No active nodes</td></tr>';
      document.getElementById('ts').textContent='Last refresh: '+new Date().toLocaleTimeString();
    }}
    setInterval(refresh,10000);
  </script>
</body></html>""")


if __name__ == "__main__":
    port = int(os.getenv("REGISTRY_PORT", "8086"))
    uvicorn.run("registry:app", host="0.0.0.0", port=port, reload=False)
