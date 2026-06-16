# tasks_view.py — Task History HTML panel (iniettato nella dashboard)
# Importato da main.py: from tasks_view import TASKS_PANEL_HTML, tasks_api_extra

TASKS_PANEL_HTML = r"""
<div id="panel-tasks" class="panel">
  <div class="sec-title">Task History</div>

  <!-- Form crea task -->
  <div class="card">
    <div class="card-title">&#x1F680; Nuovo Task</div>
    <div class="task-form">
      <div class="fg">
        <label class="label">Task ID</label>
        <input id="tId" class="inp inp-mono" placeholder="task-001"/>
      </div>
      <div class="fg">
        <label class="label">Modello</label>
        <input id="tModel" class="inp inp-mono" placeholder="phi3" value="phi3"/>
      </div>
      <div class="fg" style="grid-column:span 2">
        <label class="label">Prompt</label>
        <textarea id="tPrompt" class="inp" rows="3" placeholder="Scrivi il prompt..." style="resize:vertical"></textarea>
      </div>
    </div>
    <div class="row" style="margin-top:10px">
      <button class="btn btn-primary" onclick="createAndAssign()">&#9654; Esegui</button>
      <button class="btn btn-ghost" onclick="createTask()">Solo crea</button>
      <span class="task-status-label" id="taskStatus"></span>
    </div>
  </div>

  <!-- History -->
  <div class="row" style="justify-content:space-between">
    <span style="font-size:.7rem;color:var(--text-muted)" id="taskCount">0 tasks</span>
    <button class="btn btn-ghost btn-sm" onclick="refreshTaskHistory()">&#x21BA; Refresh</button>
  </div>

  <div id="taskHistory">
    <div class="tasks-empty">Nessun task ancora. Crea il primo sopra.</div>
  </div>
</div>

<style>
.task-form{display:grid;grid-template-columns:1fr 1fr;gap:10px}
@media(max-width:640px){.task-form{grid-template-columns:1fr}}
.task-status-label{font-size:.75rem;color:var(--text-muted);margin-left:4px}
.task-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);overflow:hidden;margin-bottom:10px;transition:border-color var(--tr)}
.task-card:hover{border-color:var(--primary)}
.task-header{display:flex;align-items:center;gap:10px;padding:12px 15px;cursor:pointer;background:var(--surface2)}
.task-id{font-family:var(--font-mono);font-size:.78rem;font-weight:700;color:var(--primary);flex:0 0 auto}
.task-node{font-family:var(--font-mono);font-size:.68rem;color:var(--text-muted);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.task-model-badge{background:var(--surface3);border:1px solid var(--border);border-radius:99px;padding:2px 9px;font-size:.65rem;font-family:var(--font-mono);color:var(--text-muted);flex:0 0 auto}
.task-status-badge{flex:0 0 auto;padding:3px 9px;border-radius:99px;font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em}
.ts-done{background:var(--success-bg);color:var(--success)}
.ts-failed{background:var(--error-bg);color:var(--error)}
.ts-assigned{background:var(--info-bg);color:var(--info)}
.ts-created{background:var(--surface3);color:var(--text-muted)}
.task-ts{font-size:.63rem;color:var(--text-faint);flex:0 0 auto}
.task-body{display:none;padding:14px 15px;border-top:1px solid var(--divider)}
.task-body.open{display:block}
.task-section{margin-bottom:12px}
.task-section-title{font-size:.62rem;font-weight:700;text-transform:uppercase;letter-spacing:.09em;color:var(--text-muted);margin-bottom:6px}
.task-prompt-box{background:var(--surface2);border:1px solid var(--divider);border-radius:var(--radius);padding:10px 12px;font-size:.8rem;color:var(--text);line-height:1.6;white-space:pre-wrap}
.task-response-box{background:var(--surface3);border:1px solid var(--divider);border-radius:var(--radius);padding:10px 12px;font-size:.8rem;color:var(--text);line-height:1.7;white-space:pre-wrap;max-height:320px;overflow-y:auto}
.task-error-box{background:var(--error-bg);border:1px solid var(--error);border-radius:var(--radius);padding:10px 12px;font-size:.75rem;color:var(--error);font-family:var(--font-mono);white-space:pre-wrap}
.tasks-empty{padding:40px;text-align:center;color:var(--text-faint);font-size:.8rem}
</style>

<script>
async function createTask(){
  const id=document.getElementById('tId').value.trim();
  const prompt=document.getElementById('tPrompt').value.trim();
  const model=document.getElementById('tModel').value.trim()||'phi3';
  if(!id){alert('Task ID obbligatorio');return;}
  document.getElementById('taskStatus').textContent='Creazione...';
  const r=await fetch('/task/create',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({task_id:id,prompt,model})});
  const d=await r.json();
  document.getElementById('taskStatus').textContent=d.message||JSON.stringify(d);
  refreshTaskHistory();
}

async function createAndAssign(){
  const id=document.getElementById('tId').value.trim();
  const prompt=document.getElementById('tPrompt').value.trim();
  const model=document.getElementById('tModel').value.trim()||'phi3';
  if(!id){alert('Task ID obbligatorio');return;}
  if(!prompt){alert('Prompt obbligatorio');return;}
  document.getElementById('taskStatus').textContent='\u23F3 Esecuzione in corso...';
  await fetch('/task/create',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({task_id:id,prompt,model})});
  const r=await fetch('/task/assign',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({task_id:id})});
  const d=await r.json();
  document.getElementById('taskStatus').textContent=d.error?'\u274C '+d.error:'\u2705 Completato';
  refreshTaskHistory();
}

function escH(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

function statusBadge(s){
  const map={done:'ts-done',failed:'ts-failed',assigned:'ts-assigned',created:'ts-created'};
  const emoji={done:'\u2705',failed:'\u274C',assigned:'\u23F3',created:'\u2022'};
  return `<span class="task-status-badge ${map[s]||'ts-created'}">${emoji[s]||''} ${s}</span>`;
}

function renderTaskCard(t){
  const id=t.id||'?';
  const node=(t.node||'').slice(0,16);
  const model=t.payload?.model||'?';
  const prompt=t.payload?.prompt||'';
  const status=t.status||'created';
  const resp=t.result?.response||'';
  const isError=resp.toLowerCase().startsWith('[ollama error]')||resp.toLowerCase().startsWith('error');
  const responseHTML=resp
    ?(isError
      ?`<div class="task-error-box">${escH(resp)}</div>`
      :`<div class="task-response-box">${escH(resp)}</div>`)
    :'<div style="color:var(--text-faint);font-size:.75rem">Nessuna risposta.</div>';
  return `
    <div class="task-card" id="tc-${escH(id)}">
      <div class="task-header" onclick="toggleTaskBody('tb-${escH(id)}')">
        <span class="task-id">#${escH(id)}</span>
        <span class="task-model-badge">${escH(model)}</span>
        ${statusBadge(status)}
        <span class="task-node" title="${escH(t.node||'')}">${node?'\uD83D\uDCBB '+node+'\u2026':''}</span>
      </div>
      <div class="task-body" id="tb-${escH(id)}">
        <div class="task-section">
          <div class="task-section-title">Prompt</div>
          <div class="task-prompt-box">${escH(prompt)||'<em>vuoto</em>'}</div>
        </div>
        <div class="task-section">
          <div class="task-section-title">Risposta</div>
          ${responseHTML}
        </div>
      </div>
    </div>`;
}

function toggleTaskBody(id){
  const el=document.getElementById(id);
  if(el)el.classList.toggle('open');
}

async function refreshTaskHistory(){
  const data=await(await fetch('/tasks')).json();
  const list=Object.values(data).reverse();
  document.getElementById('taskCount').textContent=list.length+' task'+(list.length!==1?'s':'');
  const hist=document.getElementById('taskHistory');
  if(!list.length){
    hist.innerHTML='<div class="tasks-empty">Nessun task ancora. Crea il primo sopra.</div>';
    return;
  }
  hist.innerHTML=list.map(renderTaskCard).join('');
  // Apri automaticamente l'ultimo task
  const firstBody=hist.querySelector('.task-body');
  if(firstBody)firstBody.classList.add('open');
}
setInterval(refreshTaskHistory,5000);
refreshTaskHistory();
</script>
"""
