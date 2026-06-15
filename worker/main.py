from fastapi import FastAPI
import asyncio
import httpx
import os
import threading
import time
import uuid

app = FastAPI()

# -------- CONFIG --------
WORKER_ID        = os.getenv("WORKER_ID", f"worker-{uuid.uuid4().hex[:6]}")
WORKER_HOSTNAME  = os.getenv("WORKER_HOSTNAME", WORKER_ID)  # Docker service name per DNS
WORKER_PORT      = int(os.getenv("WORKER_PORT", 8084))
AUTHORITY_URL    = os.getenv("AUTHORITY_URL", "http://authority:8080")
OLLAMA_URL       = os.getenv("OLLAMA_URL", "http://ollama:11434")
DEFAULT_MODEL    = os.getenv("OLLAMA_MODEL", "phi3")
HEARTBEAT_EVERY  = int(os.getenv("HEARTBEAT_EVERY", 30))

_registered = False

# -------- OLLAMA HELPERS --------

async def ollama_generate(prompt: str, model: str = DEFAULT_MODEL) -> str:
    """Chiama Ollama /api/generate e restituisce la risposta completa."""
    payload = {
        "model":  model,
        "prompt": prompt,
        "stream": False,
    }
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            r = await client.post(f"{OLLAMA_URL}/api/generate", json=payload)
            r.raise_for_status()
            data = r.json()
            return data.get("response", "").strip()
    except Exception as e:
        return f"[OLLAMA ERROR] {e}"


async def ollama_health() -> dict:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{OLLAMA_URL}/api/tags")
            return {"ok": True, "models": [m["name"] for m in r.json().get("models", [])]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# -------- AUTHORITY REGISTRATION --------

def register_on_authority():
    global _registered
    for attempt in range(20):
        try:
            import requests as req_lib
            r = req_lib.post(
                f"{AUTHORITY_URL}/register",
                json={
                    "node_id":      WORKER_ID,
                    "host":         WORKER_HOSTNAME,  # hostname Docker-resolvable (service name)
                    "port":         WORKER_PORT,
                    "capabilities": ["ollama", "execute"],
                },
                timeout=5,
            )
            if r.status_code == 200:
                print(f"[WORKER:{WORKER_ID}] registered on authority (host={WORKER_HOSTNAME})")
                _registered = True
                return
        except Exception as e:
            print(f"[WORKER:{WORKER_ID}] register attempt {attempt+1} failed: {e}")
        sleep_time = min(3 + attempt * 2, 30)
        time.sleep(sleep_time)
    print(f"[WORKER:{WORKER_ID}] could not register after 20 attempts")


def heartbeat_loop():
    """Manda heartbeat all'authority ogni HEARTBEAT_EVERY secondi."""
    register_on_authority()
    while True:
        try:
            import requests as req_lib
            req_lib.post(
                f"{AUTHORITY_URL}/heartbeat",
                json={"node_id": WORKER_ID},
                timeout=5,
            )
        except Exception as e:
            print(f"[WORKER:{WORKER_ID}] heartbeat error: {e}")
        time.sleep(HEARTBEAT_EVERY)


# -------- FASTAPI ENDPOINTS --------

@app.on_event("startup")
async def startup_event():
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()


@app.post("/execute")
async def execute_task(task: dict):
    task_id = task.get("task_id", "unknown")
    prompt  = task.get("prompt") or task.get("payload", {}).get("prompt") or f"Esegui il task: {task_id}"
    model   = task.get("model") or task.get("payload", {}).get("model") or DEFAULT_MODEL

    print(f"[WORKER:{WORKER_ID}] executing task {task_id} — model={model}")

    response_text = await ollama_generate(prompt, model)

    result = {
        "worker":   WORKER_ID,
        "task_id":  task_id,
        "status":   "done",
        "model":    model,
        "response": response_text,
    }
    print(f"[WORKER:{WORKER_ID}] task {task_id} done ({len(response_text)} chars)")
    return result


@app.get("/status")
def status():
    return {
        "worker_id":      WORKER_ID,
        "worker_hostname": WORKER_HOSTNAME,
        "running":        True,
        "registered":     _registered,
        "authority":      AUTHORITY_URL,
        "ollama_url":     OLLAMA_URL,
        "default_model":  DEFAULT_MODEL,
    }


@app.get("/ollama/health")
async def check_ollama():
    return await ollama_health()


@app.get("/ollama/models")
async def list_models():
    h = await ollama_health()
    if h["ok"]:
        return {"models": h.get("models", [])}
    return {"error": h.get("error")}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=WORKER_PORT)
