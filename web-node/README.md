# Web Node — Technical Specification

The **Web Node** is the browser-first execution path for Hyperspace AGI 1.03.

It allows devices that cannot run Docker or heavy local runtimes to participate in the mesh by executing lightweight, safe tasks directly in the browser.

## Goals

- Enable participation from low-resource devices (laptops, tablets, managed corporate browsers)
- Provide a low-friction onboarding path (no Docker, no model downloads)
- Execute constrained, safe tasks only
- Register capabilities dynamically with the Control Plane

## Target Tasks (Phase 1)

| Task                | Type          | Notes                              |
|---------------------|---------------|------------------------------------|
| Translation         | Text          | Small context windows              |
| Embeddings          | Vector        | Short documents                    |
| Summarization       | Text          | Extractive or abstractive (small)  |
| Moderation          | Classification| Content safety / policy checks     |
| Simple Validation   | Structured    | JSON schema validation, etc.       |

**Important**: Heavy inference or long-running tasks are **not** routed to web nodes.

## Architecture

```
Browser (Web Node)
    │
    ├── Registers with Control Plane (via Registry or direct)
    ├── Declares capabilities + resource limits
    ├── Receives Task Envelope (JSON)
    ├── Executes task locally (WebAssembly / Transformers.js / WebLLM / etc.)
    └── Returns result + metadata
```

## Communication Protocol (Planned)

The web node communicates with the Control Plane over:

- WebSocket (preferred for low latency)
- Or HTTPS + long polling (fallback)

### Registration

```json
POST /register
{
  "node_id": "web-uuid-xxx",
  "type": "web-node",
  "capabilities": ["translate", "summarize", "embed"],
  "max_context": 4096,
  "browser": "Chrome 126",
  "public_endpoint": null
}
```

### Task Envelope (example)

```json
{
  "task_id": "t-abc123",
  "type": "summarize",
  "payload": {
    "text": "...",
    "max_length": 200
  },
  "constraints": {
    "timeout_ms": 30000,
    "max_tokens": 512
  }
}
```

## Runtime Options

| Runtime                    | Status     | Use Case                     |
|---------------------------|------------|------------------------------|
| Transformers.js           | Recommended| Embeddings + small models    |
| WebLLM / WebGPU           | Future     | On-device LLMs (when stable) |
| Native browser APIs       | Basic      | Translation via Web API      |
| WASM modules              | Supported  | Custom lightweight models    |

## Folder Structure

```
web-node/
├── README.md
├── package.json
├── src/
│   ├── index.js              # Main entry / registration
│   ├── task-runner.js        # Task execution engine
│   ├── capabilities.js       # Declared capabilities
│   └── protocol/
│       ├── register.js
│       └── task-handler.js
├── extension/
│   ├── manifest.json
│   ├── background.js
│   └── popup/
├── docs/
│   └── technical-spec.md
└── tests/
```

## Security & Constraints

- All tasks must be **stateless** or use ephemeral state
- No access to local filesystem or sensitive APIs
- Strict timeout and token limits enforced by Control Plane
- Only tasks explicitly marked as "web-safe" are routed here
- Capability declaration must be honest (Control Plane can audit)

## Next Steps (Implementation)

1. Basic registration + heartbeat
2. Task envelope receiver
3. Integration with Transformers.js for embeddings + summarization
4. Browser Extension packaging
5. Capability declaration UI (for user consent)

## Status (as of 1.03)

- Folder structure created
- Basic manifest present
- Technical specification defined
- Implementation pending

This component is intentionally kept small and optional. It is an **addition** to the mesh, not a core dependency.