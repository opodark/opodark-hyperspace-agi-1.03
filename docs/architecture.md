# Architecture

Hyperspace AGI 1.03 is organized as a modular distributed agent system. The repo keeps the orchestration core, connector fabric, node runtimes, registry, memory layer, and infrastructure UI separated so each piece can evolve independently.

## Core layers

### Authority

The authority layer is the trust and seed boundary of the system. It is responsible for bootstrapping, node trust, and any role that should remain more tightly controlled than ordinary workers.

### Control Plane

The control plane is the orchestration core. It routes tasks, manages connectors, presents dashboards, and coordinates the agent fabric.

### Node / Worker

The node and worker layers are the execution runtime of the mesh. They handle task execution, model access, and lower-level agent behavior.

### Registry

The registry is the discovery and membership layer. It keeps track of active nodes and provides a shared view of who is online, what they can do, and how they should be reached.

### Memory Graph

The memory graph is the persistence and retrieval layer for shared agent memory and long-lived observations.

### Infra UI

The infra UI is the operational view of the mesh. It is separate from the control plane so that administrators and operators can inspect the network without touching execution logic.

## Connector fabric

The connector fabric lives in the control plane and is meant to make HyperSpace useful in enterprise environments. The current priority set is:

- GitHub
- Google Workspace
- Microsoft 365 / Office 365

The goal is to let the control plane behave like an enterprise agent operations center, not only a local inference runner.

## Web node path

The browser/web-node path is a lightweight client route for weaker machines. It should be able to join the mesh without Docker or heavy installation and contribute small tasks such as translation, embeddings, summarization, or moderation.

The web node should register capabilities, receive a constrained task set, and report results back to the control plane through a simple protocol.

## Separation principle

Every layer should remain independently deployable:

- authority can live behind a stricter boundary,
- control plane can run centrally,
- nodes and workers can scale horizontally,
- web nodes can join through the browser,
- the public hub can expose a free path without collapsing the enterprise boundary.