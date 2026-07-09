# opodark-hyperspace-agi-1.03

Hyperspace AGI 1.03 is the current mainline snapshot of the project: a distributed agent platform built around a control plane, authority/seed nodes, connector fabric, local runtime nodes, and a new browser/web-node path that can run without Docker or heavy local installs.

This release keeps the existing 1.02 core intact and extends it in two directions:

- **Enterprise Local**: private network profile for internal company deployments, with seed propagation constrained to the tenant.
- **Public Hub**: public-facing network profile that uses a landing page and propagates the free browser-node version.

## What is in this repo

- `authority/` — trust/seed services and node coordination entrypoints.
- `control-plane/` — orchestration, connector fabric, routing, dashboard, and task management.
- `node/` — main agent runtime.
- `worker/` — execution worker runtime.
- `registry/` — service discovery and node registry.
- `memory-graph/` — memory export and graph persistence.
- `infra-ui/` — infrastructure and status dashboard.
- `web-node/` — browser-side node subtree for lightweight clients and extension packaging.
- `shared/` — shared models, events, identity, database helpers, and registry client.
- `docs/` — architecture and deployment notes.

## Connector fabric

The control plane includes the enterprise connector layer for agents, focused on:

- GitHub
- Google Workspace
- Microsoft 365 / Office 365

These connectors are meant to turn HyperSpace into an **Enterprise Connector Fabric for Agents**, not just another agent runner.

## Network profiles

### Enterprise Local

Use this mode when the deployment must stay inside a private organization boundary.

- Seed nodes stay internal.
- Discovery is constrained to the tenant/network.
- Suitable for private corporate fleets.
- Recommended for controlled, auditable deployments.

### Public Hub

Use this mode when you want a public landing hub that distributes the free browser node.

- Landing page acts as the bootstrap point.
- Browser nodes join explicitly.
- Lowest-risk tasks can be routed to lightweight web nodes.
- Good for demos, adoption, and community propagation.

## Browser / web node

The browser node is a first-class path for weaker devices that cannot run Docker or heavier local services.

Planned responsibilities:

- register capabilities from the browser tab / extension,
- execute simple tasks such as translation, embeddings, summarization, and moderation,
- provide lightweight worker capacity to the mesh,
- keep the control plane reachable from a web-first client.

## Suggested next steps

1. Add the `web-node/` subtree if it is not already present.
2. Keep the existing 1.02 core components unchanged.
3. Wire the browser node into the control plane registry.
4. Split docs so the architecture of `Enterprise Local` and `Public Hub` is explicit.
5. Decide whether the browser node should ship as a web app, extension, or both.

## Quick start

Use the project-specific setup scripts for your platform:

```bash
./setup.sh
```

or on Windows:

```powershell
.\setup.ps1
```

## Notes

This repository is the 1.03 evolution of the HyperSpace stack. It is intentionally modular so the control plane, local agents, and browser nodes can evolve independently without forcing one deployment shape on every user.