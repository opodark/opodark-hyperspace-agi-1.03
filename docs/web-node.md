# Web Node

The web node is the browser-first worker path for Hyperspace AGI 1.03. It exists so the mesh can include weaker devices that cannot run Docker or heavy local services.

## Purpose

The browser node should make the system feel more distributed and more accessible. A user should be able to open a webpage or extension, consent to resource sharing, and immediately contribute small capabilities to the mesh.

## Target capabilities

The first useful tasks for the web node are intentionally small:

- translation,
- embeddings,
- summarization,
- moderation,
- simple validation steps.

## Expected shape

The web node should be a subtree or package that can be reused in multiple forms:

- a standalone web app,
- a browser extension,
- a side panel / popup client,
- a lightweight task worker.

## Runtime model

The browser node should not assume Docker, local model installs, or privileged system access. It should rely on browser-safe primitives and a strict task envelope so the control plane can route only safe work to it.

## Interface with the control plane

The web node should:

1. register itself with a node id,
2. declare capabilities and resource limits,
3. receive a task envelope,
4. execute the small task locally,
5. return the result to the control plane.

## Deployment modes

### Enterprise deployment

The web node can be distributed inside a company as a managed browser tool. In that mode the same control plane can coordinate both local runtime nodes and browser nodes.

### Public deployment

The web node can also be published from a central landing page as the free/public entry point into the mesh.

## Design rule

The browser node should stay small, safe, and optional. It is an addition to the mesh, not a replacement for the main execution runtimes.