# Network Profiles

Hyperspace AGI 1.03 supports two clear network profiles. They share the same codebase philosophy, but they differ in trust boundaries, propagation rules, and how nodes join the mesh.

## Enterprise Local

Enterprise Local is the private deployment mode.

### Characteristics

- Seed propagation stays inside the organization.
- Nodes join through internal infrastructure.
- Access is constrained by tenant, allowlist, or admin policy.
- Best for internal company workflows and regulated environments.

### Use cases

- Private connector fabric for enterprise teams.
- Controlled agent workflows.
- Internal dashboards and operator visibility.
- Local browser nodes on managed devices.

### Policy intent

The enterprise mesh should feel closed, auditable, and low-risk. Nothing should propagate outside the boundary unless explicitly approved.

## Public Hub

Public Hub is the open or community-facing mode.

### Characteristics

- A central landing page bootstraps the mesh.
- Browser nodes join explicitly.
- The free version can be propagated from the hub.
- Tasks should be constrained to safe, lightweight workloads.

### Use cases

- Product landing page.
- Public demo mesh.
- Community onboarding.
- Lightweight browser-first worker participation.

### Policy intent

The public mesh should maximize adoption while minimizing risk. It should expose only the functionality appropriate for a lightweight browser node and avoid leaking enterprise-only capabilities.

## Shared rules

Both profiles should follow the same core principles:

- explicit consent for resource usage,
- capability-based routing,
- short-lived task scope,
- minimal trust by default,
- clean separation between orchestration and execution.