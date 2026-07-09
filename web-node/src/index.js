// web-node/src/index.js
// Entry point for the Web Node (browser / extension)

export async function registerWebNode(config) {
  // TODO: Implement registration with Control Plane / Registry
  console.log("[WebNode] Registering with config:", config);
}

export async function handleTask(taskEnvelope) {
  // TODO: Route task to appropriate capability handler
  console.log("[WebNode] Received task:", taskEnvelope);
  return { status: "not_implemented" };
}