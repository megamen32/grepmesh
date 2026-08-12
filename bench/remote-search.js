const { execFileSync } = require("node:child_process");

const endpoint = process.env.GREPMESH_URL || "http://127.0.0.1:9419/mcp";
const sshHost = process.env.GREPMESH_BENCH_SSH_HOST;
const hostId = process.env.GREPMESH_BENCH_HOST_ID;
const root = process.env.GREPMESH_BENCH_ROOT;
const file = process.env.GREPMESH_BENCH_FILE;
const query = process.env.GREPMESH_BENCH_QUERY;
if (![sshHost, hostId, root, file, query].every(Boolean)) {
  throw new Error("Set GREPMESH_BENCH_SSH_HOST, HOST_ID, ROOT, FILE and QUERY");
}

const direct = [];
const mesh = [];
let directOk = 0;
let meshOk = 0;

async function meshCall(id) {
  const body = {
    jsonrpc: "2.0",
    id,
    method: "tools/call",
    params: {
      name: "search_text",
      arguments: {
        query,
        hosts: [hostId],
        roots: [root],
        context_lines: 0,
        max_matches: 1,
        mode: "literal",
      },
    },
  };
  const started = performance.now();
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      Accept: "application/json, text/event-stream",
      "Content-Type": "application/json",
      "MCP-Protocol-Version": "2026-07-28",
      "Mcp-Method": "tools/call",
      "Mcp-Name": "search_text",
    },
    body: JSON.stringify(body),
  });
  const value = await response.json();
  mesh.push(performance.now() - started);
  const text = value?.result?.content?.[0]?.text || "";
  meshOk += Number(text.includes(query) && text.includes(file));
}

await meshCall(0);
mesh.length = 0;
meshOk = 0;
for (let i = 1; i <= 25; i += 1) {
  const started = performance.now();
  const output = execFileSync("ssh", [sshHost, "rg", "-n", "-F", "-m", "1", query, file], {
    encoding: "utf8",
  });
  direct.push(performance.now() - started);
  directOk += Number(output.includes(query));
  await meshCall(i);
}
direct.sort((a, b) => a - b);
mesh.sort((a, b) => a - b);
console.log(JSON.stringify({
  direct_ok: directOk,
  mesh_ok: meshOk,
  direct_median_ms: Number(direct[12].toFixed(2)),
  mesh_median_ms: Number(mesh[12].toFixed(2)),
  ratio: Number((direct[12] / mesh[12]).toFixed(2)),
}));
