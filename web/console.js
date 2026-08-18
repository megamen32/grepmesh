(() => {
  "use strict";

  const api = Object.freeze({ catalog: "/api/catalog", search: "/api/search", preview: "/api/preview", backup: "/api/backup/availability" });
  // This is intentionally null until the Console has a server-issued directory-browsing contract.
  // Do not replace roots below with guessed filesystem entries.
  const directoryAdapter = Object.freeze({ listDirectory: null });
  const $ = (id) => document.getElementById(id);
  const dom = {
    connection: $("connection"), form: $("search-form"), query: $("query"), refresh: $("refresh-catalog"),
    button: $("search-button"), state: $("search-state"), hosts: $("host-sidebar"), results: $("results"),
    count: $("results-count"), preview: $("preview"), backupBadge: $("backup-badge"), backup: $("backup-body"),
  };
  const state = { catalog: null, selected: null, selectedHost: "", selectedRoot: "", results: [], mode: "browse", directoryAdapter };

  function text(value) { return value == null ? "" : String(value); }
  function pick(object, keys, fallback = undefined) { for (const key of keys) if (object && object[key] !== undefined && object[key] !== null) return object[key]; return fallback; }
  function dataOf(payload) { return payload && typeof payload.data === "object" && !Array.isArray(payload.data) ? payload.data : (payload || {}); }
  function errorMessage(error) { return error instanceof Error ? error.message : text(error) || "The local API is unavailable."; }
  function clear(node) { node.replaceChildren(); return node; }
  function element(name, className, value) { const node = document.createElement(name); if (className) node.className = className; if (value !== undefined) node.textContent = text(value); return node; }
  function formatCount(count) { return `${count} ${count === 1 ? "match" : "matches"}`; }
  function setConnection(kind, message) { dom.connection.className = `connection ${kind || ""}`; dom.connection.lastElementChild.textContent = message; }
  function setSearchState(kind, message) { dom.state.className = `search-state ${kind}`; dom.state.lastElementChild.textContent = message; }
  async function request(url, options = {}) {
    const response = await fetch(url, { headers: { Accept: "application/json", ...(options.body ? { "Content-Type": "application/json" } : {}) }, ...options });
    const raw = await response.text(); let body = {};
    try { body = raw ? JSON.parse(raw) : {}; } catch { throw new Error("The local API returned an invalid response."); }
    if (!response.ok) throw new Error(pick(body, ["error", "message"], `Request failed (${response.status}).`));
    return body;
  }
  function catalogEntries(payload) {
    const source = dataOf(payload); const rawHosts = pick(source, ["hosts", "nodes", "available_hosts"], []);
    const hosts = (Array.isArray(rawHosts) ? rawHosts : []).map((entry) => ({ id: text(typeof entry === "string" ? entry : pick(entry, ["id", "host_id", "host"])), label: text(typeof entry === "string" ? entry : pick(entry, ["label", "name", "host_id", "id"])), roots: Array.isArray(entry?.roots) ? entry.roots.map(text) : [] })).filter((entry) => entry.id);
    const rootValues = pick(source, ["roots", "available_roots"], []);
    const roots = (Array.isArray(rootValues) ? rootValues : []).map((entry) => text(typeof entry === "string" ? entry : pick(entry, ["id", "path", "root"]))).filter(Boolean);
    return { hosts, roots: [...new Set(roots)] };
  }
  function visibleRoots() {
    const host = state.catalog?.hosts.find((item) => item.id === state.selectedHost);
    return host?.roots?.length ? host.roots : (state.catalog?.roots || []);
  }
  function resetPreview() {
    const preview = clear(dom.preview); preview.className = "preview-empty";
    preview.append(element("span", "empty-icon", "▧"), element("p", "No file selected."), element("small", "Choose a search result to inspect its bounded preview. Directory browsing is not available yet."));
  }
  function renderHosts() {
    const hosts = clear(dom.hosts);
    const all = element("button", `host-item ${state.selectedHost ? "" : "selected"}`); all.type = "button"; all.append(element("span", "host-item-icon", "◉"), element("span", "", "All available hosts")); all.addEventListener("click", () => selectHost("")); hosts.append(all);
    (state.catalog?.hosts || []).forEach((host) => {
      const button = element("button", `host-item ${host.id === state.selectedHost ? "selected" : ""}`); button.type = "button";
      button.append(element("span", "host-item-icon", "◆"), element("span", "", host.label || host.id), element("small", "", host.roots.length ? `${host.roots.length} root${host.roots.length === 1 ? "" : "s"}` : "catalog host"));
      button.addEventListener("click", () => selectHost(host.id)); hosts.append(button);
    });
  }
  function selectHost(hostId) { state.selectedHost = hostId; state.selectedRoot = ""; state.mode = "browse"; renderHosts(); renderBrowseRoots(); resetPreview(); }
  function selectRoot(root) { state.selectedRoot = root; state.mode = "browse"; renderBrowseRoots(); resetPreview(); setSearchState("idle", `Search scope selected: ${root}`); }
  function renderBrowseRoots() {
    clear(dom.results); const roots = visibleRoots(); dom.count.textContent = roots.length ? `${roots.length} permitted ${roots.length === 1 ? "root" : "roots"}` : "No permitted roots";
    if (!roots.length) { const empty = element("div", "empty-results"); empty.append(element("span", "empty-icon", "—"), element("p", "No roots are available for this scope."), element("small", "Directory contents are intentionally not shown until a browse API is provided.")); dom.results.append(empty); return; }
    roots.forEach((root) => {
      const button = element("button", `root-item ${root === state.selectedRoot ? "selected" : ""}`); button.type = "button"; button.dataset.root = root;
      button.append(element("span", "root-icon", "▱"), element("span", "root-name", root), element("span", "root-kind", "Search root")); button.addEventListener("click", () => selectRoot(root)); dom.results.append(button);
    });
  }
  function renderCatalog(payload) { state.catalog = catalogEntries(payload); if (state.selectedHost && !state.catalog.hosts.some((host) => host.id === state.selectedHost)) state.selectedHost = ""; renderHosts(); renderBrowseRoots(); setConnection("ready", state.catalog.hosts.length ? `${state.catalog.hosts.length} host${state.catalog.hosts.length === 1 ? "" : "s"} available` : "Catalog is ready"); }
  async function loadCatalog() { setConnection("", "Loading catalog…"); try { renderCatalog(await request(api.catalog)); } catch (error) { clear(dom.hosts).append(element("p", "sidebar-empty", "Catalog unavailable")); clear(dom.results).append(element("div", "empty-results", `Catalog unavailable: ${errorMessage(error)}`)); setConnection("error", "Catalog unavailable"); setSearchState("error", `Catalog unavailable: ${errorMessage(error)}`); } }
  function normalizeResults(payload) { const source = dataOf(payload); const raw = pick(source, ["results", "matches", "hits"], []); return (Array.isArray(raw) ? raw : []).map((item, index) => ({ previewId: text(pick(item, ["preview_id"], "")), host: text(pick(item, ["host_id", "host"], "local")), path: text(pick(item, ["path", "file"], "")), line: Number(pick(item, ["line_number", "line", "start_line"], 1)) || 1, snippet: text(pick(item, ["snippet", "text", "line_text", "match"], "")), key: `${pick(item, ["preview_id", "id"], "")}:${index}` })).filter((item) => item.path || item.previewId); }
  function statusDetails(payload) { const source = dataOf(payload); const hostStatus = pick(source, ["host_status", "hosts"], []); const failures = (Array.isArray(hostStatus) ? hostStatus : []).filter((item) => item && item.ok === false); return { partial: Boolean(pick(source, ["partial"], false)), truncated: Boolean(pick(source, ["truncated"], false)), failures }; }
  function renderResults(results) { clear(dom.results); if (!results.length) { const empty = element("div", "empty-results"); empty.append(element("span", "empty-icon", "—"), element("p", "No matching files."), element("small", "Try a different query or broader permitted scope.")); dom.results.append(empty); return; } results.forEach((result) => { const button = element("button", "result"); button.type = "button"; button.dataset.key = result.key; const meta = element("div", "result-meta"); meta.append(element("span", "host-tag", result.host), element("span", "", `line ${result.line}`)); button.append(element("div", "result-path", result.path || "Previewable result"), element("div", "result-snippet", result.snippet || "Open bounded preview"), meta); button.addEventListener("click", () => selectResult(result, button)); dom.results.append(button); }); }
  function renderSearch(payload) { const source = dataOf(payload); state.mode = "search"; state.results = normalizeResults(payload); renderResults(state.results); dom.count.textContent = formatCount(state.results.length); const details = statusDetails(payload); if (details.partial) setSearchState("partial", `Partial result: ${details.failures.length || "one or more"} host responses were unavailable.`); else if (details.truncated) setSearchState("truncated", "Results are truncated to the allowed limit. Narrow the query or scope."); else if (pick(source, ["state", "status"], "") === "running") setSearchState("running", "Search is still running; wait for the local API to finish."); else setSearchState("complete", state.results.length ? "Search complete." : "Search complete with no matches."); if (details.partial && details.truncated) setSearchState("partial", "Partial result and truncated result set. Narrow the query, then retry."); }
  async function performSearch(event) { event.preventDefault(); const query = dom.query.value.trim(); if (!query) { dom.query.focus(); setSearchState("error", "Enter text or a pattern before searching."); return; } dom.button.disabled = true; dom.button.firstElementChild.textContent = "Searching"; setSearchState("running", "Searching available hosts…"); dom.count.textContent = "Searching…"; state.selected = null; try { const body = { query, hosts: state.selectedHost ? [state.selectedHost] : undefined, roots: state.selectedRoot ? [state.selectedRoot] : undefined }; Object.keys(body).forEach((key) => body[key] === undefined && delete body[key]); renderSearch(await request(api.search, { method: "POST", body: JSON.stringify(body) })); } catch (error) { dom.count.textContent = "Search unavailable"; setSearchState("error", `Search failed: ${errorMessage(error)}`); clear(dom.results).append(element("div", "empty-results", "The local API did not return results.")); } finally { dom.button.disabled = false; dom.button.firstElementChild.textContent = "Search"; } }
  function previewText(payload) { const source = dataOf(payload); if (typeof source.text === "string") return source.text; if (typeof source.content === "string") return source.content; if (Array.isArray(source.chunks)) return source.chunks.map((chunk) => typeof chunk === "string" ? chunk : pick(chunk, ["text", "content"], "")).join("\n"); return "No preview text was returned."; }
  async function selectResult(result, button) { state.selected = result; document.querySelectorAll(".result.selected").forEach((item) => item.classList.remove("selected")); button.classList.add("selected"); if (!result.previewId) { clear(dom.preview).className = "preview-error"; dom.preview.textContent = "Preview is unavailable because this result has no server-issued preview ID."; return; } clear(dom.preview).className = "preview-loading"; dom.preview.textContent = "Loading bounded preview…"; try { const payload = await request(api.preview, { method: "POST", body: JSON.stringify({ preview_id: result.previewId }) }); if (state.selected !== result) return; const source = dataOf(payload); const content = clear(dom.preview); content.className = "preview-content"; const title = element("div", "preview-title"); title.append(element("code", "", text(pick(source, ["path"], result.path || "Selected result"))), element("span", "", `line ${pick(source, ["start_line", "line"], result.line)}`)); content.append(title, element("pre", "", previewText(payload))); } catch (error) { clear(dom.preview).className = "preview-error"; dom.preview.textContent = `Preview unavailable: ${errorMessage(error)}`; } }
  function renderBackup(payload) { const source = dataOf(payload); const rawState = text(pick(source, ["state", "status"], "unconfigured")).toLowerCase(); const valid = ["available", "stale", "unavailable", "unconfigured"].includes(rawState) ? rawState : "unconfigured"; const descriptions = { available: "A backup catalog is available.", stale: "A backup catalog is available, but its metadata is stale.", unavailable: "The backup catalog is currently unavailable.", unconfigured: "No backup catalog is configured for this Console." }; dom.backupBadge.className = `backup-state ${valid}`; dom.backupBadge.textContent = valid; const body = clear(dom.backup); body.append(element("p", "", pick(source, ["message", "detail"], descriptions[valid]))); const count = pick(source, ["snapshot_count", "count", "entries"]); const updated = pick(source, ["updated_at", "generated_at", "checked_at"]); if (count !== undefined || updated) { const meta = element("div", "backup-meta"); if (count !== undefined) meta.append(element("span", "", `${count} catalog entries`)); if (updated) meta.append(element("span", "", `Updated ${updated}`)); body.append(meta); } body.append(element("small", "", "Backup status is separate from live search. No restore or backup-content search is performed here.")); }
  async function loadBackup() { try { renderBackup(await request(api.backup)); } catch (error) { renderBackup({ state: "unavailable", message: `Backup catalog check failed: ${errorMessage(error)}` }); } }
  dom.form.addEventListener("submit", performSearch); dom.refresh.addEventListener("click", () => { loadCatalog(); loadBackup(); }); loadCatalog(); loadBackup();
})();
