(() => {
  "use strict";

  const api = Object.freeze({ catalog: "/api/catalog", browse: "/api/browse", search: "/api/search", searchStatus: "/api/search/status", preview: "/api/preview", backup: "/api/backup/availability" });
  const $ = (id) => document.getElementById(id);
  const dom = {
    connection: $("connection"), form: $("search-form"), query: $("query"), refresh: $("refresh-catalog"),
    button: $("search-button"), state: $("search-state"), hosts: $("host-sidebar"), locations: $("locations-sidebar"), results: $("results"),
    count: $("results-count"), preview: $("preview"), failures: $("host-failures"), backupBadge: $("backup-badge"), backup: $("backup-body"),
  };
  const state = { catalog: null, selected: null, selectedHost: "", selectedRoot: null, browseTarget: null, results: [], mode: "browse", sort: "name", direction: 1, history: [], historyIndex: -1, statuses: [], telemetry: [], bookmarks: readBookmarks(), viewVersion: 0 };

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
    const hosts = (Array.isArray(rawHosts) ? rawHosts : []).map((entry) => ({ id: text(typeof entry === "string" ? entry : pick(entry, ["id", "host_id", "host"])), label: text(typeof entry === "string" ? entry : pick(entry, ["label", "name", "host_id", "id"])), health: text(typeof entry === "string" ? "unknown" : pick(entry, ["health"], "unknown")), lastError: text(typeof entry === "string" ? "" : pick(entry, ["last_error"], "")), roots: Array.isArray(entry?.roots) ? entry.roots.map((root) => ({ host: text(typeof root === "string" ? pick(entry, ["id", "host_id", "host"]) : pick(root, ["host", "host_id"], pick(entry, ["id", "host_id", "host"]))), path: text(typeof root === "string" ? root : pick(root, ["path", "root", "id"])), name: text(typeof root === "string" ? root : pick(root, ["name", "label", "path", "root", "id"])) })).filter((root) => root.host && root.path) : [] })).filter((entry) => entry.id);
    const rootValues = pick(source, ["roots", "available_roots"], []);
    const roots = hosts.flatMap((host) => host.roots);
    if (!roots.length) { const host = hosts.length === 1 ? hosts[0].id : ""; roots.push(...(Array.isArray(rootValues) ? rootValues : []).map((entry) => ({ host: text(typeof entry === "string" ? host : pick(entry, ["host", "host_id"], host)), path: text(typeof entry === "string" ? entry : pick(entry, ["id", "path", "root"])), name: text(typeof entry === "string" ? entry : pick(entry, ["name", "label", "path", "root", "id"])) })).filter((root) => root.path)); }
    return { hosts, roots: roots.filter((root, index, items) => items.findIndex((candidate) => candidate.host === root.host && candidate.path === root.path) === index) };
  }
  function visibleRoots() {
    const host = state.catalog?.hosts.find((item) => item.id === state.selectedHost);
    return state.selectedHost ? (host?.roots || []) : (state.catalog?.roots || []);
  }
  function resetPreview() {
    const preview = clear(dom.preview); preview.className = "preview-empty";
    preview.append(element("span", "file-icon folder large"), element("p", "", "No file selected."), element("small", "", "Choose a file or folder to inspect its details."));
  }
  function renderHosts() {
    const hosts = clear(dom.hosts);
    const all = element("button", `host-item ${state.selectedHost ? "" : "selected"}`); all.type = "button"; all.append(element("span", "host-item-icon", "◉"), element("span", "", "All available hosts")); all.addEventListener("click", () => selectHost("")); hosts.append(all);
    (state.catalog?.hosts || []).forEach((host) => {
      const button = element("button", `host-item ${host.id === state.selectedHost ? "selected" : ""}`); button.type = "button";
      const status = state.statuses.find(item=>item.host_id === host.id);
      button.append(element("span", "host-item-icon", "▱"), element("span", "host-name", host.label || host.id), element("small", `host-health ${host.health || "unknown"}`, host.health || "unknown"), element("small", "host-metrics", status ? `${state.statusStale ? "Last known: " : ""}${status.index_state || "Unknown"} · ${formatBytes(status.index_db_bytes)} index` : host.lastError || "Index status unavailable"));
      if(status?.index_current_path) {const current=element("small","host-current",status.index_current_path);current.title=status.index_current_path;button.append(current);}
      const timing=latestHostTiming(host.id); if(timing) button.append(element("small","host-metrics",`Last search ${formatDuration(timing.duration_ms)}`));
      button.addEventListener("click", () => selectHost(host.id)); hosts.append(button);
    });
  }
  function selectHost(hostId) { state.viewVersion++; state.selectedHost = hostId; state.selectedRoot = null; state.browseTarget = null; state.selected = null; state.results = []; state.mode = "browse"; renderHosts(); renderLocations(); renderBrowsePlaceholder(); resetPreview(); renderPath(); renderHostDetail(); renderHostFailures([]); showFiles(); }
  function selectRoot(root) { state.selectedRoot = root; state.browseTarget = root; state.selected = null; state.mode = "browse"; renderLocations(); resetPreview(); browseDirectory(root); }
  function selectLocation(event) { const button = event.target?.closest?.(".location-item"); if (!button || !dom.locations.contains(button)) return; const root = visibleRoots().find((candidate) => candidate.host === button.dataset.host && candidate.path === button.dataset.root); if (root) selectRoot(root); }
  function renderLocations() {
    const locations = clear(dom.locations); const roots = visibleRoots();
    if (!roots.length) { locations.append(element("p", "sidebar-empty", "No configured Locations are available for this Device.")); return; }
    roots.forEach((root) => {
      const button = element("button", `location-item ${root.path === state.selectedRoot?.path && root.host === state.selectedRoot?.host ? "selected" : ""}`); button.type = "button"; button.dataset.root = root.path; button.dataset.host = root.host;
      button.title = root.path; button.append(element("span", "file-icon folder"), element("span", "location-name", basename(root.path)), element("small", "", root.host || "configured drive")); locations.append(button);
    });
  }
  function renderBrowsePlaceholder() { clear(dom.results); dom.count.textContent = "Choose a Location"; const empty = element("div", "empty-results"); empty.append(element("span", "empty-icon", "▱"), element("p", "", "Select a Location to browse its immediate entries."), element("small", "", "Devices and Locations stay available in the sidebar while you browse.")); dom.results.append(empty); }
  function renderCatalog(payload) { state.catalog = catalogEntries(payload); reconcileHostStatus(); if (state.selectedHost && !state.catalog.hosts.some((host) => host.id === state.selectedHost)) state.selectedHost = ""; if (state.selectedRoot && !state.catalog.roots.some((root) => root.host === state.selectedRoot.host && root.path === state.selectedRoot.path)) state.selectedRoot = null; renderHosts(); renderLocations(); renderPath(); if (!state.browseTarget && state.mode === "browse") renderBrowsePlaceholder(); setConnection("ready", state.catalog.hosts.length ? `${state.catalog.hosts.length} hosts configured` : "Catalog is ready"); }
  async function loadCatalog() { setConnection("", "Loading catalog…"); try { renderCatalog(await request(api.catalog)); } catch (error) { clear(dom.hosts).append(element("p", "sidebar-empty", "Catalog unavailable")); clear(dom.locations).append(element("p", "sidebar-empty", "Catalog unavailable")); clear(dom.results).append(element("div", "empty-results", `Catalog unavailable: ${errorMessage(error)}`)); setConnection("error", "Catalog unavailable"); setSearchState("error", `Catalog unavailable: ${errorMessage(error)}`); } }
  function normalizeEntries(payload, host) { const source = dataOf(payload); return (Array.isArray(source.entries) ? source.entries : []).map((entry, index) => ({ host: text(pick(entry, ["host_id", "host"], host)), name: text(pick(entry, ["name"], "Unnamed entry")), path: text(pick(entry, ["path"], "")), kind: text(pick(entry, ["kind"], "other")), size: pick(entry, ["size"]), modified: pick(entry, ["modified_ms"]), created: pick(entry, ["created_ms"]), key: `${host}:${pick(entry, ["path"], index)}` })).filter((entry) => entry.path); }
  function browseEntryDetails(entry) {
    const content = clear(dom.preview); content.className = "preview-content";
    content.append(element("span", `file-icon ${entry.kind === "directory" ? "folder" : "file"} large`), element("h2", "inspector-name", entry.name || entry.path.split(/[\\/]/).pop()), element("p", "inspector-kind", entry.kind === "directory" ? "Folder" : "File"));
    const details = element("dl", "detail-grid");
    for (const [label,value] of [["Size",formatBytes(entry.size)],["Kind",entry.kind],["Created",formatDate(entry.created,true)],["Modified",formatDate(entry.modified,true)],["Host",entry.host],["Path",entry.path]]) details.append(element("dt","",label),element("dd","",value));
    content.append(details);
    if (entry.kind === "directory") { const open = element("button","quiet-button","Open folder"); open.addEventListener("click",()=>browseDirectory(entry)); content.append(open); }
    else if(!entry.previewId) content.append(element("small","","Search for this file’s text to open a bounded content preview."));
  }
  function renderBrowseEntries(entries) {
    clear(dom.results); dom.count.textContent = `${entries.length} ${entries.length === 1 ? "item" : "items"}`;
    if (!entries.length) { dom.results.append(element("div","empty-results","This folder is empty or its entries are excluded.")); return; }
    sortedEntries(entries).forEach((entry) => {
      const button = fileRow(entry); button.className += " browse-entry";
      button.addEventListener("click",()=>selectBrowseEntry(entry,button));
      button.addEventListener("dblclick",()=>{if(entry.kind === "directory") browseDirectory(entry);});
      button.addEventListener("keydown",event=>{if(event.key === "Enter" && entry.kind === "directory") {event.preventDefault(); browseDirectory(entry);}});
      dom.results.append(button);
    });
  }
  async function browseDirectory(target, remember = true) {
    if (!target?.host || !target?.path) return;
    const version = ++state.viewVersion; state.mode = "browse"; state.browseTarget = target; state.selectedHost = target.host; state.selected = null; showFiles();
    state.selectedRoot = (state.catalog?.roots || []).filter(root=>root.host === target.host && pathContains(root.path,target.path)).sort((a,b)=>b.path.length-a.path.length)[0] || target;
    if(remember && (state.history[state.historyIndex]?.path !== target.path || state.history[state.historyIndex]?.host !== target.host)) { state.history = state.history.slice(0,state.historyIndex+1); state.history.push({host:target.host,path:target.path}); state.historyIndex = state.history.length-1; }
    renderPath(); renderHosts(); renderLocations(); renderHostDetail(); resetPreview();
    renderHostFailures([]); setSearchState("running","Loading folder…"); dom.count.textContent = "Loading…";
    try { const payload = await request(api.browse,{method:"POST",body:JSON.stringify({host:target.host,path:target.path})}); if(version !== state.viewVersion) return; state.results = normalizeEntries(payload,target.host); renderBrowseEntries(state.results); setSearchState("complete","Ready"); }
    catch(error) { if(version !== state.viewVersion) return; clear(dom.results).append(element("div","empty-results",`Browse unavailable: ${errorMessage(error)}`)); dom.count.textContent = "Unavailable"; setSearchState("error",errorMessage(error)); }
  }
  function selectBrowseEntry(entry, button) { state.selected = entry; document.querySelectorAll(".result.selected").forEach(item=>item.classList.remove("selected")); button.classList.add("selected"); browseEntryDetails(entry); }
  function normalizeResults(payload) { const source = dataOf(payload); const raw = pick(source, ["results", "matches", "hits"], []); return (Array.isArray(raw) ? raw : []).map((item, index) => ({ previewId: text(pick(item, ["preview_id"], "")), host: text(pick(item, ["host_id", "host"], "local")), path: text(pick(item, ["path", "file"], "")), kind: "file", size: pick(item,["size"]), modified: pick(item,["modified_ms"]), created: pick(item,["created_ms"]), line: Number(pick(item, ["line_number", "line", "start_line"], 1)) || 1, snippet: text(pick(item, ["snippet", "text", "line_text", "match"], "")), key: `${pick(item, ["preview_id", "id"], "")}:${index}` })).filter((item) => item.path || item.previewId); }
  function statusDetails(payload) { const source = dataOf(payload); const hostStatus = pick(source, ["host_status", "hosts"], []); const failures = (Array.isArray(hostStatus) ? hostStatus : []).filter((item) => item && item.ok === false).map((item) => ({ host: text(pick(item, ["host_id", "host", "id"], "unknown host")), error: text(pick(item, ["error", "message"], "No error detail was returned.")) })); return { partial: Boolean(pick(source, ["partial"], false)), truncated: Boolean(pick(source, ["truncated"], false)), failures }; }
  function renderHostFailures(failures) { clear(dom.failures); if (!failures.length) { dom.failures.hidden = true; return; } dom.failures.append(element("p", "host-failures-heading", "Mesh host errors")); failures.forEach((failure) => { const item = element("div", "host-failure"); item.append(element("strong", "", `Host ${failure.host} reported an error`), element("code", "", failure.error)); dom.failures.append(item); }); dom.failures.hidden = false; }
  function renderResults(results) { clear(dom.results); if(!results.length){dom.results.append(element("div","empty-results","No matching files."));return;}sortedEntries(results).forEach(result=>{const button=fileRow(result);button.title=`${result.host} · ${result.path}:${result.line}\n${result.snippet}`;button.addEventListener("click",()=>selectResult(result,button));dom.results.append(button);}); }
  function renderSearch(payload) { const source = dataOf(payload); state.mode = "search"; state.results = normalizeResults(payload); renderResults(state.results); dom.count.textContent = formatCount(state.results.length); const details = statusDetails(payload); renderHostFailures(details.failures);
    if(pick(source,["state","status"],"") === "running") { const pending=Array.isArray(source.pending_hosts) ? source.pending_hosts.length : null; setSearchState("running",`Searching${pending == null ? "…" : ` · ${pending} host${pending === 1 ? "" : "s"} pending`}${details.failures.length ? ` · ${details.failures.length} failed` : ""}`);return; }
    if(details.partial && details.truncated)setSearchState("partial","Partial result and truncated result set. Narrow the query, then retry.");else if(details.partial)setSearchState("partial",`Partial result: ${details.failures.length || "one or more"} host responses were unavailable.`);else if(details.truncated)setSearchState("truncated","Results are truncated to the allowed limit. Narrow the query or scope.");else setSearchState("complete",state.results.length ? "Search complete." : "Search complete with no matches.");
  }
  async function pollSearch(payload,version) { let current=payload;if(version===state.viewVersion)renderSearch(current);while(pick(dataOf(current),["state"],"")==="running"){const source=dataOf(current),jobId=source.job_id;if(!jobId)throw new Error("The local API did not return a search job ID.");await new Promise(resolve=>setTimeout(resolve,Math.max(100,Math.min(500,Number(source.next_poll_after_ms)||100))));current=await request(api.searchStatus,{method:"POST",body:JSON.stringify({job_id:jobId})});if(version===state.viewVersion)renderSearch(current);}return current; }
  async function performSearch(event) {
    event.preventDefault();const query=dom.query.value.trim();if(!query){dom.query.focus();setSearchState("error","Enter text or a pattern before searching.");return;}
    const version=++state.viewVersion;showFiles();dom.button.disabled=true;dom.button.firstElementChild.textContent="Searching";setSearchState("running","Searching…");dom.count.textContent="Searching…";state.selected=null;resetPreview();
    try{const body={query};if(state.selectedHost)body.hosts=state.selectedHost;if(state.browseTarget){body.hosts=state.browseTarget.host;body.roots=[state.browseTarget.path];}await pollSearch(await request(api.search,{method:"POST",body:JSON.stringify(body)}),version);}
    catch(error){if(version===state.viewVersion){dom.count.textContent="Unavailable";setSearchState("error",`Search failed: ${errorMessage(error)}`);clear(dom.results).append(element("div","empty-results","Search unavailable."));}}
    finally{dom.button.disabled=false;dom.button.firstElementChild.textContent="Search";loadTelemetry();}
  }
  function previewText(payload) { const source = dataOf(payload); if (typeof source.text === "string") return source.text; if (typeof source.content === "string") return source.content; if (Array.isArray(source.chunks)) return source.chunks.map((chunk) => typeof chunk === "string" ? chunk : pick(chunk, ["text", "content"], "")).join("\n"); return "No preview text was returned."; }
  async function selectResult(result, button) { state.selected = result; document.querySelectorAll(".result.selected").forEach((item) => item.classList.remove("selected")); button.classList.add("selected");browseEntryDetails(result);const preview=element("div","preview-loading","Loading bounded preview…");dom.preview.append(preview); if(!result.previewId){preview.className="preview-error";preview.textContent="Preview is unavailable because this result has no server-issued preview ID.";return;}try{const payload=await request(api.preview,{method:"POST",body:JSON.stringify({preview_id:result.previewId})});if(state.selected!==result)return;clear(preview);preview.textContent="";preview.className="bounded-preview";preview.append(element("p","detail-label",`Content · line ${pick(dataOf(payload),["start_line","line"],result.line)}`),element("pre","",previewText(payload)));}catch(error){if(state.selected!==result)return;preview.className="preview-error";preview.textContent=`Preview unavailable: ${errorMessage(error)}`;} }
  function renderBackup(payload) { const source = dataOf(payload); const rawState = text(pick(source, ["state", "status"], "unconfigured")).toLowerCase(); const valid = ["available", "stale", "unavailable", "unconfigured"].includes(rawState) ? rawState : "unconfigured"; const descriptions = { available: "A backup catalog is available.", stale: "A backup catalog is available, but its metadata is stale.", unavailable: "The backup catalog is currently unavailable.", unconfigured: "No backup catalog is configured for this Console." }; dom.backupBadge.className = `backup-state ${valid}`; dom.backupBadge.textContent = valid; const body = clear(dom.backup); body.append(element("p", "", pick(source, ["message", "detail"], descriptions[valid]))); const count = pick(source, ["snapshot_count", "count", "entries"]); const updated = pick(source, ["updated_at", "generated_at", "checked_at"]); if (count !== undefined || updated) { const meta = element("div", "backup-meta"); if (count !== undefined) meta.append(element("span", "", `${count} catalog entries`)); if (updated) meta.append(element("span", "", `Updated ${updated}`)); body.append(meta); } body.append(element("small", "", "Backup status is separate from live search. No restore or backup-content search is performed here.")); }
  async function loadBackup() { try { renderBackup(await request(api.backup)); } catch (error) { renderBackup({ state: "unavailable", message: `Backup catalog check failed: ${errorMessage(error)}` }); } }
  function basename(path) { return text(path).split(/[\\/]/).filter(Boolean).pop() || path; }
  function formatBytes(value) { if(value == null || !Number.isFinite(Number(value))) return "—"; const n=Number(value); if(n<1024)return `${n} B`; const unit=Math.min(4,Math.floor(Math.log(n)/Math.log(1024))); return `${(n/1024**unit).toLocaleString(undefined,{maximumFractionDigits:1})} ${["B","KB","MB","GB","TB"][unit]}`; }
  function formatDuration(ms) { return ms == null ? "—" : Number(ms)<1000 ? `${Number(ms).toFixed(0)} ms` : `${(Number(ms)/1000).toFixed(2)} s`; }
  function formatDate(ms,full=false) { if(ms == null || !Number.isFinite(Number(ms)))return "—"; const date=new Date(Number(ms)); return Number.isNaN(date.getTime()) ? "—" : full ? date.toLocaleString() : date.toLocaleDateString(); }
  function pathContains(root,path) { const normalize=value=>value.replace(/\\/g,"/").replace(/\/$/,""); const a=normalize(root),b=normalize(path); return b===a || b.startsWith(`${a}/`); }
  function sortedEntries(entries) { return [...entries].sort((a,b)=> { const av=state.sort === "name" ? (a.name || basename(a.path)) : a[state.sort],bv=state.sort === "name" ? (b.name || basename(b.path)) : b[state.sort]; if(av == null && bv == null)return text(a.path).localeCompare(text(b.path)); if(av == null)return 1;if(bv == null)return -1; const delta=state.sort === "name" ? text(av).localeCompare(text(bv),undefined,{numeric:true,sensitivity:"base"}) : Number(av)-Number(bv); return delta*state.direction || text(a.path).localeCompare(text(b.path)); }); }
  function fileRow(entry) {
    const row=element("button",`result file-row ${state.selected?.key === entry.key ? "selected" : ""}`); row.type="button"; row.dataset.key=entry.key;row.title=entry.path;
    const name=element("span","file-name");name.append(element("span",`file-icon ${entry.kind === "directory" ? "folder" : "file"}`),element("span","",entry.name || basename(entry.path)));
    row.append(name,element("span","file-size",formatBytes(entry.size)),element("span","file-date",formatDate(entry.modified)),element("span","file-date",formatDate(entry.created)),element("span","file-kind",entry.kind === "directory" ? "Folder" : entry.kind || "File"));return row;
  }
  function readBookmarks() { try {const value=JSON.parse(localStorage.getItem("grepmesh.bookmarks") || "[]");return Array.isArray(value) ? value.filter(item=>typeof item.host === "string" && typeof item.path === "string").slice(0,100) : [];}catch{return [];} }
  function renderBookmarks() {
    const node=clear($("bookmarks-sidebar")); if(!state.bookmarks.length)node.append(element("p","sidebar-empty","Use ☆ to save a folder"));
    state.bookmarks.forEach(bookmark=>{const row=element("div","bookmark-row"),button=element("button","location-item");button.title=bookmark.path;button.append(element("span","file-icon folder"),element("span","location-name",basename(bookmark.path)),element("small","",bookmark.host));button.addEventListener("click",()=>browseDirectory(bookmark));const remove=element("button","remove-bookmark","×");remove.title=`Remove bookmark ${bookmark.path}`;remove.setAttribute("aria-label",remove.title);remove.addEventListener("click",()=>{state.bookmarks=state.bookmarks.filter(item=>item!==bookmark);saveBookmarks();});row.append(button,remove);node.append(row);});
  }
  function saveBookmarks() { try{localStorage.setItem("grepmesh.bookmarks",JSON.stringify(state.bookmarks));}catch{setSearchState("error","Bookmarks could not be saved by this browser.");}renderBookmarks();renderPath(); }
  function renderPath() {
    const node=clear($("breadcrumbs")); const home=element("button","crumb",state.selectedHost || "All Devices");home.addEventListener("click",()=>selectHost(state.selectedHost));node.append(home);
    const target=state.browseTarget; $("browser-heading").textContent=target ? basename(target.path) : state.selectedHost || "All Devices";
    if(target) { const win=target.path.includes("\\"),sep=win ? "\\" : "/",parts=target.path.split(/[\\/]/).filter(Boolean);let accumulated=target.path.startsWith("/") ? "/" : target.path.startsWith("\\\\") ? "\\\\" : "";
      if(accumulated === "/") node.append(element("span","crumb-separator","›"),pathCrumb("/","/",target.host));
      parts.forEach(part=>{accumulated += accumulated && !accumulated.endsWith(sep) ? sep+part : part;const path=accumulated+(win && /^[A-Za-z]:$/.test(accumulated) ? "\\" : "");node.append(element("span","crumb-separator","›"),pathCrumb(part,path,target.host));});
    }
    $("bookmark-current").disabled=!target;const saved=target && state.bookmarks.some(item=>item.host===target.host && item.path===target.path);$("bookmark-current").textContent=saved ? "★" : "☆"; $("bookmark-current").title=saved ? "Remove current bookmark" : "Bookmark current folder";
    $("go-back").disabled=state.historyIndex<=0;$("go-forward").disabled=state.historyIndex>=state.history.length-1;
  }
  function pathCrumb(label,path,host) {const button=element("button","crumb",label);const allowed=(state.catalog?.roots || []).some(root=>root.host===host && pathContains(root.path,path));button.disabled=!allowed;button.title=allowed ? path : `${path} — outside indexed locations`;if(allowed)button.addEventListener("click",()=>browseDirectory({host,path}));return button;}
  function latestHostTiming(id) {for(const record of state.telemetry){const timing=(record.host_timings || []).find(item=>item.host_id===id);if(timing)return timing;}return null;}
  function renderHostDetail() {
    const node=clear($("host-detail"));const id=state.selectedHost;if(!id)return;
    node.append(element("h2","section-label","Index · "+id));const status=state.statuses.find(item=>item.host_id===id);if(!status){node.append(element("p","muted","Index status unavailable"));return;}if(state.statusStale)node.append(element("p","muted","Refresh failed · showing last known values"));
    const grid=element("dl","detail-grid");const timing=latestHostTiming(id);
    for(const [label,value] of [["State",status.index_state || "Unknown"],["Files",status.indexed_files ?? status.file_count ?? "—"],["Database",formatBytes(status.index_db_bytes)],["Last search",timing ? formatDuration(timing.duration_ms) : "—"]])grid.append(element("dt","",label),element("dd","",value));node.append(grid);
    node.append(element("p","detail-label",status.index_current_path ? "Currently indexing" : "Indexed locations"));
    if(status.index_current_path)node.append(element("code","index-path",status.index_current_path));
    for(const path of status.roots || [status.root].filter(Boolean))node.append(element("code","index-path",path));
    if(status.index_last_error)node.append(element("p","preview-error",status.index_last_error));
  }
  function reconcileHostStatus() {
    if(!state.catalog || !state.statusResponse || state.statusStale)return;
    const reports=Array.isArray(state.statusResponse.host_status) ? state.statusResponse.host_status : [];
    state.recoveryCatalogRequests ||= new Set();let refreshNeeded=false;
    for(const host of state.catalog.hosts) {
      const report=reports.find(item=>pick(item,["host_id","host","id"])===host.id),node=state.statuses.find(item=>item.host_id===host.id);
      if(report?.ok === false) {host.health="offline";host.lastError=text(pick(report,["error","message"],"Host status unavailable"));state.recoveryCatalogRequests.delete(host.id);continue;}
      if(report?.ok === true || node) {
        const recovered=host.health==="offline" || host.health==="unknown";host.health=node?.index_state==="Degraded" || node?.index_state==="Corrupt" ? "degraded" : "healthy";host.lastError=text(node?.index_last_error);
        if(Array.isArray(node?.roots) && node.roots.length)host.roots=node.roots.filter(path=>typeof path==="string").map(path=>({host:host.id,path,name:path}));
        else if(recovered && !host.roots.length && !state.recoveryCatalogRequests.has(host.id)) {state.recoveryCatalogRequests.add(host.id);refreshNeeded=true;}
      } else if(host.health!=="offline") {host.health="unknown";host.lastError="Host was not included in the latest status response.";}
    }
    const hydrated=state.catalog.hosts.flatMap(host=>host.roots);
    state.catalog.roots=[...hydrated,...state.catalog.roots].filter((root,index,all)=>all.findIndex(item=>item.host===root.host && item.path===root.path)===index);
    if(refreshNeeded)loadCatalog();
  }
  async function loadHostStatus(){if(state.statusPending)return;state.statusPending=true;try{const data=dataOf(await request("/api/host-status"));state.statusResponse=data;const failures=(Array.isArray(data.host_status) ? data.host_status : []).filter(item=>item.ok===false).map(item=>pick(item,["host_id","host","id"]));state.statuses=(Array.isArray(data.nodes) ? data.nodes : []).filter(node=>!failures.includes(node.host_id));state.statusStale=false;reconcileHostStatus();}catch{state.statusStale=true;if(state.catalog)for(const host of state.catalog.hosts){host.health="unknown";host.lastError="Status refresh failed.";}}finally{state.statusPending=false;renderHosts();renderLocations();renderPath();renderHostDetail();}}
  async function loadTelemetry(){try{const data=dataOf(await request("/api/telemetry"));state.telemetry=Array.isArray(data.records) ? data.records : [];renderTelemetry(data.retention);renderHosts();renderHostDetail();}catch(error){clear($("telemetry-records")).append(element("p","preview-error",`History unavailable: ${errorMessage(error)}`));}}
  function renderTelemetry(retention){const node=clear($("telemetry-records"));$("telemetry-retention").textContent=retention ? `Completed searches · up to ${retention.max_records} records / ${retention.max_age_days} days` : "Completed searches";if(!state.telemetry.length)node.append(element("div","empty-results","No completed searches yet."));
    state.telemetry.forEach(record=>{const row=element("article","telemetry-record"),heading=element("div","history-heading");heading.append(element("code","",record.query),element("strong","duration",formatDuration(record.duration_ms)));row.append(heading,element("p","muted",`${formatDate(record.started_ms,true)} · ${record.status} · ${record.result_count} results`));const timings=element("div","host-timings");for(const timing of record.host_timings || [])timings.append(element("span","",`${timing.host_id}: ${formatDuration(timing.duration_ms)} · ${timing.status} · ${timing.result_count} results`));row.append(timings);node.append(row);});}
  function showFiles(){$("telemetry-panel").hidden=true;$("file-table").hidden=false;}
  $("show-telemetry").addEventListener("click",()=>{$("telemetry-panel").hidden=false;$("file-table").hidden=true;loadTelemetry();});$("close-telemetry").addEventListener("click",showFiles);
  $("go-back").addEventListener("click",()=>{if(state.historyIndex>0)browseDirectory(state.history[--state.historyIndex],false);});$("go-forward").addEventListener("click",()=>{if(state.historyIndex<state.history.length-1)browseDirectory(state.history[++state.historyIndex],false);});
  $("bookmark-current").addEventListener("click",()=>{const target=state.browseTarget;if(!target)return;const index=state.bookmarks.findIndex(item=>item.host===target.host && item.path===target.path);if(index>=0)state.bookmarks.splice(index,1);else state.bookmarks.push({host:target.host,path:target.path});saveBookmarks();});
  $("toggle-inspector").addEventListener("click",()=>document.querySelector(".finder-window").classList.toggle("hide-inspector"));
  document.querySelectorAll("[data-sort]").forEach(button=>button.addEventListener("click",()=>{state.direction=state.sort === button.dataset.sort ? -state.direction : 1;state.sort=button.dataset.sort;document.querySelectorAll("[data-sort]").forEach(item=>{item.setAttribute("aria-sort",item.dataset.sort===state.sort ? state.direction===1 ? "ascending" : "descending" : "none");item.lastElementChild.textContent=item.dataset.sort===state.sort ? state.direction===1 ? "⌃" : "⌄" : "";});if(state.mode === "browse")renderBrowseEntries(state.results);else renderResults(state.results);}));
  dom.form.addEventListener("submit", performSearch); dom.locations.addEventListener("click", selectLocation); dom.refresh.addEventListener("click", () => { loadCatalog(); loadBackup(); loadHostStatus(); loadTelemetry(); }); renderPath();renderBookmarks();loadCatalog();loadBackup();loadHostStatus();loadTelemetry();
  if(typeof setInterval === "function")setInterval(()=>{if(!document.hidden)loadHostStatus();},15000);
})();
