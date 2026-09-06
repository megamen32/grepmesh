(() => {
  "use strict";
  const $ = (id) => document.getElementById(id);
  const form = $("settings-form"), state = $("settings-state"), note = $("save-note"), list = $("effective-settings");
  const enabled = $("stt-enabled"), backend = $("stt-backend"), model = $("stt-model"), remoteUrl = $("stt-remote-url"), apiKeyEnv = $("stt-api-key-env"), download = $("stt-download");
  let current = null;
  async function request(url, options = {}) { const response = await fetch(url, { headers: { Accept: "application/json", ...(options.body ? {"Content-Type":"application/json"}: {}) }, ...options }); const body = await response.json(); if (!response.ok) throw new Error(body.error || body.message || `Request failed (${response.status})`); return body; }
  function pair(name, value) { const dt = document.createElement("dt"), dd = document.createElement("dd"); dt.textContent = name; dd.textContent = value == null ? "—" : String(value); list.append(dt, dd); }
  function render(data) { current = data; const stt = data.stt || {}; enabled.checked = Boolean(stt.enabled); backend.value = stt.backend || "auto"; model.value = stt.model || "auto"; remoteUrl.value = stt.remote_base_url || ""; apiKeyEnv.value = stt.api_key_env || "GREPMESH_STT_API_KEY"; download.checked = stt.auto_download !== false; list.replaceChildren(); pair("Host", data.host_id); pair("Primary root", data.root); pair("Index", data.index_path); pair("Bind", data.bind); pair("Config file", data.config_path); pair("Extra exclusions", (data.exclude_globs || []).join(", ") || "none"); state.className = "backup-state available"; state.textContent = stt.enabled ? "enabled" : "disabled"; }
  async function load() { try { render(await request("/api/settings")); } catch (error) { state.className = "backup-state unavailable"; state.textContent = "error"; note.textContent = error.message; } }
  form.addEventListener("submit", async (event) => { event.preventDefault(); note.textContent = "Saving…"; const stt = { ...(current?.stt || {}), enabled: enabled.checked, backend: backend.value, model: model.value, remote_base_url: remoteUrl.value || null, api_key_env: apiKeyEnv.value || "GREPMESH_STT_API_KEY", auto_download: download.checked }; try { const result = await request("/api/settings", { method: "POST", body: JSON.stringify({ stt }) }); note.textContent = result.restart_required ? "Saved. Restart GrepMesh to apply." : "Saved."; await load(); } catch (error) { note.textContent = `Save failed: ${error.message}`; } });
  load();
})();
