// ---- AI Gateway dashboard logic ----
let TOKEN = localStorage.getItem("gw_token") || "";
let STATE = null;

const $ = (id) => document.getElementById(id);

function toast(msg, bad) {
  const t = $("toast");
  t.textContent = msg;
  t.style.borderColor = bad ? "#d4675a" : "#3a3833";
  t.classList.remove("hidden");
  clearTimeout(window._tt);
  window._tt = setTimeout(() => t.classList.add("hidden"), 2600);
}

async function api(path, body, method) {
  const r = await fetch(path, {
    method: method || (body ? "POST" : "GET"),
    headers: { "Content-Type": "application/json", "X-Admin-Token": TOKEN },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (r.status === 401) { logout(); throw new Error("unauthorized"); }
  return r.json();
}

// ---- auth ----
async function doLogin() {
  const pw = $("loginpw").value;
  const res = await fetch("/admin/login", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password: pw }),
  }).then((r) => r.json());
  if (res.ok) {
    TOKEN = pw;
    localStorage.setItem("gw_token", TOKEN);
    $("login").classList.add("hidden");
    $("app").classList.remove("hidden");
    refresh();
  } else {
    $("loginerr").classList.remove("hidden");
  }
}
function logout() {
  localStorage.removeItem("gw_token"); TOKEN = "";
  $("app").classList.add("hidden"); $("login").classList.remove("hidden");
}
$("loginpw").addEventListener("keydown", (e) => { if (e.key === "Enter") doLogin(); });

// ---- tabs ----
function tab(name) {
  document.querySelectorAll(".tab").forEach((b) =>
    b.classList.toggle("active", b.dataset.tab === name));
  document.querySelectorAll("[data-pane]").forEach((p) =>
    p.classList.toggle("hidden", p.dataset.pane !== name));
}

// ---- helpers ----
function maskProxy(url) {
  // http://user:pass@ip:port -> ip:port (user:***)
  try {
    const u = url.replace(/^https?:\/\//, "");
    const at = u.indexOf("@");
    if (at === -1) return u;
    const cred = u.slice(0, at), host = u.slice(at + 1);
    const user = cred.split(":")[0];
    return `${host}  (${user}:•••)`;
  } catch { return url; }
}
const STATUS_COLOR = { ok: "#6fae6f", banned: "#d4675a", unhealthy: "#d9a657", unknown: "#a3a097" };
function fmtTime(ts) {
  if (!ts) return "";
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString();
}

// ---- render ----
function renderConn() {
  const s = STATE.settings;
  $("s_endpoint").value = s.endpoint || "";
  $("s_gateway_key").value = s.gateway_key || "";
  $("s_upstream_key").value = s.upstream_key || "";
  $("s_require_client_key").checked = (s.require_client_key || "1") === "1";
  const origin = window.location.origin;
  $("agentCfg").textContent =
    `ANTHROPIC_BASE_URL   = ${origin}\n` +
    `ANTHROPIC_API_KEY    = ${s.gateway_key || "<your gateway key>"}\n` +
    `ANTHROPIC_AUTH_TOKEN = ${s.gateway_key || "<your gateway key>"}\n` +
    `model                = claude-opus-4-8`;
}

function renderCounts() {
  const c = STATE.counts;
  $("hdrCounts").textContent = `${c.ok} ok · ${c.banned} banned · ${c.total} total`;
  $("proxyCounts").textContent =
    `${c.ok} ok · ${c.unknown} unknown · ${c.unhealthy} unhealthy · ${c.banned} banned · ${c.total} total`;
}

function renderProxies() {
  const tb = $("proxyRows"); tb.innerHTML = "";
  for (const p of STATE.proxies) {
    const tr = document.createElement("tr");
    tr.className = "border-b border-line/50" + (p.enabled ? "" : " opacity-40");
    tr.innerHTML = `
      <td class="px-3 py-2"><span class="dot" style="background:${STATUS_COLOR[p.status] || "#a3a097"}"></span>
        <span class="text-mut ml-1">${p.status}</span></td>
      <td class="px-3 py-2">${maskProxy(p.url)}</td>
      <td class="px-3 py-2 text-mut">${p.exit_ip || "—"}</td>
      <td class="px-3 py-2 text-mut">${p.latency_ms || "—"}</td>
      <td class="px-3 py-2 text-mut">${p.success_count}/${p.fail_count}</td>
      <td class="px-3 py-2 text-right whitespace-nowrap">
        <button class="btn-ghost px-2 py-1 mr-1" onclick="testProxy(${p.id})">Test</button>
        <button class="btn-ghost px-2 py-1 mr-1" onclick="resetProxy(${p.id})">Reset</button>
        <button class="btn-ghost px-2 py-1 mr-1" onclick="toggleProxy(${p.id})">${p.enabled ? "Off" : "On"}</button>
        <button class="btn-ghost px-2 py-1" onclick="delProxy(${p.id})">✕</button>
      </td>`;
    tb.appendChild(tr);
    if (p.note) {
      const nr = document.createElement("tr");
      nr.innerHTML = `<td></td><td colspan="5" class="px-3 pb-2 text-mut/70 text-[11px]">${p.note}</td>`;
      tb.appendChild(nr);
    }
  }
}

function renderKeywords() {
  const tb = $("kwRows"); tb.innerHTML = "";
  for (const k of STATE.keywords) {
    const tr = document.createElement("tr");
    tr.className = "border-b border-line/50" + (k.enabled ? "" : " opacity-40");
    tr.innerHTML = `
      <td class="px-3 py-2 break-all">${k.value}</td>
      <td class="px-3 py-2"><span class="pill" style="background:${k.mode === "block" ? "#3a2420;color:#d4675a" : "#243a24;color:#6fae6f"}">${k.mode}</span></td>
      <td class="px-3 py-2 text-mut">${k.mode === "redact" ? k.replacement : "—"}</td>
      <td class="px-3 py-2 text-right whitespace-nowrap">
        <button class="btn-ghost px-2 py-1 mr-1" onclick="toggleKw(${k.id})">${k.enabled ? "Off" : "On"}</button>
        <button class="btn-ghost px-2 py-1" onclick="delKw(${k.id})">✕</button>
      </td>`;
    tb.appendChild(tr);
  }
}

function renderLogs() {
  const tb = $("logRows"); tb.innerHTML = "";
  for (const l of STATE.logs) {
    const col = l.status >= 200 && l.status < 300 ? "#6fae6f" : (l.status === 0 ? "#d9a657" : "#d4675a");
    const tr = document.createElement("tr");
    tr.className = "border-b border-line/50";
    tr.innerHTML = `
      <td class="px-3 py-2 text-mut">${fmtTime(l.ts)}</td>
      <td class="px-3 py-2">${l.path}${l.stream ? ' <span class="text-clay">∿</span>' : ""}</td>
      <td class="px-3 py-2" style="color:${col}">${l.status}</td>
      <td class="px-3 py-2 text-mut">${l.proxy ? maskProxy(l.proxy).split("  ")[0] : "—"}</td>
      <td class="px-3 py-2 text-mut">${l.attempts}</td>
      <td class="px-3 py-2 text-mut">${l.ms}</td>
      <td class="px-3 py-2 text-mut/80">${l.note}${l.redactions ? ` · ${l.redactions} redacted` : ""}</td>`;
    tb.appendChild(tr);
  }
}

function renderSettings() {
  const s = STATE.settings;
  $("s_max_retries").value = s.max_retries || "4";
  $("s_connect_timeout").value = s.connect_timeout || "20";
  $("s_read_timeout").value = s.read_timeout || "300";
  $("s_user_agent").value = s.user_agent || "";
}

function renderAll() {
  renderConn(); renderCounts(); renderProxies();
  renderKeywords(); renderLogs(); renderSettings();
}

async function refresh() {
  try {
    STATE = await api("/admin/state");
    renderAll();
  } catch (e) { /* logout handled */ }
}

// ---- actions ----
async function saveConn() {
  await api("/admin/settings", {
    endpoint: $("s_endpoint").value.trim(),
    gateway_key: $("s_gateway_key").value.trim(),
    upstream_key: $("s_upstream_key").value.trim(),
    require_client_key: $("s_require_client_key").checked ? "1" : "0",
  });
  toast("Saved"); refresh();
}
async function saveSettings() {
  await api("/admin/settings", {
    max_retries: $("s_max_retries").value.trim(),
    connect_timeout: $("s_connect_timeout").value.trim(),
    read_timeout: $("s_read_timeout").value.trim(),
    user_agent: $("s_user_agent").value.trim(),
  });
  toast("Saved"); refresh();
}
async function savePassword() {
  const pw = $("s_admin_password").value;
  if (!pw) return;
  await api("/admin/settings", { admin_password: pw });
  TOKEN = pw; localStorage.setItem("gw_token", pw);
  $("s_admin_password").value = ""; toast("Password updated");
}
async function addProxies() {
  const text = $("proxyInput").value.trim();
  if (!text) return;
  const r = await api("/admin/proxy/add", { text });
  $("proxyInput").value = "";
  toast(`Added ${r.added} (parsed ${r.parsed})`); refresh();
}
async function testProxy(id) {
  toast("Testing…");
  const r = await api("/admin/proxy/test", { id });
  toast(r.ok ? `OK · ${r.exit_ip} · ${r.latency_ms}ms` : `Bad · ${r.detail}`, !r.ok);
  refresh();
}
async function resetProxy(id) { await api("/admin/proxy/reset", { id }); refresh(); }
async function toggleProxy(id) { await api("/admin/proxy/toggle", { id }); refresh(); }
async function delProxy(id) { if (confirm("Delete this proxy?")) { await api("/admin/proxy/delete", { id }); refresh(); } }
async function addKeyword() {
  const value = $("kwValue").value.trim();
  if (!value) return;
  await api("/admin/keyword/add", { value, mode: $("kwMode").value, replacement: $("kwRepl").value });
  $("kwValue").value = ""; toast("Added"); refresh();
}
async function toggleKw(id) { await api("/admin/keyword/toggle", { id }); refresh(); }
async function delKw(id) { await api("/admin/keyword/delete", { id }); refresh(); }
async function clearLogs() { if (confirm("Clear logs?")) { await api("/admin/logs/clear", {}); refresh(); } }

// ---- boot ----
if (TOKEN) {
  $("login").classList.add("hidden");
  $("app").classList.remove("hidden");
  refresh();
}
