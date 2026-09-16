"use strict";

// The console's client. One file, no framework, no build step — it is a control
// panel for a backend, and a toolchain in front of it would be the largest thing
// in the repository.

const $ = (id) => document.getElementById(id);

const POLL_MS = 2000;

const state = {
  actions: [],
  busy: false,
  lastSeq: 0,
  charts: { lag: "Consumer lag", throughput: "Persist rate", accept_rate: "Accept rate" },
};

// ---------------------------------------------------------------- transport

// api sends the header the server's CSRF check looks for. A cross-origin form
// cannot set it, which is the whole reason it is here.
async function api(path, options = {}) {
  const res = await fetch(path, {
    ...options,
    headers: { "Content-Type": "application/json", "X-Console-Request": "1", ...(options.headers || {}) },
  });
  let body = null;
  try {
    body = await res.json();
  } catch {
    // A 204 or a proxy error page; the status is what matters.
  }
  if (!res.ok) {
    const err = new Error((body && body.error) || `request failed (${res.status})`);
    err.status = res.status;
    throw err;
  }
  return body;
}

// ---------------------------------------------------------------- auth gate

async function boot() {
  const session = await api("/api/session").catch(() => ({ authenticated: false }));
  if (session.authenticated) {
    showApp();
  } else {
    showLogin();
  }
}

function showLogin() {
  $("app").hidden = true;
  $("login").hidden = false;
  $("password").focus();
}

async function showApp() {
  $("login").hidden = true;
  $("app").hidden = false;
  await loadActions();
  connectStream();
  poll();
  setInterval(poll, POLL_MS);
}

$("login-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const err = $("login-error");
  err.hidden = true;
  try {
    await api("/api/login", {
      method: "POST",
      body: JSON.stringify({ password: $("password").value }),
    });
    $("password").value = "";
    showApp();
  } catch (e) {
    err.textContent = e.message;
    err.hidden = false;
  }
});

$("logout").addEventListener("click", async () => {
  await api("/api/logout", { method: "POST" }).catch(() => {});
  location.reload();
});

// ---------------------------------------------------------------- controls

const GROUP_TITLES = {
  stack: "Stack",
  load: "Load",
  chaos: "Chaos",
  experiment: "Recorded experiments",
};

async function loadActions() {
  const body = await api("/api/actions");
  state.actions = body.actions;

  const host = $("controls");
  host.textContent = "";

  for (const group of ["stack", "load", "chaos", "experiment"]) {
    const inGroup = state.actions.filter((a) => a.group === group);
    if (!inGroup.length) continue;

    const wrap = el("div", "group");
    wrap.append(el("div", "group-title", GROUP_TITLES[group]));
    const row = el("div", "row");
    for (const a of inGroup) row.append(controlFor(a));
    wrap.append(row);
    host.append(wrap);
  }
}

function controlFor(a) {
  const wrap = el("div", "btn-wrap");
  const inputs = {};

  // Parameters render before the button that consumes them, so the button reads
  // as the verb at the end of the sentence.
  if (a.params.length) {
    const fields = el("div", "row");
    for (const p of a.params) {
      const field = el("div", "field");
      field.append(el("label", null, p.unit ? `${p.label} (${p.unit})` : p.label));
      let input;
      if (p.kind === "choice") {
        input = document.createElement("select");
        for (const c of p.choices) {
          const opt = document.createElement("option");
          opt.value = c;
          opt.textContent = c;
          input.append(opt);
        }
      } else {
        input = document.createElement("input");
        input.type = "number";
        input.min = p.min;
        input.max = p.max;
        // The server clamps too; this only saves a round trip to find out.
        input.title = `${p.min} – ${p.max}`;
      }
      input.value = p.default;
      field.append(input);
      inputs[p.name] = input;
      fields.append(field);
    }
    wrap.append(fields);
  }

  const btn = el("button", a.destructive ? "danger" : "", a.label);
  btn.dataset.action = a.id;
  btn.addEventListener("click", () => {
    if (a.destructive && !confirm(`${a.label}\n\n${a.summary}\n\nRun it?`)) return;
    const params = {};
    for (const [name, input] of Object.entries(inputs)) params[name] = input.value;
    run(a.id, params);
  });

  wrap.append(btn);
  wrap.append(el("div", "btn-note", a.summary));
  return wrap;
}

async function run(id, params) {
  try {
    await api("/api/run", { method: "POST", body: JSON.stringify({ action: id, params }) });
    // Match the server, which drops the previous run's buffer when a new one
    // starts. Without this a tab that stayed open would accumulate every run
    // while a tab that reconnected showed only the latest, and the two would
    // disagree about what just happened.
    $("output").textContent = "";
    state.lastSeq = 0;
    setBusy(true);
    poll();
  } catch (e) {
    if (e.status === 401) return showLogin();
    appendLine({ stream: "console", text: `-- ${e.message} --` });
  }
}

$("stop").addEventListener("click", async () => {
  await api("/api/stop", { method: "POST" }).catch(() => {});
});

function setBusy(busy) {
  state.busy = busy;
  for (const btn of document.querySelectorAll("#controls button")) btn.disabled = busy;
  $("stop").disabled = !busy;
}

// ---------------------------------------------------------------- polling

async function poll() {
  let s;
  try {
    s = await api("/api/state");
  } catch (e) {
    if (e.status === 401) return showLogin();
    $("prom-status").textContent = "console unreachable";
    return;
  }
  renderRun(s.run);
  renderStack(s.stack);
  renderMetrics(s.metrics);
}

function renderRun(run) {
  setBusy(!!run.running);
  const host = $("run-status");
  host.textContent = "";

  if (!run.action_id) {
    host.append(dot("idle"), text("idle"));
    return;
  }
  if (run.running) {
    const secs = Math.round((Date.now() - new Date(run.started).getTime()) / 1000);
    host.append(dot("busy"), text(`${run.label} — ${secs}s`));
    return;
  }
  const failed = run.err || run.exit_code !== 0;
  host.append(dot(failed ? "bad" : "ok"), text(failed ? `${run.label} — ${run.err || "failed"}` : `${run.label} — done`));
}

function renderStack(stack) {
  const summary = $("stack-summary");
  summary.textContent = "";

  if (stack.err && !stack.containers?.length) {
    summary.append(text("stack "), strong("down"));
    $("containers").textContent = stack.err;
    return;
  }

  summary.append(text("stack "), strong(stack.up ? "up" : "down"));
  summary.append(text("workers "), strong(String(stack.workers ?? 0)));
  summary.append(text("containers "), strong(String(stack.containers?.length ?? 0)));

  const host = $("containers");
  host.textContent = "";
  for (const c of stack.containers || []) {
    const row = el("div", "ct");
    row.append(el("span", null, c.name));
    const st = el("span", `state state-${(c.state || "").toLowerCase()}`, c.state || "?");
    row.append(st);
    row.append(el("span", "status", c.health ? `${c.status} · ${c.health}` : c.status || ""));
    host.append(row);
  }
  if (!host.children.length) host.append(el("div", "muted", "no containers"));
}

function renderMetrics(m) {
  $("prom-status").textContent = m.reachable ? "" : m.err || "prometheus unreachable";

  const host = $("tiles");
  host.textContent = "";
  for (const [key, t] of Object.entries(m.tiles || {})) {
    host.append(tile(key, t));
  }

  const charts = $("charts");
  charts.textContent = "";
  for (const [key, label] of Object.entries(state.charts)) {
    charts.append(sparkline(label, (m.sparks || {})[key] || []));
  }
}

// tile decides its own severity. Dead letters and paused partitions are red at
// any non-zero value because both mean something stopped moving; rejections are
// amber, because a rejection under overload is the system working as designed.
function tile(key, t) {
  const node = el("div", "tile");
  node.append(el("div", "k", t.label));

  const v = el("div", "v");
  if (t.value === null || t.value === undefined) {
    v.append(text("—"));
  } else {
    v.append(text(format(key, t.value)));
    if (t.unit) v.append(el("span", "u", t.unit));
  }
  node.append(v);

  const n = t.value || 0;
  if ((key === "dlq" || key === "partitions_paused") && n > 0) node.classList.add("alert");
  else if (key === "rejected" && n > 0) node.classList.add("warn");
  else if (key === "lag" && n > 10000) node.classList.add("warn");
  return node;
}

function format(key, v) {
  if (key.startsWith("p99")) {
    // Seconds on the wire; milliseconds are what anyone actually reads a
    // latency in, until they are not, and then seconds are.
    return v < 1 ? `${(v * 1000).toFixed(1)} ms` : `${v.toFixed(2)} s`;
  }
  if (key.endsWith("_rate") || key === "throughput") return v.toFixed(0);
  return Math.round(v).toLocaleString();
}

// sparkline draws the last five minutes as an SVG polyline.
//
// Scaled from zero rather than from the minimum: a lag chart auto-scaled to its
// own floor makes 3 records and 300,000 look identical, which is precisely the
// difference the chart exists to show.
function sparkline(label, series) {
  const W = 300, H = 48, pad = 2;
  const node = el("div", "chart");

  const head = el("div", "chart-head");
  head.append(el("span", null, label));
  const peak = series.length ? Math.max(...series.map((p) => p[1])) : 0;
  head.append(el("span", "peak", series.length ? `peak ${format("", peak)}` : "no data"));
  node.append(head);

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.setAttribute("preserveAspectRatio", "none");

  if (series.length > 1) {
    const max = Math.max(peak, 1);
    const x = (i) => pad + (i / (series.length - 1)) * (W - 2 * pad);
    const y = (val) => H - pad - (val / max) * (H - 2 * pad);
    const points = series.map((p, i) => `${x(i).toFixed(1)},${y(p[1]).toFixed(1)}`).join(" ");

    const area = document.createElementNS("http://www.w3.org/2000/svg", "polygon");
    area.setAttribute("points", `${pad},${H - pad} ${points} ${W - pad},${H - pad}`);
    area.setAttribute("fill", "currentColor");
    area.setAttribute("opacity", "0.12");
    svg.append(area);

    const lineEl = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
    lineEl.setAttribute("points", points);
    lineEl.setAttribute("fill", "none");
    lineEl.setAttribute("stroke", "currentColor");
    lineEl.setAttribute("stroke-width", "1.5");
    lineEl.setAttribute("vector-effect", "non-scaling-stroke");
    svg.append(lineEl);
  }

  node.append(svg);
  return node;
}

// ---------------------------------------------------------------- output

// connectStream keeps the output pane attached.
//
// EventSource reconnects by itself, which matters here more than usual: several
// of these buttons take the machine's network stack or its containers away for a
// while, and the console has to come back on its own when they return.
function connectStream() {
  const es = new EventSource("/api/stream");
  es.onmessage = (e) => {
    const l = JSON.parse(e.data);
    // The server replays its buffer on every connect, so a reconnect mid-run
    // would otherwise print the run twice.
    if (l.seq && l.seq <= state.lastSeq) return;
    state.lastSeq = l.seq || 0;
    appendLine(l);
  };
  es.onerror = () => {
    // Left to EventSource's own retry. Logging here would fill the pane with
    // reconnect noise during exactly the outages being demonstrated.
  };
}

function appendLine(l) {
  const out = $("output");
  const atBottom = out.scrollHeight - out.scrollTop - out.clientHeight < 40;

  const span = el("span", `l-${l.stream}`, l.text + "\n");
  out.append(span);

  while (out.childNodes.length > 2000) out.removeChild(out.firstChild);
  if ($("follow").checked && atBottom) out.scrollTop = out.scrollHeight;
}

// ---------------------------------------------------------------- dom helpers

function el(tag, cls, txt) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (txt !== undefined && txt !== null) node.textContent = txt;
  return node;
}

function text(s) { return document.createTextNode(s); }
function strong(s) { const b = document.createElement("b"); b.textContent = s + " "; return b; }
function dot(kind) { return el("span", `dot ${kind}`); }

boot();
