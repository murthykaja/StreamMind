const SERVICES = ["checkout", "payments", "inventory", "search", "auth"];
const WINDOW_MS = 5000;
const state = { metric: "rate", series: {}, alerts: [], truth: [], incidents: [], metrics: null, busy: {} };

const $ = (id) => document.getElementById(id);
const el = (tag, cls, text) => {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
};
const fmt = (v, d = 1, unit = "") => (v === null || v === undefined ? "–" : Number(v).toFixed(d) + unit);
const pct = (v) => (v === null || v === undefined ? "–" : Math.round(v * 100) + "%");
const ago = (ms) => {
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  return s < 60 ? s + "s ago" : Math.floor(s / 60) + "m " + (s % 60) + "s ago";
};

async function get(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(path + " " + r.status);
  return r.json();
}

function renderKpis() {
  const m = state.metrics;
  if (!m) return;
  const cards = [
    ["Alert → hypothesis", fmt(m.alert_to_hypothesis_s.p50, 2, " s"), "p95 " + fmt(m.alert_to_hypothesis_s.p95, 2, " s") + " · n=" + m.alert_to_hypothesis_s.n],
    ["Detection recall", pct(m.detection.recall), m.detection.true_positives + " of " + m.detection.injected_anomalies_settled + " injected anomalies"],
    ["Alert precision", pct(m.detection.precision), m.detection.false_positive_alerts + " false alerts (" + m.detection.false_positives_on_distractors + " on distractors)"],
    ["Freshness p50", fmt(m.freshness_ms.alerts.p50 / 1000, 1, " s"), "event → alert, p95 " + fmt(m.freshness_ms.alerts.p95 / 1000, 1, " s")],
    ["Root-cause accuracy", pct(m.root_cause.category_accuracy), "suspect service " + pct(m.root_cause.suspect_accuracy) + " · n=" + m.root_cause.incidents_scored],
    ["Detection delay", fmt(m.detection.detection_delay_s.p50, 0, " s"), "fault start → alert, p95 " + fmt(m.detection.detection_delay_s.p95, 0, " s")],
  ];
  const box = $("kpis");
  box.replaceChildren();
  for (const [label, value, sub] of cards) {
    const c = el("div", "kpi");
    c.append(el("div", "label", label), el("div", "value", value), el("div", "sub", sub));
    box.append(c);
  }
}

function renderFlow() {
  const m = state.metrics;
  if (!m) return;
  const p = m.pipeline;
  const nodes = [
    ["Generator", "synthetic events + faults", Number(p.events_total).toLocaleString() + " events"],
    ["Kafka", "events, alerts, context", "8 topics"],
    ["Stream detector", "dedup · watermark · windows", p.windows_closed.toLocaleString() + " windows"],
    ["Dedup / late", "exactly-once effect", p.duplicates_dropped + " dup · " + p.late_dropped + " late"],
    ["Alerts", "separate from agent path", p.alerts_emitted + " emitted"],
    ["LangGraph agent", "context → hypothesis → approval", Object.keys(m.root_cause.engines).join(", ") || "idle"],
  ];
  const box = $("flow");
  box.replaceChildren();
  for (const [t, s, v] of nodes) {
    const n = el("div", "node");
    n.append(el("div", "n-title", t), el("div", "n-sub", s), el("div", "n-val", v));
    box.append(n);
  }
}

function valueOf(point) {
  if (state.metric === "rate") return point.requests / (WINDOW_MS / 1000);
  if (state.metric === "err") return point.requests ? point.errors / point.requests : 0;
  return point.avg_latency_ms;
}

function metricLabel(v) {
  if (state.metric === "rate") return v.toFixed(1) + " req/s";
  if (state.metric === "err") return (v * 100).toFixed(1) + "% errors";
  return v.toFixed(0) + " ms";
}

const charts = {};
function ensureCharts() {
  const box = $("charts");
  if (box.children.length) return;
  for (const s of SERVICES) {
    const wrap = el("div", "chart");
    const head = el("div", "c-head");
    const name = el("span", "c-name", s);
    const val = el("span", "c-val", "–");
    head.append(name, val);
    const canvas = document.createElement("canvas");
    wrap.append(head, canvas);
    box.append(wrap);
    charts[s] = { wrap, canvas, val };
  }
}

function drawChart(service) {
  const { canvas, wrap, val } = charts[service];
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);
  const pts = (state.series[service] || []).slice(-72);
  if (pts.length < 2) return;
  const t0 = pts[0].t, t1 = pts[pts.length - 1].t + WINDOW_MS;
  const vals = pts.map(valueOf);
  let max = Math.max(...vals) * 1.15 || 1;
  if (state.metric === "err") max = Math.max(max, 0.1);
  const x = (t) => ((t - t0) / (t1 - t0)) * w;
  const y = (v) => h - 14 - (v / max) * (h - 22);

  ctx.strokeStyle = "#243049";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 2; i++) {
    const yy = 6 + ((h - 20) / 2) * i;
    ctx.beginPath(); ctx.moveTo(0, yy); ctx.lineTo(w, yy); ctx.stroke();
  }
  ctx.fillStyle = "#8a97b1";
  ctx.font = "10px sans-serif";
  ctx.fillText(metricLabel(max), 2, 12);

  for (const tr of state.truth) {
    if (!tr.affected.includes(service) || tr.end_ms < t0 || tr.start_ms > t1) continue;
    ctx.fillStyle = tr.kind === "anomaly" ? "rgba(245,185,66,.22)" : "rgba(138,151,177,.15)";
    const a = Math.max(0, x(tr.start_ms)), b = Math.min(w, x(tr.end_ms));
    ctx.fillRect(a, 0, Math.max(2, b - a), h - 12);
  }
  let alerting = false;
  for (const al of state.alerts) {
    if (al.service !== service || al.detected_at < t0) continue;
    const xx = x(Math.min(al.window_end, t1));
    ctx.fillStyle = "#ff5d6c";
    ctx.fillRect(xx - 1, 0, 2, h - 12);
    if (Date.now() - al.detected_at < 60000) alerting = true;
  }
  wrap.classList.toggle("alerting", alerting);

  const grad = ctx.createLinearGradient(0, 0, 0, h);
  grad.addColorStop(0, "rgba(91,140,255,.35)");
  grad.addColorStop(1, "rgba(91,140,255,0)");
  ctx.beginPath();
  pts.forEach((p, i) => (i ? ctx.lineTo(x(p.t + WINDOW_MS / 2), y(vals[i])) : ctx.moveTo(x(p.t + WINDOW_MS / 2), y(vals[i]))));
  ctx.strokeStyle = "#5b8cff";
  ctx.lineWidth = 1.8;
  ctx.stroke();
  ctx.lineTo(x(pts[pts.length - 1].t + WINDOW_MS / 2), h - 14);
  ctx.lineTo(x(pts[0].t + WINDOW_MS / 2), h - 14);
  ctx.fillStyle = grad;
  ctx.fill();
  val.textContent = metricLabel(vals[vals.length - 1]);
}

function renderCharts() {
  ensureCharts();
  for (const s of SERVICES) drawChart(s);
}

function incidentCard(inc) {
  const hyp = inc.hypothesis;
  const card = el("div", "inc " + inc.status);
  const top = el("div", "inc-top");
  top.append(el("span", "svc", inc.service));
  const sev = inc.first_alert.severity;
  top.append(el("span", "chip " + (sev === "critical" ? "crit" : ""), inc.first_alert.metric + " " + inc.first_alert.direction));
  if (hyp) top.append(el("span", "chip cat", hyp.category.replace("_", " ")));
  top.append(el("span", "chip " + inc.status, inc.status.replace("_", " ")));
  top.append(el("span", "time", ago(inc.created_at) + (inc.alert_to_hypothesis_s !== undefined ? " · alert→hypothesis " + inc.alert_to_hypothesis_s + "s" : "")));
  card.append(top);

  if (!hyp) {
    card.append(el("div", "summary reason", "Agent is retrieving logs, traces and deployments…"));
    return card;
  }
  card.append(el("div", "summary", hyp.summary));
  card.append(el("div", "reason", hyp.reasoning));

  const g = el("div", "grid2");
  const ev = el("div", "evidence");
  for (const e of inc.evidence || []) {
    const cited = hyp.citations.includes(e.id);
    if (!cited && inc.evidence.length > 8 && !e.id.match(/^E[1-3]$/)) continue;
    const row = el("div", "ev" + (cited ? " cited" : ""));
    row.append(el("b", "", e.id), el("span", "src", e.source), el("span", "", e.text));
    ev.append(row);
  }
  g.append(ev);

  const act = el("div", "action");
  act.append(el("div", "a-title", "Proposed action · confidence " + Math.round(hyp.confidence * 100) + "% · " + inc.engine));
  const a = hyp.action;
  act.append(el("div", "a-body", a.type === "none" ? "No action proposed" : a.type.replace("_", " ") + " → " + a.target + ": " + a.detail));
  if (inc.status === "pending_approval") {
    const btns = el("div", "btns");
    const ok = el("button", "btn ok", "Approve");
    const no = el("button", "btn no", "Reject");
    ok.disabled = no.disabled = !!state.busy[inc.id];
    ok.onclick = () => decide(inc.id, "approve");
    no.onclick = () => decide(inc.id, "reject");
    btns.append(ok, no);
    act.append(btns);
  } else if (inc.action_result) {
    act.append(el("div", "reason", inc.action_result.message || "Rejected by " + (inc.decided_by || "operator")));
  } else if (inc.status === "deciding") {
    act.append(el("div", "reason", "Decision submitted…"));
  }
  g.append(act);
  card.append(g);
  return card;
}

function renderIncidents() {
  const box = $("incidents");
  box.replaceChildren();
  if (!state.incidents.length) {
    box.append(el("div", "empty", "No incidents yet. The generator warms up for ~90 s, then injects faults every 40–70 s."));
    return;
  }
  for (const inc of state.incidents) box.append(incidentCard(inc));
}

async function decide(id, verb) {
  state.busy[id] = true;
  renderIncidents();
  try {
    await fetch("/api/incidents/" + id + "/" + verb + "?by=dashboard", { method: "POST" });
  } finally {
    delete state.busy[id];
    refresh();
  }
}

async function refresh() {
  try {
    const [series, alerts, truth, incidents, metrics] = await Promise.all([
      get("/api/series?minutes=8"), get("/api/alerts?limit=100"), get("/api/ground-truth?limit=40"),
      get("/api/incidents?limit=12"), get("/api/metrics"),
    ]);
    Object.assign(state, { series, alerts, truth, incidents, metrics });
    $("live").textContent = "live";
    $("live").className = "pill on";
    renderKpis();
    renderFlow();
    renderCharts();
    if (!Object.keys(state.busy).length) renderIncidents();
  } catch (e) {
    $("live").textContent = "offline";
    $("live").className = "pill off";
  }
}

$("metric-toggle").addEventListener("click", (e) => {
  const m = e.target.dataset && e.target.dataset.m;
  if (!m) return;
  state.metric = m;
  document.querySelectorAll("#metric-toggle button").forEach((b) => b.classList.toggle("on", b.dataset.m === m));
  renderCharts();
});

refresh();
setInterval(refresh, 2000);
