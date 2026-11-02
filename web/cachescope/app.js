"use strict";

const HISTORY_MS = 60000;
const numberFormat = new Intl.NumberFormat(undefined);
const oneFormat = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const clockFormat = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

const state = {
  conn: "connecting",
  ready: false,
  metrics: null,
  info: null,
  keys: [],
  lru: [],
  lruIndex: new Map(),
  events: [],
  replay: [],
  replaySource: "none",
  history: [],
  prev: null,
  receivedAt: 0,
  ticks: 0,
  sort: "recency",
  dir: "desc",
  selected: null,
  keySignature: "",
  tab: "log",
  eventFilter: "all",
  cursor: null,
};

const els = {
  host: document.querySelector("#host"),
  engine: document.querySelector("#engine"),
  conn: document.querySelector("#conn"),
  uptime: document.querySelector("#uptime"),
  theme: document.querySelector("#theme"),
  refresh: document.querySelector("#refresh"),
  banner: document.querySelector("#banner"),
  opsValue: document.querySelector("#opsValue"),
  opsDetail: document.querySelector("#opsDetail"),
  opsSpark: document.querySelector("#opsSpark"),
  hitValue: document.querySelector("#hitValue"),
  hitDetail: document.querySelector("#hitDetail"),
  hitSpark: document.querySelector("#hitSpark"),
  keysValue: document.querySelector("#keysValue"),
  keysDetail: document.querySelector("#keysDetail"),
  keysSpark: document.querySelector("#keysSpark"),
  memValue: document.querySelector("#memValue"),
  memDetail: document.querySelector("#memDetail"),
  memSpark: document.querySelector("#memSpark"),
  evictValue: document.querySelector("#evictValue"),
  evictDetail: document.querySelector("#evictDetail"),
  evictSpark: document.querySelector("#evictSpark"),
  chart: document.querySelector("#chart"),
  chartTip: document.querySelector("#chartTip"),
  chartSummary: document.querySelector("#chartSummary"),
  chartLive: document.querySelector("#chartLive"),
  legendOps: document.querySelector("#legendOps"),
  legendLat: document.querySelector("#legendLat"),
  tipTime: document.querySelector("#tipTime"),
  tipOps: document.querySelector("#tipOps"),
  tipLat: document.querySelector("#tipLat"),
  tabLog: document.querySelector("#tab-log"),
  tabAppend: document.querySelector("#tab-append"),
  eventFilters: document.querySelector("#eventFilters"),
  eventCount: document.querySelector("#eventCount"),
  eventList: document.querySelector("#eventList"),
  keyFilter: document.querySelector("#keyFilter"),
  keyCountLabel: document.querySelector("#keyCountLabel"),
  inspector: document.querySelector("#inspector"),
  keysBody: document.querySelector("#keysBody"),
  commandForm: document.querySelector("#commandForm"),
  commandName: document.querySelector("#commandName"),
  commandKey: document.querySelector("#commandKey"),
  commandValue: document.querySelector("#commandValue"),
  commandTTL: document.querySelector("#commandTTL"),
  valueField: document.querySelector("#valueField"),
  ttlField: document.querySelector("#ttlField"),
  commandOutput: document.querySelector("#commandOutput"),
  commandSubmit: document.querySelector("#commandSubmit"),
};

let stream = null;
let streamGeneration = 0;

function setConn(mode) {
  state.conn = mode;
  const labels = {
    connecting: "Connecting",
    live: "Live",
    reconnecting: "Reconnecting",
    offline: "Offline",
  };
  els.conn.textContent = labels[mode] || mode;
  els.conn.className = "conn" + (mode === "live" ? " live" : mode === "offline" || mode === "reconnecting" ? " bad" : "");
  document.body.dataset.conn = mode;
}

function showBanner(message, isError) {
  els.banner.hidden = false;
  els.banner.classList.toggle("error", Boolean(isError));
  els.banner.setAttribute("role", isError ? "alert" : "status");
  els.banner.replaceChildren();
  const text = document.createElement("span");
  text.textContent = message;
  const retry = document.createElement("button");
  retry.type = "button";
  retry.textContent = "Retry";
  retry.addEventListener("click", () => {
    connectStream();
    refresh().catch((error) => showBanner(error.message, true));
  });
  els.banner.append(text, retry);
}

function clearBanner() {
  els.banner.hidden = true;
  els.banner.replaceChildren();
}

async function fetchJSON(path, options) {
  let response;
  try {
    response = await fetch(path, options);
  } catch {
    throw new Error("TinyCache is not responding.");
  }
  let data = null;
  try {
    data = await response.json();
  } catch {
    data = null;
  }
  if (!response.ok) {
    throw new Error((data && data.error) || `Request failed (${response.status})`);
  }
  if (data == null) {
    throw new Error("Empty response");
  }
  return data;
}

function applySummary(summary) {
  state.metrics = summary.metrics || {};
  state.keys = Array.isArray(summary.keys) ? summary.keys : [];
  state.lru = Array.isArray(summary.lru) ? summary.lru : [];
  state.lruIndex = new Map(state.lru.map((key, index) => [key, index + 1]));
  state.events = Array.isArray(summary.events) ? summary.events : [];
  state.receivedAt = Date.now();
  state.ready = true;
  if (state.selected && !state.keys.some((item) => item.key === state.selected)) {
    state.selected = null;
  }
  noteSample(state.metrics);
  render();
}

function applyReplay(payload) {
  state.replay = Array.isArray(payload.records) ? payload.records : [];
  state.replaySource = payload.source || "none";
  if (state.tab === "append") renderEvents();
}

function noteSample(metrics) {
  const now = Date.now();
  const prev = state.prev;
  let ops = null;
  if (prev) {
    const dt = (now - prev.at) / 1000;
    if (dt >= 0.4 && dt <= 5) {
      ops = (Number(metrics.totalRequests || 0) - prev.total) / dt;
    }
  }
  const lookups = Number(metrics.hits || 0) + Number(metrics.misses || 0);
  const point = {
    at: now,
    ops,
    latency: p95(metrics),
    hitRatio: lookups > 0 ? Number(metrics.hitRatio || 0) : null,
    keys: Number(metrics.keyCount || 0),
    memory: Number(metrics.memoryBytes || 0),
    evictions: Number(metrics.evictions || 0),
  };
  state.prev = {
    at: now,
    total: Number(metrics.totalRequests || 0),
    evictions: Number(metrics.evictions || 0),
  };
  const last = state.history[state.history.length - 1];
  if (last && now - last.at < 400) {
    point.at = last.at;
    if (point.ops == null) point.ops = last.ops;
    state.history[state.history.length - 1] = point;
  } else {
    state.history.push(point);
  }
  pruneHistory();
}

function pruneHistory() {
  const cutoff = Date.now() - HISTORY_MS;
  let dropped = 0;
  while (state.history.length > 2 && state.history[0].at < cutoff) {
    state.history.shift();
    dropped += 1;
  }
  while (state.history.length > 120) {
    state.history.shift();
    dropped += 1;
  }
  if (state.cursor != null && dropped) {
    state.cursor = Math.max(0, Math.min(state.history.length - 1, state.cursor - dropped));
  }
}

async function refresh() {
  const [summary, replay] = await Promise.all([fetchJSON("/debug/summary"), fetchJSON("/debug/replay")]);
  applySummary(summary);
  applyReplay(replay);
  if (state.conn === "connecting" || state.conn === "offline") setConn("live");
  if (state.conn === "live") clearBanner();
}

async function refreshReplay() {
  try {
    applyReplay(await fetchJSON("/debug/replay"));
  } catch {
    /* replay is optional; the event stream still updates the log */
  }
}

function loadInfo() {
  fetchJSON("/debug/info")
    .then((info) => {
      state.info = info;
      renderHeader();
      renderStats();
    })
    .catch(() => {
      /* older servers omit this; the rest of the dashboard still works */
    });
}

function connectStream() {
  if (!window.EventSource) {
    if (!state.poll) {
      state.poll = window.setInterval(() => {
        refresh().catch(() => {
          setConn(state.ready ? "reconnecting" : "offline");
          showBanner("TinyCache is not responding.", true);
        });
      }, 2000);
    }
    return;
  }
  streamGeneration += 1;
  const generation = streamGeneration;
  if (stream) stream.close();
  stream = new EventSource("/debug/events/stream");
  stream.addEventListener("snapshot", (event) => {
    if (generation !== streamGeneration) return;
    try {
      applySummary(JSON.parse(event.data));
      setConn("live");
      clearBanner();
      state.ticks += 1;
      if (state.ticks % 5 === 0) refreshReplay();
    } catch {
      showBanner("Could not read the live snapshot.", true);
    }
  });
  stream.addEventListener("error", () => {
    if (generation !== streamGeneration) return;
    if (state.ready) {
      setConn("reconnecting");
      showBanner("Stream interrupted. Showing the last snapshot.", false);
    } else {
      setConn("offline");
      showBanner("TinyCache is not responding.", true);
      render();
    }
  });
}

function render() {
  renderHeader();
  renderStats();
  renderChart();
  renderKeys();
  renderEvents();
}

function renderHeader() {
  els.host.textContent = location.host;
  const policy = state.info && state.info.evictionPolicy ? String(state.info.evictionPolicy).toUpperCase() : "";
  const maxKeys = state.info && state.info.maxKeys ? formatNumber(state.info.maxKeys) : "";
  els.engine.textContent = [policy, maxKeys ? `${maxKeys} max` : ""].filter(Boolean).join(" · ");
  if (state.metrics) {
    els.uptime.textContent = "Up " + formatDuration(currentUptime());
    if (state.metrics.startedAt) {
      els.uptime.title = `Started ${formatClock(state.metrics.startedAt)}`;
    }
  }
}

function renderStats() {
  const metrics = state.metrics;
  const latest = latestSample();
  if (!metrics) {
    return;
  }
  const lookups = Number(metrics.hits || 0) + Number(metrics.misses || 0);
  els.opsValue.textContent = latest && latest.ops != null ? formatOps(latest.ops) : "—";
  els.opsDetail.textContent = Number(metrics.totalRequests || 0) > 0
    ? `${formatNumber(metrics.totalRequests)} requests`
    : "Waiting for samples";
  if (lookups > 0) {
    els.hitValue.textContent = formatPercent(metrics.hitRatio || 0);
    els.hitDetail.textContent = `${formatNumber(metrics.hits || 0)} hits · ${formatNumber(metrics.misses || 0)} misses`;
  } else {
    els.hitValue.textContent = "—";
    els.hitDetail.textContent = "No reads yet";
  }
  els.keysValue.textContent = formatNumber(metrics.keyCount || 0);
  if (state.info && state.info.maxKeys) {
    const pct = state.info.maxKeys > 0 ? Math.round((Number(metrics.keyCount || 0) / state.info.maxKeys) * 100) : 0;
    els.keysDetail.textContent = `${pct}% of ${formatNumber(state.info.maxKeys)}`;
  } else {
    els.keysDetail.textContent = "Resident";
  }
  els.memValue.textContent = formatBytes(metrics.memoryBytes || 0);
  els.memDetail.textContent = Number(metrics.memoryBytes || 0) >= 1024
    ? `${formatNumber(metrics.memoryBytes)} B`
    : "Estimated";
  els.evictValue.textContent = formatNumber(metrics.evictions || 0);
  const evictDelta = windowDelta((sample) => sample.evictions);
  els.evictDetail.textContent = evictDelta == null ? "Total" : `${signed(evictDelta)} in 60s`;

  setSpark(els.opsSpark, state.history.map((sample) => sample.ops));
  setSpark(els.hitSpark, state.history.map((sample) => (sample.hitRatio == null ? null : sample.hitRatio * 100)));
  setSpark(els.keysSpark, state.history.map((sample) => sample.keys));
  setSpark(els.memSpark, state.history.map((sample) => sample.memory));
  setSpark(els.evictSpark, state.history.map((sample) => sample.evictions));
}

function latestSample() {
  for (let i = state.history.length - 1; i >= 0; i -= 1) {
    if (state.history[i].ops != null) return state.history[i];
  }
  return null;
}

function windowDelta(pick) {
  if (state.history.length < 2) return null;
  const first = pick(state.history[0]);
  const last = pick(state.history[state.history.length - 1]);
  if (first == null || last == null) return null;
  return last - first;
}

function renderKeys() {
  const filter = els.keyFilter.value.trim().toLowerCase();
  const filtered = state.keys.filter((item) => String(item.key).toLowerCase().includes(filter));
  const sorted = filtered.slice().sort(compareKeys);
  const signature = [
    state.sort,
    state.dir,
    filter,
    state.ready ? "1" : "0",
    sorted.map((item) => [item.key, item.sizeBytes, item.accessCount, item.lastAccessedAt, item.expiresAt || ""].join("\u0001")).join("\u0002"),
  ].join("\u0003");
  els.keyCountLabel.textContent = keyCountText(filtered.length);
  if (signature === state.keySignature) {
    renderInspector();
    return;
  }
  state.keySignature = signature;
  const activeKey = document.activeElement && document.activeElement.closest("#keysBody tr")
    ? document.activeElement.dataset.key
    : "";
  const scroll = els.keysBody.parentElement.scrollTop;
  els.keysBody.replaceChildren();
  if (!state.ready) {
    els.keysBody.append(emptyRow(state.conn === "offline" ? "No snapshot yet" : "Loading keys"));
    renderInspector();
    return;
  }
  if (state.keys.length === 0) {
    els.keysBody.append(emptyRow("No keys in memory"));
    renderInspector();
    return;
  }
  if (sorted.length === 0) {
    els.keysBody.append(emptyRow("No keys match"));
    renderInspector();
    return;
  }
  sorted.forEach((item, index) => {
    const selected = item.key === state.selected;
    const row = document.createElement("tr");
    row.dataset.key = item.key;
    row.tabIndex = selected || (!state.selected && index === 0) ? 0 : -1;
    row.setAttribute("aria-selected", selected ? "true" : "false");
    row.append(cell("td", item.key, "key", item.key));
    row.append(cell("td", "string", "type"));
    row.append(cell("td", formatBytes(item.sizeBytes || 0), "num"));
    const ttl = document.createElement("td");
    ttl.className = "num";
    ttl.dataset.field = "ttl";
    ttl.dataset.expires = item.expiresAt || "";
    ttl.dataset.ttl = String(item.ttl);
    ttl.textContent = ttlLabel(item);
    row.append(ttl);
    row.append(cell("td", formatNumber(item.accessCount || 0), "num"));
    const recency = document.createElement("td");
    recency.className = "num";
    recency.dataset.field = "recency";
    recency.dataset.at = item.lastAccessedAt || "";
    recency.title = recencyTitle(item);
    recency.textContent = recencyLabel(item.lastAccessedAt);
    row.append(recency);
    els.keysBody.append(row);
  });
  els.keysBody.parentElement.scrollTop = scroll;
  if (activeKey) {
    const row = els.keysBody.querySelector(`[data-key="${CSS.escape(activeKey)}"]`);
    if (row) row.focus();
  }
  renderInspector();
}

function keyCountText(shown) {
  if (!state.ready) return "Loading";
  if (shown !== state.keys.length) return `${formatNumber(shown)} of ${formatNumber(state.keys.length)}`;
  return `${formatNumber(state.keys.length)} ${state.keys.length === 1 ? "key" : "keys"}`;
}

function renderInspector() {
  const item = state.keys.find((entry) => entry.key === state.selected);
  if (!item) {
    els.inspector.hidden = true;
    els.inspector.textContent = "";
    return;
  }
  const rank = state.lruIndex.get(item.key);
  const parts = [
    item.key,
    "string",
    formatBytes(item.sizeBytes || 0),
    `freq ${formatNumber(item.accessCount || 0)}`,
    rank ? `lru ${rank} of ${state.lru.length}` : "",
    item.createdAt ? `created ${formatClock(item.createdAt)}` : "",
    item.updatedAt ? `updated ${formatClock(item.updatedAt)}` : "",
  ].filter(Boolean);
  els.inspector.hidden = false;
  els.inspector.textContent = parts.join("  ·  ");
}

function selectKey(key) {
  state.selected = key;
  for (const row of els.keysBody.querySelectorAll("tr[data-key]")) {
    const selected = row.dataset.key === key;
    row.setAttribute("aria-selected", selected ? "true" : "false");
    row.tabIndex = selected ? 0 : -1;
  }
  if (document.activeElement !== els.commandKey) els.commandKey.value = key;
  renderInspector();
}

function renderEvents() {
  const scroll = els.eventList.scrollTop;
  els.eventList.replaceChildren();
  els.eventFilters.hidden = state.tab !== "log";
  if (state.tab === "append") {
    renderAppend(scroll);
    return;
  }
  if (!state.ready) {
    els.eventCount.textContent = "";
    els.eventList.append(paragraph(state.conn === "offline" ? "No snapshot yet" : "Loading events"));
    return;
  }
  const events = state.events.filter(eventMatches).slice(-80).reverse();
  els.eventCount.textContent = events.length ? String(events.length) : "";
  if (events.length === 0) {
    els.eventList.append(paragraph(state.events.length === 0 ? "No operations yet" : "No events match"));
    return;
  }
  for (const event of events) {
    const row = document.createElement("div");
    row.className = "event" + (event.type === "evict" ? " evict" : "");
    const time = document.createElement("time");
    time.dateTime = event.at || "";
    time.textContent = event.at ? formatClock(event.at) : "—";
    const type = document.createElement("span");
    type.className = "etype";
    type.textContent = event.type || "event";
    const key = document.createElement("span");
    key.className = "ekey";
    key.textContent = event.key || event.reason || "—";
    key.title = event.key || event.reason || "";
    const detail = document.createElement("span");
    detail.className = "edetail";
    detail.textContent = eventDetail(event);
    row.append(time, type, key, detail);
    els.eventList.append(row);
  }
  els.eventList.scrollTop = scroll;
}

function renderAppend(scroll) {
  if (!state.ready) {
    els.eventCount.textContent = "";
    els.eventList.append(paragraph("Loading append log"));
    return;
  }
  const records = state.replay.slice(-80).reverse();
  els.eventCount.textContent = records.length ? String(records.length) : "";
  if (records.length === 0) {
    const message = state.replaySource === "appendLog"
      ? "Append log is empty"
      : "No append log. Start the server with --aof-path to record mutations.";
    els.eventList.append(paragraph(message));
    return;
  }
  for (const record of records) {
    const row = document.createElement("div");
    row.className = "event";
    const time = document.createElement("time");
    time.dateTime = record.at || "";
    time.textContent = record.at ? formatClock(record.at) : "—";
    const type = document.createElement("span");
    type.className = "etype";
    type.textContent = record.command || "log";
    const key = document.createElement("span");
    key.className = "ekey";
    key.textContent = record.key || "—";
    const detail = document.createElement("span");
    detail.className = "edetail";
    detail.textContent = record.ttlSeconds ? `${formatNumber(record.ttlSeconds)}s` : "";
    row.append(time, type, key, detail);
    els.eventList.append(row);
  }
  els.eventList.scrollTop = scroll;
}

function eventMatches(event) {
  const type = event.type || "";
  if (state.eventFilter === "evict") return type === "evict";
  if (state.eventFilter === "read") return type === "get" || type === "ttl";
  if (state.eventFilter === "write") return type === "set" || type === "delete" || type === "expire";
  return true;
}

function eventDetail(event) {
  const parts = [];
  if (event.hit === true) parts.push("hit");
  if (event.hit === false) parts.push("miss");
  if (event.reason && event.key) parts.push(event.reason);
  if (event.latencyUs) parts.push(formatLatency(event.latencyUs));
  return parts.join(" · ");
}

function renderChart() {
  const canvas = els.chart;
  const frame = canvas.parentElement;
  const width = frame.clientWidth;
  const height = frame.clientHeight;
  describeChart();
  if (width < 20 || height < 20) return;
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  canvas.width = Math.round(width * dpr);
  canvas.height = Math.round(height * dpr);
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);

  const style = getComputedStyle(document.documentElement);
  const muted = style.getPropertyValue("--muted").trim();
  const line = style.getPropertyValue("--line").trim();
  const accent = style.getPropertyValue("--accent").trim();
  const second = style.getPropertyValue("--chart").trim();
  const strong = style.getPropertyValue("--line-strong").trim();
  const pad = { l: 48, r: 52, t: 8, b: 24 };
  const plotW = width - pad.l - pad.r;
  const plotH = height - pad.t - pad.b;
  ctx.font = "11px ui-sans-serif, system-ui, sans-serif";
  ctx.fillStyle = muted;
  ctx.textBaseline = "middle";

  const ops = state.history.map((sample) => sample.ops).filter((value) => value != null && Number.isFinite(value));
  if (plotW < 24 || plotH < 24 || ops.length < 2) {
    ctx.textAlign = "center";
    ctx.fillText(state.ready ? "Waiting for samples" : "Loading", width / 2, height / 2);
    return;
  }

  const lat = state.history.map((sample) => sample.latency).filter((value) => value != null && Number.isFinite(value));
  const opsMax = Math.max(1, ...ops);
  const latMax = Math.max(1, ...lat);
  ctx.strokeStyle = line;
  ctx.lineWidth = 1;
  ctx.textAlign = "right";
  for (let i = 0; i <= 3; i += 1) {
    const y = Math.round(pad.t + (plotH * i) / 3) + 0.5;
    ctx.beginPath();
    ctx.moveTo(pad.l, y);
    ctx.lineTo(pad.l + plotW, y);
    ctx.stroke();
    ctx.fillText(formatAxis(opsMax * (1 - i / 3)), pad.l - 8, y);
  }
  ctx.textAlign = "left";
  ctx.fillText(formatLatency(latMax), pad.l + plotW + 8, pad.t + 4);
  ctx.fillText("0", pad.l + plotW + 8, pad.t + plotH);

  const xOf = (index) => pad.l + (plotW * index) / (state.history.length - 1);
  strokeSeries(ctx, (sample) => sample.latency, (value) => pad.t + plotH - (value / latMax) * plotH, xOf, second, [4, 3]);
  strokeSeries(ctx, (sample) => sample.ops, (value) => pad.t + plotH - (value / opsMax) * plotH, xOf, accent, []);

  ctx.fillStyle = muted;
  ctx.textBaseline = "top";
  ctx.textAlign = "left";
  ctx.fillText(formatClock(state.history[0].at), pad.l, pad.t + plotH + 8);
  ctx.textAlign = "right";
  ctx.fillText(formatClock(state.history[state.history.length - 1].at), pad.l + plotW, pad.t + plotH + 8);

  if (state.cursor != null && state.history[state.cursor]) {
    const x = Math.round(xOf(state.cursor)) + 0.5;
    ctx.strokeStyle = strong;
    ctx.setLineDash([]);
    ctx.beginPath();
    ctx.moveTo(x, pad.t);
    ctx.lineTo(x, pad.t + plotH);
    ctx.stroke();
  }
}

function strokeSeries(ctx, pick, yOf, xOf, color, dash) {
  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5;
  ctx.lineJoin = "round";
  ctx.lineCap = "round";
  ctx.setLineDash(dash);
  ctx.beginPath();
  let started = false;
  state.history.forEach((sample, index) => {
    const value = pick(sample);
    if (value == null || !Number.isFinite(value)) {
      started = false;
      return;
    }
    const x = xOf(index);
    const y = yOf(value);
    if (!started) {
      ctx.moveTo(x, y);
      started = true;
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();
  ctx.restore();
}

function describeChart() {
  const latest = latestSample();
  const last = state.history[state.history.length - 1];
  const ops = latest ? formatOps(latest.ops) : "—";
  const latency = last && last.latency != null ? formatLatency(last.latency) : "—";
  els.legendOps.textContent = ops;
  els.legendLat.textContent = latency;
  const summary = `Latest ops per second ${ops}. p95 latency ${latency}.`;
  els.chartSummary.textContent = summary;
  els.chart.setAttribute("aria-label", `Throughput. ${summary}`);
  els.chart.dataset.samples = String(state.history.length);
}

function setCursor(index, announce) {
  if (index == null || !state.history[index]) {
    state.cursor = null;
    els.chartTip.hidden = true;
    renderChart();
    return;
  }
  state.cursor = index;
  const sample = state.history[index];
  els.tipTime.textContent = formatClock(sample.at);
  els.tipOps.textContent = formatOps(sample.ops);
  els.tipLat.textContent = sample.latency == null ? "—" : formatLatency(sample.latency);
  els.chartTip.hidden = false;
  renderChart();
  const rect = els.chart.getBoundingClientRect();
  const plotW = rect.width - 48 - 52;
  const x = 48 + (state.history.length <= 1 ? 0 : (plotW * index) / (state.history.length - 1));
  const tipW = els.chartTip.offsetWidth || 140;
  let left = x + 12;
  if (left + tipW > rect.width - 8) left = x - tipW - 12;
  els.chartTip.style.left = `${Math.max(8, left)}px`;
  els.chartTip.style.top = "8px";
  if (announce) {
    els.chartLive.textContent = `${formatClock(sample.at)}, ${formatOps(sample.ops)} ops per second, p95 ${els.tipLat.textContent}`;
  }
}

function chartIndexFromEvent(event) {
  const rect = els.chart.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const plotW = rect.width - 48 - 52;
  if (state.history.length < 2 || plotW <= 0) return null;
  const ratio = (x - 48) / plotW;
  return Math.max(0, Math.min(state.history.length - 1, Math.round(ratio * (state.history.length - 1))));
}

function updateSortHeaders() {
  document.querySelectorAll("th[aria-sort]").forEach((th) => {
    const button = th.querySelector("[data-sort]");
    const mark = th.querySelector(".mark");
    if (!button || !mark) return;
    if (button.dataset.sort === state.sort) {
      th.setAttribute("aria-sort", state.dir === "asc" ? "ascending" : "descending");
      mark.textContent = state.dir === "asc" ? "↑" : "↓";
    } else {
      th.setAttribute("aria-sort", "none");
      mark.textContent = "";
    }
  });
}

function compareKeys(a, b) {
  let result = 0;
  if (state.sort === "key") result = String(a.key).localeCompare(String(b.key));
  else if (state.sort === "type") result = 0;
  else if (state.sort === "size") result = Number(a.sizeBytes || 0) - Number(b.sizeBytes || 0);
  else if (state.sort === "ttl") result = ttlSortValue(a) - ttlSortValue(b);
  else if (state.sort === "freq") result = Number(a.accessCount || 0) - Number(b.accessCount || 0);
  else result = timeValue(a.lastAccessedAt) - timeValue(b.lastAccessedAt);
  if (result === 0) result = String(a.key).localeCompare(String(b.key));
  return state.dir === "desc" ? -result : result;
}

function setOutput(text, bad) {
  els.commandOutput.hidden = !text;
  els.commandOutput.textContent = text || "";
  els.commandOutput.title = text || "";
  els.commandOutput.classList.toggle("bad", Boolean(bad));
}

function syncCommandFields() {
  const command = els.commandName.value;
  els.valueField.hidden = command !== "set";
  els.ttlField.hidden = command !== "set" && command !== "expire";
}

async function executeCommand(event) {
  event.preventDefault();
  if (els.commandSubmit.disabled) return;
  const command = els.commandName.value;
  const key = els.commandKey.value.trim();
  const value = els.commandValue.value;
  const ttlSeconds = Number(els.commandTTL.value || 0);
  els.commandSubmit.disabled = true;
  try {
    let result;
    if (command === "set") {
      result = await fetchJSON("/command/set", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ key, value, ttlSeconds }),
      });
    } else if (command === "get") {
      result = await fetchJSON(`/command/get?key=${encodeURIComponent(key)}`);
    } else if (command === "del") {
      result = await fetchJSON(`/command/del?key=${encodeURIComponent(key)}`, { method: "DELETE" });
    } else if (command === "expire") {
      result = await fetchJSON("/command/expire", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ key, ttlSeconds }),
      });
    } else {
      result = await fetchJSON(`/command/ttl?key=${encodeURIComponent(key)}`);
    }
    setOutput(JSON.stringify(result), false);
    await refresh();
  } catch (error) {
    setOutput(error.message, true);
  } finally {
    els.commandSubmit.disabled = false;
  }
}

function tick() {
  if (state.metrics) els.uptime.textContent = "Up " + formatDuration(currentUptime());
  const now = Date.now();
  document.querySelectorAll("[data-field=ttl]").forEach((cell) => {
    cell.textContent = ttlLabel({ expiresAt: cell.dataset.expires || null, ttl: Number(cell.dataset.ttl) }, now);
  });
  document.querySelectorAll("[data-field=recency]").forEach((cell) => {
    cell.textContent = recencyLabel(cell.dataset.at, now);
  });
}

function applyTheme(value) {
  if (value === "light" || value === "dark") {
    document.documentElement.setAttribute("data-theme", value);
    try { localStorage.setItem("cachescope-theme", value); } catch { /* ignore */ }
  } else {
    document.documentElement.removeAttribute("data-theme");
    try { localStorage.removeItem("cachescope-theme"); } catch { /* ignore */ }
  }
  renderChart();
}

function cell(tag, text, className, title) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (title) node.title = title;
  node.textContent = text;
  return node;
}

function emptyRow(text) {
  const row = document.createElement("tr");
  const td = document.createElement("td");
  td.colSpan = 6;
  td.className = "empty";
  td.textContent = text;
  row.append(td);
  return row;
}

function paragraph(text) {
  const node = document.createElement("p");
  node.className = "empty";
  node.textContent = text;
  return node;
}

function setSpark(svg, values) {
  const nums = values.filter((value) => value != null && Number.isFinite(value));
  svg.replaceChildren();
  if (nums.length < 2) return;
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const span = max - min || 1;
  const width = 80;
  const height = 24;
  const step = (width - 2) / (nums.length - 1);
  const points = nums.map((value, index) => {
    const x = 1 + index * step;
    const y = 1 + (height - 2) - ((value - min) / span) * (height - 4);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  const line = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
  line.setAttribute("points", points);
  line.setAttribute("fill", "none");
  line.setAttribute("stroke", "currentColor");
  line.setAttribute("stroke-width", "1");
  line.setAttribute("stroke-linejoin", "round");
  line.setAttribute("stroke-linecap", "round");
  svg.append(line);
}

function p95(metrics) {
  const latencies = metrics.latencyByCommand || {};
  let max = 0;
  let any = false;
  for (const item of Object.values(latencies)) {
    any = true;
    max = Math.max(max, Number(item.p95Us || 0));
  }
  return any ? max : null;
}

function currentUptime() {
  if (!state.metrics) return 0;
  const base = Number(state.metrics.uptimeSeconds || 0);
  if (!state.receivedAt) return base;
  return base + (Date.now() - state.receivedAt) / 1000;
}

function ttlLabel(item, now = Date.now()) {
  if (item.expiresAt) {
    const seconds = Math.round((new Date(item.expiresAt).getTime() - now) / 1000);
    if (!Number.isFinite(seconds)) return "none";
    return formatTTL(Math.max(0, seconds));
  }
  if (item.ttl === -1) return "none";
  if (item.ttl === -2 || item.ttl == null || Number.isNaN(item.ttl)) return "—";
  return formatTTL(Math.max(0, Number(item.ttl)));
}

function ttlSortValue(item) {
  if (item.expiresAt) {
    const time = new Date(item.expiresAt).getTime();
    return Number.isFinite(time) ? time : Number.POSITIVE_INFINITY;
  }
  if (item.ttl === -1) return Number.POSITIVE_INFINITY;
  return Date.now() + Number(item.ttl || 0) * 1000;
}

function recencyLabel(value, now = Date.now()) {
  const time = timeValue(value);
  if (!time) return "—";
  const delta = Math.max(0, Math.round((now - time) / 1000));
  if (delta < 60) return `${delta}s`;
  if (delta < 3600) return `${Math.floor(delta / 60)}m`;
  if (delta < 86400) return `${Math.floor(delta / 3600)}h`;
  return `${Math.floor(delta / 86400)}d`;
}

function recencyTitle(item) {
  const rank = state.lruIndex.get(item.key);
  const when = item.lastAccessedAt ? formatClock(item.lastAccessedAt) : "unknown";
  return rank ? `Last access ${when}. LRU position ${rank} of ${state.lru.length}.` : `Last access ${when}.`;
}

function timeValue(value) {
  if (!value) return 0;
  const time = new Date(value).getTime();
  return Number.isFinite(time) ? time : 0;
}

function formatNumber(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "—";
  return numberFormat.format(number);
}

function formatOps(value) {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  const number = Number(value);
  if (Math.abs(number) >= 100) return numberFormat.format(Math.round(number));
  return oneFormat.format(number);
}

function formatAxis(value) {
  if (value >= 10000) return `${oneFormat.format(value / 1000)}k`;
  return formatOps(value);
}

function formatPercent(ratio) {
  if (!Number.isFinite(Number(ratio))) return "—";
  return `${oneFormat.format(Number(ratio) * 100)}%`;
}

function formatBytes(value) {
  const number = Number(value) || 0;
  if (number < 1024) return `${numberFormat.format(number)} B`;
  if (number < 1024 * 1024) return `${oneFormat.format(number / 1024)} KB`;
  if (number < 1024 * 1024 * 1024) return `${oneFormat.format(number / 1024 / 1024)} MB`;
  return `${oneFormat.format(number / 1024 / 1024 / 1024)} GB`;
}

function formatLatency(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "—";
  if (number < 1000) return `${numberFormat.format(Math.round(number))} µs`;
  if (number < 1000000) return `${oneFormat.format(number / 1000)} ms`;
  return `${oneFormat.format(number / 1000000)} s`;
}

function formatTTL(seconds) {
  const whole = Math.max(0, Math.floor(seconds));
  if (whole < 60) return `${whole}s`;
  const minutes = Math.floor(whole / 60);
  const remain = whole % 60;
  if (minutes < 60) return `${minutes}m ${String(remain).padStart(2, "0")}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${String(minutes % 60).padStart(2, "0")}m`;
}

function formatDuration(seconds) {
  if (seconds == null || !Number.isFinite(seconds)) return "—";
  const whole = Math.max(0, Math.floor(seconds));
  const days = Math.floor(whole / 86400);
  const hours = Math.floor((whole % 86400) / 3600);
  const minutes = Math.floor((whole % 3600) / 60);
  const secs = whole % 60;
  const pad = (value) => String(value).padStart(2, "0");
  if (days > 0) return `${days}d ${pad(hours)}:${pad(minutes)}:${pad(secs)}`;
  if (hours > 0) return `${hours}:${pad(minutes)}:${pad(secs)}`;
  return `${minutes}:${pad(secs)}`;
}

function formatClock(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return clockFormat.format(date);
}

function signed(value) {
  const number = Math.round(value);
  return number > 0 ? `+${formatNumber(number)}` : formatNumber(number);
}

function moveFocus(buttons, key) {
  const index = buttons.indexOf(document.activeElement);
  if (index < 0) return false;
  if (key !== "ArrowRight" && key !== "ArrowLeft" && key !== "Home" && key !== "End") return false;
  let next = index;
  if (key === "ArrowRight") next = (index + 1) % buttons.length;
  if (key === "ArrowLeft") next = (index - 1 + buttons.length) % buttons.length;
  if (key === "Home") next = 0;
  if (key === "End") next = buttons.length - 1;
  buttons[next].focus();
  buttons[next].click();
  return true;
}

els.host.textContent = location.host;
els.theme.value = document.documentElement.getAttribute("data-theme") || "system";
updateSortHeaders();
syncCommandFields();
renderKeys();
renderEvents();

els.refresh.addEventListener("click", () => {
  refresh().catch((error) => showBanner(error.message, true));
});
els.theme.addEventListener("change", () => applyTheme(els.theme.value));
els.keyFilter.addEventListener("input", () => {
  state.keySignature = "";
  renderKeys();
});
els.commandName.addEventListener("change", syncCommandFields);
els.commandForm.addEventListener("submit", (event) => {
  executeCommand(event).catch((error) => {
    setOutput(error.message, true);
  });
});

document.querySelectorAll("[data-sort]").forEach((button) => {
  button.addEventListener("click", () => {
    const column = button.dataset.sort;
    if (state.sort === column) state.dir = state.dir === "asc" ? "desc" : "asc";
    else {
      state.sort = column;
      state.dir = column === "key" || column === "type" || column === "ttl" ? "asc" : "desc";
    }
    state.keySignature = "";
    updateSortHeaders();
    renderKeys();
  });
});

els.keysBody.addEventListener("click", (event) => {
  const row = event.target.closest("tr[data-key]");
  if (!row) return;
  selectKey(row.dataset.key);
  row.focus();
});

els.keysBody.addEventListener("keydown", (event) => {
  const row = event.target.closest("tr[data-key]");
  if (!row) return;
  if (event.key === "ArrowDown" || event.key === "ArrowUp") {
    const next = event.key === "ArrowDown" ? row.nextElementSibling : row.previousElementSibling;
    if (!next || !next.dataset.key) return;
    event.preventDefault();
    selectKey(next.dataset.key);
    next.tabIndex = 0;
    next.focus();
    next.scrollIntoView({ block: "nearest" });
  }
});

els.eventFilters.addEventListener("click", (event) => {
  const button = event.target.closest("[data-filter]");
  if (!button) return;
  state.eventFilter = button.dataset.filter;
  for (const item of els.eventFilters.querySelectorAll("[data-filter]")) {
    item.setAttribute("aria-pressed", item === button ? "true" : "false");
  }
  renderEvents();
});

els.eventFilters.addEventListener("keydown", (event) => {
  const buttons = [...els.eventFilters.querySelectorAll("[data-filter]")];
  if (moveFocus(buttons, event.key)) event.preventDefault();
});

function selectTab(name) {
  state.tab = name;
  const selected = name === "log" ? els.tabLog : els.tabAppend;
  const other = name === "log" ? els.tabAppend : els.tabLog;
  selected.setAttribute("aria-selected", "true");
  selected.tabIndex = 0;
  other.setAttribute("aria-selected", "false");
  other.tabIndex = -1;
  renderEvents();
}

els.tabLog.addEventListener("click", () => selectTab("log"));
els.tabAppend.addEventListener("click", () => selectTab("append"));
document.querySelector(".tabs").addEventListener("keydown", (event) => {
  const buttons = [els.tabLog, els.tabAppend];
  if (moveFocus(buttons, event.key)) event.preventDefault();
});

els.chart.addEventListener("mousemove", (event) => {
  const index = chartIndexFromEvent(event);
  if (index == null) return;
  setCursor(index, false);
});
els.chart.addEventListener("mouseleave", () => {
  if (document.activeElement !== els.chart) setCursor(null, false);
});
els.chart.addEventListener("keydown", (event) => {
  if (!state.history.length) return;
  if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
  event.preventDefault();
  let index = state.cursor == null ? state.history.length - 1 : state.cursor;
  if (event.key === "ArrowLeft") index -= 1;
  if (event.key === "ArrowRight") index += 1;
  if (event.key === "Home") index = 0;
  if (event.key === "End") index = state.history.length - 1;
  setCursor(Math.max(0, Math.min(state.history.length - 1, index)), true);
});
els.chart.addEventListener("blur", () => setCursor(null, false));

document.addEventListener("keydown", (event) => {
  const tag = document.activeElement && document.activeElement.tagName;
  const typing = tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT";
  if (event.key === "/" && !typing) {
    event.preventDefault();
    els.keyFilter.focus();
  } else if ((event.key === "r" || event.key === "R") && !typing && !event.metaKey && !event.ctrlKey && !event.altKey) {
    event.preventDefault();
    refresh().catch((error) => showBanner(error.message, true));
  }
});

window.addEventListener("resize", () => renderChart());
if (window.ResizeObserver) {
  new ResizeObserver(() => renderChart()).observe(els.chart.parentElement);
}
window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
  if (!document.documentElement.hasAttribute("data-theme")) renderChart();
});

window.setInterval(tick, 1000);
loadInfo();
connectStream();
refresh().catch((error) => {
  if (!state.ready) {
    setConn("offline");
    showBanner(error.message, true);
    render();
  }
});
