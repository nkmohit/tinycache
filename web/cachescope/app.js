const state = {
  metrics: null,
  keys: [],
  lru: [],
  events: [],
  replay: [],
};

const els = {
  connection: document.querySelector("#connectionStatus"),
  refresh: document.querySelector("#refreshButton"),
  keyCount: document.querySelector("#keyCount"),
  hitRatio: document.querySelector("#hitRatio"),
  evictions: document.querySelector("#evictions"),
  memoryBytes: document.querySelector("#memoryBytes"),
  p95Latency: document.querySelector("#p95Latency"),
  keyFilter: document.querySelector("#keyFilter"),
  keysTable: document.querySelector("#keysTable"),
  lruList: document.querySelector("#lruList"),
  eventsList: document.querySelector("#eventsList"),
  replayList: document.querySelector("#replayList"),
  commandForm: document.querySelector("#commandForm"),
  commandName: document.querySelector("#commandName"),
  commandKey: document.querySelector("#commandKey"),
  commandValue: document.querySelector("#commandValue"),
  commandTTL: document.querySelector("#commandTTL"),
  valueField: document.querySelector("#valueField"),
  ttlField: document.querySelector("#ttlField"),
  commandOutput: document.querySelector("#commandOutput"),
};

function setConnection(label, mode) {
  els.connection.textContent = label;
  els.connection.className = `connection ${mode || ""}`.trim();
}

async function fetchJSON(path, options) {
  const response = await fetch(path, options);
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || `Request failed with ${response.status}`);
  }
  return data;
}

async function refreshSnapshot() {
  const [summary, replay] = await Promise.all([
    fetchJSON("/debug/summary?sort=access&desc=true"),
    fetchJSON("/debug/replay"),
  ]);

  state.metrics = summary.metrics;
  state.keys = summary.keys || [];
  state.lru = summary.lru || [];
  state.events = summary.events || [];
  state.replay = replay.records || [];
  render();
}

function connectStream() {
  if (!window.EventSource) {
    refreshSnapshot().catch(() => setConnection("Offline", "error"));
    return;
  }

  const stream = new EventSource("/debug/events/stream");
  stream.addEventListener("open", () => setConnection("Live", "live"));
  stream.addEventListener("error", () => setConnection("Reconnecting", "error"));
  stream.addEventListener("snapshot", (event) => {
    const data = JSON.parse(event.data);
    state.metrics = data.metrics;
    state.keys = data.keys || [];
    state.lru = data.lru || [];
    state.events = data.events || [];
    render();
    setConnection("Live", "live");
  });
}

function render() {
  const metrics = state.metrics || {};
  els.keyCount.textContent = formatNumber(metrics.keyCount || 0);
  els.hitRatio.textContent = `${Math.round((metrics.hitRatio || 0) * 100)}%`;
  els.evictions.textContent = formatNumber(metrics.evictions || 0);
  els.memoryBytes.textContent = formatBytes(metrics.memoryBytes || 0);
  els.p95Latency.textContent = `${formatNumber(maxP95Latency(metrics.latencyByCommand || {}))} us`;

  renderKeys();
  renderLRU();
  renderEvents();
  renderReplay();
}

function renderKeys() {
  const filter = els.keyFilter.value.trim().toLowerCase();
  const rows = state.keys
    .filter((item) => item.key.toLowerCase().includes(filter))
    .sort((a, b) => a.key.localeCompare(b.key))
    .map((item) => {
      return `<tr>
        <td>${escapeHTML(item.key)}</td>
        <td>${formatTTL(item.ttl)}</td>
        <td class="${item.accessCount > 3 ? "hot" : ""}">${formatNumber(item.accessCount || 0)}</td>
        <td>${formatBytes(item.sizeBytes || 0)}</td>
        <td>${formatTime(item.lastAccessedAt)}</td>
      </tr>`;
    });

  els.keysTable.innerHTML = rows.join("") || `<tr><td colspan="5">No keys</td></tr>`;
}

function renderLRU() {
  els.lruList.innerHTML = state.lru
    .map((key) => `<li>${escapeHTML(key)}</li>`)
    .join("") || "<li>No keys</li>";
}

function renderEvents() {
  const events = [...state.events].reverse().slice(0, 40);
  els.eventsList.innerHTML = events
    .map((event) => {
      const meta = [event.key, event.reason, event.hit === true ? "hit" : event.hit === false ? "miss" : ""]
        .filter(Boolean)
        .join(" · ");
      return `<div class="event">
        <span class="event-type">${escapeHTML(event.type || "")}</span>
        <span>${escapeHTML(meta || "cache")}</span>
        <span class="event-meta">${formatTime(event.at)}</span>
      </div>`;
    })
    .join("") || `<div class="event-meta">No events yet</div>`;
}

function renderReplay() {
  const records = [...state.replay].reverse().slice(0, 30);
  els.replayList.innerHTML = records
    .map((record) => {
      const meta = [record.key, record.ttlSeconds ? `${record.ttlSeconds}s` : "", record.value ? "value" : ""]
        .filter(Boolean)
        .join(" · ");
      return `<div class="event">
        <span class="event-type">${escapeHTML(record.command || "")}</span>
        <span>${escapeHTML(meta || "mutation")}</span>
        <span class="event-meta">${formatTime(record.at)}</span>
      </div>`;
    })
    .join("") || `<div class="event-meta">Enable --aof-path to record replay data</div>`;
}

async function executeCommand(event) {
  event.preventDefault();
  const command = els.commandName.value;
  const key = els.commandKey.value.trim();
  const value = els.commandValue.value;
  const ttlSeconds = Number(els.commandTTL.value || 0);

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
  } else if (command === "ttl") {
    result = await fetchJSON(`/command/ttl?key=${encodeURIComponent(key)}`);
  }

  els.commandOutput.textContent = JSON.stringify(result, null, 2);
  await refreshSnapshot();
}

function syncCommandFields() {
  const command = els.commandName.value;
  els.valueField.style.display = command === "set" ? "grid" : "none";
  els.ttlField.style.display = command === "set" || command === "expire" ? "grid" : "none";
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(value);
}

function formatBytes(value) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 / 1024).toFixed(1)} MB`;
}

function formatTTL(ttl) {
  if (ttl === -1) return "No expiry";
  if (ttl === -2) return "Missing";
  return `${ttl}s`;
}

function maxP95Latency(latencies) {
  return Object.values(latencies).reduce((max, item) => Math.max(max, item.p95Us || 0), 0);
}

function formatTime(value) {
  if (!value) return "";
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function escapeHTML(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

els.refresh.addEventListener("click", () => {
  refreshSnapshot().catch((error) => {
    els.commandOutput.textContent = error.message;
  });
});
els.keyFilter.addEventListener("input", renderKeys);
els.commandForm.addEventListener("submit", (event) => {
  executeCommand(event).catch((error) => {
    els.commandOutput.textContent = error.message;
  });
});
els.commandName.addEventListener("change", syncCommandFields);

syncCommandFields();
connectStream();
refreshSnapshot().catch(() => setConnection("Offline", "error"));
