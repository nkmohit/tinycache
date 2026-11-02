#!/usr/bin/env node
// Capture CacheScope frames over the Chrome DevTools protocol.
// No extra packages: Node 22's global WebSocket talks to headless Chrome.

const fs = require("fs");
const path = require("path");

const url = process.env.CACHESCOPE_URL || "http://127.0.0.1:8080";
const debugPort = process.env.CHROME_DEBUG_PORT || "9222";
const outDir = process.env.CACHESCOPE_FRAMES || "/tmp/tinycache-frames";
const frames = Number(process.env.CACHESCOPE_FRAMES_COUNT || "16");
const intervalMs = Number(process.env.CACHESCOPE_FRAME_MS || "400");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function browserSocket() {
  const version = await fetch(`http://127.0.0.1:${debugPort}/json/version`).then((r) => r.json());
  return version.webSocketDebuggerUrl;
}

async function main() {
  fs.mkdirSync(outDir, { recursive: true });
  for (const name of fs.readdirSync(outDir)) {
    if (name.endsWith(".png")) {
      fs.unlinkSync(path.join(outDir, name));
    }
  }

  const ws = new WebSocket(await browserSocket());
  await new Promise((resolve, reject) => {
    ws.addEventListener("open", resolve);
    ws.addEventListener("error", () => reject(new Error("chrome devtools socket failed")));
  });

  let nextId = 0;
  const pending = new Map();
  ws.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    if (message.id && pending.has(message.id)) {
      const { resolve, reject } = pending.get(message.id);
      pending.delete(message.id);
      if (message.error) {
        reject(new Error(message.error.message || "cdp error"));
        return;
      }
      resolve(message.result);
    }
  });

  function send(method, params, sessionId) {
    const id = ++nextId;
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      const payload = { id, method, params };
      if (sessionId) {
        payload.sessionId = sessionId;
      }
      ws.send(JSON.stringify(payload));
    });
  }

  const { targetId } = await send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });
  await send("Page.enable", {}, sessionId);
  await send("Runtime.enable", {}, sessionId);
  await send(
    "Emulation.setDeviceMetricsOverride",
    { width: 1280, height: 800, deviceScaleFactor: 1, mobile: false },
    sessionId,
  );
  await send("Page.navigate", { url }, sessionId);
  await sleep(1200);

  for (let i = 0; i < frames; i++) {
    const shot = await send(
      "Page.captureScreenshot",
      { format: "png", fromSurface: true },
      sessionId,
    );
    const file = path.join(outDir, `frame-${String(i).padStart(3, "0")}.png`);
    fs.writeFileSync(file, Buffer.from(shot.data, "base64"));
    process.stdout.write(`saved ${file}\n`);
    await sleep(intervalMs);
  }

  await send("Target.closeTarget", { targetId });
  ws.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
