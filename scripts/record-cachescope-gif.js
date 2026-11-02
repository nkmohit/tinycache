#!/usr/bin/env node
"use strict";

// Record CacheScope frames with headless Chrome, then optionally stitch a GIF.
// Set CACHESCOPE_DRIVE=1 to SET a new key before each frame so an 8-key cache
// evicts on camera. Set CACHESCOPE_GIF to write the GIF with ffmpeg.

const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");
const { sleep, launchChrome, connect } = require("./chrome-cdp");

const url = process.env.CACHESCOPE_URL || "http://127.0.0.1:8080";
const port = Number(process.env.CHROME_DEBUG_PORT || "9222");
const outDir = process.env.CACHESCOPE_FRAMES || "/tmp/tinycache-frames";
const frames = Number(process.env.CACHESCOPE_FRAMES_COUNT || "16");
const intervalMs = Number(process.env.CACHESCOPE_FRAME_MS || "400");
const width = Number(process.env.CACHESCOPE_WIDTH || "1280");
const height = Number(process.env.CACHESCOPE_HEIGHT || "800");
const gif = process.env.CACHESCOPE_GIF || "";
const fps = process.env.CACHESCOPE_FPS || "4";

async function drive(index) {
  if (process.env.CACHESCOPE_DRIVE !== "1") return;
  const key = `evict:${String(index).padStart(3, "0")}`;
  const response = await fetch(`${url}/command/set`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key, value: `payload-${index}`, ttlSeconds: 120 }),
  });
  if (!response.ok) {
    throw new Error(`drive SET failed: ${response.status}`);
  }
}

async function main() {
  fs.mkdirSync(outDir, { recursive: true });
  for (const name of fs.readdirSync(outDir)) {
    if (name.endsWith(".png")) fs.unlinkSync(path.join(outDir, name));
  }

  const profile = process.env.CHROME_PROFILE || path.join("/tmp", `cachescope-gif-${port}`);
  const chrome = await launchChrome(port, profile);
  try {
    const client = await connect(chrome.version.webSocketDebuggerUrl);
    const { targetId } = await client.send("Target.createTarget", { url: "about:blank" });
    const { sessionId } = await client.send("Target.attachToTarget", { targetId, flatten: true });
    await client.send("Page.enable", {}, sessionId);
    await client.send("Runtime.enable", {}, sessionId);
    await client.send("Emulation.setDeviceMetricsOverride", {
      width,
      height,
      deviceScaleFactor: 1,
      mobile: false,
    }, sessionId);
    await client.send("Page.navigate", { url }, sessionId);
    for (let attempt = 0; attempt < 40; attempt += 1) {
      await sleep(200);
      const evaluated = await client.send("Runtime.evaluate", {
        expression: "document.body && document.body.dataset.conn",
        returnByValue: true,
      }, sessionId);
      if (evaluated.result && evaluated.result.value === "live") break;
    }

    for (let index = 0; index < frames; index += 1) {
      await drive(index);
      await sleep(intervalMs);
      const shot = await client.send("Page.captureScreenshot", { format: "png", fromSurface: true }, sessionId);
      const file = path.join(outDir, `frame-${String(index).padStart(3, "0")}.png`);
      fs.writeFileSync(file, Buffer.from(shot.data, "base64"));
      process.stdout.write(`saved ${file}\n`);
    }

    await client.send("Target.closeTarget", { targetId });
    client.ws.close();
  } finally {
    chrome.proc.kill();
  }

  if (!gif) return;
  const result = spawnSync("ffmpeg", [
    "-y",
    "-framerate", fps,
    "-i", path.join(outDir, "frame-%03d.png"),
    "-vf", `scale=${width}:-1:flags=lanczos,split[s0][s1];[s0]palettegen=max_colors=48[p];[s1][p]paletteuse`,
    "-loop", "0",
    gif,
  ], { stdio: "inherit" });
  if (result.status !== 0) {
    throw new Error("ffmpeg failed");
  }
  console.log(`saved ${gif}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
