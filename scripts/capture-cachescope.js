"use strict";

// Screenshot CacheScope with headless Chrome. Playwright is used only when
// Chrome itself cannot be launched.

const fs = require("fs");
const path = require("path");
const { sleep, launchChrome, connect } = require("./chrome-cdp");

const url = process.env.CACHESCOPE_URL || "http://127.0.0.1:8080";
const output = process.env.CACHESCOPE_SCREENSHOT || "docs/assets/cachescope.png";
const width = Number(process.env.CACHESCOPE_WIDTH || "1440");
const height = Number(process.env.CACHESCOPE_HEIGHT || "900");
const scheme = process.env.CACHESCOPE_COLOR_SCHEME || "";
const minSamples = Number(process.env.CACHESCOPE_MIN_SAMPLES || "0");

async function captureWithChrome() {
  const port = Number(process.env.CHROME_DEBUG_PORT || "9222");
  const profile = process.env.CHROME_PROFILE || path.join("/tmp", `cachescope-chrome-${port}`);
  const chrome = await launchChrome(port, profile);
  const errors = [];
  try {
    const client = await connect(chrome.version.webSocketDebuggerUrl, (message) => {
      if (message.method === "Runtime.exceptionThrown") {
        errors.push(message.params.exceptionDetails.text || "exception");
      }
      if (message.method === "Runtime.consoleAPICalled" && message.params.type === "error") {
        errors.push("console.error");
      }
    });
    const { targetId } = await client.send("Target.createTarget", { url: "about:blank" });
    const { sessionId } = await client.send("Target.attachToTarget", { targetId, flatten: true });
    await client.send("Page.enable", {}, sessionId);
    await client.send("Runtime.enable", {}, sessionId);
    await client.send("Page.addScriptToEvaluateOnNewDocument", {
      source: "window.__cachescopeErrors=[];window.addEventListener('error',function(e){window.__cachescopeErrors.push(String(e.message));});window.addEventListener('unhandledrejection',function(e){window.__cachescopeErrors.push(String(e.reason));});",
    }, sessionId);
    await client.send("Emulation.setDeviceMetricsOverride", {
      width,
      height,
      deviceScaleFactor: 1,
      mobile: false,
    }, sessionId);
    const features = [];
    if (scheme === "light" || scheme === "dark") {
      features.push({ name: "prefers-color-scheme", value: scheme });
    }
    if (features.length) {
      await client.send("Emulation.setEmulatedMedia", { features }, sessionId);
    }
    await client.send("Page.navigate", { url }, sessionId);
    let samples = 0;
    let conn = "";
    for (let attempt = 0; attempt < 40; attempt += 1) {
      await sleep(250);
      const evaluated = await client.send("Runtime.evaluate", {
        expression: "({conn: document.body && document.body.dataset.conn, samples: Number((document.querySelector('#chart') || {}).dataset.samples || 0), errors: window.__cachescopeErrors || []})",
        returnByValue: true,
      }, sessionId);
      const value = evaluated.result && evaluated.result.value;
      if (!value) continue;
      conn = value.conn || "";
      samples = value.samples || 0;
      if (conn === "live" && samples >= minSamples) break;
    }
    if (conn !== "live") {
      throw new Error(`CacheScope did not become live (conn=${conn || "missing"})`);
    }
    const reported = await client.send("Runtime.evaluate", {
      expression: "window.__cachescopeErrors || []",
      returnByValue: true,
    }, sessionId);
    const pageErrors = (reported.result && reported.result.value) || [];
    if (pageErrors.length) {
      throw new Error(`page errors: ${pageErrors.join("; ")}`);
    }
    fs.mkdirSync(path.dirname(output), { recursive: true });
    const shot = await client.send("Page.captureScreenshot", { format: "png", fromSurface: true }, sessionId);
    fs.writeFileSync(output, Buffer.from(shot.data, "base64"));
    await client.send("Target.closeTarget", { targetId });
    client.ws.close();
    if (errors.length) {
      throw new Error(`browser errors: ${errors.join("; ")}`);
    }
    console.log(`saved ${output}`);
  } finally {
    chrome.proc.kill();
  }
}

async function captureWithPlaywright() {
  const { chromium } = require("playwright");
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width, height }, deviceScaleFactor: 1, colorScheme: scheme || undefined });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  await page.goto(url, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.body && document.body.dataset.conn === "live", null, { timeout: 8000 });
  fs.mkdirSync(path.dirname(output), { recursive: true });
  await page.screenshot({ path: output });
  await browser.close();
  if (errors.length) throw new Error(`browser errors: ${errors.join("; ")}`);
  console.log(`saved ${output}`);
}

captureWithChrome().catch((chromeError) => {
  captureWithPlaywright().catch((playwrightError) => {
    console.error(chromeError.message);
    console.error(playwrightError.message);
    process.exit(1);
  });
});
