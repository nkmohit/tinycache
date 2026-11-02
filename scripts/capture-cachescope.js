const { chromium } = require("playwright");
const fs = require("fs");

const url = process.env.CACHESCOPE_URL || "http://localhost:8080";
const output = process.env.CACHESCOPE_SCREENSHOT || "docs/assets/cachescope.png";
const chromeCandidates = [
  process.env.CHROME_PATH,
  process.env.CHROMIUM_PATH,
  "/usr/bin/google-chrome",
  "/usr/bin/google-chrome-stable",
  "/usr/bin/chromium",
  "/usr/bin/chromium-browser",
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
].filter(Boolean);
const chromePath = chromeCandidates.find((candidate) => fs.existsSync(candidate));

(async () => {
  const launchOptions = chromePath ? { executablePath: chromePath } : {};
  const browser = await chromium.launch(launchOptions);
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 }, deviceScaleFactor: 1 });
  await page.goto(url, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(1500);
  await page.screenshot({ path: output, fullPage: true });
  await browser.close();
  console.log(`saved ${output}`);
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
