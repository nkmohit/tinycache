const { chromium } = require("playwright");
const fs = require("fs");

const url = process.env.CACHESCOPE_URL || "http://localhost:8080";
const output = process.env.CACHESCOPE_SCREENSHOT || "docs/assets/cachescope.png";
const chromePath = process.env.CHROME_PATH || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";

(async () => {
  const launchOptions = fs.existsSync(chromePath) ? { executablePath: chromePath } : {};
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
