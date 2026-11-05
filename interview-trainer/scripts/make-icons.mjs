// Renders web/public/icon.svg to the PNG icons installable apps need (Chrome prefers 192/512 PNGs, iOS ignores SVG touch icons).
// Run it after changing the SVG:  node scripts/make-icons.mjs   (uses the Chromium that Playwright already installed)
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from '@playwright/test';

const pub = join(resolve(dirname(fileURLToPath(import.meta.url)), '..'), 'web/public');
const svg = readFileSync(join(pub, 'icon.svg'), 'utf8');
// Full-bleed variant for maskable and touch icons: the platform applies its own corner mask, so the artwork must reach the edges.
const fullBleed = svg.replace(' rx="24"', '');

const targets = [
  { file: 'icon-192.png', size: 192, art: svg, transparent: true },
  { file: 'icon-512.png', size: 512, art: svg, transparent: true },
  { file: 'icon-maskable-512.png', size: 512, art: fullBleed, transparent: false },
  { file: 'apple-touch-icon.png', size: 180, art: fullBleed, transparent: false },
];

const browser = await chromium.launch();
try {
  for (const t of targets) {
    const page = await browser.newPage({ viewport: { width: t.size, height: t.size } });
    await page.setContent(`<style>html,body{margin:0;background:transparent}svg{display:block;width:${t.size}px;height:${t.size}px}</style>${t.art}`);
    writeFileSync(join(pub, t.file), await page.screenshot({ omitBackground: t.transparent, clip: { x: 0, y: 0, width: t.size, height: t.size } }));
    await page.close();
    console.log(`wrote web/public/${t.file}`);
  }
} finally {
  await browser.close();
}
