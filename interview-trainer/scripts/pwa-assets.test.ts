/** The installable-app files are consistent (QA S2-23): every icon the manifest and index.html name exists and has the size it claims. */
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const web = resolve(dirname(fileURLToPath(import.meta.url)), '../web');
const manifest = JSON.parse(readFileSync(join(web, 'public/manifest.webmanifest'), 'utf8')) as {
  start_url: string;
  icons: { src: string; sizes: string; type: string; purpose: string }[];
};
const html = readFileSync(join(web, 'index.html'), 'utf8');

/** Width and height from a PNG's IHDR chunk. */
function pngSize(file: string): [number, number] {
  const b = readFileSync(file);
  expect(b.subarray(0, 8).toString('hex'), `${file} is not a PNG`).toBe('89504e470d0a1a0a');
  return [b.readUInt32BE(16), b.readUInt32BE(20)];
}

describe('manifest icons', () => {
  it('has a 192 and a 512 PNG for "any", and a separate maskable PNG (one image must not serve both purposes)', () => {
    const png = manifest.icons.filter((i) => i.type === 'image/png');
    expect(png.some((i) => i.sizes === '192x192' && i.purpose === 'any')).toBe(true);
    expect(png.some((i) => i.sizes === '512x512' && i.purpose === 'any')).toBe(true);
    expect(png.some((i) => i.purpose === 'maskable')).toBe(true);
    expect(manifest.icons.some((i) => i.purpose.includes('any') && i.purpose.includes('maskable'))).toBe(false);
  });

  it.each(manifest.icons.filter((i) => i.type === 'image/png'))('$src is a PNG of the size it declares', (icon) => {
    const [w, h] = icon.sizes.split('x').map(Number);
    expect(pngSize(join(web, 'public', icon.src))).toEqual([w, h]);
  });
});

describe('index.html', () => {
  it('points the iOS touch icon at a PNG (iOS ignores SVG)', () => {
    const href = /rel="apple-touch-icon" href="([^"]+)"/.exec(html)?.[1];
    expect(href).toMatch(/\.png$/);
    expect(pngSize(join(web, 'public', href!))).toEqual([180, 180]);
  });
});
