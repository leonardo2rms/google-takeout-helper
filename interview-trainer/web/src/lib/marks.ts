export interface Piece {
  text: string;
  hit: boolean;
}

const escape = (s: string): string => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** Splits `text` into pieces, marking the ones that contain any of the search `words` (case-insensitive). No words means one plain piece. */
export function splitMarks(text: string, words: readonly string[]): Piece[] {
  const useful = words.filter((w) => w.length > 0);
  if (text === '' || useful.length === 0) return [{ text, hit: false }];
  const re = new RegExp(`(${useful.map(escape).join('|')})`, 'gi');
  const pieces: Piece[] = [];
  let last = 0;
  for (let m = re.exec(text); m; m = re.exec(text)) {
    if (m.index > last) pieces.push({ text: text.slice(last, m.index), hit: false });
    pieces.push({ text: m[0], hit: true });
    last = m.index + m[0].length;
  }
  if (last < text.length) pieces.push({ text: text.slice(last), hit: false });
  return pieces;
}
