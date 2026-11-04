/** Cumulative XP ladder. XP only ever goes up, so a rank is never lost. */
export const RANKS: readonly { title: string; xp: number }[] = [
  { title: 'Associate', xp: 0 },
  { title: 'Engineer', xp: 150 },
  { title: 'Senior', xp: 500 },
  { title: 'Senior II', xp: 1000 },
  { title: 'Staff', xp: 2000 },
  { title: 'Senior Staff', xp: 3500 },
  { title: 'Principal', xp: 5500 },
  { title: 'Distinguished', xp: 8500 },
  { title: 'Fellow', xp: 12000 },
];

export interface RankInfo {
  index: number;
  title: string;
  floor: number;
  /** XP needed for the next rank, or null at the top. */
  next: number | null;
  nextTitle: string | null;
  /** 0..1 progress from this rank's floor to the next one (1 at the top). */
  fraction: number;
}

export function rankFor(xp: number): RankInfo {
  const safe = Number.isFinite(xp) ? Math.max(0, xp) : 0;
  let index = 0;
  for (let i = 0; i < RANKS.length; i++) if (safe >= RANKS[i]!.xp) index = i;
  const cur = RANKS[index]!;
  const nxt = RANKS[index + 1];
  return {
    index,
    title: cur.title,
    floor: cur.xp,
    next: nxt ? nxt.xp : null,
    nextTitle: nxt ? nxt.title : null,
    fraction: nxt ? (safe - cur.xp) / (nxt.xp - cur.xp) : 1,
  };
}
