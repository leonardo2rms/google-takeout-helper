import type { CSSProperties } from 'react';
import { isIconName, type IconName } from '../components/icons';

/** Icon used when a unit has no `icon` in its YAML or names one the app does not ship. */
export const FALLBACK_UNIT_ICON: IconName = 'braces';

/** Units with a dedicated pen colour in the stylesheet (`--pen-<id>`). Anything else falls back to cobalt. */
const PENS = new Set([
  'kotlin', 'coroutines', 'spring', 'jvm', 'ddd', 'scale', 'complexity', 'ai', 'data', 'messaging', 'system-design', 'testing', 'security-observability',
]);

/** Wayfinding style for a unit: its icon and the CSS custom property (`--pen`) used for the glyph outline and the spine. */
export function unitStyle(unit: { id: string; icon?: string }): { icon: IconName; style: CSSProperties } {
  const icon = isIconName(unit.icon) ? unit.icon : FALLBACK_UNIT_ICON;
  const pen = PENS.has(unit.id) ? `var(--pen-${unit.id})` : 'var(--cobalt)';
  return { icon, style: { ['--pen' as string]: pen } };
}
