import { TEXT_SIZES, type Progress, type TextSize } from '@trainer/shared';

interface Props {
  progress: Progress;
  update: (fn: (p: Progress) => Progress) => void;
}

/** The scale each size applies (kept in step with the data-text-size rules in styles.css), so each button previews its own size. */
const PREVIEW_SCALE: Record<TextSize, number> = { S: 0.9, M: 1, L: 1.2, XL: 1.4 };

const SIZE_NAMES: Record<TextSize, string> = { S: 'Small', M: 'Medium', L: 'Large', XL: 'Extra large' };

/** The closed group's one-line state: "Medium text", "Large text, easier font". */
export const readingSummary = (r: Progress['prefs']['reading']): string => `${SIZE_NAMES[r.textSize]} text${r.dyslexiaFont ? ', easier font' : ''}`;

/** Settings -> Reading: text size and an optional dyslexia-friendly font stack. Applied live through data attributes (see useReadingPrefs). */
export function ReadingSection({ progress, update }: Props) {
  const { textSize, dyslexiaFont } = progress.prefs.reading;
  const set = (patch: Partial<Progress['prefs']['reading']>) =>
    update((p) => ({ ...p, prefs: { ...p.prefs, reading: { ...p.prefs.reading, ...patch } } }));

  return (
    <>
      <div className="choice-grid four reading-sizes" role="radiogroup" aria-label="Text size">
        {TEXT_SIZES.map((size) => (
          <label key={size} className={`choice ${textSize === size ? 'on' : ''}`}>
            <input type="radio" name="text-size" checked={textSize === size} onChange={() => set({ textSize: size })} aria-label={SIZE_NAMES[size]} data-testid={`text-size-${size}`} />
            <span><b aria-hidden="true" style={{ fontSize: `${(18 * PREVIEW_SCALE[size]).toFixed(1)}px` }}>Aa</b></span>
          </label>
        ))}
      </div>
      <p className="muted reading-sample">This is how a sentence reads at the size you chose.</p>
      <label className="toggle-row">
        <input type="checkbox" checked={dyslexiaFont} onChange={(e) => set({ dyslexiaFont: e.target.checked })} data-testid="dyslexia-font" />
        <span>
          <b>Easier-to-read font</b>
          <small>A plain, wide-lettered font with more spacing. Code stays monospaced.</small>
        </span>
      </label>
    </>
  );
}
