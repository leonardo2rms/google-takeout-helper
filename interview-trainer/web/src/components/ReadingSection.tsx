import { TEXT_SIZES, type Progress, type TextSize } from '@trainer/shared';

interface Props {
  progress: Progress;
  update: (fn: (p: Progress) => Progress) => void;
}

const SIZE_NAMES: Record<TextSize, string> = { S: 'Small', M: 'Medium', L: 'Large', XL: 'Extra large' };

/** Settings -> Reading: text size and an optional dyslexia-friendly font stack. Applied live through data attributes (see useReadingPrefs). */
export function ReadingSection({ progress, update }: Props) {
  const { textSize, dyslexiaFont } = progress.prefs.reading;
  const set = (patch: Partial<Progress['prefs']['reading']>) =>
    update((p) => ({ ...p, prefs: { ...p.prefs, reading: { ...p.prefs.reading, ...patch } } }));

  return (
    <section aria-labelledby="reading-h">
      <h3 id="reading-h">Reading</h3>
      <div className="choice-grid four reading-sizes" role="radiogroup" aria-label="Text size">
        {TEXT_SIZES.map((size) => (
          <label key={size} className={`choice ${textSize === size ? 'on' : ''}`}>
            <input type="radio" name="text-size" checked={textSize === size} onChange={() => set({ textSize: size })} aria-label={SIZE_NAMES[size]} data-testid={`text-size-${size}`} />
            <span><b>{size}</b></span>
          </label>
        ))}
      </div>
      <label className="toggle-row">
        <input type="checkbox" checked={dyslexiaFont} onChange={(e) => set({ dyslexiaFont: e.target.checked })} data-testid="dyslexia-font" />
        <span>
          <b>Easier-to-read font</b>
          <small>Uses a plain, wide-lettered font from your device with more space between letters and lines. Code stays monospaced.</small>
        </span>
      </label>
    </section>
  );
}
