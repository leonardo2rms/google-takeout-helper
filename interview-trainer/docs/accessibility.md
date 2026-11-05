# Accessibility

The goal is WCAG 2.2 AA behaviour for a phone-first, keyboard-operable, screen-reader-usable app, checked by tests rather than by promises.
Visual design (palette, typography, icons) is deliberately out of scope here: a redesign is coming, so **colour contrast is a tracked exception** (below).

## How it is checked

| Layer | What | Where |
|---|---|---|
| axe-core over every screen and state | Home (new learner with the onboarding card, active learner, dark mode), streak / goal / settings sheets, Progress, share sheet, not-found and all-clear screens, **every question type** (unanswered, answered, feedback right, feedback wrong), skipped question, Ask why sheet, session summary. Fails on any `serious` or `critical` violation. Tags: `wcag2a`, `wcag2aa`, `wcag21a`, `wcag21aa`, `wcag22aa`, `best-practice`. | `e2e/a11y.spec.ts` (`@axe-core/playwright`) |
| Behaviour that axe cannot see | skip link, one `main` landmark, route titles and focus, focus movement in a lesson, keyboard-only play of match / order / spot, results readable without colour, a visible focus ring on **every Tab stop** of Home, Settings and each question type, Reading settings (S/M/L/XL, easier-to-read font, no horizontal overflow at 390px), reduced motion | `e2e/a11y-behaviour.spec.ts` |
| Pure logic | none needed: the accessibility code is DOM wiring | |

Run: `E2E_PORT=4182 npx playwright test e2e/a11y.spec.ts e2e/a11y-behaviour.spec.ts`.

### Tracked exception: colour contrast

`KNOWN_EXCEPTIONS = ['color-contrast']` in `e2e/a11y.spec.ts` is the **only** rule excluded, in one constant, with a `TODO(redesign)`.
The current palette (bright green buttons with white text, muted grey captions, tinted feedback cards) fails AA contrast in places and fixing it on a palette that is about to be replaced would be thrown away.
When the redesign lands: delete the constant, run the spec, fix what it finds. Everything else in the checklist below is already enforced.

## What the app does

**Structure.** One `<main id="main">` for every screen (the lesson no longer nests its own), a *Skip to main content* link as the first Tab stop (it moves focus instead of changing the hash, because the hash is the router), `lang="en"`,
a document title per screen, labelled regions (`section aria-labelledby`) for feedback, `role="dialog" aria-modal` sheets with a focus trap, Escape to close and focus returned to the opener.

**Focus follows the flow.** Hash routing does not move focus, so after the first render every route change focuses the new screen's `<h1>` (`useRouteAnnouncer`). In a lesson: a new question focuses its prompt; **Check** moves focus to the verdict heading
(Tab then walks the explanation to *Continue*; Enter on the heading also continues); the Ask why sheet returns focus to its button. The verdict is a labelled region and focus is the announcement, so there is no live region that would read it twice.
`scroll-padding` keeps the focused control clear of the sticky header and the fixed lesson bar (WCAG 2.4.11).

**Live regions** (`role="status"`, polite): "Checking your answer" / "Running your code" while a check or test run is in flight, test results, pairing progress in *match* ("Selected: X. Now choose its match", "2 of 4 paired"), placement progress in *order* ("3 of 5 placed"), and the Ask why conversation log.
There are no countdown timers in the practice flow; the code runner has a time budget but reports it as a test result, which is already announced.

**Operable by keyboard.** Every interaction is a real `<button>`/input: options are `radio`/`checkbox` roles, *spot* lines, *match* items, *order* lines and *fill* tokens are buttons (Space/Enter), placed lines say "Position n" and "tap to remove", filled blanks say what they hold.
After checking, each item states its result in text for screen readers ("your answer, correct", "a correct answer you did not select"), and the choice markers use check/cross glyphs, so colour is never the only signal (WCAG 1.4.1).

**No keyboard traps.** The code editor indents on Tab, so a keyboard user could never leave it. Escape releases the key (Escape, then Tab moves on), Shift+Tab always leaves, and the textarea is described by a hint that says so (`aria-describedby`). The behaviour test covers it, and the Tab-order scan uses the same Escape.

**Focus rings.** One global `:focus-visible` outline (3px, blue, offset 2px). No control removes it: text areas used to swap it for a border colour and now keep the ring. The Tab-order test fails if any stop has none.

**Reduced motion.** `@media (prefers-reduced-motion: reduce)` disables every CSS animation and transition (including the progress ring and the sheet slide-in); JS-driven motion is gated too: smooth scrolling uses `behavior: 'auto'` (`scrollIntoViewGently`), and confetti is never mounted.
The test asserts `document.getAnimations().length === 0` on Home, the settings sheet and a feedback card, that every `scrollIntoView` is instant, and (with the preference off) that the same screens *do* animate, so the check is not vacuous.

**Reading settings** (Settings -> Reading, stored in `progress.prefs.reading`, local to the device):

- *Text size* S / M / L / XL scales every font size by 0.9 / 1 / 1.2 / 1.4. Implementation: every `font-size: Npx` in `styles.css` is `calc(Npx * var(--text-scale, 1))` and `<html data-text-size>` sets `--text-scale`, so text grows while layout (tap targets, paddings) stays put and phone layouts keep fitting (tested at 390px, XL, on Home, Progress, Settings and each question type).
  New CSS should write font sizes the same way.
- *Easier-to-read font* sets `<html data-font="dyslexia">`: a system stack (`OpenDyslexic`, `Atkinson Hyperlegible` if installed, then Verdana, Trebuchet MS, Tahoma, Segoe UI, system-ui), wider letter and word spacing and taller lines. No web font is downloaded (the app is offline-first), and code keeps its monospace font and normal spacing.
  This is a convenience, not a medical claim: evidence for dyslexia-specific fonts is mixed; spacing and size matter more, which is why both are adjustable.

## Rules for new UI

1. A real element for every interaction (`button`, `a`, `input`), never a clickable `div`. Name it by its visible text, or include that text in `aria-label` (axe `label-content-name-mismatch`).
2. Never remove an outline; style `:focus-visible` instead.
3. Result states need a text or glyph cue besides colour.
4. Anything that appears after an action either takes focus (new content the learner must read, a sheet) or is announced by a persistent polite live region; not both.
5. Font sizes via `calc(Npx * var(--text-scale, 1))`; tap targets at least 44px; no horizontal overflow at 390px at the XL size.
6. Motion: put it in CSS (the global reduced-motion rule covers it) or check `prefersReducedMotion()`.
7. Add the new screen/state to `e2e/a11y.spec.ts`.
