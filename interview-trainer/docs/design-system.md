# Design system: "Drafting sheet" and "Blueprint"

The look of the app is an engineer's drafting sheet: ink on cool paper, hairline rules, mono for labels and numbers, and verdicts that read like code-review marks
(a rail, a squiggle, a word) instead of game feedback. Dark mode is the same drawing as a blueprint: deep navy ground, light ink.

Everything lives in two files, `web/src/styles.css` (tokens, shell, question types, Home, Summary, sheets) and `web/src/features-a.css` (hints, Library, challenge, badges, mock interview).
There is no CSS framework and no runtime theme code beyond the guarded dark-mode pattern below.

## Rules

1. **Colours come from tokens.** Never write a hex value in a component rule. `scripts/contrast.mjs` checks the tokens, so a new colour must become a token first.
2. **A verdict is never colour alone.** Correct, incorrect, partial and "this was the answer" each carry an icon, a word and a border or underline style (table below).
3. **Flat.** 1px `--edge` outlines, 6px radii, no shadows, no gradients, no pill shapes. Pressed means a background shift, not a movement.
4. **Hue has a job.** Cobalt means selected or interactive, teal correct, vermilion incorrect, ochre partial or a hint. Ink is the primary action. Nothing else is saturated.
5. **Font sizes are tokens** (`--fs-11` ... `--fs-72`, each `calc(Npx * var(--text-scale, 1))`) so the Reading text size (Settings) keeps scaling text but not layout.
6. **Tap targets are at least 44px**; rows and options 48 to 56px.
7. **Motion explains a state change and nothing else.** 80 to 160ms, ease-out, no bounce. `prefers-reduced-motion` switches all of it off in one global rule.

## Tokens

Light is the `:root` block, dark is `:root[data-theme="dark"]` and the identical values under `@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) }` (the repo's existing guarded pattern; the contrast script fails if the two dark blocks differ).

| Token | Role | Light | Dark |
|---|---|---|---|
| `--paper` | page ground | `#F2F4F6` | `#0D1721` |
| `--ink` | text, primary button, progress fills | `#0F1B26` | `#E7EDF2` |
| `--graphite` | secondary text, labels, icons at rest | `#4A5967` | `#9DADBB` |
| `--edge` | borders of every control (3:1 or better) | `#7B8996` | `#62758A` |
| `--cobalt` | selected, link, focus ring, info, "up next" | `#2447C4` | `#86A6FF` |
| `--teal` | correct | `#00695F` | `#46D2B8` |
| `--vermilion` | incorrect | `#AE320C` | `#FF8F6B` |
| `--ochre` | partial, hint, flagged | `#8A5300` | `#EBB650` |
| `--sheet` | raised surfaces: options, inputs, cards, panels, bottom bar | `#FBFCFD` | `#142230` |
| `--hairline` | decorative dividers, gauge tracks, pressed state | `#D3D9DF` | `#243546` |
| `--code` | code ground (dark: darker than paper, an inset well) | `#E9EDF1` | `#09121A` |
| `--cobalt-tint` `--teal-tint` `--vermilion-tint` `--ochre-tint` | 12% (light) / 16% (dark) of the hue over paper | `#D9DFF0` `#D5E3E4` `#EADDDA` `#E6E1D8` | `#202E45` `#163539` `#342A2D` `#313029` |
| `--tok-*` | syntax: keyword, type, string, number, annotation, comment | `#5A3BC2` `#0B5F8A` `#0A6A5A` `#9A4A00` `#9A2F7D` `#566474` | `#B49CFF` `#6FC3F0` `#5FD6B8` `#F0A860` `#F08CCB` `#8B9CAB` |
| `--pen-*` | one muted colour per unit, used only for the glyph outline and the spine (same value in both themes, at least 3:1) | see `styles.css` | same |

Body text on a verdict panel is always `--ink`; the hue is used for the icon, the word, the rail and the border. Solid markers put `--paper` on the hue (right polarity in both themes).

### Contrast

```bash
node scripts/contrast.mjs        # prints OK / the failing pairs; exit code 1 on any failure
npm test                         # runs the same check (scripts/contrast.test.ts)
```

The script parses the tokens from `styles.css` and checks 228 pairings (114 per theme): text 4.5:1 (ink and graphite on every surface and tint, hue as text on paper, sheet and its own tint,
`paper` on every solid marker, syntax colours on code and the tints) and UI 3:1 (`--edge` on every surface, hue as border or icon, pens, focus ring, progress fill on its track).
Add a pair to `pairs()` when you add a new combination. `e2e/a11y.spec.ts` then enforces axe's `color-contrast` on the rendered screens, light and dark, with no exception.

### Colour-blind safety

| State | Hue | Icon | Word | Border, rail, mark |
|---|---|---|---|---|
| Correct | teal | `check` | Correct | solid 2px look, solid 4px rail on panels |
| Incorrect | vermilion | `x` | Incorrect | solid 2px look, **line-through** on option text, **wavy underline** on code, solid rail |
| Partial | ochre | `contrast` (half circle) | Partial 2/3 points | **dashed** rail |
| The answer you missed | teal | `check` in a dashed circle | Answer | **dashed** teal border |
| Selected, before checking | cobalt | `check` in a filled marker | none | solid 2px look |
| Info, interviewer, up next | cobalt | `message-square-quote`, `info` | label | 4px rail on the cobalt tint |

Vermilion and ochre are close for red-green colour blindness, so they never appear as the only difference: partial always has its own icon, word and dashed rail.

## Type

| Role | Face | Delivery |
|---|---|---|
| Prose, prompts, headings, buttons | IBM Plex Sans, variable `wght` 100 to 700 | self-hosted: a Latin subset (about 23 kB woff2) of `@fontsource-variable/ibm-plex-sans`, embedded as base64 in the single-file build |
| Code, labels, numbers, tags | system mono stack | no bytes |

SIL Open Font License 1.1, the licence text is `web/src/fonts/OFL.txt`. No font is fetched from a network at run time (check: `grep -r "fonts.googleapis" web` is empty and the built CSS only references `data:` or same-origin files).
To change the character set edit `scripts/subset-font.sh` and run it (needs `pip install fonttools brotli`); the output is committed so a normal build needs no Python.

Scale (px at the default size, multiplied by `--text-scale`): 11 to 12 mono labels, 13 to 14 secondary text, 16 body, 18 headings, 21 question prompt, 26 estimate input, 30 summary heading, 72 the big readout numbers.
Weights 400, 500, 600 only. Numerals in mono use `tabular-nums slashed-zero`. No italics, no all-caps buttons, no letter-spaced body text (the Easier-to-read font setting still swaps the family and spacing).

## Shape and space

Space is a 4px scale (`--s1` to `--s7`), the page gutter is 16px and `#root` is at most 640px wide. Radii: `--r1` 3px (inline code, tags, checkbox squares), `--r2` 6px (buttons, options, inputs, rows),
`--r3` 10px (unit sections, bottom sheet). Controls are a 1px `--edge` outline; a selected or verdict state swaps the colour and adds `box-shadow: inset 0 0 0 1px <colour>`, which gives a 2px look with no layout shift.

## Icons

`web/src/components/icons.tsx`: inline SVG, 24px grid, `stroke="currentColor"`, 1.75px stroke, round caps and joins, 1.25em by default so they follow the text size. `<Icon name="check" />` is decorative (`aria-hidden`);
pass `title="..."` when an icon carries meaning alone (it then gets `role="img"` and a `<title>`). Meaning is otherwise carried by adjacent text. XP has no icon, the word is clearer.

The shapes are copied from [Lucide](https://lucide.dev) (ISC licence, v1.68.0). To add one, copy its path markup from the Lucide SVG into `ICONS`; the unit and badge tests fail if a YAML `icon:` or a badge names an icon that is not there.
`scripts/icons.test.ts` also fails if an emoji or text glyph (check marks, arrows, stars) sneaks back into the web sources.

Units name their icon in YAML (`icon: braces`); badges carry an `icon` name in `shared/src/achievements.ts`. Unknown or missing names fall back to a default instead of breaking.

### Lucide licence (ISC, and MIT for the Feather-derived icons)

Lucide is ISC licensed; a number of its icons (`check`, `x`, `chevron-*`, `arrow-*`, `info`, `target`, `terminal`, `hash`, `database`, `compass`, `clock` and others used here) derive from Feather and are MIT licensed
(Copyright (c) 2013-present Cole Bemis). The full text of both licences, as shipped with `lucide-static@1.68.0`, is kept next to the icon data in
[`web/src/components/LUCIDE-LICENSE.txt`](../web/src/components/LUCIDE-LICENSE.txt), and the notice is repeated at the top of `icons.tsx` as the licence requires.
The font licence (SIL OFL 1.1) is in `web/src/fonts/OFL.txt`.

## Units: pens and icons

Each unit has a pen colour (`--pen-<unit id>`, defined for all 13 course units) and an icon. Both are wayfinding: they appear on the Home title block (3px spine and a 34px outlined glyph), the Library, the topic rows on Progress and the mock scorecard.
They are never used as text colour, as a fill behind text or as a verdict. A unit without a pen falls back to cobalt (`web/src/lib/unit-style.ts`).

## Screens

- **Home**: a lesson index. Header with wordmark and Settings, rank line and gauge, a readout row (streak, XP, daily goal ring), the Today card (ink icon tile, ink "Start"), practice rows, a segmented lesson bar,
  then one title block per unit with 56px lesson rows (number, title, mastery gauge, done box). The next lesson has a cobalt tint, a rail and an "Up next" tag.
- **Lesson**: close, a gauge with one tick per question, XP readout; a mono kicker, the prompt, the answer area; a fixed bottom bar with a 3px verdict stripe (teal or vermilion) after checking. The primary button stays ink in every state.
- **Feedback**: a neutral sheet panel with a 4px rail, a verdict line (icon and word), the heading, ink body text, and the details.
- **Question types**: options with letter markers and a tag word; blanks as form underlines; order rows numbered by a CSS counter; spot lines 44px with a line-number gutter and a mark; estimate as one field with the unit inside; match items with numbered badges
  (the number carries the pairing, the pen hue is secondary); code editor with 44px symbol keys and test rows (icon, call, expected/got diff); open answers with a status line (`bot` or `wifi-off`) and a counter.
- **Summary**: a mono readout, a ruled table (XP earned, accuracy, streak, to revisit), the goal ring, notes with icons. "Next lesson" is the primary action when there is one.
- **Progress, Library, Mock interview, Settings and the sheets** use the same parts: ruled lists (`.facts`, `.calib-rows`, `.mock-history`), 8px gauges, outlined chips, dashed borders for "flagged" and "partial".
- **Share card** (`web/src/lib/share-image.ts`): drawn on a canvas in the light palette whatever the app theme is (a picture that leaves the app should look the same everywhere), with a drawing border, a title block, a ruled readout and tick gauges, in Plex Sans and the system mono.

## Rules for new UI

1. Use tokens and the type tokens; no raw colours, no `px` font sizes (`var(--fs-16)`).
2. A new state needs an icon, a word and a border or underline style, not only a hue.
3. Add any new foreground/background combination to `pairs()` in `scripts/contrast.mjs`.
4. Icons from `icons.tsx` only; add the Lucide shape there. No emoji, no text glyphs as icons.
5. 44px minimum targets, no horizontal overflow at 320 and 390px, and check the screen in dark mode.
6. Add the screen or state to `e2e/a11y.spec.ts`.
