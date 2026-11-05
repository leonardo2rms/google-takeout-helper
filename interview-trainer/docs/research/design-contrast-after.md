> **Archived research, snapshot.** Measured WCAG contrast of the NEW drafting-sheet palette (light and dark) before it was implemented. scripts/contrast.mjs now enforces this in npm test.
>
> Written on 2026-11-04/05 while this app was being built, by an agent working from the brief described in [the handoff](../HANDOFF.md). It is kept as written so later decisions can be traced back to it. Paths such as `$S/...`, `$QA/...`, `$QA2/...` or `/tmp/claude-0/...` point at screenshots and scripts in that session's scratch space and are **not** in the repository. For what has since been built or fixed, the handoff is the source of truth.

| theme | pairing | fg | bg | ratio | need | |
|---|---|---|---|---|---|---|
| light | ink on paper | #0F1B26 | #F2F4F6 | 15.81 | 4.5 | pass |
| light | graphite on paper | #4A5967 | #F2F4F6 | 6.53 | 4.5 | pass |
| light | ink on sheet | #0F1B26 | #FBFCFD | 16.97 | 4.5 | pass |
| light | graphite on sheet | #4A5967 | #FBFCFD | 7.01 | 4.5 | pass |
| light | ink on code | #0F1B26 | #E9EDF1 | 14.81 | 4.5 | pass |
| light | graphite on code | #4A5967 | #E9EDF1 | 6.12 | 4.5 | pass |
| light | cobalt text on paper | #2447C4 | #F2F4F6 | 6.87 | 4.5 | pass |
| light | cobalt text on sheet | #2447C4 | #FBFCFD | 7.37 | 4.5 | pass |
| light | cobalt text on tint | #2447C4 | #d9dff0 | 5.68 | 4.5 | pass |
| light | ink on cobalt tint | #0F1B26 | #d9dff0 | 13.08 | 4.5 | pass |
| light | graphite on cobalt tint | #4A5967 | #d9dff0 | 5.41 | 4.5 | pass |
| light | paper glyph on solid cobalt (marker fill) | #F2F4F6 | #2447C4 | 6.87 | 4.5 | pass |
| light | NON-TEXT cobalt border/rail/icon vs paper | #2447C4 | #F2F4F6 | 6.87 | 3 | pass |
| light | NON-TEXT cobalt border/rail/icon vs sheet | #2447C4 | #FBFCFD | 7.37 | 3 | pass |
| light | NON-TEXT cobalt border/rail/icon vs tint | #2447C4 | #d9dff0 | 5.68 | 3 | pass |
| light | teal text on paper | #00695F | #F2F4F6 | 5.98 | 4.5 | pass |
| light | teal text on sheet | #00695F | #FBFCFD | 6.42 | 4.5 | pass |
| light | teal text on tint | #00695F | #d5e3e4 | 5.00 | 4.5 | pass |
| light | ink on teal tint | #0F1B26 | #d5e3e4 | 13.23 | 4.5 | pass |
| light | graphite on teal tint | #4A5967 | #d5e3e4 | 5.47 | 4.5 | pass |
| light | paper glyph on solid teal (marker fill) | #F2F4F6 | #00695F | 5.98 | 4.5 | pass |
| light | NON-TEXT teal border/rail/icon vs paper | #00695F | #F2F4F6 | 5.98 | 3 | pass |
| light | NON-TEXT teal border/rail/icon vs sheet | #00695F | #FBFCFD | 6.42 | 3 | pass |
| light | NON-TEXT teal border/rail/icon vs tint | #00695F | #d5e3e4 | 5.00 | 3 | pass |
| light | vermilion text on paper | #AE320C | #F2F4F6 | 5.84 | 4.5 | pass |
| light | vermilion text on sheet | #AE320C | #FBFCFD | 6.27 | 4.5 | pass |
| light | vermilion text on tint | #AE320C | #eaddda | 4.86 | 4.5 | pass |
| light | ink on vermilion tint | #0F1B26 | #eaddda | 13.16 | 4.5 | pass |
| light | graphite on vermilion tint | #4A5967 | #eaddda | 5.44 | 4.5 | pass |
| light | paper glyph on solid vermilion (marker fill) | #F2F4F6 | #AE320C | 5.84 | 4.5 | pass |
| light | NON-TEXT vermilion border/rail/icon vs paper | #AE320C | #F2F4F6 | 5.84 | 3 | pass |
| light | NON-TEXT vermilion border/rail/icon vs sheet | #AE320C | #FBFCFD | 6.27 | 3 | pass |
| light | NON-TEXT vermilion border/rail/icon vs tint | #AE320C | #eaddda | 4.86 | 3 | pass |
| light | ochre text on paper | #8A5300 | #F2F4F6 | 5.74 | 4.5 | pass |
| light | ochre text on sheet | #8A5300 | #FBFCFD | 6.16 | 4.5 | pass |
| light | ochre text on tint | #8A5300 | #e6e1d8 | 4.86 | 4.5 | pass |
| light | ink on ochre tint | #0F1B26 | #e6e1d8 | 13.38 | 4.5 | pass |
| light | graphite on ochre tint | #4A5967 | #e6e1d8 | 5.53 | 4.5 | pass |
| light | paper glyph on solid ochre (marker fill) | #F2F4F6 | #8A5300 | 5.74 | 4.5 | pass |
| light | NON-TEXT ochre border/rail/icon vs paper | #8A5300 | #F2F4F6 | 5.74 | 3 | pass |
| light | NON-TEXT ochre border/rail/icon vs sheet | #8A5300 | #FBFCFD | 6.16 | 3 | pass |
| light | NON-TEXT ochre border/rail/icon vs tint | #8A5300 | #e6e1d8 | 4.86 | 3 | pass |
| light | primary button label (paper on ink) | #F2F4F6 | #0F1B26 | 15.81 | 4.5 | pass |
| light | graphite on hairline (disabled btn; exempt, kept >=3) | #4A5967 | #D3D9DF | 5.06 | 3 | pass |
| light | NON-TEXT edge vs paper | #7B8996 | #F2F4F6 | 3.25 | 3 | pass |
| light | NON-TEXT edge vs sheet | #7B8996 | #FBFCFD | 3.49 | 3 | pass |
| light | NON-TEXT edge vs code bg | #7B8996 | #E9EDF1 | 3.05 | 3 | pass |
| light | NON-TEXT ink progress fill vs hairline track | #0F1B26 | #D3D9DF | 12.25 | 3 | pass |
| light | NON-TEXT focus ring cobalt vs paper | #2447C4 | #F2F4F6 | 6.87 | 3 | pass |
| light | NON-TEXT hairline vs paper (decorative, informational only) | #D3D9DF | #F2F4F6 | 1.29 | 1.1 | pass |
| light | syntax keyword on code bg | #5A3BC2 | #E9EDF1 | 6.29 | 4.5 | pass |
| light | syntax keyword on cobalt tint (order slot) | #5A3BC2 | #d9dff0 | 5.55 | 4.5 | pass |
| light | syntax keyword on teal tint | #5A3BC2 | #d5e3e4 | 5.61 | 4.5 | pass |
| light | syntax keyword on vermilion tint | #5A3BC2 | #eaddda | 5.58 | 4.5 | pass |
| light | syntax type on code bg | #0B5F8A | #E9EDF1 | 5.91 | 4.5 | pass |
| light | syntax type on cobalt tint (order slot) | #0B5F8A | #d9dff0 | 5.22 | 4.5 | pass |
| light | syntax type on teal tint | #0B5F8A | #d5e3e4 | 5.28 | 4.5 | pass |
| light | syntax type on vermilion tint | #0B5F8A | #eaddda | 5.25 | 4.5 | pass |
| light | syntax string on code bg | #0A6A5A | #E9EDF1 | 5.54 | 4.5 | pass |
| light | syntax string on cobalt tint (order slot) | #0A6A5A | #d9dff0 | 4.89 | 4.5 | pass |
| light | syntax string on teal tint | #0A6A5A | #d5e3e4 | 4.95 | 4.5 | pass |
| light | syntax string on vermilion tint | #0A6A5A | #eaddda | 4.92 | 4.5 | pass |
| light | syntax number on code bg | #9A4A00 | #E9EDF1 | 5.32 | 4.5 | pass |
| light | syntax number on cobalt tint (order slot) | #9A4A00 | #d9dff0 | 4.70 | 4.5 | pass |
| light | syntax number on teal tint | #9A4A00 | #d5e3e4 | 4.75 | 4.5 | pass |
| light | syntax number on vermilion tint | #9A4A00 | #eaddda | 4.73 | 4.5 | pass |
| light | syntax annotation on code bg | #9A2F7D | #E9EDF1 | 5.80 | 4.5 | pass |
| light | syntax annotation on cobalt tint (order slot) | #9A2F7D | #d9dff0 | 5.12 | 4.5 | pass |
| light | syntax annotation on teal tint | #9A2F7D | #d5e3e4 | 5.18 | 4.5 | pass |
| light | syntax annotation on vermilion tint | #9A2F7D | #eaddda | 5.15 | 4.5 | pass |
| light | syntax comment on code bg | #566474 | #E9EDF1 | 5.14 | 4.5 | pass |
| light | syntax comment on cobalt tint (order slot) | #566474 | #d9dff0 | 4.54 | 4.5 | pass |
| light | syntax comment on teal tint | #566474 | #d5e3e4 | 4.59 | 4.5 | pass |
| light | syntax comment on vermilion tint | #566474 | #eaddda | 4.57 | 4.5 | pass |
| light | syntax plain on code bg | #0F1B26 | #E9EDF1 | 14.81 | 4.5 | pass |
| light | syntax plain on cobalt tint (order slot) | #0F1B26 | #d9dff0 | 13.08 | 4.5 | pass |
| light | syntax plain on teal tint | #0F1B26 | #d5e3e4 | 13.23 | 4.5 | pass |
| light | syntax plain on vermilion tint | #0F1B26 | #eaddda | 13.16 | 4.5 | pass |
| dark | ink on paper | #E7EDF2 | #0D1721 | 15.32 | 4.5 | pass |
| dark | graphite on paper | #9DADBB | #0D1721 | 7.86 | 4.5 | pass |
| dark | ink on sheet | #E7EDF2 | #142230 | 13.67 | 4.5 | pass |
| dark | graphite on sheet | #9DADBB | #142230 | 7.02 | 4.5 | pass |
| dark | ink on code | #E7EDF2 | #09121A | 15.99 | 4.5 | pass |
| dark | graphite on code | #9DADBB | #09121A | 8.20 | 4.5 | pass |
| dark | cobalt text on paper | #86A6FF | #0D1721 | 7.67 | 4.5 | pass |
| dark | cobalt text on sheet | #86A6FF | #142230 | 6.85 | 4.5 | pass |
| dark | cobalt text on tint | #86A6FF | #202e45 | 5.79 | 4.5 | pass |
| dark | ink on cobalt tint | #E7EDF2 | #202e45 | 11.57 | 4.5 | pass |
| dark | graphite on cobalt tint | #9DADBB | #202e45 | 5.93 | 4.5 | pass |
| dark | paper glyph on solid cobalt (marker fill) | #0D1721 | #86A6FF | 7.67 | 4.5 | pass |
| dark | NON-TEXT cobalt border/rail/icon vs paper | #86A6FF | #0D1721 | 7.67 | 3 | pass |
| dark | NON-TEXT cobalt border/rail/icon vs sheet | #86A6FF | #142230 | 6.85 | 3 | pass |
| dark | NON-TEXT cobalt border/rail/icon vs tint | #86A6FF | #202e45 | 5.79 | 3 | pass |
| dark | teal text on paper | #46D2B8 | #0D1721 | 9.62 | 4.5 | pass |
| dark | teal text on sheet | #46D2B8 | #142230 | 8.59 | 4.5 | pass |
| dark | teal text on tint | #46D2B8 | #163539 | 6.97 | 4.5 | pass |
| dark | ink on teal tint | #E7EDF2 | #163539 | 11.10 | 4.5 | pass |
| dark | graphite on teal tint | #9DADBB | #163539 | 5.70 | 4.5 | pass |
| dark | paper glyph on solid teal (marker fill) | #0D1721 | #46D2B8 | 9.62 | 4.5 | pass |
| dark | NON-TEXT teal border/rail/icon vs paper | #46D2B8 | #0D1721 | 9.62 | 3 | pass |
| dark | NON-TEXT teal border/rail/icon vs sheet | #46D2B8 | #142230 | 8.59 | 3 | pass |
| dark | NON-TEXT teal border/rail/icon vs tint | #46D2B8 | #163539 | 6.97 | 3 | pass |
| dark | vermilion text on paper | #FF8F6B | #0D1721 | 8.09 | 4.5 | pass |
| dark | vermilion text on sheet | #FF8F6B | #142230 | 7.22 | 4.5 | pass |
| dark | vermilion text on tint | #FF8F6B | #342a2d | 6.20 | 4.5 | pass |
| dark | ink on vermilion tint | #E7EDF2 | #342a2d | 11.74 | 4.5 | pass |
| dark | graphite on vermilion tint | #9DADBB | #342a2d | 6.03 | 4.5 | pass |
| dark | paper glyph on solid vermilion (marker fill) | #0D1721 | #FF8F6B | 8.09 | 4.5 | pass |
| dark | NON-TEXT vermilion border/rail/icon vs paper | #FF8F6B | #0D1721 | 8.09 | 3 | pass |
| dark | NON-TEXT vermilion border/rail/icon vs sheet | #FF8F6B | #142230 | 7.22 | 3 | pass |
| dark | NON-TEXT vermilion border/rail/icon vs tint | #FF8F6B | #342a2d | 6.20 | 3 | pass |
| dark | ochre text on paper | #EBB650 | #0D1721 | 9.76 | 4.5 | pass |
| dark | ochre text on sheet | #EBB650 | #142230 | 8.71 | 4.5 | pass |
| dark | ochre text on tint | #EBB650 | #313029 | 7.15 | 4.5 | pass |
| dark | ink on ochre tint | #E7EDF2 | #313029 | 11.22 | 4.5 | pass |
| dark | graphite on ochre tint | #9DADBB | #313029 | 5.76 | 4.5 | pass |
| dark | paper glyph on solid ochre (marker fill) | #0D1721 | #EBB650 | 9.76 | 4.5 | pass |
| dark | NON-TEXT ochre border/rail/icon vs paper | #EBB650 | #0D1721 | 9.76 | 3 | pass |
| dark | NON-TEXT ochre border/rail/icon vs sheet | #EBB650 | #142230 | 8.71 | 3 | pass |
| dark | NON-TEXT ochre border/rail/icon vs tint | #EBB650 | #313029 | 7.15 | 3 | pass |
| dark | primary button label (paper on ink) | #0D1721 | #E7EDF2 | 15.32 | 4.5 | pass |
| dark | graphite on hairline (disabled btn; exempt, kept >=3) | #9DADBB | #243546 | 5.46 | 3 | pass |
| dark | NON-TEXT edge vs paper | #62758A | #0D1721 | 3.81 | 3 | pass |
| dark | NON-TEXT edge vs sheet | #62758A | #142230 | 3.41 | 3 | pass |
| dark | NON-TEXT edge vs code bg | #62758A | #09121A | 3.98 | 3 | pass |
| dark | NON-TEXT ink progress fill vs hairline track | #E7EDF2 | #243546 | 10.64 | 3 | pass |
| dark | NON-TEXT focus ring cobalt vs paper | #86A6FF | #0D1721 | 7.67 | 3 | pass |
| dark | NON-TEXT hairline vs paper (decorative, informational only) | #243546 | #0D1721 | 1.44 | 1.1 | pass |
| dark | syntax keyword on code bg | #B49CFF | #09121A | 8.21 | 4.5 | pass |
| dark | syntax keyword on cobalt tint (order slot) | #B49CFF | #202e45 | 5.94 | 4.5 | pass |
| dark | syntax keyword on teal tint | #B49CFF | #163539 | 5.70 | 4.5 | pass |
| dark | syntax keyword on vermilion tint | #B49CFF | #342a2d | 6.03 | 4.5 | pass |
| dark | syntax type on code bg | #6FC3F0 | #09121A | 9.65 | 4.5 | pass |
| dark | syntax type on cobalt tint (order slot) | #6FC3F0 | #202e45 | 6.98 | 4.5 | pass |
| dark | syntax type on teal tint | #6FC3F0 | #163539 | 6.70 | 4.5 | pass |
| dark | syntax type on vermilion tint | #6FC3F0 | #342a2d | 7.09 | 4.5 | pass |
| dark | syntax string on code bg | #5FD6B8 | #09121A | 10.60 | 4.5 | pass |
| dark | syntax string on cobalt tint (order slot) | #5FD6B8 | #202e45 | 7.67 | 4.5 | pass |
| dark | syntax string on teal tint | #5FD6B8 | #163539 | 7.36 | 4.5 | pass |
| dark | syntax string on vermilion tint | #5FD6B8 | #342a2d | 7.79 | 4.5 | pass |
| dark | syntax number on code bg | #F0A860 | #09121A | 9.41 | 4.5 | pass |
| dark | syntax number on cobalt tint (order slot) | #F0A860 | #202e45 | 6.81 | 4.5 | pass |
| dark | syntax number on teal tint | #F0A860 | #163539 | 6.54 | 4.5 | pass |
| dark | syntax number on vermilion tint | #F0A860 | #342a2d | 6.91 | 4.5 | pass |
| dark | syntax annotation on code bg | #F08CCB | #09121A | 8.37 | 4.5 | pass |
| dark | syntax annotation on cobalt tint (order slot) | #F08CCB | #202e45 | 6.06 | 4.5 | pass |
| dark | syntax annotation on teal tint | #F08CCB | #163539 | 5.82 | 4.5 | pass |
| dark | syntax annotation on vermilion tint | #F08CCB | #342a2d | 6.15 | 4.5 | pass |
| dark | syntax comment on code bg | #8B9CAB | #09121A | 6.69 | 4.5 | pass |
| dark | syntax comment on cobalt tint (order slot) | #8B9CAB | #202e45 | 4.84 | 4.5 | pass |
| dark | syntax comment on teal tint | #8B9CAB | #163539 | 4.64 | 4.5 | pass |
| dark | syntax comment on vermilion tint | #8B9CAB | #342a2d | 4.91 | 4.5 | pass |
| dark | syntax plain on code bg | #E7EDF2 | #09121A | 15.99 | 4.5 | pass |
| dark | syntax plain on cobalt tint (order slot) | #E7EDF2 | #202e45 | 11.57 | 4.5 | pass |
| dark | syntax plain on teal tint | #E7EDF2 | #163539 | 11.10 | 4.5 | pass |
| dark | syntax plain on vermilion tint | #E7EDF2 | #342a2d | 11.74 | 4.5 | pass |
