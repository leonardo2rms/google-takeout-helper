> **Archived research, snapshot.** Measured WCAG contrast of every text/background pairing in the ORIGINAL palette (light and dark), from the design audit. Most pairings failed AA.
>
> Written on 2026-11-04/05 while this app was being built, by an agent working from the brief described in [the handoff](../HANDOFF.md). It is kept as written so later decisions can be traced back to it. Paths such as `$S/...`, `$QA/...`, `$QA2/...` or `/tmp/claude-0/...` point at screenshots and scripts in that session's scratch space and are **not** in the repository. For what has since been built or fixed, the handoff is the source of truth.

| theme | pairing | fg | bg | ratio | need | |
|---|---|---|---|---|---|---|
| light | body text on bg | #3c3c3c | #ffffff | 11.03 | 4.5 | pass |
| light | body text on surface (.answer-box) | #3c3c3c | #f7f7f7 | 10.30 | 4.5 | pass |
| light | muted on bg (kicker, hint, .overall) | #777777 | #ffffff | 4.48 | 4.5 | **FAIL** |
| light | muted on surface (placeholder, .pill, .key) | #777777 | #f7f7f7 | 4.18 | 4.5 | **FAIL** |
| light | muted on disabled btn (border) | #777777 | #e5e5e5 | 3.55 | 4.5 | **FAIL** |
| light | white on primary btn (green #58cc02) | #ffffff | #58cc02 | 2.09 | 4.5 | **FAIL** |
| light | white on secondary btn (blue #1cb0f6) | #ffffff | #1cb0f6 | 2.44 | 4.5 | **FAIL** |
| light | white on bad-state Continue (red #ff4b4b) | #ffffff | #ff4b4b | 3.30 | 4.5 | **FAIL** |
| light | white on option marker selected (blue) | #ffffff | #1cb0f6 | 2.44 | 3 | **FAIL** |
| light | white on correct marker (green) | #ffffff | #58cc02 | 2.09 | 3 | **FAIL** |
| light | white on wrong marker (red) | #ffffff | #ff4b4b | 3.30 | 3 | pass |
| light | good feedback: green-text on green-bg | #3d7f00 | #d7ffb8 | 4.46 | 4.5 | **FAIL** |
| light | bad feedback: red-text on red-bg | #c0392b | #ffdfe0 | 4.37 | 4.5 | **FAIL** |
| light | meh feedback: amber-text on amber-bg | #8a6100 | #fff3d1 | 5.01 | 4.5 | pass |
| light | feedback .hint (opacity .8) on red-bg | #c0392b | #ffdfe0 | 3.26 | 4.5 | **FAIL** |
| light | feedback .hint (opacity .8) on amber-bg | #8a6100 | #fff3d1 | 3.43 | 4.5 | **FAIL** |
| light | red-text on inline-code in feedback (rgba(0,0,0,.08) over red-bg) | #c0392b | blend | 3.67 | 4.5 | **FAIL** |
| light | .option.missed green-text on bg | #3d7f00 | #ffffff | 4.96 | 4.5 | pass |
| light | link / blue-dark on bg | #1899d6 | #ffffff | 3.20 | 4.5 | **FAIL** |
| light | .pill.on / interviewer-note blue-dark on blue-bg | #1899d6 | #ddf4ff | 2.81 | 4.5 | **FAIL** |
| light | .rubric .yes b green-dark on amber-bg | #58a700 | #fff3d1 | 2.74 | 3 | **FAIL** |
| light | .rubric .no b red-dark on amber-bg | #ea2b2b | #fff3d1 | 3.89 | 3 | pass |
| light | .tests pass b green-dark on red-bg (bad feedback) | #58a700 | #ffdfe0 | 2.43 | 3 | **FAIL** |
| light | .tests fail b red-dark on red-bg | #ea2b2b | #ffdfe0 | 3.46 | 3 | pass |
| light | stars #e0a800 on bg | #e0a800 | #ffffff | 2.15 | 3 | **FAIL** |
| light | summary tile label white on XP yellow | #ffffff | #ffc800 | 1.55 | 4.5 | **FAIL** |
| light | summary tile label white on streak orange | #ffffff | #ff9600 | 2.18 | 4.5 | **FAIL** |
| light | summary tile label white on accuracy green | #ffffff | #58cc02 | 2.09 | 4.5 | **FAIL** |
| light | unit banner/node white on kotlin #7f52ff | #ffffff | #7f52ff | 4.61 | 4.5 | pass |
| light | unit banner/node white on coroutines #1cb0f6 | #ffffff | #1cb0f6 | 2.44 | 4.5 | **FAIL** |
| light | unit banner/node white on spring #58cc02 | #ffffff | #58cc02 | 2.09 | 4.5 | **FAIL** |
| light | unit banner/node white on jvm #ff9600 | #ffffff | #ff9600 | 2.18 | 4.5 | **FAIL** |
| light | unit banner/node white on ddd #ce82ff | #ffffff | #ce82ff | 2.54 | 4.5 | **FAIL** |
| light | unit banner/node white on scale #ffc800 | #ffffff | #ffc800 | 1.55 | 4.5 | **FAIL** |
| light | unit banner/node white on complexity #ff4b4b | #ffffff | #ff4b4b | 3.30 | 4.5 | **FAIL** |
| light | unit banner/node white on ai #2b70c9 | #ffffff | #2b70c9 | 4.93 | 4.5 | pass |
| light | unit-count: white on rgba(0,0,0,.18) over coroutines blue | #ffffff | blend | 3.57 | 4.5 | **FAIL** |
| light | code token keyword on code-bg | #cf222e | #f4f5f7 | 4.91 | 4.5 | pass |
| light | code token string on code-bg | #0a7b3e | #f4f5f7 | 4.92 | 4.5 | pass |
| light | code token comment on code-bg | #6e7781 | #f4f5f7 | 4.17 | 4.5 | **FAIL** |
| light | code token number on code-bg | #0550ae | #f4f5f7 | 6.96 | 4.5 | pass |
| light | code token annotation on code-bg | #8250df | #f4f5f7 | 4.63 | 4.5 | pass |
| light | code token type on code-bg | #953800 | #f4f5f7 | 6.77 | 4.5 | pass |
| light | code token plain on code-bg | #24292f | #f4f5f7 | 13.43 | 4.5 | pass |
| light | code token comment on blue-bg (order slot) | #6e7781 | #ddf4ff | 4.00 | 4.5 | **FAIL** |
| light | NON-TEXT border (#e5e5e5-ish) on bg (input/option outline) | #e5e5e5 | #ffffff | 1.26 | 3 | **FAIL** |
| light | NON-TEXT border on surface (answer-box) | #e5e5e5 | #f7f7f7 | 1.18 | 3 | **FAIL** |
| light | NON-TEXT blue focus/selected border on bg | #1cb0f6 | #ffffff | 2.44 | 3 | **FAIL** |
| light | NON-TEXT green border (correct) on bg | #58cc02 | #ffffff | 2.09 | 3 | **FAIL** |
| light | NON-TEXT green border on green-bg | #58cc02 | #d7ffb8 | 1.88 | 3 | **FAIL** |
| light | NON-TEXT red border on bg | #ff4b4b | #ffffff | 3.30 | 3 | pass |
| light | NON-TEXT red border on red-bg | #ff4b4b | #ffdfe0 | 2.65 | 3 | **FAIL** |
| light | NON-TEXT progress fill (violet #7f52ff) vs track | #7f52ff | #e5e5e5 | 3.66 | 3 | pass |
| light | NON-TEXT progress fill coroutines blue vs track | #1cb0f6 | #e5e5e5 | 1.94 | 3 | **FAIL** |
| light | NON-TEXT progress fill yellow #ffc800 vs track | #ffc800 | #e5e5e5 | 1.23 | 3 | **FAIL** |
| light | NON-TEXT progress fill yellow #ffc800 vs bg | #ffc800 | #ffffff | 1.55 | 3 | **FAIL** |
| dark | body text on bg | #e5edf1 | #131f24 | 14.18 | 4.5 | pass |
| dark | body text on surface (.answer-box) | #e5edf1 | #1f2c33 | 12.08 | 4.5 | pass |
| dark | muted on bg (kicker, hint, .overall) | #93a4ad | #131f24 | 6.53 | 4.5 | pass |
| dark | muted on surface (placeholder, .pill, .key) | #93a4ad | #1f2c33 | 5.56 | 4.5 | pass |
| dark | muted on disabled btn (border) | #93a4ad | #37464f | 3.79 | 4.5 | **FAIL** |
| dark | white on primary btn (green #58cc02) | #ffffff | #58cc02 | 2.09 | 4.5 | **FAIL** |
| dark | white on secondary btn (blue #1cb0f6) | #ffffff | #1cb0f6 | 2.44 | 4.5 | **FAIL** |
| dark | white on bad-state Continue (red #ff4b4b) | #ffffff | #ff4b4b | 3.30 | 4.5 | **FAIL** |
| dark | white on option marker selected (blue) | #ffffff | #1cb0f6 | 2.44 | 3 | **FAIL** |
| dark | white on correct marker (green) | #ffffff | #58cc02 | 2.09 | 3 | **FAIL** |
| dark | white on wrong marker (red) | #ffffff | #ff4b4b | 3.30 | 3 | pass |
| dark | good feedback: green-text on green-bg | #8ee04a | #1d3b17 | 7.64 | 4.5 | pass |
| dark | bad feedback: red-text on red-bg | #ff8a8a | #3b1d1f | 6.70 | 4.5 | pass |
| dark | meh feedback: amber-text on amber-bg | #ffd36b | #3b3017 | 9.12 | 4.5 | pass |
| dark | feedback .hint (opacity .8) on red-bg | #ff8a8a | #3b1d1f | 4.83 | 4.5 | pass |
| dark | feedback .hint (opacity .8) on amber-bg | #ffd36b | #3b3017 | 6.44 | 4.5 | pass |
| dark | red-text on inline-code in feedback (rgba(0,0,0,.08) over red-bg) | #ff8a8a | blend | 6.95 | 4.5 | pass |
| dark | .option.missed green-text on bg | #8ee04a | #131f24 | 10.34 | 4.5 | pass |
| dark | link / blue-dark on bg | #1899d6 | #131f24 | 5.26 | 4.5 | pass |
| dark | .pill.on / interviewer-note blue-dark on blue-bg | #1899d6 | #17323f | 4.20 | 4.5 | **FAIL** |
| dark | .rubric .yes b green-dark on amber-bg | #58a700 | #3b3017 | 4.28 | 3 | pass |
| dark | .rubric .no b red-dark on amber-bg | #ea2b2b | #3b3017 | 3.01 | 3 | pass |
| dark | .tests pass b green-dark on red-bg (bad feedback) | #58a700 | #3b1d1f | 5.03 | 3 | pass |
| dark | .tests fail b red-dark on red-bg | #ea2b2b | #3b1d1f | 3.53 | 3 | pass |
| dark | stars #e0a800 on bg | #e0a800 | #131f24 | 7.82 | 3 | pass |
| dark | summary tile label white on XP yellow | #ffffff | #ffc800 | 1.55 | 4.5 | **FAIL** |
| dark | summary tile label white on streak orange | #ffffff | #ff9600 | 2.18 | 4.5 | **FAIL** |
| dark | summary tile label white on accuracy green | #ffffff | #58cc02 | 2.09 | 4.5 | **FAIL** |
| dark | unit banner/node white on kotlin #7f52ff | #ffffff | #7f52ff | 4.61 | 4.5 | pass |
| dark | unit banner/node white on coroutines #1cb0f6 | #ffffff | #1cb0f6 | 2.44 | 4.5 | **FAIL** |
| dark | unit banner/node white on spring #58cc02 | #ffffff | #58cc02 | 2.09 | 4.5 | **FAIL** |
| dark | unit banner/node white on jvm #ff9600 | #ffffff | #ff9600 | 2.18 | 4.5 | **FAIL** |
| dark | unit banner/node white on ddd #ce82ff | #ffffff | #ce82ff | 2.54 | 4.5 | **FAIL** |
| dark | unit banner/node white on scale #ffc800 | #ffffff | #ffc800 | 1.55 | 4.5 | **FAIL** |
| dark | unit banner/node white on complexity #ff4b4b | #ffffff | #ff4b4b | 3.30 | 4.5 | **FAIL** |
| dark | unit banner/node white on ai #2b70c9 | #ffffff | #2b70c9 | 4.93 | 4.5 | pass |
| dark | unit-count: white on rgba(0,0,0,.18) over coroutines blue | #ffffff | blend | 3.57 | 4.5 | **FAIL** |
| dark | code token keyword on code-bg | #ff7b72 | #0e171b | 7.20 | 4.5 | pass |
| dark | code token string on code-bg | #7ee787 | #0e171b | 11.81 | 4.5 | pass |
| dark | code token comment on code-bg | #8b949e | #0e171b | 5.90 | 4.5 | pass |
| dark | code token number on code-bg | #79c0ff | #0e171b | 9.33 | 4.5 | pass |
| dark | code token annotation on code-bg | #d2a8ff | #0e171b | 9.32 | 4.5 | pass |
| dark | code token type on code-bg | #ffa657 | #0e171b | 9.37 | 4.5 | pass |
| dark | code token plain on code-bg | #d7e2e7 | #0e171b | 13.76 | 4.5 | pass |
| dark | code token comment on blue-bg (order slot) | #8b949e | #17323f | 4.36 | 4.5 | **FAIL** |
| dark | NON-TEXT border (#e5e5e5-ish) on bg (input/option outline) | #37464f | #131f24 | 1.72 | 3 | **FAIL** |
| dark | NON-TEXT border on surface (answer-box) | #37464f | #1f2c33 | 1.47 | 3 | **FAIL** |
| dark | NON-TEXT blue focus/selected border on bg | #1cb0f6 | #131f24 | 6.88 | 3 | pass |
| dark | NON-TEXT green border (correct) on bg | #58cc02 | #131f24 | 8.05 | 3 | pass |
| dark | NON-TEXT green border on green-bg | #58cc02 | #1d3b17 | 5.95 | 3 | pass |
| dark | NON-TEXT red border on bg | #ff4b4b | #131f24 | 5.09 | 3 | pass |
| dark | NON-TEXT red border on red-bg | #ff4b4b | #3b1d1f | 4.60 | 3 | pass |
| dark | NON-TEXT progress fill (violet #7f52ff) vs track | #7f52ff | #37464f | 2.12 | 3 | **FAIL** |
| dark | NON-TEXT progress fill coroutines blue vs track | #1cb0f6 | #37464f | 3.99 | 3 | pass |
| dark | NON-TEXT progress fill yellow #ffc800 vs track | #ffc800 | #37464f | 6.28 | 3 | pass |
| dark | NON-TEXT progress fill yellow #ffc800 vs bg | #ffc800 | #131f24 | 10.82 | 3 | pass |
