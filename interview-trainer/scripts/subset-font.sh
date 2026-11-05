#!/usr/bin/env bash
# Regenerates web/src/fonts/plex-sans-var-latin.woff2: a Latin subset of the IBM Plex Sans variable font
# shipped by @fontsource-variable/ibm-plex-sans (the npm package is the source of truth; SIL OFL 1.1).
# The committed output is what the build embeds, so Python is only needed when you change the character list.
#   pip install fonttools brotli && ./scripts/subset-font.sh
set -euo pipefail
cd "$(dirname "$0")/.."
SRC=node_modules/@fontsource-variable/ibm-plex-sans/files/ibm-plex-sans-latin-wght-normal.woff2
mkdir -p web/src/fonts
pyftsubset "$SRC" \
  --unicodes="U+0020-007E,U+00A0,U+00B1,U+00B7,U+00D7,U+2013,U+2014,U+2018,U+2019,U+201C,U+201D,U+2022,U+2026,U+2191,U+2193,U+2212" \
  --flavor=woff2 --layout-features='kern,liga,tnum,zero' \
  --output-file=web/src/fonts/plex-sans-var-latin.woff2
cp node_modules/@fontsource-variable/ibm-plex-sans/LICENSE web/src/fonts/OFL.txt
ls -l web/src/fonts
