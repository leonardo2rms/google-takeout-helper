# Google Takeout Metadata Fixer

Stamps missing date metadata into photos exported from Google Takeout, so they show up correctly in Amazon Photos (and any other photo manager that reads EXIF).

Google Takeout doesn't embed dates inside the image files — it puts them in sidecar `.json` files instead. This script reads those JSON files and writes the date directly into each image's EXIF metadata.

## Requirements

- Python 3.10+
- `exiftool` (only needed for HEIC and video files): `brew install exiftool`

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
# Preview what would change — no files are modified
python fix_metadata.py /path/to/takeout/folder --dry-run

# Apply
python fix_metadata.py /path/to/takeout/folder
```

### Options

| Flag | Description |
|------|-------------|
| `--dry-run` | Print what would be changed without writing anything |
| `--no-recursive` | Only process the top-level folder, skip subdirectories |
| `--verbose` | Also log files that already have a date (silent by default) |

## How it works

For each image file (`.jpg`, `.jpeg`, `.png`, `.heic`, `.mp4`, `.mov`):

1. Checks if `DateTimeOriginal` is already set → skips if so
2. Looks for a matching JSON sidecar using all known Google Takeout naming patterns:
   - `photo.jpg.json`
   - `photo.jpg.supplemental-metadata.json`
   - `photo.jpg.suppl.json`
   - `photo.json` (stem-only variants of all the above)
   - Truncated filename variants (Google truncates long names at 46 chars)
3. Reads `photoTakenTime.timestamp`, falling back to `creationTime.timestamp`
4. Writes the date into the image EXIF in-place, preserving all other metadata

File system timestamps (modified/accessed dates) are preserved after writing.

## Supported formats

| Format | How metadata is written |
|--------|------------------------|
| JPEG, TIFF | EXIF via `piexif` |
| PNG | `Creation Time` text chunk via Pillow |
| HEIC, MP4, MOV, etc. | `exiftool` CLI — install with `brew install exiftool` |
