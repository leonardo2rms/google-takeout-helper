#!/usr/bin/env python3
"""
Google Takeout Metadata Fixer
Reads photos from a folder and, for images missing a date, finds the matching
Google Takeout JSON sidecar and writes the date into the image's EXIF metadata.

Usage:
    python fix_metadata.py <photos_dir> [--dry-run] [--no-recursive] [--verbose]
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

try:
    import piexif
    from PIL import Image
except ImportError:
    print("Missing dependencies. Run:  pip install -r requirements.txt")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

JPEG_EXTENSIONS = {".jpg", ".jpeg", ".tiff", ".tif"}
PNG_EXTENSIONS  = {".png"}
EXIFTOOL_EXTENSIONS = {".heic", ".heif", ".mp4", ".mov", ".m4v", ".avi"}

ALL_SUPPORTED = JPEG_EXTENSIONS | PNG_EXTENSIONS | EXIFTOOL_EXTENSIONS

# Google truncates filenames at 46 chars (base, without extension) in some exports
GOOGLE_TRUNCATE_LEN = 46

# Sidecar suffixes tried in order
SIDECAR_SUFFIXES = [
    ".json",
    ".supplemental-metadata.json",
    ".suppl.json",
]

# ---------------------------------------------------------------------------
# Logging setup  (file handler is attached in main() once --log-file is known)
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def setup_log_file(log_path: Path) -> None:
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s"))
    logger.addHandler(handler)
    print(f"Logging to: {log_path}\n", flush=True)


# ---------------------------------------------------------------------------
# JSON sidecar discovery
# ---------------------------------------------------------------------------

def find_sidecar(image_path: Path) -> Path | None:
    """
    Try every known Google Takeout sidecar naming pattern for *image_path*.
    Returns the first existing sidecar Path, or None.
    """
    parent = image_path.parent
    name   = image_path.name        # e.g. "IMG_1234.jpg"
    stem   = image_path.stem        # e.g. "IMG_1234"

    candidates = []

    # Pattern 1-3: append suffix to full filename (most common)
    for suffix in SIDECAR_SUFFIXES:
        candidates.append(parent / (name + suffix))

    # Pattern 4-6: append suffix to stem only (no image extension)
    for suffix in SIDECAR_SUFFIXES:
        candidates.append(parent / (stem + suffix))

    # Pattern 7: truncation fallback — Google truncates long stems
    # e.g. "A_very_long_filename_that_exceeds_the_limit.jpg" →
    #      "A_very_long_filename_that_exceeds_the_li.jpg.json"
    if len(stem) > GOOGLE_TRUNCATE_LEN:
        truncated_stem = stem[:GOOGLE_TRUNCATE_LEN]
        for suffix in SIDECAR_SUFFIXES:
            candidates.append(parent / (truncated_stem + image_path.suffix + suffix))
            candidates.append(parent / (truncated_stem + suffix))

    for candidate in candidates:
        if candidate.exists():
            return candidate

    return None


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------

def parse_timestamp(sidecar_path: Path) -> datetime | None:
    """
    Parse photoTakenTime.timestamp (falling back to creationTime.timestamp)
    from a Google Takeout JSON sidecar. Returns a UTC datetime or None.
    """
    try:
        with open(sidecar_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("  ✗  Could not read JSON %s: %s", sidecar_path.name, exc)
        return None

    for key in ("photoTakenTime", "creationTime"):
        entry = data.get(key)
        if entry and entry.get("timestamp"):
            try:
                ts = int(entry["timestamp"])
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except (ValueError, OSError):
                continue

    logger.warning("  ✗  No usable timestamp in %s", sidecar_path.name)
    return None


def dt_to_exif_str(dt: datetime) -> str:
    """Convert a datetime to the EXIF 'YYYY:MM:DD HH:MM:SS' string format."""
    return dt.strftime("%Y:%m:%d %H:%M:%S")


# EXIF lives in the APP1 segment near the start of a JPEG. 128 KB is enough
# to cover any real-world EXIF header (metadata + small thumbnail) without
# loading the full image pixel data, which can be tens of megabytes.
_EXIF_HEADER_BYTES = 131072


def _load_exif_header(image_path: Path) -> dict:
    """
    Read only the first _EXIF_HEADER_BYTES of a JPEG/TIFF and parse EXIF from
    that slice. Avoids loading full image pixel data just to read metadata.
    """
    with open(image_path, "rb") as f:
        header = f.read(_EXIF_HEADER_BYTES)
    try:
        return piexif.load(header)
    except Exception:
        return {"0th": {}, "Exif": {}, "GPS": {}, "1st": {}}


# ---------------------------------------------------------------------------
# EXIF date checkers
# ---------------------------------------------------------------------------

def jpeg_has_date(image_path: Path) -> bool:
    """Return True if the JPEG/TIFF already has DateTimeOriginal set."""
    try:
        exif_data = _load_exif_header(image_path)
        value = exif_data.get("Exif", {}).get(piexif.ExifIFD.DateTimeOriginal)
        return bool(value and value.strip(b"\x00"))
    except Exception:
        return False


def png_has_date(image_path: Path) -> bool:
    """Return True if the PNG already has a Creation Time text chunk."""
    try:
        with Image.open(image_path) as img:
            info = img.info
            return "Creation Time" in info or "date:create" in info
    except Exception:
        return False


def exiftool_has_date(image_path: Path) -> bool:
    """Return True if exiftool reports a DateTimeOriginal for this file."""
    if not shutil.which("exiftool"):
        return False
    try:
        result = subprocess.run(
            ["exiftool", "-DateTimeOriginal", "-s3", str(image_path)],
            capture_output=True, text=True, timeout=10,
        )
        return bool(result.stdout.strip())
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Metadata writers
# ---------------------------------------------------------------------------

def write_jpeg_date(image_path: Path, dt: datetime, dry_run: bool) -> bool:
    """Write DateTimeOriginal + DateTime + DateTimeDigitized into a JPEG/TIFF."""
    exif_str = dt_to_exif_str(dt).encode("ascii")

    try:
        # Load only the header slice — no need to read pixel data to get existing tags
        exif_data = _load_exif_header(image_path)

        exif_data.setdefault("0th", {})[piexif.ImageIFD.DateTime] = exif_str
        exif_data.setdefault("Exif", {})[piexif.ExifIFD.DateTimeOriginal]   = exif_str
        exif_data["Exif"][piexif.ExifIFD.DateTimeDigitized] = exif_str

        if dry_run:
            return True

        stat = image_path.stat()
        exif_bytes = piexif.dump(exif_data)
        piexif.insert(exif_bytes, str(image_path))
        # Restore file timestamps so the filesystem date doesn't change
        os.utime(image_path, (stat.st_atime, stat.st_mtime))
        return True

    except Exception as exc:
        logger.error("  ✗  Failed to write EXIF to %s: %s", image_path.name, exc)
        return False


def write_png_date(image_path: Path, dt: datetime, dry_run: bool) -> bool:
    """Write Creation Time metadata into a PNG via Pillow."""
    date_str = dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    try:
        if dry_run:
            return True

        stat = image_path.stat()
        with Image.open(image_path) as img:
            metadata = img.info.copy()
            metadata["Creation Time"] = date_str
            # PngImagePlugin uses pnginfo for saving text chunks
            from PIL import PngImagePlugin
            png_info = PngImagePlugin.PngInfo()
            for k, v in metadata.items():
                if isinstance(k, str) and isinstance(v, str):
                    png_info.add_text(k, v)
            img.save(image_path, pnginfo=png_info)

        os.utime(image_path, (stat.st_atime, stat.st_mtime))
        return True

    except Exception as exc:
        logger.error("  ✗  Failed to write PNG metadata to %s: %s", image_path.name, exc)
        return False


def write_exiftool_date(image_path: Path, dt: datetime, dry_run: bool) -> bool:
    """Write DateTimeOriginal via the exiftool CLI."""
    if not shutil.which("exiftool"):
        return False

    exif_str = dt_to_exif_str(dt)
    cmd = [
        "exiftool",
        f"-DateTimeOriginal={exif_str}",
        f"-CreateDate={exif_str}",
        f"-ModifyDate={exif_str}",
        "-overwrite_original",
        str(image_path),
    ]
    if dry_run:
        return True

    try:
        stat = image_path.stat()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            logger.error("  ✗  exiftool error on %s: %s", image_path.name, result.stderr.strip())
            return False
        os.utime(image_path, (stat.st_atime, stat.st_mtime))
        return True
    except Exception as exc:
        logger.error("  ✗  exiftool failed on %s: %s", image_path.name, exc)
        return False


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------

def process_file(image_path: Path, dry_run: bool, verbose: bool) -> str:
    """
    Process a single image file.
    Returns one of: 'fixed', 'already_dated', 'no_json', 'no_timestamp',
                    'write_failed', 'no_exiftool'
    """
    ext = image_path.suffix.lower()

    # --- Check if date already present ---
    if ext in JPEG_EXTENSIONS:
        has_date = jpeg_has_date(image_path)
    elif ext in PNG_EXTENSIONS:
        has_date = png_has_date(image_path)
    else:
        has_date = exiftool_has_date(image_path)

    if has_date:
        if verbose:
            logger.info("  —  Already dated: %s", image_path.name)
        return "already_dated"

    # --- Find sidecar ---
    sidecar = find_sidecar(image_path)
    if sidecar is None:
        logger.warning("  ✗  No JSON sidecar found for: %s", image_path)
        return "no_json"

    # --- Parse timestamp ---
    dt = parse_timestamp(sidecar)
    if dt is None:
        return "no_timestamp"

    # --- Write metadata ---
    if ext in JPEG_EXTENSIONS:
        ok = write_jpeg_date(image_path, dt, dry_run)
    elif ext in PNG_EXTENSIONS:
        ok = write_png_date(image_path, dt, dry_run)
    else:
        if not shutil.which("exiftool"):
            logger.warning(
                "\n  ⚠  Skipping %s (%s) — install exiftool to handle this format",
                image_path.name, ext,
            )
            return "no_exiftool"
        ok = write_exiftool_date(image_path, dt, dry_run)

    if ok:
        return "fixed"
    else:
        return "write_failed"


# ---------------------------------------------------------------------------
# Directory walker
# ---------------------------------------------------------------------------

def walk_directory(photos_dir: Path, recursive: bool):
    """Yield image Paths under photos_dir."""
    if recursive:
        for root, _, files in os.walk(photos_dir):
            for fname in files:
                p = Path(root) / fname
                if p.suffix.lower() in ALL_SUPPORTED:
                    yield p
    else:
        for p in photos_dir.iterdir():
            if p.is_file() and p.suffix.lower() in ALL_SUPPORTED:
                yield p


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Stamp Google Takeout JSON timestamps into image EXIF metadata."
    )
    parser.add_argument("photos_dir", type=Path, help="Folder containing photos and JSON sidecars")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    parser.add_argument("--no-recursive", action="store_true", help="Do not walk subdirectories")
    parser.add_argument("--verbose", action="store_true", help="Also log already-dated files")
    parser.add_argument("--workers", type=int, default=min(64, (os.cpu_count() or 4) * 8),
                        help="Number of parallel worker threads (default: 8x CPU cores)")
    parser.add_argument("--log-file", type=Path, default=Path("fix_metadata.log"),
                        help="Path to log file (default: fix_metadata.log)")
    args = parser.parse_args()

    setup_log_file(args.log_file.expanduser().resolve())

    photos_dir: Path = args.photos_dir.expanduser().resolve()
    if not photos_dir.is_dir():
        logger.error("Not a directory: %s", photos_dir)
        sys.exit(1)

    if args.dry_run:
        logger.info("=== DRY RUN — no files will be modified ===\n")

    counters: Counter = Counter()

    print("Scanning for image files...", flush=True)
    image_paths = list(walk_directory(photos_dir, recursive=not args.no_recursive))
    total_found = len(image_paths)
    print(f"Found {total_found:,} image files — processing with {args.workers} workers...\n", flush=True)

    processed = 0

    def _print_progress():
        print(f"\r  Progress: {processed:,} / {total_found:,}", end="", flush=True)

    _print_progress()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_file, p, args.dry_run, args.verbose): p
            for p in image_paths
        }
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                logger.error("  ✗  Unexpected error on %s: %s", futures[future].name, exc)
                result = "write_failed"
            counters[result] += 1
            processed += 1
            _print_progress()

    print()  # newline after progress line

    # --- Summary ---
    total = total_found
    action = "Would fix" if args.dry_run else "Fixed"
    print()
    print("=" * 50)
    print(f"  Scanned:                      {total}")
    print(f"  ✔  {action}:               {counters['fixed']}")
    print(f"  —  Already had date (skipped): {counters['already_dated']}")
    print(f"  ✗  No JSON sidecar found:      {counters['no_json']}")
    print(f"  ✗  JSON had no timestamp:      {counters['no_timestamp']}")
    print(f"  ✗  Write failed:               {counters['write_failed']}")
    if counters["no_exiftool"]:
        print(f"  ⚠  Needs exiftool (HEIC/video): {counters['no_exiftool']}")
        print("     → Install with:  brew install exiftool")
    print("=" * 50)


if __name__ == "__main__":
    main()
