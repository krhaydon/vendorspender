#!/usr/bin/env python3
"""
Crawl logger (CSV edition) — updated

- CSV path is provided via command line (--csv) and is required.
- "Action" vocabulary renamed conceptually to "issues" with new choices.
- Prompt order changed: note asked BEFORE next (but CSV still writes next then note
  according to HEADERS = ["seed","date","action","next","note"] so columns remain the same).
- Interactive and non-interactive modes supported.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import shutil
import sys
import time

# ---- Config (no hard-coded CSV path) ----
TIMEZONE = "America/New_York"

# "Issues" controlled vocabulary (replaces previous ACTION_CHOICES)
ISSUE_CHOICES = [
    "missing content/links",
    "page errors (404)",
    "didn't finish",
    "QA issues",
    "updated/added seed",
    "other",
]

# Headers (unchanged)
HEADERS = ["seed", "date", "action", "next", "note"]
LEGACY_HEADERS = ["seed", "date", "action", "note", "next"]

# Timezone support
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None


def prompt_nonempty(label: str) -> str:
    while True:
        v = input(f"{label}: ").strip()
        if v:
            return v
        print("  (This field cannot be blank.)")


def yes_no(prompt: str, default_no: bool = True) -> bool:
    suffix = "y/N" if default_no else "Y/n"
    ans = input(f"{prompt} [{suffix}]: ").strip().lower()
    if not ans:
        return not default_no
    return ans in ("y", "yes")


def select_issue_interactive() -> str:
    print("\nSelect issue:")
    for i, a in enumerate(ISSUE_CHOICES, start=1):
        print(f"  {i}) {a}")
    while True:
        raw = input("Enter number or type the issue exactly: ").strip()
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(ISSUE_CHOICES):
                return ISSUE_CHOICES[idx - 1]
        # case-insensitive exact match
        lowered = raw.lower()
        for a in ISSUE_CHOICES:
            if lowered == a.lower():
                return a
        print("  (Please choose a number from the list or type one of the options exactly.)")


def select_issue_from_string(raw: str) -> Optional[str]:
    """Return normalized issue if raw matches any choice (case-insensitive), else None."""
    if not raw:
        return None
    raw = raw.strip()
    if raw.isdigit():
        idx = int(raw)
        if 1 <= idx <= len(ISSUE_CHOICES):
            return ISSUE_CHOICES[idx - 1]
        return None
    lowered = raw.lower()
    for a in ISSUE_CHOICES:
        if lowered == a.lower():
            return a
    return None


def now_date_str(tz_name: str) -> str:
    tz_info: Optional[ZoneInfo] = None
    if ZoneInfo is not None:
        try:
            tz_info = ZoneInfo(tz_name)  # type: ignore[arg-type]
        except Exception:
            print(f"Warning: Unknown timezone '{tz_name}', using local time.")
            tz_info = None
    dt = datetime.now(tz_info) if tz_info else datetime.now()
    return dt.date().isoformat()


def make_backup(path: Path, suffix: str = "bak") -> Optional[Path]:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return None
    except Exception:
        return None
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    bak = path.with_suffix(f".{ts}.{suffix}.csv")
    try:
        shutil.copy2(path, bak)
        return bak
    except Exception:
        return None


def ensure_csv(path: Path) -> None:
    """Create parent dirs; create file with HEADERS if missing/empty; migrate legacy header if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADERS)
        return

    # Check current header
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            current = next(r, [])
    except Exception:
        current = []

    if current == HEADERS:
        return  # all good

    if current == LEGACY_HEADERS:
        # Auto-migrate: backup, then rewrite with new header and swapped columns
        print("🔁 Detected legacy header (note,next). Migrating to (next,note)...")
        backup = make_backup(path, suffix="pre-migrate")
        if backup:
            print(f"  Backup created: {backup.name}")
        rows = []
        with path.open("r", newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            _ = next(r, None)  # skip header
            for row in r:
                # row is: [seed, date, action, note, next]
                # convert to: [seed, date, action, next, note]
                if len(row) >= 5:
                    seed, date, action, note, nxt = row[:5]
                    rows.append([seed, date, action, nxt, note])
                else:
                    row = (row + [""] * 5)[:5]
                    seed, date, action, note, nxt = row
                    rows.append([seed, date, action, nxt, note])

        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(HEADERS)
            w.writerows(rows)
        print("  Migration complete.")
        return

    # If some other header order, keep it and warn (we'll still append using our new order)
    if current:
        print(f"⚠️ CSV has an unexpected header: {current}. "
              f"New rows will use {HEADERS} order.")


def append_rows(path: Path, rows: List[List[str]], tries: int = 3, dry_run: bool = False) -> int:
    """Append multiple rows to the CSV. Returns rows written."""
    if dry_run:
        print("Dry run enabled — would append the following rows:")
        for r in rows:
            print("  " + ", ".join(r))
        return len(rows)
    for attempt in range(1, tries + 1):
        try:
            with path.open("a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(rows)
            return len(rows)
        except PermissionError:
            if attempt == 1:
                print(f"⚠️  '{path.name}' appears to be open. Close it to save.")
            time.sleep(0.8)
    print(f"ERROR: Could not write to '{path}'.")
    return 0


def read_seeds_file(path: Path) -> List[str]:
    text = ""
    if str(path) == "-":
        text = sys.stdin.read()
    else:
        text = path.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()
    seen = set()
    seeds: List[str] = []
    for line in lines:
        t = line.strip()
        if not t or t.startswith("#"):
            continue
        if t not in seen:
            seeds.append(t)
            seen.add(t)
    return seeds


def interactive_single_loop(csv_path: Path, tz_name: str, dry_run: bool = False) -> None:
    print("\nSingle-entry mode. Press Ctrl+C to stop.")
    total = 0
    try:
        while True:
            seed = prompt_nonempty("seed")
            issue = select_issue_interactive()                 # issue (action) FIRST
            note = input("note (optional): ").strip()         # note asked BEFORE next
            nxt = input("next (optional): ").strip()          # next asked AFTER note
            date_str = now_date_str(tz_name)
            # Write in CSV order: seed, date, action, next, note
            wrote = append_rows(csv_path, [[seed, date_str, issue, nxt, note]], dry_run=dry_run)
            if wrote == 0:
                break
            print("✅ Added 1 row.")
            total += 1
            if not yes_no("Add another?", default_no=False):
                break
    except KeyboardInterrupt:
        print("\n⏹ Stopped by user.")
    print(f"Done. {total} entr{'y' if total == 1 else 'ies'} added.")


def interactive_bulk_from_txt(csv_path: Path, tz_name: str, dry_run: bool = False) -> None:
    print("\nBulk mode (from .txt file).")
    while True:
        p = input("Path to seeds .txt (one seed per line, or '-' for STDIN): ").strip()
        if not p:
            print("  (Please provide a file path.)")
            continue
        seeds_path = Path(p) if p != "-" else Path("-")
        if p == "-" or (seeds_path.exists() and seeds_path.is_file()):
            break
        print("  (File not found. Please try again.)")

    issue = select_issue_interactive()
    note = input("note (optional, applied to every seed): ").strip()   # NOTE before NEXT
    nxt = input("next (optional, applied to every seed): ").strip()

    seeds = read_seeds_file(seeds_path)
    if not seeds:
        print("No seeds found in file (only blanks/comments?). Nothing to do.")
        return

    print(f"\nFound {len(seeds)} seed(s). Example(s):")
    for s in seeds[:5]:
        print(f"  - {s}")
    if len(seeds) > 5:
        print("  ...")

    if not yes_no("Proceed to append all rows?"):
        print("Cancelled.")
        return

    date_str = now_date_str(tz_name)
    rows = [[s, date_str, issue, nxt, note] for s in seeds]

    backup = make_backup(csv_path, suffix="pre-bulk")
    if backup:
        print(f"  Backup created: {backup.name}")

    wrote = append_rows(csv_path, rows, dry_run=dry_run)
    if wrote:
        print(f"✅ Added {wrote} row(s).")


def noninteractive_bulk(csv_path: Path, seeds_path: Path | str, issue_raw: str, nxt: str, note: str, tz_name: str, dry_run: bool = False) -> None:
    issue = select_issue_from_string(issue_raw)
    if issue is None:
        print(f"ERROR: Issue '{issue_raw}' not recognized. Allowed: {ISSUE_CHOICES}")
        return
    seeds = read_seeds_file(Path(seeds_path) if seeds_path != "-" else Path("-"))
    if not seeds:
        print("No seeds found in file (only blanks/comments?). Nothing to do.")
        return
    date_str = now_date_str(tz_name)
    rows = [[s, date_str, issue, nxt, note] for s in seeds]
    backup = make_backup(csv_path, suffix="pre-bulk")
    if backup:
        print(f"  Backup created: {backup.name}")
    wrote = append_rows(csv_path, rows, dry_run=dry_run)
    if wrote:
        print(f"✅ Added {wrote} row(s).")


def noninteractive_add_single(csv_path: Path, seed: str, issue_raw: str, nxt: str, note: str, tz_name: str, dry_run: bool = False) -> None:
    issue = select_issue_from_string(issue_raw)
    if issue is None:
        print(f"ERROR: Issue '{issue_raw}' not recognized. Allowed: {ISSUE_CHOICES}")
        return
    date_str = now_date_str(tz_name)
    wrote = append_rows(csv_path, [[seed, date_str, issue, nxt, note]], dry_run=dry_run)
    if wrote:
        print("✅ Added 1 row.")


def parse_args():
    p = argparse.ArgumentParser(prog="crawl_logger", description="Crawl logger (CSV) — interactive + batch")
    p.add_argument("--csv", "-c", required=True, help="CSV file path (required)")
    sub = p.add_mutually_exclusive_group()
    sub.add_argument("--mode", choices=["interactive", "bulk"], help="Choose initial mode (interactive or bulk). If omitted, script will prompt.")
    p.add_argument("--bulk-file", "-b", help="Path to .txt with seeds (one per line). Use '-' to read from STDIN. If provided, runs bulk non-interactively (requires --action).")
    p.add_argument("--add-seed", "-s", help="Add a single seed non-interactively (requires --action).")
    p.add_argument("--action", "-a", help=f"Issue to record (one of: {', '.join(ISSUE_CHOICES)}). Case-insensitive.")
    p.add_argument("--next", dest="nxt", default="", help="Value for 'next' field (optional).")
    p.add_argument("--note", default="", help="Value for 'note' field (optional).")
    p.add_argument("--tz", default=TIMEZONE, help="Timezone name for date (default America/New_York).")
    p.add_argument("--dry-run", action="store_true", help="Do not write files; print what would be written.")
    return p.parse_args()


def main():
    args = parse_args()
    csv_path = Path(args.csv).expanduser()
    tz = args.tz

    print("=== Crawl Logger (interactive, CSV) ===")
    print(f"CSV file: {csv_path}")
    print(f"Timezone: {tz}")
    ensure_csv(csv_path)

    # If non-interactive single add
    if args.add_seed:
        if not args.action:
            print("ERROR: --add-seed requires --action to be provided.")
            return
        noninteractive_add_single(csv_path, args.add_seed, args.action, args.nxt, args.note, tz, dry_run=args.dry_run)
        return

    # If non-interactive bulk
    if args.bulk_file:
        if not args.action:
            print("ERROR: --bulk-file requires --action to be provided.")
            return
        noninteractive_bulk(csv_path, args.bulk_file, args.action, args.nxt, args.note, tz, dry_run=args.dry_run)
        return

    # Otherwise interactive choices
    while True:
        mode = args.mode
        if not mode:
            print("\nChoose a mode:")
            print("  1) Single-entry (prompt each row)")
            print("  2) BULK from .txt (one seed per line)")
            choice = input("Enter 1 or 2: ").strip()
            if choice == "2":
                mode = "bulk"
            elif choice == "1":
                mode = "interactive"
            else:
                print("  (Please enter 1 or 2.)")
                continue

        if mode == "bulk":
            interactive_bulk_from_txt(csv_path, tz, dry_run=args.dry_run)
        else:
            interactive_single_loop(csv_path, tz, dry_run=args.dry_run)

        if not yes_no("Do you want to add more (choose another mode or continue)?"):
            break
        # reset mode to prompt unless user provided a mode explicitly
        if args.mode:
            break
        mode = None

    print("\nAll done. Goodbye!")


if __name__ == "__main__":
    main()
