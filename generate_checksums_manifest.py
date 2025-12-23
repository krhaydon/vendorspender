#!/usr/bin/env python3
"""
generate_checksums_manifest.py

Checksum + manifest generator consistent with copy_with_checksums_manifest.py.

- Prompts for Technician and package root (the already-copied top-level folder).
- Immediately creates checksums_<stamp>.txt in the package root and appends each SHA-256 line as files are hashed.
- Writes manifest_<stamp>.json and transfer_receipt_<stamp>.txt in the package root.
- Progress printed every N files (default 100).
- Safe: no copying, no deletions.
"""
from __future__ import annotations
import os
import sys
import shlex
import hashlib
import json
import time
import argparse
import traceback
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

CHUNK_SIZE = 16 * 1024 * 1024
DEFAULT_PROGRESS_INTERVAL = 100

def normalize_path(p: str) -> str:
    if p is None:
        return None
    s = p.strip()
    if not s:
        return s
    try:
        toks = shlex.split(s)
        if toks:
            s = toks[0]
    except Exception:
        pass
    return os.path.abspath(os.path.expanduser(s))

def prompt(text: str, default: str = "") -> str:
    try:
        r = input(text)
        if r.strip() == "" and default:
            return default
        return r.strip()
    except EOFError:
        return default

def now_stamp():
    """Return (stamp, iso_ts). stamp is YYYYMMDD_HHMMSS using America/New_York if available."""
    if ZoneInfo:
        try:
            tz = ZoneInfo("America/New_York")
            t = datetime.now(tz)
        except Exception:
            t = datetime.now()
    else:
        t = datetime.now()
    return t.strftime("%Y%m%d_%H%M%S"), t.isoformat(timespec="seconds")

def list_files_recursive(root: str):
    items = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            try:
                size = os.path.getsize(full)
            except OSError:
                size = None
            items.append({"rel_path": rel, "size": size})
    return items

def compute_sha256_stream(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def write_checksums_and_progress(pkg_root: str, relpaths: list, checksums_path: str, interval: int):
    total = len(relpaths)
    file_shas = {}
    start = time.time()
    with open(checksums_path, "a", encoding="utf-8") as fh:
        # start header (file exists immediately)
        fh.write(f"# checksums pass started at {datetime.now().isoformat(timespec='seconds')}\n")
        fh.flush()
        for i, rel in enumerate(relpaths, start=1):
            absfp = os.path.join(pkg_root, rel.replace("/", os.sep))
            try:
                sha = compute_sha256_stream(absfp)
                file_shas[rel] = sha
                fh.write(f"{sha}  *{rel}\n")
            except Exception as e:
                file_shas[rel] = None
                fh.write(f"ERROR_NO_SHA  *{rel}\n")
            fh.flush()
            if i % interval == 0 or i == total:
                now = time.time()
                elapsed = now - start
                avg = elapsed / i
                remaining = total - i
                eta = remaining * avg
                pct = (i / total) * 100.0
                elapsed_h = time.strftime("%H:%M:%S", time.gmtime(elapsed))
                eta_h = time.strftime("%H:%M:%S", time.gmtime(max(0, eta)))
                print(f"Hashed {i}/{total} ({pct:.1f}%) — elapsed {elapsed_h}, ETA {eta_h}")
    return file_shas

def write_manifest_and_receipt(pkg_root: str, stamp: str, tech: str, rel_items: list, file_shas: dict):
    manifest_name = f"manifest_{stamp}.json"
    receipt_name = f"transfer_receipt_{stamp}.txt"
    manifest_path = os.path.join(pkg_root, manifest_name)
    receipt_path = os.path.join(pkg_root, receipt_name)
    created_iso = datetime.now().isoformat(timespec="seconds")
    files_section = []
    for item in sorted(rel_items, key=lambda r: r["rel_path"]):
        rel = item["rel_path"]
        files_section.append({
            "rel_path": rel,
            "size": item.get("size"),
            "sha256": file_shas.get(rel)
        })
    manifest = {
        "package_name": os.path.basename(pkg_root.rstrip(os.sep)),
        "technician": tech,
        "created_at": created_iso,
        "stamp": stamp,
        "file_count": len(files_section),
        "files": files_section,
        "status": {"checksums_created": True, "overall": "READY"}
    }
    with open(manifest_path, "w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)
    with open(receipt_path, "w", encoding="utf-8") as rf:
        lines = [
            "Transfer receipt",
            f"Package: {os.path.basename(pkg_root.rstrip(os.sep))}",
            f"Technician: {tech}",
            f"Created at: {created_iso}",
            f"Files hashed: {len(files_section)}",
            f"Checksums file: checksums_{stamp}.txt",
        ]
        rf.write("\n".join(lines) + "\n")
    return manifest_path, receipt_path

def parse_args():
    p = argparse.ArgumentParser(description="Generate checksums and manifest for an existing package root.")
    p.add_argument("--package", "-p", help="Package root path (top-level copied folder). If omitted, you'll be prompted.")
    p.add_argument("--technician", "-t", help="Technician name. If omitted, you'll be prompted.")
    p.add_argument("--progress-interval", "-n", type=int, default=DEFAULT_PROGRESS_INTERVAL, help="Print progress every N files.")
    return p.parse_args()

def main():
    args = parse_args()
    try:
        if args.technician:
            tech = args.technician
        else:
            tech = prompt("Technician name: ").strip()
            if not tech:
                print("Technician name required. Aborting."); return

        pkg_raw = args.package or prompt("Package root directory (the already-copied top folder): ")
        pkg = normalize_path(pkg_raw)
        if not pkg or not os.path.isdir(pkg):
            print("ERROR: package root not found or not a directory:", pkg); return

        interval = max(1, int(args.progress_interval))

        print("Scanning files under:", pkg)
        items = list_files_recursive(pkg)
        total = len(items)
        if total == 0:
            print("No files found under package root. Nothing to do."); return
        relpaths = [r["rel_path"] for r in items]
        print(f"Found {total} files. Creating checksums file and starting hashing pass.")

        stamp, _ = now_stamp()
        checksums_name = f"checksums_{stamp}.txt"
        checksums_path = os.path.join(pkg, checksums_name)

        # create the checksums file immediately with header
        with open(checksums_path, "w", encoding="utf-8") as fh:
            fh.write(f"# checksums file started at {datetime.now().isoformat(timespec='seconds')}\n")
            fh.write(f"# package: {os.path.basename(pkg.rstrip(os.sep))}\n")
            fh.write(f"# technician: {tech}\n")
            fh.flush()
        print("Checksums file created:", checksums_path)

        # run hashing and write progressively
        file_shas = write_checksums_and_progress(pkg, relpaths, checksums_path, interval)
        print("Hashing complete.")

        # write manifest and receipt using same stamp
        manifest_path, receipt_path = write_manifest_and_receipt(pkg, stamp, tech, items, file_shas)
        print("Manifest written to:", manifest_path)
        print("Receipt written to:", receipt_path)

        print("\nDone. Outputs in package root:", pkg)
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception:
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
