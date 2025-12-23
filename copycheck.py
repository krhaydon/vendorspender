#!/usr/bin/env python3
"""
copycheck.py

ALL flow (recommended):
  1. Create source checksums BEFORE copy
     -> SOURCE/aa_logs/checksums_source_<stamp>.txt
  2. Copy source top folder into destination parent (content only)
  3. Create destination checksums AFTER copy
     -> DEST/aa_logs/checksums_dest_<stamp>.txt
  4. Compare source vs destination checksums
  5. Write manifest with verification summary
     -> DEST/aa_logs/manifest_<stamp>.json

All operational files live under aa_logs/.
No transfer receipts. No packaging. No operational files mixed with content.
"""

from __future__ import annotations
import os, sys, shlex, argparse, json, hashlib, time
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

CHUNK_SIZE = 16 * 1024 * 1024
DEFAULT_PROGRESS_INTERVAL = 100
DEFAULT_COPY_INTERVAL = 50
EXCLUDE_PREFIXES = ("checksums_source_",)

# ---------------- utilities ----------------

def normalize_path(p: str) -> str:
    if not p:
        return p
    try:
        p = shlex.split(p)[0]
    except Exception:
        pass
    return os.path.abspath(os.path.expanduser(p.strip()))

def prompt(msg: str) -> str:
    return input(msg).strip()

def now_stamp():
    if ZoneInfo:
        t = datetime.now(ZoneInfo("America/New_York"))
    else:
        t = datetime.now()
    return t.strftime("%Y%m%d_%H%M%S"), t.isoformat(timespec="seconds")

def ensure_legacy_dir(root: str) -> str:
    p = os.path.join(root, "aa_logs")
    os.makedirs(p, exist_ok=True)
    return p

# ---------------- listing ----------------

def list_files(root: str):
    out = []
    for d, _, files in os.walk(root):
        for f in files:
            full = os.path.join(d, f)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            out.append(rel)
    return out

# ---------------- copy ----------------

def copy_tree(src: str, dest_parent: str):
    top = os.path.basename(src.rstrip(os.sep))
    dest_root = os.path.join(dest_parent, top)
    os.makedirs(dest_root, exist_ok=True)

    files = list_files(src)
    files = [f for f in files if not os.path.basename(f).startswith(EXCLUDE_PREFIXES)]

    total = len(files)
    print(f"[COPY] {total} files")

    start = time.time()
    for i, rel in enumerate(files, 1):
        s = os.path.join(src, rel.replace("/", os.sep))
        d = os.path.join(dest_root, rel.replace("/", os.sep))
        os.makedirs(os.path.dirname(d), exist_ok=True)
        print(f"[COPY] {i}/{total}: {rel}")
        with open(s, "rb") as rf, open(d, "wb") as wf:
            while True:
                b = rf.read(CHUNK_SIZE)
                if not b:
                    break
                wf.write(b)
        if i % DEFAULT_COPY_INTERVAL == 0 or i == total:
            elapsed = time.time() - start
            print(f"[COPY] progress {i}/{total} elapsed {int(elapsed)}s")

    return dest_root, files

# ---------------- checksums ----------------

def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()

def write_checksums(root: str, rels: list, out_path: str):
    print(f"[HASH] writing {out_path}")
    hashes = {}
    with open(out_path, "w", encoding="utf-8") as fh:
        for i, rel in enumerate(rels, 1):
            print(f"[HASH] {i}/{len(rels)}: {rel}")
            h = sha256(os.path.join(root, rel.replace("/", os.sep)))
            hashes[rel] = h
            fh.write(f"{h}  *{rel}\n")
    return hashes

# ---------------- compare ----------------

def compare(src: dict, dest: dict):
    keys = set(src) | set(dest)
    return {
        "matched": len([k for k in keys if src.get(k) == dest.get(k)]),
        "mismatched": len([k for k in keys if k in src and k in dest and src[k] != dest[k]]),
        "missing_in_destination": len([k for k in src if k not in dest]),
        "extra_in_destination": len([k for k in dest if k not in src]),
    }

# ---------------- manifest ----------------

def write_manifest(dest_root, stamp, tech, dest_hashes, verify, src_chk, dest_chk):
    lm = ensure_legacy_dir(dest_root)
    path = os.path.join(lm, f"manifest_{stamp}.json")

    overall = "PASS" if all(verify[k] == 0 for k in verify if k != "matched") else "FAIL"

    manifest = {
        "technician": tech,
        "created_at": now_stamp()[1],
        "result": overall,
        "checksums": {
            "source": src_chk,
            "destination": dest_chk
        },
        "verification": verify,
        "file_count": len(dest_hashes)
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"[MANIFEST] {path}")

# ---------------- main ALL flow ----------------

def run_all():
    tech = prompt("Technician name: ")
    src = normalize_path(prompt("Source top folder: "))
    dest_parent = normalize_path(prompt("Destination parent: "))

    stamp, _ = now_stamp()

    # SOURCE CHECKSUMS
    src_lm = ensure_legacy_dir(src)
    src_chk = os.path.join(src_lm, f"checksums_source_{stamp}.txt")
    src_files = list_files(src)
    src_hashes = write_checksums(src, src_files, src_chk)

    # COPY
    dest_root, copied = copy_tree(src, dest_parent)

    # DEST CHECKSUMS
    dest_lm = ensure_legacy_dir(dest_root)
    dest_chk = os.path.join(dest_lm, f"checksums_dest_{stamp}.txt")
    dest_hashes = write_checksums(dest_root, copied, dest_chk)

    # VERIFY
    verify = compare(src_hashes, dest_hashes)
    print("[VERIFY]", verify)

    # MANIFEST
    write_manifest(dest_root, stamp, tech, dest_hashes, verify,
                   os.path.basename(src_chk), os.path.basename(dest_chk))

    print("[DONE]")

if __name__ == "__main__":
    run_all()
