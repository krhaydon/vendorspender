#!/usr/bin/env python3
"""
interactive_copy_manifest.py

Interactive prompt:
  1) Technician name
  2) Source directory (top folder to copy)
  3) Destination parent directory (top folder will be created inside)

Then: copy the source top folder into destination parent, compute checksums,
write JSON manifest and a plain-text receipt inside the copied package root.

Safety: never deletes source files. If the final destination exists, script
asks for explicit confirmation to allow overwriting individual files.
"""

from __future__ import annotations
import os
import sys
import shlex
import hashlib
import json
import traceback
from datetime import datetime

CHUNK_SIZE = 16 * 1024 * 1024

def normalize_path(p: str) -> str:
    if p is None:
        return None
    s = p.strip()
    if not s:
        return s
    try:
        tokens = shlex.split(s)
        if tokens:
            s = tokens[0]
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

def now_stamps():
    t = datetime.now()
    return t.isoformat(timespec="seconds"), t.strftime("%Y%m%d_%H%M%S")

def list_files_recursive(root: str):
    recs = []
    total = 0
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            try:
                size = os.path.getsize(full)
            except OSError:
                size = None
            recs.append({"rel_path": rel, "size": size})
            total += 1
    return recs, total

def safe_copy_file(src: str, dst: str):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(src, "rb") as rf, open(dst, "wb") as wf:
        while True:
            chunk = rf.read(CHUNK_SIZE)
            if not chunk:
                break
            wf.write(chunk)
    try:
        os.chmod(dst, 0o644)
    except Exception:
        pass

def copy_include_top(src: str, dest_parent: str):
    """
    Copy source top folder into dest_parent (creates dest_parent/<top_name>).
    Returns final_dest (full path) and list of relpaths copied (relative to package root).
    """
    top_name = os.path.basename(src.rstrip(os.sep))
    final_dest = os.path.join(dest_parent, top_name)
    os.makedirs(final_dest, exist_ok=True)
    copied = []
    for dirpath, _, filenames in os.walk(src):
        rel_dir = os.path.relpath(dirpath, src)
        rel_dir_posix = "" if rel_dir == "." else rel_dir.replace(os.sep, "/")
        target_dir = os.path.join(final_dest, rel_dir_posix) if rel_dir_posix else final_dest
        os.makedirs(target_dir, exist_ok=True)
        try:
            os.chmod(target_dir, 0o755)
        except Exception:
            pass
        for fn in filenames:
            src_fp = os.path.join(dirpath, fn)
            rel_fp = (os.path.join(rel_dir, fn) if rel_dir != "." else fn).replace(os.sep, "/")
            dst_fp = os.path.join(final_dest, rel_fp)
            safe_copy_file(src_fp, dst_fp)
            copied.append(rel_fp)
    return final_dest, copied

def compute_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def write_checksums(checksums_path: str, base_dir: str, relpaths: list):
    with open(checksums_path, "w", encoding="utf-8") as f:
        for rel in sorted(relpaths):
            abs_fp = os.path.join(base_dir, rel.replace("/", os.sep))
            try:
                sha = compute_sha256(abs_fp)
            except Exception:
                sha = None
            if sha:
                f.write(f"{sha}  *{rel}\n")
            else:
                f.write(f"ERROR_NO_SHA  *{rel}\n")

def write_manifest(manifest_path: str, manifest_obj: dict):
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest_obj, f, indent=2)

def write_receipt(receipt_path: str, package_name: str, technician: str, created_at: str, file_count: int, checksums_rel: str):
    os.makedirs(os.path.dirname(receipt_path), exist_ok=True)
    lines = [
        "Transfer receipt",
        f"Package: {package_name}",
        f"Technician: {technician}",
        f"Created at: {created_at}",
        f"Files transferred: {file_count}",
        f"Checksums file: {checksums_rel}",
    ]
    with open(receipt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

def main():
    try:
        tech = prompt("1) Technician name: ")
        if not tech:
            print("Technician name is required. Aborting.")
            return

        src_raw = prompt("2) Source directory (drag/paste or type path): ")
        src = normalize_path(src_raw)
        if not src or not os.path.isdir(src):
            print("ERROR: source directory invalid or doesn't exist:", src)
            return

        dest_raw = prompt("3) Destination parent directory (drag/paste or type path): ")
        dest_parent = normalize_path(dest_raw)
        if not dest_parent or not os.path.isdir(dest_parent):
            print("ERROR: destination parent directory invalid or doesn't exist:", dest_parent)
            return

        # Prepare
        top_name = os.path.basename(src.rstrip(os.sep))
        final_dest = os.path.join(dest_parent, top_name)

        files_list, total = list_files_recursive(src)
        relpaths = [r["rel_path"] for r in files_list]

        print(f"\nFound {total} files under source '{src}'.")
        print("Planned final destination:", final_dest)

        if os.path.exists(final_dest):
            print("\nNOTICE: Final destination already exists:", final_dest)
            print("This operation will NOT delete anything. Copying into an existing destination may overwrite files with identical relative paths.")
            confirm = prompt("Type 'YES' to proceed and allow overwrites, or press Enter to cancel: ")
            if confirm != "YES":
                print("Cancelled by user.")
                return

        created_iso, stamp = now_stamps()
        print("\nCopying files (this may take a while)...")
        final_dest_path, copied_relpaths = copy_include_top(src, dest_parent)
        print(f"Copied {len(copied_relpaths)} files into: {final_dest_path}")

        # checksums
        checksums_path = os.path.join(final_dest_path, "checksums.txt")
        print("Computing checksums (sha256)...")
        write_checksums(checksums_path, final_dest_path, copied_relpaths)
        print("Checksums written to:", checksums_path)

        # manifest
        manifest_dir = os.path.join(final_dest_path, "metadata", "submissionDocumentation")
        manifest_path = os.path.join(manifest_dir, f"package_manifest_{stamp}.json")

        # read checksums into dict
        file_shas = {}
        try:
            with open(checksums_path, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split()
                    if not parts:
                        continue
                    sha = parts[0]
                    rel = parts[-1]
                    if rel.startswith("*"):
                        rel = rel[1:]
                    file_shas[rel] = sha
        except Exception:
            pass

        manifest = {
            "package_name": top_name,
            "technician": tech,
            "created_at": created_iso,
            "source_root": src,
            "final_dest": final_dest_path,
            "source_file_count": total,
            "files": [{"rel_path": rel, "size": next((r["size"] for r in files_list if r["rel_path"]==rel), None), "sha256": file_shas.get(rel)} for rel in sorted(relpaths)],
            "status": {"files_copied": True, "overall": "READY"}
        }

        write_manifest(manifest_path, manifest)
        print("Manifest written to:", manifest_path)

        # receipt
        receipt_path = os.path.join(manifest_dir, f"transfer_receipt_{stamp}.txt")
        write_receipt(receipt_path, top_name, tech, created_iso, len(copied_relpaths), os.path.relpath(checksums_path, final_dest_path))
        print("Receipt written to:", receipt_path)

        print("\nAll done. Package at:", final_dest_path)
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception:
        traceback.print_exc()
        print("ERROR: unexpected failure.")

if __name__ == "__main__":
    main()
