#!/usr/bin/env python3
"""
generate_checksums_manifest.py

Prompt for technician and package root. Compute SHA-256 checksums for every file
under the package root (paths relative to package root), write checksums.txt,
a JSON manifest, and a plain-text receipt under metadata/submissionDocumentation/.

Safe: does not modify or delete existing files (it writes new/overwrites the outputs).
"""
from __future__ import annotations
import os, sys, hashlib, json, shlex, traceback
from datetime import datetime

CHUNK_SIZE = 16 * 1024 * 1024

def normalize_path(p: str) -> str:
    if p is None: return None
    s = p.strip()
    try:
        toks = shlex.split(s)
        if toks: s = toks[0]
    except Exception:
        pass
    return os.path.abspath(os.path.expanduser(s))

def prompt(text: str, default: str="") -> str:
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
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            try:
                size = os.path.getsize(full)
            except OSError:
                size = None
            recs.append({"rel_path": rel, "size": size})
    return recs

def compute_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk: break
            h.update(chunk)
    return h.hexdigest()

def write_checksums(checksums_path: str, base_dir: str, relpaths):
    with open(checksums_path, "w", encoding="utf-8") as fh:
        for rel in sorted(relpaths):
            absfp = os.path.join(base_dir, rel.replace("/", os.sep))
            try:
                sha = compute_sha256(absfp)
            except Exception as e:
                sha = None
                print(f"WARNING: failed to hash {rel}: {e}", file=sys.stderr)
            if sha:
                fh.write(f"{sha}  *{rel}\n")
            else:
                fh.write(f"ERROR_NO_SHA  *{rel}\n")
    return checksums_path

def write_manifest(manifest_path: str, manifest_obj: dict):
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest_obj, fh, indent=2)

def write_receipt(path: str, package_name: str, technician: str, created_at: str, file_count: int, checksums_rel: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    lines = [
        "Transfer receipt",
        f"Package: {package_name}",
        f"Technician: {technician}",
        f"Created at: {created_at}",
        f"Files hashed: {file_count}",
        f"Checksums file: {checksums_rel}",
    ]
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

def main():
    try:
        tech = prompt("Technician name: ")
        if not tech:
            print("Technician name required. Aborting."); return

        pkg_raw = prompt("Package root directory (the copied top folder): ")
        pkg = normalize_path(pkg_raw)
        if not pkg or not os.path.isdir(pkg):
            print("ERROR: package root not found or not a directory:", pkg); return

        print("Scanning files under:", pkg)
        files = list_files_recursive(pkg)
        relpaths = [r["rel_path"] for r in files]
        total = len(relpaths)
        if total == 0:
            print("No files found under package root. Aborting."); return
        print(f"Found {total} files. Starting checksum pass (SHA-256). This may take a while.")

        created_iso, stamp = now_stamps()
        checksums_path = os.path.join(pkg, "checksums.txt")
        write_checksums(checksums_path, pkg, relpaths)
        print("Checksums written to:", checksums_path)

        # assemble manifest
        manifest_dir = os.path.join(pkg, "metadata", "submissionDocumentation")
        manifest_path = os.path.join(manifest_dir, f"package_manifest_{stamp}.json")

        # read checksums into dict
        file_shas = {}
        with open(checksums_path, "r", encoding="utf-8") as fh:
            for line in fh:
                parts = line.strip().split()
                if not parts: continue
                sha = parts[0]
                rel = parts[-1]
                if rel.startswith("*"): rel = rel[1:]
                file_shas[rel] = sha

        manifest = {
            "package_name": os.path.basename(pkg.rstrip(os.sep)),
            "technician": tech,
            "created_at": created_iso,
            "package_root": pkg,
            "file_count": total,
            "files": [{"rel_path": r["rel_path"], "size": r["size"], "sha256": file_shas.get(r["rel_path"])} for r in files],
            "status": {"checksums_created": True, "overall": "READY"}
        }
        write_manifest(manifest_path, manifest)
        print("Manifest written to:", manifest_path)

        receipt_path = os.path.join(manifest_dir, f"transfer_receipt_{stamp}.txt")
        write_receipt(receipt_path, manifest["package_name"], tech, created_iso, total, os.path.relpath(checksums_path, pkg))
        print("Receipt written to:", receipt_path)

        print("\nDone.")
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception:
        traceback.print_exc()

if __name__ == "__main__":
    main()
