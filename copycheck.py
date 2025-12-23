#!/usr/bin/env python3
"""
copycheck.py

Interactive-first toolkit. 'all' flow:
  - compute source checksums (checksums_source_<stamp>.txt) in source root
  - copy top folder into destination parent (EXCLUDING checksums_source_*.txt)
  - compute destination checksums (checksums_dest_<stamp>.txt) in destination package root
  - compare source vs destination checksums
  - write manifest_<stamp>.json in destination package root (includes verification summary,
    references to the two checksum filenames and SHA-256 of those checksum files)

No transfer_receipt files are created (manifest-only).
"""
from __future__ import annotations
import os, sys, shlex, argparse, json, hashlib, time, traceback, glob
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

CHUNK_SIZE = 16 * 1024 * 1024
DEFAULT_PROGRESS_INTERVAL = 100
DEFAULT_COPY_INTERVAL = 50

# Files/prefixes to exclude from copy (so operational files in source don't get copied)
EXCLUDE_PREFIXES = ("checksums_source_",)

# ---------------- utilities ----------------

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

# ---------------- copy with progress (EXCLUDES EXCLUDE_PREFIXES) ----------------

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

def copy_include_top_with_progress(src: str, dest_parent: str, copy_interval: int = DEFAULT_COPY_INTERVAL):
    """
    Copy src top folder into dest_parent (dest_parent/top_name) while printing progress per file.
    Excludes files whose filename starts with any prefix in EXCLUDE_PREFIXES.
    Returns final_dest and list of relpaths copied.
    """
    top_name = os.path.basename(src.rstrip(os.sep))
    final_dest = os.path.join(dest_parent, top_name)
    os.makedirs(final_dest, exist_ok=True)

    # Build explicit list of relative file paths to copy, excluding operational files
    files_to_copy = []
    for dirpath, _, filenames in os.walk(src):
        for fn in filenames:
            if any(fn.startswith(p) for p in EXCLUDE_PREFIXES):
                # skip adding to files_to_copy so it won't be counted or copied
                continue
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, src).replace(os.sep, "/")
            files_to_copy.append(rel)

    total = len(files_to_copy)
    print(f"[COPY] Target (final): {final_dest}")
    print(f"[COPY] Total files to copy (excluding operational files): {total}")

    copied = []
    start = time.time()
    for i, rel_fp in enumerate(files_to_copy, start=1):
        src_fp = os.path.join(src, rel_fp.replace("/", os.sep))
        dst_fp = os.path.join(final_dest, rel_fp.replace("/", os.sep))
        # ensure destination dir
        os.makedirs(os.path.dirname(dst_fp), exist_ok=True)
        print(f"[COPY] {i}/{total}: {rel_fp}")
        try:
            safe_copy_file(src_fp, dst_fp)
            copied.append(rel_fp)
        except Exception as e:
            print(f"[COPY][ERROR] failed to copy {rel_fp}: {e}")
        if i % copy_interval == 0 or i == total:
            elapsed = time.time() - start
            avg = elapsed / i if i else 0
            remaining = total - i
            eta = remaining * avg
            elapsed_h = time.strftime("%H:%M:%S", time.gmtime(elapsed))
            eta_h = time.strftime("%H:%M:%S", time.gmtime(max(0, eta)))
            print(f"[COPY] Progress: {i}/{total} — elapsed {elapsed_h}, ETA {eta_h}")
    print("[COPY] Done.")
    return final_dest, copied

# ---------------- checksums (streamed) ----------------

def compute_sha256_stream(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def write_checksums_file(pkg_root: str, relpaths: list, checksums_path: str, interval: int = DEFAULT_PROGRESS_INTERVAL):
    total = len(relpaths)
    shas = {}
    start = time.time()
    # create file and write header (visible immediately)
    with open(checksums_path, "w", encoding="utf-8") as fh:
        fh.write(f"# checksums file started at {datetime.now().isoformat(timespec='seconds')}\n")
        fh.write(f"# package: {os.path.basename(pkg_root.rstrip(os.sep))}\n")
        fh.flush()
    with open(checksums_path, "a", encoding="utf-8") as fh:
        for i, rel in enumerate(relpaths, start=1):
            print(f"[HASH] {i}/{total}: {rel}")
            absfp = os.path.join(pkg_root, rel.replace("/", os.sep))
            try:
                sha = compute_sha256_stream(absfp)
                shas[rel] = sha
                fh.write(f"{sha}  *{rel}\n")
            except Exception as e:
                shas[rel] = None
                fh.write(f"ERROR_NO_SHA  *{rel}\n")
                print(f"[HASH][ERROR] {rel}: {e}")
            fh.flush()
            if i % interval == 0 or i == total:
                elapsed = time.time() - start
                avg = elapsed / i if i else 0
                remaining = total - i
                eta = remaining * avg
                elapsed_h = time.strftime("%H:%M:%S", time.gmtime(elapsed))
                eta_h = time.strftime("%H:%M:%S", time.gmtime(max(0, eta)))
                print(f"[HASH] Progress: {i}/{total} — elapsed {elapsed_h}, ETA {eta_h}")
    return shas

# ---------------- compute sha for checksum files themselves ----------------

def compute_small_file_sha256(path: str) -> str:
    """Compute SHA-256 for a (text) file; streaming read to be safe on larger files."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

# ---------------- compare & reporting ----------------

def compare_shas(src_shas: dict, dest_shas: dict):
    src_keys = set(src_shas.keys())
    dest_keys = set(dest_shas.keys())
    all_keys = sorted(src_keys.union(dest_keys))

    matched = []
    mismatched = []
    missing_in_dest = []
    extra_in_dest = []

    for k in all_keys:
        s = src_shas.get(k)
        d = dest_shas.get(k)
        if s is None and d is None:
            continue
        if s is None:
            extra_in_dest.append(k)
        elif d is None:
            missing_in_dest.append(k)
        elif s == d:
            matched.append(k)
        else:
            mismatched.append(k)

    summary = {
        "matched": len(matched),
        "mismatched": len(mismatched),
        "missing_in_destination": len(missing_in_dest),
        "extra_in_destination": len(extra_in_dest),
        "mismatch_sample": mismatched[:200],
        "missing_sample": missing_in_dest[:200],
        "extra_sample": extra_in_dest[:200],
    }
    return summary

# ---------------- manifest (manifest-only; includes checksum-file sha256s) ----------------

def write_manifest(pkg_root: str, stamp: str, tech: str, items: list, dest_shas: dict,
                   verification_summary: dict, src_checksums_name: str, dest_checksums_name: str,
                   src_checksums_hash: str = None, dest_checksums_hash: str = None):
    manifest_name = f"manifest_{stamp}.json"
    manifest_path = os.path.join(pkg_root, manifest_name)
    created_iso = datetime.now().isoformat(timespec="seconds")

    files_section = []
    for item in sorted(items, key=lambda r: r["rel_path"]):
        rel = item["rel_path"]
        files_section.append({
            "rel_path": rel,
            "size": item.get("size"),
            "sha256": dest_shas.get(rel)
        })

    manifest = {
        "package_name": os.path.basename(pkg_root.rstrip(os.sep)),
        "technician": tech,
        "created_at": created_iso,
        "stamp": stamp,
        "file_count": len(files_section),
        "files": files_section,
        "checksums": {
            "source": {"file": src_checksums_name, "sha256": src_checksums_hash},
            "destination": {"file": dest_checksums_name, "sha256": dest_checksums_hash}
        },
        "verification": verification_summary,
            overall_status = "PASS" if (
            verification_summary["mismatched"] == 0 and
            verification_summary["missing_in_destination"] == 0 and
            verification_summary["extra_in_destination"] == 0
        ) else "FAIL"
    }
    with open(manifest_path, "w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)
    print(f"[MANIFEST] Wrote manifest: {manifest_path}")
    return manifest_path

# ---------------- top-level 'all' flow ----------------

def run_all_flow(interactive_args):
    tech = interactive_args.technician
    src = normalize_path(interactive_args.source or prompt("Source top folder (drag/paste or type path): "))
    if not src or not os.path.isdir(src):
        print("ERROR: invalid source:", src); return 2
    dest_parent = normalize_path(interactive_args.dest_parent or prompt("Destination parent (drag/paste or type path): "))
    if not dest_parent or not os.path.isdir(dest_parent):
        print("ERROR: invalid destination parent:", dest_parent); return 3
    top_name = os.path.basename(src.rstrip(os.sep))
    final_dest = os.path.join(dest_parent, top_name)
    if os.path.exists(final_dest) and not interactive_args.force:
        print(f"[ALL] Destination exists: {final_dest}")
        confirm = prompt("Type 'YES' to proceed and allow overwrites, or Enter to cancel: ")
        if confirm != "YES":
            print("Cancelled."); return 4

    stamp, created_iso = now_stamp()

    # 1) source checksums BEFORE copy
    print("[ALL] Creating source checksums BEFORE copy...")
    items_src = list_files_recursive(src)
    rels_src = [r["rel_path"] for r in items_src]
    src_checksums_name = f"checksums_source_{stamp}.txt"
    src_checksums_path = os.path.join(src, src_checksums_name)
    src_shas = write_checksums_file(src, rels_src, src_checksums_path, interval=interactive_args.progress_interval)
    print(f"[ALL] Source checksums written: {src_checksums_path}")

    # Compute SHA256 of the source checksums file itself
    try:
        src_checksums_hash = compute_small_file_sha256(src_checksums_path)
        print(f"[ALL] Source checksums file sha256: {src_checksums_hash}")
    except Exception as e:
        src_checksums_hash = None
        print(f"[ALL][WARN] Could not compute hash of source checksum file: {e}")

    # 2) copy (EXCLUDING checksums_source_* files)
    print("[ALL] Copying source to destination (excluding source checksums) ...")
    final_dest_path, copied_relpaths = copy_include_top_with_progress(src, dest_parent, copy_interval=interactive_args.progress_copy_interval)
    print(f"[ALL] Copy completed into: {final_dest_path}")

    # 3) destination checksums AFTER copy
    print("[ALL] Creating destination checksums AFTER copy...")
    dest_checksums_name = f"checksums_dest_{stamp}.txt"
    dest_checksums_path = os.path.join(final_dest_path, dest_checksums_name)
    dest_shas = write_checksums_file(final_dest_path, copied_relpaths, dest_checksums_path, interval=interactive_args.progress_interval)
    print(f"[ALL] Destination checksums written: {dest_checksums_path}")

    # Compute SHA256 of the destination checksums file itself
    try:
        dest_checksums_hash = compute_small_file_sha256(dest_checksums_path)
        print(f"[ALL] Destination checksums file sha256: {dest_checksums_hash}")
    except Exception as e:
        dest_checksums_hash = None
        print(f"[ALL][WARN] Could not compute hash of destination checksum file: {e}")

    # 4) compare
    print("[ALL] Comparing source and destination checksums...")
    verification_summary = compare_shas(src_shas, dest_shas)

    # 5) write manifest into destination root (manifest_<stamp>.json) - includes verification summary and checksum file hashes
    items_dest = list_files_recursive(final_dest_path)
    manifest_path = write_manifest(final_dest_path, stamp, tech, items_dest, dest_shas, verification_summary, src_checksums_name, dest_checksums_name, src_checksums_hash, dest_checksums_hash)

    # 6) final status
    problems = verification_summary["mismatched"] + verification_summary["missing_in_destination"] + verification_summary["extra_in_destination"]
    if problems == 0:
        print("[ALL] Verification OK: all files matched.")
        return 0
    else:
        print(f"[ALL] Verification found issues: mismatched={verification_summary['mismatched']}, missing_in_destination={verification_summary['missing_in_destination']}, extra_in_destination={verification_summary['extra_in_destination']}")
        print(f"[ALL] See manifest in {final_dest_path}")
        return 3

# ---------------- CLI + interactive ----------------

def make_parser():
    p = argparse.ArgumentParser(description="copycheck: copy + checksums + manifest + verify (interactive-first). 'all' does pre-copy source checksums, copy (excluding source checksum file), post-copy dest checksums, compare, manifest.")
    sub = p.add_subparsers(dest="cmd")

    p_all = sub.add_parser("all", help="Run full flow: source-checksum -> copy (exclude source checksums) -> dest-checksum -> compare -> manifest")
    p_all.add_argument("--source", "-s", help="Source top folder")
    p_all.add_argument("--dest-parent", "-d", help="Destination parent folder")
    p_all.add_argument("--technician", "-t", help="Technician name (if not provided interactive will prompt)")
    p_all.add_argument("--force", "-f", action="store_true", help="Allow overwriting destination")
    p_all.add_argument("--progress-copy-interval", type=int, default=DEFAULT_COPY_INTERVAL)
    p_all.add_argument("--progress-interval", "-n", type=int, default=DEFAULT_PROGRESS_INTERVAL)
    p_all.set_defaults(func_cmd="all")

    # lightweight copy/checksum/verify commands retained (but 'all' is recommended)
    p_copy = sub.add_parser("copy", help="Just copy (interactive prompts). Excludes checksums_source_* files.")
    p_copy.add_argument("--source", "-s")
    p_copy.add_argument("--dest-parent", "-d")
    p_copy.add_argument("--force", "-f", action="store_true")
    p_copy.add_argument("--progress-copy-interval", type=int, default=DEFAULT_COPY_INTERVAL)
    p_copy.set_defaults(func_cmd="copy")

    p_checksum = sub.add_parser("checksum", help="Generate checksums in an existing package root.")
    p_checksum.add_argument("--package", "-p")
    p_checksum.add_argument("--progress-interval", "-n", type=int, default=DEFAULT_PROGRESS_INTERVAL)
    p_checksum.set_defaults(func_cmd="checksum")

    p_verify = sub.add_parser("verify", help="Compare existing source and dest checksum files (or compute if missing).")
    p_verify.add_argument("--source", "-s")
    p_verify.add_argument("--dest", "-d")
    p_verify.add_argument("--progress-interval", "-n", type=int, default=DEFAULT_PROGRESS_INTERVAL)
    p_verify.set_defaults(func_cmd="verify")

    return p

def interactive_menu():
    print("copycheck — interactive menu")
    print("  1) all (recommended): compute source checksums, copy (excl. source checksums), compute dest checksums, compare, manifest")
    print("  2) copy (just copy, excludes source checksum files)")
    print("  3) checksum (generate checksums in a package root)")
    print("  4) verify (compare source vs dest using checksum files)")
    print("  5) quit")
    return prompt("Enter 1/2/3/4/5: ")

def main():
    parser = make_parser()
    args = parser.parse_args()

    if args.cmd is None:
        tech = prompt("Technician name (will be reused): ")
        if not tech:
            print("Technician required. Exiting.")
            return 1
        while True:
            choice = interactive_menu()
            if choice in ("1", "all"):
                class A: pass
                a = A()
                a.source = None
                a.dest_parent = None
                a.technician = tech
                a.force = False
                a.progress_copy_interval = DEFAULT_COPY_INTERVAL
                a.progress_interval = DEFAULT_PROGRESS_INTERVAL
                rc = run_all_flow(a)
                input("Press Enter to return to menu...")
            elif choice in ("2", "copy"):
                src = normalize_path(prompt("Source top folder: "))
                dest_parent = normalize_path(prompt("Destination parent: "))
                copy_include_top_with_progress(src, dest_parent, copy_interval=DEFAULT_COPY_INTERVAL)
                input("Press Enter to return to menu...")
            elif choice in ("3", "checksum"):
                pkg = normalize_path(prompt("Package root (top folder): "))
                if not pkg or not os.path.isdir(pkg):
                    print("Invalid package root."); continue
                items = list_files_recursive(pkg)
                rels = [r["rel_path"] for r in items]
                stamp, _ = now_stamp()
                name = f"checksums_manual_{stamp}.txt"
                path = os.path.join(pkg, name)
                write_checksums_file(pkg, rels, path, interval=DEFAULT_PROGRESS_INTERVAL)
                print("Checksums written:", path)
                input("Press Enter to return to menu...")
            elif choice in ("4", "verify"):
                src = normalize_path(prompt("Source top folder: "))
                dest = normalize_path(prompt("Destination top folder: "))
                if not src or not dest:
                    print("Invalid paths."); continue
                # find or compute checksums for both and compare (interactive)
                src_existing = sorted(glob.glob(os.path.join(src, "checksums_*.txt")))
                if src_existing:
                    use = prompt(f"Found source checksums {os.path.basename(src_existing[-1])}. Use it? [Y/n]: ", "Y")
                    if use.lower() in ("", "y", "yes"):
                        src_path = src_existing[-1]
                        src_shas = {}
                        with open(src_path, "r", encoding="utf-8") as fh:
                            for line in fh:
                                if not line or line.startswith("#"): continue
                                parts = line.strip().split()
                                if len(parts) >= 2:
                                    sha = parts[0]; rel = parts[-1]
                                    if rel.startswith("*"): rel = rel[1:]
                                    src_shas[rel] = sha
                    else:
                        src_path = None
                else:
                    src_path = None
                if not src_path:
                    items_src = list_files_recursive(src)
                    rels_src = [r["rel_path"] for r in items_src]
                    stamp_src, _ = now_stamp()
                    src_name = f"checksums_source_{stamp_src}.txt"
                    src_path = os.path.join(src, src_name)
                    src_shas = write_checksums_file(src, rels_src, src_path, interval=DEFAULT_PROGRESS_INTERVAL)
                dest_existing = sorted(glob.glob(os.path.join(dest, "checksums_*.txt")))
                if dest_existing:
                    use = prompt(f"Found dest checksums {os.path.basename(dest_existing[-1])}. Use it? [Y/n]: ", "Y")
                    if use.lower() in ("", "y", "yes"):
                        dest_path = dest_existing[-1]
                        dest_shas = {}
                        with open(dest_path, "r", encoding="utf-8") as fh:
                            for line in fh:
                                if not line or line.startswith("#"): continue
                                parts = line.strip().split()
                                if len(parts) >= 2:
                                    sha = parts[0]; rel = parts[-1]
                                    if rel.startswith("*"): rel = rel[1:]
                                    dest_shas[rel] = sha
                    else:
                        dest_path = None
                else:
                    dest_path = None
                if not dest_path:
                    items_dest = list_files_recursive(dest)
                    rels_dest = [r["rel_path"] for r in items_dest]
                    stamp_dest, _ = now_stamp()
                    dest_name = f"checksums_dest_{stamp_dest}.txt"
                    dest_path = os.path.join(dest, dest_name)
                    dest_shas = write_checksums_file(dest, rels_dest, dest_path, interval=DEFAULT_PROGRESS_INTERVAL)
                summary = compare_shas(src_shas, dest_shas)
                print("Verification summary:", summary)
                input("Press Enter to return to menu...")
            elif choice in ("5", "quit", "q", "exit"):
                print("Goodbye."); return 0
            else:
                print("Invalid choice.")
    else:
        func = getattr(args, "func_cmd", None)
        if func == "all":
            class A: pass
            a = A()
            a.source = args.source
            a.dest_parent = args.dest_parent
            a.technician = args.technician
            a.force = args.force
            a.progress_copy_interval = args.progress_copy_interval
            a.progress_interval = args.progress_interval
            return run_all_flow(a)
        elif func == "copy":
            src = normalize_path(args.source or prompt("Source top folder: "))
            dest_parent = normalize_path(args.dest_parent or prompt("Destination parent: "))
            copy_include_top_with_progress(src, dest_parent, copy_interval=args.progress_copy_interval)
            return 0
        elif func == "checksum":
            pkg = normalize_path(args.package or prompt("Package root: "))
            if not pkg or not os.path.isdir(pkg):
                print("Invalid package"); return 2
            items = list_files_recursive(pkg)
            rels = [r["rel_path"] for r in items]
            stamp, _ = now_stamp()
            name = f"checksums_manual_{stamp}.txt"
            path = os.path.join(pkg, name)
            write_checksums_file(pkg, rels, path, interval=args.progress_interval)
            return 0
        elif func == "verify":
            print("Non-interactive verify is limited; use interactive verify or 'all' for automated flow.")
            return 1
        else:
            parser.print_help()
            return 1

if __name__ == "__main__":
    try:
        rc = main()
        if isinstance(rc, int):
            sys.exit(rc)
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        sys.exit(1)
    except Exception:
        traceback.print_exc()
        sys.exit(2)
