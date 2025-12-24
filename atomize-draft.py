#!/usr/bin/env python3
"""
atomize_all.py

Interactive packager: moves originals into data/objects/, moves *_legacy_* into
data/submissionDocumentation/, writes metadata.csv, optionally filemap.csv,
and generates mets.xml (package root + copy into data/metadata/).

Usage:
  python3 atomize_all.py --dry-run      # preview only
  python3 atomize_all.py --execute      # actually perform moves and write METS
"""
from pathlib import Path
import argparse, shutil, csv, logging, sys, fnmatch
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, List, Dict

# Logging
LOGFILE = Path.cwd() / "atomize_all.log"
logging.basicConfig(filename=str(LOGFILE), level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s: %(message)s")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(console)

# METS namespaces
NS = {"mets": "http://www.loc.gov/METS/", "xlink": "http://www.w3.org/1999/xlink", "dc": "http://purl.org/dc/elements/1.1/"}
for pfx, uri in NS.items():
    ET.register_namespace(pfx, uri)

METADATA_FIELDS = ["packageName","technician","identifier","title","eventDateStart","eventDateEnd","conditionsGoverningAccess"]
FILEMAP_HEADERS = ["filename","relative_path","original_path"]

def ask(prompt: str, default: Optional[str]=None) -> str:
    if default is None:
        return input(f"{prompt}: ").strip()
    else:
        r = input(f"{prompt} [{default}]: ").strip()
        return r if r else default

def yesno(prompt: str, default: bool = True) -> bool:
    d = "Y/n" if default else "y/N"
    resp = input(f"{prompt} ({d}): ").strip().lower()
    if not resp:
        return default
    return resp in ("y","yes","yep")

def ensure_dir(p: Path, dry: bool):
    if dry:
        logging.info(f"[DRY] Would create: {p}")
    else:
        p.mkdir(parents=True, exist_ok=True)
        logging.debug(f"ensure_dir: {p}")

def move_or_copy(src: Path, dst_dir: Path, dry: bool) -> Dict[str,str]:
    """
    Try shutil.move; on failure use copy then delete. Return filemap row.
    """
    dst = dst_dir / src.name
    logging.info(f"{'[DRY] Would move' if dry else 'Moving'}: {src} -> {dst}")
    if dry:
        return {"filename": src.name, "relative_path": str(Path("data") / "objects" / src.name).replace("\\","/"), "original_path": str(src)}
    dst_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(src), str(dst))
    except Exception as e:
        logging.warning(f"shutil.move failed ({e}), attempting copy fallback for {src} -> {dst}")
        try:
            if src.is_dir():
                if dst.exists():
                    for child in src.iterdir():
                        target = dst / child.name
                        if child.is_dir():
                            shutil.copytree(str(child), str(target), dirs_exist_ok=True)
                        else:
                            shutil.copy2(str(child), str(target))
                else:
                    shutil.copytree(str(src), str(dst))
                shutil.rmtree(str(src))
            else:
                shutil.copy2(str(src), str(dst))
                src.unlink()
        except Exception as e2:
            logging.error(f"Copy fallback failed for {src}: {e2}")
            raise
    logging.info(f"Moved: {src} -> {dst}")
    return {"filename": dst.name, "relative_path": str(dst.relative_to(dst.parents[1] if len(dst.parents)>=2 else dst.parent)).replace("\\","/"), "original_path": str(src)}

def find_legacy_folders(root: Path) -> List[Path]:
    """Return list of top-level or common nested folders matching '*_legacy_*' (case-insensitive)."""
    results = []
    # top-level
    for item in root.iterdir():
        if item.is_dir() and fnmatch.fnmatch(item.name.lower(), "*_legacy_*"):
            results.append(item)
    # check data/ and metadata/
    for p in (root/"data", root/"metadata"):
        if p.exists() and p.is_dir():
            for item in p.iterdir():
                if item.is_dir() and fnmatch.fnmatch(item.name.lower(), "*_legacy_*"):
                    results.append(item)
    return results

def write_csv_rows(path: Path, headers: List[str], rows: List[Dict[str,str]], dry: bool):
    if dry:
        logging.info(f"[DRY] Would write CSV: {path} ({len(rows)} rows)")
        for r in rows[:10]:
            logging.info(f"  [DRY] {r}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    logging.info(f"Wrote CSV: {path}")

def build_mets(package_root: Path, object_relpaths: List[Path], metadata_row: Dict[str,str], dry: bool) -> Path:
    mets_ns = NS["mets"]; xlink_ns = NS["xlink"]; dc_ns = NS["dc"]
    ET.register_namespace("mets", mets_ns); ET.register_namespace("xlink", xlink_ns); ET.register_namespace("dc", dc_ns)
    mets_root = ET.Element(ET.QName(mets_ns, "mets"), {"TYPE": "SIP"})
    metsHdr = ET.SubElement(mets_root, ET.QName(mets_ns, "metsHdr"), {"CREATEDATE": datetime.utcnow().isoformat()})
    agent = ET.SubElement(metsHdr, ET.QName(mets_ns, "agent"), {"ROLE": "CREATOR", "TYPE": "OTHER"})
    ET.SubElement(agent, ET.QName(mets_ns,"name")).text = "atomize_all.py"
    # dmdSecs
    for idx, rel in enumerate(object_relpaths, start=1):
        dmdSec = ET.SubElement(mets_root, ET.QName(mets_ns, "dmdSec"), {"ID": f"dmd_{idx}"})
        mdWrap = ET.SubElement(dmdSec, ET.QName(mets_ns, "mdWrap"), {"MDTYPE":"DC"})
        xmlData = ET.SubElement(mdWrap, ET.QName(mets_ns, "xmlData"))
        ET.SubElement(xmlData, ET.QName(dc_ns, "title")).text = f"{metadata_row.get('title','')} — {rel.name}"
        if metadata_row.get("identifier"):
            ET.SubElement(xmlData, ET.QName(dc_ns, "identifier")).text = f"{metadata_row.get('identifier','')}-{rel.stem}"
        if metadata_row.get("eventDateStart"):
            ET.SubElement(xmlData, ET.QName(dc_ns, "date")).text = metadata_row.get("eventDateStart")
        if metadata_row.get("conditionsGoverningAccess"):
            ET.SubElement(xmlData, ET.QName(dc_ns, "rights")).text = metadata_row.get("conditionsGoverningAccess")
    # fileSec / fileGrp
    fileSec = ET.SubElement(mets_root, ET.QName(mets_ns, "fileSec"))
    fileGrp = ET.SubElement(fileSec, ET.QName(mets_ns, "fileGrp"), {"USE":"OBJECTS"})
    for idx, rel in enumerate(object_relpaths, start=1):
        fileElem = ET.SubElement(fileGrp, ET.QName(mets_ns, "file"), {"ID": f"file_{idx}"})
        ET.SubElement(fileElem, ET.QName(mets_ns, "FLocat"), {"LOCTYPE":"URL", ET.QName(xlink_ns, "href"): str(Path("data")/rel).replace("\\","/")})
    # structMap
    structMap = ET.SubElement(mets_root, ET.QName(mets_ns, "structMap"), {"TYPE":"logical"})
    div_root = ET.SubElement(structMap, ET.QName(mets_ns, "div"), {"TYPE":"package", "LABEL": metadata_row.get("title","")})
    for idx, _ in enumerate(object_relpaths, start=1):
        ET.SubElement(div_root, ET.QName(mets_ns, "fptr"), {"FILEID": f"file_{idx}"})
    mets_path = package_root / "mets.xml"
    if dry:
        logging.info(f"[DRY] Would write METS: {mets_path} (objects: {len(object_relpaths)})")
        return mets_path
    tree = ET.ElementTree(mets_root)
    tree.write(str(mets_path), encoding="utf-8", xml_declaration=True, short_empty_elements=True)
    # copy into data/metadata/mets.xml
    copy_dest = package_root / "data" / "metadata" / "mets.xml"
    copy_dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(mets_path), str(copy_dest))
    logging.info(f"Wrote METS: {mets_path} and copy to {copy_dest}")
    return mets_path

def gather_objects(package_root: Path) -> List[Path]:
    objs = []
    objects_dir = package_root / "data" / "objects"
    if not objects_dir.exists():
        return objs
    for p in sorted(objects_dir.rglob("*")):
        if p.is_file():
            objs.append(p.relative_to(package_root))
    return objs

def collect_package_metadata(package_name: str, technician_default: Optional[str]=None) -> Dict[str,str]:
    print("\n--- Package metadata (minimal) ---")
    technician = ask("Technician", default=technician_default or "")
    identifier = ask("Identifier (e.g., accession number)", default="")
    title = ask("Title", default=package_name)
    event_start = ask("Event start date (YYYY-MM-DD)", default="")
    event_end = ask("Event end date (YYYY-MM-DD)", default="")
    access = ask("Conditions governing access", default="")
    return {"packageName": package_name, "technician": technician, "identifier": identifier, "title": title, "eventDateStart": event_start, "eventDateEnd": event_end, "conditionsGoverningAccess": access}

def main():
    parser = argparse.ArgumentParser(description="atomize_all: move + generate METS for Archivematica")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--execute", action="store_true", help="Perform moves and write METS (must supply to change filesystem)")
    args = parser.parse_args()
    dry = args.dry_run and not args.execute
    if args.execute:
        dry = False

    logging.info("Starting atomize_all (dry=%s, execute=%s)", dry, args.execute)
    # technician first
    tech = ask("Technician name (enter your name first)")
    # package path
    while True:
        top_in = ask("Path to the folder you already have (package root)")
        pkg = Path(top_in).expanduser().resolve()
        if not pkg.exists() or not pkg.is_dir():
            logging.error("Invalid path: %s", pkg)
            if yesno("Try again?", True):
                continue
            else:
                sys.exit(1)
        break

    # choose use-as or rename/create new package sibling
    print("\nHow to treat this path:")
    print("  1) Use this path itself as the package (no rename)")
    print("  2) Create a new package folder (rename) as a sibling and move content into it")
    choice = ask("Enter 1 or 2", default="1").strip()
    if choice == "2":
        parent = pkg.parent
        newname = ask("New package folder name", default=pkg.name)
        package_dir = parent / newname
        if not dry:
            package_dir.mkdir(parents=True, exist_ok=True)
        source_root = pkg
        target_root = package_dir
    else:
        package_dir = pkg
        source_root = pkg
        target_root = package_dir

    logging.info("Package directory: %s", package_dir)
    # create canonical dirs
    data_root = target_root / "data"
    objects_dir = data_root / "objects"
    metadata_dir = data_root / "metadata"
    submission_dir = data_root / "submissionDocumentation"
    for d in (data_root, objects_dir, metadata_dir, submission_dir):
        ensure_dir(d, dry)

    # find and move legacy folders
    legacy_list = find_legacy_folders(source_root)
    if legacy_list:
        logging.info("Legacy folders found: %s", legacy_list)
        for cand in legacy_list:
            move_or_copy(cand, submission_dir, dry)
    else:
        logging.info("No '*_legacy_*' folders found.")

    # move top-level items into data/objects (preserve top-level data and metadata)
    filemap_rows = []
    preserve = {"data", "metadata", ".git", ".gitignore"}
    existing_inner_objects = source_root / "data" / "objects"
    if existing_inner_objects.exists() and existing_inner_objects.is_dir() and source_root.resolve() == package_dir.resolve():
        logging.info("Detected existing data/objects in package; will not bulk-move top-level items.")
        # gather list of existing objects
        for p in sorted(existing_inner_objects.rglob("*")):
            if p.is_file():
                filemap_rows.append({"filename": p.name, "relative_path": str(p.relative_to(package_dir)).replace("\\","/"), "original_path": str(p)})
    else:
        for item in sorted(source_root.iterdir()):
            # skip canonical data or metadata folders to avoid nesting
            if item.name == "data":
                logging.info("Processing top-level 'data' folder contents to avoid nesting.")
                inner_objs = item / "objects"
                if inner_objs.exists() and inner_objs.is_dir():
                    for p in sorted(inner_objs.iterdir()):
                        row = move_or_copy(p, objects_dir, dry)
                        filemap_rows.append(row)
                # move any files directly under data/ into objects/ (rare)
                for p in sorted(item.iterdir()):
                    if p.is_file():
                        row = move_or_copy(p, objects_dir, dry)
                        filemap_rows.append(row)
                continue
            if item.name in preserve:
                logging.info("Preserving top-level: %s", item.name)
                # scan metadata for potential misplaced binaries
                if item.name == "metadata":
                    for p in sorted(item.iterdir()):
                        if p.is_file() and p.suffix.lower() not in (".csv",".xml",".json",".md",".txt"):
                            logging.info("Moving likely original from metadata/: %s", p)
                            row = move_or_copy(p, objects_dir, dry)
                            filemap_rows.append(row)
                continue
            # avoid moving package_dir into itself
            try:
                if item.resolve() == package_dir.resolve():
                    logging.info("Skipping package dir itself: %s", item)
                    continue
            except Exception:
                pass
            row = move_or_copy(item, objects_dir, dry)
            filemap_rows.append(row)

    # Determine whether to write a filemap.csv
    write_filemap = yesno("Create data/metadata/filemap.csv? (contains filename, relative_path, original_path)", default=False)
    # build authoritative object_relpaths
    object_relpaths = []
    for p in sorted(objects_dir.rglob("*")):
        if p.is_file():
            object_relpaths.append(p.relative_to(package_dir))

    # collect or read metadata
    # attempt to read existing metadata.csv under data/metadata/
    existing_meta_csv = metadata_dir / "metadata.csv"
    metadata_row = None
    if existing_meta_csv.exists():
        try:
            with existing_meta_csv.open(newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for r in reader:
                    metadata_row = {k: (v if v is not None else "") for k,v in r.items()}
                    break
            logging.info("Read existing metadata.csv and will reuse it.")
        except Exception as e:
            logging.warning("Failed to read existing metadata.csv: %s", e)
            metadata_row = None
    if metadata_row is None:
        metadata_row = collect_package_metadata(package_dir.name, tech)

    # write metadata.csv into data/metadata/ (overwrite or create)
    write_csv_rows = []
    if not dry:
        metadata_dir.mkdir(parents=True, exist_ok=True)
    write_csv_rows.append(metadata_row)
    write_csv = metadata_dir / "metadata.csv"
    write_csv_rows = [metadata_row]
    write_csv_rows_func = lambda: write_csv_rows_func  # noop placeholder to keep linter happy
    write_csv_rows_path = write_csv
    write_csv_rows_func = None
    # write metadata.csv
    write_csv_rows_func = lambda path, headers, rows, dryflag: write_csv_rows(path, headers, rows, dryflag)
    write_csv_rows_func(write_csv, METADATA_FIELDS, [metadata_row], dry)

    # optionally write filemap
    if write_filemap:
        fm_path = metadata_dir / "filemap.csv"
        if filemap_rows:
            write_csv_rows(fm_path, FILEMAP_HEADERS, filemap_rows, dry)
        else:
            # build from object_relpaths if filemap_rows empty
            fm_rows = []
            for rel in object_relpaths:
                fm_rows.append({"filename": rel.name, "relative_path": str(rel).replace("\\","/"), "original_path": ""})
            write_csv_rows(fm_path, FILEMAP_HEADERS, fm_rows, dry)

    # generate METS
    if not object_relpaths:
        logging.warning("No object files found under data/objects/. METS will not be generated.")
        print("No files found under data/objects/. METS not generated.")
    else:
        build_mets(package_dir, object_relpaths, metadata_row, dry)

    logging.info("Done. Log saved to %s", LOGFILE)
    print("\nDone. Log saved to:", LOGFILE)
    if dry:
        print("This was a dry run. Re-run with --execute to perform actions.")
    else:
        print("Package ready at:", package_dir)
        print("Check data/objects/, data/metadata/metadata.csv, and mets.xml at package root (and copy under data/metadata/).")

if __name__ == "__main__":
    main()
