#!/usr/bin/env python3
"""Delete clips hand-labeled as skip (human_label=0) from the dataset.

Preview by default (read-only). Pass --execute to actually delete:
  - remove the video file (path)
  - remove the sidecar metadata/{platform}_{video_id}.json
  - set DB row status='skipped', path=NULL, download_date=NULL
  - rewrite manifest.csv from the remaining status='downloaded' rows

Do NOT run --execute while a download/scrape is writing the same DB/manifest.

  python delete_skips.py --out D:\\ytds            # preview
  python delete_skips.py --out D:\\ytds --execute  # delete
"""
import argparse
import csv
import sqlite3
from pathlib import Path

MANIFEST_FIELDS = ["video_id", "platform", "url", "title", "score",
                   "channel", "published_at", "path", "download_date"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path, help="dataset dir (e.g. D:\\ytds)")
    ap.add_argument("--execute", action="store_true", help="actually delete (default: preview)")
    args = ap.parse_args()

    db_path = args.out / "dataset.db"
    meta_dir = args.out / "metadata"
    manifest = args.out / "manifest.csv"

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row

    rows = db.execute(
        "SELECT key, video_id, platform, path, filesize_mb, status "
        "FROM videos WHERE human_label=0"
    ).fetchall()

    have_file = [r for r in rows if r["path"]]
    total_mb = sum((r["filesize_mb"] or 0) for r in have_file)

    print(f"human_label=0 rows:        {len(rows)}")
    print(f"  with a path on disk:     {len(have_file)}")
    print(f"  total size:              {total_mb/1024:.2f} GB ({total_mb:.0f} MB)")

    # verify files actually exist + locate sidecars
    missing_vid = 0
    missing_side = 0
    plan = []
    for r in have_file:
        vid = Path(r["path"])
        side = meta_dir / f"{r['platform']}_{r['video_id']}.json"
        if not vid.exists():
            missing_vid += 1
        if not side.exists():
            missing_side += 1
        plan.append((r["key"], vid, side))
    print(f"  video files missing:     {missing_vid}")
    print(f"  sidecar files missing:   {missing_side}")

    if not args.execute:
        print("\nPREVIEW only. Re-run with --execute to delete.")
        print("Sample (first 5):")
        for _, vid, side in plan[:5]:
            print(f"  {vid.name}  +  {side.name}")
        return

    # ---- destructive from here ----
    removed_vid = removed_side = 0
    for key, vid, side in plan:
        if vid.exists():
            vid.unlink()
            removed_vid += 1
        if side.exists():
            side.unlink()
            removed_side += 1
        db.execute(
            "UPDATE videos SET status='skipped', path=NULL, download_date=NULL "
            "WHERE key=?", (key,))
    db.commit()
    print(f"\nDeleted {removed_vid} video files, {removed_side} sidecars.")
    print(f"Marked {len(plan)} DB rows status='skipped', path=NULL.")

    # rewrite manifest.csv from remaining downloaded rows
    keep = db.execute(
        "SELECT video_id, platform, url, title, score, channel, published_at, "
        "path, download_date FROM videos WHERE status='downloaded' ORDER BY download_date"
    ).fetchall()
    with manifest.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(MANIFEST_FIELDS)
        for r in keep:
            w.writerow([r[c] for c in MANIFEST_FIELDS])
    print(f"Rewrote {manifest} with {len(keep)} downloaded rows.")


if __name__ == "__main__":
    main()
