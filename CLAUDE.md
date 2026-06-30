# CLAUDE.md — CCTV Dataset Collector

Building a labeled ML dataset of **residential** home-security / CCTV / doorbell
videos scraped from YouTube, for a larger home-security model.

## Goal clarified (2026-06-26)
Dataset feeds a **home-security SEMANTIC VISION pipeline** (train + test). Target
classes = **baby monitoring, childcare/staff (nanny) compliance, intrusion alerts,
animal alerts, indoor/outdoor ambient**. Want inside-home + around-home feeds across
these tasks — NOT just the outdoor doorbell/crime footage the metadata scorer favors.
Key constraint: nanny/staff + indoor-ambient are **metadata-invisible + scarce on
YouTube** — only the vision pass can confirm + class-tag them. See memory
[[project-distribution-findings]].

## Where things are
- **Dataset on `D:\ytds`** (videos/, metadata/, manifest.csv, dataset.db) — NOT the
  repo's empty `dataset/`. Always `--out D:\ytds`.
- `youtube_scraper.py` (main), `label_app.py` (web labeler).
- `.venv` Python 3.10+; run `.venv\Scripts\python.exe`. `.env` has key; `cookies.txt`,
  `deno`, pip ffmpeg present. Ollama installed (`gemma3:4b` fits 4GB VRAM).

## Keep/skip criterion (refined 2026-06-25)
Keep = clip **contains real home-camera footage** (indoor/outdoor, event optional),
even if monetized/compilation/AI-generated-that-looks-like-CCTV. Skip = product
demos/reviews, store/traffic CCTV, tutorials. NEW directives:
- **Drop pure wildlife / trail-cam** (home-security dataset, not nature; animal-at-home ok).
- **Drop TV news entirely** (most have no footage; trimming not wanted) — even footage-news.
- **Rebalance off theft** → want normal **indoor/ambient** home footage, not just crime.
Never auto-filter news by channel name (tried, too noisy) — handle via query-pruning.

## State (2026-06-26)
- `D:\ytds`: **659 downloaded, 41 queued (kept, not yet drained)**.
- **567 hand labels** (`human_label`) — ALL the OLD downloaded clips. The **92 NEW
  clips from this scrape are UNLABELED** (label them next via `label_app.py`, now
  Gengar-themed).
- **This scrape** (new vertical queries): scanned 925, kept 195, downloaded 92,
  failed 62 (dups/<720p), scorer-skipped 324. Used ~3737 API units (of 10k/day).
- **PENDING next session (in order):**
  1. **Drain the 41-clip queue** — `--mode download --out D:\ytds --max-height 720`
     (keep `manifest.csv` CLOSED — Excel lock crashed a run; PermissionError mid-append).
  2. **Label the ~92+41 new haul** via `label_app.py --out D:\ytds`.
  3. **Per-query keep-rate on human labels** (now possible — `query` column stored).
     Prune dead queries (live/continuous phrasing all scored ~0; see findings memory).
  4. **`--mode rescore`** to backfill `score_terms` on existing 1006 rows AND apply the
     retuned negatives (will re-flag some downloaded rows as now-failing — reports
     only, deletes nothing without `--delete-junk`).
  5. **Deeper discover** at `--max-per-query 50` (default 25 was the throttle, NOT
     YouTube running dry; ~44% of results were already-seen dups of existing set).
- Still-open older TODO: delete the human_label=0 (skip) clips to clean dataset
  (one-off; preview first). Lower priority than the drain/label cycle above.

## Scorer status
Metadata scoring caps **~0.83 precision** — confirmed by BOTH keyword scorer and a
Gemma 3 4B LLM bake-off (`--mode llm-eval`). Both fail the same cases: news/vlog
whose metadata says "footage" but the video has none (information limit). **Metadata-
LLM dropped** (no gain). Scorer overfit to old queries (NEW-clip precision only 0.42,
mostly news flood) — `queries.txt` + scorer retuned (wildlife→negative, crime/news
queries removed, indoor-heavy). The 413-eval NEW numbers are polluted by deleted
queries — re-validate after a fresh scrape.

## Real next levers
1. **Re-scrape with new `queries.txt`** and label that haul (clean validation).
2. **Vision pass** — sample frames → Gemma vision (image input) to break the 0.83
   ceiling (separate news-with-footage from anchor-desk). Only real fix left.
3. Export dataset by `human_label`, not the scorer.

## Scorer changes (2026-06-26)
Full 567-label analysis: scorer does NOT rank within the downloaded set (keep mean
0.87 vs skip 0.81; precision flat 0.57→0.59 across all thresholds). Threshold tuning
dead — metadata only retrieves footage when the TITLE names an EVENT. Retuned for
**junk-rejection** only: removed `night vision`/`infrared`/`live feed|cam|stream` from
positives (title hits ~22-23% keep = product/idle-stream); added negatives `night
vision camera` -1.0, `security cameras` -0.8, `cctv camera` -0.6, `camera for home`
-1.0, `for home` -0.6, `wifi` -0.6. Verified: ad/product titles now score 0.

## DB schema additions (2026-06-26)
- **`query`** column — the discover query that surfaced each row (per-query analysis).
  `INSERT OR IGNORE` → first query to find a video wins attribution. Existing 1006
  rows = NULL (pre-tracking).
- **`score_terms`** column — JSON of keywords that fired: `{"pos":[[field,kw,w],...],
  "neg":[...],"channel_penalty":w}`. `score_breakdown(meta)` returns `(score, terms)`;
  `score_metadata` unchanged. Written by discover going forward; **rescore backfills
  it onto all rows** (also fixed: rescore now applies channel penalty, was ignored).

## queries.txt (2026-06-26)
Re-segmented by the 5 target verticals + continuous-feed sources; footage-genre
phrasing; avoids ad triggers (best/wifi/for home/night-vision-camera).

## Modes / tooling
discover/download/full/eval/rescore/enrich/dedupe/llm-eval. `--min-seconds`,
`--min-height`, `--quota-limit`, `--llm-model`, `--relabel`, `OLLAMA_NUM_GPU` env.
`--max-per-query` (default **50** — was 25; raised for depth). `--query-file queries.txt`.
