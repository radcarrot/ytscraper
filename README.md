# CCTV Dataset Collector

A pipeline for building a labeled dataset of public home-security / CCTV / doorbell videos from YouTube. It searches the YouTube Data API, scores each result's metadata (title, description, tags, channel) for relevance to residential security footage, downloads the high-scoring ones with `yt-dlp`, and stores everything in a queryable SQLite database with per-video metadata.

It collects **public** videos only and records the source URL + uploader for every clip so the dataset stays traceable. Intended for research / model training, not redistribution. Respect YouTube's Terms of Service.

---

## What it does

```
search (API)  ->  score metadata  ->  filter  ->  dedupe/queue  ->  download (yt-dlp)  ->  store
```

- **Relevance scoring** — weighted keyword match tuned for *residential* footage. Filters out store/traffic CCTV, product ads, tutorials, and creepy/horror clickbait. Keeps Shorts, compilations, and animal footage.
- **Channel blocklist** — penalizes known horror/comedy/product channels.
- **Near-duplicate detection** — skips reposts via normalized titles.
- **Rich metadata** — duration, view/like/comment counts, resolution, fps, definition, captions, language, and more, per video.
- **Resumable** — all state lives in SQLite; kill and restart any time.
- **Anti-throttling** — token-bucket rate limiting, randomized jitter, exponential backoff, browser TLS impersonation, and an optional residential proxy.

---

## Requirements

- **Python 3.10+**
- **deno** — solves YouTube's JS "n-challenge" (without it, downloads return no formats). [Install guide](https://docs.deno.com/runtime/getting_started/installation/).
- **ffmpeg** — to merge separate video/audio streams. Installed automatically via the `imageio-ffmpeg` pip package, so no manual step is needed.
- A **YouTube Data API v3 key** (for search/metadata).
- A **`cookies.txt`** from a logged-in YouTube account (for downloads — see below).

---

## Install

```bash
git clone <your-repo-url>
cd <repo>

python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

Install **deno** (one-time, system-wide):

```bash
# Windows (PowerShell):
irm https://deno.land/install.ps1 | iex
# macOS/Linux:
curl -fsSL https://deno.land/install.sh | sh
# or via npm on any platform:
npm i -g deno
```

---

## Setup

### 1. YouTube Data API key

1. Go to the [Google Cloud Console](https://console.cloud.google.com).
2. Create a project (or pick an existing one).
3. **APIs & Services → Library →** enable **"YouTube Data API v3"**.
4. **APIs & Services → Credentials → Create credentials → API key**.
5. (Recommended) Restrict the key to the YouTube Data API.

The free quota is **10,000 units/day**. A search costs 100 units (~100 searches/day); metadata lookups are 1 unit.

### 2. Cookies (for downloads)

YouTube gates video downloads behind a "Sign in to confirm you're not a bot" check. A logged-in cookie jar is the most reliable way past it. **Use a throwaway Google account, not your main one.**

1. Create / log into a throwaway account at YouTube.
2. Install the browser extension **"Get cookies.txt LOCALLY"** (Chrome / Edge / Firefox — the open-source *LOCALLY* one, which does not upload anything).
3. With `youtube.com` open and logged in, click the extension → **Export** (Netscape format).
4. Save the file as **`cookies.txt`** in the project root.

Cookies expire — if downloads start failing with the bot wall again, re-export.

### 3. Configure `.env`

Copy the template and fill it in:

```bash
cp .env.example .env
```

```ini
YOUTUBE_API_KEY=your-key-here
YOUTUBE_COOKIES=cookies.txt
# optional residential proxy for the download leg:
DATASET_PROXY=
```

`.env` and `cookies.txt` are git-ignored — they will not be committed.

---

## Usage

Queries live in `queries.txt` (one per line, `#` for comments). A residential-focused set is included.

**1. Discover** — search + score + queue, no downloads (cheap, API-only):

```bash
python youtube_scraper.py --mode discover --query-file queries.txt --max-per-query 50
```

**2. Download** — pull the queued videos (heavy, bandwidth):

```bash
python youtube_scraper.py --mode download
```

**Or do both in one pass:**

```bash
python youtube_scraper.py --mode full --query-file queries.txt
```

Output lands in `dataset/` by default (override with `--out <dir>`):

```
dataset/
├── videos/        # the .mp4 files
├── metadata/      # per-video sidecar JSON
├── manifest.csv   # flat index
└── dataset.db     # SQLite source of truth (all state + metadata)
```

### Other modes

| Mode | What it does |
|------|--------------|
| `--mode eval --labels labels.csv` | Score a hand-labeled CSV, report precision/recall/F1 + a threshold sweep. Use to tune the scorer. |
| `--mode rescore` | Re-score every stored row with the current engine (no API). Add `--delete-junk` to remove already-downloaded files that now fail. |
| `--mode enrich` | Backfill metadata (duration, stats, etc.) for existing rows and integrity-check downloaded files. |

A complete flag reference is in the [Tuning](#tuning-the-scorer) section below.

---

## Tuning the scorer

Relevance is driven by keyword weight tables (`STRONG`, `CONTEXT`, `NEGATIVE`) and a `CHANNEL_BLOCK` set at the top of `youtube_scraper.py`. To calibrate the threshold:

1. Hand-label ~20–30 videos in a CSV (`label,title,description,tags`; `label` is `1`/`0`). See `sample_labels.csv`.
2. Run `--mode eval --labels yourfile.csv` to see precision/recall/F1 and the best threshold.
3. Adjust weights or `--threshold`, then `--mode rescore` to re-apply to the queue.

See `cctv_dataset_plan.md` for the full design rationale.

### In-code tuning constants

These live near the top of `youtube_scraper.py` and govern scoring/download behavior. Edit them directly:

| Constant | Default | What it controls |
|----------|---------|------------------|
| `STRONG` | dict | Strong positive keywords (camera types, "home security") |
| `CONTEXT` | dict | Residential context keywords (porch, driveway, intruder…) |
| `NEGATIVE` | dict | Negative keywords (ads, tutorials, creepy/horror, non-residential) |
| `CHANNEL_BLOCK` | set | Channel-title substrings to penalize (horror/comedy/product) |
| `CHANNEL_PENALTY` | `-3.0` | Score penalty applied to a blocked channel (effectively a veto) |
| `FIELD_WEIGHT` | `1.0 / 0.7 / 0.3` | Weight of title / tags / description matches |
| `POS_CAP_PER_FIELD` | `2.0` | Max positive contribution per field (stops keyword-stuffing) |
| `NEG_CAP_PER_FIELD` | `2.5` | Max negative contribution per field |
| `DEFAULT_THRESHOLD` | `0.35` | Keep videos scoring ≥ this (override with `--threshold`) |
| `MIN_FILE_BYTES` | `100_000` | Downloaded files smaller than this are marked `failed` |

Download / anti-bot defaults live in the `DownloadOpts` dataclass and are all overridable by CLI flags:

| Field | Default | Flag |
|-------|---------|------|
| `impersonate` | `chrome` | `--impersonate` |
| `player_clients` | `tv,web_safari,web` | `--player-clients` |
| `sleep_min,sleep_max` | `2.0, 6.0` | `--dl-sleep min,max` |
| `max_height` | `0` (no cap) | `--max-height` |
| `max_seconds` | `0` (no cap) | `--max-seconds` |

### Full CLI flag list

| Flag | Default | Purpose |
|------|---------|---------|
| `--mode` | `full` | `discover` / `download` / `full` / `eval` / `rescore` / `enrich` |
| `--query "..."` | — | A single query (repeatable) |
| `--query-file FILE` | — | Load queries from a file |
| `--max-per-query N` | 50 | Results per query |
| `--threshold X` | 0.35 | Min relevance score (0–1) to keep |
| `--out DIR` | `dataset` | Output directory |
| `--rps X` | 1.0 | Max outbound requests/sec (token bucket) |
| `--refresh-seconds N` | 0 | Skip a query if searched within this window |
| `--proxy URL` | from `DATASET_PROXY` | Proxy for API + download legs |
| `--cookies FILE` | from `YOUTUBE_COOKIES` | Netscape cookies file |
| `--cookies-from-browser B` | from env | Pull cookies live from a browser |
| `--impersonate NAME` | chrome | Browser TLS fingerprint (`''` to disable) |
| `--player-clients LIST` | tv,web_safari,web | yt-dlp player_client fallback chain |
| `--dl-sleep min,max` | 2,6 | Randomized per-download sleep seconds |
| `--max-height N` | 0 | Cap resolution (0 = no cap) |
| `--max-seconds N` | 0 | Cap duration (0 = no cap) |
| `--labels FILE` | — | Labeled CSV for `--mode eval` |
| `--delete-junk` | off | In `rescore`, delete downloaded files that now fail |

---

## Legal & ethics

- Public posts only — no login-walled or private content.
- Source URL + uploader are stored for every clip for traceability and takedown handling.
- Downloading via `yt-dlp` is a gray area under YouTube's ToS; use responsibly for private research, not redistribution.
- This footage often shows identifiable people and private homes — minimize and anonymize PII if your task allows.

---

## License

MIT
