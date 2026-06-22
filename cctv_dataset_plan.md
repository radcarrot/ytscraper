# CCTV / Home-Security Video Collection — Plan

**Goal:** Build a labeled ML/CV dataset of publicly-posted home security / CCTV / doorbell videos by searching social platforms, scoring each result's *metadata* (title, description, tags) against keywords like `cctv`, `home`, `security`, `doorbell`, and downloading the ones that score high enough.

> Note on terminology: what you described is a **relevance-scoring / filtering pipeline**, not statistical correlation. You score each video by how strongly its metadata matches your target concept, then keep the high-scoring ones. That's the right approach for building a dataset, and it's what the starter code implements.

---

## 1. Platform reality (read this first)

The three platforms are not equal — each has a very different legal/technical path.

**YouTube — the realistic one.** YouTube has an official **Data API v3** that returns search results, titles, descriptions, and tags legitimately. This is where you should build first and probably where most usable footage lives (channels share "package thief caught on camera," doorbell clips, etc.). Downloading the actual video files is a separate step (`yt-dlp`) and is subject to YouTube's Terms; for a private research dataset that's the common tool, but be aware it's a gray area under YouTube ToS.

**Instagram & Facebook (Meta) — not realistically scrapable.** Meta's Terms of Service explicitly prohibit automated scraping, there is no public search API that returns this kind of content, and they actively block scrapers (login walls, rate limits, legal action). Building a scraper for these is high-risk: ToS violation, possible account/IP bans, and legal exposure. The realistic options are: (a) skip them, (b) use Meta's official Graph API which only covers content you own or have been granted, or (c) use a licensed third-party data provider. I'd recommend starting YouTube-only and treating Meta as a later, separate decision.

**"And more" — other legitimate sources** worth more than scraping Meta: TikTok (has a Research API for approved applicants), Reddit (official API; subreddits like r/CaughtOnCamera), Rumble, and Creative-Commons / open datasets (e.g. academic surveillance datasets) which may already give you cleaner labeled data with no legal risk.

## 2. Legal & ethical guardrails

Because this footage shows private homes and often identifiable people, bake these in from the start:

- **Only public posts.** No login-walled or private content.
- **Store source URLs and uploader credit** so every clip is traceable and you can honor takedown requests.
- **Respect `robots.txt` and rate limits.** Don't hammer endpoints.
- **Keep it for research/model training, not redistribution.** Don't republish other people's footage.
- **Minimize PII.** If faces/plates/addresses aren't needed for your task, plan a blurring/anonymization step.
- **Per-platform ToS** is the binding constraint — API path good, scraping path risky.

## 3. Architecture

A simple, queue-based pipeline keeps each stage independent and restartable:

```
 Search  ->  Relevance  ->  De-dupe /  ->  Download  ->  Store +
 (per src)   scoring        filter         (yt-dlp)      metadata
 YouTube API keyword wts.    video seen?    mp4 + .json   manifest.csv
```

- **Source adapters** — one module per platform exposing a common `search(query) -> [VideoMeta]`. Start with YouTube; add others behind the same interface.
- **Relevance scorer** — weighted keyword match over title/description/tags, returns a 0–1 score. Tunable threshold.
- **De-dupe** — track seen video IDs (and optionally near-duplicate titles) so re-runs don't re-download.
- **Downloader** — `yt-dlp`, capped resolution/length, writes the file plus a sidecar JSON of all metadata.
- **Manifest** — append-only CSV/JSONL: `video_id, platform, url, title, score, path, uploader, download_date`. This is your dataset index.

## 4. Relevance scoring (the "correlation" part)

Instead of a hard keyword match, give each keyword a weight and look at where it appears (a title hit counts more than a description hit). A video is "of interest" if its combined score clears a threshold.

- **Strong signals** (high weight): `cctv`, `security camera`, `surveillance`, `doorbell`, `ring camera`, `nest cam`.
- **Context signals** (medium): `home`, `house`, `residence`, `front porch`, `driveway`, `caught on camera`.
- **Negative signals** (subtract): `gameplay`, `movie`, `cctv music`, `unboxing`, `review`, `installation tutorial` — these pull in false positives.

Tune the threshold against a small hand-labeled sample, then run at scale. Later you can swap the keyword scorer for an embedding-similarity score or a small classifier without changing the rest of the pipeline.

## 5. Build phases

1. **Phase 1 — YouTube MVP (the starter code).** Search → score → download → manifest. Get an API key, run on a few queries, hand-check the results, tune weights/threshold.
2. **Phase 2 — Scale & dedupe.** Pagination, quota management, persistent "seen" store, resumable runs.
3. **Phase 3 — Quality & anonymization.** Filter by duration/resolution, optional face/plate blurring, drop near-duplicates.
4. **Phase 4 — More sources (optional).** Add Reddit and TikTok-Research adapters behind the same interface. Decide deliberately about Meta given the risks above.
5. **Phase 5 — Labeling.** Add task-specific labels (event type, day/night, indoor/outdoor) for supervised training.

## 6. Cost / quota notes

- YouTube Data API is free up to a daily **quota** (default 10,000 units/day); a `search.list` call costs 100 units, so ~100 searches/day. Request more quota if needed.
- Storage: video files are the bulk — cap resolution (e.g. 720p) and length to control size.

## 7. Sustainable collection — rate limits, headers, IP reputation

Goal: a long-running pipeline that mimics legitimate client behavior and respects server capacity, rather than evading detection adversarially. Platforms flag automation via three vectors: **volumetric thresholds** (rate limits), **reputation** (IP/network), and **behavioral anomalies**. Manage them with programmatic discipline.

### 7.1 Volumetric traffic management
Servers count requests per IP/token per time window; exceeding it returns `429 Too Many Requests` or an IP block.

- **Client-side throttling.** Never loop unconstrained. Cap max requests/sec with a token-bucket or leaky-bucket limiter in the ingestion layer.
- **Randomized jitter.** Fixed intervals (exactly one request every 5.0s) leave a robotic signature in logs. Add random variance to delays:
  ```python
  import time, random
  base_delay = 5.0
  jitter = random.uniform(0.5, 2.5)
  time.sleep(base_delay + jitter)
  ```
- **Adaptive backoff on 429.** Parse the `Retry-After` response header and wait exactly that long. If absent, use **exponential backoff** — double the wait after each successive failure.

### 7.2 Header hygiene & session integrity
Default library headers (Python `requests`, Node `axios`) are immediate automation tells.

- **User-Agent.** Send a realistic modern-browser `User-Agent`.
- **Standard browser headers.** Also send `Accept-Language`, `Accept-Encoding`, `Connection: keep-alive`. Omitting these signals an incomplete browser profile.
- **Session persistence.** Reuse a persistent connection object (`requests.Session()`) to reuse the TCP connection across requests — less overhead, mirrors real browsing.

### 7.3 Distributed ingestion (IP reputation)
If throughput needs exceed what one IP allows within safe limits, scale horizontally.

- **Residential proxies.** Datacenter ranges (AWS, DigitalOcean, Azure) are widely blacklisted. Route through rotating residential proxy pools that appear as consumer connections.
- **Decoupled workers.** Run the search/discovery phase and the media-download phase on separate schedules or compute nodes — downloads consume far more bandwidth and are tracked differently than metadata queries.

### 7.4 Maximizing official API quotas
With the YouTube Data API the constraint is a structured quota, not a fuzzy bot wall.

- **Minimize high-cost calls.** `search.list` = 100 units; `videos.list` = 1 unit.
- **Two-step query pattern.** Run a broad `search.list` to collect a batch of `videoId`s in one op, queue them locally, then hydrate in batches of up to 50 via `videos.list`. (The scaffold already does this — keep it.)
- **Caching layer.** Cache completed queries in a local DB (SQLite/Postgres). Before any external call, check whether the tag/entity was already processed within the refresh window.

### 7.5 Implementation status (done in `youtube_scraper.py`)

All of §7 is implemented in the scaffold:

| Mechanic | Where in code |
|----------|---------------|
| Token bucket + randomized jitter (thread-safe) | `RateLimiter.acquire()`, called before every `search.list`/`videos.list`/download; `--rps` flag |
| `Retry-After` + exponential backoff; hard stop on quota | `with_backoff()`, `QuotaExceeded`; yt-dlp gets its own `retries`/`retry_sleep_functions` |
| Header hygiene + session integrity | `BROWSER_HEADERS` (UA, Accept-Language/Encoding, keep-alive) passed to yt-dlp `http_headers` |
| Residential proxy (API + download legs) | `--proxy` / `DATASET_PROXY`; httplib2 `ProxyInfo` for the API, yt-dlp `proxy` for media |
| Decoupled discover/download workers | `--mode discover\|download\|full`; discover enqueues to DB, download drains the `kept` queue |
| Two-step `search.list`→`videos.list` (100u→1u) | `YouTubeSource.search()`/`_hydrate()` |
| SQLite store: dedupe + work queue + query cache | `Store` (`dataset.db`); `--refresh-seconds` controls the query-cache window. Replaces the old flat `seen_ids.txt`; `manifest.csv` + sidecar JSON still written for downstream tooling |

### 7.7 Download-leg anti-bot (2026 reality)

The official Data API is not bot-gated — quota is the only limit. The **download** leg (yt-dlp) is the exposed surface: since 2025 YouTube gates downloads behind *"Sign in to confirm you're not a bot."* What actually helps, in order of impact, all wired into `download()`:

| Lever | Flag | Notes |
|-------|------|-------|
| **Logged-in cookies** (biggest win) | `--cookies cookies.txt` or `--cookies-from-browser chrome` | A real session is the single most effective signal. Use a throwaway account, not your main. |
| **Don't use datacenter IPs** | `--proxy` (residential only) | AWS/GCP/Azure ranges are pre-flagged. Residential/mobile or your home IP pass; datacenter proxies make it *worse*. |
| **Slow down** | `--dl-sleep 2,6` | yt-dlp-native randomized sleep between downloads, on top of our token bucket + jitter. |
| **Real TLS fingerprint** | `--impersonate chrome` (needs `curl_cffi`) | Matches a browser's JA3/TLS handshake so the connection itself doesn't look like Python. Degrades gracefully if `curl_cffi` absent. |
| **Fresh player client** | `--player-clients tv,web_safari,web` | The plain `web` client is the most bot-flagged; `tv`/`web_safari` clients often still serve. yt-dlp tries the chain in order. |
| **Keep yt-dlp current** | — | Bot checks change weekly; an out-of-date yt-dlp is the most common failure. `pip install -U yt-dlp`. |

Not implemented (heavier, decide deliberately): rotating a **pool** of accounts/cookie jars, a PO-token provider (`bgutil-ytdlp-pot-provider` — largely neutralized as of 2026), and Invidious fallback. Also legitimate: prefer **Creative-Commons / open surveillance datasets** and the **YouTube API's own** thumbnails/metadata where the video file isn't strictly needed — zero bot risk.

> Reminder (plan §1/§2): downloading is the ToS-gray part. These levers reduce *friction*, not legal exposure. Keep it public-only, traceable, research-use.

### 7.6 Threshold tuning — `--eval`

Before scaling, calibrate the cutoff against a hand-labeled sample:

```
python youtube_scraper.py --mode eval --labels labels.csv --threshold 0.35
```

`labels.csv` needs a header with a `label` column (`1/0`, `keep/skip`, `yes/no`) plus any of `title`, `description`, `tags` (tags split on `|` or `,`). It reports precision / recall / F1 / accuracy at the chosen threshold and a full sweep (0.05–0.90), flagging the best-F1 cutoff. Tune weights or pick a threshold from this before a large run.

---

See `youtube_scraper.py` for a runnable Phase-1 scaffold implementing search → scoring → download → manifest.
