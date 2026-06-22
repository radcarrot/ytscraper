#!/usr/bin/env python3
"""
youtube_scraper.py — home-security / CCTV video dataset collector.

Pipeline:  search (YouTube Data API v3)  ->  score metadata  ->  filter  ->
           download (yt-dlp)  ->  write manifest + sidecar JSON

Phase-2 hardening (see cctv_dataset_plan.md §7):
  - token-bucket rate limiter + randomized jitter on every outbound call
  - Retry-After / exponential backoff on 429 / 5xx; hard stop on quota (403)
  - SQLite store: dedupe seen-set + query cache + work queue (replaces flat files)
  - decoupled modes: discover (cheap metadata) vs download (heavy bandwidth)
  - optional residential proxy for the download leg
  - cookies + browser TLS impersonation + fresh player client for the download leg

Collects ONLY public videos and records source URL + uploader for every clip so
the dataset stays traceable and takedowns can be honored. Research/training use,
not redistribution. Respect each platform's ToS.

Setup
-----
    pip install google-api-python-client yt-dlp
    export YOUTUBE_API_KEY="your-key"        # Windows: set YOUTUBE_API_KEY=...
    # optional download proxy:
    export DATASET_PROXY="http://user:pass@residential-proxy:port"

Run
---
    # dry scoring run (no download), default combined mode:
    python youtube_scraper.py --query "package thief caught on camera" \
        --query "doorbell camera porch" --max-per-query 25 --threshold 0.35

    # decoupled: discover cheaply now, download later (e.g. on another node):
    python youtube_scraper.py --mode discover --query "ring doorbell intruder"
    python youtube_scraper.py --mode download           # drains the queue
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, TypeVar

T = TypeVar("T")


def _load_dotenv(path: str = ".env") -> None:
    """Minimal no-dependency .env loader. Existing env vars win."""
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip("'\"")
        if k and k not in os.environ:
            os.environ[k] = v

# ---------------------------------------------------------------------------
# Relevance scoring config — tune these against a hand-labeled sample.
# Weights are summed for every keyword found; a title hit is worth more than a
# description/tag hit. The raw sum is squashed to 0..1 so THRESHOLD is stable.
# ---------------------------------------------------------------------------
# Tuned for RESIDENTIAL footage (homes/houses/yards), not store/traffic/business
# CCTV. Camera-type words alone are not enough — a residential context word must
# also be present, and commercial/non-home contexts are pushed down hard.
STRONG = {
    "cctv": 0.9, "security camera": 0.9, "surveillance": 0.8, "doorbell": 1.0,
    "ring camera": 1.0, "ring doorbell": 1.0, "nest cam": 0.9, "security cam": 0.9,
    "home security": 1.2, "home surveillance": 1.1,
}
CONTEXT = {  # residential signals — these are what make it a HOME video
    "home": 0.6, "house": 0.6, "residence": 0.7, "residential": 0.7,
    "front porch": 0.8, "porch": 0.6, "driveway": 0.7, "front door": 0.7,
    "front yard": 0.6, "backyard": 0.6, "back yard": 0.6, "garage": 0.4,
    "neighborhood": 0.4, "suburb": 0.4, "my house": 0.7, "my home": 0.7,
    "caught on camera": 0.5, "caught on cctv": 0.5, "porch pirate": 0.8,
    "package thief": 0.8, "home invasion": 0.7, "burglar": 0.5, "intruder": 0.5,
    "trespasser": 0.5, "front gate": 0.5,
}
NEGATIVE = {
    # content that isn't real residential footage
    "gameplay": -1.5, "movie": -1.2, "film": -1.0, "trailer": -1.2, "music": -1.2,
    "unboxing": -1.2, "review": -1.0, "how to install": -1.5, "installation": -1.2,
    "tutorial": -1.2, "setup": -0.8, "for sale": -1.0, "best ": -0.8, "vlog": -0.6,
    "explained": -0.6, "documentary": -0.6,
    # non-residential CCTV (store / road / business) — push these out
    "store": -1.0, "shop": -0.8, "supermarket": -1.2, "mall": -1.0, "retail": -1.0,
    "gas station": -1.2, "petrol": -1.0, "bank": -1.0, "atm": -1.0, "casino": -1.0,
    "factory": -1.0, "warehouse": -1.0, "office": -0.8, "school": -0.8,
    "hospital": -0.8, "restaurant": -0.8, "hotel": -0.8, "traffic": -1.2,
    "highway": -1.2, "road accident": -1.2, "dashcam": -1.2, "dash cam": -1.2,
    "city cctv": -0.8, "subway": -1.0, "airport": -1.0, "parking lot": -0.7,
    # product listings / ads / reviews of cameras (not footage)
    "auto-tracking": -1.2, "auto tracking": -1.2, "ai alerts": -1.2,
    "person detection": -1.0,  # NOTE: 'night vision'/'motion detection' removed —
    # they appear in legit residential footage, not just product specs.
    "360°": -1.0, "360 degree": -1.0, "wired": -0.8, "wireless": -0.8,
    "wifi camera": -1.2, "wi-fi camera": -1.2, "smart camera": -1.2,
    "solar camera": -1.2, "solar cctv": -1.2, "4g camera": -1.2, "ptz": -1.0,
    "buy": -1.0, "price": -1.2, "₹": -1.2, "rs.": -1.0, "rupees": -1.0,
    "only in": -1.0, "discount": -1.0, "amazon link": -1.0, "best budget": -1.2,
    "vs ": -0.9, " vs ": -0.9, "comparison": -0.9, "specs": -1.0, "megapixel": -1.0,
    "resolution": -0.6, "buyer": -0.8, "lifehack": -1.0,
    # common camera brand/product names (ads, not residential footage)
    "tapo": -1.2, "qubo": -1.2, "eufy": -1.2, "aosu": -1.2, "tp-link": -1.2,
    "hikvision": -1.0, "cp plus": -1.2, "imou": -1.2, "reolink": -1.0,
    "wyze": -1.0, "arlo": -1.0, "blink": -0.9, "swann": -1.0, "simplisafe": -1.2,
    "trueview": -1.2, "amaryllo": -1.2, "tuya": -1.2, "mini ip camera": -1.2,
    # NOTE: Shorts intentionally NOT filtered (user wants them kept).
    # creepy / paranormal / horror clickbait — not real security footage
    "creepy": -2.5, "creepiest": -2.5, "scary": -2.2, "scariest": -2.5,
    "ghost": -2.5, "paranormal": -2.5, "haunted": -2.5, "skinwalker": -2.5,
    "demon": -2.5, "horror": -2.2, "terrifying": -2.0, "frightening": -1.8,
    "chilling": -1.6, "skin crawl": -2.0, "strange figure": -2.0, "evil": -1.5,
    "mysterious creature": -2.2, "mystery": -1.2, "mysterious": -1.2,
    "unexplained": -2.0, "wtf": -1.5, "supernatural": -2.2, "cryptid": -2.2,
    "shocking": -1.2, "you won't believe": -1.8, "won't believe": -1.8,
    "most disturbing": -2.5, "disturbing": -2.0,  # creepy clickbait genre
    # NOTE: compilations / fails / karma / pranks intentionally NOT filtered
    # (user keeps them — montages of real footage don't harm the dataset).
    # tutorials / app help / how-to (not footage)
    "how to": -1.5, "how to pair": -1.8, "how to build": -1.8, "how to save": -1.8,
    "how to see": -1.8, "how to use": -1.5, "event history": -1.5, "saved video": -1.2,
    "saved videos": -1.2, "step by step": -1.2,
    # product / system ads still leaking via stuffed descriptions
    "camera system": -1.2, "camera under": -1.5, "which is right for": -1.5,
    "home camera system": -1.5, "24/7": -1.0, "subscribe": -1.0, "giveaway": -1.5,
    # clickbait hooks
    "at 3 am": -1.0, "3am": -1.0, "i'm speechless": -1.2, "speechless": -1.0,
    "you need to see": -1.2, "wait for it": -1.0,
}
FIELD_WEIGHT = {"title": 1.0, "tags": 0.7, "description": 0.3}
DEFAULT_THRESHOLD = 0.35
# Cap each field's POSITIVE and NEGATIVE keyword sums, then field-weight both,
# so neither positive nor negative keyword-stuffing in a (low-trust) description
# can dominate. A negative in the TITLE still vetoes (full field weight); the
# same word only in a spam description can't, by itself, kill a clean title.
POS_CAP_PER_FIELD = 2.0
NEG_CAP_PER_FIELD = 2.5  # magnitude
_POSWORDS = {**STRONG, **CONTEXT}

# Pretend to be a current desktop Chrome for the download leg. yt-dlp sends a
# realistic UA already; we set these explicitly so the profile is complete.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


@dataclass
class VideoMeta:
    video_id: str
    platform: str
    url: str
    title: str
    description: str
    tags: list[str] = field(default_factory=list)
    channel: str = ""
    channel_id: str = ""
    published_at: str = ""
    score: float = 0.0
    # --- enriched from videos.list contentDetails + statistics ---
    duration_sec: int = 0          # 0 = unknown
    definition: str = ""           # "hd" / "sd"
    has_caption: bool = False
    category_id: str = ""
    default_language: str = ""
    live_content: str = ""         # "none" / "live" / "upcoming"
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    thumbnail: str = ""
    is_short: bool = False         # heuristic: duration <= 60s
    # --- filled after download from the yt-dlp info dict ---
    width: int = 0
    height: int = 0
    fps: float = 0.0
    ext: str = ""
    filesize_mb: float = 0.0
    actual_duration: int = 0       # from the media file (may differ from API)


# Channels that almost exclusively post horror/creepy, comedy/reaction, or
# product content — higher-signal than keywords. Matched case-insensitively as
# substrings of the channel title. A hit applies CHANNEL_PENALTY.
CHANNEL_BLOCK = {
    "mr. nightmare", "nightmare", "chilling scares", "scary", "horror",
    "lets read", "corpse", "wendigoon", "brain time", "5-minute", "5 minute",
    "america's funniest", "afv", "fail", "funny", "comedy",
    "unboxing", "review", "tech", "gadget", "deals",
}
CHANNEL_PENALTY = -3.0  # strong; effectively vetoes a blocked channel


def _parse_iso8601_duration(s: str) -> int:
    """'PT1H2M3S' -> seconds. Returns 0 if unparseable/empty."""
    m = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", s or "")
    if not m:
        return 0
    h, mn, sec = (int(x) if x else 0 for x in m.groups())
    return h * 3600 + mn * 60 + sec


def _channel_blocked(channel: str) -> bool:
    c = channel.lower()
    return any(b in c for b in CHANNEL_BLOCK)


def _norm_title(title: str) -> str:
    """Normalize a title for near-duplicate detection: lowercase, strip emoji /
    punctuation / hashtags / part markers, collapse whitespace."""
    t = (title or "").lower()
    t = re.sub(r"#\w+", " ", t)                       # hashtags
    t = re.sub(r"\b(part|pt|vol|episode|ep)\s*\.?\s*\d+\b", " ", t)  # part markers
    t = re.sub(r"[^a-z0-9 ]+", " ", t)                # punctuation/emoji
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ---------------------------------------------------------------------------
# Relevance scoring
# ---------------------------------------------------------------------------
def _squash(x: float) -> float:
    """Map an unbounded score to 0..1 (logistic-ish, monotonic)."""
    return max(0.0, min(1.0, x / (1.0 + abs(x)) * 1.5 + 0.0)) if x > 0 else 0.0


def score_metadata(meta: VideoMeta) -> float:
    """Weighted keyword match across title / tags / description -> 0..1.

    Positive hits are summed per field then capped (POS_CAP_PER_FIELD) and
    field-weighted, so a keyword-stuffed description can't saturate the score.
    Negative hits are full-strength and uncapped (counted in any field) so a
    junk signal vetoes an otherwise-stuffed entry.
    """
    fields = {
        "title": meta.title.lower(),
        "tags": " ".join(meta.tags).lower(),
        "description": meta.description.lower(),
    }
    raw = 0.0
    for fname, text in fields.items():
        fw = FIELD_WEIGHT[fname]
        pos = sum(w for kw, w in _POSWORDS.items() if kw in text)
        neg = sum(w for kw, w in NEGATIVE.items() if kw in text)  # negative sum
        raw += min(pos, POS_CAP_PER_FIELD) * fw
        raw += max(neg, -NEG_CAP_PER_FIELD) * fw
    if meta.channel and _channel_blocked(meta.channel):
        raw += CHANNEL_PENALTY
    return round(_squash(raw), 4)


# ---------------------------------------------------------------------------
# Evaluation — score a hand-labeled CSV and report precision/recall/F1, plus a
# threshold sweep so you can pick the cutoff before scaling.
#
# Labels CSV columns (header required): label (1/0/keep/skip/yes/no), and any of
#   title, description, tags  (tags split on '|' or ',').
# ---------------------------------------------------------------------------
_TRUE = {"1", "keep", "yes", "true", "y", "pos", "positive"}


def _parse_label(raw: str) -> bool:
    return str(raw).strip().lower() in _TRUE


def _prf(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    return prec, rec, f1


def evaluate(labels_csv: Path, threshold: float) -> None:
    rows: list[tuple[bool, float]] = []  # (truth, score)
    with labels_csv.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "label" not in reader.fieldnames:
            sys.exit("labels CSV needs a header with a 'label' column.")
        for r in reader:
            raw_tags = r.get("tags") or ""
            tags = [t.strip() for t in re.split(r"[|,]", raw_tags) if t.strip()]
            meta = VideoMeta(
                video_id="", platform="eval", url="",
                title=r.get("title") or "", description=r.get("description") or "",
                tags=tags,
            )
            rows.append((_parse_label(r["label"]), score_metadata(meta)))

    if not rows:
        sys.exit("labels CSV has no data rows.")

    def confusion(th: float) -> tuple[int, int, int, int]:
        tp = fp = fn = tn = 0
        for truth, sc in rows:
            pred = sc >= th
            tp += truth and pred
            fp += (not truth) and pred
            fn += truth and (not pred)
            tn += (not truth) and (not pred)
        return tp, fp, fn, tn

    tp, fp, fn, tn = confusion(threshold)
    prec, rec, f1 = _prf(tp, fp, fn)
    n = len(rows)
    pos = sum(1 for t, _ in rows if t)
    print(f"labeled rows: {n}  (positives={pos}, negatives={n - pos})")
    print(f"\n@ threshold {threshold:.2f}: "
          f"TP={tp} FP={fp} FN={fn} TN={tn}")
    print(f"  precision={prec:.3f}  recall={rec:.3f}  F1={f1:.3f}  "
          f"accuracy={(tp + tn) / n:.3f}")

    print("\nthreshold sweep:")
    print("  thr   prec   rec    F1     TP FP FN TN")
    best = (-1.0, 0.0)
    for i in range(1, 19):
        th = round(i * 0.05, 2)
        a, b, c, d = confusion(th)
        p, rr, ff = _prf(a, b, c)
        mark = ""
        if ff > best[0]:
            best = (ff, th)
        print(f"  {th:.2f}  {p:.3f}  {rr:.3f}  {ff:.3f}  {a:2d} {b:2d} {c:2d} {d:2d}")
    print(f"\nbest F1={best[0]:.3f} at threshold {best[1]:.2f} "
          f"(current default {DEFAULT_THRESHOLD}).")


# ---------------------------------------------------------------------------
# §7.1 Rate limiting — token bucket + jitter. Thread-safe so a future
# multi-worker download pool shares one budget per host.
# ---------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, rps: float, burst: float | None = None):
        self.rate = max(rps, 1e-6)
        self.capacity = burst if burst is not None else max(1.0, rps)
        self.tokens = self.capacity
        self.timestamp = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, jitter: tuple[float, float] = (0.0, 0.3)) -> None:
        """Block until a token is available, then add small random jitter."""
        with self._lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.timestamp) * self.rate)
            self.timestamp = now
            wait = 0.0 if self.tokens >= 1.0 else (1.0 - self.tokens) / self.rate
            self.tokens -= 1.0
        if wait > 0:
            time.sleep(wait)
        lo, hi = jitter
        if hi > 0:
            time.sleep(random.uniform(lo, hi))


class QuotaExceeded(RuntimeError):
    """YouTube Data API daily quota is exhausted — stop, don't retry."""


# ---------------------------------------------------------------------------
# §7.1 Adaptive backoff. Parses Retry-After; falls back to exponential.
# ---------------------------------------------------------------------------
def with_backoff(fn: Callable[[], T], *, what: str, max_tries: int = 5,
                 base: float = 2.0, cap: float = 120.0) -> T:
    from googleapiclient.errors import HttpError  # lazy

    last: Exception | None = None
    for attempt in range(max_tries):
        try:
            return fn()
        except HttpError as e:
            last = e
            status = getattr(e.resp, "status", None)
            reason = str(e).lower()
            if status == 403 and ("quota" in reason or "ratelimitexceeded" not in reason
                                  and "dailylimitexceeded" in reason):
                raise QuotaExceeded(str(e)) from e
            if status == 429 or (status is not None and 500 <= status < 600) or \
                    (status == 403 and "ratelimit" in reason):
                retry_after = None
                try:
                    retry_after = e.resp.get("retry-after")
                except Exception:  # noqa: BLE001
                    retry_after = None
                wait = float(retry_after) if retry_after else min(cap, base ** attempt)
                wait += random.uniform(0.0, 1.0)  # jitter the retry too
                print(f"  ~ {what}: {status}, retry in {wait:.1f}s "
                      f"(attempt {attempt + 1}/{max_tries})", file=sys.stderr)
                time.sleep(wait)
                continue
            raise  # non-retryable (400/401/404…)
    assert last is not None
    raise last


# ---------------------------------------------------------------------------
# Source adapter — YouTube. §7.4 two-step pattern: search.list (100u) gathers
# ids, videos.list (1u) hydrates 50 at a time.
# ---------------------------------------------------------------------------
class YouTubeSource:
    platform = "youtube"

    def __init__(self, api_key: str, limiter: RateLimiter, proxy: str | None = None):
        from googleapiclient.discovery import build  # lazy import
        self.limiter = limiter
        http = self._build_http(proxy) if proxy else None
        if http is not None:
            self.yt = build("youtube", "v3", developerKey=api_key,
                            cache_discovery=False, http=http)
        else:
            self.yt = build("youtube", "v3", developerKey=api_key, cache_discovery=False)

    @staticmethod
    def _build_http(proxy: str):
        """Route the API leg through a proxy (httplib2)."""
        import httplib2
        from urllib.parse import urlparse
        u = urlparse(proxy)
        proxy_info = httplib2.ProxyInfo(
            proxy_type=httplib2.socks.PROXY_TYPE_HTTP,
            proxy_host=u.hostname, proxy_port=u.port or 8080,
            proxy_user=u.username, proxy_pass=u.password,
        )
        return httplib2.Http(proxy_info=proxy_info, timeout=30)

    def search(self, query: str, max_results: int = 25) -> list[VideoMeta]:
        ids: list[str] = []
        page_token = None
        while len(ids) < max_results:
            self.limiter.acquire()
            resp = with_backoff(
                lambda: self.yt.search().list(
                    q=query, part="id", type="video",
                    maxResults=min(50, max_results - len(ids)),
                    safeSearch="none", pageToken=page_token,
                ).execute(),
                what=f"search.list({query!r})",
            )
            ids += [it["id"]["videoId"] for it in resp.get("items", [])
                    if it["id"].get("videoId")]
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return self._hydrate(ids)

    def _hydrate(self, ids: list[str]) -> list[VideoMeta]:
        """Fetch snippet + contentDetails + statistics (search.list omits these).
        videos.list is 1 quota unit regardless of how many parts we request."""
        out: list[VideoMeta] = []
        for i in range(0, len(ids), 50):
            chunk = ids[i:i + 50]
            self.limiter.acquire()
            resp = with_backoff(
                lambda: self.yt.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=",".join(chunk)).execute(),
                what="videos.list",
            )
            for it in resp.get("items", []):
                out.append(self._parse_video(it))
        return out

    def _parse_video(self, it: dict) -> VideoMeta:
        sn = it.get("snippet", {})
        cd = it.get("contentDetails", {})
        stx = it.get("statistics", {})
        thumbs = sn.get("thumbnails", {})
        thumb = (thumbs.get("maxres") or thumbs.get("high") or thumbs.get("default")
                 or {}).get("url", "")
        dur = _parse_iso8601_duration(cd.get("duration", ""))
        return VideoMeta(
            video_id=it["id"], platform=self.platform,
            url=f"https://www.youtube.com/watch?v={it['id']}",
            title=sn.get("title", ""), description=sn.get("description", ""),
            tags=sn.get("tags", []), channel=sn.get("channelTitle", ""),
            channel_id=sn.get("channelId", ""), published_at=sn.get("publishedAt", ""),
            duration_sec=dur, definition=cd.get("definition", ""),
            has_caption=cd.get("caption") == "true", category_id=sn.get("categoryId", ""),
            default_language=sn.get("defaultAudioLanguage") or sn.get("defaultLanguage") or "",
            live_content=sn.get("liveBroadcastContent", ""),
            view_count=int(stx.get("viewCount", 0) or 0),
            like_count=int(stx.get("likeCount", 0) or 0),
            comment_count=int(stx.get("commentCount", 0) or 0),
            thumbnail=thumb, is_short=0 < dur <= 60,
        )


# ---------------------------------------------------------------------------
# §7.4 Persistence — SQLite. One DB holds: the dedupe/work-queue table and a
# query cache. manifest.csv + sidecar JSON are still written for downstream
# tooling, but the DB is the source of truth and makes runs resumable.
#
# video status lifecycle:  discovered -> kept|skipped ;  kept -> downloaded
# ---------------------------------------------------------------------------
class Store:
    MANIFEST_FIELDS = ["video_id", "platform", "url", "title", "score",
                       "channel", "published_at", "path", "download_date"]

    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.videos_dir = out_dir / "videos"
        self.meta_dir = out_dir / "metadata"
        self.manifest = out_dir / "manifest.csv"
        self.db_path = out_dir / "dataset.db"
        for d in (self.videos_dir, self.meta_dir):
            d.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.db_path)
        self.db.row_factory = sqlite3.Row
        self._init_schema()
        if not self.manifest.exists():
            with self.manifest.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(self.MANIFEST_FIELDS)

    # columns added after v1; (name, sql-type) — applied to existing DBs too
    _EXTRA_COLS = [
        ("channel_id", "TEXT"), ("norm_title", "TEXT"), ("duration_sec", "INTEGER"),
        ("definition", "TEXT"), ("has_caption", "INTEGER"), ("category_id", "TEXT"),
        ("default_language", "TEXT"), ("live_content", "TEXT"), ("view_count", "INTEGER"),
        ("like_count", "INTEGER"), ("comment_count", "INTEGER"), ("thumbnail", "TEXT"),
        ("is_short", "INTEGER"), ("width", "INTEGER"), ("height", "INTEGER"),
        ("fps", "REAL"), ("ext", "TEXT"), ("filesize_mb", "REAL"),
        ("actual_duration", "INTEGER"),
    ]

    def _init_schema(self) -> None:
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS videos (
                key           TEXT PRIMARY KEY,   -- platform:video_id
                video_id      TEXT NOT NULL,
                platform      TEXT NOT NULL,
                url           TEXT NOT NULL,
                title         TEXT,
                description   TEXT,
                tags          TEXT,               -- json
                channel       TEXT,
                published_at  TEXT,
                score         REAL,
                status        TEXT NOT NULL,    -- discovered|kept|skipped|downloaded|failed
                path          TEXT,
                discovered_at TEXT,
                download_date TEXT
            );
            CREATE TABLE IF NOT EXISTS query_cache (
                query    TEXT PRIMARY KEY,
                last_run TEXT NOT NULL
            );
            """
        )
        existing = {r[1] for r in self.db.execute("PRAGMA table_info(videos)")}
        for name, typ in self._EXTRA_COLS:
            if name not in existing:
                self.db.execute(f"ALTER TABLE videos ADD COLUMN {name} {typ}")
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_norm_title ON videos(norm_title)")
        self.db.commit()

    # -- dedupe -------------------------------------------------------------
    def is_seen(self, key: str) -> bool:
        cur = self.db.execute("SELECT 1 FROM videos WHERE key=?", (key,))
        return cur.fetchone() is not None

    def is_dup_title(self, title: str) -> bool:
        """True if a near-identical (normalized) title is already stored."""
        nt = _norm_title(title)
        if not nt:
            return False
        cur = self.db.execute("SELECT 1 FROM videos WHERE norm_title=? LIMIT 1", (nt,))
        return cur.fetchone() is not None

    # -- §7.4 query cache ---------------------------------------------------
    def query_fresh(self, query: str, refresh_seconds: int) -> bool:
        """True if this query ran within the refresh window (skip re-searching)."""
        if refresh_seconds <= 0:
            return False
        cur = self.db.execute("SELECT last_run FROM query_cache WHERE query=?", (query,))
        row = cur.fetchone()
        if not row:
            return False
        last = datetime.fromisoformat(row["last_run"])
        age = (datetime.now(timezone.utc) - last).total_seconds()
        return age < refresh_seconds

    def mark_query_run(self, query: str) -> None:
        self.db.execute(
            "INSERT INTO query_cache(query, last_run) VALUES(?, ?) "
            "ON CONFLICT(query) DO UPDATE SET last_run=excluded.last_run",
            (query, datetime.now(timezone.utc).isoformat(timespec="seconds")),
        )
        self.db.commit()

    # -- queue --------------------------------------------------------------
    def enqueue(self, meta: VideoMeta, status: str) -> None:
        key = f"{meta.platform}:{meta.video_id}"
        self.db.execute(
            "INSERT OR IGNORE INTO videos(key, video_id, platform, url, title, "
            "description, tags, channel, channel_id, published_at, score, status, "
            "discovered_at, norm_title, duration_sec, definition, has_caption, "
            "category_id, default_language, live_content, view_count, like_count, "
            "comment_count, thumbnail, is_short) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (key, meta.video_id, meta.platform, meta.url, meta.title,
             meta.description, json.dumps(meta.tags), meta.channel, meta.channel_id,
             meta.published_at, meta.score, status,
             datetime.now(timezone.utc).isoformat(timespec="seconds"),
             _norm_title(meta.title), meta.duration_sec, meta.definition,
             int(meta.has_caption), meta.category_id, meta.default_language,
             meta.live_content, meta.view_count, meta.like_count, meta.comment_count,
             meta.thumbnail, int(meta.is_short)),
        )
        self.db.commit()
        if status in ("kept", "downloaded"):
            self._write_sidecar(meta)

    def pending_downloads(self) -> list[VideoMeta]:
        cur = self.db.execute("SELECT * FROM videos WHERE status='kept'")
        return [self._row_to_meta(r) for r in cur.fetchall()]

    def mark_downloaded(self, meta: VideoMeta, path: str) -> None:
        key = f"{meta.platform}:{meta.video_id}"
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.db.execute(
            "UPDATE videos SET status='downloaded', path=?, download_date=?, "
            "width=?, height=?, fps=?, ext=?, filesize_mb=?, actual_duration=? WHERE key=?",
            (path, now, meta.width, meta.height, meta.fps, meta.ext,
             round(meta.filesize_mb, 2), meta.actual_duration, key),
        )
        self.db.commit()
        self._write_sidecar(meta)
        with self.manifest.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                meta.video_id, meta.platform, meta.url, meta.title, meta.score,
                meta.channel, meta.published_at, path, now,
            ])

    def mark_failed(self, meta: VideoMeta, reason: str) -> None:
        key = f"{meta.platform}:{meta.video_id}"
        self.db.execute("UPDATE videos SET status='failed' WHERE key=?", (key,))
        self.db.commit()
        print(f"  ! marked failed ({reason}): {meta.video_id}", file=sys.stderr)

    # -- helpers ------------------------------------------------------------
    def _write_sidecar(self, meta: VideoMeta) -> None:
        (self.meta_dir / f"{meta.platform}_{meta.video_id}.json").write_text(
            json.dumps(asdict(meta), indent=2, ensure_ascii=False), encoding="utf-8")

    @staticmethod
    def _row_to_meta(r: sqlite3.Row) -> VideoMeta:
        k = r.keys()
        g = lambda n, d=None: (r[n] if n in k else d)  # noqa: E731 - tolerate old DBs
        return VideoMeta(
            video_id=r["video_id"], platform=r["platform"], url=r["url"],
            title=r["title"] or "", description=r["description"] or "",
            tags=json.loads(r["tags"] or "[]"), channel=r["channel"] or "",
            channel_id=g("channel_id") or "", published_at=r["published_at"] or "",
            score=r["score"] or 0.0,
            duration_sec=g("duration_sec") or 0, definition=g("definition") or "",
            has_caption=bool(g("has_caption")), category_id=g("category_id") or "",
            default_language=g("default_language") or "", live_content=g("live_content") or "",
            view_count=g("view_count") or 0, like_count=g("like_count") or 0,
            comment_count=g("comment_count") or 0, thumbnail=g("thumbnail") or "",
            is_short=bool(g("is_short")),
            width=g("width") or 0, height=g("height") or 0, fps=g("fps") or 0.0,
            ext=g("ext") or "", filesize_mb=g("filesize_mb") or 0.0,
            actual_duration=g("actual_duration") or 0,
        )

    def close(self) -> None:
        self.db.close()


# ---------------------------------------------------------------------------
# §7.7 Anti-bot config for the yt-dlp download leg. As of 2026 YouTube gates
# downloads behind "Sign in to confirm you're not a bot." The things that
# actually move the needle (in order): a logged-in cookie jar, NOT using a
# datacenter IP, slowing down, a real browser TLS fingerprint, and a fresh
# player client. All optional + off by default so the script still runs bare.
# ---------------------------------------------------------------------------
@dataclass
class DownloadOpts:
    proxy: str | None = None
    cookies_file: str | None = None          # --cookies cookies.txt (Netscape)
    cookies_from_browser: str | None = None   # e.g. "chrome", "firefox", "edge"
    impersonate: str | None = "chrome"        # real TLS/JA3 fingerprint (needs curl_cffi)
    player_clients: str = "tv,web_safari,web"  # fallback chain; avoid plain "web"
    sleep_min: float = 2.0                    # yt-dlp's own per-download sleep …
    sleep_max: float = 6.0                    # … randomized in [min,max]
    max_height: int = 720  # 0 = no resolution cap (best available)
    max_seconds: int = 0   # 0 = no duration cap


def download(meta: VideoMeta, videos_dir: Path, limiter: RateLimiter,
             cfg: DownloadOpts) -> str | None:
    import yt_dlp
    limiter.acquire(jitter=(0.5, 2.5))  # §7.1 heavier jitter for the media leg
    h = f"[height<={cfg.max_height}]" if cfg.max_height else ""
    opts = {
        "format": f"bestvideo{h}+bestaudio/best{h}/best",
        "merge_output_format": "mp4",  # mux video+audio into one .mp4 (needs ffmpeg)
        "outtmpl": str(videos_dir / f"{meta.platform}_{meta.video_id}.%(ext)s"),
        "http_headers": BROWSER_HEADERS,
        "retries": 5, "fragment_retries": 5, "retry_sleep_functions": {
            "http": lambda n: min(60, 2 ** n)},  # exponential backoff
        "ratelimit": 5_000_000,  # ~5 MB/s cap, polite bandwidth
        # §7.7 yt-dlp-native randomized sleep between downloads (on top of our jitter)
        "sleep_interval": cfg.sleep_min, "max_sleep_interval": cfg.sleep_max,
        "sleep_interval_requests": 1,
        # §7.7 try fresh player clients before the bot-flagged default web client
        "extractor_args": {"youtube": {"player_client": cfg.player_clients.split(",")}},
        "quiet": True, "noprogress": True, "ignoreerrors": True,
    }
    if cfg.max_seconds:
        opts["match_filter"] = yt_dlp.utils.match_filter_func(f"duration < {cfg.max_seconds}")
    try:  # use the pip-bundled ffmpeg so video+audio actually merge
        import imageio_ffmpeg
        opts["ffmpeg_location"] = imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:  # noqa: BLE001 - fall back to a system ffmpeg if present
        pass
    if cfg.proxy:
        opts["proxy"] = cfg.proxy
    if cfg.cookies_file:                       # §7.7 logged-in session = biggest win
        opts["cookiefile"] = cfg.cookies_file
    if cfg.cookies_from_browser:
        opts["cookiesfrombrowser"] = (cfg.cookies_from_browser,)
    if cfg.impersonate:                        # §7.7 real browser TLS fingerprint
        target = yt_dlp.networking.impersonate.ImpersonateTarget.from_str(cfg.impersonate)
        probe = yt_dlp.YoutubeDL({"quiet": True})
        if probe._impersonate_target_available(target):
            opts["impersonate"] = target
        else:
            print(f"  ~ impersonate '{cfg.impersonate}' unavailable "
                  f"(pip install curl_cffi) — continuing without it", file=sys.stderr)
        probe.close()
    with yt_dlp.YoutubeDL(opts) as ydl:
        try:
            info = ydl.extract_info(meta.url, download=True)
            if not info:
                return None
            path = ydl.prepare_filename(info)
            # merged container may differ from the per-format ext yt-dlp guessed
            merged = Path(path).with_suffix(".mp4")
            if merged.exists():
                path = str(merged)
            # capture media fields from the info dict for richer metadata
            meta.width = info.get("width") or 0
            meta.height = info.get("height") or 0
            meta.fps = float(info.get("fps") or 0.0)
            meta.ext = Path(path).suffix.lstrip(".")
            meta.actual_duration = int(info.get("duration") or 0)
            meta.filesize_mb = (os.path.getsize(path) / 1e6) if os.path.exists(path) else 0.0
            return path
        except Exception as e:  # noqa: BLE001 - keep the run going
            print(f"  ! download failed for {meta.video_id}: {e}", file=sys.stderr)
            return None


# Minimum acceptable merged-file size; below this it's a failed/empty download.
MIN_FILE_BYTES = 100_000  # 100 KB


# ---------------------------------------------------------------------------
# Orchestration — three modes (§7.3 decoupled ingestion):
#   discover : search + score + enqueue, no media (cheap, API-bound)
#   download : drain the 'kept' queue, no search (heavy, bandwidth-bound)
#   full     : both, in one pass (default; back-compat with the old behavior)
# ---------------------------------------------------------------------------
def discover(src: YouTubeSource, store: Store, queries: Iterable[str],
             max_per_query: int, threshold: float, refresh_seconds: int) -> tuple[int, int]:
    kept = scanned = 0
    for q in queries:
        if store.query_fresh(q, refresh_seconds):
            print(f"\n=== query: {q!r} (cached, skipped) ===")
            continue
        print(f"\n=== query: {q!r} ===")
        for meta in src.search(q, max_per_query):
            scanned += 1
            key = f"{meta.platform}:{meta.video_id}"
            if store.is_seen(key):
                continue
            if store.is_dup_title(meta.title):  # near-duplicate of a stored video
                print(f"  [dup] {meta.title[:66]}")
                store.enqueue(meta, "skipped")
                continue
            meta.score = score_metadata(meta)
            keep = meta.score >= threshold
            print(f"  [{'KEEP' if keep else 'skip'} {meta.score:.2f}] {meta.title[:70]}")
            store.enqueue(meta, "kept" if keep else "skipped")
            kept += int(keep)
        store.mark_query_run(q)
    return scanned, kept


def drain_downloads(store: Store, limiter: RateLimiter, cfg: DownloadOpts) -> int:
    pending = store.pending_downloads()
    print(f"\n=== download queue: {len(pending)} pending ===")
    done = failed = 0
    for meta in pending:
        path = download(meta, store.videos_dir, limiter, cfg)
        # integrity check: a real video must exist and clear the size floor
        if not path or not os.path.exists(path) or os.path.getsize(path) < MIN_FILE_BYTES:
            store.mark_failed(meta, "missing/tiny file")
            if path and os.path.exists(path):
                os.remove(path)  # drop the empty/partial artifact
            failed += 1
            continue
        store.mark_downloaded(meta, path)
        done += 1
        print(f"  + {meta.video_id} -> {path}  ({meta.width}x{meta.height}, "
              f"{meta.filesize_mb:.1f}MB, {meta.actual_duration}s)")
    print(f"  ({done} ok, {failed} failed)")
    return done


def rescore(store: Store, threshold: float, delete_junk: bool) -> None:
    """Re-score every stored row with the CURRENT engine (no API calls).

    - pending rows: flip kept<->skipped by the new score
    - already-downloaded rows that now fail: report (and optionally delete the
      file + mark 'skipped'). Downloaded rows that still pass are left as-is.
    """
    rows = store.db.execute(
        "SELECT key, title, description, tags, status, path FROM videos").fetchall()
    rejects: list[tuple[str, str, float]] = []  # (path, title, size_mb)
    for r in rows:
        meta = VideoMeta("", "", "", r["title"] or "", r["description"] or "",
                         json.loads(r["tags"] or "[]"))
        s = score_metadata(meta)
        keep = s >= threshold
        if r["status"] in ("kept", "skipped"):
            store.db.execute("UPDATE videos SET score=?, status=? WHERE key=?",
                             (s, "kept" if keep else "skipped", r["key"]))
        elif r["status"] == "downloaded":
            store.db.execute("UPDATE videos SET score=? WHERE key=?", (s, r["key"]))
            if not keep:  # downloaded but now fails the tightened engine
                p = r["path"]
                size = os.path.getsize(p) / 1e6 if p and os.path.exists(p) else 0.0
                rejects.append((p, r["title"] or "", size))
    store.db.commit()

    n_kept = store.db.execute("SELECT count(*) FROM videos WHERE status='kept'").fetchone()[0]
    n_skip = store.db.execute("SELECT count(*) FROM videos WHERE status='skipped'").fetchone()[0]
    print(f"rescored {len(rows)} rows @ threshold {threshold}")
    print(f"pending queue now: kept={n_kept}  skipped={n_skip}")
    print(f"\nalready-DOWNLOADED files that now FAIL the engine: {len(rejects)}")
    total = sum(s for _, _, s in rejects)
    for p, t, s in rejects:
        print(f"  {s:6.1f}MB  {t[:60]}")
    print(f"  -> total {total:.0f} MB")
    if delete_junk:
        deleted = 0
        for p, _, _ in rejects:
            if p and os.path.exists(p):
                os.remove(p)
                deleted += 1
        # downloaded->skipped for the deleted ones
        for p, _, _ in rejects:
            store.db.execute("UPDATE videos SET status='skipped', path=NULL WHERE path=?", (p,))
        store.db.commit()
        print(f"\ndeleted {deleted} junk files, marked them skipped.")
    elif rejects:
        listing = store.out_dir / "rescore_rejects.txt"
        listing.write_text("\n".join(f"{s:.1f}MB\t{p}\t{t}" for p, t, s in rejects),
                           encoding="utf-8")
        print(f"\n(not deleted) list written to {listing} — re-run with --delete-junk to remove.")


def enrich(src: YouTubeSource, store: Store) -> None:
    """Backfill rich metadata (duration/stats/etc.) for existing rows via the
    API, recompute norm_title + is_short, and integrity-check downloaded files."""
    rows = store.db.execute("SELECT key, video_id, title, status, path FROM videos").fetchall()
    ids = [r["video_id"] for r in rows]
    by_id: dict[str, VideoMeta] = {}
    for i in range(0, len(ids), 50):
        for m in src._hydrate(ids[i:i + 50]):
            by_id[m.video_id] = m
    updated = 0
    for r in rows:
        m = by_id.get(r["video_id"])
        if m:
            store.db.execute(
                "UPDATE videos SET channel_id=?, duration_sec=?, definition=?, "
                "has_caption=?, category_id=?, default_language=?, live_content=?, "
                "view_count=?, like_count=?, comment_count=?, thumbnail=?, is_short=?, "
                "norm_title=? WHERE key=?",
                (m.channel_id, m.duration_sec, m.definition, int(m.has_caption),
                 m.category_id, m.default_language, m.live_content, m.view_count,
                 m.like_count, m.comment_count, m.thumbnail, int(m.is_short),
                 _norm_title(m.title), r["key"]),
            )
            updated += 1
        else:  # video deleted/private since download — still set norm_title
            store.db.execute("UPDATE videos SET norm_title=? WHERE key=?",
                             (_norm_title(r["title"] or ""), r["key"]))
    store.db.commit()
    # integrity check existing downloaded files
    failed = 0
    for r in rows:
        if r["status"] == "downloaded":
            p = r["path"]
            if not p or not os.path.exists(p) or os.path.getsize(p) < MIN_FILE_BYTES:
                store.db.execute("UPDATE videos SET status='failed' WHERE key=?", (r["key"],))
                failed += 1
    store.db.commit()
    print(f"enriched {updated}/{len(rows)} rows from API; "
          f"flagged {failed} downloaded files as failed (missing/tiny).")
    print(f"({len(rows) - updated} rows had no API data — deleted/private videos.)")


def run(queries: Iterable[str], out_dir: Path, max_per_query: int, threshold: float,
        mode: str, rps: float, refresh_seconds: int, dl: DownloadOpts,
        delete_junk: bool = False) -> None:
    store = Store(out_dir)
    limiter = RateLimiter(rps)
    scanned = kept = downloaded = 0
    try:
        if mode == "rescore":
            rescore(store, threshold, delete_junk)
            return
        if mode == "enrich":
            api_key = os.environ.get("YOUTUBE_API_KEY")
            if not api_key:
                sys.exit("Set YOUTUBE_API_KEY (enrich needs the API).")
            enrich(YouTubeSource(api_key, limiter, dl.proxy), store)
            return
        if mode in ("discover", "full"):
            api_key = os.environ.get("YOUTUBE_API_KEY")
            if not api_key:
                sys.exit("Set YOUTUBE_API_KEY (see header of this file).")
            src = YouTubeSource(api_key, limiter, dl.proxy)
            try:
                scanned, kept = discover(src, store, queries, max_per_query,
                                         threshold, refresh_seconds)
            except QuotaExceeded as e:
                print(f"\n! API quota exhausted, stopping discovery: {e}", file=sys.stderr)
        if mode in ("download", "full"):
            downloaded = drain_downloads(store, limiter, dl)
        print(f"\nDone. mode={mode} scanned={scanned} kept={kept} "
              f"downloaded={downloaded} -> {store.manifest}")
    finally:
        store.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CCTV/home-security video collector (YouTube).")
    p.add_argument("--query", action="append", dest="queries", default=[],
                   help="Search query (repeatable). Required for discover/full modes.")
    p.add_argument("--query-file", type=Path,
                   help="Text file of queries, one per line (# comments allowed).")
    p.add_argument("--out", default="dataset", type=Path, help="Output directory.")
    p.add_argument("--max-per-query", type=int, default=25)
    p.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                   help="Min relevance score (0..1) to keep a video.")
    p.add_argument("--mode",
                   choices=["discover", "download", "full", "eval", "rescore", "enrich"],
                   default="full",
                   help="discover=search only, download=drain queue, full=both, "
                        "eval=score a labeled CSV, rescore=re-score stored rows "
                        "(no API), enrich=backfill metadata for existing rows (API).")
    p.add_argument("--delete-junk", action="store_true",
                   help="In rescore mode, delete already-downloaded files that now fail.")
    p.add_argument("--labels", type=Path,
                   help="Labeled CSV for --mode eval (columns: label[,title,description,tags]).")
    p.add_argument("--rps", type=float, default=1.0,
                   help="Max outbound requests/sec (token bucket).")
    p.add_argument("--refresh-seconds", type=int, default=0,
                   help="Skip a query if searched within this window (0=always run).")
    p.add_argument("--proxy", default=os.environ.get("DATASET_PROXY"),
                   help="Proxy URL for API + download legs (or set DATASET_PROXY).")
    # §7.7 anti-bot knobs for the yt-dlp download leg
    g = p.add_argument_group("anti-bot (download leg)")
    g.add_argument("--cookies", default=os.environ.get("YOUTUBE_COOKIES") or None,
                   help="Netscape cookies.txt with a logged-in YouTube session "
                        "(or set YOUTUBE_COOKIES).")
    g.add_argument("--cookies-from-browser",
                   default=os.environ.get("YOUTUBE_COOKIES_FROM_BROWSER") or None,
                   help="Pull live cookies from a browser, e.g. chrome|firefox|edge|brave "
                        "(or set YOUTUBE_COOKIES_FROM_BROWSER).")
    g.add_argument("--impersonate", default="chrome",
                   help="Browser TLS fingerprint (needs curl_cffi). '' to disable.")
    g.add_argument("--player-clients", default="tv,web_safari,web",
                   help="yt-dlp YouTube player_client fallback chain.")
    g.add_argument("--dl-sleep", default="2,6",
                   help="Randomized per-download sleep 'min,max' seconds.")
    g.add_argument("--max-height", type=int, default=0, help="Cap resolution (0=no cap).")
    g.add_argument("--max-seconds", type=int, default=0, help="Cap duration (0=no cap).")
    args = p.parse_args(argv)
    if args.query_file:
        args.queries += [ln.strip() for ln in
                         args.query_file.read_text(encoding="utf-8").splitlines()
                         if ln.strip() and not ln.strip().startswith("#")]
    if args.mode in ("discover", "full") and not args.queries:
        p.error("--query/--query-file is required for mode 'discover'/'full'.")
    if args.mode == "eval" and not args.labels:
        p.error("--labels is required for mode 'eval'.")
    # cookies.txt wins over browser extraction if both are set (avoid yt-dlp ambiguity)
    if args.cookies and args.cookies_from_browser:
        print("  ~ both cookie sources set - using cookies file, ignoring browser.",
              file=sys.stderr)
        args.cookies_from_browser = None
    if args.cookies and not Path(args.cookies).exists():
        p.error(f"--cookies file not found: {args.cookies}")
    return args


if __name__ == "__main__":
    # Windows consoles default to cp1252; YouTube titles carry emoji/non-latin.
    # Force UTF-8 so prints never crash the run (replace anything unmappable).
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    _load_dotenv()
    a = parse_args()
    if a.mode == "eval":
        evaluate(a.labels, a.threshold)
    else:
        smin, _, smax = a.dl_sleep.partition(",")
        dl = DownloadOpts(
            proxy=a.proxy, cookies_file=a.cookies,
            cookies_from_browser=a.cookies_from_browser,
            impersonate=a.impersonate or None,
            player_clients=a.player_clients,
            sleep_min=float(smin), sleep_max=float(smax or smin),
            max_height=a.max_height, max_seconds=a.max_seconds,
        )
        run(a.queries, a.out, a.max_per_query, a.threshold, a.mode, a.rps,
            a.refresh_seconds, dl, a.delete_junk)
