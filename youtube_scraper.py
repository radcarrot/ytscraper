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
    "trespasser": 0.5, "trespass": 0.5, "front gate": 0.5,
    # indoor rooms / roles — event-free interior home footage
    "indoor": 0.4, "living room": 0.6, "kitchen": 0.5, "bedroom": 0.5,
    "hallway": 0.4, "nursery": 0.5, "inside my house": 0.7, "inside my home": 0.7,
    "nanny cam": 0.7, "baby monitor": 0.5, "pet cam": 0.6, "pet camera": 0.6,
    # night / recording mode. NOTE: 'night vision'/'infrared'/'live feed|cam|stream'
    # REMOVED from positives (2026-06-26 hand-label analysis) — title hits on these
    # are only ~22-23% keep (product-spec + idle-stream language), so boosting them
    # cost precision. Now neutral; 'night vision camera' is a NEGATIVE below.
    "motion detection": 0.3, "time lapse": 0.4,
    "caught on tape": 0.5, "overnight": 0.4,
}
# NOTE: pure wildlife / trail-cam footage is NOT wanted — this dataset is for a
# home-security model, not nature. Animal-at-the-door clips still pass via the
# home/doorbell CONTEXT words; standalone wildlife/trail-cam is pushed down by
# the NEGATIVE entries below.

# Near-duplicate detection: two clips whose frame dhashes differ by <= this many
# bits (Hamming, out of 64) are treated as the same video. ~5 is a safe default.
PHASH_MAX_DISTANCE = 5
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
    # pure wildlife / trail-cam / nature — NOT wanted (home-security dataset).
    # Animal-at-a-home clips still pass via home/doorbell CONTEXT words.
    "trail cam": -1.5, "trail camera": -1.5, "wildlife": -1.2, "wilderness": -1.5,
    "nature cam": -1.2, "bird feeder": -1.2, "game camera": -1.5, "safari": -1.5,
    "national park": -1.5, "in the wild": -1.2,
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
    # plural/product phrasings surfaced by hand-label analysis (2026-06-26): title
    # hits on these are <25% keep — listicles, spec sheets, "camera for home" ads.
    # ('security cameras'/'cctv camera' still net-positive via STRONG, just damped.)
    "security cameras": -0.8, "cctv camera": -0.6, "night vision camera": -1.0,
    "camera for home": -1.0, "for home": -0.6, "wifi": -0.6,
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
    # ad-speak that appears in PRODUCT promos but not in real-footage titles.
    # Tuned against hand-labeled false positives: these are pure ads with NO
    # CCTV footage, so pushing them down doesn't cost any footage we want.
    "smart doorbell": -1.2, "smart home": -0.9, "must-have": -1.3, "must have": -1.3,
    "game changer": -1.5, "meet the": -1.2, "remote monitoring": -1.3,
    "monitor visitors": -1.3, "without subscription": -1.3, "you need": -1.2,
    "what's inside": -1.5, "whats inside": -1.5, "spy camera": -1.3,
    "spy magnet": -1.5, "ultimate": -0.8, "saved my home": -1.3, "3rd gen": -1.3,
    "remote view": -1.0, "high hd": -1.2, "1080p": -1.0, "2mp": -1.0, "4mp": -1.0,
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
    phash: str = ""                # perceptual dhash (hex) of a sampled frame
    src_query: str = ""            # discover query that surfaced this video
    score_terms: str = ""          # json: keywords that drove the score (analysis)


# Channels that almost exclusively post horror/creepy, comedy/reaction, or
# product content — higher-signal than keywords. Matched case-insensitively as
# substrings of the channel title. A hit applies CHANNEL_PENALTY.
CHANNEL_BLOCK = {
    # Horror/creepy/paranormal channels — fabricated or clickbait, never real
    # footage. Safe to veto.
    "mr. nightmare", "nightmare", "chilling scares", "horror channel",
    "lets read", "corpse", "wendigoon",
    # Pure product/ad channels — gear promos, no CCTV footage.
    "unboxing", "gadget review", "tech review", "camera review", "best deals",
    # NOTE: compilation/fails channels (America's Funniest, Brain Time, AFV,
    # 5-Minute, etc.) are NOT blocked — they post REAL footage montages we keep.
    # The labeling criterion is "does the clip contain CCTV footage", and these
    # do; blocking them was dropping wanted videos (false negatives).
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


# TV-news channels are dropped entirely: most clips are anchor/commentary with no
# CCTV footage, and the ones that do embed footage need manual trimming we don't
# want. Detect US station call-signs (uppercase K/W + letters at start) plus
# network/news terms. The user accepts losing the rare footage-bearing news clip.
_NEWS_CALLSIGN = re.compile(r"^[KW][A-Z]{2,3}\b")
_NEWS_TERMS = (
    "news", "abc", "cbs", "nbc", "fox", "cnn", "ndtv", "cbc", "wion",
    "eyewitness", "inside edition", "action news", "on your side",
    "aaj tak", "republic tv", "telemundo", "univision",
)


def _is_news_channel(channel: str) -> bool:
    if not channel:
        return False
    if _NEWS_CALLSIGN.match(channel):  # call-signs are uppercase, match raw case
        return True
    c = channel.lower()
    return any(t in c for t in _NEWS_TERMS)


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


def score_breakdown(meta: VideoMeta) -> tuple[float, dict]:
    """Like score_metadata but also returns WHICH keywords fired, for later
    analysis. The dict is {"pos":[[field,kw,weight],...], "neg":[...],
    optional "channel_penalty":w}. Terms are the raw matches (pre field-cap), so
    cross-reference POS_CAP_PER_FIELD / NEG_CAP_PER_FIELD when summing by hand.
    """
    fields = {
        "title": meta.title.lower(),
        "tags": " ".join(meta.tags).lower(),
        "description": meta.description.lower(),
    }
    raw = 0.0
    hits: dict = {"pos": [], "neg": []}
    for fname, text in fields.items():
        fw = FIELD_WEIGHT[fname]
        pos_terms = [(kw, w) for kw, w in _POSWORDS.items() if kw in text]
        neg_terms = [(kw, w) for kw, w in NEGATIVE.items() if kw in text]
        raw += min(sum(w for _, w in pos_terms), POS_CAP_PER_FIELD) * fw
        raw += max(sum(w for _, w in neg_terms), -NEG_CAP_PER_FIELD) * fw
        hits["pos"] += [[fname, kw, w] for kw, w in pos_terms]
        hits["neg"] += [[fname, kw, w] for kw, w in neg_terms]
    if meta.channel and _channel_blocked(meta.channel):
        raw += CHANNEL_PENALTY
        hits["channel_penalty"] = CHANNEL_PENALTY
    return round(_squash(raw), 4), hits


def score_metadata(meta: VideoMeta) -> float:
    """Weighted keyword match across title / tags / description -> 0..1.

    Positive hits are summed per field then capped (POS_CAP_PER_FIELD) and
    field-weighted, so a keyword-stuffed description can't saturate the score.
    Negative hits are full-strength and uncapped (counted in any field) so a
    junk signal vetoes an otherwise-stuffed entry.
    """
    return score_breakdown(meta)[0]


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
    _report_eval(rows, threshold)


def _report_eval(rows: list[tuple[bool, float]], threshold: float) -> None:
    """Print precision/recall/F1 + a threshold sweep for (truth, score) rows."""
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


def evaluate_db(store: Store, threshold: float) -> None:
    """Evaluate the scorer against HUMAN labels stored in the DB (set via
    label_app.py). This is real validation — your judgment vs. the scorer —
    unlike the synthetic sample CSV."""
    cur = store.db.execute(
        "SELECT title, description, tags, channel, score, human_label FROM videos "
        "WHERE human_label IS NOT NULL")
    rows: list[tuple[bool, float]] = []
    for r in cur.fetchall():
        # re-score live so eval reflects the CURRENT engine, not the stored score
        meta = VideoMeta("", "", "", r["title"] or "", r["description"] or "",
                         json.loads(r["tags"] or "[]"), channel=r["channel"] or "")
        rows.append((bool(r["human_label"]), score_metadata(meta)))
    if not rows:
        sys.exit("No human-labeled rows yet. Run: python label_app.py --out <dir>")
    print(f"evaluating against {len(rows)} HUMAN-labeled videos from the DB\n")
    _report_eval(rows, threshold)


# ---------------------------------------------------------------------------
# Local LLM classifier (Ollama / Gemma). Reads ONLY metadata — title, desc,
# tags, channel — and judges keep/skip semantically. Stdlib urllib, no new dep.
# Used to bake-off against the keyword scorer on the human-labeled set.
# ---------------------------------------------------------------------------
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434/api/chat")
OLLAMA_MODEL = "gemma3:4b"

_LLM_SYSTEM = (
    "You classify YouTube videos for a RESIDENTIAL home-security CCTV dataset, "
    "using ONLY the metadata. The ONE question that decides it: does the video "
    "CONTAIN actual home-security / CCTV / doorbell camera footage of a house, "
    "yard, porch, driveway, or home interior?\n\n"
    "KEEP if the video shows such footage — REGARDLESS of anything else. "
    "Specifically, these are NOT reasons to skip, keep them if footage is shown:\n"
    "  - monetization: affiliate/Amazon links, 'subscribe', merch, promo codes\n"
    "  - format: compilations, 'funniest'/'fails'/'caught in 4K'/karma/comedy montages\n"
    "  - news clips that play real security-camera footage of the incident\n"
    "  - AI-generated or recreated clips, AS LONG AS they look like CCTV footage\n"
    "  - quiet/ambient footage with no event; wildlife on a home camera\n\n"
    "SKIP only when the video does NOT contain residential camera footage:\n"
    "  - product reviews / unboxings / buying guides / ads ABOUT a camera "
    "(the video is gear talk, not footage)\n"
    "  - store / traffic / business CCTV, dashcam\n"
    "  - pure talking-head news or commentary with no camera footage shown\n"
    "  - tutorials / app how-tos\n\n"
    "Judge by what the video CONTAINS, not by whether it also sells something. "
    "A funny doorbell compilation with Amazon links is KEEP (it shows footage). "
    "A review of a doorbell camera is SKIP (it shows the product, not footage). "
    "Reply ONLY with JSON: "
    '{"label":"keep"|"skip","confidence":0.0-1.0,"reason":"<short>"}'
)


def llm_classify(title: str, description: str, tags: list[str], channel: str,
                 model: str = OLLAMA_MODEL, timeout: float = 60.0
                 ) -> tuple[int, float, str]:
    """Classify one video via the local Ollama chat API. Returns
    (label 1/0, confidence 0..1, reason). Raises RuntimeError if Ollama is
    unreachable so the caller can stop with a clear message."""
    import urllib.request
    import urllib.error

    user = (
        f"Title: {title}\n"
        f"Channel: {channel}\n"
        f"Tags: {', '.join(tags) if tags else '(none)'}\n"
        f"Description: {(description or '(none)')[:1500]}"
    )
    options = {"temperature": 0.0}
    # Optional GPU/CPU split: OLLAMA_NUM_GPU = number of layers to offload to the
    # GPU (rest run on CPU/RAM). Use a partial value when the model is bigger than
    # VRAM (0 = all CPU). Applied per-request, no Ollama restart needed.
    _ngpu = os.environ.get("OLLAMA_NUM_GPU")
    if _ngpu is not None and _ngpu.strip() != "":
        try:
            options["num_gpu"] = int(_ngpu)
        except ValueError:
            pass
    payload = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": _LLM_SYSTEM},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "format": "json",
        "options": options,
    }).encode("utf-8")
    req = urllib.request.Request(OLLAMA_URL, data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Ollama unreachable at {OLLAMA_URL} ({e}). Start Ollama and run "
            f"`ollama pull {model}`.") from e
    content = (body.get("message") or {}).get("content", "")
    try:
        verdict = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        # model didn't return clean JSON — treat as low-confidence skip
        return 0, 0.0, f"unparseable: {content[:80]}"
    label = 1 if str(verdict.get("label", "")).lower().startswith("keep") else 0
    try:
        conf = float(verdict.get("confidence", 0.5))
    except (TypeError, ValueError):
        conf = 0.5
    conf = max(0.0, min(1.0, conf))
    reason = str(verdict.get("reason", ""))[:300]
    return label, conf, reason


def llm_classify_store(store: Store, model: str, relabel: bool) -> int:
    """Classify the human-labeled rows with the LLM, caching verdicts to the DB.
    Resumable: skips rows that already have llm_label unless relabel=True."""
    where = "human_label IS NOT NULL"
    if not relabel:
        where += " AND llm_label IS NULL"
    rows = store.db.execute(
        f"SELECT key, title, description, tags, channel FROM videos WHERE {where}"
    ).fetchall()
    total_labeled = store.db.execute(
        "SELECT count(*) FROM videos WHERE human_label IS NOT NULL").fetchone()[0]
    if not rows:
        print(f"all {total_labeled} labeled rows already classified "
              f"(use --relabel to redo).")
        return 0
    print(f"classifying {len(rows)} rows with {model} "
          f"({total_labeled - len(rows)} already cached)...")
    done = 0
    for r in rows:
        tags = json.loads(r["tags"] or "[]")
        label, conf, reason = llm_classify(
            r["title"] or "", r["description"] or "", tags, r["channel"] or "", model)
        store.db.execute(
            "UPDATE videos SET llm_label=?, llm_confidence=?, llm_reason=? WHERE key=?",
            (label, conf, reason, r["key"]))
        store.db.commit()
        done += 1
        if done % 10 == 0 or done == len(rows):
            print(f"  {done}/{len(rows)}")
    return done


def evaluate_llm(store: Store, model: str, threshold: float, relabel: bool) -> None:
    """Classify (or reuse cached) LLM verdicts on the human-labeled set, then
    report precision/recall/F1 of LLM-vs-human — comparable to the keyword eval.
    Score per row = confidence if keep else 1-confidence, so the sweep works."""
    llm_classify_store(store, model, relabel)
    cur = store.db.execute(
        "SELECT human_label, llm_label, llm_confidence FROM videos "
        "WHERE human_label IS NOT NULL AND llm_label IS NOT NULL")
    rows: list[tuple[bool, float]] = []
    for r in cur.fetchall():
        conf = r["llm_confidence"] if r["llm_confidence"] is not None else 0.5
        score = conf if r["llm_label"] else (1.0 - conf)
        rows.append((bool(r["human_label"]), score))
    if not rows:
        sys.exit("No LLM verdicts to evaluate.")
    print(f"\nLLM ({model}) vs {len(rows)} HUMAN labels "
          f"(keyword scorer baseline: precision 0.830 / recall 1.000 / F1 0.907)\n")
    _report_eval(rows, threshold)


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


class QuotaMeter:
    """Track estimated Data API unit spend so a big multi-query run doesn't hit
    a surprise hard-stop. search.list=100u, videos.list=1u. Warns near the cap
    and raises QuotaExceeded once the local limit is crossed (0 = no local cap).
    """
    def __init__(self, limit: int = 10_000, warn_at: float = 0.8):
        self.limit = limit
        self.warn_at = warn_at
        self.used = 0
        self._warned = False

    def charge(self, units: int, what: str) -> None:
        self.used += units
        if self.limit > 0:
            if not self._warned and self.used >= self.limit * self.warn_at:
                self._warned = True
                print(f"  ~ quota ~{self.used}/{self.limit} units used "
                      f"({self.used / self.limit:.0%}) — approaching daily cap.",
                      file=sys.stderr)
            if self.used >= self.limit:
                raise QuotaExceeded(
                    f"local quota cap reached (~{self.used}/{self.limit} units) "
                    f"before {what}; raise --quota-limit or wait for daily reset.")


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

    def __init__(self, api_key: str, limiter: RateLimiter, proxy: str | None = None,
                 quota: QuotaMeter | None = None):
        from googleapiclient.discovery import build  # lazy import
        self.limiter = limiter
        self.quota = quota or QuotaMeter(limit=0)  # 0 = no local cap
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
            self.quota.charge(100, f"search.list({query!r})")  # 100u each
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
            self.quota.charge(1, "videos.list")  # 1u regardless of parts/ids
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
        ("actual_duration", "INTEGER"), ("phash", "TEXT"),
        ("human_label", "INTEGER"),  # 1/0 set by the manual labeler (label_app.py)
        ("llm_label", "INTEGER"),    # 1/0 from the local LLM (Ollama/Gemma)
        ("llm_confidence", "REAL"), ("llm_reason", "TEXT"),
        ("query", "TEXT"),  # the discover query that surfaced this row (per-query analysis)
        ("score_terms", "TEXT"),  # json: keywords that drove the score (analysis)
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

    def find_phash_dup(self, phash: str, exclude_key: str,
                       max_dist: int = PHASH_MAX_DISTANCE) -> str | None:
        """Return the key of an already-stored video whose frame dhash is within
        max_dist of `phash` (a content re-upload under a different title), or None.
        Only compares against rows that have a phash and a real file."""
        if not phash:
            return None
        cur = self.db.execute(
            "SELECT key, phash FROM videos WHERE phash IS NOT NULL AND phash<>'' "
            "AND status='downloaded' AND key<>?", (exclude_key,))
        for r in cur.fetchall():
            if _phash_distance(phash, r["phash"]) <= max_dist:
                return r["key"]
        return None

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
            "comment_count, thumbnail, is_short, query, score_terms) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (key, meta.video_id, meta.platform, meta.url, meta.title,
             meta.description, json.dumps(meta.tags), meta.channel, meta.channel_id,
             meta.published_at, meta.score, status,
             datetime.now(timezone.utc).isoformat(timespec="seconds"),
             _norm_title(meta.title), meta.duration_sec, meta.definition,
             int(meta.has_caption), meta.category_id, meta.default_language,
             meta.live_content, meta.view_count, meta.like_count, meta.comment_count,
             meta.thumbnail, int(meta.is_short), meta.src_query, meta.score_terms),
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
            "width=?, height=?, fps=?, ext=?, filesize_mb=?, actual_duration=?, "
            "phash=? WHERE key=?",
            (path, now, meta.width, meta.height, meta.fps, meta.ext,
             round(meta.filesize_mb, 2), meta.actual_duration, meta.phash, key),
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
            actual_duration=g("actual_duration") or 0, phash=g("phash") or "",
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
            meta.phash = _frame_dhash(path) or ""  # for near-dup detection
            return path
        except Exception as e:  # noqa: BLE001 - keep the run going
            print(f"  ! download failed for {meta.video_id}: {e}", file=sys.stderr)
            return None


# Minimum acceptable merged-file size; below this it's a failed/empty download.
MIN_FILE_BYTES = 100_000  # 100 KB

# Quality floors (0 = disabled). Applied at enqueue (API duration) and after
# download (decoded height). Overridable by --min-seconds / --min-height.
DEFAULT_MIN_SECONDS = 0
DEFAULT_MIN_HEIGHT = 0


def _frame_dhash(path: str) -> str | None:
    """Perceptual 64-bit difference-hash of one sampled frame, as hex.

    Extracts a single 9x8 grayscale frame via ffmpeg (no PIL needed) and builds
    a row-wise dhash. Returns None if extraction fails (corrupt/short clip)."""
    import subprocess
    try:
        import imageio_ffmpeg
        exe = imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:  # noqa: BLE001
        return None

    def _grab(seek: str) -> bytes:
        cmd = [exe, "-v", "error", "-ss", seek, "-i", path, "-frames:v", "1",
               "-vf", "scale=9:8,format=gray", "-f", "rawvideo", "-"]
        try:
            return subprocess.run(cmd, capture_output=True, timeout=30).stdout
        except Exception:  # noqa: BLE001
            return b""

    raw = _grab("1")
    if len(raw) < 72:            # short clip — try the very first frame
        raw = _grab("0")
    if len(raw) < 72:
        return None
    px = raw[:72]                # 9 wide x 8 tall, 1 byte/pixel
    bits = 0
    for row in range(8):
        for col in range(8):     # 8 adjacent-column comparisons per row
            left = px[row * 9 + col]
            right = px[row * 9 + col + 1]
            bits = (bits << 1) | (1 if left > right else 0)
    return f"{bits:016x}"


def _phash_distance(a: str, b: str) -> int:
    """Hamming distance between two hex dhashes; 999 if either is missing."""
    if not a or not b:
        return 999
    try:
        return bin(int(a, 16) ^ int(b, 16)).count("1")
    except ValueError:
        return 999


# ---------------------------------------------------------------------------
# Orchestration — three modes (§7.3 decoupled ingestion):
#   discover : search + score + enqueue, no media (cheap, API-bound)
#   download : drain the 'kept' queue, no search (heavy, bandwidth-bound)
#   full     : both, in one pass (default; back-compat with the old behavior)
# ---------------------------------------------------------------------------
def discover(src: YouTubeSource, store: Store, queries: Iterable[str],
             max_per_query: int, threshold: float, refresh_seconds: int,
             min_seconds: int = 0) -> tuple[int, int]:
    kept = scanned = 0
    for q in queries:
        if store.query_fresh(q, refresh_seconds):
            print(f"\n=== query: {q!r} (cached, skipped) ===")
            continue
        print(f"\n=== query: {q!r} ===")
        for meta in src.search(q, max_per_query):
            scanned += 1
            meta.src_query = q   # tag source query before any enqueue path
            key = f"{meta.platform}:{meta.video_id}"
            if store.is_seen(key):
                continue
            if store.is_dup_title(meta.title):  # near-duplicate of a stored video
                print(f"  [dup] {meta.title[:66]}")
                store.enqueue(meta, "skipped")
                continue
            # quality floor: drop clips shorter than min_seconds (when API knows
            # the duration; duration_sec==0 means unknown, so don't filter).
            if min_seconds and 0 < meta.duration_sec < min_seconds:
                print(f"  [short {meta.duration_sec}s] {meta.title[:62]}")
                store.enqueue(meta, "skipped")
                continue
            meta.score, _terms = score_breakdown(meta)
            meta.score_terms = json.dumps(_terms, ensure_ascii=False)
            keep = meta.score >= threshold
            print(f"  [{'KEEP' if keep else 'skip'} {meta.score:.2f}] {meta.title[:70]}")
            store.enqueue(meta, "kept" if keep else "skipped")
            kept += int(keep)
        store.mark_query_run(q)
    return scanned, kept


def drain_downloads(store: Store, limiter: RateLimiter, cfg: DownloadOpts,
                    min_height: int = 0) -> int:
    pending = store.pending_downloads()
    print(f"\n=== download queue: {len(pending)} pending ===")
    done = failed = dropped = 0
    for meta in pending:
        path = download(meta, store.videos_dir, limiter, cfg)
        # integrity check: a real video must exist and clear the size floor
        if not path or not os.path.exists(path) or os.path.getsize(path) < MIN_FILE_BYTES:
            store.mark_failed(meta, "missing/tiny file")
            if path and os.path.exists(path):
                os.remove(path)  # drop the empty/partial artifact
            failed += 1
            continue
        # quality floor: drop low-resolution clips below min_height
        if min_height and 0 < meta.height < min_height:
            os.remove(path)
            store.mark_failed(meta, f"below {min_height}p ({meta.height}p)")
            dropped += 1
            continue
        # near-duplicate: same content re-uploaded under a different title
        key = f"{meta.platform}:{meta.video_id}"
        dup = store.find_phash_dup(meta.phash, key)
        if dup:
            os.remove(path)
            store.mark_failed(meta, f"phash-dup of {dup}")
            print(f"  [dup~] {meta.video_id} matches {dup} — dropped")
            dropped += 1
            continue
        store.mark_downloaded(meta, path)
        done += 1
        print(f"  + {meta.video_id} -> {path}  ({meta.width}x{meta.height}, "
              f"{meta.filesize_mb:.1f}MB, {meta.actual_duration}s)")
    print(f"  ({done} ok, {failed} failed, {dropped} dropped [low-res/dup])")
    return done


def rescore(store: Store, threshold: float, delete_junk: bool) -> None:
    """Re-score every stored row with the CURRENT engine (no API calls).

    - pending rows: flip kept<->skipped by the new score
    - already-downloaded rows that now fail: report (and optionally delete the
      file + mark 'skipped'). Downloaded rows that still pass are left as-is.
    """
    rows = store.db.execute(
        "SELECT key, title, description, tags, channel, status, path FROM videos").fetchall()
    rejects: list[tuple[str, str, float]] = []  # (path, title, size_mb)
    for r in rows:
        meta = VideoMeta("", "", "", r["title"] or "", r["description"] or "",
                         json.loads(r["tags"] or "[]"), channel=r["channel"] or "")
        s, terms = score_breakdown(meta)
        st = json.dumps(terms, ensure_ascii=False)
        keep = s >= threshold
        if r["status"] in ("kept", "skipped"):
            store.db.execute("UPDATE videos SET score=?, status=?, score_terms=? WHERE key=?",
                             (s, "kept" if keep else "skipped", st, r["key"]))
        elif r["status"] == "downloaded":
            store.db.execute("UPDATE videos SET score=?, score_terms=? WHERE key=?", (s, st, r["key"]))
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


def dedupe(store: Store, delete: bool) -> None:
    """Find near-duplicate DOWNLOADED files via perceptual frame hash and report
    (or delete with --delete-junk). Backfills phash for rows missing it. Keeps
    the first occurrence of each cluster, flags the rest as duplicates."""
    rows = store.db.execute(
        "SELECT key, video_id, title, path, phash FROM videos "
        "WHERE status='downloaded'").fetchall()
    # backfill missing phashes from the files on disk
    backfilled = 0
    hashes: list[tuple[str, str, str, str]] = []  # (key, title, path, phash)
    for r in rows:
        ph = r["phash"]
        p = r["path"]
        if (not ph) and p and os.path.exists(p):
            ph = _frame_dhash(p) or ""
            if ph:
                store.db.execute("UPDATE videos SET phash=? WHERE key=?", (ph, r["key"]))
                backfilled += 1
        hashes.append((r["key"], r["title"] or "", p or "", ph))
    store.db.commit()

    kept: list[tuple[str, str, str, str]] = []
    dups: list[tuple[str, str, str]] = []  # (path, title, matches_key)
    for key, title, path, ph in hashes:
        if not ph:
            kept.append((key, title, path, ph))   # can't hash — keep it
            continue
        match = next((k for k, _, _, kph in kept
                      if _phash_distance(ph, kph) <= PHASH_MAX_DISTANCE), None)
        if match:
            dups.append((path, title, match))
        else:
            kept.append((key, title, path, ph))

    print(f"scanned {len(rows)} downloaded files; backfilled {backfilled} phashes.")
    print(f"near-duplicate clusters found: {len(dups)} extra copies")
    for path, title, match in dups:
        print(f"  dup of {match}: {title[:60]}")
    if delete:
        removed = 0
        for path, _, _ in dups:
            if path and os.path.exists(path):
                os.remove(path)
                removed += 1
            store.db.execute(
                "UPDATE videos SET status='skipped', path=NULL WHERE path=?", (path,))
        store.db.commit()
        print(f"\ndeleted {removed} duplicate files, marked them skipped.")
    elif dups:
        print("\n(not deleted) re-run with --delete-junk to remove duplicates.")


def run(queries: Iterable[str], out_dir: Path, max_per_query: int, threshold: float,
        mode: str, rps: float, refresh_seconds: int, dl: DownloadOpts,
        delete_junk: bool = False, min_seconds: int = 0, min_height: int = 0,
        quota_limit: int = 10_000) -> None:
    store = Store(out_dir)
    limiter = RateLimiter(rps)
    scanned = kept = downloaded = 0
    quota = QuotaMeter(limit=quota_limit)
    try:
        if mode == "rescore":
            rescore(store, threshold, delete_junk)
            return
        if mode == "dedupe":
            dedupe(store, delete_junk)
            return
        if mode == "enrich":
            api_key = os.environ.get("YOUTUBE_API_KEY")
            if not api_key:
                sys.exit("Set YOUTUBE_API_KEY (enrich needs the API).")
            enrich(YouTubeSource(api_key, limiter, dl.proxy, quota), store)
            return
        if mode in ("discover", "full"):
            api_key = os.environ.get("YOUTUBE_API_KEY")
            if not api_key:
                sys.exit("Set YOUTUBE_API_KEY (see header of this file).")
            src = YouTubeSource(api_key, limiter, dl.proxy, quota)
            try:
                scanned, kept = discover(src, store, queries, max_per_query,
                                         threshold, refresh_seconds, min_seconds)
            except QuotaExceeded as e:
                print(f"\n! API quota exhausted, stopping discovery: {e}", file=sys.stderr)
        if mode in ("download", "full"):
            downloaded = drain_downloads(store, limiter, dl, min_height)
        if mode in ("discover", "full"):
            print(f"  (~{quota.used} API units used this run)")
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
                   choices=["discover", "download", "full", "eval", "rescore",
                            "enrich", "dedupe", "llm-eval"],
                   default="full",
                   help="discover=search only, download=drain queue, full=both, "
                        "eval=score a labeled CSV, rescore=re-score stored rows "
                        "(no API), enrich=backfill metadata for existing rows (API), "
                        "dedupe=remove near-duplicate downloaded files (no API), "
                        "llm-eval=classify human-labeled rows with a local LLM "
                        "(Ollama) and report precision/recall vs the keyword scorer.")
    p.add_argument("--delete-junk", action="store_true",
                   help="In rescore/dedupe modes, delete the flagged files.")
    p.add_argument("--llm-model", default=OLLAMA_MODEL,
                   help="Ollama model for --mode llm-eval (default gemma3:4b).")
    p.add_argument("--relabel", action="store_true",
                   help="In llm-eval, re-classify rows that already have a cached LLM verdict.")
    p.add_argument("--min-seconds", type=int, default=DEFAULT_MIN_SECONDS,
                   help="Drop clips shorter than this at discover (0=no floor).")
    p.add_argument("--min-height", type=int, default=DEFAULT_MIN_HEIGHT,
                   help="Drop downloads below this resolution height (0=no floor).")
    p.add_argument("--quota-limit", type=int, default=10_000,
                   help="Local Data API unit cap per run; stop before overrun (0=off).")
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
    # eval with no --labels falls back to human labels in the DB (label_app.py).
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
        if a.labels:
            evaluate(a.labels, a.threshold)
        else:  # no CSV → evaluate against human labels stored in the DB
            _store = Store(a.out)
            try:
                evaluate_db(_store, a.threshold)
            finally:
                _store.close()
    elif a.mode == "llm-eval":
        _store = Store(a.out)
        try:
            evaluate_llm(_store, a.llm_model, a.threshold, a.relabel)
        finally:
            _store.close()
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
            a.refresh_seconds, dl, a.delete_junk, a.min_seconds, a.min_height,
            a.quota_limit)
