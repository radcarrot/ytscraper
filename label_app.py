#!/usr/bin/env python3
"""
label_app.py — tiny local web UI to hand-label the collected videos.

Plays each downloaded clip in the browser and lets you mark it relevant (Keep)
or not (Skip) with one keypress. Your call is written straight back into
dataset.db (the `human_label` column), so it's resumable and feeds `--mode eval`
directly — no CSV round-trip.

    python label_app.py --out D:\\ytds
    # then open the browser tab it launches; label with the keyboard:
    #   K = keep (relevant)    J = skip (not relevant)    B = back    O = open on YouTube

Why this matters: the scorer and the synthetic sample CSV were both written by
the same hand, so eval against them is circular. YOUR labels are real ground
truth. Borderline clips (score near the threshold) are shown FIRST because
that's where the cutoff actually bites.

Notes:
- Only DOWNLOADED clips have a local file to play, so those are what you label.
  That measures false positives (junk that scored high) — the main risk after a
  cleanup. To also judge wrongly-SKIPPED videos, use the "Open on YouTube" link.
- Stdlib only. No extra pip installs.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote

DEFAULT_THRESHOLD = 0.35

GENGAR = ("https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/"
          "pokemon/other/official-artwork/94.png")

PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>CCTV dataset labeler</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,500;12..96,700;12..96,800&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
<style>
  *{box-sizing:border-box}
  html,body{margin:0;padding:0}
  body{font-family:'Space Mono',ui-monospace,monospace;color:#ece6f7;
    background:radial-gradient(1100px 650px at 85% -12%, #381f5e 0%, rgba(56,31,94,0) 58%),
               radial-gradient(900px 600px at -5% 112%, #271343 0%, rgba(39,19,67,0) 55%), #100a1c;
    min-height:100vh}
  #bg{position:fixed;right:-100px;bottom:-80px;width:400px;opacity:.05;pointer-events:none}
  #wrap{max-width:880px;margin:0 auto;padding:26px 18px 48px;position:relative;z-index:1}
  .head{display:flex;align-items:center;gap:14px;margin-bottom:22px;
    border-bottom:1px solid rgba(150,90,230,.18);padding-bottom:18px}
  .head img{width:48px;height:48px;object-fit:contain;
    filter:drop-shadow(0 0 12px rgba(150,90,230,.55));animation:ggFloat 4.5s ease-in-out infinite}
  .h-title{font-family:'Bricolage Grotesque',sans-serif;font-weight:800;font-size:23px;
    letter-spacing:-.3px;line-height:1}
  .h-sub{font-size:11px;color:#9c82cc;letter-spacing:.6px;margin-top:5px;text-transform:uppercase}
  .keys{display:flex;gap:7px;flex-wrap:wrap}
  .keys span{font-size:10.5px;color:#b6a0dd;border:1px solid rgba(150,90,230,.28);
    border-radius:5px;padding:3px 7px}
  .progbar{display:flex;gap:14px;margin:0 0 14px;align-items:center;flex-wrap:wrap}
  .progbar .ptext{font-size:12px;color:#a98fd6;letter-spacing:.4px}
  .ptrack{flex:1;min-width:120px;height:5px;border-radius:99px;background:rgba(150,90,230,.14);overflow:hidden}
  #pfill{display:block;height:100%;width:0%;background:linear-gradient(90deg,#8b5fd6,#c9a6ff)}
  #curlabel{font-size:11px;font-weight:700;letter-spacing:.6px;text-transform:uppercase}
  .vidbox{position:relative;border-radius:12px;overflow:hidden;border:1px solid rgba(150,90,230,.3);
    box-shadow:0 18px 50px rgba(24,6,52,.55);aspect-ratio:16/9;max-height:60vh;background:#0a0710}
  video{width:100%;height:100%;object-fit:contain;background:#0a0710;display:block}
  .rec{position:absolute;top:11px;left:12px;display:flex;align-items:center;gap:6px;
    font-size:10.5px;font-weight:700;color:#e6473a;letter-spacing:1px;pointer-events:none;
    text-shadow:0 1px 2px #000}
  .rec span{width:7px;height:7px;border-radius:50%;background:#e6473a;box-shadow:0 0 8px #e6473a}
  .cam{position:absolute;bottom:11px;right:13px;font-size:11.5px;color:#9d86c7;letter-spacing:.5px;
    text-shadow:0 1px 2px #000;pointer-events:none}
  .bar{display:flex;gap:9px;margin:16px 0;flex-wrap:wrap}
  button{font-family:'Space Mono',monospace;font-size:14px;font-weight:700;letter-spacing:.5px;
    padding:12px 18px;border-radius:8px;cursor:pointer;display:flex;align-items:center;gap:9px;
    background:transparent;color:#c6b2ec;border:1px solid rgba(150,90,230,.3)}
  button kbd{font-size:11px;border:1px solid rgba(198,178,236,.35);border-radius:4px;
    padding:1px 6px;opacity:.8;font-family:inherit}
  .keep{border-color:#3fc063;color:#9dffb8;background:rgba(63,192,99,.12)}
  .keep kbd{border-color:rgba(157,255,184,.4)}
  .skip{border-color:#cf2f5f;color:#ff9bb6;background:rgba(207,47,95,.12)}
  .skip kbd{border-color:rgba(255,155,182,.4)}
  .spacer{flex:1}
  .meta{font-size:13.5px;line-height:1.55;background:rgba(255,255,255,.025);
    border:1px solid rgba(150,90,230,.16);border-radius:11px;padding:16px 18px}
  .meta .row1{display:flex;align-items:baseline;gap:11px;flex-wrap:wrap;margin-bottom:9px}
  .score{font-family:'Bricolage Grotesque',sans-serif;font-weight:800;font-size:19px;
    letter-spacing:-.3px;color:#fff}
  .scorer{font-size:10.5px;font-weight:700;letter-spacing:.6px;text-transform:uppercase}
  .meta .title{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;font-size:16.5px;
    letter-spacing:-.2px;color:#f3eefc;margin-bottom:5px}
  .meta .chan{color:#9d86c7;font-size:12.5px}
  .meta a{color:#b48bff;text-decoration:none}
  #done{display:none;text-align:center;padding:48px 0 30px;animation:ggPop .4s ease both}
  #done img{width:168px;animation:ggFloat 4.5s ease-in-out infinite;
    filter:drop-shadow(0 0 28px rgba(150,90,230,.5))}
  #done .big{font-family:'Bricolage Grotesque',sans-serif;font-weight:800;font-size:30px;
    letter-spacing:-.5px;margin-top:12px}
  #done .msg{color:#b297e0;font-size:13.5px;margin-top:10px;line-height:1.7;max-width:520px;
    margin:10px auto 0}
  #done code{background:rgba(150,90,230,.18);padding:2px 7px;border-radius:5px;color:#dcc8ff}
  @keyframes ggFloat{0%,100%{transform:translateY(0)}50%{transform:translateY(-9px)}}
  @keyframes ggPop{0%{transform:scale(.85);opacity:0}100%{transform:scale(1);opacity:1}}
</style></head>
<body>
<img id="bg" src="__GENGAR__" alt="" aria-hidden="true">
<div id="wrap">
  <div class="head">
    <img src="__GENGAR__" alt="Gengar">
    <div style="flex:1">
      <div class="h-title">CCTV dataset labeler</div>
      <div class="h-sub">ghost-grade ground truth</div>
    </div>
    <div class="keys"><span>K keep</span><span>J skip</span><span>B back</span><span>O yt</span></div>
  </div>

  <div id="labeling">
    <div class="progbar">
      <span class="ptext" id="progress">…</span>
      <span class="ptrack"><span id="pfill"></span></span>
      <span id="curlabel"></span>
    </div>
    <div class="vidbox">
      <video id="vid" controls autoplay muted></video>
      <div class="rec"><span></span>REC · LOCAL CLIP</div>
      <div class="cam" id="cam"></div>
    </div>
    <div class="bar">
      <button class="keep" onclick="label(1)">KEEP <kbd>K</kbd></button>
      <button class="skip" onclick="label(0)">SKIP <kbd>J</kbd></button>
      <span class="spacer"></span>
      <button onclick="back()">◀ BACK <kbd>B</kbd></button>
      <button onclick="openYt()">YOUTUBE ↗ <kbd>O</kbd></button>
    </div>
    <div class="meta">
      <div class="row1"><span class="score" id="score"></span><span class="scorer" id="scorer"></span></div>
      <div class="title" id="title"></div>
      <div class="chan"><span id="channel"></span> · <a id="url" href="#" target="_blank"></a></div>
    </div>
  </div>

  <div id="done">
    <img src="__GENGAR__" alt="Gengar">
    <div class="big">All clips labeled.</div>
    <div class="msg">Gengar fed every shadow back into the dataset. You can close this tab.<br>
      Run <code>python youtube_scraper.py --mode eval --out OUTDIR</code> to score it.</div>
  </div>
</div>
<script>
let cur=null;
async function load(){
  const r=await fetch('/api/next');const d=await r.json();
  if(d.done){
    document.getElementById('labeling').style.display='none';
    document.getElementById('done').style.display='block';return;}
  cur=d;
  document.getElementById('labeling').style.display='block';
  document.getElementById('done').style.display='none';
  document.getElementById('vid').src='/video?key='+encodeURIComponent(d.key);
  document.getElementById('cam').textContent='CAM '+d.key+' · 03:14:07';
  document.getElementById('progress').textContent=
    d.labeled+' / '+d.total+' labeled · '+d.remaining+' left';
  document.getElementById('pfill').style.width=
    (d.total?Math.round(d.labeled/d.total*100):0)+'%';
  const cl=document.getElementById('curlabel');
  if(d.human_label===null){cl.textContent='○ unlabeled';cl.style.color='#a98fd6';}
  else if(d.human_label){cl.textContent='★ keep';cl.style.color='#74e88f';}
  else{cl.textContent='✗ skip';cl.style.color='#f0809f';}
  const sk=d.score>=d.threshold;
  document.getElementById('score').textContent='score '+d.score.toFixed(2);
  const sc=document.getElementById('scorer');
  sc.textContent=sk?'↳ scorer: keep':'↳ scorer: skip';
  sc.style.color=sk?'#74e88f':'#a98fd6';
  document.getElementById('title').textContent=d.title;
  document.getElementById('channel').textContent=d.channel||'';
  const a=document.getElementById('url');a.textContent=d.url;a.href=d.url;
}
async function label(v){
  if(!cur)return;
  await fetch('/api/label',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({key:cur.key,label:v})});
  load();
}
async function back(){await fetch('/api/back',{method:'POST'});load();}
function openYt(){if(cur)window.open(cur.url,'_blank');}
document.addEventListener('keydown',e=>{
  const k=e.key.toLowerCase();
  if(k==='k')label(1);
  else if(k==='j')label(0);
  else if(k==='b')back();
  else if(k==='o')openYt();
});
load();
</script></body></html>
""".replace("__GENGAR__", GENGAR)


class Labeler:
    """Holds DB + the ordered worklist. Borderline (score near threshold) first."""

    def __init__(self, out_dir: Path, threshold: float, relabel: bool):
        self.db_path = out_dir / "dataset.db"
        if not self.db_path.exists():
            raise SystemExit(f"no DB at {self.db_path}")
        self.db = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._ensure_human_label_col()
        self.threshold = threshold
        self.relabel = relabel
        self._lock = threading.Lock()
        self.history: list[str] = []   # keys we've served, for Back
        self._build_worklist()

    def _ensure_human_label_col(self) -> None:
        """Add the human_label column if this DB predates it (so label_app works
        standalone, without first running youtube_scraper to migrate)."""
        cols = {r[1] for r in self.db.execute("PRAGMA table_info(videos)")}
        if "human_label" not in cols:
            self.db.execute("ALTER TABLE videos ADD COLUMN human_label INTEGER")
            self.db.commit()

    def _build_worklist(self) -> None:
        # only downloaded rows have a local file to play
        rows = self.db.execute(
            "SELECT key, score FROM videos WHERE status='downloaded' "
            "AND path IS NOT NULL").fetchall()
        th = self.threshold
        # borderline first: ascending distance from the threshold
        rows = sorted(rows, key=lambda r: abs((r["score"] or 0.0) - th))
        self.order = [r["key"] for r in rows]
        self.total = len(self.order)

    def _labeled_count(self) -> int:
        return self.db.execute(
            "SELECT count(*) FROM videos WHERE status='downloaded' "
            "AND human_label IS NOT NULL").fetchone()[0]

    def next_item(self) -> dict:
        with self._lock:
            for key in self.order:
                r = self.db.execute(
                    "SELECT key, video_id, platform, title, channel, score, url, "
                    "human_label, path FROM videos WHERE key=?", (key,)).fetchone()
                if r is None:
                    continue
                if (not self.relabel) and r["human_label"] is not None:
                    continue
                if self.history[-1:] != [key]:
                    self.history.append(key)
                return {
                    "key": r["key"], "title": r["title"] or "",
                    "channel": r["channel"] or "", "score": r["score"] or 0.0,
                    "url": r["url"] or "", "human_label": r["human_label"],
                    "threshold": self.threshold,
                    "labeled": self._labeled_count(), "total": self.total,
                    "remaining": self._remaining(), "done": False,
                }
            return {"done": True, "labeled": self._labeled_count(),
                    "total": self.total, "remaining": 0}

    def _remaining(self) -> int:
        if self.relabel:
            return self.total - 0
        done = self._labeled_count()
        return max(0, self.total - done)

    def set_label(self, key: str, label: int) -> None:
        with self._lock:
            self.db.execute("UPDATE videos SET human_label=? WHERE key=?",
                            (1 if label else 0, key))
            self.db.commit()

    def back(self) -> None:
        with self._lock:
            # drop current, re-serve previous and clear its label so it reappears
            if len(self.history) >= 2:
                self.history.pop()              # current
                prev = self.history.pop()       # will be re-served by next_item
                self.db.execute("UPDATE videos SET human_label=NULL WHERE key=?", (prev,))
                self.db.commit()

    def file_for(self, key: str) -> Path | None:
        r = self.db.execute("SELECT path FROM videos WHERE key=?", (key,)).fetchone()
        if r and r["path"]:
            p = Path(r["path"])
            if p.exists():
                return p
        return None


def make_handler(lab: Labeler):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):  # quiet
            pass

        def _json(self, obj, code=200):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urlparse(self.path)
            if u.path == "/":
                body = PAGE.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif u.path == "/api/next":
                self._json(lab.next_item())
            elif u.path == "/video":
                key = (parse_qs(u.query).get("key") or [""])[0]
                self._serve_video(unquote(key))
            else:
                self.send_error(404)

        def do_POST(self):
            u = urlparse(self.path)
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                data = {}
            if u.path == "/api/label":
                lab.set_label(data.get("key", ""), int(data.get("label", 0)))
                self._json({"ok": True})
            elif u.path == "/api/back":
                lab.back()
                self._json({"ok": True})
            else:
                self.send_error(404)

        def _serve_video(self, key: str):
            p = lab.file_for(key)
            if not p:
                self.send_error(404)
                return
            size = p.stat().st_size
            rng = self.headers.get("Range")
            start, end = 0, size - 1
            if rng and rng.startswith("bytes="):
                part = rng.split("=", 1)[1].split("-")
                if part[0]:
                    start = int(part[0])
                if len(part) > 1 and part[1]:
                    end = int(part[1])
                end = min(end, size - 1)
            length = end - start + 1
            self.send_response(206 if rng else 200)
            self.send_header("Content-Type", "video/mp4")
            self.send_header("Accept-Ranges", "bytes")
            if rng:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.send_header("Content-Length", str(length))
            self.end_headers()
            with p.open("rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(64 * 1024, remaining))
                    if not chunk:
                        break
                    try:
                        self.wfile.write(chunk)
                    except (BrokenPipeError, ConnectionResetError):
                        break  # browser seeked/closed — fine
                    remaining -= len(chunk)
    return H


def main() -> None:
    ap = argparse.ArgumentParser(description="Local web UI to hand-label collected videos.")
    ap.add_argument("--out", default="dataset", type=Path, help="Dataset dir (has dataset.db).")
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                    help="Used only to order borderline clips first.")
    ap.add_argument("--port", type=int, default=8731)
    ap.add_argument("--relabel", action="store_true",
                    help="Include already-labeled clips (re-review everything).")
    ap.add_argument("--no-open", action="store_true", help="Don't auto-open the browser.")
    a = ap.parse_args()

    lab = Labeler(a.out, a.threshold, a.relabel)
    if lab.total == 0:
        raise SystemExit("No downloaded clips with a local file to label.")
    httpd = ThreadingHTTPServer(("127.0.0.1", a.port), make_handler(lab))
    url = f"http://127.0.0.1:{a.port}/"
    print(f"Labeling {lab.total} clips ({lab._labeled_count()} already labeled).")
    print(f"Open {url}  —  keys: K=keep  J=skip  B=back  O=YouTube. Ctrl+C to stop.")
    if not a.no_open:
        threading.Thread(target=lambda: webbrowser.open(url), daemon=True).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped. Labels saved in the DB.")
    finally:
        httpd.server_close()
        lab.db.close()


if __name__ == "__main__":
    main()
