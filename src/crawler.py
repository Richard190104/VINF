#!/usr/bin/env python3
"""
Simple polite crawler for soccerway.com (VI2026 project, consultation 2a).

Pipeline: frontier (BFS queue) -> robots.txt check -> fetch (timeout, retries,
sleep) -> save raw HTML + metadata -> extract links with REGEX -> back to frontier.

Usage:
    pip install requests
    python crawler.py --max-pages 20            # small test run
    python crawler.py --max-pages 600           # real run (can be resumed)
Metadata are stored in a TSV file (SQL databases are not allowed in the course).
Nothing is ever deleted: re-running continues where the last run stopped.
"""
import argparse
import csv
import hashlib
import random
import re
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urldefrag, urlparse
from urllib.robotparser import RobotFileParser

import requests

BASE_URL = "https://www.soccerway.com"
CONTACT = "pukacrichard1@gmail.com"
USER_AGENT = f"VI2026-SchoolProject/1.0 (university course crawler; {CONTACT})"
TIMEOUT = (10, 30)   # (connect, read) seconds
MAX_RETRIES = 3
METADATA_FIELDS = ["url", "page_type", "depth", "status_code", "fetched_at",
                   "fetch_seconds", "size_bytes", "sha256", "content_type",
                   "links_found", "file"]

# Only these page types are followed (REGEX on URL path, no XPath / page structure).
URL_PATTERNS = {
    "team": re.compile(r"^/team/[a-z0-9-]+/[A-Za-z0-9]{8}/$"),
    "player": re.compile(r"^/player/[a-z0-9-]+/[A-Za-z0-9]{8}/$"),
}
HREF_RE = re.compile(r'href="([^"#]+)"')

DEFAULT_SEEDS = [
    "/england/premier-league/standings/",
    "/team/liverpool/lId4TMwf/",
]


def classify(path):
    """Return page type ('team'/'player') or None if the path is not interesting."""
    for page_type, pattern in URL_PATTERNS.items():
        if pattern.match(path):
            return page_type
    return None


def extract_links(html):
    """Extract absolute, normalized URLs of teams and players from raw HTML."""
    links = set()
    for href in HREF_RE.findall(html):
        url, _ = urldefrag(urljoin(BASE_URL, href))
        parsed = urlparse(url)
        if parsed.netloc != "www.soccerway.com":
            continue
        path = parsed.path if parsed.path.endswith("/") else parsed.path + "/"
        if classify(path):
            links.add(f"{BASE_URL}{path}")
    return links


def load_robots(session):
    """Download robots.txt with our own User-Agent and parse it."""
    resp = session.get(f"{BASE_URL}/robots.txt", timeout=TIMEOUT)
    parser = RobotFileParser()
    parser.parse(resp.text.splitlines())
    return parser


class BotBlocked(Exception):
    """Raised when the site serves a bot challenge. We stop instead of bypassing it."""


def fetch(session, url):
    """GET with retries and exponential backoff. Returns (response, seconds) or (None, 0)."""
    for attempt in range(1, MAX_RETRIES + 1):
        started = time.monotonic()
        try:
            resp = session.get(url, timeout=TIMEOUT)
        except requests.RequestException as exc:
            print(f"  ! network error ({exc.__class__.__name__}), attempt {attempt}")
            time.sleep(2 ** attempt)
            continue
        elapsed = time.monotonic() - started
        if resp.headers.get("cf-mitigated") or resp.status_code == 403:
            raise BotBlocked(f"{url} -> HTTP {resp.status_code} (bot protection)")
        if resp.status_code in (429, 500, 502, 503, 504):
            retry_after = resp.headers.get("Retry-After", "")
            wait = int(retry_after) if retry_after.isdigit() else 2 ** attempt * 2
            print(f"  ! HTTP {resp.status_code}, waiting {wait}s (attempt {attempt})")
            time.sleep(wait)
            continue
        return resp, elapsed
    return None, 0.0


def repair_metadata(meta_path):
    """If the last run died while writing a row, drop that half-written last line.
    The HTML file of that page stays on disk and the page is simply downloaded again."""
    if not meta_path.exists():
        return
    content = meta_path.read_bytes()
    if content and not content.endswith(b"\n"):
        meta_path.write_bytes(content[: content.rfind(b"\n") + 1])
        print("Repaired metadata.tsv: removed an incomplete last row.")


def file_name_for(url):
    """/player/alisson/UZrQ3bwP/ -> player__alisson__UZrQ3bwP.html"""
    return urlparse(url).path.strip("/").replace("/", "__") + ".html"


def main():
    ap = argparse.ArgumentParser(description="Polite soccerway.com crawler")
    ap.add_argument("--out", default="data", help="output directory")
    ap.add_argument("--max-pages", type=int, default=20)
    ap.add_argument("--delay-min", type=float, default=1.5)
    ap.add_argument("--delay-max", type=float, default=3.0)
    ap.add_argument("--seeds", nargs="*", default=DEFAULT_SEEDS)
    args = ap.parse_args()

    raw_dir = Path(args.out) / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    meta_path = Path(args.out) / "metadata.tsv"

    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html",
        "Accept-Language": "en",
    })
    robots = load_robots(session)

    # Resume: remember what is already downloaded and rebuild the frontier from it.
    visited, frontier, queued = set(), deque(), set()
    repair_metadata(meta_path)
    if meta_path.exists():
        with meta_path.open(encoding="utf-8", newline="") as meta_in:
            for record in csv.DictReader(meta_in, delimiter="\t"):
                visited.add(record["url"])
                saved = raw_dir / record["file"]
                if record["status_code"] == "200" and saved.exists():
                    html = saved.read_text(encoding="utf-8", errors="ignore")
                    for link in sorted(extract_links(html)):
                        if link not in visited and link not in queued:
                            frontier.append((link, int(record["depth"]) + 1))
                            queued.add(link)
    for seed in args.seeds:
        url = urljoin(BASE_URL, seed)
        if url not in visited and url not in queued:
            frontier.appendleft((url, 0))
            queued.add(url)

    fetched = 0
    write_header = not meta_path.exists() or meta_path.stat().st_size == 0
    try:
        with meta_path.open("a", encoding="utf-8", newline="") as meta_file:
            writer = csv.DictWriter(meta_file, fieldnames=METADATA_FIELDS, delimiter="\t")
            if write_header:
                writer.writeheader()
            while frontier and fetched < args.max_pages:
                url, depth = frontier.popleft()
                if url in visited:
                    continue
                if not robots.can_fetch(USER_AGENT, url):
                    print(f"skip (robots.txt): {url}")
                    visited.add(url)
                    continue

                resp, seconds = fetch(session, url)
                visited.add(url)
                if resp is None:
                    print(f"give up: {url}")
                    continue

                body = resp.content
                file_name = file_name_for(url)
                (raw_dir / file_name).write_bytes(body)
                html = body.decode(resp.encoding or "utf-8", errors="ignore")
                links = extract_links(html) if resp.status_code == 200 else set()
                for link in sorted(links):
                    if link not in visited and link not in queued:
                        frontier.append((link, depth + 1))
                        queued.add(link)

                record = {
                    "url": url,
                    "page_type": classify(urlparse(url).path) or "seed",
                    "depth": depth,
                    "status_code": resp.status_code,
                    "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "fetch_seconds": round(seconds, 2),
                    "size_bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "content_type": resp.headers.get("Content-Type", ""),
                    "links_found": len(links),
                    "file": file_name,
                }
                writer.writerow(record)
                meta_file.flush()
                fetched += 1
                print(f"[{fetched}/{args.max_pages}] {resp.status_code} "
                      f"{record['page_type']:<6} {len(body) // 1024:>5} KB "
                      f"links={len(links):<3} queue={len(frontier)}  {url}")

                time.sleep(random.uniform(args.delay_min, args.delay_max))
    except BotBlocked as exc:
        print(f"\nSTOPPED: {exc}\nThe site blocks automated clients; not trying to bypass it.")
    except KeyboardInterrupt:
        print("\nInterrupted - progress is saved, run again to resume.")

    print(f"\nDone: {fetched} new pages, {len(visited)} URLs visited in total, "
          f"{len(frontier)} URLs waiting in the frontier.")


if __name__ == "__main__":
    main()
