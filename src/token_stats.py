#!/usr/bin/env python3
"""
BONUS (consultation 2b): count tokens of the downloaded pages with tiktoken.

Only the VISIBLE TEXT of each page is counted (scripts, styles and tags are removed
by html_to_lines from extractor.py), because raw HTML is mostly JavaScript.

Usage (run after the crawl has finished, in the folder with extractor.py):
    pip install tiktoken
    python token_stats.py                  # reads data/raw
    python token_stats.py --raw data/raw
Note: tiktoken downloads its encoding file on first use, so internet is needed once.
"""
import argparse
from collections import defaultdict
from pathlib import Path

import tiktoken

from extractor import html_to_lines  # same folder as this script

ENCODING_NAME = "cl100k_base"  # tokenizer used by GPT-3.5 / GPT-4 class models


def fmt(number):
    """12345 -> '12 345' (space as thousands separator)."""
    return f"{number:,}".replace(",", " ")


def page_type(file_name):
    """player__alisson__X.html -> 'player'; team__... -> 'team'; anything else -> 'other'."""
    prefix = file_name.split("__")[0]
    return prefix if prefix in ("player", "team") else "other"


def main():
    ap = argparse.ArgumentParser(description="Token statistics of crawled pages")
    ap.add_argument("--raw", default="data/raw")
    args = ap.parse_args()

    encoding = tiktoken.get_encoding(ENCODING_NAME)
    pages = defaultdict(int)
    tokens = defaultdict(int)
    chars = defaultdict(int)

    for path in sorted(Path(args.raw).glob("*.html")):
        html = path.read_text(encoding="utf-8", errors="ignore")
        text = "\n".join(html_to_lines(html))
        kind = page_type(path.name)
        pages[kind] += 1
        chars[kind] += len(text)
        # disallowed_special=() so text that looks like a special token is counted as plain text
        tokens[kind] += len(encoding.encode(text, disallowed_special=()))

    total_pages = sum(pages.values())
    total_tokens = sum(tokens.values())
    if not total_pages:
        print(f"No HTML files found in {args.raw}")
        return

    print(f"=== Token statistics (encoding {ENCODING_NAME}); paste into the statistics section ===")
    print("^ Typ stránky ^ Počet stránok ^ Znaky (viditeľný text) ^ Tokeny ^ Priemer tokenov na stránku ^")
    for kind in ("player", "team", "other"):
        if pages[kind]:
            print(f"| {kind} | {pages[kind]} | {fmt(chars[kind])} | {fmt(tokens[kind])} "
                  f"| {fmt(tokens[kind] // pages[kind])} |")
    print(f"| **spolu** | **{total_pages}** | **{fmt(sum(chars.values()))}** "
          f"| **{fmt(total_tokens)}** | **{fmt(total_tokens // total_pages)}** |")
    print()
    print(f"Celkový počet tokenov (tiktoken, {ENCODING_NAME}): **{fmt(total_tokens)}**")


if __name__ == "__main__":
    main()
