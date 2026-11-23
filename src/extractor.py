#!/usr/bin/env python3
"""
REGEX extractor for pages downloaded by crawler.py (VI2026 project).

Reads data/raw/*.html and writes three TSV files into data/extracted/:
    players.tsv         one row per player (profile)
    player_career.tsv   one row per player/season/team/competition
    teams.tsv           one row per team (stadium, capacity, coach, squad ids)

Approach: the HTML is converted to plain text lines (scripts, styles and tags
are removed) and the values are found with regular expressions on that text.
No XPath, no CSS classes, no HTML parser - only `re` and `csv`.

Usage:
    python extractor.py                       # default dirs: data/raw -> data/extracted
    python extractor.py --raw data/raw --out data/extracted
"""
import argparse
import csv
import html
import re
import time
from pathlib import Path

# ---------------------------------------------------------------- HTML -> text
SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b.*?</\1>", re.S | re.I)
COMMENT_RE = re.compile(r"<!--.*?-->", re.S)
TAG_RE = re.compile(r"<[^>]+>")


def html_to_lines(page_html):
    """Return the visible text of a page as a list of non-empty stripped lines."""
    text = SCRIPT_STYLE_RE.sub("", page_html)
    text = COMMENT_RE.sub("", text)
    text = html.unescape(TAG_RE.sub("\n", text))
    return [line.strip() for line in text.split("\n") if line.strip()]


# ---------------------------------------------------------------- small parsers
DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
SEASON_RE = re.compile(r"^\d{4}(?:/\d{4})?$")
RATING_RE = re.compile(r"^\d{1,2}\.\d$")
MARKET_VALUE_RE = re.compile(r"€\s*([\d.,]+)\s*(k|m|bn)?", re.I)


def iso_date(text):
    """'02.10.1992' -> '1992-10-02' (empty string if not a date)."""
    return DATE_RE.sub(r"\3-\2-\1", text) if text and DATE_RE.fullmatch(text) else ""


def market_value_eur(text):
    """'€15.1m' -> 15100000, '€500k' -> 500000 (empty string if not parsable)."""
    match = MARKET_VALUE_RE.fullmatch(text.strip()) if text else None
    if not match:
        return ""
    multiplier = {"k": 1e3, "m": 1e6, "bn": 1e9}.get((match.group(2) or "").lower(), 1)
    return int(round(float(match.group(1).replace(",", "")) * multiplier))


def to_number(token):
    """'7' -> 7, '77.8' -> 77.8, '-' -> None."""
    if re.fullmatch(r"\d+", token):
        return int(token)
    if re.fullmatch(r"\d+\.\d+", token):
        return float(token)
    return None


def id_from_file(path):
    """player__alisson__UZrQ3bwP.html -> UZrQ3bwP"""
    return path.stem.split("__")[-1]


# ---------------------------------------------------------------- player pages
TITLE_NAME_RE = re.compile(r"<title>\s*(?P<name>.+?)\s+Stats\s*\|", re.S)


def player_header_re(name):
    """Header = country, name, [injury/status note], position, [(club)],
    [On loan from: club (Until: date)], [Age: n (dd.mm.yyyy)].
    The name (from <title>) is fixed in the pattern, so the optional lines cannot shift
    the other groups. Retired players show 'End of career' instead of a position."""
    return re.compile(
        r"^(?P<country>[^\n]+)\n" + re.escape(name) + r"\n"
        r"(?:(?P<status>[^\n]+)\n)?"
        r"(?P<position>Goalkeeper|Defender|Midfielder|Forward|Coach|End of career)\n"
        r"(?:\((?P<club>[^\n]+)\)\n)?"
        r"(?:On loan from:\n(?P<loan>[^\n]+)\n(?:\(Until: [^\n]+\)\n)?)?"
        r"(?:Age:\n(?P<age>\d+)(?:\n\((?P<birth>\d{2}\.\d{2}\.\d{4})\))?)?",
        re.M,
    )


CLUB_LINK_RE = re.compile(
    r'href="/team/[a-z0-9-]+/(?P<id>[A-Za-z0-9]{8})/"[^>]*title="(?P<title>[^"]*)"')
MARKET_VALUE_LINE_RE = re.compile(r"^Market value:\n(?P<value>[^\n]+)", re.M)
CONTRACT_LINE_RE = re.compile(r"^Contract expires:\n(?P<date>\d{2}\.\d{2}\.\d{4})", re.M)


def parse_player(page_html, player_id):
    """Extract the profile of one player; returns (profile dict, career rows) or None."""
    lines = html_to_lines(page_html)
    text = "\n".join(lines)
    title = TITLE_NAME_RE.search(page_html)
    if not title:
        return None
    name = html.unescape(title.group("name"))
    header = player_header_re(name).search(text)
    if not header:
        return None

    club = header.group("club") or ""
    club_id = ""
    if club:
        # compare unescaped titles, so names like "Nott'm Forest" match whatever
        # entity form (&#039; or &#x27;) the page uses
        for link in CLUB_LINK_RE.finditer(page_html):
            if html.unescape(link.group("title")) == club:
                club_id = link.group("id")
                break

    position = header.group("position")
    status = header.group("status") or ""
    if position == "End of career":          # retired: the position is no longer shown
        position, status = "", (status + "; End of career").strip("; ")

    value = MARKET_VALUE_LINE_RE.search(text)
    contract = CONTRACT_LINE_RE.search(text)
    profile = {
        "player_id": player_id,
        "name": name,
        "country": header.group("country"),
        "position": position,
        "status": status,
        "club": club,
        "club_id": club_id,
        "loan_from": header.group("loan") or "",
        "age": int(header.group("age")) if header.group("age") else "",
        "birth_date": iso_date(header.group("birth") or ""),
        "market_value_eur": market_value_eur(value.group("value")) if value else "",
        "contract_expires": iso_date(contract.group("date")) if contract else "",
    }
    if profile["position"] == "Coach":
        # Coaches live under /player/ too; their career table has different columns,
        # so only the profile is extracted.
        return profile, []
    return profile, parse_career(lines, player_id, position)


def parse_career(lines, player_id, position):
    """Parse season rows: <season> <team> <competition> <numbers...>."""
    try:
        start = lines.index("Season")
    except ValueError:
        return []
    end = next((i for i in range(start, len(lines)) if lines[i] == "Transfers"), len(lines))
    if "SV%" in lines[start:start + 12]:
        position = "Goalkeeper"      # goalkeeper columns, even if the position is not shown

    rows, i = [], start
    while i < end:
        if SEASON_RE.match(lines[i]) and i + 2 < end:
            season, team, competition = lines[i:i + 3]
            j, values = i + 3, []
            while j < end and not SEASON_RE.match(lines[j]) and lines[j] not in ("Total", "Season"):
                values.append(lines[j])
                j += 1
            rows.append(career_row(player_id, season, team, competition, values, position))
            i = j
        else:
            i += 1
    return rows


def career_row(player_id, season, team, competition, values, position):
    """Rating is optional (the only decimal in first place); the rest are fixed columns."""
    values = list(values)
    rating = to_number(values.pop(0)) if values and RATING_RE.match(values[0]) else None
    numbers = [to_number(v) for v in values] + [None] * 5
    row = {"player_id": player_id, "season": season, "team": team,
           "competition": competition, "rating": rating,
           "matches": numbers[0], "goals": None, "assists": None,
           "save_pct": None, "clean_sheets": None,
           "yellow_cards": numbers[3], "red_cards": numbers[4]}
    if position == "Goalkeeper":
        row["save_pct"], row["clean_sheets"] = numbers[1], numbers[2]
    else:
        row["goals"], row["assists"] = numbers[1], numbers[2]
    return row


# ---------------------------------------------------------------- team pages
TEAM_TITLE_RE = re.compile(r"<title>\s*(?P<name>.+?)\s+stats,", re.S)


def team_header_re(name):
    """Header = country, name, ['FIFA' + ranking for national teams],
    [Stadium: x (city) Capacity: n]. Youth, reserve and national teams have no stadium.
    The name (from <title>) is fixed in the pattern, like for players."""
    return re.compile(
        r"^(?P<country>[^\n]+)\n" + re.escape(name) + r"\n"
        r"(?P<national>FIFA\n)?"
        r"(?:Stadium:\n(?P<stadium>[^\n]+)\n(?:\((?P<city>[^\n]+)\)\n)?"
        r"Capacity:\n(?P<capacity>[\d \u00a0]+)\n)?",
        re.M,
    )


# Some clubs have a shorter name in the page header than in <title> ("Tottenham Hotspur" vs "Tottenham").
# Fallback: any name line is accepted, but then the stadium block is required, so that a player
# page stored under /team/ (no stadium) is still rejected.
TEAM_HEADER_FALLBACK_RE = re.compile(
    r"^(?P<country>[^\n]+)\n[^\n]+\n"
    r"(?P<national>)"
    r"Stadium:\n(?P<stadium>[^\n]+)\n(?:\((?P<city>[^\n]+)\)\n)?"
    r"Capacity:\n(?P<capacity>[\d  ]+)(?:\n|\Z)",
    re.M,
)


COACH_RE = re.compile(r"^Coach\n#\nName\nAge\n(?P<coach>[^\n]+)\n\d+", re.M)
PLAYER_LINK_RE = re.compile(r'href="/player/[a-z0-9-]+/([A-Za-z0-9]{8})/"')


def parse_team(page_html, team_id):
    """Extract stadium, capacity, coach and the ids of squad players."""
    text = "\n".join(html_to_lines(page_html))
    title = TEAM_TITLE_RE.search(page_html)
    if not title:
        return None
    name = html.unescape(title.group("name"))
    header = team_header_re(name).search(text) or TEAM_HEADER_FALLBACK_RE.search(text)
    if not header:
        return None
    coach = COACH_RE.search(text)
    squad_ids = sorted(set(PLAYER_LINK_RE.findall(page_html)))
    return {
        "team_id": team_id,
        "name": name,
        "country": name if header.group("national") else header.group("country"),
        "stadium": header.group("stadium") or "",
        "city": header.group("city") or "",
        "capacity": (int(re.sub(r"\D", "", header.group("capacity")))
                     if header.group("capacity") else ""),
        "coach": coach.group("coach") if coach else "",
        "squad_size": len(squad_ids),
        "squad_player_ids": ";".join(squad_ids),
    }


# ---------------------------------------------------------------- output
PLAYER_FIELDS = ["player_id", "name", "country", "position", "status", "club", "club_id", "loan_from",
                 "age", "birth_date", "market_value_eur", "contract_expires"]
CAREER_FIELDS = ["player_id", "season", "team", "competition", "rating", "matches",
                 "goals", "assists", "save_pct", "clean_sheets", "yellow_cards", "red_cards"]
TEAM_FIELDS = ["team_id", "name", "country", "stadium", "city", "capacity",
               "coach", "squad_size", "squad_player_ids"]


def write_tsv(path, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def coverage(rows, fields):
    """How many rows have each field filled in (used for the statistics table)."""
    return {f: sum(1 for r in rows if r.get(f) not in ("", None)) for f in fields}


def main():
    ap = argparse.ArgumentParser(description="REGEX extractor for soccerway pages")
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--out", default="data/extracted")
    ap.add_argument("--limit", type=int, default=0, help="process only the first N pages (0 = all)")
    ap.add_argument("--only", choices=("players", "teams"),
                    help="process only this kind of page; the other TSV files stay untouched")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    prefixes = {"players": ("player__",), "teams": ("team__",)}.get(args.only, ("player__", "team__"))
    files = [p for p in sorted(Path(args.raw).glob("*.html")) if p.name.startswith(prefixes)]
    if args.limit:
        files = files[:args.limit]
    print(f"Extracting {len(files)} pages from {args.raw} ...", flush=True)

    players, career, teams, failed = [], [], [], []
    page_times = []            # (seconds, file name) for the slowest-pages report
    read_seconds = 0.0
    started = time.monotonic()
    try:
        for number, path in enumerate(files, 1):
            page_started = time.monotonic()
            page_html = path.read_text(encoding="utf-8", errors="ignore")
            read_time = time.monotonic() - page_started
            read_seconds += read_time

            if path.name.startswith("player__"):
                result = parse_player(page_html, id_from_file(path))
                if result:
                    players.append(result[0])
                    career.extend(result[1])
                else:
                    failed.append(path.name)
            else:
                result = parse_team(page_html, id_from_file(path))
                if result:
                    teams.append(result)
                else:
                    failed.append(path.name)

            took = time.monotonic() - page_started
            page_times.append((took, path.name))
            if took > 5:
                print(f"  SLOW page ({took:.1f} s, of which reading {read_time:.1f} s): {path.name}",
                      flush=True)
            if number in (10, 100) or number % 250 == 0 or number == len(files):
                elapsed = time.monotonic() - started
                left = elapsed / number * (len(files) - number)
                print(f"[{number}/{len(files)}] {elapsed:.0f} s elapsed, about {left:.0f} s left",
                      flush=True)
    except KeyboardInterrupt:
        print("\nInterrupted - writing the results parsed so far.")

    if args.only != "teams":              # "--only teams" leaves the player files untouched
        write_tsv(out_dir / "players.tsv", PLAYER_FIELDS, players)
        write_tsv(out_dir / "player_career.tsv", CAREER_FIELDS, career)
    if args.only != "players":
        write_tsv(out_dir / "teams.tsv", TEAM_FIELDS, teams)

    print(f"players: {len(players)}   career rows: {len(career)}   teams: {len(teams)}")
    print("player field coverage:", coverage(players, PLAYER_FIELDS))
    if failed:
        print(f"NOT PARSED ({len(failed)}; check the regexes):", ", ".join(failed[:20]),
              "..." if len(failed) > 20 else "")
    total = time.monotonic() - started
    print(f"time: {total:.0f} s in total, of which reading files {read_seconds:.0f} s")
    slowest = sorted(page_times, reverse=True)[:3]
    print("slowest pages:", "; ".join(f"{name} {sec:.2f} s" for sec, name in slowest))


if __name__ == "__main__":
    main()
