# Spark spracovanie Wikipedia dumpu (bz2, bez rozbalenia). Kazda stranka sa regexmi rozparsuje,
# z infoboxu football biography / football club sa vytiahnu atributy hracov a klubov do parquetu.
import argparse
import html
import re
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import types as T

from common import PROJECT, file_uri, get_spark, write_tsv

TITLE_RE = re.compile(r"<title>(.*?)</title>", re.S)
NS_ID_RE = re.compile(r"<ns>(\d+)</ns>\s*<id>(\d+)</id>")
REDIRECT_RE = re.compile(r'<redirect title="([^"]*)"')
TEXT_RE = re.compile(r"<text[^>]*>(.*?)</text>", re.S)
CATEGORY_RE = re.compile(r"\[\[\s*Category\s*:", re.I)
INFOBOX_RE = re.compile(r"\{\{\s*infobox\s+([^|\n{}]+?)\s*(?=[|\n}])", re.I)
BRACES_RE = re.compile(r"\{\{|\}\}")
PARAM_RE = re.compile(r"^\s*\|\s*([\w\- ]+?)\s*=\s*(.*?)(?=^\s*\|\s*[\w\- ]+?\s*=|\Z)", re.M | re.S)
COMMENT_RE = re.compile(r"<!--.*?-->", re.S)
REF_RE = re.compile(r"<ref[^>/]*/>|<ref[^>]*>.*?</ref>", re.S | re.I)
TEMPLATE_RE = re.compile(r"\{\{([^{}|]*)(?:\|([^{}]*))?\}\}")
WIKILINK_RE = re.compile(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]")
EXTLINK_RE = re.compile(r"\[https?://\S+\s*([^\]]*)\]")
TAG_RE = re.compile(r"<[^>]+>")
ISO_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
DAY_MONTH_YEAR_RE = re.compile(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})")
MONTH_DAY_YEAR_RE = re.compile(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})")
YOUTH_TEAM_RE = re.compile(r"\bU-?\d{2}\b|under|youth|olympic|\bB\b|amateur", re.I)
MONTHS = {m: i for i, m in enumerate(["january", "february", "march", "april", "may", "june", "july", "august",
                                      "september", "october", "november", "december"], 1)}

GENERIC = ["page_id", "title", "redirect_to", "kind", "infobox_type", "text_length", "n_categories"]
PLAYER_FIELDS = ["name", "full_name", "birth_date", "birth_place", "height_m", "position", "current_club",
                 "club_number", "senior_clubs", "youth_clubs", "senior_debut_year", "total_caps",
                 "total_goals", "national_team", "national_caps", "national_goals"]
CLUB_FIELDS = ["club_name", "full_name", "nickname", "founded_year", "ground", "capacity", "manager",
               "owner", "league"]
INT_COLS = {"page_id", "text_length", "n_categories", "club_number", "senior_clubs", "youth_clubs",
            "senior_debut_year", "total_caps", "total_goals", "national_caps", "national_goals",
            "founded_year", "capacity"}
DOUBLE_COLS = {"height_m"}


def first_number(text):
    found = re.search(r"\d+(?:\.\d+)?", text or "")
    return float(found.group(0)) if found else 0.0


def first_int(text):
    found = re.search(r"\d[\d,]*", text or "")
    return int(found.group(0).replace(",", "")) if found else None


def template(match):
    name = match.group(1).strip().lower()
    args = [a.strip() for a in (match.group(2) or "").split("|")]
    positional = [a for a in args if "=" not in a]
    named = {k.strip().lower(): v.strip() for k, v in (a.split("=", 1) for a in args if "=" in a)}
    if name.startswith(("birth date", "birth-date")) or name in ("bda", "dob"):
        numbers = [int(a) for a in positional if a.isdigit()][:3]
        return "%04d-%02d-%02d" % tuple(numbers) if len(numbers) == 3 and numbers[0] > 1000 else ""
    if name.startswith(("start date", "start-date")) or name in ("founded", "year"):
        return "-".join([a for a in positional if a.isdigit()][:3])
    if name == "height":
        if "m" in named:
            return named["m"] + " m"
        feet, inches = first_number(named.get("ft")), first_number(named.get("in"))
        return f"{feet * 0.3048 + inches * 0.0254:.2f} m" if feet else ""
    if name == "convert":
        return " ".join(positional[:2])
    if name in ("nowrap", "small", "nobold", "lang", "abbr", "smallcaps"):
        return positional[-1] if positional else ""
    return ""


def clean(value):
    text = REF_RE.sub("", COMMENT_RE.sub("", value or ""))
    for _ in range(4):
        replaced = TEMPLATE_RE.sub(template, text)
        if replaced == text:
            break
        text = replaced
    text = EXTLINK_RE.sub(r"\1", WIKILINK_RE.sub(r"\1", text))
    text = re.sub(r"<br\s*/?>", "; ", text, flags=re.I)
    text = TAG_RE.sub("", text).replace("'''", "").replace("''", "")
    text = html.unescape(text).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip(" ;,*")


def iso_date(text):
    if not text:
        return ""
    found = ISO_RE.search(text)
    if found:
        return found.group(0)
    found = DAY_MONTH_YEAR_RE.search(text)
    if found and found.group(2).lower() in MONTHS:
        return "%s-%02d-%02d" % (found.group(3), MONTHS[found.group(2).lower()], int(found.group(1)))
    found = MONTH_DAY_YEAR_RE.search(text)
    if found and found.group(1).lower() in MONTHS:
        return "%s-%02d-%02d" % (found.group(3), MONTHS[found.group(1).lower()], int(found.group(2)))
    return ""


def height_m(text):
    found = re.search(r"(\d\.\d{1,2})\s*m\b", text or "")
    if found:
        return float(found.group(1))
    found = re.search(r"\b(\d{3})\s*cm\b", text or "")
    if found:
        return int(found.group(1)) / 100
    found = re.search(r"(\d)\s*(?:ft|feet|')\s*(\d{1,2})?", text or "")
    if found:
        return round(int(found.group(1)) * 0.3048 + int(found.group(2) or 0) * 0.0254, 2)
    return None


def infobox(text):
    start = INFOBOX_RE.search(text)
    if not start:
        return None, {}
    depth, end = 1, None
    for brace in BRACES_RE.finditer(text, start.start() + 2):
        depth += 1 if brace.group() == "{{" else -1
        if depth == 0:
            end = brace.start()
            break
    body = text[start.end():end if end is not None else start.end() + 20000]
    params = {}
    for m in PARAM_RE.finditer(body):
        key, value = m.group(1).strip().lower(), m.group(2).strip()
        params[key] = value
        params.setdefault(key.replace("_", "").replace(" ", ""), value)
    return start.group(1).strip().lower(), params


def numbered(params, prefix):
    return {n: clean(params[f"{prefix}{n}"]) for n in range(1, 41) if f"{prefix}{n}" in params}


def parse_player(params):
    get = lambda key: clean(params.get(key, ""))
    clubs, caps, goals, years = (numbered(params, p) for p in ("clubs", "caps", "goals", "years"))
    teams, ncaps, ngoals = (numbered(params, p) for p in ("nationalteam", "nationalcaps", "nationalgoals"))
    senior = next((n for n, t in teams.items() if t and not YOUTH_TEAM_RE.search(t)), None)
    debut = next((int(re.search(r"\d{4}", v).group(0)) for v in years.values() if re.search(r"\d{4}", v)), None)
    return {
        "name": get("name"), "full_name": get("fullname"),
        "birth_date": iso_date(get("birth_date")), "birth_place": get("birth_place"),
        "height_m": height_m(get("height")), "position": get("position"),
        "current_club": get("currentclub"), "club_number": first_int(get("clubnumber")),
        "senior_clubs": len(clubs) or None, "youth_clubs": len(numbered(params, "youthclubs")) or None,
        "senior_debut_year": debut,
        "total_caps": first_int(get("totalcaps")) if "totalcaps" in params
        else (sum(first_int(v) or 0 for v in caps.values()) if caps else None),
        "total_goals": first_int(get("totalgoals")) if "totalgoals" in params
        else (sum(first_int(v) or 0 for v in goals.values()) if goals else None),
        "national_team": teams.get(senior) if senior else None,
        "national_caps": first_int(ncaps.get(senior)) if senior else None,
        "national_goals": first_int(ngoals.get(senior)) if senior else None,
    }


def parse_club(params):
    get = lambda *keys: next((clean(params[k]) for k in keys if k in params and clean(params[k])), "")
    founded = re.search(r"\b(1[5-9]\d{2}|20[0-2]\d)\b", get("founded"))
    return {
        "club_name": get("clubname"), "full_name": get("fullname"), "nickname": get("nickname"),
        "founded_year": int(founded.group(1)) if founded else None, "ground": get("ground", "stadium"),
        "capacity": first_int(get("capacity")), "manager": get("manager", "head coach", "headcoach", "coach"),
        "owner": get("owner", "chairman", "president"), "league": get("league"),
    }


def parse_page(record):
    ns_id, title = NS_ID_RE.search(record), TITLE_RE.search(record)
    if not ns_id or not title or ns_id.group(1) != "0":
        return None
    redirect, text = REDIRECT_RE.search(record), TEXT_RE.search(record)
    text = html.unescape(text.group(1)) if text else ""
    page = dict.fromkeys(PLAYER_FIELDS + CLUB_FIELDS)
    page.update({"page_id": int(ns_id.group(2)), "title": html.unescape(title.group(1)),
                 "redirect_to": html.unescape(redirect.group(1)) if redirect else None,
                 "text_length": len(text), "n_categories": len(CATEGORY_RE.findall(text)),
                 "infobox_type": None, "kind": "redirect" if redirect else "other"})
    if redirect:
        return page
    box, params = infobox(text)
    page["infobox_type"] = box
    try:
        if box and box.startswith("football biography"):
            page.update(parse_player(params), kind="player")
        elif box == "football club":
            page.update(parse_club(params), kind="club")
    except (ValueError, TypeError, AttributeError):
        page["kind"] = "parse_error"
    return page


def parse_partition(records):
    for _, record in records:
        page = parse_page(str(record))
        if page is not None:
            yield page


def to_df(spark, rdd, fields):
    def kind(f):
        return T.LongType() if f in INT_COLS else T.DoubleType() if f in DOUBLE_COLS else T.StringType()

    def row(p):
        return tuple(None if p.get(f) in ("", None) else
                     int(p[f]) if f in INT_COLS else float(p[f]) if f in DOUBLE_COLS else str(p[f])
                     for f in fields)
    schema = T.StructType([T.StructField(f, kind(f), True) for f in fields])
    return spark.createDataFrame(rdd.map(row), schema)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", default=str(Path.home() / "Downloads" / "enwiki-latest-pages-articles-multistream.xml.bz2"))
    ap.add_argument("--out", default=str(PROJECT / "data" / "wiki"))
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--memory", default="2g")
    args = ap.parse_args()

    spark = get_spark("vinf-wiki", args.master, args.memory)
    started, out = time.monotonic(), Path(args.out)
    dump = args.dump if "://" in args.dump else file_uri(args.dump)
    records = spark.sparkContext.newAPIHadoopFile(
        dump, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
        "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
        conf={"textinputformat.record.delimiter": "</page>"})
    pages = records.mapPartitions(parse_partition).persist(StorageLevel.DISK_ONLY)

    for name, kind, fields in (("wiki_players", "player", PLAYER_FIELDS), ("wiki_clubs", "club", CLUB_FIELDS)):
        df = to_df(spark, pages.filter(lambda p, kind=kind: p["kind"] == kind), GENERIC + fields)
        df.write.mode("overwrite").parquet(str(out / "parquet" / name))
        print(f"{name}: {df.count()} rows")

    generic = to_df(spark, pages, GENERIC)
    kinds = dict(generic.groupBy("kind").count().collect())
    boxes = generic.where("infobox_type IS NOT NULL").groupBy("infobox_type").count() \
        .orderBy("count", ascending=False).limit(15).collect()
    stats = [("articles (ns 0), incl. redirects", sum(kinds.values())), ("redirects", kinds.get("redirect", 0)),
             ("articles with football biography infobox", kinds.get("player", 0)),
             ("articles with football club infobox", kinds.get("club", 0)),
             ("other articles", kinds.get("other", 0))]
    stats += [(f"infobox: {r['infobox_type']}", r["count"]) for r in boxes]
    stats += [("seconds (Spark wiki step)", round(time.monotonic() - started))]
    write_tsv(spark.createDataFrame(stats, "metric string, value long"), out / "wiki_stats.tsv")
    for metric, value in stats:
        print(f"  {metric:<50} {value:>12,}")
    spark.stop()


if __name__ == "__main__":
    main()
