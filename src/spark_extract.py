# Spark extrakcia stiahnutych stranok soccerway. HTML subory sa zbalia do parquetu, kazdu stranku
# spracuju regexy z extractor.py a vystup (players, player_career, teams) sa porovna s data/extracted.
import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import PROJECT, file_uri, get_spark, write_tsv

PLAYER_FIELDS = ["player_id", "name", "country", "position", "status", "club", "club_id", "loan_from",
                 "age", "birth_date", "market_value_eur", "contract_expires"]
CAREER_FIELDS = ["player_id", "season", "team", "competition", "rating", "matches", "goals", "assists",
                 "save_pct", "clean_sheets", "yellow_cards", "red_cards"]
TEAM_FIELDS = ["team_id", "name", "country", "stadium", "city", "capacity", "coach", "squad_size",
               "squad_player_ids"]
INT_COLS = {"age", "market_value_eur", "matches", "goals", "assists", "clean_sheets", "yellow_cards",
            "red_cards", "capacity", "squad_size"}
DOUBLE_COLS = {"rating", "save_pct"}


def pack(spark, raw, out, batch_size):
    spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
    files = sorted(Path(raw).glob("*.html"))
    for i in range(0, len(files), batch_size):
        part = out / f"batch={i // batch_size:04d}"
        if (part / "_SUCCESS").exists():
            continue
        (spark.read.format("binaryFile").load([file_uri(p) for p in files[i:i + batch_size]])
         .select(F.regexp_extract("path", r"[^/]+$", 0).alias("file_name"), "content")
         .write.mode("overwrite").parquet(str(part)))
        print(f"packed {min(i + batch_size, len(files))}/{len(files)}", flush=True)


def process(row):
    import extractor as ex
    file_name, content = row
    page = bytes(content).decode("utf-8", errors="ignore")
    page_id = file_name[:-5].split("__")[-1]
    if file_name.startswith("player__"):
        result = ex.parse_player(page, page_id)
        if not result:
            return [("failed", file_name)]
        return [("player", result[0])] + [("career", r) for r in result[1]]
    if file_name.startswith("team__"):
        result = ex.parse_team(page, page_id)
        return [("team", result) if result else ("failed", file_name)]
    return []


def to_df(spark, rdd, fields):
    def kind(f):
        return T.LongType() if f in INT_COLS else T.DoubleType() if f in DOUBLE_COLS else T.StringType()

    def row(d):
        return tuple(None if d.get(f) in ("", None) else
                     int(d[f]) if f in INT_COLS else float(d[f]) if f in DOUBLE_COLS else str(d[f])
                     for f in fields)
    schema = T.StructType([T.StructField(f, kind(f), True) for f in fields])
    return spark.createDataFrame(rdd.map(row), schema)


def as_text(df, fields):
    return df.select(*[F.coalesce(F.col(f).cast("double").cast("string") if f in INT_COLS | DOUBLE_COLS
                                  else F.col(f), F.lit("")).alias(f) for f in fields])


def compare(spark, df, fields, path):
    original = spark.read.csv(str(path), sep="\t", header=True, quote='"', escape='"')
    a, b = as_text(df, fields), as_text(original, fields)
    only_spark, only_orig = a.exceptAll(b).count(), b.exceptAll(a).count()
    same = only_spark == only_orig == 0
    print(f"  {path.name:<18} spark {df.count():>6} | original {original.count():>6} | "
          f"only spark {only_spark} | only original {only_orig} -> {'IDENTICAL' if same else 'DIFFERENT'}")
    return same


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=str(PROJECT / "data" / "raw"))
    ap.add_argument("--html", default=str(PROJECT / "data" / "html.parquet"))
    ap.add_argument("--out", default=str(PROJECT / "data" / "crawl"))
    ap.add_argument("--original", default=str(PROJECT / "data" / "extracted"))
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--memory", default="2g")
    args = ap.parse_args()

    spark = get_spark("vinf-extract", args.master, args.memory)
    pack(spark, args.raw, Path(args.html), args.batch_size)

    pages = spark.read.parquet(args.html).rdd.map(lambda r: (r["file_name"], r["content"]))
    tagged = pages.flatMap(process).persist()
    failed = tagged.filter(lambda t: t[0] == "failed").map(lambda t: t[1]).collect()
    tables = {}
    for tag, name, fields in (("player", "players", PLAYER_FIELDS), ("career", "player_career", CAREER_FIELDS),
                              ("team", "teams", TEAM_FIELDS)):
        tables[name] = to_df(spark, tagged.filter(lambda t, tag=tag: t[0] == tag).map(lambda t: t[1]), fields)
        n = write_tsv(tables[name].orderBy(fields[0]), Path(args.out) / f"{name}.tsv")
        print(f"{name}: {n} rows")
    print(f"pages: {pages.count()}, not parsed: {len(failed)} {failed[:5]}")

    original = Path(args.original)
    if (original / "players.tsv").exists():
        same = [compare(spark, tables["players"], PLAYER_FIELDS, original / "players.tsv"),
                compare(spark, tables["player_career"], CAREER_FIELDS, original / "player_career.tsv"),
                compare(spark, tables["teams"], TEAM_FIELDS, original / "teams.tsv")]
        print("RESULT:", "Spark output == original output" if all(same) else "differences found")
    spark.stop()


if __name__ == "__main__":
    main()
