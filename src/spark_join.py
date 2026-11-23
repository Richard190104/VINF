# Spark join crawlu so soccerway a wiki dat: hraci cez meno + datum narodenia, kluby cez meno
# + stadion/kapacitu. Zapise spojene tabulky, statistiku a report (DokuWiki tabulky).
import argparse
import time
from pathlib import Path

from common import PROJECT, get_spark, norm_club, norm_name, write_tsv

PLAYER_WIKI_ATTRS = {
    "height_m": "výška v metroch; soccerway ju nemá, dá sa použiť vo filtroch (height>=1.90) a v dopytoch na brankárov",
    "birth_place": "miesto narodenia; nové textové pole (dopyty 'born in Hillingdon')",
    "total_caps": "počet zápasov v celej seniorskej kariére; z crawlu máme len posledné sezóny",
    "total_goals": "góly v celej seniorskej kariére; doplnok k góloom po sezónach z crawlu",
    "national_team": "reprezentácia (senior); crawl má len krajinu, nie za ktorý tím hrá",
    "national_caps": "zápasy za reprezentáciu; filter pre 'skúsených' hráčov",
    "senior_clubs": "počet seniorských klubov v kariére (kariérna dráha)",
    "youth_clubs": "počet mládežníckych klubov (akadémia)",
    "senior_debut_year": "rok seniorského debutu; vek kariéry a filtre",
    "n_categories": "počet kategórií článku; lacný signál popularity hráča pre ranking",
    "text_length": "dĺžka článku; ďalší signál popularity/kvality pre ranking",
}
CLUB_WIKI_ATTRS = {
    "founded_year": "rok založenia; soccerway ho nemá (filter 'founded<1900')",
    "nickname": "prezývka klubu ('The Reds'); užívateľ hľadá takto, v crawle chýba",
    "manager": "tréner podľa wiki; kontrola proti `coach` z crawlu",
    "owner": "majiteľ/predseda; nové pole",
    "league": "liga klubu; kontext k dopytom",
    "ground": "štadión podľa wiki; kontrola proti `stadium` z crawlu a kľúč pri nejednoznačnosti",
    "capacity": "kapacita podľa wiki; kontrola proti `capacity` z crawlu",
}


def read_tsv(spark, path):
    return spark.read.csv(str(path), sep="\t", header=True, inferSchema=False)


def exploded_keys(df, key_fn, name_cols):
    from pyspark.sql import functions as F
    keys = F.array(*[key_fn(F.col(c)) for c in name_cols])
    return (df.withColumn("key", F.explode(keys)).where("key != ''")
              .dropDuplicates(["page_id", "key"]))


def one_per(df, partition, order):
    from pyspark.sql import Window, functions as F
    window = Window.partitionBy(partition).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def join_players(spark, crawl, wiki):
    from pyspark.sql import functions as F
    nn, nc = F.udf(norm_name), F.udf(norm_club)
    c = (crawl.select(F.col("player_id"), F.col("name").alias("crawl_name"), F.col("birth_date").alias("crawl_birth"),
                      F.col("club").alias("crawl_club"))
         .withColumn("key", nn("crawl_name")))
    w = wiki.withColumn("w_birth", F.col("birth_date"))
    wk = exploded_keys(w.withColumnRenamed("name", "wiki_name"), nn, ["title", "wiki_name", "full_name"])
    wk = wk.select("page_id", "key", "title", "w_birth", "current_club")

    strong = (c.join(F.broadcast(wk), ["key"]).where(F.col("crawl_birth") == F.col("w_birth"))
              .withColumn("match_type", F.lit("name+birth_date")))
    strong_one = one_per(strong, "player_id", [F.col("page_id")])
    n_strong_conflicts = strong.groupBy("player_id").agg(F.countDistinct("page_id").alias("n")).where("n > 1").count()

    rest = c.join(strong_one.select("player_id"), "player_id", "left_anti")
    key_count = c.groupBy("key").agg(F.count("*").alias("crawl_same_name"))
    cand = (rest.join(F.broadcast(wk), ["key"])
            .where(F.col("w_birth").isNull() | F.col("crawl_birth").isNull()))
    per_player = (cand.groupBy("player_id").agg(F.countDistinct("page_id").alias("n_pages"), F.min("page_id").alias("page_id")))
    cand = (cand.join(per_player.select("player_id", "n_pages", F.col("page_id").alias("_pid")), "player_id")
            .where(F.col("page_id") == F.col("_pid")).drop("_pid")
            .join(key_count, "key"))
    name_only = one_per(cand.where("n_pages = 1 AND crawl_same_name = 1"), "player_id", [F.col("page_id")]) \
        .withColumn("match_type", F.lit("name_only_unique"))
    ambiguous = cand.where("n_pages > 1 OR crawl_same_name > 1").select("player_id").distinct().count()

    cols = ["player_id", "crawl_name", "crawl_birth", "crawl_club", "page_id", "title", "match_type", "current_club"]
    both = strong_one.select(*cols).unionByName(name_only.select(*cols))
    attrs = [a for a in PLAYER_WIKI_ATTRS] + ["position"]
    joined = (both.join(wiki.select("page_id", *attrs), "page_id")
              .withColumn("club_agrees", F.when(
                  (F.col("current_club").isNotNull()) & (F.col("crawl_club").isNotNull()),
                  nc("current_club") == nc("crawl_club")))
              .withColumnRenamed("page_id", "wiki_page_id").withColumnRenamed("title", "wiki_title")
              .withColumnRenamed("current_club", "wiki_current_club").withColumnRenamed("position", "wiki_position"))
    return joined, {"strong_conflicts": n_strong_conflicts, "ambiguous": ambiguous}


def join_clubs(spark, crawl, wiki):
    from pyspark.sql import Window, functions as F
    nn, nc = F.udf(norm_name), F.udf(norm_club)
    c = (crawl.select(F.col("team_id"), F.col("name").alias("crawl_name"), F.col("stadium").alias("crawl_stadium"),
                      F.col("capacity").cast("long").alias("crawl_capacity"))
         .withColumn("key", nc("crawl_name")).where("key != ''"))
    wk = exploded_keys(wiki, nc, ["title", "club_name", "full_name"])
    wk = wk.select("page_id", "key", "title", "ground", F.col("capacity").alias("w_capacity"))
    j = (c.join(F.broadcast(wk), ["key"])
         .withColumn("ground_ok", (nn("crawl_stadium") == nn("ground")).cast("int"))
         .withColumn("capacity_ok", (F.col("crawl_capacity") == F.col("w_capacity")).cast("int"))
         .withColumn("score", F.coalesce(F.col("ground_ok"), F.lit(0)) + F.coalesce(F.col("capacity_ok"), F.lit(0))))
    by_team = Window.partitionBy("team_id")
    j = (j.withColumn("n_pages", F.size(F.collect_set("page_id").over(by_team)))
          .withColumn("best", F.max("score").over(by_team)).where(F.col("score") == F.col("best"))
          .withColumn("n_best", F.size(F.collect_set("page_id").over(by_team))))
    ok = j.where("n_best = 1 AND (n_pages = 1 OR best > 0)")
    ambiguous = j.where("NOT (n_best = 1 AND (n_pages = 1 OR best > 0))").select("team_id").distinct().count()
    ok = one_per(ok, "team_id", [F.col("page_id")]).withColumn(
        "match_type", F.when(F.col("best") > 0, "name+ground/capacity").otherwise("name_only_unique"))
    attrs = list(CLUB_WIKI_ATTRS)
    joined = (ok.select("team_id", "crawl_name", "crawl_stadium", "crawl_capacity", "page_id", "match_type")
              .join(wiki.select("page_id", F.col("title").alias("wiki_title"), *attrs), "page_id")
              .withColumn("ground_agrees", (nn("crawl_stadium") == nn("ground")))
              .withColumn("capacity_agrees", F.col("crawl_capacity") == F.col("capacity"))
              .withColumnRenamed("page_id", "wiki_page_id"))
    return joined, {"ambiguous": ambiguous}


def coverage(df, attrs):
    from pyspark.sql import functions as F
    row = df.agg(*[F.count(F.when(F.col(a).isNotNull(), 1)).alias(a) for a in attrs]).first()
    return {a: row[a] for a in attrs}


def pct(part, whole):
    return f"{100 * part / whole:.1f} %" if whole else "-"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--crawl", default=str(PROJECT / "data" / "crawl"))
    ap.add_argument("--wiki", default=str(PROJECT / "data" / "wiki"))
    ap.add_argument("--out", default=str(PROJECT / "data" / "join"))
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--memory", default="2g")
    args = ap.parse_args()
    from pyspark.sql import functions as F

    spark = get_spark("vinf-join", args.master, args.memory)
    started, out = time.monotonic(), Path(args.out)
    crawl_players, crawl_teams = read_tsv(spark, Path(args.crawl) / "players.tsv"), read_tsv(spark, Path(args.crawl) / "teams.tsv")
    wiki_players = spark.read.parquet(str(Path(args.wiki) / "parquet" / "wiki_players"))
    wiki_clubs = spark.read.parquet(str(Path(args.wiki) / "parquet" / "wiki_clubs"))

    jp, pinfo = join_players(spark, crawl_players, wiki_players)
    jc, cinfo = join_clubs(spark, crawl_teams, wiki_clubs)
    jp, jc = jp.cache(), jc.cache()
    write_tsv(jp.orderBy("player_id"), out / "join_players.tsv")
    write_tsv(jc.orderBy("team_id"), out / "join_clubs.tsv")

    n_cp, n_ct = crawl_players.count(), crawl_teams.count()
    n_wp, n_wc = wiki_players.count(), wiki_clubs.count()
    n_jp, n_jc = jp.count(), jc.count()
    u_jp = jp.select("wiki_page_id").distinct().count()
    u_jc = jc.select("wiki_page_id").distinct().count()
    by_type = {r["match_type"]: r["count"] for r in jp.groupBy("match_type").count().collect()}
    cby_type = {r["match_type"]: r["count"] for r in jc.groupBy("match_type").count().collect()}
    agree = {r["match_type"]: (r["yes"], r["known"]) for r in jp.groupBy("match_type").agg(
        F.count(F.when(F.col("club_agrees"), 1)).alias("yes"), F.count("club_agrees").alias("known")).collect()}
    cap_yes = jc.where("capacity_agrees").count(); cap_known = jc.where("capacity_agrees IS NOT NULL").count()
    p_cov, c_cov = coverage(jp, list(PLAYER_WIKI_ATTRS)), coverage(jc, list(CLUB_WIKI_ATTRS))

    stats = [("crawl: players", n_cp), ("crawl: clubs", n_ct), ("wiki: football biography pages (in dump part)", n_wp),
             ("wiki: football club pages (in dump part)", n_wc),
             ("joined players (crawl rows)", n_jp), ("  by name+birth_date", by_type.get("name+birth_date", 0)),
             ("  by unique name only", by_type.get("name_only_unique", 0)),
             ("players left ambiguous (not joined)", pinfo["ambiguous"]),
             ("UNIQUE wiki player pages joined", u_jp),
             ("joined clubs (crawl rows)", n_jc), ("  by name+ground/capacity", cby_type.get("name+ground/capacity", 0)),
             ("  by unique name only", cby_type.get("name_only_unique", 0)),
             ("clubs left ambiguous (not joined)", cinfo["ambiguous"]),
             ("UNIQUE wiki club pages joined", u_jc), ("UNIQUE wiki pages joined, total", u_jp + u_jc),
             ("join step seconds", round(time.monotonic() - started))]
    write_tsv(spark.createDataFrame([(k.strip(), v) for k, v in stats], "metric string, value long"), out / "join_stats.tsv")
    for k, v in stats:
        print(f"  {k:<48} {v:>8}")

    samples = jp.where("match_type = 'name+birth_date' AND total_caps IS NOT NULL").orderBy(F.desc("total_caps")).limit(8).collect()
    sample_clubs = jc.where("founded_year IS NOT NULL").orderBy(F.desc("capacity")).limit(6).collect()
    lines = ["====== Konzultácia 3: spojenie crawlu s wiki dátami (vygenerované spark_join.py) ======", "",
             "===== Počty =====", "^ Metrika ^ Hodnota ^ Podiel ^"]
    shares = {"joined players (crawl rows)": pct(n_jp, n_cp), "joined clubs (crawl rows)": pct(n_jc, n_ct)}
    for k, v in stats:
        lines.append(f"| {k.strip() if not k.startswith('  ') else '… ' + k.strip()} | {v} | {shares.get(k, '')} |")
    lines += ["", "===== Kontrola správnosti spojenia =====", "^ Typ spojenia ^ Klub z crawlu = klub z wiki ^",
              *[f"| {t} | {y} z {n} ({pct(y, n)}) |" for t, (y, n) in agree.items()],
              f"| kapacita štadióna crawl = wiki (kluby) | {cap_yes} z {cap_known} ({pct(cap_yes, cap_known)}) |", "",
              "===== Zaujímavé atribúty z wiki =====", "^ Atribút ^ Vyplnený (hráči/kluby) ^ Prečo je zaujímavý ^"]
    for a, why in PLAYER_WIKI_ATTRS.items():
        lines.append(f"| hráč: %s | %d z %d (%s) | %s |" % (a, p_cov[a], n_jp, pct(p_cov[a], n_jp), why))
    for a, why in CLUB_WIKI_ATTRS.items():
        lines.append(f"| klub: %s | %d z %d (%s) | %s |" % (a, c_cov[a], n_jc, pct(c_cov[a], n_jc), why))
    lines += ["", "===== Ukážka spojených hráčov =====", "^ crawl: meno ^ wiki stránka ^ narodený ^ miesto narodenia ^ výška ^ zápasy ^ góly ^ reprezentácia ^"]
    for r in samples:
        lines.append(f"| {r['crawl_name']} | {r['wiki_title']} | {r['crawl_birth']} | {r['birth_place'] or ''} | {r['height_m'] or ''} | {r['total_caps'] or ''} | {r['total_goals'] or ''} | {r['national_team'] or ''} ({r['national_caps'] or ''}) |")
    lines += ["", "===== Ukážka spojených klubov =====", "^ crawl: klub ^ wiki stránka ^ založený ^ prezývka ^ štadión (crawl / wiki) ^ kapacita (crawl / wiki) ^"]
    for r in sample_clubs:
        lines.append(f"| {r['crawl_name']} | {r['wiki_title']} | {r['founded_year'] or ''} | {r['nickname'] or ''} | {r['crawl_stadium']} / {r['ground'] or ''} | {r['crawl_capacity']} / {r['capacity'] or ''} |")
    (out / "wiki_report.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"report: {out / 'wiki_report.txt'}")
    spark.stop()


if __name__ == "__main__":
    main()
