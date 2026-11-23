# PyLucene index nad hracmi a klubmi (crawl + wiki atributy z joinu) a vyhladavanie.
# Prikazy: build (vytvori index), search (dopyt), compare (stary TF-IDF index vs Lucene, DokuWiki tabulky).
import argparse
import csv
import re
import sys
import time
import unicodedata
from collections import defaultdict
from pathlib import Path

import lucene

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
sys.path.insert(0, str(HERE))

TEXT_FIELDS = {"name": 4.0, "wiki_title": 3.0, "club": 2.0, "country": 2.0, "position": 2.0,
               "national_team": 1.5, "nickname": 2.0, "stadium": 1.5, "coach": 1.5, "city": 1.0,
               "birth_place": 1.0, "league": 1.0, "career": 0.5}
NUMBER_FIELDS = ["age", "born", "value", "contract", "goals", "matches", "height", "caps", "national_caps",
                 "debut", "capacity", "squad", "founded"]
FILTER_RE = re.compile(r"^(?P<field>" + "|".join(NUMBER_FIELDS) + r")(?P<op><=|>=|<|>|=)"
                       r"(?P<num>\d+(?:\.\d+)?)(?P<unit>[km])?$", re.I)
UNITS = {"k": 1_000, "m": 1_000_000}
FOLD = str.maketrans({"ø": "o", "ł": "l", "đ": "d", "ß": "ss", "æ": "ae", "œ": "oe", "ð": "d", "þ": "th", "ı": "i"})


def fold(text):
    text = unicodedata.normalize("NFKD", text or "").translate(FOLD)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def read_tsv(path):
    with Path(path).open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def number(text):
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def year(text):
    return number(text[:4]) if text else None


def players(crawl, join):
    careers = defaultdict(list)
    for row in read_tsv(crawl / "player_career.tsv"):
        careers[row["player_id"]].append(row)
    wiki = {row["player_id"]: row for row in read_tsv(join / "join_players.tsv")}
    for p in read_tsv(crawl / "players.tsv"):
        rows, w = careers.get(p["player_id"], []), wiki.get(p["player_id"], {})
        height = w.get("height_m")
        yield {
            "id": p["player_id"], "type": "player",
            "title": f'{p["name"]} - ' + ", ".join(x for x in (p["position"] or p["status"], p["country"], p["club"]) if x),
            "text": {"name": p["name"], "wiki_title": w.get("wiki_title", ""), "club": p["club"],
                     "country": p["country"], "position": " ".join((p["position"], p["status"], w.get("wiki_position", ""))),
                     "national_team": w.get("national_team", ""), "birth_place": w.get("birth_place", ""),
                     "career": " ".join(f'{r["team"]} {r["competition"]}' for r in rows)},
            "numbers": {"age": number(p["age"]), "born": year(p["birth_date"]), "value": number(p["market_value_eur"]),
                        "contract": year(p["contract_expires"]),
                        "goals": sum(number(r["goals"]) or 0 for r in rows) if rows else None,
                        "matches": sum(number(r["matches"]) or 0 for r in rows) if rows else None,
                        "height": round(float(height) * 100) if height else None,
                        "caps": number(w.get("total_caps")), "national_caps": number(w.get("national_caps")),
                        "debut": number(w.get("senior_debut_year"))},
        }


def teams(crawl, join):
    wiki = {row["team_id"]: row for row in read_tsv(join / "join_clubs.tsv")}
    for t in read_tsv(crawl / "teams.tsv"):
        w = wiki.get(t["team_id"], {})
        yield {
            "id": t["team_id"], "type": "team",
            "title": f'{t["name"]} - team, {t["country"]}' + (f', stadium {t["stadium"]}' if t["stadium"] else ""),
            "text": {"name": t["name"], "wiki_title": w.get("wiki_title", ""), "country": t["country"],
                     "city": t["city"], "stadium": " ".join((t["stadium"], w.get("ground", ""))),
                     "coach": " ".join((t["coach"], w.get("manager", ""))), "nickname": w.get("nickname", ""),
                     "league": w.get("league", "")},
            "numbers": {"capacity": number(t["capacity"]), "squad": number(t["squad_size"]),
                        "founded": number(w.get("founded_year"))},
        }


class Lucene:
    def __init__(self, index_dir):
        lucene.initVM(vmargs=["-Djava.awt.headless=true"])
        from java.nio.file import Paths
        from org.apache.lucene.analysis.en import EnglishAnalyzer
        from org.apache.lucene.analysis.standard import StandardAnalyzer
        from org.apache.lucene.store import FSDirectory
        self.directory = FSDirectory.open(Paths.get(str(index_dir)))
        self.analyzer = StandardAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET)

    def build(self, docs):
        from org.apache.lucene.document import (Document, Field, LongPoint, NumericDocValuesField, StoredField,
                                                StringField, TextField)
        from org.apache.lucene.index import IndexWriter, IndexWriterConfig
        config = IndexWriterConfig(self.analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        writer, count = IndexWriter(self.directory, config), 0
        for d in docs:
            doc = Document()
            doc.add(StringField("id", d["id"], Field.Store.YES))
            doc.add(StringField("type", d["type"], Field.Store.YES))
            doc.add(StoredField("title", d["title"]))
            for field, value in d["text"].items():
                if value.strip():
                    doc.add(TextField(field, fold(value), Field.Store.NO))
            for field, value in d["numbers"].items():
                if value is not None:
                    doc.add(LongPoint(field, value))
                    doc.add(NumericDocValuesField(field, value))
                    doc.add(StoredField(field, value))
            writer.addDocument(doc)
            count += 1
        writer.commit()
        writer.close()
        return count

    def search(self, query, top=10, any_word=False, kind=None):
        from org.apache.lucene.document import LongPoint
        from org.apache.lucene.queryparser.classic import QueryParser
        words, filters = [], []
        for raw in query.split():
            m = FILTER_RE.match(raw)
            if not m:
                words.append(raw)
                continue
            field, op = m.group("field").lower(), m.group("op")
            value = float(m.group("num")) * UNITS.get((m.group("unit") or "").lower(), 1)
            if field == "height" and value < 3:
                value *= 100
            value = int(round(value))
            low, high = {"<": (-2**62, value - 1), "<=": (-2**62, value), ">": (value + 1, 2**62),
                         ">=": (value, 2**62), "=": (value, value)}[op]
            filters.append((field, op, LongPoint.newRangeQuery(field, low, high)))

        text = " ".join(words)
        results, parsed = self.run(text, filters, top, kind, QueryParser.Operator.OR if any_word else QueryParser.Operator.AND)
        if not results and not any_word:
            results, parsed = self.run(text, filters, top, kind, QueryParser.Operator.OR)
        return results, parsed

    def run(self, text, filters, top, kind, operator):
        from java.lang import Float
        from java.util import HashMap
        from org.apache.lucene.index import DirectoryReader, Term
        from org.apache.lucene.queryparser.classic import MultiFieldQueryParser
        from org.apache.lucene.search import (BooleanClause, BooleanQuery, IndexSearcher, MatchAllDocsQuery, Sort,
                                              SortField, TermQuery)
        builder = BooleanQuery.Builder()
        if text.strip("-+ "):
            boosts = HashMap()
            for field, boost in TEXT_FIELDS.items():
                boosts.put(field, Float(boost))
            parser = MultiFieldQueryParser(list(TEXT_FIELDS), self.analyzer, boosts)
            parser.setDefaultOperator(operator)
            builder.add(MultiFieldQueryParser.parse(parser, fold(text)), BooleanClause.Occur.MUST)
        else:
            builder.add(MatchAllDocsQuery(), BooleanClause.Occur.MUST)
        for _, _, range_query in filters:
            builder.add(range_query, BooleanClause.Occur.FILTER)
        if kind:
            builder.add(TermQuery(Term("type", kind)), BooleanClause.Occur.FILTER)
        lucene_query = builder.build()

        searcher = IndexSearcher(DirectoryReader.open(self.directory))
        if text.strip("-+ ") or not filters:
            hits = searcher.search(lucene_query, top).scoreDocs
        else:
            field, op, _ = filters[0]
            sort = Sort(SortField(field, SortField.Type.LONG, op not in ("<", "<=")))
            hits = searcher.search(lucene_query, top, sort).scoreDocs
        stored = searcher.storedFields()
        results = []
        for hit in hits:
            doc = stored.document(hit.doc)
            score = hit.score if hit.score == hit.score else float(doc.get(filters[0][0]) or 0)
            results.append((doc.get("type") + ":" + doc.get("id"), doc.get("title"), score))
        return results, lucene_query.toString()


def cmd_build(args):
    started = time.monotonic()
    crawl, join = Path(args.crawl), Path(args.join)
    count = Lucene(args.index).build(list(players(crawl, join)) + list(teams(crawl, join)))
    print(f"Lucene index: {count} documents in {time.monotonic() - started:.1f} s -> {args.index}")


def cmd_search(args):
    started = time.monotonic()
    results, parsed = Lucene(args.index).search(args.query, args.top, args.any, args.type)
    print(f"query: {parsed}  ({(time.monotonic() - started) * 1000:.0f} ms)")
    if not results:
        print("No results.")
    for rank, (doc_id, title, score) in enumerate(results, 1):
        print(f"{rank:>2}. {score:8.3f}  {title}   [{doc_id}]")


def cmd_compare(args):
    import indexer
    old = indexer.Index(Path(args.old_index))
    new = Lucene(args.index)
    for query in args.queries:
        started = time.monotonic()
        before, _ = old.search(query, "classic", args.top, False, "log")
        old_ms = (time.monotonic() - started) * 1000
        started = time.monotonic()
        after, parsed = new.search(query, args.top)
        new_ms = (time.monotonic() - started) * 1000
        print(f"==== ''{query}'' ====\n")
        print(f"^ # ^ starý index (TF-IDF, {old_ms:.0f} ms) ^ Lucene ({new_ms:.0f} ms) ^")
        for i in range(max(len(before), len(after), 1)):
            left = before[i][0]["title"] if i < len(before) else "-"
            right = after[i][1] if i < len(after) else "-"
            print(f"| {i + 1} | {left.replace('|', '/')} | {right.replace('|', '/')} |")
        print()


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", default=str(PROJECT / "data" / "index_lucene"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("build")
    p.add_argument("--crawl", default=str(PROJECT / "data" / "crawl"))
    p.add_argument("--join", default=str(PROJECT / "data" / "join"))
    p = sub.add_parser("search")
    p.add_argument("query")
    p.add_argument("--top", type=int, default=10)
    p.add_argument("--any", action="store_true")
    p.add_argument("--type", choices=("player", "team"))
    p = sub.add_parser("compare")
    p.add_argument("queries", nargs="+")
    p.add_argument("--top", type=int, default=5)
    p.add_argument("--old-index", default=str(PROJECT / "data" / "index_tfidf"))
    args = ap.parse_args()
    {"build": cmd_build, "search": cmd_search, "compare": cmd_compare}[args.cmd](args)


if __name__ == "__main__":
    main()
