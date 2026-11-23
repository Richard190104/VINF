#!/usr/bin/env python3
"""
Own indexer + search for the soccerway data (VI2026 project, consultation 2b).

Documents : one per player (players.tsv + his career rows) and one per team (teams.tsv).
Index     : inverted index  term -> {document: TF}  (TF = raw number of occurrences).
Ranking   : score(d, q) = coord(d, q) * sum over query terms of  w(TF(t, d)) * IDF(t)
              w(TF)  = 1 + log10(TF)   (default, --tf log) or the raw TF (--tf raw).
                       Sublinear: a term repeated 20 times cannot beat a document that
                       matches more of the query terms.
              coord  = (query terms found in the document) / (query terms in the index)
            Two IDF methods:
              classic : idf = log10(N / df)
              smooth  : idf = ln(1 + N / df)       (never zero, even for terms in every document)
No stemming / lemmatization (not required). Only the standard library is used.

Query language = words + optional filters:
    brazil goalkeeper                         words, ranked by TF-IDF
    forward england born>=2000 goals>=10      numeric filters on the structured data
    capacity>60000                            filters only (sorted by the filtered attribute)
    premier league defender -england          -word removes documents containing the word
Filter attributes:  age | born (birth year) | value (market value in EUR, suffix k or m allowed)
                    | contract (year the contract expires) | goals, matches (sums over all career
                    rows) | capacity (stadium of a team) | squad (players listed on a team page)
Operators: <  <=  >  >=  =        A document without the attribute never passes its filter.

Commands (run in the folder that contains data/extracted):
    python indexer.py build                            # build the index, save it to data/index
    python indexer.py search "brazil goalkeeper"       # search (loads the saved index)
    python indexer.py search "premier league forward" --idf smooth --top 20 --all --tf raw
    python indexer.py compare "brazil goalkeeper" "uruguay defender" "anfield"
          -> compares both IDF methods, output is a ready-to-paste DokuWiki table
    python indexer.py report "Brazilian goalkeepers::brazil goalkeeper" "capacity>60000"
          -> question / query / system answer (top 3, AND mode) as a DokuWiki table
"""
import argparse
import csv
import math
import operator
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

TOKEN_RE = re.compile(r"[a-z0-9]+")
# letters that Unicode decomposition does not turn into plain ASCII
EXTRA_LETTERS = str.maketrans({"ø": "o", "ł": "l", "đ": "d", "ß": "ss", "æ": "ae",
                               "œ": "oe", "ð": "d", "þ": "th", "ı": "i"})

ATTRIBUTES = ["age", "born", "value", "contract", "goals", "matches", "capacity", "squad"]
FILTER_RE = re.compile(
    r"^(?P<attr>" + "|".join(ATTRIBUTES) + r")(?P<op><=|>=|<|>|=)"
    r"(?P<num>\d+(?:\.\d+)?)(?P<unit>[km])?$", re.I)
UNITS = {"k": 1_000, "m": 1_000_000}
OPERATORS = {"<": operator.lt, "<=": operator.le, ">": operator.gt,
             ">=": operator.ge, "=": operator.eq}


# ---------------------------------------------------------------- text processing
def tokenize(text):
    """Lower-case, strip diacritics (Kanté -> kante), split to [a-z0-9]+ tokens."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    return TOKEN_RE.findall(text.translate(EXTRA_LETTERS))


def parse_query(query):
    """Split a query into (words, filters, excluded words).
    filters = [(attribute, operator, number)], e.g. ('born', '>=', 2000)."""
    words, filters, excluded = [], [], []
    for raw in query.split():
        match = FILTER_RE.match(raw)
        if match:
            number = float(match.group("num")) * UNITS.get((match.group("unit") or "").lower(), 1)
            filters.append((match.group("attr").lower(), match.group("op"), number))
        elif raw.startswith("-") and len(raw) > 1:
            excluded += tokenize(raw[1:])
        else:
            words += tokenize(raw)
    return list(dict.fromkeys(words)), filters, list(dict.fromkeys(excluded))


def fmt_number(value):
    return str(int(value)) if float(value).is_integer() else str(value)


def describe(filters, excluded):
    parts = [f"{attr}{op}{fmt_number(value)}" for attr, op, value in filters]
    return ", ".join(parts + [f"-{term}" for term in excluded])


# ---------------------------------------------------------------- IDF methods
def idf_classic(n_docs, df):
    return math.log10(n_docs / df)


def idf_smooth(n_docs, df):
    return math.log(1 + n_docs / df)


IDF_METHODS = {"classic": idf_classic, "smooth": idf_smooth}

# ---------------------------------------------------------------- TF weighting
TF_SCHEMES = {
    "raw": lambda tf: tf,
    "log": lambda tf: 1 + math.log10(tf),
}


# ---------------------------------------------------------------- documents
def read_tsv(path):
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def number_or_none(text):
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def build_documents(data_dir):
    """Return a list of (doc_id, type, title, searchable text, numeric attributes)."""
    players = read_tsv(data_dir / "players.tsv")
    teams = read_tsv(data_dir / "teams.tsv") if (data_dir / "teams.tsv").exists() else []
    careers = defaultdict(list)
    for row in read_tsv(data_dir / "player_career.tsv"):
        careers[row["player_id"]].append(row)

    documents = []
    for p in players:
        rows = careers.get(p["player_id"], [])
        parts = [p["name"], p["country"], p["position"], p["status"], p["club"],
                 p.get("loan_from", "")]
        if p["birth_date"]:
            parts.append("born " + p["birth_date"][:4])
        for row in rows:                                  # repeated seasons give a higher TF
            parts += [row["team"], row["competition"]]
        attrs = {
            "age": number_or_none(p["age"]),
            "born": number_or_none(p["birth_date"][:4]) if p["birth_date"] else None,
            "value": number_or_none(p["market_value_eur"]),
            "contract": number_or_none(p["contract_expires"][:4]) if p["contract_expires"] else None,
            "goals": sum(number_or_none(r["goals"]) or 0 for r in rows) if rows else None,
            "matches": sum(number_or_none(r["matches"]) or 0 for r in rows) if rows else None,
        }
        details = ", ".join(x for x in (p["position"] or p["status"], p["country"], p["club"]) if x)
        documents.append((f'player:{p["player_id"]}', "player", f'{p["name"]} - {details}',
                          " ".join(parts), attrs))
    for t in teams:                         # youth / national teams have no stadium
        parts = [t["name"], t["country"], t["city"]]
        if t["stadium"]:
            parts += ["stadium", t["stadium"]]
        if t["coach"]:
            parts += ["coach", t["coach"]]
        title = f'{t["name"]} - team, {t["country"]}' + (f', stadium {t["stadium"]}' if t["stadium"] else "")
        attrs = {"capacity": number_or_none(t["capacity"]),
                 "squad": number_or_none(t.get("squad_size", ""))}
        documents.append((f'team:{t["team_id"]}', "team", title, " ".join(parts), attrs))
    return documents


# ---------------------------------------------------------------- index build / save / load
def build_index(documents):
    postings = defaultdict(dict)          # term -> {doc_index: tf}
    lengths = []
    for doc_index, document in enumerate(documents):
        tokens = tokenize(document[3])
        lengths.append(len(tokens))
        for term, tf in Counter(tokens).items():
            postings[term][doc_index] = tf
    return postings, lengths


def save_index(out_dir, documents, postings, lengths, seconds):
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "docs.tsv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["doc_index", "doc_id", "type", "title", "length"] + ATTRIBUTES)
        for i, (doc_id, kind, title, _, attrs) in enumerate(documents):
            writer.writerow([i, doc_id, kind, title, lengths[i]]
                            + ["" if attrs.get(a) is None else attrs[a] for a in ATTRIBUTES])
    with (out_dir / "index.tsv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["term", "df", "postings (doc_index:tf)"])
        for term in sorted(postings):
            docs = postings[term]
            writer.writerow([term, len(docs), ",".join(f"{d}:{tf}" for d, tf in sorted(docs.items()))])
    # one line per build, so the report can say how many times the index was created
    log_path = out_dir / "build_log.tsv"
    new_log = not log_path.exists()
    with log_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        if new_log:
            writer.writerow(["built_at", "documents", "terms", "postings", "seconds"])
        writer.writerow([datetime.now(timezone.utc).isoformat(timespec="seconds"), len(documents),
                         len(postings), sum(len(d) for d in postings.values()), round(seconds, 2)])


class Index:
    """Index loaded from disk."""

    def __init__(self, out_dir):
        self.docs = read_tsv(out_dir / "docs.tsv")
        self.attrs = [{a: number_or_none(row.get(a, "")) for a in ATTRIBUTES} for row in self.docs]
        self.postings = {}
        for row in read_tsv(out_dir / "index.tsv"):
            pairs = (item.split(":") for item in row["postings (doc_index:tf)"].split(","))
            self.postings[row["term"]] = {int(d): int(tf) for d, tf in pairs}
        self.n_docs = len(self.docs)
        # an index built by an older version has no numeric columns, so filters cannot work
        self.has_attributes = bool(self.docs) and all(a in self.docs[0] for a in ATTRIBUTES)

    def idf(self, term, method):
        return IDF_METHODS[method](self.n_docs, len(self.postings[term]))

    def passes(self, doc_index, filters):
        """True if the document has every filtered attribute and satisfies all filters."""
        for attr, op, value in filters:
            actual = self.attrs[doc_index].get(attr)
            if actual is None or not OPERATORS[op](actual, value):
                return False
        return True

    def search(self, query, method="classic", top=10, require_all=False, tf_scheme="log"):
        """Return (ranked [(doc_row, score)], unknown query words)."""
        words, filters, excluded = parse_query(query)
        banned = set()
        for term in excluded:
            banned.update(self.postings.get(term, {}))

        if not words:                                        # only filters / exclusions
            return (self._filter_only(filters, banned, top) if filters else []), []

        known = [t for t in words if t in self.postings]
        unknown = [t for t in words if t not in self.postings]
        if not known or (require_all and unknown):
            return [], unknown
        scores, matched = defaultdict(float), defaultdict(int)
        tf_weight = TF_SCHEMES[tf_scheme]
        for term in known:
            weight = self.idf(term, method)
            for doc_index, tf in self.postings[term].items():
                scores[doc_index] += tf_weight(tf) * weight
                matched[doc_index] += 1
        for doc_index in scores:          # coordination: more matched query terms = higher rank
            scores[doc_index] *= matched[doc_index] / len(known)
        candidates = [d for d in scores
                      if d not in banned and self.passes(d, filters)
                      and (not require_all or matched[d] == len(known))]
        candidates.sort(key=lambda d: (-scores[d], d))
        return [(self.docs[d], scores[d]) for d in candidates[:top]], unknown

    def _filter_only(self, filters, banned, top):
        """No query words: list all documents passing the filters, sorted by the first
        filtered attribute (descending for > and >=, ascending for < and <=)."""
        attr, op, _ = filters[0]
        sign = 1 if op in ("<", "<=") else -1
        candidates = [d for d in range(self.n_docs) if d not in banned and self.passes(d, filters)]
        candidates.sort(key=lambda d: (sign * self.attrs[d][attr], d))
        return [(self.docs[d], float(self.attrs[d][attr])) for d in candidates[:top]]


# ---------------------------------------------------------------- commands
def check_attributes(index, queries):
    """Filters need the numeric columns that only a freshly built index contains."""
    if not index.has_attributes and any(parse_query(q)[1] for q in queries):
        sys.exit("This index was built by an older version and has no numeric attributes,\n"
                 "so filters (born>=, value>=, goals>=, ...) cannot work.\n"
                 "Build it again:  python indexer.py build")


def cmd_build(args):
    started = time.monotonic()
    documents = build_documents(Path(args.data))
    postings, lengths = build_index(documents)
    seconds = time.monotonic() - started
    save_index(Path(args.out), documents, postings, lengths, seconds)
    kinds = Counter(d[1] for d in documents)
    print(f"Index built in {seconds:.2f} s: {len(documents)} documents "
          f"({kinds['player']} players, {kinds['team']} teams), {len(postings)} terms, "
          f"{sum(len(d) for d in postings.values())} postings -> {args.out}")


def cmd_search(args):
    index = Index(Path(args.out))
    check_attributes(index, [args.query])
    _, filters, excluded = parse_query(args.query)
    if filters or excluded:
        print(f"(filters: {describe(filters, excluded)})")
    results, unknown = index.search(args.query, args.idf, args.top, args.all, args.tf)
    if unknown:
        print(f"(terms not in the index: {', '.join(unknown)})")
    if not results:
        print("No results.")
    for rank, (doc, score) in enumerate(results, 1):
        print(f"{rank:>2}. {score:8.3f}  {doc['title']}   [{doc['doc_id']}]")


def cmd_compare(args):
    index = Index(Path(args.out))
    check_attributes(index, args.queries)
    for query in args.queries:
        terms, filters, excluded = parse_query(query)
        print(f'==== Dopyt: "{query}" ====\n')
        if filters or excluded:
            print(f"Filtre: {describe(filters, excluded)}\n")
        print("^ term ^ df ^ idf classic ^ idf smooth ^")
        for term in terms:
            if term in index.postings:
                print(f"| {term} | {len(index.postings[term])} | {index.idf(term, 'classic'):.3f} "
                      f"| {index.idf(term, 'smooth'):.3f} |")
            else:
                print(f"| {term} | 0 | - | - |")
        classic, _ = index.search(query, "classic", args.top, tf_scheme=args.tf)
        smooth, _ = index.search(query, "smooth", args.top, tf_scheme=args.tf)
        print("\n^ # ^ classic: dokument ^ skóre ^ smooth: dokument ^ skóre ^")
        for i in range(max(len(classic), len(smooth))):
            left = classic[i] if i < len(classic) else None
            right = smooth[i] if i < len(smooth) else None
            print(f"| {i + 1} | {left[0]['title'] if left else '-'} | {f'{left[1]:.3f}' if left else '-'} "
                  f"| {right[0]['title'] if right else '-'} | {f'{right[1]:.3f}' if right else '-'} |")
        ids_c, ids_s = [d['doc_id'] for d, _ in classic], [d['doc_id'] for d, _ in smooth]
        same_first = bool(ids_c and ids_s and ids_c[0] == ids_s[0])
        print(f"\nPrienik top-{args.top}: {len(set(ids_c) & set(ids_s))}/{args.top}; "
              f"rovnaké prvé miesto: {'áno' if same_first else 'nie'}; "
              f"rovnaké poradie: {'áno' if ids_c == ids_s else 'nie'}\n")


def cmd_report(args):
    """Question/answer table for the report. Items are 'question::query' or just 'query';
    the query is evaluated in AND mode (every word must match)."""
    index = Index(Path(args.out))
    pairs = [item.partition("::") for item in args.items]
    check_attributes(index, [query or question for question, _, query in pairs])
    print(f"^ # ^ Otázka ^ Dopyt ^ Odpoveď systému (top {args.top}) ^")
    for number, (question, _, query) in enumerate(pairs, 1):
        query = query or question
        results, _ = index.search(query, "classic", args.top, True, args.tf)
        answer = "; ".join(doc["title"].replace("|", "/") for doc, _ in results) or "bez výsledku"
        print(f"| {number} | {question} | ''{query}'' | {answer} |")


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Own indexer and search (TF-IDF)")
    ap.add_argument("--data", default="data/extracted", help="folder with the extractor TSV files")
    ap.add_argument("--out", default="data/index_tfidf", help="folder for the saved index")
    sub = ap.add_subparsers(dest="command", required=True)

    sub.add_parser("build", help="build and save the index")
    p_search = sub.add_parser("search", help="search the saved index")
    p_search.add_argument("query")
    p_search.add_argument("--idf", choices=list(IDF_METHODS), default="classic")
    p_search.add_argument("--top", type=int, default=10)
    p_search.add_argument("--all", action="store_true", help="every query word must match (AND)")
    p_search.add_argument("--tf", choices=list(TF_SCHEMES), default="log",
                          help="TF weighting: log = 1+log10(tf) (default), raw = plain count")
    p_compare = sub.add_parser("compare", help="compare the IDF methods on several queries")
    p_compare.add_argument("queries", nargs="+")
    p_compare.add_argument("--top", type=int, default=5)
    p_compare.add_argument("--tf", choices=list(TF_SCHEMES), default="log")

    p_report = sub.add_parser("report", help="question/answer table for the report")
    p_report.add_argument("items", nargs="+", help='"question::query" or just "query"')
    p_report.add_argument("--top", type=int, default=3)
    p_report.add_argument("--tf", choices=list(TF_SCHEMES), default="log")

    args = ap.parse_args()
    {"build": cmd_build, "search": cmd_search, "compare": cmd_compare,
     "report": cmd_report}[args.command](args)


if __name__ == "__main__":
    main()
