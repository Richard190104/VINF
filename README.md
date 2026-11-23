# VINF: vyhľadávač futbalových hráčov a klubov (soccerway.com + Wikipédia)

## Priečinky

| Priečinok | Obsah |
|---|---|
| `src/` | celý kód |
| `tests/` | unit testy extraktora a starého indexu |
| `data/` | dáta a výstupy (tabuľka nižšie) |
| `wiki/` | texty odovzdaní a konzultácií pre DokuWiki |
| `prednasky/` | prednášky z predmetu |
| `hadoop/` | `winutils.exe` a `hadoop.dll`, potrebné pre Spark na Windows |

## Postup

Všetko sa spúšťa z koreňa projektu. Python 3.12, `pip install requests tiktoken pyspark==3.5.3`, Java 17. Lucene beží v Dockeri.

| # | Krok | Príkaz | Výstup |
|---|---|---|---|
| 1 | crawl soccerway.com | `python src/crawler.py --max-pages 5000` | `data/raw/`, `data/metadata.tsv` |
| 2 | regex extrakcia (1. odovzdanie) | `python src/extractor.py` | `data/extracted/` |
| 3 | vlastný TF-IDF index (1. odovzdanie) | `python src/indexer.py build`, `python src/indexer.py search "brazil goalkeeper"` | `data/index_tfidf/` |
| 4 | extrakcia cez Spark (K3) | `python src/spark_extract.py` | `data/html.parquet/`, `data/crawl/` |
| 5 | wiki dump cez Spark (K3) | `python src/spark_wiki.py --dump <cesta k enwiki-...-multistream.xml.bz2> --memory 8g --master local[8]` | `data/wiki/` |
| 6 | join crawl x wiki (K3) | `python src/spark_join.py` | `data/join/` |
| 7 | Lucene index (K4) | `lucene build`, `lucene search "goalkeeper height>=195"`, `lucene compare "mesi~"` | `data/index_lucene/` |

`lucene.cmd` spustí `src/lucene_search.py` v Docker image `coady/pylucene`.

## Kód

| Súbor | Čo robí |
|---|---|
| `src/crawler.py` | crawler soccerway.com (robots.txt, pauzy 1,5 až 3 s) |
| `src/extractor.py` | regexy na HTML stránky hráčov a klubov |
| `src/indexer.py` | vlastný invertovaný index, TF-IDF (2 metódy idf), filtre |
| `src/token_stats.py` | počet tokenov stiahnutých stránok (tiktoken) |
| `src/spark_extract.py` | zbalí HTML do parquetu, Spark extrakcia cez `extractor.py`, porovnanie s `data/extracted` |
| `src/spark_wiki.py` | regexy na wikitext, Spark spracovanie wiki dumpu |
| `src/spark_join.py` | join hráčov a klubov s wiki, štatistika a report |
| `src/common.py` | spustenie Sparku, zápis TSV, normalizácia mien |
| `src/lucene_search.py` | PyLucene index, vyhľadávanie, porovnanie so starým indexom |
| `src/win_worker_fix/` | oprava Python workera Sparku na Windows |

## Dáta

| Priečinok | Obsah |
|---|---|
| `data/raw/` | 5 020 stiahnutých HTML stránok |
| `data/metadata.tsv` | metadáta crawlu (URL, typ, hĺbka, veľkosť, hash) |
| `data/extracted/` | výstup `extractor.py`: hráči, kariéra, kluby |
| `data/index_tfidf/` | starý index |
| `data/html.parquet/` | HTML zbalené do 51 parquet dávok |
| `data/crawl/` | výstup Spark extrakcie (zhodný s `data/extracted`) |
| `data/wiki/` | hráči a kluby z wiki dumpu (parquet), štatistika |
| `data/join/` | spojenie crawl x wiki, `wiki_report.txt` |
| `data/index_lucene/` | Lucene index |

Testy: `python tests/test_extractor.py`, `python tests/test_indexer.py`.
