# Spolocne funkcie pre Spark skripty: spustenie Sparku, cesty, zapis TSV a normalizacia mien pre join.
import csv
import os
import re
import sys
import unicodedata
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
sys.path.insert(0, str(HERE))

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
if os.name == "nt":
    os.environ["PYTHONPATH"] = str(HERE / "win_worker_fix") + os.pathsep + os.environ.get("PYTHONPATH", "")
    if not os.environ.get("HADOOP_HOME") and (PROJECT / "hadoop" / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(PROJECT / "hadoop")
        os.environ["PATH"] = str(PROJECT / "hadoop" / "bin") + os.pathsep + os.environ.get("PATH", "")


def get_spark(app, master="local[*]", memory="2g"):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master(master).appName(app)
             .config("spark.driver.memory", memory)
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.ui.enabled", "false")
             .config("spark.python.worker.faulthandler.enabled", "true")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addPyFile(str(HERE / "extractor.py"))
    spark.sparkContext.addPyFile(str(HERE / "common.py"))
    return spark


def file_uri(path):
    posix = Path(path).resolve().as_posix()
    return ("file://" if posix.startswith("/") else "file:///") + posix


def write_tsv(df, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = df.collect()
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(df.columns)
        for row in rows:
            writer.writerow(["" if v is None else v for v in row])
    return len(rows)


PAREN_RE = re.compile(r"\s*\([^)]*\)")
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
CLUB_WORDS = {"fc", "afc", "cf", "sc", "ac", "as", "ssc", "fk", "sk", "club", "football", "de",
              "the", "calcio", "cd", "ud", "rc", "sv", "vfl", "vfb", "tsg", "fsv", "us", "bk", "if"}


def norm_name(text):
    if not text:
        return ""
    text = PAREN_RE.sub("", text)
    text = text.lower().replace("ø", "o").replace("đ", "d").replace("ł", "l").replace("ß", "ss")
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = text.replace(".", "").replace("'", "").replace("’", "")
    return NON_ALNUM_RE.sub(" ", text).strip()


def norm_club(text):
    tokens = [t for t in norm_name(text).split() if t not in CLUB_WORDS]
    while tokens and tokens[0].isdigit():
        tokens.pop(0)
    return " ".join(tokens)
