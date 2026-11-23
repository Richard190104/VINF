@echo off
rem Spusti src\lucene_search.py v Dockeri (coady/pylucene), napr.: lucene search "brazil goalkeeper"
pushd "%~dp0"
docker run --rm -v "%cd%:/vinf" -w /vinf coady/pylucene python src/lucene_search.py %*
popd
