"""Capa BRONZE: descarga cruda de cada fuente pública a data/bronze/<fuente>/*.parquet.

Uso:
    python pipeline/ingest.py                      # todas las fuentes
    python pipeline/ingest.py --source footballdata clubelo
    python pipeline/ingest.py --source fbref --no-cache

Cada fuente es independiente: si una falla, se registra en el log y el resto sigue.
El proceso sale con código 1 solo si fallan TODAS las fuentes pedidas.
"""
from __future__ import annotations

import argparse
import io
import sys
import time
from datetime import date, timedelta

import pandas as pd
import requests

import config as C

log = C.get_logger("ingest")


def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    """soccerdata devuelve índices y columnas multinivel -> tabla plana con nombres snake_case."""
    df = df.reset_index()
    if isinstance(df.columns, pd.MultiIndex):
        cols = []
        for tup in df.columns:
            parts = [str(p) for p in tup if str(p) and not str(p).startswith("Unnamed")]
            cols.append("_".join(parts))
        df.columns = cols
    # "Cmp%" y "Cmp" chocarían al normalizar -> el % pasa a "_pct" antes
    df.columns = [C.norm_key(str(c).replace("%", "_pct").replace("+", "_")).replace(" ", "_") or f"col_{i}"
                  for i, c in enumerate(df.columns)]
    # columnas duplicadas tras aplanar -> sufijo
    seen: dict[str, int] = {}
    new = []
    for c in df.columns:
        seen[c] = seen.get(c, 0) + 1
        new.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
    df.columns = new
    return df


def _get(url: str, intentos: int = 3, espera: int = 10) -> bytes:
    """GET con reintentos: las webs pequeñas (ClubElo) devuelven 5xx a ratos."""
    for i in range(1, intentos + 1):
        try:
            r = requests.get(url, headers=C.HTTP_HEADERS, timeout=60)
            r.raise_for_status()
            return r.content
        except requests.RequestException as e:
            if i == intentos:
                raise
            log.warning("GET %s falló (%s). Reintento %s/%s en %ss", url, e, i, intentos - 1, espera)
            time.sleep(espera * i)
    raise RuntimeError("inalcanzable")


# --------------------------------------------------------------------------
# 1. football-data.co.uk — resultados, tiros, córners, tarjetas, cuotas (riesgo 0)
# --------------------------------------------------------------------------
def ingest_footballdata(no_cache: bool) -> None:
    raw = _get(C.FOOTBALL_DATA_URL)
    df = pd.read_csv(io.BytesIO(raw), encoding="latin-1")
    df = df.dropna(how="all")
    df = df[df["HomeTeam"].notna()]
    C.save_bronze(df, "footballdata", "partidos")
    log.info("footballdata | partidos: %s filas, %s columnas", len(df), df.shape[1])


# --------------------------------------------------------------------------
# 2. ClubElo — Elo diario de todos los clubes, filtramos España (riesgo 0)
# --------------------------------------------------------------------------
def ingest_clubelo(no_cache: bool) -> None:
    # Si hoy no responde, probamos los días anteriores (el Elo cambia poco de un día a otro)
    df, hoy = None, None
    for dias_atras in range(0, 4):
        fecha = (date.today() - timedelta(days=dias_atras)).isoformat()
        try:
            raw = _get(C.CLUBELO_URL.format(fecha=fecha), intentos=2)
            df, hoy = pd.read_csv(io.BytesIO(raw)), fecha
            break
        except requests.RequestException as e:
            log.warning("clubelo | %s no disponible: %s", fecha, e)
    if df is None:
        raise RuntimeError("ClubElo no responde para ninguno de los últimos 4 días")
    if hoy != date.today().isoformat():
        log.warning("clubelo | usando el Elo de %s (hoy no disponible)", hoy)
    df = df[(df["Country"] == "ESP") & (df["Level"] == 1)].copy()
    df["fecha_snapshot"] = hoy
    C.save_bronze(df, "clubelo", "elo_hoy")
    log.info("clubelo | elo_hoy: %s equipos", len(df))

    # Histórico acumulado para poder pintar evolución (se reconstruye con los snapshots diarios)
    prev = C.load_bronze("clubelo", "elo_historico")
    hist = pd.concat([prev, df]) if prev is not None else df
    hist = hist.drop_duplicates(subset=["Club", "fecha_snapshot"], keep="last")
    C.save_bronze(hist, "clubelo", "elo_historico")
    log.info("clubelo | elo_historico: %s filas", len(hist))


# --------------------------------------------------------------------------
# 3. Understat — xG por partido y por jugador (soccerdata, riesgo medio)
# --------------------------------------------------------------------------
def ingest_understat(no_cache: bool) -> None:
    import soccerdata as sd

    us = sd.Understat(leagues=C.LEAGUE_SD, seasons=C.SEASON_CODE, no_cache=no_cache)
    partidos = _flatten(us.read_team_match_stats())
    C.save_bronze(partidos, "understat", "partidos")
    log.info("understat | partidos: %s filas | columnas: %s", len(partidos), list(partidos.columns))

    jugadores = _flatten(us.read_player_season_stats())
    C.save_bronze(jugadores, "understat", "jugadores")
    log.info("understat | jugadores: %s filas | columnas: %s", len(jugadores), list(jugadores.columns))


# --------------------------------------------------------------------------
# 4. Transfermarkt (dataset Kaggle davidcariboo/player-scores) — valor de mercado
# --------------------------------------------------------------------------
def ingest_transfermarkt(no_cache: bool) -> None:
    from kaggle.api.kaggle_api_extended import KaggleApi  # requiere kaggle.json o KAGGLE_USERNAME/KAGGLE_KEY

    api = KaggleApi()
    api.authenticate()
    tmp = C.BRONZE / "transfermarkt" / "_raw"
    tmp.mkdir(parents=True, exist_ok=True)
    for f in C.KAGGLE_FILES:
        api.dataset_download_file(C.KAGGLE_DATASET, f, path=str(tmp), force=True, quiet=True)

    def _read(name: str) -> pd.DataFrame:
        # Kaggle a veces entrega el fichero comprimido como <name>.zip
        z = tmp / f"{name}.zip"
        return pd.read_csv(z if z.exists() else tmp / name)

    # Guardamos TODOS los jugadores en activo (cualquier liga), no solo La Liga: así cruzan los fichajes
    # de verano y los jugadores de los recién ascendidos aunque el dataset de Kaggle vaya retrasado.
    players = _read("players.csv")
    ult = int(players.loc[players["current_club_domestic_competition_id"] == C.TM_COMPETITION, "last_season"].max())
    players = players[players["last_season"] >= ult - 1]
    C.save_bronze(players, "transfermarkt", "jugadores")
    log.info("transfermarkt | jugadores en activo (last_season>=%s): %s | de La Liga: %s",
             ult - 1, len(players), (players["current_club_domestic_competition_id"] == C.TM_COMPETITION).sum())
    if ult < C.SEASON_START:
        log.warning("transfermarkt | el dataset de Kaggle aún va por la temporada %s (la actual es %s): "
                    "valores y plantillas pueden estar desfasados", ult, C.SEASON_START)

    clubs = _read("clubs.csv")
    clubs = clubs[(clubs["domestic_competition_id"] == C.TM_COMPETITION) & (clubs["last_season"] == ult)]
    C.save_bronze(clubs, "transfermarkt", "clubes")
    log.info("transfermarkt | clubes La Liga: %s", len(clubs))


# --------------------------------------------------------------------------
# 5. FBref — stats avanzadas de jugador y equipo (soccerdata, lento: ~10 req/min)
# --------------------------------------------------------------------------
def ingest_fbref(no_cache: bool) -> None:
    import soccerdata as sd

    fb = sd.FBref(leagues=C.LEAGUE_SD, seasons=C.SEASON_CODE, no_cache=no_cache)
    errores = 0

    for st in C.FBREF_PLAYER_STATS:
        try:
            df = _flatten(fb.read_player_season_stats(stat_type=st))
            C.save_bronze(df, "fbref", f"jugadores_{st}")
            log.info("fbref | jugadores_%s: %s filas | columnas: %s", st, len(df), list(df.columns))
        except Exception as e:  # noqa: BLE001
            errores += 1
            log.warning("fbref | jugadores_%s FALLÓ: %s", st, e)

    for st in C.FBREF_TEAM_STATS:
        try:
            df = _flatten(fb.read_team_season_stats(stat_type=st))
            C.save_bronze(df, "fbref", f"equipos_{st}")
            log.info("fbref | equipos_%s: %s filas | columnas: %s", st, len(df), list(df.columns))
        except Exception as e:  # noqa: BLE001
            errores += 1
            log.warning("fbref | equipos_%s FALLÓ: %s", st, e)

    total = len(C.FBREF_PLAYER_STATS) + len(C.FBREF_TEAM_STATS)
    if errores == total:
        raise RuntimeError("Todas las tablas de FBref han fallado (¿bloqueo 403 / Cloudflare?)")


SOURCES = {
    "footballdata": ingest_footballdata,
    "clubelo": ingest_clubelo,
    "understat": ingest_understat,
    "transfermarkt": ingest_transfermarkt,
    "fbref": ingest_fbref,
}
DEFAULT_SOURCES = ["footballdata", "clubelo", "understat", "transfermarkt"]


def main() -> int:
    ap = argparse.ArgumentParser()
    # FBref queda fuera por defecto: exige resolver un captcha de Cloudflare a mano y ya no publica
    # las métricas avanzadas. Sigue disponible con --source fbref para pruebas puntuales.
    ap.add_argument("--source", nargs="+", choices=list(SOURCES), default=DEFAULT_SOURCES)
    ap.add_argument("--no-cache", action="store_true", help="fuerza re-descarga en soccerdata")
    args = ap.parse_args()

    log.info("=== INGESTA | temporada %s (%s) | fuentes: %s ===", C.SEASON_LABEL, C.SEASON_CODE, args.source)
    ok, ko = [], []
    for name in args.source:
        t0 = time.time()
        try:
            SOURCES[name](args.no_cache)
            ok.append(name)
            log.info("%s OK en %.1fs", name, time.time() - t0)
        except Exception as e:  # noqa: BLE001
            ko.append(name)
            log.exception("%s FALLÓ en %.1fs: %s", name, time.time() - t0, e)

    log.info("=== FIN INGESTA | OK: %s | FALLIDAS: %s ===", ok, ko)
    return 1 if not ok else 0


if __name__ == "__main__":
    sys.exit(main())
