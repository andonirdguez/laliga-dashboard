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
import re
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
    for s in C.SEASONS:
        code = C.season_code(s)
        raw = _get(C.FOOTBALL_DATA_URL.format(code=code))
        df = pd.read_csv(io.BytesIO(raw), encoding="latin-1")
        df = df.dropna(how="all")
        df = df[df["HomeTeam"].notna()]
        C.save_bronze(df, "footballdata", f"partidos_{code}")
        log.info("footballdata | %s | partidos: %s filas, %s columnas", C.season_label(s), len(df), df.shape[1])


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
    C.save_bronze(df, "clubelo", "elo_hoy", historico=True)
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

    for s in C.SEASONS:
        code = C.season_code(s)
        # Temporadas cerradas: la caché de soccerdata vale (no cambian). La actual se fuerza a refrescar.
        us = sd.Understat(leagues=C.LEAGUE_SD, seasons=code, no_cache=no_cache or s == C.SEASON_START)
        partidos = _flatten(us.read_team_match_stats())
        C.save_bronze(partidos, "understat", f"partidos_{code}")
        log.info("understat | %s | partidos: %s filas", C.season_label(s), len(partidos))

        jugadores = _flatten(us.read_player_season_stats())
        C.save_bronze(jugadores, "understat", f"jugadores_{code}")
        log.info("understat | %s | jugadores: %s filas", C.season_label(s), len(jugadores))

        # Tiros y stats de jugador por partido: una página por partido -> incremental.
        # Solo se descargan los partidos jugados que aún no tenemos guardados en bronze.
        gid = next((c for c in ("game_id", "game") if c in partidos.columns), None)
        gcol_goles = next((c for c in ("home_goals",) if c in partidos.columns), None)
        jugados = partidos[partidos[gcol_goles].notna()] if gcol_goles else partidos
        ids_jugados = set(pd.to_numeric(jugados[gid], errors="coerce").dropna().astype(int))
        for nombre, metodo in (("tiros", us.read_shot_events), ("jugadores_partido", us.read_player_match_stats)):
            previo = C.load_bronze("understat", f"{nombre}_{code}")
            hechos = set()
            if previo is not None and "game_id" in previo.columns:
                hechos = set(pd.to_numeric(previo["game_id"], errors="coerce").dropna().astype(int))
            pendientes = sorted(ids_jugados - hechos)
            if not pendientes:
                log.info("understat | %s | %s: al día (%s partidos)", C.season_label(s), nombre, len(hechos))
                continue
            nuevos = _flatten(metodo(match_id=pendientes))
            total = pd.concat([previo, nuevos], ignore_index=True) if previo is not None else nuevos
            C.save_bronze(total, "understat", f"{nombre}_{code}")
            log.info("understat | %s | %s: +%s partidos (%s filas nuevas, %s en total) | columnas: %s",
                     C.season_label(s), nombre, len(pendientes), len(nuevos), len(total), list(nuevos.columns))


# --------------------------------------------------------------------------
# Wikidata — estadio, capacidad, coordenadas, fundación (CC0). Se guarda y solo se buscan clubes nuevos.
# --------------------------------------------------------------------------
WD_API = "https://www.wikidata.org/w/api.php"
WD_SPARQL = "https://query.wikidata.org/sparql"


def _wd_get(url: str, params: dict, intentos: int = 4) -> dict:
    """GET a Wikidata respetando su límite: si devuelve 429 espera lo que indique Retry-After."""
    for i in range(1, intentos + 1):
        r = requests.get(url, params=params, timeout=60,
                         headers={**C.WIKIDATA_HEADERS, "Accept": "application/json"})
        if r.status_code == 429 and i < intentos:
            espera = int(r.headers.get("Retry-After", "10")) + 2
            log.warning("wikidata | límite de peticiones, espero %ss", espera)
            time.sleep(espera)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("inalcanzable")


def _wd_buscar_club(nombre: str) -> str | None:
    """Busca el QID del club por nombre: primer resultado cuya descripción sea de club de fútbol.
    Prueba variantes porque "Valencia" a secas devuelve antes la ciudad que el club."""
    for texto in (nombre, f"{nombre} CF", f"{nombre} FC", f"Real {nombre}", f"Club {nombre}"):
        for lang in ("es", "en"):
            time.sleep(1.0)
            res = _wd_get(WD_API, {"action": "wbsearchentities", "search": texto, "language": lang,
                                   "type": "item", "limit": 10, "format": "json"})
            for it in res.get("search", []):
                desc = (it.get("description") or "").lower()
                label = (it.get("label") or "").strip()
                es_club = ("club" in desc or "team" in desc or "equipo" in desc) and ("fútbol" in desc or "football" in desc)
                # Fuera filiales, reservas, cantera y femeninos: queremos el primer equipo
                descartar = any(w in desc for w in ("femenino", "women", "filial", "reserv", "dependiente", "juvenil",
                                                    "youth", "academy", "cantera", "b team", " b ")) \
                    or label.endswith(" B") or " B " in f" {label} "
                if es_club and not descartar:
                    return it["id"]
    return None


def ingest_wikidata(no_cache: bool) -> None:
    # Equipos de todas las temporadas, según los partidos de Understat ya descargados
    nombres = {}
    for s in C.SEASONS:
        p = C.load_bronze("understat", f"partidos_{C.season_code(s)}")
        if p is not None:
            for n in pd.concat([p["home_team"], p["away_team"]]).dropna().unique():
                nombres.setdefault(C.team_key(n), n)
    if not nombres:
        raise RuntimeError("Ejecuta antes la ingesta de Understat (de ahí salen los equipos)")

    previo = C.load_bronze("wikidata", "clubes")
    hechos = set(previo["equipo_id"]) if previo is not None and not no_cache else set()
    pendientes = {k: v for k, v in nombres.items() if k not in hechos}
    if not pendientes:
        log.info("wikidata | al día (%s clubes)", len(hechos))
        return

    qids = {}
    for k, n in pendientes.items():
        q = C.WIKIDATA_QID_MANUAL.get(k) or _wd_buscar_club(n)
        if q:
            qids[q] = k
        else:
            log.warning("wikidata | no encuentro el club '%s' (añádelo a WIKIDATA_QID_MANUAL)", n)
    if not qids:
        return

    valores = " ".join(f"wd:{q}" for q in qids)
    consulta = f"""
    SELECT ?club ?clubLabel ?fundacion ?estadioLabel ?capacidad ?coords ?ciudadLabel ?sedeLabel ?fin ?rango WHERE {{
      VALUES ?club {{ {valores} }}
      OPTIONAL {{ ?club wdt:P571 ?fundacion. }}
      OPTIONAL {{ ?club wdt:P159 ?sede. }}
      OPTIONAL {{ ?club p:P115 ?st. ?st ps:P115 ?estadio; wikibase:rank ?rango.
                 OPTIONAL {{ ?st pq:P582 ?fin. }}
                 OPTIONAL {{ ?estadio wdt:P1083 ?capacidad. }}
                 OPTIONAL {{ ?estadio wdt:P625 ?coords. }}
                 OPTIONAL {{ ?estadio wdt:P131 ?ciudad. }} }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "es,en". }}
    }}"""
    res = _wd_get(WD_SPARQL, {"query": consulta, "format": "json"})
    filas = []
    for b in res["results"]["bindings"]:
        v = {k: x["value"] for k, x in b.items()}
        q = v["club"].rsplit("/", 1)[-1]
        lon = lat = None
        if "coords" in v and v["coords"].startswith("Point("):
            lon, lat = (float(x) for x in v["coords"][6:-1].split())
        filas.append({"equipo_id": qids.get(q), "wikidata_id": q, "club_wikidata": v.get("clubLabel"),
                      "anio_fundacion": int(v["fundacion"][:4]) if v.get("fundacion", "")[:4].isdigit() else None,
                      "estadio": v.get("estadioLabel"), "capacidad": float(v["capacidad"]) if "capacidad" in v else None,
                      "latitud": lat, "longitud": lon,
                      "_historico": int("fin" in v),
                      "_preferente": int(v.get("rango", "").endswith("PreferredRank")),
                      # Ciudad = sede del club (P159); si no hay, el municipio del estadio. Sin etiqueta -> vacío.
                      "ciudad": next((x for x in (v.get("sedeLabel"), v.get("ciudadLabel"))
                                      if x and not re.fullmatch(r"Q\d+", x)), None)})
    nuevos = pd.DataFrame(filas)
    # Varias fechas de fundación (refundaciones, fusiones): nos quedamos con la más antigua
    nuevos["anio_fundacion"] = nuevos.groupby("equipo_id")["anio_fundacion"].transform("min")
    # Wikidata guarda todos los estadios del club (también los derribados, p. ej. Sarriá). Elegimos el actual:
    # sin fecha de fin, luego el marcado como preferente y, a igualdad, el de mayor capacidad.
    nuevos = (nuevos.sort_values(["_historico", "_preferente", "capacidad"], ascending=[True, False, False],
                                 na_position="last")
              .drop_duplicates("equipo_id").drop(columns=["_historico", "_preferente"]))
    total = pd.concat([previo, nuevos], ignore_index=True) if previo is not None and not no_cache else nuevos
    total = total.drop_duplicates("equipo_id", keep="last")
    C.save_bronze(total, "wikidata", "clubes")
    log.info("wikidata | +%s clubes (%s en total): %s", len(nuevos), len(total),
             dict(zip(nuevos["equipo_id"], nuevos["estadio"])))


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
    "wikidata": ingest_wikidata,
    "fbref": ingest_fbref,
}
DEFAULT_SOURCES = ["footballdata", "clubelo", "understat", "transfermarkt", "wikidata"]


def main() -> int:
    ap = argparse.ArgumentParser()
    # FBref queda fuera por defecto: exige resolver un captcha de Cloudflare a mano y ya no publica
    # las métricas avanzadas. Sigue disponible con --source fbref para pruebas puntuales.
    ap.add_argument("--source", nargs="+", choices=list(SOURCES), default=DEFAULT_SOURCES)
    ap.add_argument("--no-cache", action="store_true", help="fuerza re-descarga en soccerdata")
    args = ap.parse_args()

    log.info("=== INGESTA | temporadas %s | fuentes: %s ===", [C.season_label(s) for s in C.SEASONS], args.source)
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
