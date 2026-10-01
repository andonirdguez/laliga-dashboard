"""Capas SILVER y GOLD.

silver: cada fuente limpia, tipada y con claves normalizadas (equipo_id, jugador_key).
gold:   modelo estrella en CSV, listo para Power BI (todas las temporadas de config.SEASONS):
        dim_temporada, dim_equipo, dim_jugador, dim_fecha,
        fact_partido, fact_equipo_partido, fact_equipo_temporada, fact_jugador_temporada, meta

Es tolerante: si una fuente no existe en bronze, se omite lo que depende de ella y se avisa.
Uso: python pipeline/transform.py
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd

import config as C

log = C.get_logger("transform")


# --------------------------------------------------------------------------
# Utilidades
# --------------------------------------------------------------------------
def find_col(df: pd.DataFrame, *cands: str) -> str | None:
    """Primera columna que coincide exacta; si no, la primera que TERMINA en el candidato."""
    cols = list(df.columns)
    for c in cands:
        if c in cols:
            return c
    for c in cands:
        for col in cols:
            if col.endswith("_" + c):
                return col
    return None


def pick(df: pd.DataFrame, spec: dict[str, tuple[str, ...]], tabla: str) -> pd.DataFrame:
    """Selecciona y renombra columnas según {nombre_destino: (candidatos...)}. Loguea las que falten."""
    out, faltan = {}, []
    for dest, cands in spec.items():
        col = find_col(df, *cands)
        if col is None:
            faltan.append(dest)
        else:
            out[dest] = df[col]
    if faltan:
        log.warning("%s | columnas NO encontradas: %s | disponibles: %s", tabla, faltan, list(df.columns))
    return pd.DataFrame(out)


def to_num(s: pd.Series | None) -> pd.Series | float:
    if s is None:  # columna que no existe en esa temporada/fuente
        return np.nan
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False), errors="coerce")


def pos_grupo(pos: str) -> str:
    """FBref 'FW,MF' / Understat 'F M S' -> GK / DF / MF / FW (primera posición manda)."""
    if pd.isna(pos) or not str(pos).strip():
        return "ND"
    p = str(pos).strip().upper()[:2]
    if p.startswith("GK"):
        return "GK"
    if p.startswith("D"):
        return "DF"
    if p.startswith("M"):
        return "MF"
    if p.startswith(("F", "S", "A")):
        return "FW"
    return "ND"


# --------------------------------------------------------------------------
# SILVER
# --------------------------------------------------------------------------
def por_temporada(fn, nombre: str) -> pd.DataFrame | None:
    """Ejecuta fn(season_start) para cada temporada configurada y concatena con columna 'temporada'."""
    partes = []
    for s in C.SEASONS:
        df = fn(s)
        if df is None:
            log.warning("%s | %s no disponible en bronze", nombre, C.season_label(s))
            continue
        df.insert(0, "temporada", C.season_label(s))
        partes.append(df)
    if not partes:
        return None
    out = pd.concat(partes, ignore_index=True)
    C.save_silver(out, nombre)
    return out


def silver_partidos_fd(season: int) -> pd.DataFrame | None:
    df = C.load_bronze("footballdata", f"partidos_{C.season_code(season)}")
    if df is None:
        return None
    fecha = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    odds = [("AvgH", "AvgD", "AvgA"), ("B365H", "B365D", "B365A"), ("PSH", "PSD", "PSA")]
    oh = od = oa = None
    for h, d_, a in odds:
        if {h, d_, a}.issubset(df.columns):
            oh, od, oa = (to_num(df[x]) for x in (h, d_, a))
            log.info("football-data | %s | cuotas usadas: %s/%s/%s", C.season_label(season), h, d_, a)
            break
    out = pd.DataFrame({
        "fecha": fecha.dt.date,
        "local": df["HomeTeam"], "visitante": df["AwayTeam"],
        "goles_local": to_num(df["FTHG"]), "goles_visitante": to_num(df["FTAG"]),
        "resultado": df["FTR"],
        "tiros_local": to_num(df.get("HS")), "tiros_visitante": to_num(df.get("AS")),
        "tiros_puerta_local": to_num(df.get("HST")), "tiros_puerta_visitante": to_num(df.get("AST")),
        "corners_local": to_num(df.get("HC")), "corners_visitante": to_num(df.get("AC")),
        "faltas_local": to_num(df.get("HF")), "faltas_visitante": to_num(df.get("AF")),
        "amarillas_local": to_num(df.get("HY")), "amarillas_visitante": to_num(df.get("AY")),
        "rojas_local": to_num(df.get("HR")), "rojas_visitante": to_num(df.get("AR")),
        "cuota_local": oh, "cuota_empate": od, "cuota_visitante": oa,
        # Más/menos de 2,5 goles y hándicap asiático (medias de mercado; en temporadas antiguas pueden faltar)
        "cuota_mas25": to_num(df.get("Avg>2.5", df.get("B365>2.5"))),
        "cuota_menos25": to_num(df.get("Avg<2.5", df.get("B365<2.5"))),
        "handicap_asiatico_local": to_num(df.get("AHh")),
        "cuota_ah_local": to_num(df.get("AvgAHH", df.get("B365AHH"))),
        "cuota_ah_visitante": to_num(df.get("AvgAHA", df.get("B365AHA"))),
    })
    out["equipo_local_id"] = out["local"].map(C.team_key)
    out["equipo_visitante_id"] = out["visitante"].map(C.team_key)
    return out


def silver_partidos_us(season: int) -> pd.DataFrame | None:
    df = C.load_bronze("understat", f"partidos_{C.season_code(season)}")
    if df is None:
        return None
    out = pick(df, {
        "fecha": ("date",), "local": ("home_team",), "visitante": ("away_team",), "us_game_id": ("game_id",),
        "xg_local": ("home_xg",), "xg_visitante": ("away_xg",),
        "npxg_local": ("home_np_xg",), "npxg_visitante": ("away_np_xg",),
        "ppda_local": ("home_ppda",), "ppda_visitante": ("away_ppda",),
        "deep_local": ("home_deep_completions",), "deep_visitante": ("away_deep_completions",),
    }, "understat.partidos")
    out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce").dt.date
    for c in out.columns.difference(["fecha", "local", "visitante"]):
        out[c] = to_num(out[c])
    out["equipo_local_id"] = out["local"].map(C.team_key)
    out["equipo_visitante_id"] = out["visitante"].map(C.team_key)
    return out


# Métricas de jugador por tabla FBref: destino -> (tabla, candidatos de columna aplanada)
FBREF_PLAYER_SPEC: dict[str, tuple[str, tuple[str, ...]]] = {
    "minutos": ("standard", ("playing_time_min",)),
    "partidos": ("standard", ("playing_time_mp",)),
    "titularidades": ("standard", ("playing_time_starts",)),
    "goles": ("standard", ("performance_gls",)),
    "asistencias": ("standard", ("performance_ast",)),
    "goles_sin_penalti": ("standard", ("performance_g_pk",)),
    "amarillas": ("standard", ("performance_crdy",)),
    "rojas": ("standard", ("performance_crdr",)),
    "xg": ("standard", ("expected_xg",)),
    "npxg": ("standard", ("expected_npxg",)),
    "xag": ("standard", ("expected_xag",)),
    "conducciones_progresivas": ("standard", ("progression_prgc",)),
    "pases_progresivos": ("standard", ("progression_prgp",)),
    "recepciones_progresivas": ("standard", ("progression_prgr",)),
    "tiros": ("shooting", ("standard_sh",)),
    "tiros_puerta": ("shooting", ("standard_sot",)),
    "pases_completados": ("passing", ("total_cmp",)),
    "pases_intentados": ("passing", ("total_att",)),
    "pases_clave": ("passing", ("kp",)),
    "pases_ultimo_tercio": ("passing", ("1_3",)),
    "pases_area": ("passing", ("ppa",)),
    "sca": ("goal_shot_creation", ("sca_sca",)),
    "gca": ("goal_shot_creation", ("gca_gca",)),
    "entradas": ("defense", ("tackles_tkl",)),
    "entradas_ganadas": ("defense", ("tackles_tklw",)),
    "intercepciones": ("defense", ("int",)),
    "bloqueos": ("defense", ("blocks_blocks",)),
    "despejes": ("defense", ("clr",)),
    "toques": ("possession", ("touches_touches",)),
    "regates_intentados": ("possession", ("take_ons_att",)),
    "regates_completados": ("possession", ("take_ons_succ",)),
    "recuperaciones": ("misc", ("performance_recov",)),
    "duelos_aereos_ganados": ("misc", ("aerial_duels_won",)),
    "duelos_aereos_perdidos": ("misc", ("aerial_duels_lost",)),
    "porterias_cero": ("keeper", ("performance_cs",)),
    "paradas": ("keeper", ("performance_saves",)),
    "tiros_puerta_recibidos": ("keeper", ("performance_sota",)),
    "goles_encajados": ("keeper", ("performance_ga",)),
}


def silver_jugadores_fbref() -> pd.DataFrame | None:
    if not C.USE_FBREF:
        return None
    base = C.load_bronze("fbref", "jugadores_standard")
    if base is None:
        log.warning("FBref jugadores no disponible en bronze")
        return None
    keys = pick(base, {"jugador": ("player",), "equipo": ("team",), "nacionalidad": ("nation",),
                       "posicion": ("pos",), "nacimiento": ("born",)}, "fbref.standard")
    keys = keys.reset_index(drop=True)
    tablas: dict[str, pd.DataFrame] = {}
    out = keys.copy()
    for dest, (tabla, cands) in FBREF_PLAYER_SPEC.items():
        if tabla not in tablas:
            t = C.load_bronze("fbref", f"jugadores_{tabla}")
            tablas[tabla] = t if t is not None else pd.DataFrame()
        t = tablas[tabla]
        if t.empty:
            out[dest] = np.nan
            continue
        col = find_col(t, *cands)
        if col is None:
            log.warning("fbref | métrica '%s' no encontrada en jugadores_%s", dest, tabla)
            out[dest] = np.nan
            continue
        pj, tm = find_col(t, "player"), find_col(t, "team")
        m = t[[pj, tm, col]].rename(columns={pj: "jugador", tm: "equipo", col: dest})
        m = m.drop_duplicates(["jugador", "equipo"])
        out = out.merge(m, on=["jugador", "equipo"], how="left")
    for tabla in sorted({t for t, _ in FBREF_PLAYER_SPEC.values()}):
        if tablas[tabla].empty:
            log.warning("fbref | tabla jugadores_%s no está en bronze: sus métricas quedan vacías", tabla)
    for c in FBREF_PLAYER_SPEC:
        out[c] = to_num(out[c])
    out["nacimiento"] = to_num(out["nacimiento"]).astype("Int64")
    out["nacionalidad"] = out["nacionalidad"].astype(str).str.split().str[-1]  # "es ESP" -> "ESP"
    out["fuente"] = "fbref"
    C.save_silver(out, "jugadores_fbref")
    return out


def silver_jugadores_understat(season: int) -> pd.DataFrame | None:
    df = C.load_bronze("understat", f"jugadores_{C.season_code(season)}")
    if df is None:
        return None
    out = pick(df, {
        "jugador": ("player",), "equipo": ("team",), "posicion": ("position",), "us_player_id": ("player_id",),
        "partidos": ("matches",), "minutos": ("minutes",), "goles": ("goals",), "asistencias": ("assists",),
        "xg": ("xg",), "npxg": ("np_xg",), "xag": ("xa",), "tiros": ("shots",), "pases_clave": ("key_passes",),
        "amarillas": ("yellow_cards",), "rojas": ("red_cards",),
        "xg_chain": ("xg_chain",), "xg_buildup": ("xg_buildup",),
    }, "understat.jugadores")
    for c in out.columns.difference(["jugador", "equipo", "posicion"]):
        out[c] = to_num(out[c])
    out["fuente"] = "understat"
    return out


SITUACION = {"OpenPlay": "Jugada", "FromCorner": "Córner", "SetPiece": "Balón parado",
             "DirectFreekick": "Falta directa", "Penalty": "Penalti"}
RESULTADO_TIRO = {"Goal": "Gol", "SavedShot": "Parado", "MissedShots": "Fuera", "MissedShot": "Fuera",
                  "BlockedShot": "Bloqueado",
                  "ShotOnPost": "Poste", "OwnGoal": "Gol en propia"}
PARTE_CUERPO = {"RightFoot": "Pie derecho", "LeftFoot": "Pie izquierdo", "Head": "Cabeza",
                "OtherBodyPart": "Otra"}


def silver_tiros(season: int) -> pd.DataFrame | None:
    df = C.load_bronze("understat", f"tiros_{C.season_code(season)}")
    if df is None:
        return None
    out = pick(df, {
        "us_game_id": ("game_id",), "us_shot_id": ("shot_id",), "equipo": ("team",), "jugador": ("player",),
        "us_player_id": ("player_id",), "us_asistente_id": ("assist_player_id",), "asistente": ("assist_player",),
        "minuto": ("minute",), "xg": ("xg",), "x": ("location_x",), "y": ("location_y",),
        "situacion": ("situation",), "parte_cuerpo": ("body_part",), "resultado": ("result",),
    }, "understat.tiros")
    for c in ("us_game_id", "us_shot_id", "us_player_id", "us_asistente_id", "minuto", "xg", "x", "y"):
        if c in out.columns:
            out[c] = to_num(out[c])
    return out


def silver_jugadores_partido(season: int) -> pd.DataFrame | None:
    df = C.load_bronze("understat", f"jugadores_partido_{C.season_code(season)}")
    if df is None:
        return None
    out = pick(df, {
        "us_game_id": ("game_id",), "equipo": ("team",), "jugador": ("player",), "us_player_id": ("player_id",),
        "posicion": ("position",), "minutos": ("minutes",), "goles": ("goals",), "goles_propia": ("own_goals",),
        "tiros": ("shots",), "xg": ("xg",), "xg_chain": ("xg_chain",), "xg_buildup": ("xg_buildup",),
        "asistencias": ("assists",), "xag": ("xa",), "pases_clave": ("key_passes",),
        "amarillas": ("yellow_cards",), "rojas": ("red_cards",),
    }, "understat.jugadores_partido")
    for c in out.columns.difference(["equipo", "jugador", "posicion"]):
        out[c] = to_num(out[c])
    return out


def silver_wikidata() -> pd.DataFrame | None:
    df = C.load_bronze("wikidata", "clubes")
    if df is None:
        log.warning("Wikidata (estadios) no disponible en bronze")
    return df


def silver_transfermarkt() -> pd.DataFrame | None:
    df = C.load_bronze("transfermarkt", "jugadores")
    if df is None:
        log.warning("Transfermarkt no disponible en bronze")
        return None
    out = pd.DataFrame({
        "tm_player_id": df["player_id"],
        "jugador_tm": df["name"],
        "nacimiento": pd.to_datetime(df.get("date_of_birth"), errors="coerce").dt.year.astype("Int64"),
        "equipo_tm": df.get("current_club_name"),
        "valor_mercado_eur": to_num(df.get("market_value_in_eur")),
        "valor_maximo_eur": to_num(df.get("highest_market_value_in_eur")),
        "fin_contrato": pd.to_datetime(df.get("contract_expiration_date"), errors="coerce").dt.date,
        "pie": df.get("foot"),
        "altura_cm": to_num(df.get("height_in_cm")),
        "imagen_url": df.get("image_url"),
        "nacionalidad_tm": df.get("country_of_citizenship"),
        "competicion_tm": df.get("current_club_domestic_competition_id"),
    })
    out["equipo_id"] = out["equipo_tm"].map(C.team_key)
    C.save_silver(out, "jugadores_transfermarkt")
    return out


def silver_elo() -> pd.DataFrame | None:
    df = C.load_bronze("clubelo", "elo_historico")
    if df is None:
        log.warning("ClubElo no disponible en bronze")
        return None
    out = pd.DataFrame({
        "fecha": pd.to_datetime(df["fecha_snapshot"]).dt.date,
        "equipo_elo": df["Club"], "elo": to_num(df["Elo"]), "rank_elo_mundial": to_num(df["Rank"]),
    })
    out["equipo_id"] = out["equipo_elo"].map(C.team_key)
    C.save_silver(out, "elo")
    return out


# --------------------------------------------------------------------------
# GOLD
# --------------------------------------------------------------------------
def build_fact_partido(fd: pd.DataFrame | None, us: pd.DataFrame | None) -> pd.DataFrame | None:
    if fd is None and us is None:
        return None
    if fd is None:
        p = us.copy()
    else:
        p = fd.copy()
        if us is not None:
            # Cada emparejamiento local-visitante es único en una temporada: no dependemos de la fecha
            # (los aplazados pueden tener fecha distinta en cada fuente).
            cols_us = [c for c in us.columns if c not in ("local", "visitante", "fecha")]
            claves = ["temporada", "equipo_local_id", "equipo_visitante_id"]
            p = p.merge(us[cols_us].drop_duplicates(claves), on=claves, how="left")
            sin_xg = p["xg_local"].isna().sum()
            if sin_xg:
                log.warning("fact_partido | %s partidos sin xG de Understat (fecha o equipo no cruzan)", sin_xg)
    p = p.sort_values("fecha").reset_index(drop=True)
    p["fecha"] = pd.to_datetime(p["fecha"])
    p["partido_id"] = (p["fecha"].dt.strftime("%Y%m%d") + "_" + p["equipo_local_id"].map(C.slug)
                       + "_" + p["equipo_visitante_id"].map(C.slug))
    p["fecha_id"] = p["fecha"].dt.strftime("%Y%m%d").astype(int)

    if {"cuota_local", "cuota_empate", "cuota_visitante"}.issubset(p.columns):
        inv = 1 / p[["cuota_local", "cuota_empate", "cuota_visitante"]]
        overround = inv.sum(axis=1)
        p["prob_local"] = inv["cuota_local"] / overround
        p["prob_empate"] = inv["cuota_empate"] / overround
        p["prob_visitante"] = inv["cuota_visitante"] / overround
        p["margen_casa"] = overround - 1
    if {"cuota_mas25", "cuota_menos25"}.issubset(p.columns):
        inv25 = 1 / p[["cuota_mas25", "cuota_menos25"]]
        p["prob_mas25"] = inv25["cuota_mas25"] / inv25.sum(axis=1)
        p["mas25_real"] = np.where(p["goles_local"].isna(), np.nan,
                                   ((p["goles_local"] + p["goles_visitante"]) > 2.5).astype(float))
        fav = inv.idxmax(axis=1).map({"cuota_local": "H", "cuota_empate": "D", "cuota_visitante": "A"})
        p["gana_favorito"] = np.where(p["resultado"].isna(), np.nan, (fav == p["resultado"]).astype(float))

    p = p.drop(columns=[c for c in ("local", "visitante") if c in p.columns])
    first = ["partido_id", "fecha_id", "fecha", "temporada", "equipo_local_id", "equipo_visitante_id"]
    return p[first + [c for c in p.columns if c not in first]]


def build_fact_equipo_partido(fp: pd.DataFrame) -> pd.DataFrame:
    """Una fila por equipo y partido + acumulados (para gráficos de evolución)."""
    def lado(es_local: bool) -> pd.DataFrame:
        a, b = ("local", "visitante") if es_local else ("visitante", "local")
        d = pd.DataFrame({
            "partido_id": fp["partido_id"], "temporada": fp["temporada"],
            "fecha_id": fp["fecha_id"], "fecha": fp["fecha"],
            "equipo_id": fp[f"equipo_{a}_id"], "rival_id": fp[f"equipo_{b}_id"],
            "es_local": int(es_local),
        })
        pares = {"goles_favor": f"goles_{a}", "goles_contra": f"goles_{b}",
                 "xg_favor": f"xg_{a}", "xg_contra": f"xg_{b}",
                 "tiros": f"tiros_{a}", "tiros_contra": f"tiros_{b}",
                 "tiros_puerta": f"tiros_puerta_{a}", "corners": f"corners_{a}",
                 "amarillas": f"amarillas_{a}", "rojas": f"rojas_{a}",
                 "ppda": f"ppda_{a}", "prob_victoria": f"prob_{a}"}
        for dest, src in pares.items():
            d[dest] = fp[src] if src in fp.columns else np.nan
        return d

    e = pd.concat([lado(True), lado(False)], ignore_index=True)
    jugado = e["goles_favor"].notna() & e["goles_contra"].notna()
    e["puntos"] = np.select([e["goles_favor"] > e["goles_contra"], e["goles_favor"] == e["goles_contra"]],
                            [3, 1], 0).astype(float)
    e.loc[~jugado, "puntos"] = np.nan
    e["resultado"] = np.select([e["puntos"] == 3, e["puntos"] == 1, e["puntos"] == 0], ["V", "E", "D"], "")
    e = e.sort_values(["temporada", "equipo_id", "fecha"]).reset_index(drop=True)
    e["jornada_equipo"] = e.groupby(["temporada", "equipo_id"]).cumcount() + 1  # nº de partido del equipo
    g = e.groupby(["temporada", "equipo_id"])
    for c in ("puntos", "goles_favor", "goles_contra", "xg_favor", "xg_contra"):
        e[f"{c}_acum"] = g[c].cumsum()
    # Posición tras N partidos jugados (aproximación a "jornada"; ignora aplazados y el golaveraje particular)
    e["dg_acum"] = e["goles_favor_acum"] - e["goles_contra_acum"]
    e = e.sort_values(["temporada", "jornada_equipo", "puntos_acum", "dg_acum", "goles_favor_acum"],
                      ascending=[True, True, False, False, False])
    e["posicion_jornada"] = e.groupby(["temporada", "jornada_equipo"]).cumcount() + 1
    return e.sort_values(["fecha", "partido_id", "es_local"], ascending=[True, True, False]).reset_index(drop=True)


def build_fact_equipo_temporada(fep: pd.DataFrame | None, elo: pd.DataFrame | None,
                                tm: pd.DataFrame | None) -> pd.DataFrame | None:
    if fep is None:
        return None
    j = fep[fep["puntos"].notna()]
    t = j.groupby(["temporada", "equipo_id"]).agg(
        pj=("partido_id", "count"),
        victorias=("puntos", lambda s: (s == 3).sum()),
        empates=("puntos", lambda s: (s == 1).sum()),
        derrotas=("puntos", lambda s: (s == 0).sum()),
        goles_favor=("goles_favor", "sum"), goles_contra=("goles_contra", "sum"),
        puntos=("puntos", "sum"),
        xg_favor=("xg_favor", "sum"), xg_contra=("xg_contra", "sum"),
        tiros=("tiros", "sum"), tiros_contra=("tiros_contra", "sum"),
        ppda_medio=("ppda", "mean"),
    ).reset_index()
    t["diferencia_goles"] = t["goles_favor"] - t["goles_contra"]
    t["xg_diferencia"] = t["xg_favor"] - t["xg_contra"]
    t["puntos_por_partido"] = t["puntos"] / t["pj"]
    t = desempatar(t, j)
    forma = (j.sort_values("fecha").groupby(["temporada", "equipo_id"])["resultado"]
              .apply(lambda s: "".join(s.tail(5))).rename("forma_ultimos5").reset_index())
    t = t.merge(forma, on=["temporada", "equipo_id"], how="left")
    t["es_temporada_actual"] = (t["temporada"] == C.SEASON_LABEL).astype(int)
    if elo is not None:  # el Elo es una foto de hoy: solo tiene sentido en la temporada actual
        ultimo = elo.sort_values("fecha").groupby("equipo_id").tail(1)[["equipo_id", "elo", "rank_elo_mundial"]]
        ultimo["temporada"] = C.SEASON_LABEL
        t = t.merge(ultimo, on=["temporada", "equipo_id"], how="left")
    return t


def desempatar(t: pd.DataFrame, j: pd.DataFrame) -> pd.DataFrame:
    """Orden de La Liga: puntos; si hay empate y ya se han jugado todos los enfrentamientos entre los
    empatados -> puntos y diferencia de goles en esos partidos (mini-liga); después DG general y GF.
    Con la temporada en curso y enfrentamientos pendientes, se usa DG general (como hace la propia liga)."""
    partes = []
    for temp, tt in t.groupby("temporada"):
        tt = tt.copy()
        tt["h2h_pts"], tt["h2h_dg"] = 0.0, 0.0
        jt = j[j["temporada"] == temp]
        for pts, grupo in tt.groupby("puntos"):
            if len(grupo) < 2:
                continue
            eq = set(grupo["equipo_id"])
            h = jt[jt["equipo_id"].isin(eq) & jt["rival_id"].isin(eq)]
            if len(h) < len(eq) * (len(eq) - 1):  # faltan enfrentamientos directos por jugar
                continue
            agg = h.groupby("equipo_id").agg(p=("puntos", "sum"), gf=("goles_favor", "sum"), gc=("goles_contra", "sum"))
            idx = grupo.index
            tt.loc[idx, "h2h_pts"] = grupo["equipo_id"].map(agg["p"]).values
            tt.loc[idx, "h2h_dg"] = grupo["equipo_id"].map(agg["gf"] - agg["gc"]).values
        tt = tt.sort_values(["puntos", "h2h_pts", "h2h_dg", "diferencia_goles", "goles_favor"], ascending=False)
        tt["posicion"] = np.arange(1, len(tt) + 1)
        partes.append(tt.drop(columns=["h2h_pts", "h2h_dg"]))
    return pd.concat(partes, ignore_index=True)


def add_market_expected_points(t: pd.DataFrame, fp: pd.DataFrame) -> pd.DataFrame:
    if not {"prob_local", "prob_empate", "prob_visitante"}.issubset(fp.columns):
        return t
    j = fp[fp["goles_local"].notna()]
    loc = pd.DataFrame({"temporada": j["temporada"], "equipo_id": j["equipo_local_id"],
                        "xpts": 3 * j["prob_local"] + j["prob_empate"]})
    vis = pd.DataFrame({"temporada": j["temporada"], "equipo_id": j["equipo_visitante_id"],
                        "xpts": 3 * j["prob_visitante"] + j["prob_empate"]})
    xp = (pd.concat([loc, vis]).groupby(["temporada", "equipo_id"])["xpts"].sum()
          .rename("puntos_esperados_mercado").reset_index())
    t = t.merge(xp, on=["temporada", "equipo_id"], how="left")
    t["puntos_sobre_esperado"] = t["puntos"] - t["puntos_esperados_mercado"]
    return t


UNDERSTAT_METRICS = ["xg", "npxg", "xag", "tiros", "pases_clave", "xg_chain", "xg_buildup"]

P90_METRICS = ["goles", "asistencias", "goles_sin_penalti", "xg", "npxg", "xag", "tiros", "tiros_puerta",
               "pases_clave", "pases_progresivos", "conducciones_progresivas", "recepciones_progresivas",
               "pases_ultimo_tercio", "pases_area", "sca", "gca", "entradas_ganadas", "intercepciones",
               "bloqueos", "despejes", "regates_completados", "recuperaciones", "duelos_aereos_ganados",
               "paradas", "xg_chain", "xg_buildup"]


def build_jugadores(fb: pd.DataFrame | None, us: pd.DataFrame | None,
                    tm: pd.DataFrame | None) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    # Understat es la base: no tiene captcha y trae xG. FBref solo se usa si no hay Understat.
    base = us if us is not None else fb
    if base is None:
        return None, None
    log.info("jugadores | fuente base: %s", base["fuente"].iat[0])
    j = base.copy()
    j["equipo_id"] = j["equipo"].map(C.team_key)
    j["posicion_grupo"] = j["posicion"].map(pos_grupo)
    if "nacimiento" not in j.columns:
        j["nacimiento"] = pd.array([pd.NA] * len(j), dtype="Int64")
    j["jugador_key"] = j["jugador"].map(C.norm_key)
    if "us_player_id" in j.columns and j["us_player_id"].notna().all():
        j["jugador_id"] = "us-" + j["us_player_id"].astype("Int64").astype(str)
    else:
        j["jugador_id"] = j["jugador_key"].str.replace(" ", "-") + j["nacimiento"].map(
        lambda x: "" if pd.isna(x) else f"-{int(x)}")

    # Base FBref (identidad, minutos, goles) + métricas de xG de Understat.
    # FBref dejó de publicar las métricas Opta (xG, progresivos...), así que Understat las aporta.
    if base is fb and us is not None:
        us_cols = [c for c in UNDERSTAT_METRICS if c in us.columns]
        u = us[["jugador", "equipo"] + us_cols].copy()
        u["jugador_key"] = u["jugador"].map(C.norm_key)
        u["equipo_id"] = u["equipo"].map(C.team_key)
        u["apellido"] = u["jugador_key"].str.split().str[-1]
        j["apellido"] = j["jugador_key"].str.split().str[-1]
        j["_us_ok"] = False
        for c in us_cols:
            if c not in j.columns:
                j[c] = np.nan
        # Cascada: nombre+equipo, apellido+equipo, primer nombre+equipo (claves únicas en ambos lados)
        u["nombre1"] = u["jugador_key"].str.split().str[0]
        j["nombre1"] = j["jugador_key"].str.split().str[0]
        for claves in (["jugador_key", "equipo_id"], ["apellido", "equipo_id"], ["nombre1", "equipo_id"]):
            pend = ~j["_us_ok"]
            uu = u.drop_duplicates(claves, keep=False)
            jj = j.loc[pend, claves].drop_duplicates(keep=False)
            m = j.loc[pend, claves].reset_index().merge(jj, on=claves).merge(uu[claves + us_cols], on=claves)
            if m.empty:
                continue
            m = m.set_index("index")
            for c in us_cols:
                j.loc[m.index, c] = j.loc[m.index, c].fillna(m[c]) if c in FBREF_PLAYER_SPEC else m[c]
            j.loc[m.index, "_us_ok"] = True
            log.info("jugadores | cruce FBref-Understat por %s: +%s", "+".join(claves), len(m))
        con_min = j["minutos"].fillna(0) > 0
        log.info("jugadores | con datos de Understat: %.0f%% (%.0f%% de los que han jugado)",
                 100 * j["_us_ok"].mean(), 100 * j.loc[con_min, "_us_ok"].mean())
        sin = j.loc[con_min & ~j["_us_ok"]].sort_values("minutos", ascending=False)
        if len(sin):
            log.info("jugadores | con minutos y sin cruzar con Understat (top 20): %s",
                     list(zip(sin["jugador"].head(20), sin["equipo"].head(20))))
        j = j.drop(columns=["_us_ok", "apellido", "nombre1"])

    # ---- dim_jugador (+ Transfermarkt) ----
    # Una fila por jugador: la de su temporada más reciente y, dentro de ella, el equipo con más minutos
    dim = (j.sort_values(["temporada", "minutos"], ascending=[False, False])
             .drop_duplicates("jugador_id")
             [["jugador_id", "jugador", "jugador_key", "nacimiento", "posicion", "posicion_grupo", "equipo_id"]
              + [c for c in ("nacionalidad", "us_player_id") if c in j.columns]].copy())
    if tm is not None:
        t = tm.copy()
        t["jugador_key"] = t["jugador_tm"].map(C.norm_key)
        t["apellido"] = t["jugador_key"].str.split().str[-1]
        dim["apellido"] = dim["jugador_key"].str.split().str[-1]
        t["inicial"] = t["jugador_key"].str[:1]
        # solo para nombres de 2+ palabras (un mononombre como "Chupe" no tiene inicial fiable)
        dim["inicial"] = np.where(dim["jugador_key"].str.contains(" "), dim["jugador_key"].str[:1], None)
        cols_tm = ["tm_player_id", "valor_mercado_eur", "valor_maximo_eur", "fin_contrato", "pie",
                   "altura_cm", "imagen_url", "nacimiento_tm", "nacionalidad_tm"]
        t["nacimiento_tm"] = t["nacimiento"]
        for c in cols_tm:
            dim[c] = pd.Series([pd.NA] * len(dim), dtype="object")
        # Cascada de cruces, del más fiable al menos; en cada paso solo claves únicas en Transfermarkt
        pasos = [["jugador_key", "nacimiento"],   # nombre + año nacimiento (base FBref)
                 ["apellido", "nacimiento"],      # "Vinícius Júnior" vs "Vinicius Junior", nombres cortos
                 ["jugador_key", "equipo_id"],    # base Understat (sin año de nacimiento)
                 ["apellido", "equipo_id"],
                 ["jugador_key"],                 # fichajes / ascendidos: nombre único entre todos los activos
                 ["apellido", "inicial"]]         # "Álex Grimaldo" ~ "Alejandro Grimaldo" (único entre activos)
        for claves in pasos:
            pend = dim["tm_player_id"].isna() & dim[claves].notna().all(axis=1)
            if not pend.any():
                continue
            tu = t.dropna(subset=claves).drop_duplicates(claves, keep=False)
            m = dim.loc[pend, ["jugador_id"] + claves].merge(tu[claves + cols_tm], on=claves, how="inner")
            m = m.drop_duplicates("jugador_id", keep=False).set_index("jugador_id")[cols_tm]
            dim = dim.set_index("jugador_id")
            dim.loc[m.index, cols_tm] = m.values
            dim = dim.reset_index()
            log.info("dim_jugador | cruce TM por %s: +%s", "+".join(claves), len(m))
        # Último recurso: todas las palabras de un nombre contenidas en el otro ("Kylian Mbappe-Lottin" ~
        # "Kylian Mbappé"), solo si hay un único candidato entre los jugadores en activo.
        pend_idx = dim.index[dim["tm_player_id"].isna()]
        tm_tokens = [(set(k.split()), i) for i, k in zip(t.index, t["jugador_key"]) if k]
        usados = set(dim["tm_player_id"].dropna())
        n_tok = 0
        for idx in pend_idx:
            toks = set(str(dim.at[idx, "jugador_key"]).split())
            if len(toks) < 2:
                continue
            cands = [i for tk, i in tm_tokens if len(tk) >= 2 and (tk <= toks or toks <= tk)
                     and t.at[i, "tm_player_id"] not in usados]
            if len(cands) == 1:
                dim.loc[idx, cols_tm] = t.loc[cands[0], cols_tm].values
                n_tok += 1
        log.info("dim_jugador | cruce TM por palabras del nombre: +%s", n_tok)
        dim["nacimiento"] = dim["nacimiento"].astype("Float64").fillna(
            pd.to_numeric(dim["nacimiento_tm"], errors="coerce")).astype("Int64")
        for c in ("valor_mercado_eur", "valor_maximo_eur", "altura_cm"):
            dim[c] = pd.to_numeric(dim[c], errors="coerce")
        if "nacionalidad" in dim.columns:
            dim["nacionalidad"] = dim["nacionalidad"].fillna(dim["nacionalidad_tm"])
        else:
            dim["nacionalidad"] = dim["nacionalidad_tm"]
        dim = dim.drop(columns=["apellido", "inicial", "nacimiento_tm", "nacionalidad_tm"])
        tasa = dim["tm_player_id"].notna().mean()
        log.info("dim_jugador | cruce con Transfermarkt: %.0f%% (%s de %s)", 100 * tasa,
                 dim["tm_player_id"].notna().sum(), len(dim))
        sin = dim[dim["tm_player_id"].isna()].head(15)["jugador"].tolist()
        if sin:
            log.info("dim_jugador | ejemplos sin cruzar con Transfermarkt: %s", sin)
    # equipo_id fuera de la dimensión: el equipo va en el hecho (un jugador puede tener 2 en la temporada)
    dim = dim.drop(columns=["jugador_key", "equipo_id"])
    if "nacimiento" in dim.columns:
        dim["edad"] = C.SEASON_START - dim["nacimiento"].astype("Float64")

    # ---- fact_jugador_temporada: p90 + percentiles por posición ----
    f = j.drop(columns=["jugador", "equipo", "posicion", "jugador_key", "nacionalidad", "nacimiento", "fuente",
                        "us_player_id"],
               errors="ignore").copy()
    min90 = f["minutos"] / 90
    for m in P90_METRICS:
        if m in f.columns:
            f[f"{m}_p90"] = np.where(f["minutos"] > 0, f[m] / min90, np.nan)
    if {"pases_completados", "pases_intentados"}.issubset(f.columns):
        f["pct_pases"] = f["pases_completados"] / f["pases_intentados"].replace(0, np.nan)
    if {"regates_completados", "regates_intentados"}.issubset(f.columns):
        f["pct_regates"] = f["regates_completados"] / f["regates_intentados"].replace(0, np.nan)
    if {"goles_sin_penalti", "npxg"}.issubset(f.columns):
        f["goles_menos_xg"] = f["goles_sin_penalti"] - f["npxg"]

    # Jugadores traspasados dentro de La Liga tienen 2 filas (una por equipo); el percentil se hace
    # sobre el total del jugador en la temporada y se asigna a ambas filas.
    tot_cols = ["minutos"] + [m for m in P90_METRICS if m in f.columns]
    # Percentiles dentro de cada temporada y posición.
    tot = f.groupby(["temporada", "jugador_id", "posicion_grupo"], as_index=False)[tot_cols].sum()
    elegibles = tot["minutos"] >= C.MIN_MINUTES_PERCENTIL
    el = tot.loc[elegibles]
    f = f.merge(el[["temporada", "jugador_id"]].drop_duplicates().assign(elegible_percentil=1),
                on=["temporada", "jugador_id"], how="left")
    f["elegible_percentil"] = f["elegible_percentil"].fillna(0).astype(int)
    pct = el[["temporada", "jugador_id"]].copy()
    grupos = [el["temporada"], el["posicion_grupo"]]
    for m in P90_METRICS:
        if m in el.columns and el[m].notna().any():
            v = el[m] / (el["minutos"] / 90)
            pct[f"{m}_p90_pctl"] = (v.groupby(grupos).rank(pct=True) * 100).round(1)
    pct = pct.drop_duplicates(["temporada", "jugador_id"])
    f = f.merge(pct, on=["temporada", "jugador_id"], how="left")
    first = ["temporada", "jugador_id", "equipo_id", "posicion_grupo"]
    f = f[first + [c for c in f.columns if c not in first]]
    return dim, f


def _enlace_partido(fp: pd.DataFrame) -> pd.DataFrame:
    return (fp[["us_game_id", "partido_id", "fecha_id", "temporada", "equipo_local_id", "equipo_visitante_id"]]
            .dropna(subset=["us_game_id"]).drop_duplicates("us_game_id"))


def build_fact_tiro(tiros: pd.DataFrame | None, fp: pd.DataFrame | None,
                    jp: pd.DataFrame | None = None) -> pd.DataFrame | None:
    if tiros is None or fp is None or "us_game_id" not in fp.columns:
        return None
    # El assist_player_id de Understat NO es el id del jugador (es un id interno del pase).
    # Identificamos al asistente por su nombre entre los jugadores de su equipo en ese mismo partido.
    if jp is not None and "asistente" in tiros.columns:
        clave = jp[["us_game_id", "equipo", "jugador", "us_player_id"]].dropna(subset=["jugador"]).copy()
        clave["_k"] = clave["jugador"].map(C.norm_key)
        clave = clave.drop_duplicates(["us_game_id", "equipo", "_k"], keep=False)
        tiros = tiros.copy()
        tiros["_k"] = tiros["asistente"].map(C.norm_key)
        tiros = tiros.drop(columns="us_asistente_id", errors="ignore").merge(
            clave[["us_game_id", "equipo", "_k", "us_player_id"]].rename(columns={"us_player_id": "us_asistente_id"}),
            on=["us_game_id", "equipo", "_k"], how="left").drop(columns="_k")
        con = tiros["asistente"].notna() & (tiros["asistente"].astype(str).str.strip() != "")
        log.info("fact_tiro | asistentes identificados: %s de %s tiros asistidos",
                 tiros.loc[con, "us_asistente_id"].notna().sum(), con.sum())
    elif "us_asistente_id" in tiros.columns:
        tiros = tiros.assign(us_asistente_id=np.nan)
    t = tiros.drop(columns="temporada").merge(_enlace_partido(fp), on="us_game_id", how="inner")
    t["equipo_id"] = t["equipo"].map(C.team_key)
    t["rival_id"] = np.where(t["equipo_id"] == t["equipo_local_id"], t["equipo_visitante_id"], t["equipo_local_id"])
    t["es_local"] = (t["equipo_id"] == t["equipo_local_id"]).astype(int)
    t["jugador_id"] = "us-" + t["us_player_id"].astype("Int64").astype(str)
    t["asistente_id"] = np.where(t["us_asistente_id"].notna(),
                                 "us-" + t["us_asistente_id"].astype("Int64").astype(str), None)
    # soccerdata devuelve "Open Play" / Understat "OpenPlay": se normaliza quitando espacios
    for col, dic in (("situacion", SITUACION), ("resultado", RESULTADO_TIRO), ("parte_cuerpo", PARTE_CUERPO)):
        clave = t[col].astype(str).str.replace(" ", "", regex=False)
        t[col] = clave.map(dic).fillna(t[col])
    # soccerdata deja vacíos los penaltis (situación) y los cabezazos/otras partes (cuerpo). Comprobado con los
    # datos reales: los tiros sin situación tienen todos xG 0,743 (penalti en Understat) y los sin parte del
    # cuerpo están a ~11 m de media, casi siempre tras córner o centro.
    t["situacion"] = t["situacion"].fillna("Penalti")
    t["parte_cuerpo"] = t["parte_cuerpo"].fillna("Cabeza u otra")
    t["es_gol"] = (t["resultado"] == "Gol").astype(int)
    t["es_gol_propia"] = (t["resultado"] == "Gol en propia").astype(int)
    t["a_puerta"] = t["resultado"].isin(["Gol", "Parado"]).astype(int)
    # Understat: x e y en [0,1] desde la perspectiva del que tira (x=1 es la línea de gol rival).
    # En metros sobre un campo de 105x68 para pintar el mapa de tiros.
    t["x_m"], t["y_m"] = (t["x"] * 105).round(2), (t["y"] * 68).round(2)
    t["distancia_m"] = np.sqrt(((1 - t["x"]) * 105) ** 2 + ((t["y"] - 0.5) * 68) ** 2).round(1)
    t["tramo_minuto"] = pd.cut(t["minuto"], [-1, 15, 30, 45, 60, 75, 90, 200],
                               labels=["0-15", "16-30", "31-45", "46-60", "61-75", "76-90", "90+"]).astype(str)
    cols = ["temporada", "partido_id", "fecha_id", "us_shot_id", "equipo_id", "rival_id", "es_local", "jugador_id",
            "asistente_id", "minuto", "tramo_minuto", "xg", "x", "y", "x_m", "y_m", "distancia_m", "situacion",
            "parte_cuerpo", "resultado", "es_gol", "es_gol_propia", "a_puerta"]
    return t[cols].sort_values(["fecha_id", "partido_id", "minuto"]).reset_index(drop=True)


def build_fact_jugador_partido(jp: pd.DataFrame | None, fp: pd.DataFrame | None) -> pd.DataFrame | None:
    if jp is None or fp is None or "us_game_id" not in fp.columns:
        return None
    j = jp.drop(columns="temporada").merge(_enlace_partido(fp), on="us_game_id", how="inner")
    j["equipo_id"] = j["equipo"].map(C.team_key)
    j["rival_id"] = np.where(j["equipo_id"] == j["equipo_local_id"], j["equipo_visitante_id"], j["equipo_local_id"])
    j["es_local"] = (j["equipo_id"] == j["equipo_local_id"]).astype(int)
    j["jugador_id"] = "us-" + j["us_player_id"].astype("Int64").astype(str)
    j["titular"] = (~j["posicion"].astype(str).str.upper().isin(["SUB", "NAN", ""])).astype(int)
    j = j.drop(columns=["us_game_id", "us_player_id", "equipo", "jugador", "equipo_local_id", "equipo_visitante_id"])
    first = ["temporada", "partido_id", "fecha_id", "jugador_id", "equipo_id", "rival_id", "es_local", "posicion"]
    return j[first + [c for c in j.columns if c not in first]].sort_values(["fecha_id", "partido_id"]).reset_index(drop=True)


def build_dim_equipo(fuentes: dict[str, pd.Series], master: set[str]) -> pd.DataFrame:
    """Un equipo por clave canónica. Nombre visible: el de la primera fuente disponible en este orden."""
    filas = {}
    for fuente, nombres in fuentes.items():
        for n in pd.Series(nombres).dropna().unique():
            k = C.team_key(n)
            filas.setdefault(k, {"equipo_id": k, "equipo": n})
            filas[k][f"nombre_{fuente}"] = n
    dim = pd.DataFrame(filas.values())
    if master:
        extra = set(dim["equipo_id"]) - master
        if extra:
            log.warning("dim_equipo | claves que NO cruzan con los equipos de la liga "
                        "(añadir a TEAM_ALIASES en config.py): %s",
                        {k: {c: v for c, v in filas[k].items() if c.startswith("nombre_")} for k in extra})
        faltan = master - set(dim["equipo_id"])
        if faltan:
            log.warning("dim_equipo | equipos de la liga sin nombre en ninguna fuente: %s", faltan)
        dim = dim[dim["equipo_id"].isin(master)]
    return dim.sort_values("equipo").reset_index(drop=True)


DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
         "septiembre", "octubre", "noviembre", "diciembre"]


def build_dim_temporada() -> pd.DataFrame:
    return pd.DataFrame({
        "temporada": [C.season_label(s) for s in C.SEASONS],
        "anio_inicio": C.SEASONS,
        "es_temporada_actual": [int(s == C.SEASON_START) for s in C.SEASONS],
    })


def build_dim_fecha() -> pd.DataFrame:
    r = pd.date_range(f"{C.SEASONS[0]}-07-01", f"{C.SEASON_END}-06-30", freq="D")
    inicio = np.where(r.month >= 7, r.year, r.year - 1)
    return pd.DataFrame({
        "fecha_id": r.strftime("%Y%m%d").astype(int), "fecha": r.date,
        "anio": r.year, "mes": r.month, "mes_nombre": [MESES[m - 1] for m in r.month],
        "anio_mes": r.strftime("%Y-%m"), "orden_mes_temporada": ((r.month - 7) % 12) + 1,
        "dia_semana": [DIAS[d] for d in r.dayofweek], "num_dia_semana": r.dayofweek + 1,
        "es_fin_de_semana": (r.dayofweek >= 5).astype(int),
        "semana_iso": r.isocalendar().week.astype(int).values,
        "temporada": [C.season_label(int(x)) for x in inicio],
    })


# --------------------------------------------------------------------------
def main() -> int:
    log.info("=== TRANSFORM | temporadas %s ===", [C.season_label(s) for s in C.SEASONS])
    fd = por_temporada(silver_partidos_fd, "partidos_footballdata")
    us_p = por_temporada(silver_partidos_us, "partidos_understat")
    us_j = por_temporada(silver_jugadores_understat, "jugadores_understat")
    fb = silver_jugadores_fbref()
    tiros = por_temporada(silver_tiros, "tiros")
    jp = por_temporada(silver_jugadores_partido, "jugadores_partido")
    wd = silver_wikidata()
    tm, elo = silver_transfermarkt(), silver_elo()

    fp = build_fact_partido(fd, us_p)
    if fp is not None:
        # Los 20 equipos de los partidos son la lista maestra; se re-mapean las fuentes con ella
        C.TEAM_MASTER.update(set(fp["equipo_local_id"]) | set(fp["equipo_visitante_id"]))
        if tm is not None:
            tm["equipo_id"] = tm["equipo_tm"].map(C.team_key)
        if elo is not None:
            elo["equipo_id"] = elo["equipo_elo"].map(C.team_key)
    fep = build_fact_equipo_partido(fp) if fp is not None else None
    fet = build_fact_equipo_temporada(fep, elo, tm)
    if fet is not None and fp is not None:
        fet = add_market_expected_points(fet, fp)
    dim_j, fjt = build_jugadores(fb, us_j, tm)

    # Valor de plantilla = suma del valor TM de los jugadores que han jugado en el equipo ESTA temporada
    # (según Understat), no el club que diga Transfermarkt, que puede ir retrasado.
    if fet is not None and dim_j is not None and fjt is not None and "valor_mercado_eur" in dim_j.columns:
        actual = fjt[fjt["temporada"] == C.SEASON_LABEL]
        vp = (actual[["jugador_id", "equipo_id"]].drop_duplicates()
              .merge(dim_j[["jugador_id", "valor_mercado_eur"]], on="jugador_id", how="left")
              .groupby("equipo_id").agg(valor_plantilla_eur=("valor_mercado_eur", "sum"),
                                        jugadores_usados=("jugador_id", "count"),
                                        jugadores_con_valor=("valor_mercado_eur", "count")).reset_index())
        vp["temporada"] = C.SEASON_LABEL  # valor de mercado = foto actual -> solo temporada actual
        fet = fet.merge(vp, on=["temporada", "equipo_id"], how="left")

    master = set(fep["equipo_id"]) if fep is not None else set()
    actuales = set(fep.loc[fep["temporada"] == C.SEASON_LABEL, "equipo_id"]) if fep is not None else set()
    fuentes = {}
    if fb is not None: fuentes["fbref"] = fb["equipo"]
    if us_p is not None: fuentes["understat"] = pd.concat([us_p["local"], us_p["visitante"]])
    elif us_j is not None: fuentes["understat"] = us_j["equipo"]
    if fd is not None: fuentes["footballdata"] = pd.concat([fd["local"], fd["visitante"]])
    if elo is not None: fuentes["clubelo"] = elo["equipo_elo"]
    dim_e = build_dim_equipo(fuentes, master) if fuentes else None
    if dim_e is not None:
        dim_e["en_temporada_actual"] = dim_e["equipo_id"].isin(actuales).astype(int)
        if wd is not None:
            dim_e = dim_e.merge(wd.drop(columns=["club_wikidata"], errors="ignore"), on="equipo_id", how="left")
            dim_e["ciudad"] = dim_e["equipo_id"].map(C.CIUDAD_MANUAL).fillna(dim_e["ciudad"])
            sin = dim_e.loc[dim_e["estadio"].isna(), "equipo"].tolist()
            if sin:
                log.warning("dim_equipo | sin datos de estadio (Wikidata): %s", sin)
    ftiro = build_fact_tiro(tiros, fp, jp)
    fjp = build_fact_jugador_partido(jp, fp)
    if ftiro is not None:
        log.info("fact_tiro | %s tiros | goles: %s | xG total: %.1f", len(ftiro), ftiro["es_gol"].sum(), ftiro["xg"].sum())

    gold = {"dim_temporada": build_dim_temporada(), "dim_equipo": dim_e, "dim_jugador": dim_j,
            "dim_fecha": build_dim_fecha(),
            "fact_partido": fp, "fact_equipo_partido": fep, "fact_equipo_temporada": fet,
            "fact_jugador_temporada": fjt, "fact_jugador_partido": fjp, "fact_tiro": ftiro}
    meta = []
    for name, df in gold.items():
        if df is None:
            log.warning("gold | %s NO generada (falta su fuente)", name)
            continue
        C.save_gold(df, name)
        meta.append({"tabla": name, "filas": len(df), "columnas": df.shape[1]})
        log.info("gold | %-24s %6s filas  %3s columnas", name, len(df), df.shape[1])

    ahora = datetime.now(timezone.utc)
    m = pd.DataFrame(meta)
    m["temporadas"] = ", ".join(C.season_label(s) for s in C.SEASONS)
    m["actualizado_utc"] = ahora.strftime("%Y-%m-%d %H:%M")
    m["fuentes_disponibles"] = ", ".join(sorted(k for k, v in {
        "fbref": fb, "understat": us_p if us_p is not None else us_j, "footballdata": fd, "wikidata": wd,
        "clubelo": elo, "transfermarkt": tm}.items() if v is not None))
    C.save_gold(m, "meta")
    log.info("=== FIN TRANSFORM ===")
    return 0 if fp is not None or fjt is not None else 1


if __name__ == "__main__":
    sys.exit(main())
