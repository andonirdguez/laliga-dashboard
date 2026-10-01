"""Configuración compartida del pipeline La Liga (bronze -> silver -> gold)."""
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date
from pathlib import Path

import pandas as pd

# --------------------------------------------------------------------------
# Rutas
# --------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
BRONZE = DATA / "bronze"          # crudo por fuente (NO se sube a Git)
HISTORY = BRONZE / "_history"     # snapshots diarios (NO se sube a Git)
SILVER = DATA / "silver"          # limpio y normalizado (NO se sube a Git)
GOLD = DATA / "gold"              # modelo estrella en CSV (SÍ se sube a Git)
LOGS = ROOT / "logs"

for p in (BRONZE, HISTORY, SILVER, GOLD, LOGS):
    p.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------
# Temporada: se calcula sola. De julio en adelante = temporada que empieza ese año.
# --------------------------------------------------------------------------
def current_season(today: date | None = None) -> tuple[int, int]:
    today = today or date.today()
    start = today.year if today.month >= 7 else today.year - 1
    return start, start + 1


SEASON_START, SEASON_END = current_season()
SEASON_LABEL = f"{SEASON_START}-{str(SEASON_END)[-2:]}"              # 2026-27
SEASON_CODE = f"{str(SEASON_START)[-2:]}{str(SEASON_END)[-2:]}"      # 2627 (soccerdata / football-data)

# --------------------------------------------------------------------------
# Fuentes
# --------------------------------------------------------------------------
LEAGUE_SD = "ESP-La Liga"  # clave de soccerdata
FOOTBALL_DATA_URL = f"https://www.football-data.co.uk/mmz4281/{SEASON_CODE}/SP1.csv"
CLUBELO_URL = "http://api.clubelo.com/{fecha}"
KAGGLE_DATASET = "davidcariboo/player-scores"
KAGGLE_FILES = ["players.csv", "clubs.csv"]
TM_COMPETITION = "ES1"  # La Liga en Transfermarkt

# FBref dejó de publicar las tablas de Opta (shooting, passing, defense, possession, gca):
# solo pedimos las básicas. El xG de jugador sale de Understat.
FBREF_PLAYER_STATS = ["standard", "keeper", "misc"]
FBREF_TEAM_STATS = ["standard"]

MIN_MINUTES_PERCENTIL = 450

# FBref fuera del pipeline (captcha de Cloudflare + sin métricas Opta). True solo para pruebas manuales.
USE_FBREF = False  # mínimo de minutos para entrar en percentiles

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (laliga-dashboard; proyecto personal sin fines comerciales)"}


# --------------------------------------------------------------------------
# Normalización de nombres (el problema nº1 al cruzar fuentes)
# --------------------------------------------------------------------------
def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


_STOPWORDS = {"fc", "cf", "cd", "ud", "rcd", "sd", "rc", "ca", "sad", "s", "a", "d", "club", "de", "futbol",
              "balompie", "real", "reial", "deportiu", "deportivo", "union", "deportiva", "team", "dubai"}

# Claves canónicas de los equipos de la temporada (las fija transform.py a partir de los partidos).
# Si un nombre no cruza por alias, se busca un equipo cuyas palabras estén TODAS contenidas en él.
TEAM_MASTER: set[str] = set()


def norm_key(s: str) -> str:
    """Clave de comparación: sin acentos, minúsculas, sin puntuación."""
    if pd.isna(s):
        return ""
    s = strip_accents(str(s)).lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def team_key(s: str) -> str:
    """Clave de equipo: norm_key + alias conocidos + sin palabras genéricas."""
    k = norm_key(s)
    if k in TEAM_ALIASES:
        return TEAM_ALIASES[k]
    k2 = " ".join(w for w in k.split() if w not in _STOPWORDS)
    k2 = TEAM_ALIASES.get(k2, k2 or k)
    if TEAM_MASTER and k2 not in TEAM_MASTER:
        palabras = set(k.split()) | set(k2.split())
        cands = [m for m in TEAM_MASTER if set(m.split()) <= palabras]
        if len(cands) == 1:
            return cands[0]
    return k2


# Cada fuente llama distinto a los equipos. Clave = norm_key del nombre en la fuente,
# valor = clave canónica. Añade aquí los que el log marque como "sin cruzar".
TEAM_ALIASES = {
    # Athletic
    "ath bilbao": "athletic", "athletic club": "athletic", "bilbao": "athletic", "athletic bilbao": "athletic",
    # Atlético
    "ath madrid": "atletico madrid", "atletico": "atletico madrid", "atletico madrid": "atletico madrid",
    "club atletico de madrid": "atletico madrid", "atl madrid": "atletico madrid",
    # Real Madrid / Betis / Sociedad / Oviedo / Valladolid / Mallorca (el "real" se quita)
    "real madrid": "madrid", "madrid": "madrid",
    "betis": "betis", "real betis": "betis", "real betis balompie": "betis",
    "sociedad": "sociedad", "real sociedad": "sociedad",
    "mallorca": "mallorca", "rcd mallorca": "mallorca",
    "oviedo": "oviedo", "real oviedo": "oviedo",
    "valladolid": "valladolid", "real valladolid": "valladolid",
    "zaragoza": "zaragoza", "real zaragoza": "zaragoza",
    # Otros
    "barcelona": "barcelona", "fc barcelona": "barcelona", "barca": "barcelona",
    "celta": "celta", "celta vigo": "celta", "celta de vigo": "celta", "rc celta": "celta",
    "espanol": "espanyol", "espanyol": "espanyol", "rcd espanyol": "espanyol",
    "alaves": "alaves", "deportivo alaves": "alaves",
    "vallecano": "rayo vallecano", "rayo vallecano": "rayo vallecano", "rayo": "rayo vallecano",
    "la coruna": "deportivo", "deportivo": "deportivo", "deportivo la coruna": "deportivo", "depor": "deportivo",
    "gijon": "sporting", "sporting gijon": "sporting", "sporting": "sporting",
    "las palmas": "las palmas", "leganes": "leganes", "getafe": "getafe", "girona": "girona",
    "osasuna": "osasuna", "sevilla": "sevilla", "valencia": "valencia", "villarreal": "villarreal",
    "levante": "levante", "elche": "elche", "cadiz": "cadiz", "granada": "granada", "almeria": "almeria",
    "racing santander": "racing", "santander": "racing", "racing": "racing",
    "eibar": "eibar", "huesca": "huesca", "mirandes": "mirandes", "burgos": "burgos",
    "cordoba": "cordoba", "malaga": "malaga", "albacete": "albacete", "tenerife": "tenerife",
    "castellon": "castellon", "ceuta": "ceuta", "andorra": "andorra",
    # Nombres legales de Transfermarkt (tras quitar palabras genéricas)
    "espanyol barcelona": "espanyol", "atletico osasuna": "osasuna", "rayo vallecano madrid": "rayo vallecano",
    "rc deportivo": "deportivo", "dep coruna": "deportivo", "a coruna": "deportivo",
    "racing sant": "racing", "sporting gijon": "sporting", "racing santander": "racing",
}


def slug(s: str) -> str:
    return norm_key(s).replace(" ", "-")


# --------------------------------------------------------------------------
# IO
# --------------------------------------------------------------------------
def save_bronze(df: pd.DataFrame, source: str, name: str) -> Path:
    folder = BRONZE / source
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{name}.parquet"
    df.to_parquet(path, index=False)
    # Muestra legible en CSV para revisar a ojo (bronze no va a Git)
    df.head(300).to_csv(folder / f"{name}.muestra.csv", index=False, encoding="utf-8")
    hist = HISTORY / date.today().isoformat() / source
    hist.mkdir(parents=True, exist_ok=True)
    df.to_parquet(hist / f"{name}.parquet", index=False)
    return path


def load_bronze(source: str, name: str) -> pd.DataFrame | None:
    path = BRONZE / source / f"{name}.parquet"
    return pd.read_parquet(path) if path.exists() else None


def save_silver(df: pd.DataFrame, name: str) -> None:
    df.to_parquet(SILVER / f"{name}.parquet", index=False)


def save_gold(df: pd.DataFrame, name: str) -> None:
    # CSV con punto decimal y UTF-8: en Power Query se tipa con cultura "en-US".
    df.to_csv(GOLD / f"{name}.csv", index=False, encoding="utf-8", float_format="%.4f")


# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOGS / f"{date.today().isoformat()}.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger
