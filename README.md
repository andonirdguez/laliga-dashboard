# La Liga Dashboard — pipeline de datos abiertos

Pipeline medallón (bronze → silver → gold) que descarga cada día datos públicos de La Liga,
construye un modelo estrella en CSV y lo publica en este repo para que Power BI lo lea sin gateway.

```
fuentes públicas ──► ingest.py ──► data/bronze (parquet, NO en Git)
                                        │
                     transform.py ──────┼──► data/silver (parquet, NO en Git)
                                        └──► data/gold   (CSV, SÍ en Git) ──► raw.githubusercontent.com ──► Power BI
GitHub Actions: cada día 05:00 UTC ─ ejecuta todo y hace commit de data/gold
```

## Fuentes

| Fuente | Acceso | Aporta | Riesgo |
|---|---|---|---|
| football-data.co.uk | CSV directo | resultados, tiros, córners, tarjetas, cuotas | nulo |
| ClubElo | API CSV | Elo diario (el histórico se acumula día a día) | nulo |
| Understat | soccerdata | xG por partido, PPDA, xG chain/buildup por jugador | medio |
| Transfermarkt (Kaggle `davidcariboo/player-scores`) | API Kaggle | valor de mercado, contrato, pie, altura | bajo |
| FBref | soccerdata (~10 req/min) | stats avanzadas de jugador | alto (bloqueos) |

Cada fuente es independiente: si una falla, el resto sigue y `transform.py` genera lo que pueda.
Si FBref cae, las tablas de jugador se construyen con Understat como base.

## Modelo gold (`data/gold/*.csv`)

| Tabla | Grano | Clave |
|---|---|---|
| `dim_equipo` | equipo | `equipo_id` (+ nombre del equipo en cada fuente) |
| `dim_jugador` | jugador | `jugador_id` (nombre-año) + datos Transfermarkt |
| `dim_fecha` | día (1 jul – 30 jun) | `fecha_id` (yyyymmdd) |
| `fact_partido` | partido | `partido_id`; goles, tiros, xG, cuotas, prob. implícita |
| `fact_equipo_partido` | equipo × partido | acumulados y posición tras N partidos (evolución) |
| `fact_equipo_temporada` | equipo | clasificación, xG, Elo, valor plantilla, puntos esperados según mercado |
| `fact_jugador_temporada` | jugador × equipo | totales, p90 y percentiles por posición (mín. 450') |
| `meta` | tabla | filas, fecha de actualización, fuentes disponibles |

## Puesta en marcha

### 1. Repo
```bash
git init && git add . && git commit -m "pipeline inicial"
git branch -M main
git remote add origin https://github.com/TU_USUARIO/laliga-dashboard.git
git push -u origin main
```
El repo debe ser **público** (Actions ilimitado y URLs raw sin autenticación).

### 2. Entorno local (Windows)
```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
Kaggle: kaggle.com → Settings → API → *Create New Token* → guarda `kaggle.json` en `C:\Users\<tú>\.kaggle\`.

### 3. Ingesta fuente a fuente (de menos a más riesgo)
```bat
python pipeline\ingest.py --source footballdata
python pipeline\ingest.py --source clubelo
python pipeline\ingest.py --source understat
python pipeline\ingest.py --source transfermarkt
python pipeline\ingest.py --source fbref
```
El log (`logs/AAAA-MM-DD.log`) imprime las columnas de cada tabla descargada.

### 4. Transformación
```bat
python pipeline\transform.py
```
Revisa en el log:
- `columnas NO encontradas` / `métrica ... no encontrada` → el nombre de columna de la fuente cambió; se corrige en `FBREF_PLAYER_SPEC` o en el `pick(...)` correspondiente.
- `claves que NO cruzan con los 20 equipos` → añade el alias en `TEAM_ALIASES` (`config.py`).
- `cruce con Transfermarkt: X%` → por encima del 85–90% es razonable; los ejemplos sin cruzar salen listados.

### 5. GitHub Actions
1. Settings → Secrets and variables → Actions → `KAGGLE_USERNAME`, `KAGGLE_KEY`.
2. Settings → Actions → General → Workflow permissions → **Read and write**.
3. Actions → *Actualización diaria La Liga* → **Run workflow** (se puede limitar a unas fuentes).
4. El log queda como artefacto de cada ejecución.

Si FBref devuelve 403 desde los runners: lanza el workflow sin `fbref` y usa `run_local.bat fbref`
(o todo el pipeline) desde el Programador de tareas de Windows.

### 6. Power BI
- `powerbi/conexion.m`: parámetro `pRepo`, función `fnGold` con `Web.Contents` + `RelativePath` (refresh en el Service sin gateway, credenciales *Anónimo*) y tipado con cultura `en-US`.
- `powerbi/medidas.dax`: tabla `_Medidas` y relaciones esperadas.

## Estructura
```
pipeline/config.py      rutas, temporada automática, normalización de nombres, IO, logging
pipeline/ingest.py      bronze (una función por fuente, --source para elegir)
pipeline/transform.py   silver + gold
.github/workflows/daily.yml
powerbi/conexion.m  powerbi/medidas.dax
run_local.bat           ejecución local + commit (plan B)
```

La temporada se calcula sola (de julio en adelante = temporada nueva): hoy `2026-27`, código `2627`.
