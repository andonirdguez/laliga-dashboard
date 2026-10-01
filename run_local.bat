@echo off
REM Plan B / pruebas: ejecuta el pipeline en local y sube data/gold a GitHub.
REM Programable con el Programador de tareas de Windows (Acción: iniciar este .bat, "Iniciar en": carpeta del repo).
setlocal
cd /d "%~dp0"

if not exist .venv (
    python -m venv .venv
    call .venv\Scripts\activate
    pip install -r requirements.txt
) else (
    call .venv\Scripts\activate
)

git pull --rebase

python pipeline\ingest.py %*
python pipeline\transform.py
if errorlevel 1 (
    echo Transform fallo. Revisa logs\
    exit /b 1
)

git add data\gold
git diff --cached --quiet && (echo Sin cambios en gold) || (
    git commit -m "datos: actualizacion local %date% %time%"
    git push
)
endlocal
