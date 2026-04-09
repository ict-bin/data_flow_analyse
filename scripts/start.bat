@echo off
cd /d "%~dp0\.."
if not exist output mkdir output
if not exist sessions mkdir sessions
if not exist workspace mkdir workspace

where pi >nul 2>nul || (echo pi not found. npm install -g @mariozechner/pi-coding-agent & exit /b 1)
python -c "import fastapi" >nul 2>nul || pip install -r requirements.txt

if "%1"=="--cli" (shift & python cli.py %*) else (python main.py)
