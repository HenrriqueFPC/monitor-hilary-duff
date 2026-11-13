@echo off
cd /d "%~dp0"
python "%~dp0monitor.py" --stop
timeout /t 3 >nul
