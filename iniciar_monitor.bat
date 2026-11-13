@echo off
rem Inicia o monitor em segundo plano (sem janela). Para ver o que ele esta fazendo, abra monitor.log
cd /d "%~dp0"
start "" pythonw "%~dp0monitor.py"
echo Monitor iniciado em segundo plano. Log: %~dp0monitor.log
timeout /t 3 >nul
