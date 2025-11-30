@echo off
REM Lancer l'application Gestion Emploi
cd /d "%~dp0"
call .venv\Scripts\activate
python run.py
pause
