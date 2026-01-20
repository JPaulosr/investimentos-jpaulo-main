@echo off
cd /d "%~dp0"

call venv\Scripts\activate

python -m streamlit run app_investimentos.py

pause
