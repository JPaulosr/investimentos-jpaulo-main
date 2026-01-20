# data_gsheets.py
import gspread
import streamlit as st
from google.oauth2.service_account import Credentials

SCOPES_RW = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def get_gc():
    creds_info = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if not creds_info:
        raise RuntimeError("Faltou [GCP_SERVICE_ACCOUNT] no secrets.toml")
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES_RW)
    return gspread.authorize(creds)

def open_sheet(sheet_id: str):
    gc = get_gc()
    return gc.open_by_key(sheet_id)

def ensure_worksheet(sh, title: str, rows: int = 2000, cols: int = 40):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

def set_headers(ws, headers: list[str]):
    ws.update("A1", [headers])  # linha 1
    try:
        ws.freeze(rows=1)
    except Exception:
        pass

def ensure_schema(sheet_id: str, schema: dict[str, list[str]]):
    sh = open_sheet(sheet_id)
    for tab, headers in schema.items():
        ws = ensure_worksheet(sh, tab)
        set_headers(ws, headers)
    return True
