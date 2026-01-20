# utils/data_io.py
import pandas as pd
import streamlit as st

@st.cache_data(show_spinner=False)
def read_csv_upload(file) -> pd.DataFrame | None:
    if file is None:
        return None
    df = pd.read_csv(file)
    df.columns = [str(c).strip() for c in df.columns]
    return df

@st.cache_data(show_spinner=False)
def read_public_gsheets_csv(url: str) -> pd.DataFrame:
    if not url or not str(url).strip():
        return pd.DataFrame()
    df = pd.read_csv(url)
    df.columns = [str(c).strip() for c in df.columns]
    return df
def read_gsheet_tabs(sheet_url: str, tabs: dict) -> dict:
    """
    tabs = {
        "ativos": "ativos",
        "posicoes": "posicoes_snapshot",
        "proventos": "proventos"
    }
    """
    out = {}
    for key, tab in tabs.items():
        url = f"{sheet_url}&single=true&output=csv&gid={tab}"
        out[key] = pd.read_csv(url)
        out[key].columns = [c.strip() for c in out[key].columns]
    return out
