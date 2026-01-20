# app_investimentos.py
# Feed Inteligente (DB novo) — versão estável (sem f-string problemática)

import streamlit as st
import pandas as pd
import numpy as np
from datetime import date

import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Investimentos • Feed Inteligente", layout="wide")

SHEET_ID = st.secrets["SHEET_ID_NOVO"]
ABA_PROVENTOS = st.secrets.get("ABA_PROVENTOS_NOVO", "proventos")

# =========================
# GSheets
# =========================
def get_gc():
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(
        st.secrets["GCP_SERVICE_ACCOUNT"],
        scopes=scopes,
    )
    return gspread.authorize(creds)

@st.cache_data(ttl=300)
def read_proventos(sheet_id: str, aba: str) -> pd.DataFrame:
    gc = get_gc()
    ws = gc.open_by_key(sheet_id).worksheet(aba)
    rows = ws.get_all_records()
    return pd.DataFrame(rows)

# =========================
# Normalização (DB novo)
# =========================
def to_num(x):
    return pd.to_numeric(x, errors="coerce")

def normalize_proventos_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["id", "ticker", "tipo", "data", "quantidade", "unitario", "total"])

    needed = [
        "id", "data_pagamento", "ticker", "tipo_provento",
        "quantidade_na_data", "valor_total", "valor_por_cota"
    ]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"Schema inesperado na aba '{ABA_PROVENTOS}'. "
            f"Faltando colunas: {missing}. Colunas atuais: {list(df.columns)}"
        )

    out = pd.DataFrame({
        "id": df["id"].astype(str).str.strip(),
        "ticker": df["ticker"].astype(str).str.strip().str.upper(),
        "tipo": df["tipo_provento"].astype(str).str.strip(),
        "data": pd.to_datetime(df["data_pagamento"], errors="coerce", dayfirst=True),
        "quantidade": to_num(df["quantidade_na_data"]),
        "unitario": to_num(df["valor_por_cota"]),
        "total": to_num(df["valor_total"]),
    })

    out = out.dropna(subset=["ticker", "data"])
    out = out[out["ticker"].str.len() > 0]
    out = out.sort_values(["ticker", "tipo", "data"], ascending=[True, True, True])
    out = out.dropna(subset=["total", "unitario"], how="all")
    return out

# =========================
# Formatação
# =========================
def fmt_date(x):
    if pd.isna(x):
        return "-"
    return pd.to_datetime(x).strftime("%d/%m/%Y")

def fmt_brl(x):
    if pd.isna(x):
        return "-"
    s = f"{float(x):,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_unit(x):
    if pd.isna(x):
        return "-"
    return f"{float(x):.4f}".replace(".", ",")

def fmt_qtd(x):
    if pd.isna(x):
        return "-"
    xf = float(x)
    if abs(xf - round(xf)) < 1e-9:
        return str(int(round(xf)))
    return str(xf).replace(".", ",")

# =========================
# Motor de Insights (comparação por MESMO tipo)
# =========================
def classify_event(tipo: str) -> str:
    t = (tipo or "").strip().lower()
    if "jcp" in t:
        return "EXTRA (JCP)"
    if "div" in t:
        return "EXTRA (Dividendo)"
    if "rend" in t:
        return "RECORRENTE (Rendimento)"
    return "OUTRO"

def build_insights(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    last_rows = (
        df.sort_values("data")
          .groupby(["ticker", "tipo"], as_index=False)
          .tail(1)
          .copy()
    )

    prev_rows = (
        df.sort_values("data")
          .groupby(["ticker", "tipo"])
          .nth(-2)
          .reset_index()
          .copy()
    )

    last_rows = last_rows.rename(columns={
        "data": "ult_data",
        "quantidade": "ult_qtd",
        "unitario": "ult_unit",
        "total": "ult_total",
        "id": "ult_id",
    })

    prev_rows = prev_rows.rename(columns={
        "data": "ant_data",
        "quantidade": "ant_qtd",
        "unitario": "ant_unit",
        "total": "ant_total",
        "id": "ant_id",
    })

    ins = last_rows.merge(prev_rows, on=["ticker", "tipo"], how="left")

    ins["tem_anterior"] = ~ins["ant_data"].isna()

    ins["delta_qtd"] = ins["ult_qtd"] - ins["ant_qtd"]
    ins["delta_unit"] = ins["ult_unit"] - ins["ant_unit"]
    ins["delta_total"] = ins["ult_total"] - ins["ant_total"]

    ins["efeito_qtd"] = ins["delta_qtd"] * ins["ult_unit"]
    ins["efeito_provento"] = ins["delta_unit"] * ins["ant_qtd"]

    today = pd.Timestamp(date.today())
    ins["dias_sem_pagar"] = (today - ins["ult_data"]).dt.days

    ins["queda_unit"] = ins["tem_anterior"] & (ins["delta_unit"] < 0)
    ins["queda_total"] = ins["tem_anterior"] & (ins["delta_total"] < 0)

    ins["classe_evento"] = ins["tipo"].apply(classify_event)

    ins["impacto_abs"] = (ins["delta_total"].abs()).fillna(0)

    ins = ins.sort_values(
        by=["queda_unit", "queda_total", "impacto_abs", "dias_sem_pagar"],
        ascending=[False, False, False, False],
    )

    return ins

def make_feed_text(r: pd.Series) -> str:
    header = f"**{r['ticker']}** • **{r['tipo']}** • {r['classe_evento']}\n\n"

    if not bool(r.get("tem_anterior", False)):
        lines = [
            header,
            f"- Último pagamento: **{fmt_date(r['ult_data'])}**",
            f"- Cotas na data: **{fmt_qtd(r['ult_qtd'])}**",
            f"- Provento/cota: **{fmt_unit(r['ult_unit'])}**",
            f"- Total recebido: **{fmt_brl(r['ult_total'])}**",
            "",
            "Sem comparação: não existe registro anterior **do mesmo tipo** para este ativo.",
        ]
        return "\n".join(lines)

    lines = [
        header,
        f"Último: **{fmt_date(r['ult_data'])}** (anterior do mesmo tipo: **{fmt_date(r['ant_data'])}**)",
        "",
        f"- Cotas: **{fmt_qtd(r['ant_qtd'])} → {fmt_qtd(r['ult_qtd'])}**",
        f"- Provento/cota: **{fmt_unit(r['ant_unit'])} → {fmt_unit(r['ult_unit'])}**",
        f"- Total: **{fmt_brl(r['ant_total'])} → {fmt_brl(r['ult_total'])}**",
        "",
        "**Δ explicado (por quê mudou?)**",
        f"- Quantidade (compras/vendas): **{fmt_brl(r['efeito_qtd'])}**",
        f"- Provento (unitário): **{fmt_brl(r['efeito_provento'])}**",
    ]

    if str(r["classe_evento"]).startswith("EXTRA") and bool(r.get("queda_total", False)):
        lines += ["", "⚠️ Observação: este tipo de provento costuma ser **pontual/irregular**; variações grandes são comuns."]

    return "\n".join(lines)

# =========================
# UI
# =========================
st.title("🧠 Investimentos • Feed Inteligente (DB novo)")

with st.sidebar:
    st.header("Filtros")
    so_quedas = st.checkbox("Só quedas de provento/cota (mesmo tipo)", value=False)
    so_quedas_total = st.checkbox("Só quedas de total (mesmo tipo)", value=False)
    max_dias = st.slider("Último pagamento até (dias)", 30, 365, 365)

    st.divider()
    st.caption("Fonte: DB novo (aba 'proventos')")

try:
    raw = read_proventos(SHEET_ID, ABA_PROVENTOS)
    df = normalize_proventos_df(raw)
    ins = build_insights(df)
except Exception as e:
    st.error(f"Erro ao carregar dados: {e}")
    st.stop()

if ins.empty:
    st.info("Nenhum dado de proventos encontrado no DB novo.")
    st.stop()

# filtros
ins = ins[ins["dias_sem_pagar"] <= max_dias]
if so_quedas:
    ins = ins[ins["queda_unit"] == True]
if so_quedas_total:
    ins = ins[ins["queda_total"] == True]

# KPIs
c1, c2, c3, c4 = st.columns(4)
c1.metric("Pares (Ativo x Tipo)", len(ins))
c2.metric("Quedas unitárias", int(ins["queda_unit"].sum()))
c3.metric("Quedas de total", int(ins["queda_total"].sum()))
c4.metric("Silenciosos (>90d)", int((ins["dias_sem_pagar"] > 90).sum()))

st.divider()

with st.expander("📌 Radar (compacto)", expanded=True):
    tb = ins[[
        "ticker", "tipo", "classe_evento",
        "ult_data", "ant_data",
        "ult_qtd", "ant_qtd", "delta_qtd",
        "ult_unit", "ant_unit", "delta_unit",
        "ult_total", "ant_total", "delta_total",
        "dias_sem_pagar"
    ]].copy()

    tb["ult_data"] = pd.to_datetime(tb["ult_data"]).dt.strftime("%d/%m/%Y")
    tb["ant_data"] = pd.to_datetime(tb["ant_data"]).dt.strftime("%d/%m/%Y")

    st.dataframe(tb, use_container_width=True, height=320)

st.divider()

st.subheader("🧠 Feed (explicação humana)")
for _, r in ins.iterrows():
    with st.container(border=True):
        st.markdown(make_feed_text(r))
