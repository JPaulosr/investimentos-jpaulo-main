# -*- coding: utf-8 -*-

import re
import html as html_lib
from datetime import datetime

import pandas as pd
import streamlit as st

from utils.gsheets import load_movimentacoes, load_ativos, load_cotacoes, load_proventos
from utils.core import (
    normalize_master_ativos,
    normalize_cotacoes,
    normalize_proventos,
    compute_positions_from_movs,
    enrich_positions_with_master,
    compute_income_12m,
    attach_income,
)

st.set_page_config(layout="wide", page_title="Carteira", page_icon="📊")

# =========================
# CSS (visual do seu print)
# =========================
st.markdown(
    """
<style>
  .stApp { background:#0E1117; color:#E7EAF0; }
  .block-container { padding-top: 1.4rem; max-width: 1100px; }

  .muted { color:#9AA4B2; font-size:12px; }

  .wrap {
    border-radius: 18px;
    border: 1px solid rgba(255,255,255,.08);
    background: linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.02));
    box-shadow: 0 10px 22px rgba(0,0,0,.35);
    overflow: hidden;
  }

  .head {
    display:grid;
    grid-template-columns: 64px 1.9fr 0.8fr 0.9fr 0.9fr 0.9fr 0.8fr;
    gap: 14px;
    padding: 12px 16px;
    color:#AAB4C3;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: .6px;
    border-bottom: 1px solid rgba(255,255,255,.08);
  }

  .row {
    display:grid;
    grid-template-columns: 64px 1.9fr 0.8fr 0.9fr 0.9fr 0.9fr 0.8fr;
    gap: 14px;
    align-items:center;
    padding: 14px 16px;
    border-top: 1px solid rgba(255,255,255,.06);
  }
  .row:hover { background: rgba(255,255,255,.03); }

  .logo {
    width: 46px; height: 46px;
    border-radius: 14px;
    object-fit: cover;
    background: rgba(255,255,255,.06);
    border: 1px solid rgba(255,255,255,.10);
    display:block;
  }
  .logoBox {
    width:46px; height:46px;
    border-radius: 14px;
    background: rgba(255,255,255,.06);
    border: 1px solid rgba(255,255,255,.10);
  }

  .asset .ticker {
    font-weight: 950;
    font-size: 16px;
    line-height: 1.05;
  }
  .asset .sub {
    margin-top: 2px;
    font-size: 12px;
    color:#9AA4B2;
    display:flex;
    gap:10px;
    align-items:center;
    flex-wrap:wrap;
  }

  .num { font-variant-numeric: tabular-nums; font-weight: 900; }
  .right { text-align:right; }

  .pill {
    display:inline-flex; align-items:center; justify-content:center;
    padding: 6px 10px;
    border-radius: 999px;
    font-size: 12px;
    font-weight: 900;
    background: rgba(255,255,255,.06);
    border: 1px solid rgba(255,255,255,.10);
    white-space: nowrap;
  }
  .pill.green { background: rgba(0,200,83,.12); border-color: rgba(0,200,83,.25); color:#00C853; }
  .pill.red   { background: rgba(255,82,82,.12); border-color: rgba(255,82,82,.25); color:#FF5252; }
  .pill.teal  { background: rgba(3,218,198,.12); border-color: rgba(3,218,198,.25); color:#03DAC6; }
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# Helpers
# =========================
_TAG_RE = re.compile(r"<[^>]+>")

def strip_html(x) -> str:
    s = "" if x is None else str(x)
    s = html_lib.unescape(s)
    s = _TAG_RE.sub("", s)
    return s.strip()

def _to_float_any(v) -> float:
    try:
        if v is None:
            return 0.0
        if isinstance(v, str):
            s = v.replace("R$", "").replace(" ", "")
            s = s.replace(".", "").replace(",", ".")
            return float(s) if s else 0.0
        return float(v)
    except:
        return 0.0

def brl(v) -> str:
    n = _to_float_any(v)
    s = f"{n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def pct(v) -> str:
    try:
        n = float(v or 0.0) * 100.0
    except:
        n = 0.0
    s = f"{n:,.2f}".replace(".", ",")
    return f"{s}%"

def _infer_side(v: str) -> str:
    s = ("" if v is None else str(v)).strip().lower()
    if any(x in s for x in ["compra", "buy", "c"]):
        return "BUY"
    if any(x in s for x in ["venda", "sell", "v"]):
        return "SELL"
    return "OTHER"

def _col_first(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

# =========================
# LOAD
# =========================
@st.cache_data(show_spinner=False)
def load_pipeline():
    movs = load_movimentacoes()
    ativos = load_ativos()
    cot = load_cotacoes()
    prov = load_proventos()

    mst = normalize_master_ativos(ativos)
    quotes = normalize_cotacoes(cot)
    prov_norm = normalize_proventos(prov)

    pos = compute_positions_from_movs(movs)
    df = enrich_positions_with_master(pos, mst, quotes)

    # tickers
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    if "ticker" in mst.columns:
        mst["ticker"] = mst["ticker"].astype(str).str.upper().str.strip()

    # logo_url: força merge do master
    if "logo_url" not in mst.columns:
        mst["logo_url"] = ""
    mst["logo_url"] = mst["logo_url"].fillna("").astype(str).str.strip()

    if "logo_url" not in df.columns:
        df = df.merge(mst[["ticker","logo_url"]].drop_duplicates("ticker"), on="ticker", how="left")
    df["logo_url"] = df["logo_url"].fillna("").astype(str).str.strip()

    # nome/segmento (se existirem)
    if "nome" not in df.columns and "nome" in mst.columns:
        df = df.merge(mst[["ticker","nome"]].drop_duplicates("ticker"), on="ticker", how="left")
    if "segmento" not in df.columns and "segmento" in mst.columns:
        df = df.merge(mst[["ticker","segmento"]].drop_duplicates("ticker"), on="ticker", how="left")

    # proventos 12m
    income12 = compute_income_12m(prov_norm)
    df = attach_income(df, income12)

    # garante numéricos
    for c in ["quantidade","preco_medio","preco_atual","valor_mercado","pl","peso","proventos_12m"]:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # projetado (por enquanto = 12m)
    df["proventos_projetado"] = pd.to_numeric(df.get("proventos_12m", 0.0), errors="coerce").fillna(0.0)

    # agregados de compra/venda (pra abas)
    mv = movs.copy() if isinstance(movs, pd.DataFrame) else pd.DataFrame()
    df_ops = pd.DataFrame(columns=["ticker","qtd_compra","qtd_venda","valor_compra","valor_venda"])

    if not mv.empty:
        mv.columns = [str(c).strip() for c in mv.columns]
        tcol = _col_first(mv, ["ticker","ativo","papel","codigo"])
        ocol = _col_first(mv, ["operacao","op","tipo","movimento","side","acao","ação"])
        qcol = _col_first(mv, ["quantidade","qtd","qty"])
        vcol = _col_first(mv, ["valor","valor_total","total","financeiro","valor_rs"])
        pcol = _col_first(mv, ["preco","preco_unit","preco_unitario","preço"])

        if tcol:
            mv[tcol] = mv[tcol].astype(str).str.upper().str.strip()
        if ocol:
            mv["_side"] = mv[ocol].apply(_infer_side)
        else:
            mv["_side"] = "OTHER"

        mv["_q"] = pd.to_numeric(mv[qcol], errors="coerce").fillna(0.0) if qcol else 0.0
        if vcol:
            mv["_v"] = pd.to_numeric(mv[vcol], errors="coerce").fillna(0.0)
        else:
            if pcol:
                mv["_p"] = pd.to_numeric(mv[pcol], errors="coerce").fillna(0.0)
                mv["_v"] = mv["_p"] * mv["_q"]
            else:
                mv["_v"] = 0.0

        if tcol:
            g = mv.groupby([tcol, "_side"])[["_q","_v"]].sum().reset_index()
            buy = g[g["_side"]=="BUY"].rename(columns={tcol:"ticker","_q":"qtd_compra","_v":"valor_compra"})[["ticker","qtd_compra","valor_compra"]]
            sell = g[g["_side"]=="SELL"].rename(columns={tcol:"ticker","_q":"qtd_venda","_v":"valor_venda"})[["ticker","qtd_venda","valor_venda"]]
            df_ops = buy.merge(sell, on="ticker", how="outer").fillna(0.0)

    return df, df_ops

df, df_ops = load_pipeline()

# =========================
# UI
# =========================
st.markdown("## 🧾 Posições Detalhadas")
st.markdown("<p class='muted'>Logo + Ticker ficam juntos no mesmo bloco. Colunas extras ficam nas abas.</p>", unsafe_allow_html=True)

# filtros
f1, f2 = st.columns([2,2])
with f1:
    search = (st.text_input("Buscar Ativo", placeholder="Ex: HGLG11") or "").upper().strip()
with f2:
    opts = ["Todas"] + sorted([x for x in df["classe"].astype(str).unique().tolist() if x and x != "nan"]) if "classe" in df.columns else ["Todas"]
    cls = st.selectbox("Filtrar por Classe", opts)

df_show = df.copy()
if search:
    df_show = df_show[df_show["ticker"].astype(str).str.contains(search, na=False)]
if cls != "Todas" and "classe" in df_show.columns:
    df_show = df_show[df_show["classe"] == cls]

df_show = df_show.sort_values("valor_mercado", ascending=False)

# =========
# ABAS (colunas extras)
# =========
tab_main, tab_ops, tab_prov, tab_adv = st.tabs(["📌 Principal", "🧾 Operações", "💰 Proventos", "⚙️ Avançado"])

# =========================
# TAB PRINCIPAL (visual do print)
# =========================
with tab_main:
    st.markdown(
        """
<div class="wrap">
  <div class="head">
    <div></div>
    <div>Ativo</div>
    <div class="right">Quant.</div>
    <div class="right">Preço Médio</div>
    <div class="right">Preço Atual</div>
    <div class="right">Saldo</div>
    <div class="right">% Carteira</div>
  </div>
""",
        unsafe_allow_html=True,
    )

    for _, r in df_show.iterrows():
        ticker = strip_html(r.get("ticker","")).upper()
        classe = strip_html(r.get("classe",""))
        pl = float(r.get("pl", 0.0) or 0.0)
        qtd = float(r.get("quantidade", 0.0) or 0.0)
        pm = float(r.get("preco_medio", 0.0) or 0.0)
        pa = float(r.get("preco_atual", 0.0) or 0.0)
        saldo = float(r.get("valor_mercado", 0.0) or 0.0)
        peso = float(r.get("peso", 0.0) or 0.0)

        pill = "green" if pl >= 0 else "red"
        pl_txt = f"+ {brl(pl)}" if pl >= 0 else f"{brl(pl)}"

        logo = str(r.get("logo_url","") or "").strip()
        logo_html = f'<img class="logo" src="{logo}"/>' if logo.startswith("http") else '<div class="logoBox"></div>'

        st.markdown(
            f"""
  <div class="row">
    <div>{logo_html}</div>

    <div class="asset">
      <div class="ticker">{ticker}</div>
      <div class="sub">
        <span>{classe}</span>
        <span class="pill {pill}">{pl_txt}</span>
      </div>
    </div>

    <div class="right num">{qtd:,.0f}</div>
    <div class="right num">{brl(pm)}</div>
    <div class="right num">{brl(pa)}</div>
    <div class="right num">{brl(saldo)}</div>
    <div class="right"><span class="pill teal">{pct(peso)}</span></div>
  </div>
""",
            unsafe_allow_html=True,
        )

    st.markdown("</div>", unsafe_allow_html=True)

# =========================
# TAB OPERAÇÕES (colunas extras)
# =========================
with tab_ops:
    st.markdown("### 🧾 Operações (Compra/Venda)")
    base = df_show[["ticker","classe","quantidade","preco_medio","valor_mercado"]].copy()
    base = base.merge(df_ops, on="ticker", how="left").fillna(0.0)

    # colunas “de planilha” ficam aqui (não polui principal)
    grid = base.rename(columns={
        "ticker":"Ticker",
        "classe":"Classe",
        "quantidade":"Quantidade (Líquida)",
        "preco_medio":"Preço Médio Ajustado (R$)",
        "qtd_compra":"Quantidade (Compra)",
        "qtd_venda":"Quantidade (Venda)",
        "valor_compra":"Valor (Compra R$)",
        "valor_venda":"Valor (Venda R$)",
        "valor_mercado":"Valor Atual",
    })

    st.dataframe(grid, use_container_width=True, hide_index=True)

# =========================
# TAB PROVENTOS (projetado)
# =========================
with tab_prov:
    st.markdown("### 💰 Proventos")
    grid = df_show[[
        "ticker","classe","peso","proventos_projetado","proventos_12m"
    ]].copy()

    grid = grid.rename(columns={
        "ticker":"Ticker",
        "classe":"Classe",
        "peso":"% Na Carteira",
        "proventos_projetado":"Proventos (Projetado)",
        "proventos_12m":"Proventos (12m)",
    })

    st.dataframe(grid, use_container_width=True, hide_index=True)

# =========================
# TAB AVANÇADO (campos restantes)
# =========================
with tab_adv:
    st.markdown("### ⚙️ Avançado (campos de controle)")
    st.info("Aqui entram: USD, preço teto, magic number, original classe, etc. Sem estragar a leitura.")
    # placeholders (depois você liga com seus dados reais)
    adv = df_show[["ticker","classe","segmento"]].copy() if "segmento" in df_show.columns else df_show[["ticker","classe"]].copy()
    adv["Preço Médio (US$)"] = 0.0
    adv["Cotação de Hoje (US$)"] = 0.0
    adv["Média de Compra (US$)"] = 0.0
    adv["Magic Number"] = 0.0
    adv["Ação (Preço Teto)"] = 0.0
    adv["Original Classe"] = adv.get("classe","")

    st.dataframe(adv, use_container_width=True, hide_index=True)
