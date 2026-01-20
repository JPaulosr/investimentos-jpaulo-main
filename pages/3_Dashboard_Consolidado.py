# pages/Carteira_Tatica.py
# -*- coding: utf-8 -*-

import re
import html as html_lib
from datetime import datetime
from dateutil.relativedelta import relativedelta
import math

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils.gsheets import load_movimentacoes, load_ativos, load_proventos, load_cotacoes
from utils.core import (
    normalize_master_ativos,
    normalize_proventos,
    normalize_cotacoes,
    compute_positions_from_movs,
    enrich_positions_with_master,
    compute_income_12m,
    attach_income,
    compute_portfolio_metrics,
)

# =========================================================
# CONFIG
# =========================================================
st.set_page_config(layout="wide", page_title="Carteira • Tática", page_icon="📊")

# =========================================================
# CSS PREMIUM
# =========================================================
st.markdown(
    """
<style>
  /* Fundo e Fonte */
  .stApp { background:#0E1117; color:#E7EAF0; font-family: 'Source Sans Pro', sans-serif; }
  .block-container { padding-top: 4rem; padding-bottom: 3rem; max-width: 1400px; }

  /* Cards Gerais */
  .card {
    background-color: #1F2937;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2);
    margin-bottom: 15px;
  }

  /* Header do Ativo */
  .asset-header { display: flex; align-items: center; gap: 20px; }
  .asset-logo { 
    width: 80px; height: 80px; 
    border-radius: 16px; 
    object-fit: cover; 
    border: 2px solid rgba(255,255,255,0.1);
    background: #374151;
  }
  .asset-info { flex: 1; }
  .asset-ticker { font-size: 32px; font-weight: 900; color: #fff; line-height: 1.1; }
  .asset-name { font-size: 16px; color: #9AA4B2; margin-bottom: 8px; }
  
  /* Badges */
  .badge {
    display: inline-flex; align-items: center; padding: 4px 10px;
    border-radius: 6px; font-size: 12px; font-weight: 700;
    margin-right: 6px; border: 1px solid transparent;
  }
  .bg-blue { background: rgba(59, 130, 246, 0.15); color: #60A5FA; border-color: rgba(59, 130, 246, 0.3); }
  .bg-green { background: rgba(16, 185, 129, 0.15); color: #34D399; border-color: rgba(16, 185, 129, 0.3); }
  .bg-gold { background: rgba(245, 158, 11, 0.15); color: #FBBF24; border-color: rgba(245, 158, 11, 0.3); }
  .bg-red { background: rgba(239, 68, 68, 0.15); color: #F87171; border-color: rgba(239, 68, 68, 0.3); }
  .bg-purple { background: rgba(139, 92, 246, 0.15); color: #A78BFA; border-color: rgba(139, 92, 246, 0.3); }

  /* KPI Box (Estilo Quadrado) */
  .kpi-box {
    background: rgba(255,255,255,0.03);
    border-radius: 10px;
    padding: 15px;
    border: 1px solid rgba(255,255,255,0.05);
    text-align: center;
    height: 100%;
  }
  .kpi-label { color: #9AA4B2; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px; }
  .kpi-value { color: #E7EAF0; font-size: 20px; font-weight: 800; }
  .kpi-sub { color: #6B7280; font-size: 11px; margin-top: 2px; }

  /* Insight Cards (Estilo Retangular Colorido) */
  .insight-card {
    border-radius: 10px;
    padding: 15px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    height: 100%;
    border-left: 4px solid;
  }
  .ic-title { font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 5px; opacity: 0.8; }
  .ic-value { font-size: 24px; font-weight: 900; }
  .ic-desc { font-size: 12px; opacity: 0.7; margin-top: 3px; }

  /* Títulos */
  .section-title { font-size: 18px; font-weight: 700; color: #E7EAF0; margin-bottom: 15px; border-left: 4px solid #3B82F6; padding-left: 10px; }

  /* Ajustes de Tabs */
  .stTabs [data-baseweb="tab-list"] { gap: 8px; }
  .stTabs [data-baseweb="tab"] { background-color: #1F2937; border-radius: 8px; padding: 8px 16px; border: 1px solid rgba(255,255,255,0.05); }
  .stTabs [data-baseweb="tab"][aria-selected="true"] { background-color: #3B82F6; color: white; border-color: #3B82F6; }
</style>
""",
    unsafe_allow_html=True,
)

# =========================================================
# Helpers
# =========================================================
_TAG_RE = re.compile(r"<[^>]+>")

def _strip_html(x) -> str:
    s = "" if x is None else str(x)
    s = html_lib.unescape(s)
    s = _TAG_RE.sub("", s)
    return s.strip()

def _to_float(v) -> float:
    try:
        if v is None: return 0.0
        if isinstance(v, (int, float)) and not isinstance(v, bool): return float(v)
        s = str(v).strip()
        if not s or s.lower() == "nan": return 0.0
        s = s.replace("R$", "").replace("%", "").replace(" ", "")
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."): s = s.replace(".", "").replace(",", ".")
            else: s = s.replace(",", "")
        elif "," in s: s = s.replace(",", ".")
        return float(s)
    except: return 0.0

def brl(v) -> str:
    return f"R$ {_to_float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def pct(v) -> str:
    return f"{_to_float(v)*100:,.2f}%".replace(".", ",")

def safe_upper(x) -> str:
    return ("" if x is None else str(x)).upper().strip()

def _pick_date_col(df_: pd.DataFrame):
    for c in ["data_pagamento", "pagamento", "data", "data_com", "data_evento"]:
        if c in df_.columns: return c
    return None

# =========================================================
# LOAD PIPELINE (COM LIMPEZA AGRESSIVA DE TICKERS)
# =========================================================
@st.cache_data(show_spinner=False)
def load_pipeline():
    movs = load_movimentacoes()
    ativos = load_ativos()
    prov = load_proventos()
    cot = load_cotacoes()

    mst = normalize_master_ativos(ativos)
    prov_norm = normalize_proventos(prov)
    quotes = normalize_cotacoes(cot)

    pos = compute_positions_from_movs(movs)
    df = enrich_positions_with_master(pos, mst, quotes)

    income12 = compute_income_12m(prov_norm)
    df = attach_income(df, income12)

    metrics = compute_portfolio_metrics(df, income12)

    # LIMPEZA CRÍTICA DE TICKERS
    if isinstance(df, pd.DataFrame) and not df.empty and "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

    if isinstance(mst, pd.DataFrame) and not mst.empty:
        if "ticker" in mst.columns: mst["ticker"] = mst["ticker"].astype(str).str.upper().str.strip()
        for c in ["logo_url", "nome", "classe", "segmento", "subtipo"]:
            if c not in mst.columns: mst[c] = ""

    # Limpeza na base de proventos (CORREÇÃO DE DADOS)
    if isinstance(prov_norm, pd.DataFrame) and not prov_norm.empty:
        if "ticker" in prov_norm.columns:
            prov_norm["ticker"] = prov_norm["ticker"].astype(str).str.upper().str.strip()
    
    # Merge metadados
    keep_cols = [c for c in ["ticker", "logo_url", "segmento", "classe", "subtipo", "nome"] if c in mst.columns]
    if isinstance(df, pd.DataFrame) and "ticker" in df.columns and keep_cols:
        df = df.merge(mst[keep_cols].drop_duplicates("ticker"), on="ticker", how="left", suffixes=("", "_mst"))

    # Cleanup numérico
    for c in ["quantidade", "preco_medio", "preco_atual", "valor_mercado", "pl", "peso", "custo_total", "proventos_12m"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # Proteção de Colunas
    for c in ["classe", "segmento", "subtipo", "logo_url", "nome"]:
        if c not in df.columns: df[c] = ""
        df[c] = df[c].fillna("").astype(str)

    return df, prov_norm, metrics, movs 

# =========================================================
# INIT
# =========================================================
with st.spinner("Carregando carteira..."):
    df, prov_norm, metrics, df_movs = load_pipeline()

if df is None or df.empty:
    st.info("Carteira vazia.")
    st.stop()

# =========================================================
# SELETOR DE ATIVO
# =========================================================
def _default_ticker(df_):
    if df_.empty or "ticker" not in df_.columns: return ""
    col_sort = "custo_total" if "custo_total" in df_.columns else "valor_mercado"
    if col_sort in df_.columns:
        return str(df_.loc[df_[col_sort].fillna(0).idxmax(), "ticker"])
    return str(df_.iloc[0]["ticker"])

if "ativo_sel" not in st.session_state:
    st.session_state["ativo_sel"] = safe_upper(_default_ticker(df))

# Barra de filtros superior
c_search, c_filter, c_pick = st.columns([2, 1, 1.5])
with c_search:
    termo = st.text_input("🔍 Buscar Ativo", placeholder="HGLG11, BBAS3...", label_visibility="collapsed").upper().strip()
with c_filter:
    classes = ["Todas"] + sorted([x for x in df["classe"].unique() if x])
    f_classe = st.selectbox("Classe", classes, label_visibility="collapsed")
with c_pick:
    df_show = df.copy()
    if f_classe != "Todas": df_show = df_show[df_show["classe"]==f_classe]
    if termo: df_show = df_show[df_show["ticker"].str.contains(termo, na=False)]
    
    opts = sorted(df_show["ticker"].unique())
    current = st.session_state["ativo_sel"]
    if current not in opts and opts: current = opts[0]
    
    sel = st.selectbox("Selecione", opts, index=opts.index(current) if current in opts else 0, label_visibility="collapsed")
    st.session_state["ativo_sel"] = sel

# =========================================================
# DADOS DO ATIVO
# =========================================================
ticker_sel = st.session_state["ativo_sel"]
row = df[df["ticker"]==ticker_sel].iloc[0]

ticker = row.get("ticker", "UNK")
nome = row.get("nome", "") or ticker
classe = row.get("classe", "")
logo = row.get("logo_url", "")
if logo and logo.startswith("http"):
    logo_html = f'<img src="{logo}" class="asset-logo"/>' 
else:
    logo_html = f'<div class="asset-logo" style="display:flex;align-items:center;justify-content:center;color:#fff;font-weight:bold;">{ticker[:2]}</div>'

# Métricas Financeiras
qtd = _to_float(row.get("quantidade", 0))
pm = _to_float(row.get("preco_medio", 0))
pa = _to_float(row.get("preco_atual", 0))
total_inv = _to_float(row.get("custo_total", 0))
valor_atual = _to_float(row.get("valor_mercado", 0))
pl = _to_float(row.get("pl", 0))
pl_pct = (pl / total_inv) if total_inv > 0 else 0.0
peso = _to_float(row.get("peso", 0))
prov12 = _to_float(row.get("proventos_12m", 0))

# Métricas Calculadas
dy_on_cost = (prov12 / total_inv) if total_inv > 0 else 0.0
dy_atual = (prov12 / valor_atual) if valor_atual > 0 else 0.0
preco_teto = (prov12 / qtd) / 0.06 if qtd > 0 else 0.0 

# Número Mágico
media_mensal_por_cota = (prov12 / 12) / qtd if qtd > 0 else 0
magic_number = math.ceil(pa / media_mensal_por_cota) if media_mensal_por_cota > 0 else 0

# Payback 
payback_anos = (1 / dy_atual) if dy_atual > 0 else 0

# Tempo de Casa
tempo_str = ""
try:
    if not df_movs.empty and "ticker" in df_movs.columns:
        movs_asset = df_movs[df_movs["ticker"].astype(str).str.upper().str.strip() == ticker]
        c_date = _pick_date_col(movs_asset)
        if not movs_asset.empty and c_date:
            dt_primeira = pd.to_datetime(movs_asset[c_date], dayfirst=True, errors="coerce").min()
            if pd.notna(dt_primeira):
                diff = relativedelta(datetime.now(), dt_primeira)
                partes = []
                if diff.years > 0: partes.append(f"{diff.years} ano(s)")
                if diff.months > 0: partes.append(f"{diff.months} mês(es)")
                if not partes: partes.append("recente")
                tempo_str = " • ".join(partes)
except: pass

# =========================================================
# HEADER VISUAL
# =========================================================
if tempo_str:
    st.markdown(f"""
    <div style="display:flex; align-items:center; gap:8px; margin-bottom:10px; background:rgba(59, 130, 246, 0.1); padding:8px 12px; border-radius:8px; border:1px solid rgba(59, 130, 246, 0.2); width:fit-content;">
        <span>🕰️</span> <span style="font-size:13px; font-weight:600; color:#60A5FA;">Você investe neste ativo há {tempo_str}</span>
    </div>
    """, unsafe_allow_html=True)

st.markdown(f"""
<div class="card">
    <div class="asset-header">
        {logo_html}
        <div class="asset-info">
            <div class="asset-ticker">{ticker} <span style="font-size:14px; color:#6B7280; font-weight:400; margin-left:10px;">{classe}</span></div>
            <div class="asset-name">{nome}</div>
            <div style="display:flex; gap:8px; flex-wrap:wrap;">
                 <span class="badge bg-blue">Peso: {pct(peso)}</span>
                 <span class="badge {'bg-green' if pl >=0 else 'bg-red'}">P/L: {brl(pl)} ({pct(pl_pct)})</span>
                 <span class="badge bg-gold">YoC: {pct(dy_on_cost)}</span>
                 <span class="badge bg-purple">Magic Number: {magic_number} cotas</span>
            </div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# =========================================================
# GRID DE KPIS
# =========================================================
k1, k2, k3, k4, k5, k6 = st.columns(6)
with k1:
    st.markdown(f"""<div class="kpi-box"><div class="kpi-label">Quantidade</div><div class="kpi-value">{qtd:g}</div><div class="kpi-sub">Cotas/Ações</div></div>""", unsafe_allow_html=True)
with k2:
    st.markdown(f"""<div class="kpi-box"><div class="kpi-label">Preço Médio</div><div class="kpi-value">{brl(pm).replace('R$ ','')}</div><div class="kpi-sub">Seu custo</div></div>""", unsafe_allow_html=True)
with k3:
    st.markdown(f"""<div class="kpi-box"><div class="kpi-label">Preço Atual</div><div class="kpi-value">{brl(pa).replace('R$ ','')}</div><div class="kpi-sub">Cotação</div></div>""", unsafe_allow_html=True)
with k4:
    st.markdown(f"""<div class="kpi-box"><div class="kpi-label">Investido</div><div class="kpi-value">{brl(total_inv).replace('R$ ','')}</div><div class="kpi-sub">Total aporte</div></div>""", unsafe_allow_html=True)
with k5:
    st.markdown(f"""<div class="kpi-box"><div class="kpi-label">Saldo Bruto</div><div class="kpi-value">{brl(valor_atual).replace('R$ ','')}</div><div class="kpi-sub">Valor mercado</div></div>""", unsafe_allow_html=True)
with k6:
    cor_txt = "#34D399" if pl >= 0 else "#F87171"
    st.markdown(f"""<div class="kpi-box"><div class="kpi-label">Resultado</div><div class="kpi-value" style="color:{cor_txt}">{brl(pl).replace('R$ ','')}</div><div class="kpi-sub">Latente</div></div>""", unsafe_allow_html=True)

st.write("") 

# =========================================================
# ABAS DE ANÁLISE
# =========================================================
tab_prov, tab_analise, tab_dados = st.tabs(["💰 Proventos Avançados", "📊 Análise & Decisão", "🧾 Dados Gerais"])

# ---------------------------------------------------------
# TAB 1: PROVENTOS (COM BLOCOS COLORIDOS E GRÁFICOS)
# ---------------------------------------------------------
with tab_prov:
    # Prepara dados (Filtragem Blindada pelo Ticker Limpo)
    df_prov_full = prov_norm[prov_norm["ticker"] == ticker].copy()
    date_col = _pick_date_col(df_prov_full)
    
    if df_prov_full.empty or not date_col:
        st.warning(f"Sem histórico de proventos encontrado para {ticker}.")
    else:
        df_prov_full[date_col] = pd.to_datetime(df_prov_full[date_col], errors="coerce", dayfirst=True)
        df_prov_full = df_prov_full.dropna(subset=[date_col]).sort_values(date_col)
        df_prov_full["ano"] = df_prov_full[date_col].dt.year
        df_prov_full["valor"] = df_prov_full["valor"].apply(_to_float)
        if "valor_por_cota" in df_prov_full.columns:
            df_prov_full["valor_por_cota"] = df_prov_full["valor_por_cota"].apply(_to_float)
        else:
            df_prov_full["valor_por_cota"] = 0.0

        # --- FILTROS ---
        anos_disp = sorted(df_prov_full["ano"].unique(), reverse=True)
        opcoes = ["Últimos 12 Meses", "Todo o Histórico"] + [str(y) for y in anos_disp]
        
        c_f1, c_f2 = st.columns([3, 1])
        with c_f1:
            filtro_periodo = st.selectbox("📅 Período de Análise", options=opcoes, index=0)

        hoje = datetime.now()
        df_chart = df_prov_full.copy()
        
        if filtro_periodo == "Últimos 12 Meses":
            dt_cort = hoje - relativedelta(months=12)
            df_chart = df_chart[df_chart[date_col] >= dt_cort]
        elif filtro_periodo == "Todo o Histórico":
            pass 
        else: 
            ano_sel = int(filtro_periodo)
            df_chart = df_chart[df_chart["ano"] == ano_sel]

        # --- AGRUPAMENTO MENSAL ---
        df_chart["mes_ref"] = df_chart[date_col].dt.to_period("M").dt.to_timestamp()
        
        def _fmt_mes_br(dt):
            meses = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
            return f"{meses[dt.month-1]}. /{str(dt.year)[-2:]}"

        # Gráfico 1 (Total R$)
        grouped_val = df_chart.groupby("mes_ref")["valor"].sum().reset_index().sort_values("mes_ref")
        grouped_val["mes_label"] = grouped_val["mes_ref"].apply(_fmt_mes_br)

        # Gráfico 2 (Unitário R$)
        grouped_unit = df_chart.groupby("mes_ref")["valor_por_cota"].sum().reset_index().sort_values("mes_ref")
        grouped_unit["mes_label"] = grouped_unit["mes_ref"].apply(_fmt_mes_br)

        # --- KPI BLOCKS (ESTILO SOLICITADO) ---
        total_periodo = df_chart["valor"].sum()
        media_periodo = total_periodo / max(1, len(grouped_val))
        dy_p = (total_periodo/total_inv) if total_inv>0 else 0.0

        st.markdown(f"""
        <div style="display:grid; grid-template-columns: 1fr 1fr 1fr; gap:15px; margin-bottom:20px;">
            <div style="background:#1F2937; border-radius:10px; padding:15px; border-left:4px solid #10B981;">
                <div style="color:#9AA4B2; font-size:11px; font-weight:700; text-transform:uppercase;">Total Recebido</div>
                <div style="color:#E7EAF0; font-size:24px; font-weight:800;">{brl(total_periodo)}</div>
                <div style="color:#10B981; font-size:12px;">No período</div>
            </div>
            <div style="background:#1F2937; border-radius:10px; padding:15px; border-left:4px solid #3B82F6;">
                <div style="color:#9AA4B2; font-size:11px; font-weight:700; text-transform:uppercase;">Média Mensal</div>
                <div style="color:#E7EAF0; font-size:24px; font-weight:800;">{brl(media_periodo)}</div>
                <div style="color:#60A5FA; font-size:12px;">Recorrência estimada</div>
            </div>
            <div style="background:#1F2937; border-radius:10px; padding:15px; border-left:4px solid #F59E0B;">
                <div style="color:#9AA4B2; font-size:11px; font-weight:700; text-transform:uppercase;">Yield no Período</div>
                <div style="color:#E7EAF0; font-size:24px; font-weight:800;">{pct(dy_p)}</div>
                <div style="color:#FBBF24; font-size:12px;">Sobre custo total</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # --- GRÁFICO 1: EVOLUÇÃO TOTAL ---
        st.markdown("#### 📉 Evolução do Pagamento Total (R$)")
        if not grouped_val.empty:
            fig1 = px.bar(grouped_val, x="mes_label", y="valor", text="valor", color_discrete_sequence=["#10B981"])
            fig1.update_traces(texttemplate="R$ %{y:.2f}", textposition="outside", cliponaxis=False)
            max_y = grouped_val["valor"].max() * 1.25
            fig1.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#E7EAF0"), xaxis_title=None, yaxis_title=None,
                yaxis=dict(showgrid=False, range=[0, max_y]), 
                height=300, margin=dict(l=0, r=0, t=30, b=20)
            )
            fig1.update_xaxes(type='category', tickangle=-45) 
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("Sem dados.")

        # --- GRÁFICO 2: HISTÓRICO POR COTA (BARRAS VERDES) ---
        st.markdown("#### 📊 Histórico de Rendimento por Cota (Unitário)")
        st.caption("Visualiza a estabilidade do pagamento unitário ao longo do tempo (agrupado por mês).")
        if not grouped_unit.empty:
            # Barras verdes igual ao pedido (Estilo Foto 3)
            fig2 = px.bar(grouped_unit, x="mes_label", y="valor_por_cota", text="valor_por_cota", color_discrete_sequence=["#22C55E"]) 
            
            fig2.update_traces(
                texttemplate="R$ %{y:.2f}", 
                textposition="outside", 
                cliponaxis=False,
                marker_line_width=0,
                marker_opacity=0.9
            )
            
            max_y_unit = grouped_unit["valor_por_cota"].max() * 1.25
            
            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", 
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#E7EAF0"), 
                xaxis_title=None, 
                yaxis_title=None,
                yaxis=dict(showgrid=False, range=[0, max_y_unit]), 
                height=320, 
                margin=dict(l=0, r=0, t=35, b=50), 
                bargap=0.25
            )
            fig2.update_xaxes(type='category', tickangle=-45) 
            
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("Dados de 'Valor por Cota' não disponíveis.")

# ---------------------------------------------------------
# TAB 2: ANÁLISE & DECISÃO (COM NOVOS INSIGHTS)
# ---------------------------------------------------------
with tab_analise:
    st.markdown("### ⚡ Insights de Decisão")
    
    i1, i2, i3 = st.columns(3)
    
    with i1:
        st.markdown(f"""
        <div class="insight-card" style="background:rgba(245, 158, 11, 0.1); border-color:#F59E0B;">
            <div class="ic-title" style="color:#F59E0B;">✨ Número Mágico</div>
            <div class="ic-value" style="color:#FBBF24;">{magic_number} <span style="font-size:14px; font-weight:600;">cotas</span></div>
            <div class="ic-desc" style="color:#FBBF24;">Para comprar 1 nova cota/mês sem tirar do bolso. (Atual: {int(qtd)})</div>
        </div>
        """, unsafe_allow_html=True)
        
    with i2:
        st.markdown(f"""
        <div class="insight-card" style="background:rgba(59, 130, 246, 0.1); border-color:#3B82F6;">
            <div class="ic-title" style="color:#3B82F6;">⏳ Payback Estimado</div>
            <div class="ic-value" style="color:#60A5FA;">{payback_anos:.1f} <span style="font-size:14px; font-weight:600;">anos</span></div>
            <div class="ic-desc" style="color:#60A5FA;">Tempo para recuperar 100% do capital apenas com dividendos (DY Atual: {pct(dy_atual)})</div>
        </div>
        """, unsafe_allow_html=True)
        
    with i3:
        margem = (preco_teto - pa) / pa if pa > 0 else 0
        cor_m = "#34D399" if margem > 0 else "#F87171"
        st.markdown(f"""
        <div class="insight-card" style="background:rgba(16, 185, 129, 0.1); border-color:#10B981;">
            <div class="ic-title" style="color:#10B981;">🛡️ Preço Teto (Bazin 6%)</div>
            <div class="ic-value" style="color:{cor_m};">{brl(preco_teto)}</div>
            <div class="ic-desc" style="color:{cor_m};">Margem de segurança: {pct(margem)} em relação ao preço atual.</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 🔍 Detalhamento Bazin")
    
    col_det1, col_det2 = st.columns(2)
    with col_det1:
        if pa <= preco_teto:
            st.success(f"✅ **OPORTUNIDADE:** O ativo está sendo negociado abaixo do preço teto de Bazin. (Desconto: {brl(preco_teto - pa)})")
        else:
            st.error(f"❌ **AGUARDAR:** O ativo está acima do preço teto. (Ágio: {brl(pa - preco_teto)})")
    
    with col_det2:
        st.info("O Método Bazin calcula o preço máximo justo para garantir um retorno de dividendos de pelo menos 6% ao ano, baseado na média dos últimos 12 meses.")

# ---------------------------------------------------------
# TAB 3: DADOS GERAIS
# ---------------------------------------------------------
with tab_dados:
    st.markdown("### 📋 Ficha Técnica")
    st.dataframe(
        pd.DataFrame({
            "Métrica": ["Ticker", "Nome", "Classe", "Segmento", "Subtipo", "CNPJ/Info"],
            "Valor": [ticker, nome, classe, row.get("segmento", ""), row.get("subtipo", ""), "Consulte RI"]
        }),
        use_container_width=True, hide_index=True
    )

    st.markdown("### 🧾 Extrato de Operações (Simulado)")
    st.caption("Para ver detalhes de compra e venda, acesse a página 'Movimentações'.")
    op_data = {
        "Ticker": [ticker],
        "Qtd Atual": [qtd],
        "PM": [brl(pm)],
        "Investido": [brl(total_inv)],
        "Resultado": [brl(pl)]
    }
    st.dataframe(pd.DataFrame(op_data), use_container_width=True, hide_index=True)

# Footer
st.markdown("---")
st.caption(f"Sistema Privado MD • Dados consolidados em {datetime.now().strftime('%d/%m/%Y %H:%M')}")