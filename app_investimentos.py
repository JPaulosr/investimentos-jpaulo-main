# app_investimentos.py
# -*- coding: utf-8 -*-

import re
import html as html_lib
import traceback
import locale
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd
import streamlit as st
import plotly.express as px
import streamlit.components.v1 as components

from utils.calendar_engine import build_calendar

# ✅ UM ÚNICO IMPORT (sem duplicar)
from utils.gsheets import (
    load_movimentacoes,
    load_ativos,
    load_proventos,
    load_cotacoes,
    load_proventos_anunciados,
    ensure_proventos_anunciados_tab,   # ✅ garante a aba antes de ler
)

from utils.core import (
    normalize_master_ativos,
    normalize_proventos,
    normalize_cotacoes,
    compute_positions_from_movs,
    enrich_positions_with_master,
    compute_income_12m,
    attach_income,
    compute_portfolio_metrics,
    compute_allocations,
)

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(layout="wide", page_title="Investimentos MD", page_icon="📈")

# =========================
# CSS (Dark Premium - Ajustado para Logos Grandes)
# =========================
st.markdown(
    """
<style>
  /* Fundo geral */
  .stApp { background: #0E1117; color:#E7EAF0; }
  .block-container { padding-top: 1.5rem; padding-bottom: 3rem; max-width: 1400px; }

  /* --- CARTÕES UNIFICADOS --- */
  .dashboard-card {
    background-color: #1F2937;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 20px 24px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2);
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: flex-start;
    position: relative;
    overflow: visible;
  }

  /* Cabeçalhos */
  .kpi-header {
      display: flex; align-items: center; gap: 8px; margin-bottom: 12px;
      color: #9AA4B2; font-size: 14px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;
  }

  /* Valores KPIs */
  .kpi-main-value { font-size: 28px; font-weight: 800; color: #FFFFFF; margin-bottom: 8px; line-height: 1.2; }
  .kpi-context { font-size: 13px; color: #6E7681; display: flex; align-items: center; gap: 6px; }

  /* --- TOP 5 LIST --- */
  .top5-container { display: flex; flex-direction: column; gap: 0px; width: 100%; }
  .top5-item {
      display: flex; justify-content: space-between; align-items: center;
      padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.05);
      gap: 12px;
  }
  .top5-item:last-child { border-bottom: none; }
  .top5-ticker {
      font-weight: 800; color: #E7EAF0; font-size: 16px; letter-spacing: 0.5px;
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 110px;
  }
  .top5-val {
      font-weight: 700; color: #10B981; font-size: 16px;
      white-space: nowrap; flex: 0 0 auto;
  }
  .top5-rank {
      background: linear-gradient(135deg, #FFD700 0%, #B8860B 100%);
      color: #000; font-size: 12px; font-weight: 800;
      width: 24px; height: 24px; border-radius: 6px; display: flex;
      align-items: center; justify-content: center; margin-right: 12px;
      box-shadow: 0 2px 4px rgba(0,0,0,0.3);
      flex: 0 0 auto;
  }
  .top5-item:nth-child(n+2) .top5-rank { background: #374151; color: #9AA4B2; box-shadow: none; }

  /* --- BADGE DE TEMPO --- */
  .time-badge {
      background: rgba(16, 185, 129, 0.1);
      border: 1px solid rgba(16, 185, 129, 0.3);
      color: #10B981;
      padding: 6px 12px;
      border-radius: 20px;
      font-size: 13px;
      font-weight: 600;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      margin-left: 10px;
      vertical-align: middle;
  }

  /* Cores Utilitárias */
  .text-green { color: #10B981 !important; }
  .text-red   { color: #EF4444 !important; }

  /* Ajustes Gerais */
  div[data-testid="stExpander"] { background-color: #1F2937; border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; margin-bottom: 12px; }
  div[data-testid="stExpander"] > details > summary { font-weight: 700; font-size: 15px; color: #E7EAF0; }

  /* --- TABELA DE ATIVOS (CARTÃO) --- */
  .card {
    background: #1F2937;
    border: 1px solid rgba(255,255,255,.08);
    border-radius: 12px;
    overflow-x: auto;
    margin-top: 5px;
    margin-bottom: 5px;
  }

  .row {
    display: grid;
    grid-template-columns: 80px minmax(100px, 1.5fr) repeat(5, minmax(80px, 1fr));
    gap: 12px;
    align-items: center;
    padding: 14px 16px;
    border-top: 1px solid rgba(255,255,255,.06);
    min-width: 650px;
  }

  .row:first-child { border-top: none; }
  .ticker { font-weight: 900; font-size: 16px; white-space: nowrap; }
  .sub { font-size: 12px; color: #9AA4B2; white-space: nowrap; margin-top: 4px; }
  .num { font-variant-numeric: tabular-nums; font-weight: 700; font-size:14px; white-space: nowrap; }

  .pill { display:inline-flex; padding: 4px 8px; border-radius: 6px; font-size: 11px; font-weight: 700; background: rgba(255,255,255,.06); white-space: nowrap; }
  .pill.green { color:#00C853; background: rgba(0,200,83,.12); }
  .pill.red { color:#FF5252; background: rgba(255,82,82,.12); }
  .pill.blue { color:#03DAC6; background: rgba(3,218,198,.12); }

  .logo { width: 70px; height: 70px; border-radius: 12px; object-fit: cover; background: rgba(255,255,255,.06); }
  .right { text-align:right; }
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# HELPERS
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
        if isinstance(v, float) and pd.isna(v):
            return 0.0
        s = str(v).strip()
        if s == "" or s.lower() == "nan":
            return 0.0
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
        s = s.replace("R$", "").replace("%", "").replace(" ", "")
        try:
            return float(s)
        except ValueError:
            if "," in s and "." in s:
                if s.rfind(",") > s.rfind("."):
                    s = s.replace(".", "").replace(",", ".")
                    return float(s)
                s = s.replace(",", "")
                return float(s)
            if "," in s:
                s = s.replace(",", ".")
                return float(s)
            return 0.0
    except Exception:
        return 0.0

def brl(v) -> str:
    n = _to_float_any(v)
    s = f"{n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def pct(v) -> str:
    try:
        n = float(v or 0.0) * 100.0
    except Exception:
        n = 0.0
    s = f"{n:,.2f}".replace(".", ",")
    return f"{s}%"

# =========================
# SIDEBAR COM REFRESH
# =========================
with st.sidebar:
    st.header("Opções")
    if st.button("🔄 Atualizar Dados", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# =========================
# PIPELINE (cache)
# =========================
@st.cache_data(show_spinner=False)
def load_data_pipeline():
    movs = load_movimentacoes()
    ativos = load_ativos()
    prov_raw = load_proventos()
    cot = load_cotacoes()

        # ✅ GARANTE A ABA ANTES DE LER (evita erro vermelho)
    try:
        ensure_proventos_anunciados_tab()
    except Exception:
        pass

    # ✅ SEMPRE define prov_anunciados (mesmo se falhar)
    try:
        prov_anunciados = load_proventos_anunciados()
    except Exception:
        prov_anunciados = pd.DataFrame()

    # ✅ NORMALIZA proventos_anunciados (para o calendário integrar)
    if isinstance(prov_anunciados, pd.DataFrame) and not prov_anunciados.empty:

        if "ticker" in prov_anunciados.columns:
            prov_anunciados["ticker"] = (
                prov_anunciados["ticker"].fillna("").astype(str).str.upper().str.strip()
            )

        for c in ["data_com", "data_pagamento", "capturado_em"]:
            if c in prov_anunciados.columns:
                prov_anunciados[c] = pd.to_datetime(
                    prov_anunciados[c], errors="coerce", dayfirst=True
                )

        if "valor_por_cota" in prov_anunciados.columns:
            prov_anunciados["valor_por_cota"] = prov_anunciados["valor_por_cota"].apply(_to_float_any)

        if "quantidade_ref" in prov_anunciados.columns:
            prov_anunciados["quantidade_ref"] = prov_anunciados["quantidade_ref"].apply(_to_float_any)

        if "status" in prov_anunciados.columns:
            prov_anunciados["status"] = (
                prov_anunciados["status"].fillna("").astype(str).str.upper().str.strip()
            )

        if "tipo_pagamento" in prov_anunciados.columns:
            prov_anunciados["tipo_pagamento"] = (
                prov_anunciados["tipo_pagamento"].fillna("").astype(str).str.upper().str.strip()
            )


    if isinstance(prov_raw, pd.DataFrame) and not prov_raw.empty:
        if "valor" in prov_raw.columns:
            prov_raw["valor"] = prov_raw["valor"].apply(_to_float_any)
        if "valor_por_cota" in prov_raw.columns:
            prov_raw["valor_por_cota"] = prov_raw["valor_por_cota"].apply(_to_float_any)
        if "ticker" in prov_raw.columns:
            prov_raw["ticker"] = prov_raw["ticker"].astype(str).str.upper().str.strip()

    if isinstance(movs, pd.DataFrame) and not movs.empty:
        if "ticker" in movs.columns:
            movs["ticker"] = movs["ticker"].astype(str).str.upper().str.strip()
        for col in ["quantidade", "preco_unitario", "taxa", "valor_total"]:
            if col in movs.columns:
                movs[col] = movs[col].apply(_to_float_any)

    mst = normalize_master_ativos(ativos)
    if isinstance(mst, pd.DataFrame) and not mst.empty and "ticker" in mst.columns:
        mst["ticker"] = mst["ticker"].astype(str).str.upper().str.strip()

    prov_norm = normalize_proventos(prov_raw.copy() if isinstance(prov_raw, pd.DataFrame) else prov_raw)
    if isinstance(prov_norm, pd.DataFrame) and not prov_norm.empty:
        if "valor" in prov_norm.columns:
            prov_norm["valor"] = prov_norm["valor"].apply(_to_float_any)
        if "ticker" in prov_norm.columns:
            prov_norm["ticker"] = prov_norm["ticker"].astype(str).str.upper().str.strip()

    quotes = normalize_cotacoes(cot)

    pos_calc = compute_positions_from_movs(movs)
    enriched = enrich_positions_with_master(pos_calc, mst, quotes)

    income12 = compute_income_12m(prov_norm)
    enriched = attach_income(enriched, income12)
    metrics = compute_portfolio_metrics(enriched, income12)

    return enriched, metrics, prov_raw, prov_norm, income12, mst, movs, prov_anunciados

# =========================
# LOAD
# =========================
try:
    with st.spinner("Sincronizando dados..."):
        df, metrics, df_prov_raw, df_proventos, income12, mst, df_movs, df_prov_anunciados = load_data_pipeline()
except Exception:
    st.error("❌ Erro ao carregar dados.")
    st.code(traceback.format_exc())
    st.stop()

# =========================
# EXTRAS
# =========================
tempo_investindo_str = ""
if isinstance(df_movs, pd.DataFrame) and not df_movs.empty and "data" in df_movs.columns:
    try:
        dt_series = pd.to_datetime(df_movs["data"], errors="coerce", dayfirst=True).dropna()
        if not dt_series.empty:
            primeira_compra = dt_series.min()
            hoje = datetime.now()
            diff = relativedelta(hoje, primeira_compra)
            texto = (
                f"{diff.years} ano(s) e {diff.months} mes(es)"
                if diff.years > 0
                else f"{diff.months} mes(es)"
            )
            tempo_investindo_str = f"<span class='time-badge'>🕒 {texto} investindo</span>"
    except Exception:
        pass

top5_html = ""
if isinstance(df_prov_raw, pd.DataFrame) and not df_prov_raw.empty:
    try:
        tmp = df_prov_raw.copy()
        if "ticker" in tmp.columns:
            tmp["ticker"] = tmp["ticker"].astype(str).str.upper().str.strip()
        if "valor" in tmp.columns:
            tmp["valor"] = tmp["valor"].apply(_to_float_any)

        if "ticker" in tmp.columns and "valor" in tmp.columns:
            ranking = tmp.groupby("ticker")["valor"].sum().sort_values(ascending=False).head(5)

            items = []
            for idx, (ticker, val) in enumerate(ranking.items(), 1):
                items.append(
                    f"<div class='top5-item'>"
                    f"  <div style='display:flex; align-items:center;'>"
                    f"    <div class='top5-rank'>{idx}</div>"
                    f"    <div class='top5-ticker'>{ticker}</div>"
                    f"  </div>"
                    f"  <div class='top5-val'>+ {brl(val)}</div>"
                    f"</div>"
                )
            top5_html = "<div class='top5-container'>" + "".join(items) + "</div>"
    except Exception:
        top5_html = ""

# =========================
# HEADER
# =========================
st.markdown(f"## 📈 **Investimentos MD** {tempo_investindo_str}", unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

# ABA CARTEIRA REMOVIDA, AGORA É TUDO NO RESUMO
tab_resumo, tab_calendario = st.tabs(["🏠 Visão Geral", "📅 Calendário"])

# =========================
# TAB 1: VISÃO GERAL (RESUMO + ATIVOS)
# =========================
with tab_resumo:
    st.markdown("<br>", unsafe_allow_html=True)

    # --- LINHA 1: KPIs ---
    patrimonio = metrics.get("patrimonio_total", 0.0)
    custo = metrics.get("custo_total", 0.0)
    pl_total = metrics.get("pl_total", 0.0)
    rentab = metrics.get("rentab_pct", 0.0)
    prov_12m = metrics.get("proventos_12m", 0.0)

    cor_pl = "text-green" if float(pl_total or 0.0) >= 0 else "text-red"
    sinal = "+" if float(pl_total or 0.0) >= 0 else ""

    k1, k2, k3, k4 = st.columns(4)

    with k1:
        st.markdown(
            f"""
        <div class="dashboard-card">
            <div class="kpi-header">🏛️ Patrimônio</div>
            <div class="kpi-main-value">{brl(patrimonio)}</div>
            <div class="kpi-context"><span class="{cor_pl}" style="font-weight:700;">{sinal}{pct(rentab)}</span> &nbsp;rentabilidade</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with k2:
        st.markdown(
            f"""
        <div class="dashboard-card">
            <div class="kpi-header">💰 Investido</div>
            <div class="kpi-main-value">{brl(custo)}</div>
            <div class="kpi-context">Custo de aquisição</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with k3:
        st.markdown(
            f"""
        <div class="dashboard-card">
            <div class="kpi-header">📉 P/L Total</div>
            <div class="kpi-main-value {cor_pl}">{sinal}{brl(pl_total)}</div>
            <div class="kpi-context">Ganho/Perda Capital</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with k4:
        st.markdown(
            f"""
        <div class="dashboard-card">
            <div class="kpi-header">💵 Proventos (12m)</div>
            <div class="kpi-main-value text-green">{brl(prov_12m)}</div>
            <div class="kpi-context">Últimos 12 meses</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # --- LINHA 2: GRÁFICOS ---
    c_main1, c_main2 = st.columns([1, 2])

    # Alocação
    with c_main1:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.markdown('<div class="kpi-header">🍰 Alocação Atual</div>', unsafe_allow_html=True)

        alloc, _ = compute_allocations(df)
        if alloc is not None and not alloc.empty:
            fig_pie = px.pie(
                alloc,
                values="valor_mercado",
                names="classe",
                hole=0.6,
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            fig_pie.update_traces(
                textposition="outside",
                textinfo="percent+label",
                marker=dict(line=dict(color="#1F2937", width=2)),
            )
            fig_pie.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#E7EAF0"),
                showlegend=False,
                margin=dict(t=0, b=0, l=10, r=10),
                height=250,
            )
            # KEY UNICA CORRIGIDA
            st.plotly_chart(fig_pie, use_container_width=True, key="grafico_alocacao_pizza_final")
        else:
            st.info("Sem dados de alocação.")

        st.markdown("</div>", unsafe_allow_html=True)

    # Evolução de Proventos
    with c_main2:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)

        if isinstance(df_prov_raw, pd.DataFrame) and not df_prov_raw.empty:
            df_prov_chart = df_prov_raw.copy()
            if "ticker" in df_prov_chart.columns:
                df_prov_chart["ticker"] = df_prov_chart["ticker"].astype(str).str.upper().str.strip()

            date_col = None
            for c in ["data_pagamento", "pagamento", "data", "data_com"]:
                if c in df_prov_chart.columns:
                    date_col = c
                    break

            if not date_col:
                st.error("Coluna de data não encontrada na base de proventos.")
            else:
                df_prov_chart[date_col] = pd.to_datetime(df_prov_chart[date_col], errors="coerce", dayfirst=True)
                df_prov_chart = df_prov_chart.dropna(subset=[date_col])

                if "valor" in df_prov_chart.columns:
                    df_prov_chart["valor"] = df_prov_chart["valor"].apply(_to_float_any)
                else:
                    df_prov_chart["valor"] = 0.0

                if "classe" not in df_prov_chart.columns and isinstance(mst, pd.DataFrame) and not mst.empty:
                    tmp_mst = mst.copy()
                    if "ticker" in tmp_mst.columns:
                        tmp_mst["ticker"] = tmp_mst["ticker"].astype(str).str.upper().str.strip()
                    if "classe" in tmp_mst.columns and "ticker" in tmp_mst.columns:
                        df_prov_chart = df_prov_chart.merge(
                            tmp_mst[["ticker", "classe"]].drop_duplicates("ticker"),
                            on="ticker",
                            how="left",
                        )

                c_tit, c_sel1, c_sel2 = st.columns([1.5, 0.8, 0.8])
                with c_tit:
                    st.markdown('<div class="kpi-header">📊 Evolução de Proventos</div>', unsafe_allow_html=True)

                anos_unicos = sorted(df_prov_chart[date_col].dt.year.dropna().unique(), reverse=True)
                anos_str = [str(int(y)) for y in anos_unicos]
                opts_periodo = ["Últimos 12 Meses"] + anos_str + ["Todo o Histórico"]

                opts_classe = ["Todos"]
                if "classe" in df_prov_chart.columns:
                    df_prov_chart["classe"] = df_prov_chart["classe"].astype(str).str.strip()
                    opts_classe += sorted(df_prov_chart["classe"].dropna().unique().tolist())

                with c_sel1:
                    # KEY UNICA CORRIGIDA
                    sel_periodo = st.selectbox(
                        "Período", opts_periodo, index=0, key="sel_per_grafico_final", label_visibility="collapsed"
                    )
                with c_sel2:
                    # KEY UNICA CORRIGIDA
                    sel_classe = st.selectbox(
                        "Tipo", opts_classe, index=0, key="sel_cls_grafico_final", label_visibility="collapsed"
                    )

                df_final = df_prov_chart.copy()

                if sel_periodo == "Últimos 12 Meses":
                    dt_ini = hoje - relativedelta(months=12)
                    df_final = df_final[(df_final[date_col] >= dt_ini) & (df_final[date_col] <= hoje)]
                    start_m = pd.Timestamp(dt_ini.year, dt_ini.month, 1)
                    end_m = pd.Timestamp(hoje.year, hoje.month, 1)
                    meses_full = pd.date_range(start=start_m, end=end_m, freq="MS")
                elif sel_periodo == "Todo o Histórico":
                    if df_final.empty:
                        meses_full = pd.DatetimeIndex([])
                    else:
                        mn = df_final[date_col].min()
                        mx = df_final[date_col].max()
                        start_m = pd.Timestamp(mn.year, mn.month, 1)
                        end_m = pd.Timestamp(mx.year, mx.month, 1)
                        meses_full = pd.date_range(start=start_m, end=end_m, freq="MS")
                else:
                    try:
                        ano = int(sel_periodo)
                        df_final = df_final[df_final[date_col].dt.year == ano]
                        meses_full = pd.date_range(
                            start=pd.Timestamp(ano, 1, 1), end=pd.Timestamp(ano, 12, 1), freq="MS"
                        )
                    except Exception:
                        meses_full = pd.DatetimeIndex([])

                if sel_classe != "Todos" and "classe" in df_final.columns:
                    df_final = df_final[df_final["classe"] == sel_classe]

                if not df_final.empty:
                    df_final["mes_ref"] = df_final[date_col].dt.to_period("M").dt.to_timestamp()
                    df_grouped = (
                        df_final.groupby("mes_ref")["valor"]
                        .sum()
                        .reindex(meses_full, fill_value=0.0)
                        .reset_index()
                    )
                    df_grouped.columns = ["mes_ref", "valor"]
                else:
                    df_grouped = pd.DataFrame({"mes_ref": meses_full, "valor": [0.0] * len(meses_full)})

                df_grouped["mes_label"] = df_grouped["mes_ref"].dt.strftime("%b %Y")
                category_order = df_grouped["mes_label"].tolist()

                fig_evol = px.bar(
                    df_grouped,
                    x="mes_label",
                    y="valor",
                    text="valor",
                    category_orders={"mes_label": category_order},
                )
                fig_evol.update_traces(
                    texttemplate="R$ %{y:.0f}",
                    textposition="outside",
                    marker_color="#10B981",
                    cliponaxis=False,
                )
                maxy = float(df_grouped["valor"].max() or 0.0)
                fig_evol.update_yaxes(range=[0, maxy * 1.18 if maxy > 0 else 1])
                fig_evol.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#E7EAF0"),
                    xaxis_title=None,
                    yaxis_title=None,
                    margin=dict(t=40, b=20, l=0, r=0),
                    height=220,
                    yaxis=dict(showgrid=False),
                )
                # KEY UNICA CORRIGIDA
                st.plotly_chart(fig_evol, use_container_width=True, key="grafico_evolucao_final")
        else:
            st.info("Sem dados de proventos.")

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- LINHA 3: SETORES ---
    c_sec1, c_sec2 = st.columns(2)

    with c_sec1:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.markdown('<div class="kpi-header">🏢 Ações por Setor</div>', unsafe_allow_html=True)
        if isinstance(df, pd.DataFrame) and not df.empty and "segmento" in df.columns and "classe" in df.columns:
            df_acoes = df[df["classe"].astype(str).str.upper().isin(["AÇÃO", "ACAO", "ACOES", "STOCKS", "BDR"])]
            if not df_acoes.empty:
                df_setor = (
                    df_acoes.groupby("segmento")["valor_mercado"]
                    .sum()
                    .reset_index()
                    .sort_values("valor_mercado", ascending=False)
                    .head(7)
                )
                fig_setor = px.bar(df_setor, x="segmento", y="valor_mercado", text="valor_mercado")
                fig_setor.update_traces(texttemplate="R$ %{y:.2s}", marker_color="#EF4444")
                fig_setor.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#E7EAF0"),
                    xaxis_title=None,
                    yaxis_title=None,
                    margin=dict(t=10, b=10, l=0, r=0),
                    height=250,
                )
                # KEY UNICA CORRIGIDA
                st.plotly_chart(fig_setor, use_container_width=True, key="grafico_setor_acoes_final")
            else:
                st.caption("Sem dados de Ações.")
        st.markdown("</div>", unsafe_allow_html=True)

    with c_sec2:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.markdown('<div class="kpi-header">🏗️ Fundos por Segmento</div>', unsafe_allow_html=True)
        if isinstance(df, pd.DataFrame) and not df.empty and "segmento" in df.columns and "classe" in df.columns:
            df_fiis = df[df["classe"].astype(str).str.upper().isin(["FII", "FIAGRO", "FUNDO"])]
            if not df_fiis.empty:
                df_seg = (
                    df_fiis.groupby("segmento")["valor_mercado"]
                    .sum()
                    .reset_index()
                    .sort_values("valor_mercado", ascending=False)
                    .head(7)
                )
                fig_seg = px.bar(df_seg, x="segmento", y="valor_mercado", text="valor_mercado")
                fig_seg.update_traces(texttemplate="R$ %{y:.2s}", marker_color="#3B82F6")
                fig_seg.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#E7EAF0"),
                    xaxis_title=None,
                    yaxis_title=None,
                    margin=dict(t=10, b=10, l=0, r=0),
                    height=250,
                )
                # KEY UNICA CORRIGIDA
                st.plotly_chart(fig_seg, use_container_width=True, key="grafico_segmento_fiis_final")
            else:
                st.caption("Sem dados de Fundos.")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- LINHA 4: RANKINGS ---
    c_rank1, c_rank2 = st.columns([1, 2])

    with c_rank1:
        st.markdown(
            f"""
        <div class="dashboard-card" style="justify-content: flex-start;">
            <div class="kpi-header" style="color:#FFD700;">🏆 Top 5 Pagadores</div>
            {top5_html if top5_html else "<span style='color:#666'>Sem dados</span>"}
        </div>
        """,
            unsafe_allow_html=True,
        )

    with c_rank2:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.subheader("Ranking Proventos (12m)")
        if income12 is not None and not income12.empty and "ticker" in income12.columns:
            tmp = income12.copy()
            tmp["ticker"] = tmp["ticker"].astype(str).str.upper().str.strip()
            if "proventos_12m" in tmp.columns:
                tmp["proventos_12m"] = pd.to_numeric(tmp["proventos_12m"], errors="coerce").fillna(0.0)
            top_prov = tmp.sort_values("proventos_12m", ascending=False).head(10)
            fig_bar = px.bar(
                top_prov,
                x="ticker",
                y="proventos_12m",
                text="proventos_12m",
                color="proventos_12m",
                color_continuous_scale="Teal",
            )
            fig_bar.update_traces(texttemplate="R$ %{y:.0f}", textposition="outside", cliponaxis=False)
            maxy2 = float(top_prov["proventos_12m"].max() or 0.0)
            fig_bar.update_yaxes(range=[0, maxy2 * 1.18 if maxy2 > 0 else 1])
            fig_bar.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#E7EAF0"),
                xaxis_title=None,
                yaxis_title=None,
                coloraxis_showscale=False,
                margin=dict(t=25, b=0, l=0, r=0),
                height=300,
                yaxis=dict(showgrid=False, showticklabels=False),
            )
            # KEY UNICA CORRIGIDA
            st.plotly_chart(fig_bar, use_container_width=True, key="grafico_ranking_pagadores_final")
        else:
            st.info("Sem proventos 12m para exibir.")
        st.markdown("</div>", unsafe_allow_html=True)

    # =========================
    # SEÇÃO DETALHAMENTO DE ATIVOS (ANTIGA CARTEIRA)
    # =========================
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown("---")
    st.header("📂 Detalhamento de Ativos")
    st.markdown("<br>", unsafe_allow_html=True)

    f1, f2 = st.columns([2, 2])

    with f1:
        search = (st.text_input("🔍 Buscar Ativo", placeholder="Ex: HGLG11") or "").upper().strip()

    with f2:
        opts = ["Todas"]
        if isinstance(df, pd.DataFrame) and not df.empty and "classe" in df.columns:
            opts += sorted([x for x in df["classe"].astype(str).unique().tolist() if x and x != "nan"])
        cls_filter = st.selectbox("Filtrar por Classe", opts)

    df_show = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if not df_show.empty:
        # Filtros Globais
        if search:
            df_show = df_show[df_show["ticker"].astype(str).str.contains(search, na=False)]
        if cls_filter != "Todas" and "classe" in df_show.columns:
            df_show = df_show[df_show["classe"].astype(str) == cls_filter]

        if isinstance(mst, pd.DataFrame) and not mst.empty and "logo_url" in mst.columns:
            df_show = df_show.merge(
                mst[["ticker", "logo_url"]].drop_duplicates("ticker"),
                on="ticker",
                how="left",
            )
        else:
            df_show["logo_url"] = ""

        # Conversão numérica
        for c in ["quantidade", "preco_medio", "preco_atual", "valor_mercado", "pl", "peso"]:
            if c in df_show.columns:
                df_show[c] = pd.to_numeric(df_show[c], errors="coerce").fillna(0.0)

    # Lógica de Agrupamento por Classe (Estilo Investidor10)
    if df_show is None or df_show.empty:
        st.info("Sem posições para exibir.")
    else:
        # Cabeçalho da tabela (HTML)
        html_header = """
        <div class="card" style="margin-top:0px; margin-bottom:5px; border-radius:8px; background: rgba(0,0,0,0.2);">
          <div class="row" style="font-weight:600; color:#9AA4B2; font-size:13px; border-bottom:1px solid rgba(255,255,255,0.08);">
            <div></div>
            <div>ATIVO</div>
            <div class="right">QUANT.</div>
            <div class="right">P. MÉDIO</div>
            <div class="right">P. ATUAL</div>
            <div class="right">SALDO</div>
            <div class="right">% CART.</div>
          </div>
        </div>
        """

        classes_presentes = sorted(df_show["classe"].astype(str).unique().tolist())
        
        for classe_name in classes_presentes:
            if classe_name == "nan" or classe_name == "":
                label_cls = "OUTROS"
            else:
                label_cls = classe_name

            df_sub = df_show[df_show["classe"].astype(str) == classe_name]
            if df_sub.empty:
                continue
                
            val_total_sub = df_sub["valor_mercado"].sum()
            count_sub = len(df_sub)
            expander_label = f"{label_cls} ({count_sub})  •  Total: {brl(val_total_sub)}"
            
            with st.expander(expander_label, expanded=True):
                st.markdown(html_header, unsafe_allow_html=True)
                st.markdown('<div class="card" style="margin-top:0px; border-top-left-radius:0; border-top-right-radius:0;">', unsafe_allow_html=True)
                
                total_carteira = df["valor_mercado"].sum() # Percentual global

                for _, r in df_sub.sort_values("valor_mercado", ascending=False).iterrows():
                    ticker = str(r.get("ticker", "")).upper().strip()
                    qtd = float(r.get("quantidade", 0))
                    pm = float(r.get("preco_medio", 0))
                    pa = float(r.get("preco_atual", 0))
                    saldo = float(r.get("valor_mercado", 0))
                    peso = (saldo / total_carteira) if total_carteira > 0 else 0.0

                    logo = str(r.get("logo_url", "") or "").strip()
                    logo_html = f'<img class="logo" src="{logo}"/>' if logo else '<div class="logo"></div>'

                    pl = float(r.get("pl", 0))
                    pill_pl = "green" if pl >= 0 else "red"
                    pl_txt = f"+ {brl(pl)}" if pl >= 0 else f"{brl(pl)}"

                    st.markdown(
                        f"""
                        <div class="row">
                          <div>{logo_html}</div>
                          <div>
                            <div class="ticker">{ticker}</div>
                            <div class="sub">{label_cls} • <span class="pill {pill_pl}">{pl_txt}</span></div>
                          </div>
                          <div class="right num">{qtd:,.0f}</div>
                          <div class="right num">{brl(pm)}</div>
                          <div class="right num">{brl(pa)}</div>
                          <div class="right num">{brl(saldo)}</div>
                          <div class="right"><span class="pill blue">{pct(peso)}</span></div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                st.markdown("</div>", unsafe_allow_html=True)

# =========================
# TAB 2: CALENDÁRIO (CORRIGIDO: CARDS EM COLUNAS + ORDENAÇÃO + FILTRO ATIVO FUNCIONANDO)
# =========================
with tab_calendario:
    st.markdown("<br>", unsafe_allow_html=True)
    c1, c2 = st.columns([3, 1])
    c1.subheader("📅 Calendário de Eventos")

    # Toggle com chave única
    mostrar_estimativas = c2.toggle("Estimativas", value=True, key="toggle_estimativas_cal")

    # --- FILTRO DE PERÍODO + ATIVO ---
    cols_filter = st.columns([2, 1.2, 0.8])  # período | ativo | (sobra)

    with cols_filter[0]:
        view_option = st.selectbox(
            "Visualizar Período:",
            ["📅 Mês Atual", "🗓️ Próximos 90 Dias", "➡️ Próximo Mês", "📆 Ano Todo (2026)"],
            index=0,
            key="sel_periodo_cal"
        )

    with cols_filter[1]:
        # lista de tickers (carteira -> master -> proventos)
        tickers_cal = []
        try:
            if isinstance(df, pd.DataFrame) and not df.empty and "ticker" in df.columns:
                tickers_cal = sorted(
                    [t for t in df["ticker"].astype(str).str.upper().str.strip().unique().tolist()
                     if t and t != "NAN"]
                )
        except Exception:
            tickers_cal = []

        if not tickers_cal:
            try:
                if isinstance(mst, pd.DataFrame) and not mst.empty and "ticker" in mst.columns:
                    tickers_cal = sorted(
                        [t for t in mst["ticker"].astype(str).str.upper().str.strip().unique().tolist()
                         if t and t != "NAN"]
                    )
            except Exception:
                pass

        if not tickers_cal:
            try:
                if isinstance(df_prov_raw, pd.DataFrame) and not df_prov_raw.empty and "ticker" in df_prov_raw.columns:
                    tickers_cal = sorted(
                        [t for t in df_prov_raw["ticker"].astype(str).str.upper().str.strip().unique().tolist()
                         if t and t != "NAN"]
                    )
            except Exception:
                pass

        ticker_filter = st.selectbox(
            "Ativo:",
            ["Todos"] + (tickers_cal if tickers_cal else []),
            index=0,
            key="sel_ticker_cal"
        )

    # Helper de filtro no DF do calendário
    def _filtra_ticker_df(_df: pd.DataFrame, col="ticker") -> pd.DataFrame:
        if _df is None or _df.empty:
            return _df
        if ticker_filter == "Todos":
            return _df
        if col not in _df.columns:
            return _df
        tmp = _df.copy()
        tmp[col] = tmp[col].astype(str).str.upper().str.strip()
        return tmp[tmp[col] == str(ticker_filter).upper().strip()].copy()

    # Lógica de datas
    hoje = datetime.now()
    hoje_date = hoje.date()
    ano_atual = hoje.year
    mes_atual = hoje.month

    if view_option == "📅 Mês Atual":
        start_date = datetime(ano_atual, mes_atual, 1)
        next_month = start_date + relativedelta(months=1)
        end_date = next_month - timedelta(days=1)

    elif view_option == "➡️ Próximo Mês":
        start_date = (datetime(ano_atual, mes_atual, 1) + relativedelta(months=1))
        next_month = start_date + relativedelta(months=1)
        end_date = next_month - timedelta(days=1)

    elif view_option == "🗓️ Próximos 90 Dias":
        start_date = hoje
        end_date = hoje + timedelta(days=90)

    else:  # Ano Todo
        start_date = datetime(ano_atual, 1, 1)
        end_date = datetime(ano_atual, 12, 31)

    # --- INICIALIZAÇÃO DE SEGURANÇA ---
    final_html = None
    altura_calc = 500

    # 1. Gera dados brutos
    cal = build_calendar(
        prov_norm=df_proventos,
        positions_enriched=df,
        prov_anunciados=df_prov_anunciados,
        window_past_days=365,
        window_future_days=365,
        include_estimates=mostrar_estimativas
    )

    if cal is None or cal.empty:
        st.info("Sem eventos na base de dados.")
    else:
        # 2. Limpeza e filtro por data
        cal = cal.copy()
        cal["data_evento"] = pd.to_datetime(cal["data_evento"], errors="coerce")
        cal.dropna(subset=["data_evento"], inplace=True)

        # normaliza ticker SEMPRE (isso evita falha silenciosa no filtro)
        if "ticker" in cal.columns:
            cal["ticker"] = cal["ticker"].fillna("").astype(str).str.upper().str.strip()


        start_ts = pd.to_datetime(start_date)
        end_ts = pd.to_datetime(end_date)
        mask = (cal["data_evento"] >= start_ts) & (cal["data_evento"] <= end_ts)
        cal = cal.loc[mask]

        # ✅ AQUI é onde seu filtro tem que entrar (ANTES dos cards e do render)
        cal = _filtra_ticker_df(cal, col="ticker")

        if cal.empty:
            if ticker_filter == "Todos":
                st.warning(f"Nenhum evento encontrado para: {view_option}")
            else:
                st.warning(f"Nenhum evento encontrado para {ticker_filter} em: {view_option}")
        else:
            # --- CÁLCULO DOS 3 TOTAIS (JÁ FILTRADO) ---
            cal["valor_float"] = cal["valor"].apply(_to_float_any)

            # 1. Recebido (Data < Hoje)
            mask_recebido = cal["data_evento"].dt.date < hoje_date
            total_recebido = cal.loc[mask_recebido, "valor_float"].sum()

            # 2. A Receber / Provisionado (Data >= Hoje)
            mask_provisionado = cal["data_evento"].dt.date >= hoje_date
            total_provisionado = cal.loc[mask_provisionado, "valor_float"].sum()

            # 3. Geral
            total_geral = total_recebido + total_provisionado

            # --- RENDERIZAÇÃO DOS 3 CARDS ---
            ct1, ct2, ct3 = st.columns(3)

            with ct1:
                st.markdown(f"""
                <div style="background: #1F2937; border: 1px solid rgba(16, 185, 129, 0.2); border-radius: 12px; padding: 15px 20px; display: flex; align-items: center; gap: 15px;">
                    <div style="background: rgba(16, 185, 129, 0.15); color: #10B981; width: 45px; height: 45px; border-radius: 10px; display: flex; align-items: center; justify-content: center; font-size: 20px;">✅</div>
                    <div>
                        <div style="color: #9AA4B2; font-size: 11px; font-weight: 700; text-transform: uppercase; margin-bottom: 4px;">Já Recebido</div>
                        <div style="color: #10B981; font-size: 20px; font-weight: 800;">{brl(total_recebido)}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            with ct2:
                st.markdown(f"""
                <div style="background: #1F2937; border: 1px solid rgba(245, 158, 11, 0.2); border-radius: 12px; padding: 15px 20px; display: flex; align-items: center; gap: 15px;">
                    <div style="background: rgba(245, 158, 11, 0.15); color: #F59E0B; width: 45px; height: 45px; border-radius: 10px; display: flex; align-items: center; justify-content: center; font-size: 20px;">⏳</div>
                    <div>
                        <div style="color: #9AA4B2; font-size: 11px; font-weight: 700; text-transform: uppercase; margin-bottom: 4px;">A Receber</div>
                        <div style="color: #F59E0B; font-size: 20px; font-weight: 800;">{brl(total_provisionado)}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            with ct3:
                st.markdown(f"""
                <div style="background: #1F2937; border: 1px solid rgba(59, 130, 246, 0.2); border-radius: 12px; padding: 15px 20px; display: flex; align-items: center; gap: 15px;">
                    <div style="background: rgba(59, 130, 246, 0.15); color: #3B82F6; width: 45px; height: 45px; border-radius: 10px; display: flex; align-items: center; justify-content: center; font-size: 20px;">💰</div>
                    <div>
                        <div style="color: #9AA4B2; font-size: 11px; font-weight: 700; text-transform: uppercase; margin-bottom: 4px;">Total Geral</div>
                        <div style="color: #E7EAF0; font-size: 20px; font-weight: 800;">{brl(total_geral)}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # --- ORDENAÇÃO INTELIGENTE (FUTURO PRIMEIRO) ---
            corte = pd.to_datetime(hoje_date)

            df_future = cal[cal["data_evento"] >= corte].copy()
            df_past = cal[cal["data_evento"] < corte].copy()

            df_future.sort_values("data_evento", ascending=True, inplace=True)
            df_past.sort_values("data_evento", ascending=False, inplace=True)

            cal = pd.concat([df_future, df_past])

            # Enriquecimento (Logos, Quantidade)
            if isinstance(mst, pd.DataFrame) and not mst.empty and "logo_url" in mst.columns:
                cal = cal.merge(mst[["ticker", "logo_url"]].drop_duplicates("ticker"), on="ticker", how="left")
            else:
                cal["logo_url"] = ""

            if isinstance(df, pd.DataFrame) and not df.empty and "quantidade" in df.columns:
                cal["ticker"] = cal["ticker"].astype(str)
                df_temp = df.copy()
                df_temp["ticker"] = df_temp["ticker"].astype(str)
                cal = cal.merge(df_temp[["ticker", "quantidade"]], on="ticker", how="left")
                cal["quantidade"] = cal["quantidade"].fillna(0)

            dias_semana = {
                0: 'Segunda-feira', 1: 'Terça-feira', 2: 'Quarta-feira',
                3: 'Quinta-feira', 4: 'Sexta-feira', 5: 'Sábado', 6: 'Domingo'
            }

            html_content = ""

            datas_ordenadas = sorted(list(set(cal["data_evento"])), key=lambda x: list(cal["data_evento"]).index(x))

            for d in datas_ordenadas:
                df_dia = cal[cal["data_evento"] == d]

                dt_obj = pd.to_datetime(d)
                dia_num = dt_obj.day
                dia_nome = dias_semana.get(dt_obj.weekday(), "")
                total_dia = df_dia["valor"].apply(_to_float_any).sum()

                is_day_past = dt_obj.date() < hoje_date

                group_opacity = "0.5" if is_day_past else "1.0"
                titulo_cor = "#9AA4B2" if is_day_past else "#E7EAF0"

                html_content += f"""
                <div style="margin-bottom: 25px; opacity: {group_opacity};">
                    <div style="display:flex; align-items:center; gap:12px; margin-bottom:10px;">
                        <div style="background:{'#374151' if is_day_past else '#7C3AED'}; color:white; width:35px; height:35px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-weight:800; font-size:16px;">
                            {dia_num}
                        </div>
                        <div style="font-size:18px; font-weight:700; color:{titulo_cor};">
                            {dia_nome}
                        </div>
                        <div style="background:rgba(16, 185, 129, 0.15); border:1px solid rgba(16, 185, 129, 0.3); color:#10B981; padding:4px 10px; border-radius:6px; font-weight:700; font-size:13px;">
                            BR {brl(total_dia)}
                        </div>
                    </div>
                """

                for _, r in df_dia.iterrows():
                    ticker = str(r.get("ticker", "")).upper()
                    valor_total = _to_float_any(r.get("valor"))
                    qtd = float(r.get("quantidade", 0))

                    val_unit = _to_float_any(r.get("valor_por_cota"))
                    if val_unit == 0 and qtd > 0:
                        val_unit = valor_total / qtd

                    logo = str(r.get("logo_url", "") or "")
                    img_html = (
                        f'<img src="{logo}" style="width:45px; height:45px; border-radius:8px; object-fit:cover;">'
                        if logo else
                        f'<div style="width:45px; height:45px; background:#374151; border-radius:8px; display:flex; align-items:center; justify-content:center; font-weight:bold; font-size:12px;">{ticker[:2]}</div>'
                    )

                    data_com = pd.to_datetime(r.get("data_com", pd.NaT))
                    str_com = data_com.strftime("%d/%m/%Y") if pd.notna(data_com) else "-"
                    str_pag = dt_obj.strftime("%d/%m/%Y")

                    fonte = str(r.get("fonte", "")).upper()
                    is_est = fonte == "ESTIMADO"

                    tipo_real = str(r.get("tipo", "RENDIMENTO")).upper().strip()

                    if "JCP" in tipo_real or "JUROS" in tipo_real:
                        tipo_label = "JCP"
                        style_tag = "background:rgba(245, 158, 11, 0.2); color:#FBBF24; border:1px solid rgba(245, 158, 11, 0.5);"
                    elif "DIVIDENDO" in tipo_real:
                        tipo_label = "DIVIDENDO"
                        style_tag = "background:rgba(16, 185, 129, 0.2); color:#34D399; border:1px solid rgba(16, 185, 129, 0.5);"
                    else:
                        tipo_label = "RENDIMENTO"
                        style_tag = "background:rgba(59, 130, 246, 0.2); color:#60A5FA; border:1px solid rgba(59, 130, 246, 0.5);"

                    if is_day_past:
                        card_style = "opacity: 0.6; filter: grayscale(0.8); border:1px solid rgba(255,255,255,0.05);"
                    elif is_est:
                        card_style = "opacity: 1; border: 1px dashed rgba(255,255,255,0.4);"
                    else:
                        card_style = "opacity: 1; border: 1px solid rgba(255,255,255,0.15);"

                    tag_est = (
                        '<span style="background:#F59E0B; color:#000; font-size:9px; font-weight:800; padding:2px 4px; border-radius:4px; margin-left:5px;">EST</span>'
                        if is_est else ""
                    )

                    html_content += f"""
                    <div style="background:#1F2937; border-radius:12px; padding:12px 16px; margin-bottom:8px; display:flex; align-items:center; justify-content:space-between; {card_style}">
                        <div style="display:flex; align-items:center; gap:14px; min-width:220px;">
                            {img_html}
                            <div>
                                <div style="display:flex; align-items:center; gap:6px; margin-bottom:2px;">
                                    <span style="font-size:16px; font-weight:800; color:#fff;">{ticker}</span>
                                    {tag_est}
                                </div>
                                <div style="display:flex; align-items:center; gap:6px;">
                                    <span style="background:#374151; color:#9AA4B2; font-size:11px; font-weight:700; padding:1px 8px; border-radius:10px;">{int(qtd)}</span>
                                    <span style="{style_tag} font-size:10px; font-weight:700; padding:1px 6px; border-radius:4px;">{tipo_label}</span>
                                </div>
                            </div>
                        </div>
                        <div style="text-align:center; min-width:120px;">
                            <div style="font-size:16px; font-weight:800; color:#fff;">{brl(valor_total)}</div>
                            <div style="font-size:12px; color:#9AA4B2;">{brl(val_unit)} / cota</div>
                        </div>
                        <div style="text-align:right; font-size:12px; color:#9AA4B2; min-width:140px;">
                            <div style="display:flex; justify-content:space-between; margin-bottom:2px;">
                                <span>data com</span> <span style="color:#E7EAF0; font-weight:600;">{str_com}</span>
                            </div>
                            <div style="display:flex; justify-content:space-between;">
                                <span>pagamento</span> <span style="color:#E7EAF0; font-weight:600;">{str_pag}</span>
                            </div>
                        </div>
                    </div>
                    """
                html_content += "</div>"

            final_html = f"""
            <!DOCTYPE html>
            <html>
            <head>
            <style>
                body {{ margin: 0; font-family: "Source Sans Pro", sans-serif; background-color: #0E1117; }}
                body::-webkit-scrollbar {{ display: none; }}
                .container {{ padding: 5px; }}
            </style>
            </head>
            <body>
                <div class="container">
                    {html_content}
                </div>
            </body>
            </html>
            """

            altura_calc = min(900, len(cal) * 90 + len(datas_ordenadas) * 60 + 50)

    # 4. Renderização
    if final_html:
        components.html(final_html, height=altura_calc, scrolling=True)

# =========================
# FOOTER
# =========================
st.markdown("---")
st.caption(f"Sistema Privado MD • Atualizado: {datetime.now().strftime('%d/%m/%Y %H:%M')}")