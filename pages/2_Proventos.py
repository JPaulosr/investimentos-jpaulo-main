# pages/2_Proventos.py
# -*- coding: utf-8 -*-

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Seus imports
from utils.gsheets import load_proventos
from utils.core import normalize_proventos
from utils.calendar_engine import build_calendar

# =========================
# PAGE CONFIG & CSS PREMIUM
# =========================
st.set_page_config(layout="wide", page_title="Renda Passiva — MD", page_icon="🤑")

st.markdown("""
<style>
    /* Tema Geral */
    .stApp { background-color: #0E1117; color: #E0E0E0; }
    
    /* Remove padding excessivo */
    .block-container { padding-top: 1.5rem; padding-bottom: 3rem; }
    
    /* Cards KPI Modernos */
    .kpi-card {
        background-color: #1A1C24;
        border: 1px solid #333;
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 15px;
        transition: transform 0.2s;
    }
    .kpi-card:hover {
        border-color: #555;
        transform: translateY(-2px);
    }
    .kpi-title { font-size: 12px; text-transform: uppercase; letter-spacing: 1px; color: #888; margin-bottom: 8px; }
    .kpi-value { font-size: 26px; font-weight: 700; color: #FFF; }
    .kpi-sub { font-size: 11px; color: #666; margin-top: 5px; display: flex; align-items: center; gap: 5px; }
    
    /* Cores de Destaque */
    .accent-green { color: #00E676; }
    .accent-purple { color: #D500F9; }
    .accent-blue { color: #2979FF; }
    
    /* Estilo do Calendário (Timeline) */
    .event-card {
        background-color: #1A1C24;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 12px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        border-left: 4px solid #444;
    }
    .date-box {
        background: #262933;
        color: #ddd;
        border-radius: 6px;
        padding: 6px 12px;
        text-align: center;
        min-width: 60px;
    }
    .date-day { font-size: 18px; font-weight: bold; }
    .date-month { font-size: 10px; text-transform: uppercase; }
</style>
""", unsafe_allow_html=True)

# =========================
# HELPERS
# =========================
def _to_float_any(x) -> float:
    if pd.isna(x): return 0.0
    s = str(x).strip().replace("R$", "").replace("%", "").replace(" ", "")
    try:
        if "," in s and "." in s: s = s.replace(".", "").replace(",", ".")
        elif "," in s: s = s.replace(",", ".")
        return float(s)
    except: return 0.0

def brl(v) -> str:
    v = _to_float_any(v)
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

@st.cache_data(show_spinner=False)
def load_data_pipeline():
    prov = load_proventos()
    return normalize_proventos(prov)

def _get_col(df, candidates):
    cols = [c.strip().lower().replace(" ", "_") for c in df.columns]
    for cand in candidates:
        if cand in cols: return df.columns[cols.index(cand)]
    return None

# =========================
# LÓGICA DE DADOS
# =========================
st.title("💰 Gestão de Renda Passiva")

with st.sidebar:
    st.header("Filtros")
    btn_reload = st.button("🔄 Atualizar Dados", use_container_width=True)
    if btn_reload:
        st.cache_data.clear()
        st.rerun()

with st.spinner("Analisando proventos..."):
    df_raw = load_data_pipeline()

# Preparação dos dados
dfp = df_raw.copy()
if not dfp.empty:
    dfp.columns = [c.strip().lower().replace(" ", "_") for c in dfp.columns]

col_ticker = _get_col(dfp, ["ticker", "ativo", "codigo"])
col_val = _get_col(dfp, ["valor_liquido", "valor", "provento", "liquido"])
col_data = _get_col(dfp, ["data_pagamento", "data", "pagamento"])

has_data = not dfp.empty and col_ticker and col_val and col_data

if has_data:
    # Conversões
    dfp[col_data] = pd.to_datetime(dfp[col_data], errors="coerce", dayfirst=True)
    dfp[col_val] = dfp[col_val].apply(_to_float_any)
    dfp = dfp.dropna(subset=[col_data])
    dfp = dfp[dfp[col_val] != 0] # Remove zeros
    
    # Colunas auxiliares
    dfp["ano"] = dfp[col_data].dt.year
    dfp["mes_ano"] = dfp[col_data].dt.to_period("M").astype(str) # "2024-01"
    
    # --- CÁLCULO DE KPIS ---
    now = datetime.now()
    start_ytd = datetime(now.year, 1, 1)
    start_12m = now - pd.DateOffset(months=12)
    
    total_ytd = dfp.loc[dfp[col_data] >= start_ytd, col_val].sum()
    total_12m = dfp.loc[dfp[col_data] >= start_12m, col_val].sum()
    total_hist = dfp[col_val].sum()
    
    # Média mensal (apenas meses que tiveram pagamento no último ano)
    meses_unicos_12m = dfp.loc[dfp[col_data] >= start_12m, "mes_ano"].nunique()
    media_mensal = total_12m / meses_unicos_12m if meses_unicos_12m > 0 else 0

    # --- RENDERIZAÇÃO KPIS ---
    k1, k2, k3, k4 = st.columns(4)
    
    k1.markdown(f"""
    <div class="kpi-card" style="border-left: 4px solid #00E676;">
        <div class="kpi-title">Recebido em {now.year} (YTD)</div>
        <div class="kpi-value">{brl(total_ytd)}</div>
        <div class="kpi-sub accent-green">▲ Acumulado no ano</div>
    </div>""", unsafe_allow_html=True)
    
    k2.markdown(f"""
    <div class="kpi-card" style="border-left: 4px solid #2979FF;">
        <div class="kpi-title">Últimos 12 Meses</div>
        <div class="kpi-value">{brl(total_12m)}</div>
        <div class="kpi-sub accent-blue">↺ Janela móvel</div>
    </div>""", unsafe_allow_html=True)
    
    k3.markdown(f"""
    <div class="kpi-card" style="border-left: 4px solid #D500F9;">
        <div class="kpi-title">Média Mensal (12m)</div>
        <div class="kpi-value">{brl(media_mensal)}</div>
        <div class="kpi-sub accent-purple">Salário Mensal Passivo</div>
    </div>""", unsafe_allow_html=True)

    k4.markdown(f"""
    <div class="kpi-card" style="border-left: 4px solid #FFC400;">
        <div class="kpi-title">Total Histórico</div>
        <div class="kpi-value">{brl(total_hist)}</div>
        <div class="kpi-sub" style="color:#FFC400">Total acumulado</div>
    </div>""", unsafe_allow_html=True)

    # =========================
    # ABAS PRINCIPAIS
    # =========================
    tab_dash, tab_cal, tab_raw = st.tabs(["📊 Dashboard Visual", "📅 Calendário Inteligente", "📋 Dados Brutos"])

    # --- ABA 1: GRÁFICOS ---
    with tab_dash:
        col_g1, col_g2 = st.columns([2, 1])
        
        with col_g1:
            st.markdown("### 📈 Evolução de Proventos (A Escadinha)")
            # Agrupa por Mês
            evol = dfp.groupby("mes_ano")[col_val].sum().reset_index().sort_values("mes_ano")
            
            if not evol.empty:
                fig_bar = px.bar(
                    evol, x="mes_ano", y=col_val,
                    text_auto=".2s",
                    color=col_val, color_continuous_scale="Greens"
                )
                fig_bar.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#E0E0E0"), xaxis_title=None, yaxis_title=None,
                    showlegend=False, coloraxis_showscale=False,
                    margin=dict(l=0, r=0, t=20, b=20)
                )
                fig_bar.update_traces(textposition="outside", cliponaxis=False)
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("Sem dados para histórico.")

        with col_g2:
            st.markdown("### 🏆 Top Pagadores (12m)")
            df_12m = dfp[dfp[col_data] >= start_12m]
            if not df_12m.empty:
                top = df_12m.groupby(col_ticker)[col_val].sum().reset_index().sort_values(col_val, ascending=False).head(10)
                
                # CORREÇÃO AQUI: px.pie com parâmetro hole
                fig_pie = px.pie(
                    top, values=col_val, names=col_ticker,
                    hole=0.6, color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig_pie.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#E0E0E0"), showlegend=False,
                    annotations=[dict(text="Top 10", x=0.5, y=0.5, font_size=20, showarrow=False, font_color="white")],
                    margin=dict(l=0, r=0, t=20, b=20)
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("Sem dados em 12m.")

    # --- ABA 2: CALENDÁRIO ---
    with tab_cal:
        c1, c2, c3 = st.columns([1, 1, 2])
        dias_passados = c1.number_input("Dias Passados", 0, 365, 30)
        dias_futuros = c2.number_input("Dias Futuros", 0, 365, 90)
        ver_estimados = c3.toggle("Mostrar Projeções/Estimativas", value=True)
        
        st.markdown("---")
        
        # Chama seu motor de calendário existente
        try:
            cal_df = build_calendar(
                prov_norm=df_raw,
                positions_enriched=None, # Opcional se não quiser carregar posições aqui
                window_past_days=dias_passados,
                window_future_days=dias_futuros,
                include_estimates=ver_estimados
            )
        except Exception as e:
            st.error(f"Erro no motor de calendário: {e}")
            cal_df = pd.DataFrame()

        if cal_df is not None and not cal_df.empty:
            # Agrupar visualmente por Mês para ficar mais organizado
            cal_df["mes_ref"] = pd.to_datetime(cal_df["data_evento"]).dt.strftime("%B %Y")
            
            for mes, grupo in cal_df.groupby("mes_ref", sort=False):
                st.caption(f"📅 {mes.capitalize()}")
                
                for _, row in grupo.iterrows():
                    dta = pd.to_datetime(row["data_evento"])
                    ticker = str(row["ticker"]).upper()
                    valor = float(row.get("valor", 0) or 0)
                    tipo = row.get("tipo_evento", "Provento")
                    fonte = str(row.get("fonte", "REAL")).upper()
                    
                    # Estilização condicional
                    cor_status = "#00C853" if fonte == "REAL" else "#BB86FC" # Verde Confimado, Roxo Estimado
                    icon = "💰" if fonte == "REAL" else "🔮"
                    
                    html_row = f"""
                    <div class="event-card" style="border-left-color: {cor_status};">
                        <div style="display:flex; align-items:center; gap:15px;">
                            <div class="date-box">
                                <div class="date-day">{dta.day}</div>
                                <div class="date-month">{dta.strftime('%b')}</div>
                            </div>
                            <div>
                                <div style="font-weight:bold; font-size:16px; color:#FFF;">{ticker}</div>
                                <div style="font-size:12px; color:#AAA;">{tipo} • {fonte}</div>
                            </div>
                        </div>
                        <div style="text-align:right;">
                            <div style="font-weight:bold; font-size:18px; color:{cor_status};">
                                + {brl(valor)}
                            </div>
                            <div style="font-size:10px; color:#666;">{icon}</div>
                        </div>
                    </div>
                    """
                    st.markdown(html_row, unsafe_allow_html=True)
        else:
            st.info("Nenhum evento encontrado no período selecionado.")

    # --- ABA 3: DADOS BRUTOS ---
    with tab_raw:
        st.dataframe(
            dfp.sort_values(col_data, ascending=False),
            use_container_width=True,
            height=600,
            hide_index=True
        )

else:
    st.warning("⚠️ Não foi possível carregar os dados de proventos. Verifique se as colunas (Data, Valor, Ticker) existem no Google Sheets.")