# pages/2_Proventos.py
# -*- coding: utf-8 -*-

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Seus imports
from utils.gsheets import load_proventos, load_movimentacoes, load_ativos
from utils.core import normalize_proventos, compute_positions_from_movs
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
    .block-container { padding-top: 2rem; padding-bottom: 3rem; }
    
    /* Cards KPI Modernos */
    .kpi-card {
        background-color: #1A1C24;
        border: 1px solid #333;
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    .kpi-title { font-size: 13px; text-transform: uppercase; letter-spacing: 1px; color: #9AA4B2; margin-bottom: 8px; font-weight: 600; }
    .kpi-value { font-size: 28px; font-weight: 800; color: #FFF; line-height: 1.1; }
    .kpi-sub { font-size: 12px; color: #6B7280; margin-top: 6px; display: flex; align-items: center; gap: 6px; font-weight: 500; }
    
    /* Estilo "StatusInvest/Emitida10" - Tabela de Eventos */
    .raio-x-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        background-color: #1F2937;
        border-bottom: 1px solid #374151;
        padding: 12px 16px;
        transition: background 0.2s;
    }
    .raio-x-row:hover { background-color: #2D3748; }
    .raio-x-ticker { font-weight: 900; font-size: 16px; color: #FFF; width: 80px; }
    .raio-x-tag { 
        font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 4px; 
        text-transform: uppercase; margin-left: 8px; 
    }
    .tag-rend { background: rgba(16, 185, 129, 0.2); color: #34D399; }
    .tag-jcp { background: rgba(245, 158, 11, 0.2); color: #FBBF24; }
    
    .raio-x-data { font-size: 13px; color: #9AA4B2; text-align: center; width: 100px; }
    .raio-x-val { font-size: 15px; font-weight: 700; color: #E7EAF0; text-align: right; width: 100px; }
    
    /* Badge de Yield */
    .yield-badge {
        background: rgba(59, 130, 246, 0.15); 
        color: #60A5FA; 
        padding: 4px 8px; 
        border-radius: 6px; 
        font-size: 12px; 
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

# =========================
# HELPERS
# =========================================================
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

def pct(v) -> str:
    return f"{_to_float_any(v)*100:,.2f}%".replace(".", ",")

@st.cache_data(show_spinner=False)
def load_data_pipeline():
    prov = load_proventos()
    movs = load_movimentacoes()
    return normalize_proventos(prov), compute_positions_from_movs(movs)

def _get_col(df, candidates):
    cols = [c.strip().lower().replace(" ", "_") for c in df.columns]
    for cand in candidates:
        if cand in cols: return df.columns[cols.index(cand)]
    return None

# =========================
# LÓGICA DE DADOS
# =========================
st.title("💰 Gestão de Renda Passiva")

# --- CARREGAMENTO ---
with st.spinner("Processando histórico..."):
    df_raw, df_pos = load_data_pipeline()

# --- PREPARAÇÃO ---
dfp = df_raw.copy()
if not dfp.empty:
    dfp.columns = [c.strip().lower().replace(" ", "_") for c in dfp.columns]

col_ticker = _get_col(dfp, ["ticker", "ativo", "codigo"])
col_val = _get_col(dfp, ["valor_liquido", "valor", "provento", "liquido"])
col_data = _get_col(dfp, ["data_pagamento", "data", "pagamento"])
col_tipo = _get_col(dfp, ["tipo_provento", "tipo", "evento"])

has_data = not dfp.empty and col_ticker and col_val and col_data

if has_data:
    # Conversões e Limpeza
    dfp[col_data] = pd.to_datetime(dfp[col_data], errors="coerce", dayfirst=True)
    dfp[col_val] = dfp[col_val].apply(_to_float_any)
    dfp = dfp.dropna(subset=[col_data])
    dfp = dfp[dfp[col_val] != 0]
    
    # Datas auxiliares
    dfp["ano"] = dfp[col_data].dt.year
    dfp["mes_ano"] = dfp[col_data].dt.strftime("%Y-%m")
    
    # --- FILTRO LATERAL ---
    with st.sidebar:
        st.header("🎛️ Filtros")
        
        # Opções de Período
        anos_disponiveis = sorted(dfp["ano"].unique(), reverse=True)
        opcoes_periodo = ["Últimos 12 Meses (Móvel)", "Ano Atual (YTD)", "Todo o Histórico"] + [str(a) for a in anos_disponiveis]
        
        periodo_sel = st.selectbox("Período de Análise", options=opcoes_periodo, index=0)
        
        # Lógica de Filtragem
        hoje = datetime.now()
        df_filtrado = dfp.copy()
        
        if periodo_sel == "Últimos 12 Meses (Móvel)":
            dt_inicio = hoje - relativedelta(months=12)
            df_filtrado = df_filtrado[df_filtrado[col_data] >= dt_inicio]
            label_periodo = "nos últimos 12m"
        elif periodo_sel == "Ano Atual (YTD)":
            dt_inicio = datetime(hoje.year, 1, 1)
            df_filtrado = df_filtrado[df_filtrado[col_data] >= dt_inicio]
            label_periodo = f"em {hoje.year}"
        elif periodo_sel == "Todo o Histórico":
            label_periodo = "desde o início"
        else:
            ano_sel = int(periodo_sel)
            df_filtrado = df_filtrado[df_filtrado["ano"] == ano_sel]
            label_periodo = f"em {ano_sel}"

        st.divider()
        if st.button("🔄 Recarregar Dados", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # --- CÁLCULO DE MÉTRICAS DO PERÍODO ---
    if not df_filtrado.empty:
        total_periodo = df_filtrado[col_val].sum()
        
        # Média Mensal
        meses_unicos = df_filtrado["mes_ano"].nunique()
        media_mensal = total_periodo / max(1, meses_unicos)
        
        # Maior Pagador
        top_ticker = df_filtrado.groupby(col_ticker)[col_val].sum().idxmax()
        top_val = df_filtrado.groupby(col_ticker)[col_val].sum().max()
        
        # --- CORREÇÃO DO ERRO AQUI ---
        # Calcula o custo total se ele não existir
        if not df_pos.empty:
            if "valor_total" not in df_pos.columns:
                # Tenta calcular: quantidade * preco_medio
                if "quantidade" in df_pos.columns and "preco_medio" in df_pos.columns:
                    df_pos["quantidade"] = pd.to_numeric(df_pos["quantidade"], errors="coerce").fillna(0)
                    df_pos["preco_medio"] = pd.to_numeric(df_pos["preco_medio"], errors="coerce").fillna(0)
                    df_pos["custo_total_calc"] = df_pos["quantidade"] * df_pos["preco_medio"]
                    custo_total_carteira = df_pos["custo_total_calc"].sum()
                elif "custo_total" in df_pos.columns:
                    custo_total_carteira = df_pos["custo_total"].sum()
                else:
                    custo_total_carteira = 0
            else:
                custo_total_carteira = df_pos["valor_total"].sum()
        else:
            custo_total_carteira = 0
        
        yoc_periodo = (total_periodo / custo_total_carteira) if custo_total_carteira > 0 else 0
        
        # Projeção Anual
        projecao_anual = media_mensal * 12

        # --- RENDERIZAÇÃO DE KPIS (4 BLOCOS) ---
        k1, k2, k3, k4 = st.columns(4)
        
        k1.markdown(f"""
        <div class="kpi-card" style="border-left: 4px solid #10B981;">
            <div class="kpi-title">Total Recebido</div>
            <div class="kpi-value">{brl(total_periodo)}</div>
            <div class="kpi-sub" style="color:#10B981;">{label_periodo}</div>
        </div>""", unsafe_allow_html=True)
        
        k2.markdown(f"""
        <div class="kpi-card" style="border-left: 4px solid #3B82F6;">
            <div class="kpi-title">Média Mensal</div>
            <div class="kpi-value">{brl(media_mensal)}</div>
            <div class="kpi-sub" style="color:#60A5FA;">Recorrência média</div>
        </div>""", unsafe_allow_html=True)
        
        k3.markdown(f"""
        <div class="kpi-card" style="border-left: 4px solid #F59E0B;">
            <div class="kpi-title">Yield Período (YoC)</div>
            <div class="kpi-value">{pct(yoc_periodo)}</div>
            <div class="kpi-sub" style="color:#FBBF24;">Sobre custo total</div>
        </div>""", unsafe_allow_html=True)

        k4.markdown(f"""
        <div class="kpi-card" style="border-left: 4px solid #8B5CF6;">
            <div class="kpi-title">Projeção Anual</div>
            <div class="kpi-value">{brl(projecao_anual)}</div>
            <div class="kpi-sub" style="color:#A78BFA;">Base média atual</div>
        </div>""", unsafe_allow_html=True)

        # =========================
        # ABAS PRINCIPAIS
        # =========================
        tab_dash, tab_raiox, tab_cal = st.tabs(["📊 Dashboard Visual", "🔬 Raio-X (Emitida10)", "📅 Calendário"])

        # --- ABA 1: DASHBOARD GRÁFICO ---
        with tab_dash:
            col_g1, col_g2 = st.columns([2, 1])
            
            with col_g1:
                st.markdown("### 📈 Evolução Mensal")
                # Agrupa por Mês para o gráfico
                evol = df_filtrado.groupby("mes_ano")[col_val].sum().reset_index().sort_values("mes_ano")
                
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
                        margin=dict(l=0, r=0, t=20, b=20),
                        height=350
                    )
                    fig_bar.update_traces(texttemplate="R$ %{y:.0f}", textposition="outside", cliponaxis=False)
                    st.plotly_chart(fig_bar, use_container_width=True)
                else:
                    st.info("Sem dados para o gráfico.")

            with col_g2:
                st.markdown("### 🏆 Top Pagadores")
                top = df_filtrado.groupby(col_ticker)[col_val].sum().reset_index().sort_values(col_val, ascending=False).head(7)
                
                if not top.empty:
                    fig_pie = px.pie(
                        top, values=col_val, names=col_ticker,
                        hole=0.6, color_discrete_sequence=px.colors.qualitative.Prism
                    )
                    fig_pie.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        font=dict(color="#E0E0E0"), showlegend=False,
                        annotations=[dict(text=f"{len(top)} Ativos", x=0.5, y=0.5, font_size=16, showarrow=False, font_color="white")],
                        margin=dict(l=0, r=0, t=20, b=20),
                        height=350
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)

        # --- ABA 2: RAIO-X (EMITIDA 10 / STATUSINVEST) ---
        with tab_raiox:
            st.markdown("### 🔬 Detalhamento dos Pagamentos")
            
            # Ordena por data decrescente (mais recente primeiro)
            df_list = df_filtrado.sort_values(col_data, ascending=False).copy()
            
            # Cabeçalho
            st.markdown("""
            <div style="display:flex; justify-content:space-between; padding:10px 16px; border-bottom:2px solid #4B5563; margin-bottom:5px; font-weight:700; color:#9AA4B2; font-size:12px; text-transform:uppercase;">
                <div style="width:80px;">Ativo</div>
                <div style="width:100px; text-align:center;">Data Pagamento</div>
                <div style="width:100px; text-align:center;">Data Com</div>
                <div style="width:100px; text-align:right;">Valor Líquido</div>
            </div>
            """, unsafe_allow_html=True)
            
            for _, r in df_list.iterrows():
                ticker = str(r[col_ticker]).upper()
                val = float(r[col_val])
                d_pag = r[col_data].strftime("%d/%m/%Y")
                
                # Tenta pegar Data Com e Tipo se existirem
                d_com = "-" 
                # (Se você tiver a coluna data_com no dataframe, descomente abaixo)
                # if "data_com" in dfp.columns: d_com = pd.to_datetime(r["data_com"]).strftime("%d/%m/%Y")
                
                tipo = str(r.get(col_tipo, "Rendimento")).upper()
                tag_class = "tag-jcp" if "JCP" in tipo else "tag-rend"
                tag_label = "JCP" if "JCP" in tipo else "REND."
                
                html_row = f"""
                <div class="raio-x-row">
                    <div style="display:flex; align-items:center;">
                        <div class="raio-x-ticker">{ticker}</div>
                        <div class="raio-x-tag {tag_class}">{tag_label}</div>
                    </div>
                    <div class="raio-x-data">{d_pag}</div>
                    <div class="raio-x-data">{d_com}</div>
                    <div class="raio-x-val">{brl(val)}</div>
                </div>
                """
                st.markdown(html_row, unsafe_allow_html=True)

        # --- ABA 3: CALENDÁRIO ---
        with tab_cal:
            st.markdown("### 📅 Visão de Calendário")
            # Reutiliza o motor de calendário
            cal_df = build_calendar(
                prov_norm=df_raw,
                positions_enriched=None,
                window_past_days=365,
                window_future_days=365,
                include_estimates=True
            )
            
            if not cal_df.empty:
                # Filtra pelo mesmo range selecionado na sidebar
                mask_cal = (pd.to_datetime(cal_df["data_evento"]) >= df_filtrado[col_data].min()) & \
                           (pd.to_datetime(cal_df["data_evento"]) <= df_filtrado[col_data].max())
                cal_view = cal_df[mask_cal].copy()
                
                if not cal_view.empty:
                    cal_view["data_evento"] = pd.to_datetime(cal_view["data_evento"])
                    st.dataframe(
                        cal_view[["data_evento", "ticker", "valor", "tipo", "status"]].sort_values("data_evento"),
                        use_container_width=True,
                        hide_index=True,
                        height=500
                    )
                else:
                    st.info("Nenhum evento no calendário para este período.")
            else:
                st.info("Calendário vazio.")

    else:
        st.warning("⚠️ Nenhum dado encontrado para o período selecionado.")

else:
    st.error("❌ Não foi possível carregar a base de proventos. Verifique a planilha Google.")