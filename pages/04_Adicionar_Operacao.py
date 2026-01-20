# pages/04_Adicionar_Operacao.py
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
from datetime import date, datetime
import time

from utils.gsheets import (
    load_ativos,
    load_proventos,
    load_movimentacoes,
    load_cotacoes,
    append_movimentacao,
    append_movimentacao_legado,
    append_provento,
    append_provento_legado,      # <-- ADD
    get_ws_proventos_legado,     # <-- ADD
)


from utils.telegram import send_telegram_message
from utils.formatters import (
    build_trade_msg,
    build_provento_msg,
    build_renda_alert_msg,
    build_batch_summary_msg,
    fmt_brl, fmt_date_br
)
from utils.estimativas import estimate_next_month_income, get_trailing_12m_proventos
from utils.alerts_insights import check_renda_deviation, get_status_comparison
from utils.ids import make_id

st.set_page_config(page_title="Central de Lançamentos", page_icon="⚡", layout="wide")

# --- CSS ESTILO ---
st.markdown("""
<style>
    .stButton button {
        height: 3rem;
        font-weight: 600;
        border-radius: 8px;
    }
    div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stImage"]) {
        background-color: #262730;
        padding: 15px;
        border-radius: 12px;
        border: 1px solid #444;
        margin-bottom: 20px;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.4rem !important;
    }
    .stAlert {
        font-weight: bold;
        border-left: 5px solid #3498db;
    }
</style>
""", unsafe_allow_html=True)

st.title("⚡ Caixa de Lançamentos")

# CONFIG
PORTFOLIO_ID_PADRAO = 1
BOT_TOKEN = st.secrets.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "")

# SESSÃO
if "lote_ops" not in st.session_state: st.session_state.lote_ops = []
if "lote_prov" not in st.session_state: st.session_state.lote_prov = []

# Controle de Automação
if "last_ticker_op" not in st.session_state: st.session_state.last_ticker_op = None
if "last_ticker_prov" not in st.session_state: st.session_state.last_ticker_prov = None

# HELPERS
def _safe_float(x) -> float:
    try:
        if x is None: return 0.0
        s = str(x).strip()
        if s == "" or s.lower() == "nan": return 0.0
        if "," in s: s = s.replace(".", "").replace(",", ".")
        return float(s)
    except: return 0.0

def normalize_df_columns(df):
    """Garante que colunas sejam minúsculas e sem espaços extras."""
    if df is not None and not df.empty:
        df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def get_current_qty(movs_df: pd.DataFrame, ticker: str) -> float:
    if movs_df is None or movs_df.empty or "ticker" not in movs_df.columns: return 0.0
    df = movs_df.copy()
    # Filtro insensível a maiúsculas
    ticker_upper = str(ticker).upper().strip()
    df["ticker_norm"] = df["ticker"].astype(str).str.upper().str.strip()
    
    df = df[df["ticker_norm"] == ticker_upper].copy()
    if df.empty: return 0.0
    
    df["quantidade"] = df["quantidade"].apply(_safe_float)
    df["tipo_norm"] = df["tipo"].astype(str).str.upper().str.strip()
    
    buys = df[df["tipo_norm"] == "COMPRA"]["quantidade"].sum()
    sells = df[df["tipo_norm"] == "VENDA"]["quantidade"].sum()
    
    saldo = float(buys - sells)
    return saldo if saldo > 0.001 else 0.0  # Tolerância para evitar 0.0000001 (posições zeradas)

def get_last_paid_price(movs_df: pd.DataFrame, ticker: str) -> float:
    """Retorna o preço unitário da ÚLTIMA COMPRA realizada."""
    if movs_df.empty: return 0.0
    df = movs_df.copy()
    ticker_upper = str(ticker).upper().strip()
    df["ticker_norm"] = df["ticker"].astype(str).str.upper().str.strip()
    df["tipo_norm"] = df["tipo"].astype(str).str.upper().str.strip()
    
    df = df[
        (df["ticker_norm"] == ticker_upper) & 
        (df["tipo_norm"] == "COMPRA")
    ]
    if df.empty: return 0.0
    
    # Tenta ordenar por data se existir
    if "data" in df.columns:
        df["dt_temp"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
        df = df.sort_values("dt_temp")
    
    try:
        return _safe_float(df.iloc[-1]["preco_unitario"])
    except:
        return 0.0

def get_last_vpc(proventos_df: pd.DataFrame, ticker: str) -> float:
    if proventos_df.empty or "ticker" not in proventos_df.columns: return 0.0
    df = proventos_df.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df[df["ticker"] == ticker].copy()
    if df.empty: return 0.0
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df = df.sort_values("data", ascending=True)
    try:
        last_row = df.iloc[-1]
        vpc = _safe_float(last_row.get("valor_por_cota", 0))
        if vpc <= 0:
            val = _safe_float(last_row.get("valor", 0))
            qtd = _safe_float(last_row.get("quantidade_na_data", 0))
            if qtd > 0: vpc = val / qtd
        return vpc
    except: return 0.0

def get_preco_referencia(ticker: str, cotacoes_df: pd.DataFrame, movs_df: pd.DataFrame) -> float:
    ticker = ticker.upper().strip()
    if not cotacoes_df.empty:
        df_c = cotacoes_df.copy()
        row = df_c[df_c["ticker"].astype(str).str.upper().str.strip() == ticker]
        if not row.empty:
            try:
                # Tenta várias chaves possíveis para preço
                for k in ['price', 'preco', 'close', 'cotação', 'valor']:
                    if k in row.columns:
                        val = _safe_float(row.iloc[0][k])
                        if val > 0: return val
            except: pass
            
    if not movs_df.empty:
        # Tenta pegar último preço pago como referência
        return get_last_paid_price(movs_df, ticker)
        
    return 0.0

def calcular_cenario_financeiro(df_movs, ticker, qtd_op, preco_op, tipo_op):
    # (Mesma lógica, apenas garantindo normalização interna)
    if df_movs.empty: return 0.0, 0.0, 0.0, 0.0, 0.0
    
    df = df_movs.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df[df["ticker"] == ticker].copy()
    
    total_custo, total_qtd = 0.0, 0.0
    
    if not df.empty:
        for _, row in df.iterrows():
            q = _safe_float(row.get("quantidade"))
            p = _safe_float(row.get("preco_unitario"))
            t = str(row.get("tipo")).upper().strip()
            if t == "COMPRA": total_custo += q * p; total_qtd += q
            elif t == "VENDA" and total_qtd > 0:
                pm = total_custo / total_qtd
                total_custo -= q * pm; total_qtd -= q
                
    q_op, p_op = float(qtd_op), float(preco_op)
    if tipo_op == "COMPRA": total_custo += q_op * p_op; total_qtd += q_op
    elif tipo_op == "VENDA" and total_qtd > 0:
        pm = total_custo / total_qtd
        total_custo -= q_op * pm; total_qtd -= q_op
        
    pm_final = (total_custo / total_qtd) if total_qtd > 0 else 0.0
    val_mkt = total_qtd * p_op
    res_fin = val_mkt - total_custo
    res_pct = (res_fin / total_custo * 100) if total_custo > 0 else 0.0
    return pm_final, total_custo, val_mkt, res_fin, res_pct

def calcular_totais_impacto(movs_df, lote_atual, ativos_df, proventos_df):
    if not lote_atual: return {}, 0.0
    
    # Normaliza Lote
    lista_itens = []
    if isinstance(lote_atual, pd.DataFrame):
        for _, row in lote_atual.iterrows():
            d = row.to_dict()
            dt_val = d.get("data")
            if isinstance(dt_val, str):
                try: dt_val = datetime.strptime(dt_val, "%d/%m/%Y").date()
                except: 
                    try: dt_val = datetime.strptime(dt_val, "%Y-%m-%d").date()
                    except: dt_val = date.today()
            d["data_obj"] = dt_val
            lista_itens.append(d)
    else:
        lista_itens = lote_atual

    if not lista_itens: return {}, 0.0

    data_ref = lista_itens[0].get("data_obj")
    if not isinstance(data_ref, (date, datetime)): data_ref = date.today()
    mes_ref, ano_ref = data_ref.month, data_ref.year
    
    impacto_dia = {}
    total_mes_recalc = 0.0
    
    if not movs_df.empty:
        df_mes = movs_df.copy()
        df_mes['data_dt'] = pd.to_datetime(df_mes['data'], dayfirst=True, errors='coerce')
        df_mes = df_mes[
            (df_mes['data_dt'].dt.month == mes_ref) & 
            (df_mes['data_dt'].dt.year == ano_ref) &
            (df_mes['tipo'].astype(str).str.upper().str.strip() == 'COMPRA')
        ]
        
        for _, row in df_mes.iterrows():
            ticker = str(row['ticker']).upper().strip()
            r_cls = ativos_df[ativos_df['ticker'] == ticker]
            if r_cls.empty: continue
            classe = str(r_cls.iloc[0].get('classe', '')).lower().strip()
            if classe in ['fii', 'fiagro']:
                qtd = _safe_float(row['quantidade'])
                vpc = get_last_vpc(proventos_df, ticker)
                if vpc > 0: total_mes_recalc += (qtd * vpc)
        
        if isinstance(lote_atual, list):
             for item in lista_itens:
                ticker = item['ticker']
                r_cls = ativos_df[ativos_df['ticker'] == ticker]
                classe = str(r_cls.iloc[0].get('classe', '')).lower().strip() if not r_cls.empty else ''
                
                if str(item['tipo']).upper() == 'COMPRA' and classe in ['fii', 'fiagro']:
                    qtd = float(item['quantidade'])
                    vpc = get_last_vpc(proventos_df, ticker)
                    if vpc > 0: total_mes_recalc += (qtd * vpc)
        
    total_mes = total_mes_recalc

    for item in lista_itens:
        ticker = item['ticker']
        tipo = str(item['tipo']).upper()
        r_cls = ativos_df[ativos_df['ticker'] == ticker]
        classe = str(r_cls.iloc[0].get('classe', '')).lower().strip() if not r_cls.empty else ''

        if tipo == 'COMPRA' and classe in ['fii', 'fiagro']:
            qtd = _safe_float(item.get('quantidade', 0))
            vpc = get_last_vpc(proventos_df, ticker)
            if vpc > 0:
                imp = qtd * vpc
                impacto_dia[ticker] = impacto_dia.get(ticker, 0.0) + imp

    return impacto_dia, total_mes

# =========================================================
# CARGAS & NORMALIZAÇÃO (ESSENCIAL)
# =========================================================
# Carrega DataFrames
df_ativos_raw = pd.DataFrame(load_ativos())
df_proventos_raw = pd.DataFrame(load_proventos())
df_movs_raw = pd.DataFrame(load_movimentacoes())
df_cotacoes_raw = pd.DataFrame(load_cotacoes())

# Normaliza Nomes das Colunas (Para minúsculo)
ativos = normalize_df_columns(df_ativos_raw)
proventos = normalize_df_columns(df_proventos_raw)
movs = normalize_df_columns(df_movs_raw)
cotacoes = normalize_df_columns(df_cotacoes_raw)

# Validações Básicas
if ativos.empty: st.error("Base de ativos vazia."); st.stop()
ativos["ticker"] = ativos["ticker"].astype(str).str.upper().str.strip()
todos_tickers = sorted([t for t in ativos["ticker"].unique().tolist() if t and t != "nan"])

if not proventos.empty: proventos["ticker"] = proventos["ticker"].astype(str).str.upper().str.strip()
if not movs.empty: movs["ticker"] = movs["ticker"].astype(str).str.upper().str.strip()

# =========================================================
# LÓGICA INTELIGENTE: FILTRO DE CARTEIRA
# =========================================================
tickers_em_carteira = []
if not movs.empty:
    for t in todos_tickers:
        q = get_current_qty(movs, t)
        if q > 0: # AQUI GARANTE QUE SÓ ENTRA QUEM TEM QUANTIDADE
            tickers_em_carteira.append(t)

carteira_detectada = len(tickers_em_carteira) > 0

# =========================================================
# GERENCIADOR DE ENVIOS
# =========================================================
with st.expander("🔄 GERENCIADOR DE ENVIOS (Histórico & Testes)", expanded=False):
    t_hist, t_test = st.tabs(["📜 Reenviar do Histórico", "🧪 Testar Rascunho"])
    with t_hist:
        c_h1, c_h2 = st.columns(2)
        with c_h1:
            datas_disponiveis = []
            if not movs.empty and 'data' in movs.columns:
                movs['data_dt'] = pd.to_datetime(movs['data'], dayfirst=True, errors='coerce')
                datas_disponiveis = sorted(movs['data_dt'].dropna().unique(), reverse=True)
                datas_str = [d.strftime("%d/%m/%Y") for d in datas_disponiveis]
            sel_data_str = st.selectbox("Selecione a Data", datas_str, key="hist_date_sel") if datas_disponiveis else st.selectbox("Sem datas", [])
        with c_h2:
            st.write(""); st.write("")
            btn_reenviar_ops = st.button("🛒 Reenviar Operações", use_container_width=True, disabled=not datas_disponiveis)
        
        if btn_reenviar_ops and sel_data_str:
            sel_date_obj = datetime.strptime(sel_data_str, "%d/%m/%Y")
            df_dia = movs[movs['data_dt'] == sel_date_obj].copy()
            if df_dia.empty: st.warning("Nenhuma operação encontrada.")
            else:
                lista_hist = []
                for _, row in df_dia.iterrows():
                    d = row.to_dict()
                    d["data_formatada"] = sel_data_str
                    d["data_obj"] = sel_date_obj
                    d["valor_total"] = _safe_float(d.get("valor_total", 0)) or (_safe_float(d.get("quantidade",0)) * _safe_float(d.get("preco_unitario",0)))
                    
                    r_l = ativos[ativos["ticker"] == d["ticker"]]
                    if not r_l.empty:
                        d["logo_url"] = str(r_l.iloc[0].get("logo_url", "")).strip()
                        d["classe"] = str(r_l.iloc[0].get("classe", "")).lower().strip()
                    else: d["logo_url"] = ""; d["classe"] = ""
                    d["vpc_last"] = get_last_vpc(proventos, d["ticker"])
                    lista_hist.append(d)
                
                dict_dia, tot_mes = calcular_totais_impacto(movs, lista_hist, ativos, proventos)
                summary = build_batch_summary_msg(lista_hist, "OPERACAO", impacto_dia_dados=dict_dia, total_impacto_mes=tot_mes)
                send_telegram_message(BOT_TOKEN, CHAT_ID, summary)
                st.success(f"Resumo de {sel_data_str} reenviado!")

    with t_test:
        st.info("Adicione itens ao lote primeiro.")
        ct1, ct2 = st.columns(2)
        with ct1:
            if st.button("📤 Testar Cards (Ops)"):
                if not st.session_state.lote_ops: st.warning("Lote vazio.")
                else:
                    st.toast("Cards simulados!")
            if st.button("📑 Testar Resumo (Ops)"):
                if not st.session_state.lote_ops: st.warning("Lote vazio.")
                else:
                    dict_dia, tot_mes = calcular_totais_impacto(movs, st.session_state.lote_ops, ativos, proventos)
                    summary = build_batch_summary_msg(st.session_state.lote_ops, "OPERACAO", impacto_dia_dados=dict_dia, total_impacto_mes=tot_mes)
                    send_telegram_message(BOT_TOKEN, CHAT_ID, summary)
                    st.toast("Resumo simulado!")

tab1, tab2 = st.tabs(["🛒 Compras & Vendas", "💰 Proventos"])

# =========================================================
# TAB 1: OPERAÇÕES (INTELIGENTE)
# =========================================================
with tab1:
    c_form, c_list = st.columns([1, 1.3]) 
    with c_form:
        # LÓGICA ALTERADA: Checkbox agora serve para BUSCAR NOVOS
        mostrar_todos = st.checkbox(
            "🔎 Buscar na Lista Completa (Novos Ativos)", 
            value=False, 
            help="Marque para buscar ativos que você ainda não tem na carteira."
        )
        
        # Se NÃO marcar, mostra só a carteira (excluindo zerados). Se marcar, mostra todos.
        if not mostrar_todos and carteira_detectada:
            lista_op = tickers_em_carteira
        else:
            lista_op = todos_tickers

        ticker = st.selectbox("Buscar Ativo", lista_op, key="op_ticker")
        
        # --- AUTO-SUGESTÃO DE PREÇO ---
        if ticker and ticker != st.session_state.last_ticker_op:
            ultimo_preco_pago = get_last_paid_price(movs, ticker)
            if ultimo_preco_pago > 0:
                st.session_state["op_preco"] = float(ultimo_preco_pago)
                st.toast(f"Último preço pago: R$ {ultimo_preco_pago:.2f}", icon="💡")
            st.session_state.last_ticker_op = ticker
            st.rerun() 
        # -----------------------------

        logo_url = ""
        r = ativos[ativos["ticker"] == ticker]
        if not r.empty:
            cands = ["logo", "logo url", "logo_url", "url", "img"]
            found = next((c for c in cands if c in r.columns), None)
            if found: val = r.iloc[0][found]; logo_url = str(val).strip() if val else ""
        
        with st.container(border=True):
            col_img, col_info = st.columns([1, 3])
            with col_img: 
                if logo_url: st.image(logo_url, use_container_width=True)
                else: st.info("📷")
            with col_info: 
                st.markdown(f"## {ticker}")
                qtd_atual = get_current_qty(movs, ticker)
                st.caption(f"Em carteira: **{qtd_atual:g}**")

        dt = st.date_input("Data", value=date.today(), key="op_dt")
        tipo = st.selectbox("Tipo", ["COMPRA", "VENDA"], key="op_tipo")
        c1, c2 = st.columns(2)
        with c1: qtd = st.number_input("Qtd", min_value=0.0, step=1.0, key="op_qtd")
        with c2: preco = st.number_input("Preço (R$)", min_value=0.0, step=0.01, format="%.2f", key="op_preco")
        c3, c4 = st.columns(2)
        with c3: taxa = st.number_input("Taxas (R$)", min_value=0.0, step=0.01, format="%.2f", key="op_taxa")
        with c4: origem = st.text_input("Origem", value="manual", key="op_orig")
        obs = st.text_input("Observação", key="op_obs")
        st.write("") 
        
        if st.button("⬇️ ADICIONAR AO LOTE", use_container_width=True, type="secondary"):
            if qtd <= 0 or preco <= 0: st.error("Qtd/Preço > 0")
            else:
                fin = (qtd * preco) + taxa
                st.session_state.lote_ops.append({
                    "data_formatada": dt.strftime("%d/%m/%Y"), "data_obj": dt, "ticker": ticker,
                    "tipo": tipo, "quantidade": float(qtd), "preco_unitario": float(preco),
                    "taxa": float(taxa), "valor_total": float(fin), "origem": origem,
                    "observacao": obs, "logo_url": logo_url
                })
                st.toast(f"{ticker} adicionado!", icon="🛒")

    with c_list:
        st.markdown(f"### 🛒 Lista de Lançamentos")
        if len(st.session_state.lote_ops) > 0:
            df_lote = pd.DataFrame(st.session_state.lote_ops)
            edited_df = st.data_editor(
                df_lote,
                column_config={
                    "ticker": st.column_config.TextColumn("Ativo", disabled=True),
                    "tipo": st.column_config.SelectboxColumn("Tipo", options=["COMPRA", "VENDA"]),
                    "quantidade": st.column_config.NumberColumn("Qtd", min_value=0.01),
                    "preco_unitario": st.column_config.NumberColumn("Preço", format="R$ %.2f"),
                    "valor_total": st.column_config.NumberColumn("Total (Calc)", disabled=True, format="R$ %.2f")
                },
                column_order=["ticker", "tipo", "data_formatada", "quantidade", "preco_unitario", "valor_total"],
                num_rows="dynamic", use_container_width=True, key="editor_ops"
            )
            st.session_state.lote_ops = edited_df.to_dict("records")
            
            if st.button("✅ FINALIZAR LOTE", type="primary", use_container_width=True):
                if not st.session_state.lote_ops:
                    st.error("Vazio")
                else:
                    progress = st.progress(0, text="Salvando...")
                    lista = st.session_state.lote_ops

                    dict_dia, tot_mes = calcular_totais_impacto(movs, lista, ativos, proventos)

                    for idx, item in enumerate(lista):
                        # recalcula total (garantia)
                        item["valor_total"] = (
                            float(item["quantidade"]) * float(item["preco_unitario"])
                        ) + float(item.get("taxa", 0))

                        # classe
                        classe_ativo = "FII"
                        try:
                            r_cls = ativos[ativos["ticker"] == item["ticker"]]
                            if not r_cls.empty:
                                classe_ativo = str(r_cls.iloc[0].get("classe", "FII")).strip()
                        except:
                            pass
                        item["classe"] = classe_ativo

                        # monta payload da base nova
                        novo_id = make_id(item["ticker"], item["tipo"], datetime.now())
                        mov_to_save = {
                            "id": novo_id,
                            "portfolio_id": int(PORTFOLIO_ID_PADRAO),
                            "data": item["data_formatada"],
                            "ticker": item["ticker"],
                            "tipo": item["tipo"],
                            "quantidade": item["quantidade"],
                            "preco_unitario": item["preco_unitario"],
                            "taxa": item["taxa"],
                            "valor_total": item["valor_total"],
                            "origem": item["origem"],
                            "observacao": item["observacao"],
                            "classe": item["classe"],
                            "criado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }

                        # ✅ 1) SALVA NA BASE NOVA (se falhar, para esse item)
                        try:
                            append_movimentacao(mov_to_save)
                        except Exception as e:
                            st.error(f"Erro (base nova) {item['ticker']}: {e}")
                            continue

                        # ✅ 2) ESPELHO NA BASE ANTIGA (se falhar, avisa e segue)
                        try:
                            append_movimentacao_legado({
                                "ticker": item["ticker"],
                                "data": item["data_formatada"],
                                "tipo": item["tipo"],
                                "quantidade": item["quantidade"],
                                "preco_unitario": item["preco_unitario"],
                            })
                        except Exception as e:
                            st.warning(f"⚠️ Espelho falhou (base antiga) {item['ticker']}: {e}")

                        # Recupera Logo
                        logo_final = item.get("logo_url", "")
                        if not logo_final:
                            r_l = ativos[ativos["ticker"] == item["ticker"]]
                            if not r_l.empty:
                                cands = ["logo", "logo url", "logo_url", "url", "img"]
                                found = next((c for c in cands if c in r_l.columns), None)
                                if found:
                                    val = r_l.iloc[0][found]
                                    logo_final = str(val).strip() if val else ""

                        # qty pós-op
                        qty_atual = get_current_qty(movs, item["ticker"])
                        qty_pos = qty_atual + (item["quantidade"] if item["tipo"] == "COMPRA" else -item["quantidade"])

                        classe_ativo_norm = classe_ativo.lower()
                        is_fii = (classe_ativo_norm in ["fii", "fiagro"])
                        item["classe"] = classe_ativo_norm

                        # estimativas
                        est_val, est_met, est_base, vpc_last = 0.0, "", "", 0.0
                        try:
                            est_val, est_met, est_base, vpc_last = estimate_next_month_income(
                                item["ticker"], qty_pos, proventos, ativos
                            )
                        except:
                            pass

                        # cenário financeiro
                        pm_final, custo_tot, val_atual, res_fin, res_pct = calcular_cenario_financeiro(
                            movs, item["ticker"], item["quantidade"], item["preco_unitario"], item["tipo"]
                        )

                        yoc_final, dy_final, impacto_mensal, est_msg = 0.0, 0.0, 0.0, 0.0
                        preco_ref = item["preco_unitario"]

                        if is_fii:
                            yoc_final = (vpc_last / pm_final * 100) if (pm_final > 0 and vpc_last > 0) else 0.0
                            dy_final = (vpc_last / preco_ref * 100) if (preco_ref > 0 and vpc_last > 0) else 0.0
                            impacto_mensal = item["quantidade"] * vpc_last
                            est_msg = est_val
                        else:
                            div_acao_ano = (est_val * 12) / qty_pos if qty_pos > 0 else 0.0
                            dy_final = (div_acao_ano / preco_ref * 100) if preco_ref > 0 else 0.0
                            base_custo = pm_final if pm_final > 0 else preco_ref
                            yoc_final = (div_acao_ano / base_custo * 100) if base_custo > 0 else 0.0
                            est_msg = est_val

                        msg = build_trade_msg(
                            tipo=item["tipo"],
                            ticker=item["ticker"],
                            qtd=item["quantidade"],
                            total_qty=qty_pos,
                            preco=item["preco_unitario"],
                            taxa=item["taxa"],
                            pm=pm_final,
                            est_mes_total=est_msg,
                            vpc_last=vpc_last,
                            impacto_mensal=impacto_mensal,
                            yoc=yoc_final,
                            dy_mensal=dy_final,
                            preco_atual_ref=preco_ref,
                            metodo=est_met,
                            custo_total=custo_tot,
                            valor_atual=val_atual,
                            resultado_fin=res_fin,
                            resultado_pct=res_pct,
                            classe=classe_ativo_norm,
                        )

                        send_telegram_message(BOT_TOKEN, CHAT_ID, msg, image_url=logo_final)

                        progress.progress((idx + 1) / len(lista))
                        time.sleep(0.5)

                    summary = build_batch_summary_msg(lista, "OPERACAO", impacto_dia_dados=dict_dia, total_impacto_mes=tot_mes)
                    send_telegram_message(BOT_TOKEN, CHAT_ID, summary)

                    st.success("Lote Finalizado!")
                    st.session_state.lote_ops = []
                    time.sleep(1)
                    st.rerun()

        else: st.info("👈 Adicione itens.")

# =========================================================
# TAB 2: PROVENTOS (SEMPRE FILTRADO PELA CARTEIRA)
# =========================================================
with tab2:
    cp_form, cp_list = st.columns([1, 1.3])
    
    with cp_form:
        # LÓGICA ESTRITA: Mostra apenas ativos em carteira para proventos
        # Se a carteira estiver vazia, fallback para todos para não quebrar a tela
        lista_prov = tickers_em_carteira if tickers_em_carteira else todos_tickers
        
        ticker_p = st.selectbox("Buscar Ativo", lista_prov, key="prov_ticker")
        
        # INTELIGÊNCIA
        if ticker_p and ticker_p != st.session_state.last_ticker_prov:
            qtd_c = get_current_qty(movs, ticker_p)
            vpc_last = get_last_vpc(proventos, ticker_p)
            total_est = qtd_c * vpc_last
            st.session_state["prov_qtd"] = float(qtd_c)
            st.session_state["prov_val"] = float(total_est)
            st.session_state.last_ticker_prov = ticker_p
            st.rerun()

        logo_url_p = ""
        rp = ativos[ativos["ticker"] == ticker_p]
        if not rp.empty:
            cands = ["logo", "logo url", "logo_url", "url", "img"]
            found = next((c for c in cands if c in rp.columns), None)
            if found: val = rp.iloc[0][found]; logo_url_p = str(val).strip() if val else ""
        
        with st.container(border=True):
            col_img, col_info = st.columns([1, 3])
            with col_img: 
                if logo_url_p: st.image(logo_url_p, use_container_width=True)
                else: st.info("📷")
            with col_info:
                st.markdown(f"## {ticker_p}")
                last_v = get_last_vpc(proventos, ticker_p)
                st.caption(f"Ref. Histórico: **R$ {last_v:.2f} /cota**")

        dtp = st.date_input("Data Pagamento", value=date.today(), key="prov_dt")
        tipo_p = st.selectbox("Tipo", ["DIVIDENDO", "JCP", "RENDIMENTO", "AMORTIZACAO"], key="prov_tipo")
        
        c_v, c_q = st.columns(2)
        with c_v:
            valor_p = st.number_input("Valor Total (R$)", min_value=0.0, step=0.01, format="%.2f", key="prov_val")
        with c_q:
            qtd_na_data = st.number_input("Qtd na Data", min_value=0.0, step=1.0, key="prov_qtd")
            
        vpc_calc = valor_p / qtd_na_data if qtd_na_data > 0 else 0.0
        st.info(f"🔵 Unitário Calculado: **R$ {vpc_calc:,.2f}**")

        origem_p = st.selectbox("Origem", ["oficial", "estimado"], key="prov_orig")
        st.write("")
        
        if st.button("⬇️ ADICIONAR PROVENTO", use_container_width=True, type="secondary"):
            if valor_p <= 0 or qtd_na_data <= 0: st.error("Valores devem ser > 0")
            else:
                t_final = "JCP" if str(tipo_p).upper() == "JCP" else str(tipo_p).title()
                st.session_state.lote_prov.append({
                    "data_formatada": dtp.strftime("%d/%m/%Y"), "data_obj": dtp,
                    "ticker": ticker_p, "tipo": t_final,
                    "valor": float(valor_p), "quantidade_na_data": float(qtd_na_data),
                    "valor_por_cota": float(vpc_calc), "origem": origem_p, "logo_url": logo_url_p
                })
                st.toast(f"{ticker_p} adicionado!", icon="💰")

    with cp_list:
        st.markdown(f"### 💰 Lista de Proventos")
        if len(st.session_state.lote_prov) > 0:
            df_lp = pd.DataFrame(st.session_state.lote_prov)
            edited_prov = st.data_editor(
                df_lp,
                column_config={
                    "ticker": st.column_config.TextColumn("Ativo", disabled=True),
                    "tipo": st.column_config.SelectboxColumn("Tipo", options=["DIVIDENDO", "JCP", "RENDIMENTO", "AMORTIZACAO"]),
                    "valor": st.column_config.NumberColumn("Total Recebido", min_value=0.01, format="R$ %.2f", required=True),
                    "quantidade_na_data": st.column_config.NumberColumn("Qtd", min_value=0.01, step=1.0, required=True),
                    "valor_por_cota": st.column_config.NumberColumn("Unitário", disabled=True, format="R$ %.2f")
                },
                column_order=["ticker", "tipo", "data_formatada", "quantidade_na_data", "valor", "valor_por_cota"],
                num_rows="dynamic", use_container_width=True, key="editor_prov"
            )
            st.session_state.lote_prov = edited_prov.to_dict("records")
            
            if not edited_prov.empty:
                tot_p = edited_prov["valor"].sum()
                st.markdown(f"#### Total Real: **R$ {tot_p:,.2f}**")

            # =========================================================
            # ✅ FINALIZAR PROVENTOS (BLOCO COMPLETO — rápido + legado)
            # Cole ESTE BLOCO INTEIRO no lugar do seu:
            # if st.button("✅ FINALIZAR PROVENTOS", ...):
            # =========================================================

            if st.button("✅ FINALIZAR PROVENTOS", type="primary", use_container_width=True, key="save_prov"):
                if not st.session_state.lote_prov:
                    st.error("Vazio")
                else:
                    bar = st.progress(0, text="Salvando...")
                    lista = st.session_state.lote_prov

                    # ✅ abre legado 1 vez + estado para next_row (evita scan repetido)
                    ws_prov_legado = None
                    leg_state = {"next_row": None}
                    try:
                        ws_prov_legado = get_ws_proventos_legado()
                    except Exception as e:
                        st.warning(f"⚠️ Não conseguiu abrir legado (proventos): {e}")

                    for idx, item in enumerate(lista):
                        # garante unitário calculado (base nova usa, legado NÃO)
                        item["valor_por_cota"] = item["valor"] / item["quantidade_na_data"] if item["quantidade_na_data"] > 0 else 0.0

                        nid = make_id(item["ticker"], item["tipo"], datetime.now())
                        prov_save = {
                            "id": nid,
                            "portfolio_id": int(PORTFOLIO_ID_PADRAO),
                            "data": item["data_formatada"],
                            "ticker": item["ticker"],
                            "tipo": item["tipo"],
                            "valor": float(item["valor"]),
                            "quantidade_na_data": float(item["quantidade_na_data"]),
                            "valor_por_cota": float(item["valor_por_cota"]),
                            "origem": item["origem"],
                            "criado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }

                        # ✅ 1) BASE NOVA (fonte da verdade)
                        try:
                            append_provento(prov_save)
                        except Exception as e:
                            st.error(f"Erro (base nova) {item['ticker']}: {e}")
                            continue

                        # ✅ 2) LEGADO (rápido): usa ws já aberto + next_row incremental
                        if ws_prov_legado is not None:
                            try:
                                ok_leg = append_provento_legado(prov_save, ws=ws_prov_legado, state=leg_state)
                                if not ok_leg:
                                    st.warning(f"⚠️ Legado não salvou (False): {item['ticker']}")
                            except Exception as e:
                                st.error(f"❌ Erro legado proventos {item['ticker']}: {e}")

                        # ===== SEU CÓDIGO ORIGINAL (continua igual) =====

                        classe_ativo = "fii"
                        try:
                            r = ativos[ativos["ticker"] == item["ticker"]]
                            if not r.empty:
                                classe_ativo = str(r.iloc[0].get("classe", "fii")).strip().lower()
                        except:
                            pass
                        is_fii = (classe_ativo in ["fii", "fiagro"])

                        pm_cons = 0.0
                        try:
                            pm_cons, _, _, _, _ = calcular_cenario_financeiro(movs, item["ticker"], 0, 0, "COMPRA")
                        except:
                            pass

                        p_ref = get_preco_referencia(item["ticker"], cotacoes, movs)

                        yoc, dy, est_tot = 0.0, 0.0, 0.0
                        if is_fii:
                            yoc = (item["valor_por_cota"] / pm_cons * 100) if pm_cons > 0 else 0.0
                            dy = (item["valor_por_cota"] / p_ref * 100) if p_ref > 0 else 0.0
                            est_tot = item["quantidade_na_data"] * item["valor_por_cota"]
                        else:
                            hist_12m = get_trailing_12m_proventos(item["ticker"], proventos)
                            total_12m = hist_12m + item["valor"]
                            cost_tot = pm_cons * item["quantidade_na_data"]
                            val_mkt = item["quantidade_na_data"] * p_ref
                            yoc = (total_12m / cost_tot * 100) if cost_tot > 0 else 0.0
                            dy = (total_12m / val_mkt * 100) if val_mkt > 0 else 0.0

                        status = get_status_comparison(item["ticker"], item["valor_por_cota"], proventos)

                        msg = build_provento_msg(
                            ticker=item["ticker"],
                            data_ref=item["data_formatada"],
                            qtd_total=item["quantidade_na_data"],
                            valor_total=item["valor"],
                            vpc=item["valor_por_cota"],
                            estimativa_total=est_tot,
                            yoc=yoc,
                            dy=dy,
                            preco_atual_ref=p_ref,
                            metodo="Histórico",
                            status_msg=status,
                            classe=classe_ativo,
                        )

                        logo_final = item.get("logo_url", "")
                        if not logo_final:
                            r_l = ativos[ativos["ticker"] == item["ticker"]]
                            if not r_l.empty:
                                cands = ["logo", "logo url", "logo_url", "url", "img"]
                                found = next((c for c in cands if c in r_l.columns), None)
                                if found:
                                    val = r_l.iloc[0][found]
                                    logo_final = str(val).strip() if val else ""

                        send_telegram_message(BOT_TOKEN, CHAT_ID, msg, image_url=logo_final)

                        if is_fii:
                            alrt = check_renda_deviation(item["ticker"], item["valor_por_cota"], proventos)
                            if alrt:
                                m_alrt = build_renda_alert_msg(
                                    alrt["ticker"],
                                    alrt["ultimo_vpc"],
                                    alrt["media_ref"],
                                    alrt["variacao_pct"],
                                    alrt["window"],
                                )
                                send_telegram_message(BOT_TOKEN, CHAT_ID, m_alrt)

                        bar.progress((idx + 1) / len(lista))
                        # 🚀 se quiser ainda mais rápido, pode remover:
                        time.sleep(0.1)

                    summary = build_batch_summary_msg(lista, "PROVENTO")
                    send_telegram_message(BOT_TOKEN, CHAT_ID, summary)

                    st.success("Lote Finalizado!")
                    st.session_state.lote_prov = []
                    time.sleep(1)
                    st.rerun()

        else: st.info("👈 Adicione proventos.")