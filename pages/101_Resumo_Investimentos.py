@ -1,744 +1,977 @@
# app_investimentos_linkado.py — v2 com Cotações Online e Métricas
# -----------------------------------------------------------------------------------
# - UI em cards (CSS leve)
# - Prioriza Service Account (gspread) e cai para CSV só se público
# - Detecta linha de cabeçalho por palavras-chave
# - Normaliza rótulos (acentos/quebras) e padroniza colunas
# - [NOVO] Busca cotações online com yfinance
# - [NOVO] Calcula P/L % e YOC % por ativo
# - [NOVO] Adiciona KPIs de P/L % e Renda Projetada
# pages/04_Adicionar_Operacao.py
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date, datetime
import numpy as np
import re, unicodedata
import yfinance as yf # <-- ADICIONADO

st.set_page_config(page_title="📈 Investimentos – Linkado ao Google Sheets",
                   page_icon="📈", layout="wide")

# ========================
# Tema & CSS de Cards
# ========================
PLOTLY_TEMPLATE = "plotly_dark"

st.markdown("""
import time
import io
import requests

from utils.gsheets import (
    load_ativos,
    load_proventos,
    load_movimentacoes,
    load_cotacoes,
    append_movimentacao,
    append_movimentacao_legado,
    append_provento,
    append_provento_legado,      # <-- OK
    get_ws_proventos_legado,     # <-- OK
)

from utils.telegram import send_telegram_message
from utils.formatters import (
    build_trade_msg,
    build_provento_msg,
    build_renda_alert_msg,
    build_batch_summary_msg,
    fmt_brl,
    fmt_date_br,
)
from utils.estimativas import estimate_next_month_income, get_trailing_12m_proventos
from utils.alerts_insights import check_renda_deviation, get_status_comparison
from utils.ids import make_id
from utils.gsheets import load_proventos_anunciados
from utils.pdf_reports import gerar_e_enviar_pdfs

# ✅ PDF (motor único)
from utils.pdf_reports import build_pdf_executivo, build_pdf_auditoria


st.set_page_config(page_title="Central de Lançamentos", page_icon="⚡", layout="wide")

# --- CSS ESTILO ---
st.markdown(
    """
<style>
.card {
  border-radius: 16px;
  padding: 18px 18px;
  background: rgba(255,255,255,0.03);
  border: 1px solid rgba(255,255,255,0.08);
  box-shadow: 0 6px 20px rgba(0,0,0,0.25);
  margin-bottom: 16px;
}
.kpi { display: flex; flex-direction: column; gap: 4px; }
.kpi .title {font-size: 0.9rem; color: #c9c9c9;}
.kpi .value {font-size: 1.8rem; font-weight: 800;}
.kpi .hint  {font-size: 0.75rem; color: #9aa0a6;}
.card-title { font-weight: 700; font-size: 1.05rem; margin-bottom: 8px; opacity: .95; }
.divider { height: 1px; background: rgba(255,255,255,0.06); margin: 10px 0 16px; }
.small { font-size: .85rem; color: #aeb4ba; }
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

st.markdown("## 📈 Painel de Investimentos – Linkado ao Google Sheets")

# =============================================================================
# Config / secrets
# =============================================================================
SHEET_ID = st.secrets.get("SHEET_ID", "").strip()

ABA_ATIVOS      = st.secrets.get("ABA_ATIVOS", "Meus ativos")
ABA_LANCAMENTOS = st.secrets.get("ABA_LANCAMENTOS", "Compras")
ABA_PROVENTOS   = st.secrets.get("ABA_PROVENTOS", "Proventos")

# GIDs (opcionais, só para fallback CSV público)
ABA_ATIVOS_GID      = str(st.secrets.get("ABA_ATIVOS_GID", "")).strip()
ABA_LANCAMENTOS_GID = str(st.secrets.get("ABA_LANCAMENTOS_GID", "")).strip()
ABA_PROVENTOS_GID   = str(st.secrets.get("ABA_PROVENTOS_GID", "")).strip()

# =============================================================================
# Helpers numéricos / datas
# =============================================================================
def br_to_float(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none", "-", "--"}:
        return None
    s = (s.replace("R$", "").replace("US$", "").replace("$", "")
          .replace("%", "").replace(" ", ""))
    s = s.replace(".", "").replace(",", ".")
""",
    unsafe_allow_html=True,
)

st.title("⚡ Caixa de Lançamentos")

# CONFIG
PORTFOLIO_ID_PADRAO = 1
BOT_TOKEN = st.secrets.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "")

# SESSÃO
if "lote_ops" not in st.session_state:
    st.session_state.lote_ops = []
if "lote_prov" not in st.session_state:
    st.session_state.lote_prov = []
if "last_ticker_op" not in st.session_state:
    st.session_state.last_ticker_op = None
if "last_ticker_prov" not in st.session_state:
    st.session_state.last_ticker_prov = None


# =========================
# HELPERS
# =========================
def _safe_float(x) -> float:
    try:
        if x is None:
            return 0.0
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return 0.0
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return None
        return 0.0

def to_datetime_br(series):
    return pd.to_datetime(series, dayfirst=True, errors="coerce")

def moeda_br(v):
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# =============================================================================
# [NOVA FUNÇÃO] Helper de Cotações
# =============================================================================
@st.cache_data(ttl=900, show_spinner="Buscando cotações...")
def get_preco_atual_yf(ticker: str) -> float | None:
    """Busca o preço atual de um ticker B3 (adiciona .SA)."""
    if not ticker or not isinstance(ticker, str):
        return None
    
    # yfinance espera ".SA" para ações da B3
    ticker_sa = f"{ticker.strip().upper()}.SA" 
    
    try:
        # '1d' pega o último preço de fechamento/negociação
        hist = yf.Ticker(ticker_sa).history(period="1d")
        if not hist.empty:
            return float(hist['Close'].iloc[0])
        # Fallback para "fast_info" se "history" falhar (ex: alguns FIIs)
        fast_info = yf.Ticker(ticker_sa).fast_info
        if fast_info and fast_info.get('lastPrice'):
             return float(fast_info['lastPrice'])
        st.caption(f"Não foi possível achar preço para {ticker_sa}")
        return None
    except Exception:
        st.caption(f"Falha ao buscar {ticker_sa}")
        return None

# =============================================================================
# Normalização de rótulos (para casar cabeçalhos com acentos/quebras)
# =============================================================================
def _norm_label(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s)  # remove quebras de linha e múltiplos espaços
    return s.strip().lower()

def _rename_normalizado(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.rename(columns=lambda c: c if c is None else str(c).strip())
    norm2orig = {}
    for c in df.columns:
        n = _norm_label(c)
        if n and n not in norm2orig:
            norm2orig[n] = c
    df.attrs["__norm2orig__"] = norm2orig
def normalize_df_columns(df):
    """Garante que colunas sejam minúsculas e sem espaços extras."""
    if df is not None and not df.empty:
        df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def _pick(df: pd.DataFrame, *opcoes_norm: str) -> str | None:
    if df is None or df.empty:
        return None
    norm2orig = df.attrs.get("__norm2orig__", {})
    for o in opcoes_norm:
        o_norm = _norm_label(o)
        if o_norm in norm2orig:
            return norm2orig[o_norm]
    return None

def sget(df: pd.DataFrame, *opcoes_norm: str) -> pd.Series:
    """Retorna a coluna como Series; se não existir, devolve uma Series [None] alinhada ao df."""
    if df is None or df.empty:
        return pd.Series([], dtype="object")
    col = _pick(df, *opcoes_norm)
    return df[col] if col else pd.Series([None]*len(df), index=df.index, dtype="object")

# =============================================================================
# Leitura com Service Account (prioritário) + CSV fallback
# =============================================================================
def _has_sa():
    return bool(st.secrets.get("GCP_SERVICE_ACCOUNT") or st.secrets.get("gcp_service_account"))

def _get_sa_info():
    return st.secrets.get("GCP_SERVICE_ACCOUNT") or st.secrets.get("gcp_service_account") or {}

def _find_header_row(values, expect_cols):
    exp = [e.strip().lower() for e in expect_cols]
    best = None
    best_hits = 0
    for i, row in enumerate(values):
        row_low = [str(c).strip().lower() for c in row]
        hits = sum(1 for e in exp if e in row_low)
        if hits > best_hits:
            best_hits, best = hits, i
        if hits >= 2:
            return i
    return best if best is not None else 0

def _read_ws_values(sheet_id: str, aba_nome: str) -> pd.DataFrame:
    import gspread
    from google.oauth2.service_account import Credentials

    info = _get_sa_info()
    if not info:
        raise RuntimeError("Service Account ausente nos secrets.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(aba_nome)
    except Exception:
        titles = [w.title for w in sh.worksheets()]
        match = next((t for t in titles if t.casefold() == aba_nome.casefold()), None)
        if not match:
            raise
        ws = sh.worksheet(match)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    if "provent" in ws.title.lower():
        expect = ["Ticker", "Tipo Provento", "Data"]
    elif "lançamento" in ws.title.lower() or "lancamento" in ws.title.lower() or "compra" in ws.title.lower():
        expect = ["Ticker", "Data", "Tipo", "Tipo de Operação", "Tipo de Operacao"]
    else:
        expect = [c for c in values[0] if str(c).strip()]

    header_idx = _find_header_row(values, expect)
    headers_raw = [h.strip() for h in values[header_idx]]
    seen, headers = {}, []
    for h in headers_raw:
        base = h if h else "col"
        seen[base] = seen.get(base, 0) + 1
        headers.append(base if seen[base] == 1 else f"{base}_{seen[base]}")

    df = pd.DataFrame(values[header_idx + 1 :], columns=headers)
    df = df.replace({"": None}).dropna(axis=1, how="all").dropna(axis=0, how="all")
    return df
def get_current_qty(movs_df: pd.DataFrame, ticker: str) -> float:
    if movs_df is None or movs_df.empty or "ticker" not in movs_df.columns:
        return 0.0

    df = movs_df.copy()
    ticker_upper = str(ticker).upper().strip()
    df["ticker_norm"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df[df["ticker_norm"] == ticker_upper].copy()
    if df.empty:
        return 0.0

    df["quantidade"] = df["quantidade"].apply(_safe_float)
    df["tipo_norm"] = df["tipo"].astype(str).str.upper().str.strip()

    buys = df[df["tipo_norm"] == "COMPRA"]["quantidade"].sum()
    sells = df[df["tipo_norm"] == "VENDA"]["quantidade"].sum()

    saldo = float(buys - sells)
    return saldo if saldo > 0.001 else 0.0


def get_last_paid_price(movs_df: pd.DataFrame, ticker: str) -> float:
    """Retorna o preço unitário da ÚLTIMA COMPRA realizada."""
    if movs_df is None or movs_df.empty:
        return 0.0

    df = movs_df.copy()
    ticker_upper = str(ticker).upper().strip()
    df["ticker_norm"] = df["ticker"].astype(str).str.upper().str.strip()
    df["tipo_norm"] = df["tipo"].astype(str).str.upper().str.strip()

    df = df[(df["ticker_norm"] == ticker_upper) & (df["tipo_norm"] == "COMPRA")]
    if df.empty:
        return 0.0

    if "data" in df.columns:
        df["dt_temp"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
        df = df.sort_values("dt_temp")

def _read_csv_by_gid(sheet_id: str, gid: str) -> pd.DataFrame:
    import urllib.error, pandas as pd
    try:
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
        return pd.read_csv(url)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            return pd.DataFrame()
        raise
        return _safe_float(df.iloc[-1]["preco_unitario"])
    except Exception:
        return pd.DataFrame()
        return 0.0


def get_last_vpc(proventos_df: pd.DataFrame, ticker: str) -> float:
    if proventos_df is None or proventos_df.empty or "ticker" not in proventos_df.columns:
        return 0.0

    t = str(ticker).upper().strip()
    df = proventos_df.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df[df["ticker"] == t].copy()
    if df.empty:
        return 0.0

    if "data" in df.columns:
        df["data_dt"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
        df = df.sort_values("data_dt", ascending=True)

def _read_csv_by_name(sheet_id: str, aba_nome: str) -> pd.DataFrame:
    import urllib.error, pandas as pd
    from urllib.parse import quote
    try:
        aba_enc = quote(aba_nome, safe="")
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={aba_enc}"
        return pd.read_csv(url)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            return pd.DataFrame()
        raise
        last_row = df.iloc[-1]
        vpc = _safe_float(last_row.get("valor_por_cota", 0))
        if vpc <= 0:
            val = _safe_float(last_row.get("valor", 0))
            qtd = _safe_float(last_row.get("quantidade_na_data", 0))
            if qtd > 0:
                vpc = val / qtd
        return float(vpc)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=True)
def ler_aba(sheet_id: str, aba_nome: str, gid: str = "") -> pd.DataFrame:
    if sheet_id and _has_sa():
        try:
            df = _read_ws_values(sheet_id, aba_nome)
            if not df.empty:
                return df
        except Exception:
            pass
    if sheet_id and gid:
        df = _read_csv_by_gid(sheet_id, gid)
        if not df.empty:
            return df
    if sheet_id and aba_nome:
        df = _read_csv_by_name(sheet_id, aba_nome)
        if not df.empty:
            return df
    return pd.DataFrame()

# =============================================================================
# Carregar dados
# =============================================================================
if not SHEET_ID:
    st.error("❌ `SHEET_ID` não definido nos secrets.")
    st.stop()
        return 0.0

with st.spinner("Carregando dados da planilha..."):
    df_ativos_raw = ler_aba(SHEET_ID, ABA_ATIVOS, ABA_ATIVOS_GID)
    df_tx_raw     = ler_aba(SHEET_ID, ABA_LANCAMENTOS, ABA_LANCAMENTOS_GID)
    df_pv_raw     = ler_aba(SHEET_ID, ABA_PROVENTOS, ABA_PROVENTOS_GID)

# =============================================================================
# Padronizações + enriquecimento
# =============================================================================
def padronizar_ativos(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = _rename_normalizado(df)
    out = pd.DataFrame({
        "Ticker":             sget(df, "ticker"),
        "%NaCarteira":        sget(df, "% na carteira"),
        "Quantidade":         sget(df, "quantidade (liquida)", "quantidade (líquida)", "quantidade", "qtd"),
        "PrecoMedioCompra":   sget(df, "preco medio (compra r$)", "preco medio compra r$", "preco medio (compra r$)", "preco medio compra"),
        "PrecoMedioAjustado": sget(df, "preco medio ajustado (r$)"),
        "CotacaoHojeBRL":     sget(df, "cotacao de hoje (r$)"),
        "CotacaoHojeUSD":     sget(df, "cotacao de hoje (us$)"),
        "ValorInvestido":     sget(df, "valor investido"),
        "ValorAtual":         sget(df, "valor atual"),
        "ProventosMes":       sget(df, "proventos (do mes)"),
        "ProventosAnterior":  sget(df, "proventos (anterior)"),
        "ProventosProjetado": sget(df, "proventos (projetado)"),
        "Classe":             sget(df, "classe", "classe do ativo", "tipo"),
    })
    for col in ["%NaCarteira","Quantidade","PrecoMedioCompra","PrecoMedioAjustado",
                "CotacaoHojeBRL","CotacaoHojeUSD","ValorInvestido","ValorAtual",
                "ProventosMes","ProventosAnterior","ProventosProjetado"]:
        out[col] = out[col].map(br_to_float)

    # Fallback de Valor Investido = Quantidade x Preço Médio (Compra)
    if ("ValorInvestido" in out.columns) and (out["ValorInvestido"].isna().all() or out["ValorInvestido"].sum(skipna=True) == 0):
        if "Quantidade" in out and "PrecoMedioCompra" in out:
def get_preco_referencia(ticker: str, cotacoes_df: pd.DataFrame, movs_df: pd.DataFrame) -> float:
    t = str(ticker).upper().strip()

    if cotacoes_df is not None and not cotacoes_df.empty:
        df_c = cotacoes_df.copy()
        row = df_c[df_c["ticker"].astype(str).str.upper().str.strip() == t]
        if not row.empty:
            try:
                out["ValorInvestido"] = out["Quantidade"].astype(float) * out["PrecoMedioCompra"].astype(float)
                for k in ["price", "preco", "close", "cotação", "valor"]:
                    if k in row.columns:
                        val = _safe_float(row.iloc[0][k])
                        if val > 0:
                            return float(val)
            except Exception:
                pass
    return out

def padronizar_lancamentos(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = _rename_normalizado(df)
    out = pd.DataFrame(index=df.index)
    out["Classe"]        = sget(df, "classe", "classe do ativo", "tipo de ativo")
    out["Ticker"]        = sget(df, "ticker")
    out["Data"]          = sget(df, "data", "data (dd/mm/yyyy)")
    out["Tipo"]          = sget(df, "tipo", "tipo de operacao", "tipo de operação", "operacao", "operação")
    out["Quantidade"]    = sget(df, "quantidade", "qtd")
    out["Preco"]         = sget(df, "preco (por unidade)", "preco unitario", "preco unitário", "preco por unidade")
    out["Taxas"]         = sget(df, "taxa", "taxas")
    out["IRRF"]          = sget(df, "irrf")
    out["TotalOperacao"] = sget(df, "total da operacao", "total da operação", "valor bruto", "valor da operacao")

    out["Data"] = to_datetime_br(out["Data"])
    for col in ["Quantidade","Preco","Taxas","IRRF","TotalOperacao"]:
        out[col] = out[col].map(br_to_float)
    if "Tipo" in out.columns:
        out["Tipo"] = out["Tipo"].astype("string").str.upper().str.strip()
    return out

def padronizar_proventos(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = _rename_normalizado(df)
    out = pd.DataFrame(index=df.index)
    out["Data"]          = sget(df, "data")
    out["Ticker"]        = sget(df, "ticker")
    out["Tipo"]          = sget(df, "tipo", "tipo provento")
    out["ValorUnitario"] = sget(df, "unitario r$", "unitario", "unitario rs", "unitario (r$)")
    out["Valor"]         = sget(df, "total liquido r$", "total liquido", "total líquido r$", "total líquido", "valor", "total")
    out["Classe"]        = sget(df, "classe do ativo", "classe")
    out["Quantidade"]    = sget(df, "quantidade", "qtd")
    out["PTAX"]          = sget(df, "ptax")
    out["ValorBruto"]    = sget(df, "total bruto r$", "total bruto")
    out["IRRF"]          = sget(df, "irrf")

    out["Data"] = to_datetime_br(out["Data"])
    for col in ["Quantidade","ValorUnitario","Valor","ValorBruto","IRRF"]:
        out[col] = out[col].map(br_to_float)

    if out["Valor"].isna().all():
        if {"Quantidade","ValorUnitario"}.issubset(out.columns):
            out["Valor"] = out["Quantidade"].fillna(0).astype(float) * out["ValorUnitario"].fillna(0).astype(float)

    if "Tipo" in out.columns:
        out["Tipo"] = out["Tipo"].astype("string").str.upper().str.strip()
    return out

DF_ATIVOS = padronizar_ativos(df_ativos_raw)
TX        = padronizar_lancamentos(df_tx_raw)
PV        = padronizar_proventos(df_pv_raw)

# =============================================================================
# [SESSÃO SUBSTITUÍDA] Fallback de Valor Atual (com yfinance)
# =============================================================================
def _preencher_valor_atual(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """Preenche/ajusta ValorAtual.
    1) Usa ValorAtual se existir;
    2) (NOVO) Senão, Quantidade × Cotação (yfinance);
    3) (Fallback) Senão, Quantidade × CotacaoHojeBRL (planilha);
    4) (Fallback) Senão, usa ValorInvestido (neutro)."""
    
    if df is None or df.empty:
        return df, "Sem dados"

    def _sum_nonzero(s):
        try:
            return pd.to_numeric(s, errors="coerce").fillna(0).sum() if s is not None else 0
        except Exception:
            return 0

    # 1. Tenta usar o ValorAtual da planilha, se já for válido
    if "ValorAtual" in df.columns and _sum_nonzero(df["ValorAtual"]) > 0:
        return df, "Valor atual da planilha"

    # 2. Tenta calcular com yfinance (NOVA LÓGICA)
    if all(c in df.columns for c in ["Quantidade", "Ticker"]):
        df["Preco_YF"] = df["Ticker"].map(get_preco_atual_yf)
        
        # Se achamos pelo menos UMA cotação, usamos essa lógica
        if _sum_nonzero(df["Preco_YF"]) > 0:
            q = pd.to_numeric(df["Quantidade"], errors="coerce").fillna(0)
            c_yf = pd.to_numeric(df["Preco_YF"], errors="coerce").fillna(0)
            df["ValorAtual"] = (q * c_yf).astype(float)
            
            # Onde yfinance falhou, usamos o CotacaoHojeBRL da planilha (se houver)
            if "CotacaoHojeBRL" in df.columns:
                c_planilha = pd.to_numeric(df["CotacaoHojeBRL"], errors="coerce").fillna(0)
                df["ValorAtual"] = df["ValorAtual"].where(df["ValorAtual"] > 0, q * c_planilha)
            
            df = df.drop(columns=["Preco_YF"]) # Limpa a coluna temporária
            return df, "Qtd × Cotação (Online)"

    # 3. Fallback para CotacaoHojeBRL da planilha (lógica antiga)
    if all(c in df.columns for c in ["Quantidade", "CotacaoHojeBRL"]) and _sum_nonzero(df["CotacaoHojeBRL"]) > 0:
        try:
            q = pd.to_numeric(df["Quantidade"], errors="coerce").fillna(0)
            c = pd.to_numeric(df["CotacaoHojeBRL"], errors="coerce").fillna(0)
            df["ValorAtual"] = (q * c).astype(float)
            return df, "Qtd × Cotação (planilha)"
        except Exception:
            pass

    # 4. Fallback final para ValorInvestido (P/L neutro)
    if "ValorInvestido" in df.columns:
        df["ValorAtual"] = pd.to_numeric(df["ValorInvestido"], errors="coerce").fillna(0).astype(float)
        return df, "Sem cotação → usando Investido (neutro)"

    df["ValorAtual"] = 0.0
    return df, "Sem cotação"

DF_ATIVOS, _hint_valor_atual = _preencher_valor_atual(DF_ATIVOS)

# =============================================================================
# Filtros
# =============================================================================
with st.sidebar:
    st.header("Filtros")

    series_datas = []
    for s in [TX.get("Data") if isinstance(TX, pd.DataFrame) else None,
              PV.get("Data") if isinstance(PV, pd.DataFrame) else None]:
        if isinstance(s, pd.Series) and not s.empty:
            s = s.dropna()
            if not s.empty:
                series_datas.append((s.min(), s.max()))
    min_data = (min(s[0] for s in series_datas).date()
                if series_datas else date(2020,1,1))
    max_data = (max(s[1] for s in series_datas).date()
                if series_datas else date.today())

    periodo = st.date_input("Período", value=(min_data, max_data),
                             min_value=min_data, max_value=max_data)

    def uniq(series_list):
        vals = pd.Series(dtype="object")
        for s in series_list:
            if isinstance(s, pd.Series) and not s.empty:
                vals = pd.concat([vals, s.dropna().astype(str)])
        return sorted(vals.unique().tolist())

    classes = uniq([TX.get("Classe"), PV.get("Classe"), DF_ATIVOS.get("Classe")])
    classe_sel = st.multiselect("Classe", options=classes, default=classes if classes else [])
    tickers = uniq([TX.get("Ticker"), PV.get("Ticker"), DF_ATIVOS.get("Ticker")])
    ticker_sel = st.multiselect("Ticker", options=tickers, default=[])

# aplica filtros
if isinstance(periodo, tuple) and len(periodo) == 2:
    d0, d1 = periodo
else:
    d0, d1 = min_data, max_data

# Dataframes filtrados (DF_ATIVOS é filtrado depois, pois os KPIs o usam)
TX_filtrado = TX.copy()
PV_filtrado = PV.copy()

if not TX_filtrado.empty and "Data" in TX_filtrado.columns:
    TX_filtrado = TX_filtrado[TX_filtrado["Data"].notna()]
    TX_filtrado = TX_filtrado[(TX_filtrado["Data"].dt.date >= d0) & (TX_filtrado["Data"].dt.date <= d1)]

if not PV_filtrado.empty and "Data" in PV_filtrado.columns:
    PV_filtrado = PV_filtrado[PV_filtrado["Data"].notna()]
    PV_filtrado = PV_filtrado[(PV_filtrado["Data"].dt.date >= d0) & (PV_filtrado["Data"].dt.date <= d1)]

if classe_sel:
    if "Classe" in DF_ATIVOS.columns:
        DF_ATIVOS = DF_ATIVOS[DF_ATIVOS["Classe"].isin(classe_sel)]
    if not TX_filtrado.empty and "Classe" in TX_filtrado.columns:
        TX_filtrado = TX_filtrado[TX_filtrado["Classe"].isin(classe_sel)]
    if not PV_filtrado.empty and "Classe" in PV_filtrado.columns:
        PV_filtrado = PV_filtrado[PV_filtrado["Classe"].isin(classe_sel)]

if ticker_sel:
    if "Ticker" in DF_ATIVOS.columns:
        DF_ATIVOS = DF_ATIVOS[DF_ATIVOS["Ticker"].isin(ticker_sel)]
    if not TX_filtrado.empty and "Ticker" in TX_filtrado.columns:
        TX_filtrado = TX_filtrado[TX_filtrado["Ticker"].isin(ticker_sel)]
    if not PV_filtrado.empty and "Ticker" in PV_filtrado.columns:
        PV_filtrado = PV_filtrado[PV_filtrado["Ticker"].isin(ticker_sel)]

# =============================================================================
# [SESSÃO SUBSTITUÍDA] Cabeçalho em cards (KPIs)
# =============================================================================
with st.container():
    c1, c2, c3, c4, c5 = st.columns(5) # <-- MUDADO PARA 5 COLUNAS

    # KPIs usam DF_ATIVOS filtrado
    v_investido = float(pd.Series(DF_ATIVOS.get("ValorInvestido", pd.Series(dtype=float))).sum(skipna=True) or 0)
    v_atual     = float(pd.Series(DF_ATIVOS.get("ValorAtual",     pd.Series(dtype=float))).sum(skipna=True) or 0)
    pl          = v_atual - v_investido
    pl_perc     = (pl / v_investido) * 100 if v_investido != 0 else 0
    
    # KPI de Proventos usa PV_filtrado
    renda       = float(pd.Series(PV_filtrado.get("Valor", pd.Series(dtype=float))).sum(skipna=True) or 0)
    
    # KPI de Renda Projetada usa DF_ATIVOS filtrado
    v_proj_mes = float(pd.Series(DF_ATIVOS.get("ProventosProjetado", pd.Series(dtype=float))).sum(skipna=True) or 0)

    with c1:
        st.markdown(f"""
        <div class="card kpi">
          <div class="title">Valor Investido</div>
          <div class="value">{moeda_br(v_investido)}</div>
          <div class="hint">Soma da carteira filtrada</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="card kpi">
          <div class="title">Valor Atual</div>
          <div class="value">{moeda_br(v_atual)}</div>
          <div class="hint">{_hint_valor_atual}</div>
        </div>
        """, unsafe_allow_html=True)

    with c3: # <-- CARD MODIFICADO
        st.markdown(f"""
        <div class="card kpi">
          <div class="title">P/L Latente</div>
          <div class="value" style="color: {'#00f2a9' if pl >= 0 else '#f63366'};">
            {moeda_br(pl)}
          </div>
          <div class="hint" style="color: {'#00f2a9' if pl >= 0 else '#f63366'};">
            {'Lucro' if pl >= 0 else 'Prejuízo'} ({pl_perc:+.2f}%)
          </div>
        </div>
        """, unsafe_allow_html=True)

    with c4:
        st.markdown(f"""
        <div class="card kpi">
          <div class="title">Proventos no Período</div>
          <div class="value">{moeda_br(renda)}</div>
          <div class="hint">Somatório de {d0.strftime('%d/%m/%Y')} a {d1.strftime('%d/%m/%Y')}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with c5: # <-- NOVO CARD
        st.markdown(f"""
        <div class="card kpi">
          <div class="title">Renda Projetada (Mês)</div>
          <div class="value">{moeda_br(v_proj_mes)}</div>
          <div class="hint">Dos ativos filtrados</div>
        </div>
        """, unsafe_allow_html=True)

# =============================================================================
# [SESSÃO SUBSTITUÍDA] Carteira
# =============================================================================
st.markdown('<div class="card"><div class="card-title">📦 Carteira Atual</div>', unsafe_allow_html=True)
if DF_ATIVOS.empty:
    st.caption("Sem dados na aba de ativos (confira filtros).")
else:
    # --- NOVOS CÁLCulos ---
    # 1. Calcular P/L por ativo
    DF_ATIVOS["PL_Absoluto"] = DF_ATIVOS["ValorAtual"] - DF_ATIVOS["ValorInvestido"]
    DF_ATIVOS["PL_%"] = (DF_ATIVOS["PL_Absoluto"] / DF_ATIVOS["ValorInvestido"].replace(0, np.nan))

    # 2. Calcular YOC (Yield on Cost) do período (usa PV_filtrado)
    if not PV_filtrado.empty and "Valor" in PV_filtrado and "Ticker" in PV_filtrado:
        proventos_por_ticker = PV_filtrado.groupby("Ticker")["Valor"].sum()
        DF_ATIVOS["Proventos_Periodo"] = DF_ATIVOS["Ticker"].map(proventos_por_ticker).fillna(0)
        DF_ATIVOS["YOC_%"] = (DF_ATIVOS["Proventos_Periodo"] / DF_ATIVOS["ValorInvestido"].replace(0, np.nan))
    else:
        DF_ATIVOS["Proventos_Periodo"] = 0.0
        DF_ATIVOS["YOC_%"] = 0.0
    # --- FIM NOVOS CÁLCULOS ---

    gcol1, gcol2 = st.columns([1,1])
    if "ValorAtual" in DF_ATIVOS.columns and DF_ATIVOS["ValorAtual"].notna().any():
        aloc_ticker = DF_ATIVOS.groupby("Ticker", dropna=False)["ValorAtual"].sum().reset_index()
        if not aloc_ticker.empty:
            fig = px.pie(aloc_ticker, names="Ticker", values="ValorAtual", hole=0.4,
                         template=PLOTLY_TEMPLATE, title="Alocação por Ticker (Valor Atual)")
            gcol1.plotly_chart(fig, use_container_width=True, theme=None)
            
    if all(c in DF_ATIVOS.columns for c in ["Classe","ValorAtual"]):
        aloc_classe = DF_ATIVOS.dropna(subset=["Classe"]).groupby("Classe")["ValorAtual"].sum().reset_index()
        if not aloc_classe.empty:
            fig = px.bar(aloc_classe, x="Classe", y="ValorAtual", template=PLOTLY_TEMPLATE,
                         title="Alocação por Classe (Valor Atual)")
            fig.update_layout(yaxis_title="R$")
            gcol2.plotly_chart(fig, use_container_width=True, theme=None)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    
    # --- [NOVA LÓGICA DE SEGURANÇA v3 - FORÇANDO O TIPO FLOAT] ---
    
    # 1. Para P/L %
    min_pl_raw = DF_ATIVOS["PL_%"].min(skipna=True)
    max_pl_raw = DF_ATIVOS["PL_%"].max(skipna=True)

    if pd.isna(min_pl_raw):
        min_pl = -1.0
    if movs_df is not None and not movs_df.empty:
        return float(get_last_paid_price(movs_df, t))

    return 0.0


def calcular_cenario_financeiro(df_movs, ticker, qtd_op, preco_op, tipo_op):
    if df_movs is None or df_movs.empty:
        return 0.0, 0.0, 0.0, 0.0, 0.0

    df = df_movs.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    t = str(ticker).upper().strip()
    df = df[df["ticker"] == t].copy()

    total_custo, total_qtd = 0.0, 0.0

    if not df.empty:
        for _, row in df.iterrows():
            q = _safe_float(row.get("quantidade"))
            p = _safe_float(row.get("preco_unitario"))
            tp = str(row.get("tipo")).upper().strip()

            if tp == "COMPRA":
                total_custo += q * p
                total_qtd += q
            elif tp == "VENDA" and total_qtd > 0:
                pm = total_custo / total_qtd
                total_custo -= q * pm
                total_qtd -= q

    q_op, p_op = float(qtd_op), float(preco_op)
    tp_op = str(tipo_op).upper().strip()

    if tp_op == "COMPRA":
        total_custo += q_op * p_op
        total_qtd += q_op
    elif tp_op == "VENDA" and total_qtd > 0:
        pm = total_custo / total_qtd
        total_custo -= q_op * pm
        total_qtd -= q_op

    pm_final = (total_custo / total_qtd) if total_qtd > 0 else 0.0
    val_mkt = total_qtd * p_op
    res_fin = val_mkt - total_custo
    res_pct = (res_fin / total_custo * 100) if total_custo > 0 else 0.0

    return float(pm_final), float(total_custo), float(val_mkt), float(res_fin), float(res_pct)


def calcular_totais_impacto(movs_df, lote_atual, ativos_df, proventos_df):
    if not lote_atual:
        return {}, 0.0

    lista_itens = []
    if isinstance(lote_atual, pd.DataFrame):
        for _, row in lote_atual.iterrows():
            d = row.to_dict()
            dt_val = d.get("data")
            if isinstance(dt_val, str):
                try:
                    dt_val = datetime.strptime(dt_val, "%d/%m/%Y").date()
                except Exception:
                    try:
                        dt_val = datetime.strptime(dt_val, "%Y-%m-%d").date()
                    except Exception:
                        dt_val = date.today()
            d["data_obj"] = dt_val
            lista_itens.append(d)
    else:
        min_pl = float(min_pl_raw)  # Força ser um float padrão
        lista_itens = lote_atual

    if not lista_itens:
        return {}, 0.0

    data_ref = lista_itens[0].get("data_obj")
    if not isinstance(data_ref, (date, datetime)):
        data_ref = date.today()

    mes_ref, ano_ref = data_ref.month, data_ref.year
    impacto_dia = {}
    total_mes_recalc = 0.0

    if movs_df is not None and not movs_df.empty:
        df_mes = movs_df.copy()
        df_mes["data_dt"] = pd.to_datetime(df_mes["data"], dayfirst=True, errors="coerce")
        df_mes = df_mes[
            (df_mes["data_dt"].dt.month == mes_ref)
            & (df_mes["data_dt"].dt.year == ano_ref)
            & (df_mes["tipo"].astype(str).str.upper().str.strip() == "COMPRA")
        ]

        for _, row in df_mes.iterrows():
            ticker = str(row["ticker"]).upper().strip()
            r_cls = ativos_df[ativos_df["ticker"] == ticker]
            if r_cls.empty:
                continue
            classe = str(r_cls.iloc[0].get("classe", "")).lower().strip()
            if classe in ["fii", "fiagro"]:
                qtd = _safe_float(row["quantidade"])
                vpc = get_last_vpc(proventos_df, ticker)
                if vpc > 0:
                    total_mes_recalc += (qtd * vpc)

        if isinstance(lote_atual, list):
            for item in lista_itens:
                ticker = item["ticker"]
                r_cls = ativos_df[ativos_df["ticker"] == ticker]
                classe = str(r_cls.iloc[0].get("classe", "")).lower().strip() if not r_cls.empty else ""
                if str(item["tipo"]).upper() == "COMPRA" and classe in ["fii", "fiagro"]:
                    qtd = float(item["quantidade"])
                    vpc = get_last_vpc(proventos_df, ticker)
                    if vpc > 0:
                        total_mes_recalc += (qtd * vpc)

    total_mes = float(total_mes_recalc)

    for item in lista_itens:
        ticker = item["ticker"]
        tp = str(item["tipo"]).upper()
        r_cls = ativos_df[ativos_df["ticker"] == ticker]
        classe = str(r_cls.iloc[0].get("classe", "")).lower().strip() if not r_cls.empty else ""

        if tp == "COMPRA" and classe in ["fii", "fiagro"]:
            qtd = _safe_float(item.get("quantidade", 0))
            vpc = get_last_vpc(proventos_df, ticker)
            if vpc > 0:
                imp = qtd * vpc
                impacto_dia[ticker] = impacto_dia.get(ticker, 0.0) + imp

    return impacto_dia, total_mes


# =========================
# CARGAS
# =========================
df_ativos_raw = pd.DataFrame(load_ativos())
df_proventos_raw = pd.DataFrame(load_proventos())
df_movs_raw = pd.DataFrame(load_movimentacoes())
df_cotacoes_raw = pd.DataFrame(load_cotacoes())

ativos = normalize_df_columns(df_ativos_raw)
proventos = normalize_df_columns(df_proventos_raw)
movs = normalize_df_columns(df_movs_raw)
cotacoes = normalize_df_columns(df_cotacoes_raw)

if ativos.empty:
    st.error("Base de ativos vazia.")
    st.stop()

    if pd.isna(max_pl_raw):
        max_pl = 1.0
    else:
        max_pl = float(max_pl_raw)  # Força ser um float padrão
    
    # Garante que min é menor que max
    if min_pl >= max_pl:
        if min_pl == max_pl: # Se forem iguais (ex: 0.0)
            min_pl = min_pl - 0.5
            max_pl = max_pl + 0.5
        else: # Se min > max (improvável, mas cobre o caso)
             min_pl, max_pl = max_pl, min_pl # Inverte

    # 2. Para YOC %
    max_yoc_raw = DF_ATIVOS["YOC_%"].max(skipna=True)
    if pd.isna(max_yoc_raw) or max_yoc_raw <= 0:
        max_yoc = 0.15 # Default de 15%
    else:
        max_yoc = float(max_yoc_raw) # Força ser um float padrão
    # --- [FIM DA NOVA LÓGICA] ---


    # --- DATAFRAME COM COLUMN_CONFIG CORRIGIDO ---
    st.dataframe(
        DF_ATIVOS, 
        use_container_width=True,
        column_config={
            "Ticker": st.column_config.TextColumn("Ativo"),
            "PL_%": st.column_config.ProgressColumn(
                "P/L %",
                help="Lucro/Prejuízo percentual (Valor Atual / Valor Investido)",
                format="%.2f%%",
                min_val=min_pl,  # <-- CORRIGIDO
                max_val=max_pl,  # <-- CORRIGIDO
            ),
            "YOC_%": st.column_config.ProgressColumn(
                "YOC (Período) %",
                help="Yield on Cost no período filtrado (Proventos / Valor Investido)",
                format="%.2f%%",
                min_val=0,
                max_val=max_yoc, # <-- CORRIGIDO
            ),
            "ValorInvestido": st.column_config.NumberColumn("Investido", format="R$ %.2f"),
            "ValorAtual": st.column_config.NumberColumn("Valor Atual", format="R$ %.2f"),
            "PL_Absoluto": st.column_config.NumberColumn("P/L", format="R$ %.2f"),
            "Proventos_Periodo": st.column_config.NumberColumn("Proventos (Per.)", format="R$ %.2f"),
            "Quantidade": st.column_config.NumberColumn("Qtd.", format="%.0f"),
            "Classe": st.column_config.TextColumn("Classe"),
            # Ocultar colunas que não queremos ver
            "%NaCarteira": None,
            "PrecoMedioCompra": None,
            "PrecoMedioAjustado": None,
            "CotacaoHojeBRL": None,
            "CotacaoHojeUSD": None,
            "ProventosMes": None,
            "ProventosAnterior": None,
            "ProventosProjetado": None,
        },
        hide_index=True
    )
st.markdown('</div>', unsafe_allow_html=True)


# =============================================================================
# Aportes x Retiradas
# =============================================================================
st.markdown('<div class="card"><div class="card-title">💸 Aportes x Retiradas (mensal)</div>', unsafe_allow_html=True)
if TX_filtrado.empty:
    st.caption("Sem dados em lançamentos (confira filtros).")
else:
    mov = TX_filtrado.copy() # Usa o DF filtrado
    if "TotalOperacao" in mov.columns and mov["TotalOperacao"].notna().any():
        mov["Valor"] = mov["TotalOperacao"].fillna(0)
    else:
        qty = mov["Quantidade"] if "Quantidade" in mov else 0
        prc = mov["Preco"] if "Preco" in mov else 0
        mov["Valor"] = (qty.fillna(0) if isinstance(qty, pd.Series) else qty) * \
                       (prc.fillna(0) if isinstance(prc, pd.Series) else prc)
    
    mov.loc[mov["Tipo"]=="VENDA", "Valor"] *= -1
    mov.loc[mov["Tipo"]=="RETIRADA", "Valor"] *= -1
    mov = mov[(mov["Tipo"].isin(["COMPRA","VENDA","APORTE","RETIRADA"])) & mov["Data"].notna()]
    
    if mov.empty:
        st.caption("Nenhum movimento válido no período.")
    else:
        grp = mov.assign(Ano=mov["Data"].dt.year, Mes=mov["Data"].dt.month)
        grp = grp.groupby(["Ano","Mes"], dropna=False)["Valor"].sum().reset_index()
        grp["Competencia"] = pd.to_datetime(grp["Ano"].astype(str)+"-"+grp["Mes"].astype(str)+"-01")
        fig = px.bar(grp, x="Competencia", y="Valor", template=PLOTLY_TEMPLATE,
                     title="Fluxo de Caixa Mensal (Aportes líquidos)")
        fig.update_layout(xaxis_title="Competência", yaxis_title="R$")
        st.plotly_chart(fig, use_container_width=True, theme=None)

    with st.expander("Ver lançamentos filtrados"):
        st.dataframe(TX_filtrado.sort_values("Data"), use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)

# =============================================================================
# Proventos
# =============================================================================
st.markdown('<div class="card"><div class="card-title">💰 Proventos (mensal)</div>', unsafe_allow_html=True)
if PV_filtrado.empty:
    st.caption("Sem dados em 'Proventos' (confira filtros).")
else:
    pv = PV_filtrado.dropna(subset=["Data"]).copy() # Usa o DF filtrado
    if pv.empty:
        st.caption("Registros de proventos sem data.")
    else:
        grp = pv.assign(Ano=pv["Data"].dt.year, Mes=pv["Data"].dt.month)
        grp = grp.groupby(["Ano","Mes"], dropna=False)["Valor"].sum().reset_index()
        grp["Competencia"] = pd.to_datetime(grp["Ano"].astype(str)+"-"+grp["Mes"].astype(str)+"-01")
        fig = px.bar(grp, x="Competencia", y="Valor", template=PLOTLY_TEMPLATE, title="Proventos por Mês")
        fig.update_layout(xaxis_title="Competência", yaxis_title="R$")
        st.plotly_chart(fig, use_container_width=True, theme=None)

        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        st.caption("Proventos por Ticker no período filtrado")
        tab = pv.groupby(["Ticker","Tipo"], dropna=False)["Valor"].sum().reset_index().sort_values("Valor", ascending=False)
        st.dataframe(tab, hide_index=True, use_container_width=True)

    with st.expander("Ver proventos filtrados"):
        st.dataframe(PV_filtrado.sort_values("Data"), use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)
ativos["ticker"] = ativos["ticker"].astype(str).str.upper().str.strip()
todos_tickers = sorted([t for t in ativos["ticker"].unique().tolist() if t and t != "nan"])

if not proventos.empty:
    proventos["ticker"] = proventos["ticker"].astype(str).str.upper().str.strip()
if not movs.empty:
    movs["ticker"] = movs["ticker"].astype(str).str.upper().str.strip()

tickers_em_carteira = []
if not movs.empty:
    for t in todos_tickers:
        q = get_current_qty(movs, t)
        if q > 0:
            tickers_em_carteira.append(t)

carteira_detectada = len(tickers_em_carteira) > 0


# =========================
# GERENCIADOR DE ENVIOS
# =========================
with st.expander("🔄 GERENCIADOR DE ENVIOS (Histórico & Testes)", expanded=False):
    t_hist, t_test = st.tabs(["📜 Reenviar do Histórico", "🧪 Testar Rascunho"])

    with t_hist:
        c_h1, c_h2 = st.columns(2)
        with c_h1:
            datas_disponiveis = []
            if not movs.empty and "data" in movs.columns:
                movs["data_dt"] = pd.to_datetime(movs["data"], dayfirst=True, errors="coerce")
                datas_disponiveis = sorted(movs["data_dt"].dropna().unique(), reverse=True)
                datas_str = [d.strftime("%d/%m/%Y") for d in datas_disponiveis]
            sel_data_str = (
                st.selectbox("Selecione a Data", datas_str, key="hist_date_sel")
                if datas_disponiveis
                else st.selectbox("Sem datas", [])
            )

        with c_h2:
            st.write("")
            st.write("")
            btn_reenviar_ops = st.button("🛒 Reenviar Operações", use_container_width=True, disabled=not datas_disponiveis)

        if btn_reenviar_ops and sel_data_str:
            sel_date_obj = datetime.strptime(sel_data_str, "%d/%m/%Y")
            df_dia = movs[movs["data_dt"] == sel_date_obj].copy()

            if df_dia.empty:
                st.warning("Nenhuma operação encontrada.")
            else:
                lista_hist = []
                for _, row in df_dia.iterrows():
                    d = row.to_dict()
                    d["data_formatada"] = sel_data_str
                    d["data_obj"] = sel_date_obj
                    d["valor_total"] = _safe_float(d.get("valor_total", 0)) or (
                        _safe_float(d.get("quantidade", 0)) * _safe_float(d.get("preco_unitario", 0))
                    )

                    r_l = ativos[ativos["ticker"] == d["ticker"]]
                    if not r_l.empty:
                        d["logo_url"] = str(r_l.iloc[0].get("logo_url", "")).strip()
                        d["classe"] = str(r_l.iloc[0].get("classe", "")).lower().strip()
                    else:
                        d["logo_url"] = ""
                        d["classe"] = ""

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
                if not st.session_state.lote_ops:
                    st.warning("Lote vazio.")
                else:
                    st.toast("Cards simulados!")

            if st.button("📑 Testar Resumo (Ops)"):
                if not st.session_state.lote_ops:
                    st.warning("Lote vazio.")
                else:
                    dict_dia, tot_mes = calcular_totais_impacto(movs, st.session_state.lote_ops, ativos, proventos)
                    summary = build_batch_summary_msg(st.session_state.lote_ops, "OPERACAO", impacto_dia_dados=dict_dia, total_impacto_mes=tot_mes)
                    send_telegram_message(BOT_TOKEN, CHAT_ID, summary)
                    st.toast("Resumo simulado!")


tab1, tab2 = st.tabs(["🛒 Compras & Vendas", "💰 Proventos"])


# =========================
# TAB 1: OPERAÇÕES
# =========================
with tab1:
    c_form, c_list = st.columns([1, 1.3])

    with c_form:
        mostrar_todos = st.checkbox(
            "🔎 Buscar na Lista Completa (Novos Ativos)",
            value=False,
            help="Marque para buscar ativos que você ainda não tem na carteira.",
        )

        if (not mostrar_todos) and carteira_detectada:
            lista_op = tickers_em_carteira
        else:
            lista_op = todos_tickers

        ticker = st.selectbox("Buscar Ativo", lista_op, key="op_ticker")

        if ticker and ticker != st.session_state.last_ticker_op:
            ultimo_preco_pago = get_last_paid_price(movs, ticker)
            if ultimo_preco_pago > 0:
                st.session_state["op_preco"] = float(ultimo_preco_pago)
                st.toast(f"Último preço pago: R$ {ultimo_preco_pago:.2f}", icon="💡")
            st.session_state.last_ticker_op = ticker
            st.rerun()

        logo_url = ""
        r = ativos[ativos["ticker"] == ticker]
        if not r.empty:
            cands = ["logo", "logo url", "logo_url", "url", "img"]
            found = next((c for c in cands if c in r.columns), None)
            if found:
                val = r.iloc[0][found]
                logo_url = str(val).strip() if val else ""

        with st.container(border=True):
            col_img, col_info = st.columns([1, 3])
            with col_img:
                if logo_url:
                    st.image(logo_url, use_container_width=True)
                else:
                    st.info("📷")
            with col_info:
                st.markdown(f"## {ticker}")
                qtd_atual = get_current_qty(movs, ticker)
                st.caption(f"Em carteira: **{qtd_atual:g}**")

        dt = st.date_input("Data", value=date.today(), key="op_dt")
        tipo = st.selectbox("Tipo", ["COMPRA", "VENDA"], key="op_tipo")

        c1, c2 = st.columns(2)
        with c1:
            qtd = st.number_input("Qtd", min_value=0.0, step=1.0, key="op_qtd")
        with c2:
            preco = st.number_input("Preço (R$)", min_value=0.0, step=0.01, format="%.2f", key="op_preco")

        c3, c4 = st.columns(2)
        with c3:
            taxa = st.number_input("Taxas (R$)", min_value=0.0, step=0.01, format="%.2f", key="op_taxa")
        with c4:
            origem = st.text_input("Origem", value="manual", key="op_orig")

        obs = st.text_input("Observação", key="op_obs")
        st.write("")

        if st.button("⬇️ ADICIONAR AO LOTE", use_container_width=True, type="secondary"):
            if qtd <= 0 or preco <= 0:
                st.error("Qtd/Preço > 0")
            else:
                fin = (qtd * preco) + taxa
                st.session_state.lote_ops.append(
                    {
                        "data_formatada": dt.strftime("%d/%m/%Y"),
                        "data_obj": dt,
                        "ticker": ticker,
                        "tipo": tipo,
                        "quantidade": float(qtd),
                        "preco_unitario": float(preco),
                        "taxa": float(taxa),
                        "valor_total": float(fin),
                        "origem": origem,
                        "observacao": obs,
                        "logo_url": logo_url,
                    }
                )
                st.toast(f"{ticker} adicionado!", icon="🛒")

    with c_list:
        st.markdown("### 🛒 Lista de Lançamentos")

        if len(st.session_state.lote_ops) > 0:
            df_lote = pd.DataFrame(st.session_state.lote_ops)
            edited_df = st.data_editor(
                df_lote,
                column_config={
                    "ticker": st.column_config.TextColumn("Ativo", disabled=True),
                    "tipo": st.column_config.SelectboxColumn("Tipo", options=["COMPRA", "VENDA"]),
                    "quantidade": st.column_config.NumberColumn("Qtd", min_value=0.01),
                    "preco_unitario": st.column_config.NumberColumn("Preço", format="R$ %.2f"),
                    "valor_total": st.column_config.NumberColumn("Total (Calc)", disabled=True, format="R$ %.2f"),
                },
                column_order=["ticker", "tipo", "data_formatada", "quantidade", "preco_unitario", "valor_total"],
                num_rows="dynamic",
                use_container_width=True,
                key="editor_ops",
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
                        item["valor_total"] = (float(item["quantidade"]) * float(item["preco_unitario"])) + float(item.get("taxa", 0))

                        classe_ativo = "FII"
                        try:
                            r_cls = ativos[ativos["ticker"] == item["ticker"]]
                            if not r_cls.empty:
                                classe_ativo = str(r_cls.iloc[0].get("classe", "FII")).strip()
                        except Exception:
                            pass
                        item["classe"] = classe_ativo

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

                        try:
                            append_movimentacao(mov_to_save)
                        except Exception as e:
                            st.error(f"Erro (base nova) {item['ticker']}: {e}")
                            continue

                        try:
                            append_movimentacao_legado(
                                {
                                    "ticker": item["ticker"],
                                    "data": item["data_formatada"],
                                    "tipo": item["tipo"],
                                    "quantidade": item["quantidade"],
                                    "preco_unitario": item["preco_unitario"],
                                }
                            )
                        except Exception as e:
                            st.warning(f"⚠️ Espelho falhou (base antiga) {item['ticker']}: {e}")

                        logo_final = item.get("logo_url", "")
                        if not logo_final:
                            r_l = ativos[ativos["ticker"] == item["ticker"]]
                            if not r_l.empty:
                                cands = ["logo", "logo url", "logo_url", "url", "img"]
                                found = next((c for c in cands if c in r_l.columns), None)
                                if found:
                                    val = r_l.iloc[0][found]
                                    logo_final = str(val).strip() if val else ""

                        qty_atual = get_current_qty(movs, item["ticker"])
                        qty_pos = qty_atual + (item["quantidade"] if item["tipo"] == "COMPRA" else -item["quantidade"])

                        classe_ativo_norm = str(classe_ativo).lower()
                        is_fii = classe_ativo_norm in ["fii", "fiagro"]
                        item["classe"] = classe_ativo_norm

                        est_val, est_met, est_base, vpc_last = 0.0, "", "", 0.0
                        try:
                            est_val, est_met, est_base, vpc_last = estimate_next_month_income(item["ticker"], qty_pos, proventos, ativos)
                        except Exception:
                            pass

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
                        time.sleep(0.2)

                    summary = build_batch_summary_msg(lista, "OPERACAO", impacto_dia_dados=dict_dia, total_impacto_mes=tot_mes)
                    send_telegram_message(BOT_TOKEN, CHAT_ID, summary)

                    st.success("Lote Finalizado!")
                    st.session_state.lote_ops = []
                    time.sleep(1)
                    st.rerun()
        else:
            st.info("👈 Adicione itens.")


# =========================
# TAB 2: PROVENTOS
# =========================
with tab2:
    cp_form, cp_list = st.columns([1, 1.3])

    with cp_form:
        lista_prov = tickers_em_carteira if tickers_em_carteira else todos_tickers
        ticker_p = st.selectbox("Buscar Ativo", lista_prov, key="prov_ticker")

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
            if found:
                val = rp.iloc[0][found]
                logo_url_p = str(val).strip() if val else ""

        with st.container(border=True):
            col_img, col_info = st.columns([1, 3])
            with col_img:
                if logo_url_p:
                    st.image(logo_url_p, use_container_width=True)
                else:
                    st.info("📷")
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
            if valor_p <= 0 or qtd_na_data <= 0:
                st.error("Valores devem ser > 0")
            else:
                t_final = "JCP" if str(tipo_p).upper() == "JCP" else str(tipo_p).upper()
                st.session_state.lote_prov.append(
                    {
                        "data_formatada": dtp.strftime("%d/%m/%Y"),
                        "data_obj": dtp,
                        "ticker": ticker_p,
                        "tipo": t_final,
                        "valor": float(valor_p),
                        "quantidade_na_data": float(qtd_na_data),
                        "valor_por_cota": float(vpc_calc),
                        "origem": origem_p,
                        "logo_url": logo_url_p,
                    }
                )
                st.toast(f"{ticker_p} adicionado!", icon="💰")

    with cp_list:
        st.markdown("### 💰 Lista de Proventos")

        if len(st.session_state.lote_prov) > 0:
            df_lp = pd.DataFrame(st.session_state.lote_prov)
            edited_prov = st.data_editor(
                df_lp,
                column_config={
                    "ticker": st.column_config.TextColumn("Ativo", disabled=True),
                    "tipo": st.column_config.SelectboxColumn("Tipo", options=["DIVIDENDO", "JCP", "RENDIMENTO", "AMORTIZACAO"]),
                    "valor": st.column_config.NumberColumn("Total Recebido", min_value=0.01, format="R$ %.2f", required=True),
                    "quantidade_na_data": st.column_config.NumberColumn("Qtd", min_value=0.01, step=1.0, required=True),
                    "valor_por_cota": st.column_config.NumberColumn("Unitário", disabled=True, format="R$ %.2f"),
                },
                column_order=["ticker", "tipo", "data_formatada", "quantidade_na_data", "valor", "valor_por_cota"],
                num_rows="dynamic",
                use_container_width=True,
                key="editor_prov",
            )
            st.session_state.lote_prov = edited_prov.to_dict("records")

            if not edited_prov.empty:
                tot_p = edited_prov["valor"].sum()
                st.markdown(f"#### Total Real: **R$ {tot_p:,.2f}**")

            if st.button("✅ FINALIZAR PROVENTOS", type="primary", use_container_width=True, key="save_prov"):
                if not st.session_state.lote_prov:
                    st.error("Vazio")
                else:
                    bar = st.progress(0, text="Salvando...")
                    lista = st.session_state.lote_prov
                    lista_para_resumo = []

                    ws_prov_legado = None
                    try:
                        ws_prov_legado = get_ws_proventos_legado()
                    except Exception as e:
                        st.warning(f"⚠️ Não conseguiu abrir legado (proventos): {e}")

                    for idx, item in enumerate(lista):
                        item["valor_por_cota"] = item["valor"] / item["quantidade_na_data"] if item["quantidade_na_data"] > 0 else 0.0
                        lista_para_resumo.append(item.copy())

                        nid = make_id(item["ticker"], item["tipo"], datetime.now())
                        prov_save = {
                            "id": nid,
                            "portfolio_id": int(PORTFOLIO_ID_PADRAO),
                            "data": item["data_formatada"],
                            "ticker": item["ticker"],
                            "tipo": str(item["tipo"]).upper(),
                            "valor": float(item["valor"]),
                            "quantidade_na_data": float(item["quantidade_na_data"]),
                            "valor_por_cota": float(item["valor_por_cota"]),
                            "origem": item["origem"],
                            "criado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }

                        # ✅ 1) BASE NOVA
                        try:
                            append_provento(prov_save)
                        except Exception as e:
                            st.error(f"Erro (base nova) {item['ticker']}: {e}")
                            continue

                        # ✅ 2) LEGADO (compatível com versão antiga: SEM state)
                        if ws_prov_legado is not None:
                            try:
                                ok_leg = append_provento_legado(prov_save, ws=ws_prov_legado)
                                if not ok_leg:
                                    st.warning(f"⚠️ Legado não salvou (False): {item['ticker']}")
                            except Exception as e:
                                st.warning(f"⚠️ Erro legado proventos {item['ticker']}: {e}")

                        # classe
                        classe_ativo = "fii"
                        try:
                            r = ativos[ativos["ticker"] == item["ticker"]]
                            if not r.empty:
                                classe_ativo = str(r.iloc[0].get("classe", "fii")).strip().lower()
                        except Exception:
                            pass
                        is_fii = classe_ativo in ["fii", "fiagro"]

                        pm_cons = 0.0
                        try:
                            pm_cons, _, _, _, _ = calcular_cenario_financeiro(movs, item["ticker"], 0, 0, "COMPRA")
                        except Exception:
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
                        time.sleep(0.05)

                    # =========================
                    # RESUMO + PDFs (FORÇADO)
                    # =========================
                    st.info("🔄 Iniciando envio de resumo + PDFs...")

                    st.write("BOT_TOKEN:", "OK" if BOT_TOKEN else "VAZIO")
                    st.write("CHAT_ID:", CHAT_ID)

                    summary = build_batch_summary_msg(lista, "PROVENTO")
                    send_telegram_message(BOT_TOKEN, CHAT_ID, summary)

                    st.info("🧱 Gerando PDF Executivo...")
                    pdf_exec = build_pdf_executivo(
                        pd.DataFrame(load_proventos()),
                        pd.DataFrame(load_ativos()),
                        date.today(),
                    )

                    st.info("🧱 Gerando PDF Auditoria...")
                    pdf_aud = build_pdf_auditoria(
                        pd.DataFrame(load_proventos()),
                        pd.DataFrame(load_ativos()),
                        date.today(),
                    )

                    exec_bytes = pdf_exec.getvalue()
                    aud_bytes = pdf_aud.getvalue()

                    dfp = pd.DataFrame(load_proventos())
                    dfa = pd.DataFrame(load_ativos())
                    dfn = pd.DataFrame(load_proventos_anunciados())

                    res = gerar_e_enviar_pdfs(
                        BOT_TOKEN,
                        CHAT_ID,
                        date.today(),
                        dfp,
                        dfa,
                        dfn,
                    )

                    if not res.ok_exec or not res.ok_aud:
                        st.error(f"PDF falhou | exec={res.ok_exec} aud={res.ok_aud}")
                        if res.err_exec:
                            st.code(res.err_exec)
                        if res.err_aud:
                            st.code(res.err_aud)
                    else:
                        st.success("📄 PDFs enviados no Telegram")


                    st.success("Lote Finalizado!")
                    st.session_state.lote_prov = []
                    time.sleep(1)
                    st.rerun()

        else:
            st.info("👈 Adicione proventos.")