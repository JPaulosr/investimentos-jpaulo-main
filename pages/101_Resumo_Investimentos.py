# app_investimentos_linkado.py — v2 com Cotações Online e Métricas
# -----------------------------------------------------------------------------------
# - UI em cards (CSS leve)
# - Prioriza Service Account (gspread) e cai para CSV só se público
# - Detecta linha de cabeçalho por palavras-chave
# - Normaliza rótulos (acentos/quebras) e padroniza colunas
# - [NOVO] Busca cotações online com yfinance
# - [NOVO] Calcula P/L % e YOC % por ativ

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
    try:
        return float(s)
    except Exception:
        return None

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

def _read_csv_by_gid(sheet_id: str, gid: str) -> pd.DataFrame:
    import urllib.error, pandas as pd
    try:
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
        return pd.read_csv(url)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            return pd.DataFrame()
        raise
    except Exception:
        return pd.DataFrame()

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
            try:
                out["ValorInvestido"] = out["Quantidade"].astype(float) * out["PrecoMedioCompra"].astype(float)
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
    else:
        min_pl = float(min_pl_raw)  # Força ser um float padrão

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
