# utils/estimativas.py
import pandas as pd
from datetime import datetime, timedelta

def _to_num_series(s: pd.Series) -> pd.Series:
    """Converte coluna para numerico, limpando caracteres estranhos."""
    x = s.astype(str).str.strip()
    x = x.str.replace("\u00a0", "", regex=False)
    x = x.str.replace("R$", "", regex=False)
    x = x.str.replace(".", "", regex=False)
    x = x.str.replace(",", ".", regex=False)
    return pd.to_numeric(x, errors="coerce")

def get_trailing_12m_proventos(ticker: str, proventos_df: pd.DataFrame) -> float:
    """
    Soma os proventos PAGOS nos últimos 12 meses para um ticker.
    Útil para cálculo de DY/YoC Anual de Ações.
    """
    if proventos_df.empty or "ticker" not in proventos_df.columns:
        return 0.0
    
    ticker = str(ticker).upper().strip()
    df = proventos_df.copy()
    
    # Normaliza colunas
    cols = [c.lower().strip() for c in df.columns]
    df.columns = cols
    
    # Filtra Ticker
    df = df[df["ticker"].astype(str).str.upper().str.strip() == ticker]
    if df.empty: return 0.0
    
    # Filtra Data (Window 12m)
    if "data" not in df.columns: return 0.0
    
    df["data"] = pd.to_datetime(df["data"], errors="coerce", dayfirst=True)
    today = datetime.now()
    cutoff = today - timedelta(days=365)
    
    # Pega apenas o último ano
    df_12m = df[df["data"] >= cutoff]
    
    total_recebido_12m = 0.0
    if "valor" in df_12m.columns:
        s = _to_num_series(df_12m["valor"])
        total_recebido_12m = s.sum()
        
    return float(total_recebido_12m)

def estimate_next_month_income(ticker: str, qty_delta: float, proventos_df: pd.DataFrame, ativos_master: pd.DataFrame):
    """
    Estima o rendimento mensal (para FIIs) ou anual projetado (para Ações).
    """
    if qty_delta <= 0:
        return 0.0, "N/A", "Sem aumento de posição", 0.0

    ticker = str(ticker).upper().strip()

    # Tenta descobrir a classe do ativo
    ativos = ativos_master.copy()
    ativos["ticker"] = ativos["ticker"].astype(str).str.upper().str.strip()
    r = ativos[ativos["ticker"] == ticker]
    classe = str(r.iloc[0].get("classe", "")).strip().lower() if not r.empty else ""

    df = proventos_df.copy()
    if df.empty or "ticker" not in df.columns:
        return 0.0, "Sem histórico", "0 proventos", 0.0

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df[df["ticker"] == ticker].copy()
    if df.empty:
        return 0.0, "Sem histórico", "0 proventos", 0.0

    # Garante Data
    if "data" not in df.columns:
        return 0.0, "Sem base", "Coluna 'data' ausente", 0.0
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df = df.dropna(subset=["data"]).sort_values("data")

    # Normalizações numéricas (cria colunas auxiliares _num)
    if "valor" in df.columns:
        df["valor_num"] = _to_num_series(df["valor"])
    else:
        df["valor_num"] = pd.NA

    if "quantidade_na_data" in df.columns:
        df["qtd_num"] = _to_num_series(df["quantidade_na_data"])
    else:
        df["qtd_num"] = pd.NA

    if "valor_por_cota" in df.columns:
        df["vpc_num"] = _to_num_series(df["valor_por_cota"])
    else:
        df["vpc_num"] = pd.NA

    # ==========================
    # Lógica FII / Fiagro
    # ==========================
    if classe in ["fii", "fiagro"]:
        # Tenta pegar pelo VPC direto
        vpc = df.dropna(subset=["vpc_num"])
        if not vpc.empty:
            last_vpc = float(vpc.iloc[-1]["vpc_num"])
            return float(qty_delta * last_vpc), "FII: último rendimento", f"Último por cota: {last_vpc:.4f}", last_vpc

        # Fallback: Calcula VPC na marra (Total / Qtd)
        v = df.dropna(subset=["valor_num", "qtd_num"])
        v = v[v["qtd_num"] > 0]
        if v.empty:
            return 0.0, "FII sem base", "Sem valor_por_cota e sem valor/qtd válidos", 0.0
        
        last = float((v.iloc[-1]["valor_num"] / v.iloc[-1]["qtd_num"]))
        return float(qty_delta * last), "FII: valor/qtd (fallback)", f"Último por cota: {last:.4f}", last

    # ==========================
    # Lógica Ações (Base 12m)
    # ==========================
    cutoff = df["data"].max() - timedelta(days=365)
    df12 = df[df["data"] >= cutoff].copy()

    # Se tiver VPC limpo
    if df12["vpc_num"].notna().any():
        div12 = float(df12["vpc_num"].dropna().sum())
        est = float(qty_delta * (div12 / 12.0))
        return est, "Ação: média mensal 12m (vpc)", f"Div por ação 12m: {div12:.4f}", div12/12.0

    # Fallback Ações
    v12 = df12.dropna(subset=["valor_num", "qtd_num"])
    v12 = v12[v12["qtd_num"] > 0]
    if v12.empty:
        return 0.0, "Sem base 12m", "Sem valor_por_cota e sem valor/qtd válidos", 0.0
    
    # Calcula VPC médio ponderado
    div12 = float((v12["valor_num"] / v12["qtd_num"]).sum())
    est = float(qty_delta * (div12 / 12.0))
    return est, "Ação: média mensal 12m (fallback)", f"Div por ação 12m: {div12:.4f}", div12/12.0