# utils/alerts.py
import pandas as pd

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza colunas: minúsculo, sem espaços, etc."""
    if df.empty: return df
    new_cols = []
    for c in df.columns:
        s = str(c).lower().strip()
        s = s.replace(" ", "_").replace("ç", "c").replace("ã", "a").replace("õ", "o")
        new_cols.append(s)
    df.columns = new_cols
    return df

def check_renda_deviation(ticker: str, current_vpc: float, df_proventos: pd.DataFrame, window: int = 3, threshold: float = -0.15):
    """Verifica desvio negativo relevante na renda."""
    if df_proventos.empty or current_vpc <= 0: return None
    df = df_proventos.copy()
    df = _normalize_cols(df)
    ticker = str(ticker).upper().strip()
    if "ticker" not in df.columns: return None
    df = df[df["ticker"].astype(str).str.upper().str.strip() == ticker].copy()
    if df.empty: return None

    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce", dayfirst=True)
        df = df.dropna(subset=["data"]).sort_values("data", ascending=False)
    else: return None

    vpcs = []
    for _, row in df.iterrows():
        try:
            vpc_hist = 0.0
            if "valor_por_cota" in row and pd.notna(row["valor_por_cota"]):
                 try: vpc_hist = float(str(row["valor_por_cota"]).replace(",", "."))
                 except: pass
            if vpc_hist <= 0:
                val = float(str(row.get("valor", 0)).replace(",", "."))
                qtd = float(str(row.get("quantidade_na_data", 0)).replace(",", "."))
                if qtd > 0: vpc_hist = val / qtd
            if vpc_hist > 0:
                vpcs.append(vpc_hist)
                if len(vpcs) >= window: break
        except: continue

    if len(vpcs) < window: return None
    media_ref = sum(vpcs) / len(vpcs)
    if media_ref <= 0: return None
    variacao = (current_vpc / media_ref) - 1
    if variacao <= threshold:
        return {"ticker": ticker, "ultimo_vpc": current_vpc, "media_ref": media_ref, "variacao_pct": variacao * 100, "window": window}
    return None

def get_status_comparison(ticker: str, current_vpc: float, df_proventos: pd.DataFrame) -> str:
    """
    Gera status em DUAS LINHAS OBRIGATÓRIAS.
    Linha 1: Ícone + Resumo
    Linha 2: Valores De -> Para
    """
    def fmt(v): return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    
    # Texto padrão se não houver histórico (2 linhas)
    base_vazia = f"🆕 Primeiro provento registrado\nAgora: R$ {fmt(current_vpc)}"

    if df_proventos.empty: return base_vazia

    # 1. Filtra e normaliza
    df = df_proventos.copy()
    df = _normalize_cols(df)
    ticker = str(ticker).upper().strip()
    if "ticker" not in df.columns: return base_vazia
    
    df = df[df["ticker"].astype(str).str.upper().str.strip() == ticker].copy()
    if df.empty: return base_vazia

    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce", dayfirst=True)
        df = df.dropna(subset=["data"]).sort_values("data", ascending=False)
    
    # 2. Busca último VPC válido
    last_vpc = 0.0
    for _, row in df.iterrows():
        try:
            vpc_hist = 0.0
            if "valor_por_cota" in row and pd.notna(row["valor_por_cota"]):
                try: vpc_hist = float(str(row["valor_por_cota"]).replace(",", "."))
                except: pass
            if vpc_hist <= 0:
                v = float(str(row.get("valor", 0)).replace(",", "."))
                q = float(str(row.get("quantidade_na_data", 0)).replace(",", "."))
                if q > 0: vpc_hist = v / q
            if vpc_hist > 0:
                last_vpc = vpc_hist
                break
        except: continue
            
    if last_vpc <= 0: return base_vazia

    # 3. Cálculos
    delta = current_vpc - last_vpc
    eps = 0.0001
    
    str_last = fmt(last_vpc)
    str_now = fmt(current_vpc)
    str_diff = fmt(abs(delta))
    
    # Linha 2 Base
    line2_base = f"Último: R$ {str_last} → Agora: R$ {str_now}"

    # Manteve
    if abs(delta) <= eps:
        return f"🟰 Manteve o valor\n{line2_base}"
    
    # Diferença
    if delta > 0:
        return f"🟢 Pagou mais\n{line2_base} | +R$ {str_diff}"
    else:
        return f"🟡 Pagou menos\n{line2_base} | −R$ {str_diff}"