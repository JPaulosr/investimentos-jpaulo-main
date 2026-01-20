# utils/motor_positions.py
# -*- coding: utf-8 -*-

import pandas as pd

def _to_float_br(x) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none", "null", "-"):
        return 0.0
    s = s.replace("R$", "").replace(" ", "")
    # BR -> float
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

def _norm_ticker(x) -> str:
    return str(x or "").strip().upper().replace(" ", "")

def _norm_tipo(x) -> str:
    t = str(x or "").strip().lower()
    if t in ("c", "compra", "buy"):
        return "compra"
    if t in ("v", "venda", "sell"):
        return "venda"
    return t

def compute_positions_from_movs(df_movs: pd.DataFrame) -> pd.DataFrame:
    """
    Motor mínimo (custo médio):
    - Compra: aumenta qtd e custo
    - Venda: reduz qtd e reduz custo proporcional ao PM no momento da venda
    Retorna: ticker, quantidade, preco_medio
    """
    if df_movs is None or df_movs.empty:
        return pd.DataFrame(columns=["ticker", "quantidade", "preco_medio"])

    df = df_movs.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    # aliases comuns
    if "quantidade" in df.columns and "qtd" not in df.columns:
        df["qtd"] = df["quantidade"]
    if "ativo" in df.columns and "ticker" not in df.columns:
        df["ticker"] = df["ativo"]
    if "preço" in df.columns and "preco" not in df.columns:
        df["preco"] = df["preço"]

    # obrigatórias
    for col in ["data", "ticker", "tipo", "qtd", "preco"]:
        if col not in df.columns:
            df[col] = ""

    if "taxa" not in df.columns:
        df["taxa"] = 0

    df["ticker"] = df["ticker"].map(_norm_ticker)
    df["tipo"] = df["tipo"].map(_norm_tipo)
    df["qtd"] = df["qtd"].map(_to_float_br).astype(float)
    df["preco"] = df["preco"].map(_to_float_br).astype(float)
    df["taxa"] = df["taxa"].map(_to_float_br).astype(float)
    df["data"] = pd.to_datetime(df["data"], errors="coerce")

    # limpa lixo
    df = df[(df["ticker"] != "") & (df["qtd"] > 0) & (df["preco"] > 0)]
    df = df[df["tipo"].isin(["compra", "venda"])].sort_values(["ticker", "data"])

    pos_rows = []
    for ticker, g in df.groupby("ticker", sort=False):
        qtd = 0.0
        custo = 0.0

        for _, r in g.iterrows():
            tipo = r["tipo"]
            q = float(r["qtd"])
            total = float(r["qtd"] * r["preco"] + r["taxa"])

            if tipo == "compra":
                qtd += q
                custo += total

            elif tipo == "venda":
                if qtd <= 0:
                    raise ValueError(f"{ticker}: VENDA sem posição (qtd=0). Corrija a aba movimentações.")
                pm = (custo / qtd) if qtd > 0 else 0.0
                qtd -= q
                custo -= pm * q

        if qtd > 0:
            pm_final = (custo / qtd) if qtd > 0 else 0.0
            pos_rows.append({
                "ticker": ticker,
                "quantidade": round(qtd, 6),
                "preco_medio": round(pm_final, 6),
            })

    return pd.DataFrame(pos_rows)
