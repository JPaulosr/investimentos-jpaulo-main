# utils/snapshot_carteira.py
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime


def _to_num(series: pd.Series) -> pd.Series:
    """Converte para numérico — trata strings vazias e tipos mistos do gspread."""
    return pd.to_numeric(series.replace("", np.nan), errors="coerce")


def _detect_and_fix_corruption(df: pd.DataFrame, col: str, max_expected: float) -> pd.Series:
    """
    Detecta valores claramente corrompidos (sem casa decimal) e tenta corrigir.
    Se preco_medio=1232 e max_expected=500, divide por 100.
    Estratégia conservadora: só corrige se TODOS os valores forem inteiros
    e a mediana for > max_expected.
    """
    s = _to_num(df[col])
    mediana = s.median()
    todos_inteiros = (s.dropna() == s.dropna().round(0)).all()
    if todos_inteiros and mediana > max_expected:
        # Tenta divisão por 100
        s_corr = s / 100
        if s_corr.median() <= max_expected:
            print(f"⚠️  '{col}' parece corrompido (mediana={mediana:.0f}). Corrigindo ÷100.")
            return s_corr
    return s


def atualizar_snapshot_carteira(
    df_posicoes: pd.DataFrame,
    df_cotacoes: pd.DataFrame,
    df_proventos: pd.DataFrame,
    df_proventos_anunciados: pd.DataFrame,
    df_master: pd.DataFrame,
) -> pd.DataFrame:

    df = df_posicoes[["ticker", "quantidade", "preco_medio"]].copy()

    # =========================
    # COERÇÃO + DETECÇÃO DE CORRUPÇÃO
    # =========================
    df["quantidade"]  = _to_num(df["quantidade"])
    # preco_medio: ações brasileiras raramente passam de R$500 — acima disso é suspeito
    df["preco_medio"] = _detect_and_fix_corruption(df, "preco_medio", max_expected=500)

    df_cotacoes = df_cotacoes.copy()
    df_cotacoes["preco"] = _detect_and_fix_corruption(df_cotacoes, "preco", max_expected=500)

    # =========================
    # PREÇO ATUAL
    # =========================
    df = df.merge(df_cotacoes[["ticker", "preco"]], on="ticker", how="left")
    df.rename(columns={"preco": "preco_atual"}, inplace=True)

    # =========================
    # VALORES
    # =========================
    df["valor_investido"] = df["quantidade"] * df["preco_medio"]
    df["valor_mercado"]   = df["quantidade"] * df["preco_atual"]

    total_mercado = df["valor_mercado"].sum()
    df["peso_pct"] = df["valor_mercado"] / total_mercado if total_mercado > 0 else 0.0

    # =========================
    # DY 12M
    # =========================
    hoje = pd.Timestamp.today()
    _col_data = "data_pagamento" if "data_pagamento" in df_proventos.columns else "data"
    df_prov = df_proventos.copy()
    df_prov[_col_data] = pd.to_datetime(df_prov[_col_data].replace("", np.nan), errors="coerce")
    df_prov["valor"]   = _to_num(df_prov["valor"])

    ultimos_12m = df_prov[df_prov[_col_data] >= hoje - pd.DateOffset(months=12)]
    dy_map = ultimos_12m.groupby("ticker")["valor"].sum().to_dict()

    df["proventos_12m"] = df["ticker"].map(dy_map).fillna(0)
    df["dy_12m"] = (df["proventos_12m"] / df["valor_mercado"]).where(df["valor_mercado"] > 0, np.nan)

    # =========================
    # YOC
    # =========================
    df["yoc"] = (df["proventos_12m"] / df["valor_investido"]).where(df["valor_investido"] > 0, np.nan)

    # =========================
    # P/VP
    # =========================
    if "pvp" in df_proventos_anunciados.columns:
        df_pa = df_proventos_anunciados.copy()
        df_pa["pvp"] = _to_num(df_pa["pvp"])
        # Detectar pvp corrompido (ex: 82 em vez de 0.82)
        df_pa["pvp"] = df_pa["pvp"].apply(
            lambda v: v / 100 if pd.notna(v) and v > 20 else v
        )
        df_pa["capturado_em"] = df_pa["capturado_em"].replace("", np.nan)
        pvp_map = (
            df_pa.sort_values("capturado_em")
            .drop_duplicates("ticker", keep="last")
            .set_index("ticker")["pvp"]
            .to_dict()
        )
        df["pvp"] = pd.to_numeric(df["ticker"].map(pvp_map), errors="coerce")

    # =========================
    # GOVERNANÇA + CLASSE
    # =========================
    cols_master = [c for c in ["ticker", "classe", "classificacao_capital"] if c in df_master.columns]
    df = df.merge(df_master[cols_master], on="ticker", how="left")
    df.rename(columns={"classificacao_capital": "governanca"}, inplace=True)

    # =========================
    # SCORE BASE
    # =========================
    dy_num  = _to_num(df["dy_12m"]).fillna(0)
    pvp_num = _to_num(df["pvp"]).fillna(1)
    df["score_base"] = dy_num * 0.5 + (1 - pvp_num) * 0.5

    # =========================
    # TENDÊNCIA 6M (retorno de preço — placeholder calculável)
    # =========================
    # Placeholder: NaN até ter histórico de cotações
    df["tendencia_6m"] = np.nan

    # =========================
    # DESCONTO vs PREÇO MÉDIO (proxy até ter preco_teto)
    # desconto_pct = (preco_atual - preco_medio) / preco_medio
    # positivo = valorizado acima do PM, negativo = abaixo
    # =========================
    df["desconto_pct"] = (
        (df["preco_atual"] - df["preco_medio"]) / df["preco_medio"]
    ).where(df["preco_medio"] > 0, np.nan)

    # =========================
    # DATA ATUALIZAÇÃO
    # =========================
    df["atualizado_em"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    return df
