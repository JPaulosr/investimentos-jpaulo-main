# utils/snapshot_carteira.py
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime


def _to_numeric_col(series: pd.Series) -> pd.Series:
    """Converte coluna para numérico — trata strings vazias e tipos mistos do gspread."""
    return pd.to_numeric(series.replace("", np.nan), errors="coerce")


def atualizar_snapshot_carteira(
    df_posicoes: pd.DataFrame,
    df_cotacoes: pd.DataFrame,
    df_proventos: pd.DataFrame,
    df_proventos_anunciados: pd.DataFrame,
    df_master: pd.DataFrame,
) -> pd.DataFrame:

    df = df_posicoes.copy()

    # =========================
    # COERÇÃO NUMÉRICA (gspread retorna strings vazias '' para NaN)
    # =========================
    df["quantidade"]  = _to_numeric_col(df["quantidade"])
    df["preco_medio"] = _to_numeric_col(df["preco_medio"])

    df_cotacoes = df_cotacoes.copy()
    df_cotacoes["preco"] = _to_numeric_col(df_cotacoes["preco"])

    # =========================
    # PREÇO ATUAL
    # =========================
    df = df.merge(
        df_cotacoes[["ticker", "preco"]],
        on="ticker",
        how="left",
    )
    df.rename(columns={"preco": "preco_atual"}, inplace=True)

    # =========================
    # VALORES
    # =========================
    df["valor_investido"] = df["quantidade"] * df["preco_medio"]
    df["valor_mercado"]   = df["quantidade"] * df["preco_atual"]

    total_mercado = df["valor_mercado"].sum()
    df["peso_pct"] = df["valor_mercado"] / total_mercado

    # =========================
    # DY 12M
    # =========================
    hoje = pd.Timestamp.today()

    # Coluna de data na aba 'proventos' pode ser 'data' ou 'data_pagamento'
    _col_data_prov = "data_pagamento" if "data_pagamento" in df_proventos.columns else "data"
    df_proventos = df_proventos.copy()
    df_proventos[_col_data_prov] = pd.to_datetime(
        df_proventos[_col_data_prov].replace("", np.nan), errors="coerce"
    )
    df_proventos["valor"] = _to_numeric_col(df_proventos["valor"])

    ultimos_12m = df_proventos[
        df_proventos[_col_data_prov] >= hoje - pd.DateOffset(months=12)
    ]

    dy_map = (
        ultimos_12m.groupby("ticker")["valor"]
        .sum()
        .to_dict()
    )

    df["proventos_12m"] = df["ticker"].map(dy_map).fillna(0)
    df["dy_12m"] = df["proventos_12m"] / df["valor_mercado"]

    # =========================
    # YOC
    # =========================
    df["yoc"] = df["proventos_12m"] / df["valor_investido"]

    # =========================
    # PVP (mais recente do robô)
    # =========================
    if "pvp" in df_proventos_anunciados.columns:
        df_pa = df_proventos_anunciados.copy()
        df_pa["pvp"] = _to_numeric_col(df_pa["pvp"])
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
    df = df.merge(
        df_master[["ticker", "classe", "classificacao_capital"]],
        on="ticker",
        how="left",
    )
    df.rename(columns={"classificacao_capital": "governanca"}, inplace=True)

    # =========================
    # SCORE BASE SIMPLES
    # =========================
    dy_num  = _to_numeric_col(df["dy_12m"]).fillna(0)
    pvp_num = _to_numeric_col(df["pvp"]).fillna(1)
    df["score_base"] = dy_num * 0.5 + (1 - pvp_num) * 0.5

    # =========================
    # TENDÊNCIA / DESCONTO (placeholder)
    # =========================
    df["tendencia_6m"] = np.nan
    df["desconto_pct"]  = np.nan

    # =========================
    # DATA ATUALIZAÇÃO
    # =========================
    df["atualizado_em"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    return df
