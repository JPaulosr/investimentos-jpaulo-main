# utils/snapshot_carteira.py
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime


def atualizar_snapshot_carteira(
    df_posicoes: pd.DataFrame,
    df_cotacoes: pd.DataFrame,
    df_proventos: pd.DataFrame,
    df_proventos_anunciados: pd.DataFrame,
    df_master: pd.DataFrame,
) -> pd.DataFrame:

    df = df_posicoes.copy()

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
    df["valor_mercado"] = df["quantidade"] * df["preco_atual"]

    total_mercado = df["valor_mercado"].sum()
    df["peso_pct"] = df["valor_mercado"] / total_mercado

    # =========================
    # DY 12M
    # =========================
    hoje = pd.Timestamp.today()

    # A coluna de data na aba 'proventos' pode ser 'data' ou 'data_pagamento'
    _col_data_prov = "data_pagamento" if "data_pagamento" in df_proventos.columns else "data"
    df_proventos = df_proventos.copy()
    df_proventos[_col_data_prov] = pd.to_datetime(df_proventos[_col_data_prov], errors="coerce")

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
        pvp_map = (
            df_proventos_anunciados.sort_values("capturado_em")
            .drop_duplicates("ticker", keep="last")
            .set_index("ticker")["pvp"]
            .to_dict()
        )
        df["pvp"] = df["ticker"].map(pvp_map)

    # =========================
    # GOVERNANÇA + CLASSE
    # =========================
    df = df.merge(
        df_master[["ticker", "classe", "classificacao_capital"]],
        on="ticker",
        how="left",
    )

    df.rename(
        columns={"classificacao_capital": "governanca"},
        inplace=True,
    )

    # =========================
    # SCORE BASE SIMPLES
    # =========================
    df["score_base"] = (
        df["dy_12m"].fillna(0) * 0.5 +
        (1 - df["pvp"].fillna(1)) * 0.5
    )

    # =========================
    # TENDÊNCIA SIMPLES (placeholder)
    # =========================
    df["tendencia_6m"] = np.nan  # pode evoluir depois

    # =========================
    # DESCONTO (se tiver preco_teto futuramente)
    # =========================
    df["desconto_pct"] = np.nan

    # =========================
    # DATA ATUALIZAÇÃO
    # =========================
    df["atualizado_em"] = datetime.now()

    return df
