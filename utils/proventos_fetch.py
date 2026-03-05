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

    # =========================
    # BASE
    # =========================
    df = df_posicoes.copy()

    if df.empty:
        return pd.DataFrame()

    # garante tipos
    df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0)
    df["preco_medio"] = pd.to_numeric(df["preco_medio"], errors="coerce").fillna(0)

    # =========================
    # PREÇO ATUAL
    # =========================
    if not df_cotacoes.empty:
        df_cotacoes["preco"] = pd.to_numeric(df_cotacoes["preco"], errors="coerce")

        df = df.merge(
            df_cotacoes[["ticker", "preco"]],
            on="ticker",
            how="left",
        )

        df.rename(columns={"preco": "preco_atual"}, inplace=True)
    else:
        df["preco_atual"] = np.nan

    df["preco_atual"] = pd.to_numeric(df["preco_atual"], errors="coerce").fillna(0)

    # =========================
    # VALORES
    # =========================
    df["valor_investido"] = df["quantidade"] * df["preco_medio"]
    df["valor_mercado"] = df["quantidade"] * df["preco_atual"]

    total_mercado = df["valor_mercado"].sum()

    if total_mercado > 0:
        df["peso_pct"] = df["valor_mercado"] / total_mercado
    else:
        df["peso_pct"] = 0

    # =========================
    # PROVENTOS 12M
    # =========================
    if not df_proventos.empty and "data_pagamento" in df_proventos.columns:

        df_proventos["data_pagamento"] = pd.to_datetime(
            df_proventos["data_pagamento"],
            errors="coerce"
        )

        hoje = pd.Timestamp.today()

        ultimos_12m = df_proventos[
            df_proventos["data_pagamento"] >= hoje - pd.DateOffset(months=12)
        ]

        dy_map = (
            ultimos_12m.groupby("ticker")["valor"]
            .sum()
            .to_dict()
        )

        df["proventos_12m"] = df["ticker"].map(dy_map).fillna(0)

    else:
        df["proventos_12m"] = 0

    # =========================
    # DY
    # =========================
    df["dy_12m"] = np.where(
        df["valor_mercado"] > 0,
        df["proventos_12m"] / df["valor_mercado"],
        0
    )

    # =========================
    # YOC
    # =========================
    df["yoc"] = np.where(
        df["valor_investido"] > 0,
        df["proventos_12m"] / df["valor_investido"],
        0
    )

    # =========================
    # PVP (último capturado)
    # =========================
    if not df_proventos_anunciados.empty and "pvp" in df_proventos_anunciados.columns:

        df_proventos_anunciados["capturado_em"] = pd.to_datetime(
            df_proventos_anunciados["capturado_em"],
            errors="coerce"
        )

        pvp_map = (
            df_proventos_anunciados
            .sort_values("capturado_em")
            .drop_duplicates("ticker", keep="last")
            .set_index("ticker")["pvp"]
            .to_dict()
        )

        df["pvp"] = df["ticker"].map(pvp_map)

    else:
        df["pvp"] = np.nan

    # =========================
    # GOVERNANÇA + CLASSE
    # =========================
    if not df_master.empty:

        df = df.merge(
            df_master[["ticker", "classe", "classificacao_capital"]],
            on="ticker",
            how="left",
        )

        df.rename(
            columns={"classificacao_capital": "governanca"},
            inplace=True,
        )

    else:
        df["classe"] = ""
        df["governanca"] = ""

    # =========================
    # SCORE BASE
    # =========================
    df["score_base"] = (
        df["dy_12m"].fillna(0) * 0.5 +
        (1 - df["pvp"].fillna(1)) * 0.5
    )

    # =========================
    # TENDÊNCIA (placeholder)
    # =========================
    df["tendencia_6m"] = np.nan

    # =========================
    # DESCONTO
    # =========================
    df["desconto_pct"] = np.nan

    # =========================
    # DATA ATUALIZAÇÃO
    # =========================
    df["atualizado_em"] = datetime.now()

    # =========================
    # ORDENAR
    # =========================
    df.sort_values(
        "valor_mercado",
        ascending=False,
        inplace=True
    )

    return df
