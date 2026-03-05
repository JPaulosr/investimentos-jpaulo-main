# utils/snapshot_carteira.py
# -*- coding: utf-8 -*-
"""
Calcula métricas da carteira a partir das abas do Google Sheets.
IMPORTANTE: recebe dados já limpos/corrigidos — não faz nenhuma detecção de
corrupção internamente. A correção de escala é responsabilidade do chamador
(repair_posicoes.py ou proventos_job.py).
"""

import pandas as pd
import numpy as np
from datetime import datetime


def _to_num(series: pd.Series) -> pd.Series:
    """Converte para numérico tolerando strings vazias (padrão do gspread)."""
    return pd.to_numeric(series.replace("", np.nan), errors="coerce")


def atualizar_snapshot_carteira(
    df_posicoes: pd.DataFrame,
    df_cotacoes: pd.DataFrame,
    df_proventos: pd.DataFrame,
    df_proventos_anunciados: pd.DataFrame,
    df_master: pd.DataFrame,
) -> pd.DataFrame:

    # ── Colunas base ──────────────────────────────────────────────────────────
    df = df_posicoes[["ticker", "quantidade", "preco_medio"]].copy()
    df["quantidade"]  = _to_num(df["quantidade"])
    df["preco_medio"] = _to_num(df["preco_medio"])

    df_cot = df_cotacoes.copy()
    df_cot["preco"] = _to_num(df_cot["preco"])

    # ── Preço atual ───────────────────────────────────────────────────────────
    df = df.merge(df_cot[["ticker", "preco"]], on="ticker", how="left")
    df.rename(columns={"preco": "preco_atual"}, inplace=True)

    # ── Valores ───────────────────────────────────────────────────────────────
    df["valor_investido"] = df["quantidade"] * df["preco_medio"]
    df["valor_mercado"]   = df["quantidade"] * df["preco_atual"]

    total_mercado = df["valor_mercado"].sum()
    df["peso_pct"] = df["valor_mercado"] / total_mercado if total_mercado > 0 else 0.0

    # ── DY 12M ────────────────────────────────────────────────────────────────
    hoje = pd.Timestamp.today()
    _col_data = "data_pagamento" if "data_pagamento" in df_proventos.columns else "data"
    df_prov = df_proventos.copy()
    df_prov[_col_data] = pd.to_datetime(
        df_prov[_col_data].replace("", np.nan) if df_prov[_col_data].dtype == object else df_prov[_col_data],
        errors="coerce"
    )
    df_prov["valor"] = _to_num(df_prov["valor"])

    ultimos_12m = df_prov[df_prov[_col_data] >= hoje - pd.DateOffset(months=12)]
    dy_map = ultimos_12m.groupby("ticker")["valor"].sum().to_dict()

    df["proventos_12m"] = df["ticker"].map(dy_map).fillna(0)
    df["dy_12m"] = (df["proventos_12m"] / df["valor_mercado"]).where(df["valor_mercado"] > 0, np.nan)

    # ── YOC ───────────────────────────────────────────────────────────────────
    df["yoc"] = (df["proventos_12m"] / df["valor_investido"]).where(df["valor_investido"] > 0, np.nan)

    # ── P/VP ─────────────────────────────────────────────────────────────────
    if "pvp" in df_proventos_anunciados.columns:
        df_pa = df_proventos_anunciados.copy()
        df_pa["pvp"] = _to_num(df_pa["pvp"])
        df_pa["capturado_em"] = df_pa["capturado_em"].replace("", np.nan) if "capturado_em" in df_pa.columns else np.nan
        pvp_map = (
            df_pa.sort_values("capturado_em")
            .drop_duplicates("ticker", keep="last")
            .set_index("ticker")["pvp"]
            .to_dict()
        )
        df["pvp"] = pd.to_numeric(df["ticker"].map(pvp_map), errors="coerce")

    # ── Governança + Classe ───────────────────────────────────────────────────
    cols_m = [c for c in ["ticker", "classe", "classificacao_capital"] if c in df_master.columns]
    df = df.merge(df_master[cols_m], on="ticker", how="left")
    df.rename(columns={"classificacao_capital": "governanca"}, inplace=True)

    # ── Score base ────────────────────────────────────────────────────────────
    dy_n  = _to_num(df["dy_12m"]).fillna(0)
    pvp_n = _to_num(df["pvp"]).fillna(1)
    df["score_base"] = dy_n * 0.5 + (1 - pvp_n) * 0.5

    # ── Desconto vs preço médio ───────────────────────────────────────────────
    df["desconto_pct"] = (
        (df["preco_atual"] - df["preco_medio"]) / df["preco_medio"]
    ).where(df["preco_medio"] > 0, np.nan)

    # ── Placeholders ─────────────────────────────────────────────────────────
    df["tendencia_6m"] = np.nan

    # ── Timestamp ─────────────────────────────────────────────────────────────
    df["atualizado_em"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    return df
