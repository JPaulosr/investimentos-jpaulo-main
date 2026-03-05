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
    return (
        series.astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .replace("", np.nan)
        .astype(float)
    )


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

    # ── Tendência 6M dos proventos (valor_por_cota mensal) ───────────────────
    # Reutiliza a lógica do card_decisao_renda.py:
    # compara média dos últimos meses de valor_por_cota para detectar tendência.
    # Resultado: label qualitativo + % numérico — para a IA consumir sem calcular.
    def _tendencia_proventos(ticker: str):
        df_t = df_prov[df_prov["ticker"] == ticker].copy()
        if df_t.empty:
            return "Sem dados", 0.0
        _col = "data_pagamento" if "data_pagamento" in df_t.columns else "data"
        df_t["_dt"] = pd.to_datetime(df_t[_col].replace("", np.nan), errors="coerce")
        df_t = df_t.dropna(subset=["_dt"])
        col_vpc = next((c for c in ["valor_por_cota", "vpc", "valor_cota"] if c in df_t.columns), None)
        if col_vpc is None:
            if "valor" in df_t.columns and "quantidade_na_data" in df_t.columns:
                df_t["_vpc"] = df_t.apply(
                    lambda r: (pd.to_numeric(r["valor"], errors="coerce") or 0)
                              / max(pd.to_numeric(r["quantidade_na_data"], errors="coerce") or 1, 1),
                    axis=1
                )
                col_vpc = "_vpc"
            else:
                return "Sem dados", 0.0
        df_t["_vpc_v"] = pd.to_numeric(df_t[col_vpc], errors="coerce").fillna(0)
        df_t["_mes"] = df_t["_dt"].dt.to_period("M")
        serie = [float(v) for v in df_t.groupby("_mes")["_vpc_v"].mean().sort_index().tolist() if v > 0]
        if len(serie) < 2:
            return "Sem dados", 0.0
        arr = np.array(serie)
        if len(arr) >= 12:
            m6_rec, m6_ant = arr[-6:].mean(), arr[-12:-6].mean()
        elif len(arr) >= 6:
            m6_rec = arr[-3:].mean() if len(arr) >= 3 else arr.mean()
            m6_ant = arr[:3].mean() if len(arr) >= 3 else arr.mean()
        else:
            m6_rec, m6_ant = arr[-1], arr[0]
        trend6 = float((m6_rec / m6_ant - 1) * 100) if m6_ant > 0 else 0.0
        label = (
            "Crescente" if trend6 > 3.0
            else "Estável" if trend6 >= -3.0
            else "Leve queda" if trend6 > -8.0
            else "Queda relevante"
        )
        return label, round(trend6, 2)

    tend_results = [_tendencia_proventos(t) for t in df["ticker"]]
    df["tendencia_6m"] = [r[0] for r in tend_results]
    df["trend6_pct"]   = [r[1] for r in tend_results]

    # ── Timestamp ─────────────────────────────────────────────────────────────
    df["atualizado_em"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    return df
