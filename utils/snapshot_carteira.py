# utils/snapshot_carteira.py
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime


def _to_num(series: pd.Series) -> pd.Series:
    """
    Converte para float tolerando formatos pt-BR e en-US:
      24.22      → 24.22  (ponto decimal, já correto)
      24,22      → 24.22  (vírgula decimal, pt-BR)
      2.422,00   → 2422.0 (milhar com ponto + vírgula decimal, pt-BR)
      2,422.00   → 2422.0 (milhar com vírgula + ponto decimal, en-US)
    Regra: se tem vírgula → tratar como pt-BR; caso contrário não mexe.
    """
    def _parse(v):
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s or s in ("", "nan", "None", "NaN"):
            return float("nan")
        if "," in s:
            # pt-BR: remove pontos de milhar, troca vírgula por ponto
            s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return float("nan")
    return pd.to_numeric(series.apply(_parse), errors="coerce")


def atualizar_snapshot_carteira(
    df_posicoes: pd.DataFrame,
    df_cotacoes: pd.DataFrame,
    df_proventos: pd.DataFrame,
    df_proventos_anunciados: pd.DataFrame,
    df_master: pd.DataFrame,
) -> pd.DataFrame:

    df = df_posicoes[["ticker", "quantidade", "preco_medio"]].copy()

    df["quantidade"] = _to_num(df["quantidade"])
    df["preco_medio"] = _to_num(df["preco_medio"])

    df_cot = df_cotacoes.copy()
    df_cot["preco"] = _to_num(df_cot["preco"])

    df = df.merge(df_cot[["ticker", "preco"]], on="ticker", how="left")
    df.rename(columns={"preco": "preco_atual"}, inplace=True)

    df["valor_investido"] = df["quantidade"] * df["preco_medio"]
    df["valor_mercado"] = df["quantidade"] * df["preco_atual"]

    total_mercado = df["valor_mercado"].sum()

    df["peso_pct"] = (
        df["valor_mercado"] / total_mercado if total_mercado > 0 else 0.0
    )

    hoje = pd.Timestamp.today()

    _col_data = (
        "data_pagamento"
        if "data_pagamento" in df_proventos.columns
        else "data"
    )

    df_prov = df_proventos.copy()

    df_prov[_col_data] = pd.to_datetime(
        df_prov[_col_data].replace("", np.nan)
        if df_prov[_col_data].dtype == object
        else df_prov[_col_data],
        errors="coerce",
    )

    df_prov["valor"] = _to_num(df_prov["valor"])

    ultimos_12m = df_prov[
        df_prov[_col_data] >= hoje - pd.DateOffset(months=12)
    ]

    dy_map = ultimos_12m.groupby("ticker")["valor"].sum().to_dict()

    df["proventos_12m"] = df["ticker"].map(dy_map).fillna(0)

    df["dy_12m"] = (
        df["proventos_12m"] / df["valor_mercado"]
    ).where(df["valor_mercado"] > 0, np.nan)

    df["yoc"] = (
        df["proventos_12m"] / df["valor_investido"]
    ).where(df["valor_investido"] > 0, np.nan)

    if "pvp" in df_proventos_anunciados.columns:

        df_pa = df_proventos_anunciados.copy()

        df_pa["pvp"] = _to_num(df_pa["pvp"])

        if "capturado_em" in df_pa.columns:
            df_pa["capturado_em"] = df_pa["capturado_em"].replace("", np.nan)

        pvp_map = (
            df_pa.sort_values("capturado_em")
            .drop_duplicates("ticker", keep="last")
            .set_index("ticker")["pvp"]
            .to_dict()
        )

        df["pvp"] = pd.to_numeric(
            df["ticker"].map(pvp_map), errors="coerce"
        )

    cols_m = [
        c
        for c in ["ticker", "classe", "classificacao_capital"]
        if c in df_master.columns
    ]

    df = df.merge(df_master[cols_m], on="ticker", how="left")

    df.rename(columns={"classificacao_capital": "governanca"}, inplace=True)

    dy_n = _to_num(df["dy_12m"]).fillna(0)

    pvp_n = _to_num(df["pvp"]).fillna(1)

    df["score_base"] = dy_n * 0.5 + (1 - pvp_n) * 0.5

    df["desconto_pct"] = (
        (df["preco_atual"] - df["preco_medio"])
        / df["preco_medio"]
    ).where(df["preco_medio"] > 0, np.nan)

    def _tendencia_proventos(ticker: str):

        df_t = df_prov[df_prov["ticker"] == ticker].copy()

        if df_t.empty:
            return "Sem dados", 0.0

        _col = (
            "data_pagamento"
            if "data_pagamento" in df_t.columns
            else "data"
        )

        df_t["_dt"] = pd.to_datetime(
            df_t[_col].replace("", np.nan), errors="coerce"
        )

        df_t = df_t.dropna(subset=["_dt"])

        col_vpc = next(
            (
                c
                for c in ["valor_por_cota", "vpc", "valor_cota"]
                if c in df_t.columns
            ),
            None,
        )

        if col_vpc is None:

            if (
                "valor" in df_t.columns
                and "quantidade_na_data" in df_t.columns
            ):

                df_t["_vpc"] = df_t.apply(
                    lambda r: (
                        _to_num(pd.Series([r["valor"]])).iloc[0] or 0
                    ) / max(
                        _to_num(pd.Series([r["quantidade_na_data"]])).iloc[0] or 1, 1
                    ),
                    axis=1,
                )

                col_vpc = "_vpc"

            else:
                return "Sem dados", 0.0

        df_t["_vpc_v"] = _to_num(df_t[col_vpc].astype(str)).fillna(0)

        df_t["_mes"] = df_t["_dt"].dt.to_period("M")

        # CORRIGIDO: soma todos os proventos do mês (Dividendo + Rendimento + Aluguel)
        # .mean() causava distorção — ex: BBSE3 com aluguel R$0.04 no mês dava trend -28%
        serie_raw = (
            df_t.groupby("_mes")["_vpc_v"]
            .sum()
            .sort_index()
        )

        # Filtrar meses com valor muito pequeno (< 10% da mediana) — são fragmentos de aluguel/rendimento
        # que chegam no começo ou fim do mês e distorcem a tendência
        vals = [float(v) for v in serie_raw.tolist() if v > 0]
        if len(vals) >= 4:
            mediana = float(np.median(vals))
            serie = [v for v in vals if v >= mediana * 0.10]
        else:
            serie = vals

        if len(serie) < 2:
            return "Sem dados", 0.0

        arr = np.array(serie)

        if len(arr) >= 12:

            m6_rec = arr[-6:].mean()

            m6_ant = arr[-12:-6].mean()

        elif len(arr) >= 6:

            m6_rec = arr[-3:].mean()

            m6_ant = arr[:3].mean()

        else:

            m6_rec = arr[-1]

            m6_ant = arr[0]

        trend6 = (
            float((m6_rec / m6_ant - 1) * 100) if m6_ant > 0 else 0.0
        )

        label = (
            "Crescente"
            if trend6 > 3
            else "Estável"
            if trend6 >= -3
            else "Leve queda"
            if trend6 > -8
            else "Queda relevante"
        )

        return label, round(trend6, 2)

    tend_results = [_tendencia_proventos(t) for t in df["ticker"]]

    df["tendencia_6m"] = [r[0] for r in tend_results]

    df["trend6_pct"] = [r[1] for r in tend_results]

    df["atualizado_em"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    return df
