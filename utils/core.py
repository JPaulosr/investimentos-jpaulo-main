# utils/core.py
# -*- coding: utf-8 -*-
"""
CORE (blindado) — VERSÃO COMPLETA

✅ O que este core faz:
- Normalização robusta (colunas duplicadas, nomes variáveis, números BR/US)
- ✅ MOTOR: calcula posicoes_snapshot a partir de MOVIMENTAÇÕES (compra/venda) (custo médio)
- Enriquecimento de posições com master + cotações
- Proventos 12m, métricas de carteira, alocações e concentração

🔒 Regras:
- core.py NÃO depende de streamlit
- core.py NÃO lê Google Sheets (isso fica no gsheets.py)
"""

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# =========================
# 1) HELPERS (ANTI-SUJEIRA)
# =========================

def _strip_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas duplicadas (mesmo nome), mantendo a primeira.
    Isso é obrigatório antes de merges.
    """
    df = df.copy()
    return df.loc[:, ~df.columns.duplicated(keep="first")]

def _get_col_series(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Retorna Series única mesmo se houver colunas duplicadas no DF.
    """
    if df is None or len(df) == 0:
        return pd.Series(dtype=object)

    try:
        x = df.get(col)
    except Exception:
        return pd.Series(dtype=object)

    if x is None:
        return pd.Series(dtype=object)

    # Se retornar DataFrame (colunas duplicadas), pega a 1ª
    if isinstance(x, pd.DataFrame):
        return x.iloc[:, 0]

    return x

def normalize_ticker_series(s) -> pd.Series:
    """
    Normaliza tickers removendo espaços e jogando para maiúsculo.
    """
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]

    if s is None or len(s) == 0:
        return pd.Series(dtype=str)

    return (
        pd.Series(s)
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "", regex=False)
    )

def _to_float_series(s) -> pd.Series:
    """
    Converte números BR/US com regra robusta:
    - Define o separador decimal como o ÚLTIMO entre '.' e ','.
    - O outro vira separador de milhar e é removido.
    """
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]

    if s is None or len(s) == 0:
        return pd.Series(dtype=float)

    def parse_val(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return 0.0

        # já é número
        if isinstance(x, (int, float)) and not pd.isna(x):
            return float(x)

        txt = str(x).strip()
        if txt == "" or txt.lower() in ["nan", "none", "null", "-"]:
            return 0.0

        # limpa moeda, espaços e %
        txt = txt.replace("R$", "").replace(" ", "").replace("%", "")

        # se não tem separadores, tenta direto
        if ("," not in txt) and ("." not in txt):
            try:
                return float(txt)
            except:
                return 0.0

        # se tem os dois, o ÚLTIMO é decimal
        if ("," in txt) and ("." in txt):
            last_comma = txt.rfind(",")
            last_dot = txt.rfind(".")
            if last_comma > last_dot:
                # decimal = ','  milhar = '.'
                txt = txt.replace(".", "").replace(",", ".")
            else:
                # decimal = '.'  milhar = ','
                txt = txt.replace(",", "")
            try:
                return float(txt)
            except:
                return 0.0

        # se tem só vírgula, assume vírgula decimal
        if "," in txt:
            try:
                return float(txt.replace(".", "").replace(",", "."))
            except:
                return 0.0

        # se tem só ponto: pode ser decimal OU milhar
        parts = txt.split(".")
        if len(parts) == 2 and len(parts[1]) == 3 and parts[0].isdigit() and parts[1].isdigit():
            try:
                return float(parts[0] + parts[1])  # "1.234" -> 1234
            except:
                return 0.0

        # caso normal (decimal com ponto)
        try:
            return float(txt)
        except:
            return 0.0

    return pd.Series(s).apply(parse_val).fillna(0.0)

def _require_cols(df: pd.DataFrame, cols, name="Dados") -> pd.DataFrame:
    """
    Garante colunas mínimas. Se faltarem, cria com vazio.
    """
    df = df.copy()
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"AVISO [{name}]: Colunas faltando {missing}. Preenchendo com vazios.")
        for c in missing:
            df[c] = 0.0 if any(k in c.lower() for k in ["qtd", "quant", "valor", "preco", "preço", "pm", "taxa"]) else ""
    return df

# =========================
# 2) NORMALIZADORES
# =========================

def normalize_master_ativos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera algo tipo: codigo/ativo/cod -> ticker; tipo/tipo_ativo -> classe; setor/segmento (opcional)
    Blindado contra colunas duplicadas após rename.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "classe"])

    df = _strip_columns(df)
    df = _dedupe_columns(df)

    rename = {
        "codigo": "ticker",
        "cod": "ticker",
        "ativo": "ticker",
        "ticker": "ticker",
        "tipo": "classe",
        "tipo_ativo": "classe",
        "classe": "classe",
        "setor": "setor",
        "setor_economico": "setor",
        "segmento": "segmento",
    }

    cols_map = {c: rename.get(str(c).strip().lower(), c) for c in df.columns}
    df = df.rename(columns=cols_map)

    # depois do rename pode ter ticker duplicado
    df = _dedupe_columns(df)

    df = _require_cols(df, ["ticker", "classe"], "Master")
    df["ticker"] = normalize_ticker_series(_get_col_series(df, "ticker"))

    # manter só 1 linha por ticker (última prevalece)
    df = df.drop_duplicates(subset=["ticker"], keep="last")

    return df

def normalize_posicoes_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera algo tipo: ativo/cod -> ticker; qtd/qtde -> quantidade; pm/preco medio -> preco_medio
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "quantidade", "preco_medio"])

    df = _strip_columns(df)
    df = _dedupe_columns(df)

    rename = {
        "ativo": "ticker",
        "cod": "ticker",
        "ticker": "ticker",
        "qtd": "quantidade",
        "qtde": "quantidade",
        "quantidade": "quantidade",
        "pm": "preco_medio",
        "preco medio": "preco_medio",
        "preço médio": "preco_medio",
        "preco_medio": "preco_medio",
    }
    cols_map = {c: rename.get(str(c).strip().lower(), c) for c in df.columns}
    df = df.rename(columns=cols_map)
    df = _dedupe_columns(df)

    df = _require_cols(df, ["ticker", "quantidade", "preco_medio"], "Posicoes")

    df["ticker"] = normalize_ticker_series(_get_col_series(df, "ticker"))
    df["quantidade"] = _to_float_series(_get_col_series(df, "quantidade"))
    df["preco_medio"] = _to_float_series(_get_col_series(df, "preco_medio"))

    return df[["ticker", "quantidade", "preco_medio"]]

def normalize_proventos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera algo tipo: ticker, data_pagamento (ou data), valor_total (ou valor)
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "data_pagamento", "valor_total"])

    df = _strip_columns(df)
    df = _dedupe_columns(df)

    if "ticker" in df.columns:
        df["ticker"] = normalize_ticker_series(_get_col_series(df, "ticker"))

    if "valor_total" in df.columns:
        df["valor_total"] = _to_float_series(_get_col_series(df, "valor_total"))

    return df

def normalize_cotacoes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "preco_atual"])

    df = _strip_columns(df)
    df = _dedupe_columns(df)

    # ticker
    ticker_col = None
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl in ["ticker", "ativo", "codigo", "cod"]:
            ticker_col = c
            break

    # preço atual (SELETIVO)
    preco_col = None
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl in ["preco_atual", "cotacao", "cotação", "ultimo", "último", "last", "price"]:
            preco_col = c
            break

    # fallback: só usa "preco"/"preço" se NÃO existir nada melhor
    if preco_col is None:
        for c in df.columns:
            cl = str(c).strip().lower()
            if cl in ["preco", "preço"]:
                preco_col = c
                break

    out = pd.DataFrame()
    out["ticker"] = normalize_ticker_series(df[ticker_col]) if ticker_col else pd.Series(dtype=str)
    out["preco_atual"] = _to_float_series(df[preco_col]) if preco_col else pd.Series(dtype=float)

    out = _dedupe_columns(out)
    out = out.dropna(subset=["ticker"])
    out = out[out["ticker"].astype(str).str.strip() != ""]
    out = out.drop_duplicates(subset=["ticker"], keep="last")

    return out[["ticker", "preco_atual"]]

# =========================
# 2.1) MOTOR: MOVIMENTAÇÕES -> POSIÇÕES (custo médio)
# =========================

def normalize_movimentacoes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera (mínimo):
    - data
    - ticker (ou ativo/cod/codigo)
    - tipo (compra/venda)
    - qtd (ou quantidade/qtde)
    - preco (ou preço)
    - taxa (opcional)

    Retorna DF padronizado:
    - data (datetime)
    - ticker (str)
    - tipo ("compra"|"venda")
    - qtd (float)
    - preco (float)
    - taxa (float)
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["data", "ticker", "tipo", "qtd", "preco", "taxa"])

    d = df.copy()
    d = _strip_columns(d)
    d.columns = [str(c).strip().lower() for c in d.columns]
    d = _dedupe_columns(d)

    # aliases
      # aliases
    if "ativo" in d.columns and "ticker" not in d.columns:
        d["ticker"] = d["ativo"]
    if "codigo" in d.columns and "ticker" not in d.columns:
        d["ticker"] = d["codigo"]
    if "cod" in d.columns and "ticker" not in d.columns:
        d["ticker"] = d["cod"]

    # aliases de quantidade
    if "quantidade" in d.columns and "qtd" not in d.columns:
        d["qtd"] = d["quantidade"]
    if "qtde" in d.columns and "qtd" not in d.columns:
        d["qtd"] = d["qtde"]

    # aliases de preço (AJUSTADO PARA SUA BASE)
    if "preço" in d.columns and "preco" not in d.columns:
        d["preco"] = d["preço"]
    if "preco_unitario" in d.columns and "preco" not in d.columns:
        d["preco"] = d["preco_unitario"]
    if "preco_unit" in d.columns and "preco" not in d.columns:
        d["preco"] = d["preco_unit"]

    d = _require_cols(d, ["data", "ticker", "tipo", "qtd", "preco"], "Movimentacoes")



    d["ticker"] = normalize_ticker_series(_get_col_series(d, "ticker"))
    tipo_raw = _get_col_series(d, "tipo").astype(str).str.strip().str.lower()
    d["tipo"] = tipo_raw.replace({"c": "compra", "buy": "compra", "v": "venda", "sell": "venda"})

    d["qtd"] = _to_float_series(_get_col_series(d, "qtd"))
    d["preco"] = _to_float_series(_get_col_series(d, "preco"))
    d["taxa"] = _to_float_series(_get_col_series(d, "taxa"))

    # data
    d["data"] = pd.to_datetime(_get_col_series(d, "data"), dayfirst=True, errors="coerce")

    # limpeza
    d = d[(d["ticker"].astype(str).str.strip() != "")]
    d = d[(d["tipo"].isin(["compra", "venda"]))]

    # regras mínimas
    d = d[(d["qtd"] > 0) & (d["preco"] > 0)]

    d = d.sort_values(["ticker", "data"], ascending=[True, True]).reset_index(drop=True)
    return d[["data", "ticker", "tipo", "qtd", "preco", "taxa"]]

def compute_positions_from_movs(movs: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula posições (snapshot) a partir das movimentações usando CUSTO MÉDIO.
    - Compra: aumenta qtd e custo
    - Venda: reduz qtd e reduz custo proporcional ao PM do momento

    Retorna:
    - ticker
    - quantidade
    - preco_medio
    """
    d = normalize_movimentacoes(movs)
    if d.empty:
        return pd.DataFrame(columns=["ticker", "quantidade", "preco_medio"])

    rows = []
    for ticker, g in d.groupby("ticker", sort=False):
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
                    raise ValueError(f"{ticker}: VENDA sem posição (qtd atual=0). Corrija a aba movimentações.")
                pm = (custo / qtd) if qtd > 0 else 0.0
                qtd -= q
                custo -= pm * q

        if qtd > 0:
            pm_final = (custo / qtd) if qtd > 0 else 0.0
            rows.append({
                "ticker": ticker,
                "quantidade": float(qtd),
                "preco_medio": float(pm_final),
            })

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["ticker", "quantidade", "preco_medio"])

    return out.sort_values("ticker").reset_index(drop=True)

# =========================
# 3) LÓGICA DE NEGÓCIO
# =========================

def enrich_positions_with_master(
    positions: pd.DataFrame,
    master: pd.DataFrame,
    quotes: pd.DataFrame
) -> pd.DataFrame:
    """
    Junta posições + master (classe/setor/segmento) + cotações.
    Calcula custo_total, valor_mercado, pl e peso.
    Blindado contra coluna 'ticker' duplicada.
    """
    pos = positions.copy() if positions is not None and not positions.empty else pd.DataFrame(columns=["ticker","quantidade","preco_medio"])
    mst = master.copy() if master is not None and not master.empty else pd.DataFrame(columns=["ticker","classe"])
    q   = quotes.copy() if quotes is not None and not quotes.empty else pd.DataFrame(columns=["ticker","preco_atual"])

    pos = _dedupe_columns(pos)
    mst = _dedupe_columns(mst)
    q   = _dedupe_columns(q)

    pos["ticker"] = normalize_ticker_series(_get_col_series(pos, "ticker"))
    mst["ticker"] = normalize_ticker_series(_get_col_series(mst, "ticker"))
    q["ticker"]   = normalize_ticker_series(_get_col_series(q, "ticker"))

    mst = mst.drop_duplicates(subset=["ticker"], keep="last")
    q   = q.drop_duplicates(subset=["ticker"], keep="last")

    cols_mst = ["ticker", "classe"] + [c for c in ["setor", "segmento"] if c in mst.columns]
    mst_use = mst[cols_mst].copy()
    mst_use = _dedupe_columns(mst_use)

    out = pos.merge(mst_use, on="ticker", how="left")

    out["classe"] = out.get("classe", pd.Series(dtype=object)).fillna("Outros")
    out["classificacao_status"] = out["classe"].apply(lambda x: "ativo não classificado" if x == "Outros" else "ok")

    q_use = q[["ticker", "preco_atual"]].copy()
    q_use = _dedupe_columns(q_use)

    out = out.merge(q_use, on="ticker", how="left")

    out["preco_medio"] = _to_float_series(_get_col_series(out, "preco_medio"))
    out["preco_atual"] = _to_float_series(_get_col_series(out, "preco_atual"))

    out["preco_atual"] = out["preco_atual"].replace(0.0, float("nan"))
    out["preco_atual"] = out["preco_atual"].fillna(out["preco_medio"]).fillna(0.0)

    qtd = _to_float_series(_get_col_series(out, "quantidade"))
    pm  = _to_float_series(_get_col_series(out, "preco_medio"))
    pa  = _to_float_series(_get_col_series(out, "preco_atual"))

    out["quantidade"] = qtd
    out["custo_total"] = qtd * pm
    out["valor_mercado"] = qtd * pa
    out["pl"] = out["valor_mercado"] - out["custo_total"]

    # sanity preço atual
    out["preco_atual"] = out["preco_atual"].astype(float)
    out.loc[(out["preco_atual"] > 50000) | (out["preco_atual"] < 0), "preco_atual"] = float("nan")
    out["preco_atual"] = out["preco_atual"].fillna(out["preco_medio"]).fillna(0.0)

    total = float(out["valor_mercado"].sum() or 0.0)
    out["peso"] = (out["valor_mercado"] / total) if total > 0 else 0.0

    return out

def compute_income_12m(income: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna proventos somados nos últimos 12 meses por ticker.
    Espera colunas: ticker + (data_pagamento ou data) + (valor_total ou valor)
    """
    if income is None or income.empty:
        return pd.DataFrame(columns=["ticker", "proventos_12m"])

    df = income.copy()
    df = _strip_columns(df)
    df = _dedupe_columns(df)

    if "ticker" in df.columns:
        df["ticker"] = normalize_ticker_series(_get_col_series(df, "ticker"))

    if "data_pagamento" in df.columns and "data" not in df.columns:
        df = df.rename(columns={"data_pagamento": "data"})
    if "valor_total" in df.columns and "valor" not in df.columns:
        df = df.rename(columns={"valor_total": "valor"})

    df = _dedupe_columns(df)
    df = _require_cols(df, ["ticker", "data", "valor"], "Income")

    df["data"] = pd.to_datetime(_get_col_series(df, "data"), dayfirst=True, errors="coerce")
    df["valor"] = _to_float_series(_get_col_series(df, "valor"))

    today = pd.Timestamp(datetime.now().date())
    start = today - relativedelta(months=12)

    df = df[(df["data"] >= start) & (df["data"] <= today)]
    out = df.groupby("ticker", as_index=False)["valor"].sum().rename(columns={"valor": "proventos_12m"})

    return out

def attach_income(enriched: pd.DataFrame, income12: pd.DataFrame) -> pd.DataFrame:
    """
    Anexa proventos_12m e yield_12m no DF enriquecido.
    """
    df = enriched.copy() if enriched is not None else pd.DataFrame()

    if df.empty:
        return df

    if income12 is None or income12.empty:
        df["proventos_12m"] = 0.0
        df["yield_12m"] = 0.0
        return df

    inc = income12.copy()
    inc = _strip_columns(inc)
    inc = _dedupe_columns(inc)

    inc["ticker"] = normalize_ticker_series(_get_col_series(inc, "ticker"))
    if "valor_total" in inc.columns and "proventos_12m" not in inc.columns:
        inc = inc.rename(columns={"valor_total": "proventos_12m"})
    if "valor" in inc.columns and "proventos_12m" not in inc.columns:
        inc = inc.rename(columns={"valor": "proventos_12m"})

    inc = _dedupe_columns(inc)

    df = _dedupe_columns(df)
    df = df.merge(inc[["ticker", "proventos_12m"]], on="ticker", how="left")
    df["proventos_12m"] = _to_float_series(_get_col_series(df, "proventos_12m"))

    def _y(row):
        vm = float(row.get("valor_mercado", 0.0) or 0.0)
        pv = float(row.get("proventos_12m", 0.0) or 0.0)
        return (pv / vm) if vm > 0 else 0.0

    df["yield_12m"] = df.apply(_y, axis=1)
    return df

def compute_portfolio_metrics(enriched: pd.DataFrame, income12: pd.DataFrame) -> dict:
    """
    Retorna métricas agregadas da carteira.
    """
    df = enriched.copy() if enriched is not None else pd.DataFrame()

    if df.empty:
        return {
            "patrimonio_total": 0.0,
            "custo_total": 0.0,
            "pl_total": 0.0,
            "rentab_pct": 0.0,
            "proventos_12m": 0.0,
            "yield_12m": 0.0
        }

    if "proventos_12m" not in df.columns:
        df = attach_income(df, income12)

    patrimonio = float(df["valor_mercado"].sum() or 0.0)
    custo      = float(df["custo_total"].sum() or 0.0)
    pl         = float(df["pl"].sum() or 0.0)
    prov       = float(df["proventos_12m"].sum() or 0.0)

    rentab = (pl / custo) if custo > 0 else 0.0
    y_pf   = (prov / patrimonio) if patrimonio > 0 else 0.0

    return {
        "patrimonio_total": patrimonio,
        "custo_total": custo,
        "pl_total": pl,
        "rentab_pct": rentab,
        "proventos_12m": prov,
        "yield_12m": y_pf
    }

def compute_allocations(enriched: pd.DataFrame):
    """
    Retorna:
    - alocação por classe
    - alocação por setor/segmento (se existirem)
    """
    df = enriched.copy() if enriched is not None else pd.DataFrame()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Classe
    if "classe" in df.columns:
        ac = df.groupby("classe", as_index=False)["valor_mercado"].sum()
        tot = float(ac["valor_mercado"].sum() or 0.0)
        ac["peso"] = ac["valor_mercado"] / tot if tot > 0 else 0.0
        ac = ac.sort_values("peso", ascending=False)
    else:
        ac = pd.DataFrame()

    # Setor/Segmento
    cols = [c for c in ["setor", "segmento"] if c in df.columns]
    if cols:
        ass = df.groupby(cols, as_index=False)["valor_mercado"].sum()
        tot = float(ass["valor_mercado"].sum() or 0.0)
        ass["peso"] = ass["valor_mercado"] / tot if tot > 0 else 0.0
        ass = ass.sort_values("peso", ascending=False)
    else:
        ass = pd.DataFrame()

    return ac, ass

def compute_concentration(enriched: pd.DataFrame, alert_pct: float = 10.0) -> pd.DataFrame:
    """
    Concentração por ativo com alerta > alert_pct (%).
    """
    df = enriched.copy() if enriched is not None else pd.DataFrame()
    if df.empty or "ticker" not in df.columns:
        return pd.DataFrame()

    for c in ["classe", "valor_mercado", "peso"]:
        if c not in df.columns:
            df[c] = 0.0 if c in ["valor_mercado", "peso"] else "Outros"

    conc = df[["ticker", "classe", "valor_mercado", "peso"]].copy()
    conc = conc.sort_values("peso", ascending=False)
    conc["peso_pct"] = conc["peso"] * 100.0
    conc["alerta"] = conc["peso_pct"].apply(lambda x: "⚠️" if float(x) > float(alert_pct) else "")

    return conc
