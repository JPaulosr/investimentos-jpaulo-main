# utils/aporte_engine.py
# -*- coding: utf-8 -*-
"""
APORTE ENGINE (CONTRATO) — Investimentos MD

✅ Objetivo:
- Gerar candidatos e ranking SEPARADO por classe (FII/FIAGRO vs AÇÕES)
- Score relativo (Borda ponderado discreto)
- Explicável (motivos/chips por ativo)
- Simulador imutável (não grava nada, não altera estado)

🚫 Proibições (contrato):
- Não recomendar compra/venda
- Não misturar classes
- AÇÕES: sem projeção automática de renda
- FII/FIAGRO: renda condicional só com base no ÚLTIMO provento recebido

🔌 Integração esperada:
- watchlist_aporte (universo fixo)
- regras_aporte (pesos discretos)
- alertas_ativos (via utils/alerts.py, que aplica penalidade/bloqueio)
- dados existentes: posições (ticker, quantidade, preco_medio), cotacoes_cache, proventos (recebidos)

Este módulo é "core puro": sem Streamlit.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import math
import pandas as pd


# =============================================================================
# Helpers básicos
# =============================================================================

def _norm_ticker(x) -> str:
    return str(x or "").strip().upper().replace(" ", "")

def _norm_classe(x) -> str:
    s = str(x or "").strip().upper()
    if s in ("ACAO", "ACOES", "AÇÃO", "AÇÕES"):
        return "AÇÕES"
    if s in ("FII", "FIIS"):
        return "FII"
    if s in ("FIAGRO", "FIAGROS"):
        return "FIAGRO"
    return s

def _to_float(x) -> float:
    if x is None:
        return float("nan")
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none", "null", "-"):
        return float("nan")
    s = s.replace("R$", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return float("nan")

def _safe_div(a: float, b: float) -> float:
    if b is None or b == 0 or (isinstance(b, float) and (math.isnan(b) or math.isinf(b))):
        return float("nan")
    return a / b

def _is_num(x) -> bool:
    try:
        return x is not None and not (isinstance(x, float) and (math.isnan(x) or math.isinf(x)))
    except Exception:
        return False

def _coalesce(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        return v
    return None


# =============================================================================
# Regras (regras_aporte)
# =============================================================================

DEFAULT_CRITERIA_FII = [
    "desconto_pm",
    "estabilidade_proventos",
    "ultimo_yield",
    "gap_alocacao",
    "concentracao",
]

DEFAULT_CRITERIA_ACOES = [
    "desconto_pm",
    "diversificacao",
    "gap_alocacao_setor",
    "concentracao",
]

def parse_regras_aporte(df_regras: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    """
    Retorna pesos por classe:
      { "FII": {"desconto_pm":2, ...}, "FIAGRO": {...}, "AÇÕES": {...} }
    Se df vazio, usa pesos padrão (1) para critérios padrão.
    """
    out: Dict[str, Dict[str, int]] = {
        "FII": {k: 1 for k in DEFAULT_CRITERIA_FII},
        "FIAGRO": {k: 1 for k in DEFAULT_CRITERIA_FII},
        "AÇÕES": {k: 1 for k in DEFAULT_CRITERIA_ACOES},
    }

    if df_regras is None or df_regras.empty:
        return out

    d = df_regras.copy()
    d.columns = [str(c).strip().lower() for c in d.columns]
    for col in ["classe", "criterio", "peso", "ativo"]:
        if col not in d.columns:
            d[col] = ""

    # ativa?
    def _to_bool(x):
        s = str(x or "").strip().lower()
        return s in ("1", "true", "t", "sim", "s", "yes", "y", "ok", "ativo", "on")

    d["classe"] = d["classe"].map(_norm_classe)
    d["criterio"] = d["criterio"].astype(str).str.strip().str.lower()
    d["peso"] = d["peso"].map(_to_float)
    d["ativo_flag"] = d["ativo"].map(_to_bool)

    d = d[d["ativo_flag"] == True].copy()
    d = d[d["classe"].isin(["FII", "FIAGRO", "AÇÕES"])].copy()
    d = d[d["criterio"] != ""].copy()

    for _, r in d.iterrows():
        cls = str(r["classe"])
        crit = str(r["criterio"])
        w = r["peso"]
        if not _is_num(w):
            continue
        w_int = int(round(float(w)))
        if w_int < 0:
            w_int = 0
        if cls not in out:
            out[cls] = {}
        out[cls][crit] = w_int

    return out


# =============================================================================
# Universo fixo (watchlist_aporte)
# =============================================================================

def load_universe_from_watchlist(df_watchlist: pd.DataFrame, classe: str) -> List[str]:
    """
    Extrai tickers ativos da watchlist para uma classe.
    """
    if df_watchlist is None or df_watchlist.empty:
        return []

    d = df_watchlist.copy()
    d.columns = [str(c).strip().lower() for c in d.columns]
    for col in ["ticker", "classe", "ativo"]:
        if col not in d.columns:
            d[col] = ""

    def _to_bool(x):
        s = str(x or "").strip().lower()
        return s in ("1", "true", "t", "sim", "s", "yes", "y", "ok", "ativo", "on")

    cls = _norm_classe(classe)
    d["ticker"] = d["ticker"].map(_norm_ticker)
    d["classe"] = d["classe"].map(_norm_classe)
    d["ativo_flag"] = d["ativo"].map(_to_bool)

    d = d[(d["ativo_flag"] == True) & (d["classe"] == cls) & (d["ticker"] != "")].copy()
    # universo fixo: mantém ordem do Sheets
    return d["ticker"].tolist()


# =============================================================================
# Proventos recebidos (último pagamento / estabilidade)
# =============================================================================

def _normalize_proventos(df_prov: pd.DataFrame) -> pd.DataFrame:
    if df_prov is None or df_prov.empty:
        return pd.DataFrame(columns=["ticker", "data", "valor_por_cota", "valor", "quantidade_na_data"])

    d = df_prov.copy()
    d.columns = [str(c).strip().lower() for c in d.columns]
    # aliases
    if "ativo" in d.columns and "ticker" not in d.columns:
        d["ticker"] = d["ativo"]
    if "quantidade" in d.columns and "quantidade_na_data" not in d.columns:
        d["quantidade_na_data"] = d["quantidade"]
    if "qtd" in d.columns and "quantidade_na_data" not in d.columns:
        d["quantidade_na_data"] = d["qtd"]

    for col in ["ticker", "data", "valor_por_cota", "valor", "quantidade_na_data"]:
        if col not in d.columns:
            d[col] = ""

    d["ticker"] = d["ticker"].map(_norm_ticker)
    d["data"] = pd.to_datetime(d["data"], errors="coerce", dayfirst=True)
    d["valor_por_cota_num"] = d["valor_por_cota"].map(_to_float)

    # fallback VPC = valor / qtd
    v_num = d["valor"].map(_to_float)
    q_num = d["quantidade_na_data"].map(_to_float)
    vpc_fallback = v_num / q_num.replace({0.0: float("nan")})
    d["vpc"] = d["valor_por_cota_num"]
    d.loc[~d["vpc"].map(_is_num), "vpc"] = vpc_fallback.loc[~d["vpc"].map(_is_num)]

    d = d.dropna(subset=["data"]).copy()
    d = d[(d["ticker"] != "") & (d["vpc"].map(_is_num)) & (d["vpc"] > 0)].copy()
    d = d.sort_values(["ticker", "data"], ascending=[True, False])
    return d[["ticker", "data", "vpc"]].copy()

def last_vpc(df_prov_norm: pd.DataFrame, ticker: str) -> float:
    if df_prov_norm is None or df_prov_norm.empty:
        return float("nan")
    t = _norm_ticker(ticker)
    g = df_prov_norm[df_prov_norm["ticker"] == t]
    if g.empty:
        return float("nan")
    return float(g.iloc[0]["vpc"])

def stability_metrics(df_prov_norm: pd.DataFrame, ticker: str, window: int = 6) -> Dict[str, float]:
    """
    Estabilidade baseada APENAS em proventos recebidos (VPC).
    Retorna:
      - n (qtde observações)
      - cv (coef var = std/mean) quanto menor melhor
    """
    t = _norm_ticker(ticker)
    if df_prov_norm is None or df_prov_norm.empty:
        return {"n": 0.0, "cv": float("nan")}
    g = df_prov_norm[df_prov_norm["ticker"] == t]
    if g.empty:
        return {"n": 0.0, "cv": float("nan")}
    s = g["vpc"].head(window).astype(float)
    n = float(len(s))
    if n <= 1:
        return {"n": n, "cv": float("nan")}
    mean = float(s.mean())
    std = float(s.std(ddof=1))
    cv = std / mean if mean > 0 else float("nan")
    return {"n": n, "cv": cv}


# =============================================================================
# Construção de candidatos (posições + cotações + master opcional)
# =============================================================================

def build_candidates(
    df_pos: pd.DataFrame,
    df_cot: pd.DataFrame,
    df_master: Optional[pd.DataFrame],
    universe: List[str],
    classe: str,
) -> pd.DataFrame:
    """
    Cria base mínima de candidatos para uma classe.
    Espera df_pos com colunas: ticker, quantidade, preco_medio (ou aliases).
    Espera df_cot com colunas: ticker, preco (ou cotacao/preco_atual).
    df_master é opcional (pode trazer setor, classe, etc).
    Retorna DF filtrado pelo universo fixo (watchlist).
    """
    cls = _norm_classe(classe)

    # --- posições
    if df_pos is None or df_pos.empty:
        pos = pd.DataFrame(columns=["ticker", "quantidade", "preco_medio"])
    else:
        pos = df_pos.copy()
        pos.columns = [str(c).strip().lower() for c in pos.columns]
        if "ativo" in pos.columns and "ticker" not in pos.columns:
            pos["ticker"] = pos["ativo"]
        if "quantidade" not in pos.columns and "qtd" in pos.columns:
            pos["quantidade"] = pos["qtd"]
        if "preço_medio" in pos.columns and "preco_medio" not in pos.columns:
            pos["preco_medio"] = pos["preço_medio"]
        for col in ["ticker", "quantidade", "preco_medio"]:
            if col not in pos.columns:
                pos[col] = ""
        pos["ticker"] = pos["ticker"].map(_norm_ticker)
        pos["quantidade"] = pos["quantidade"].map(_to_float)
        pos["preco_medio"] = pos["preco_medio"].map(_to_float)
        pos = pos[["ticker", "quantidade", "preco_medio"]].copy()

    # --- cotações
    if df_cot is None or df_cot.empty:
        cot = pd.DataFrame(columns=["ticker", "preco_atual"])
    else:
        cot = df_cot.copy()
        cot.columns = [str(c).strip().lower() for c in cot.columns]
        if "ativo" in cot.columns and "ticker" not in cot.columns:
            cot["ticker"] = cot["ativo"]
        # tenta achar coluna de preço
        price_col = None
        for c in ["preco_atual", "preco", "cotacao", "cotação", "valor", "ultimo_preco", "último_preço"]:
            if c in cot.columns:
                price_col = c
                break
        if price_col is None:
            cot["preco_atual"] = float("nan")
        else:
            cot["preco_atual"] = cot[price_col].map(_to_float)

        cot["ticker"] = cot["ticker"].map(_norm_ticker)
        cot = cot[["ticker", "preco_atual"]].copy()

    # --- master opcional
    if df_master is None or df_master.empty:
        master = pd.DataFrame(columns=["ticker", "classe", "setor"])
    else:
        master = df_master.copy()
        master.columns = [str(c).strip().lower() for c in master.columns]
        if "ativo" in master.columns and "ticker" not in master.columns:
            master["ticker"] = master["ativo"]
        for col in ["ticker"]:
            if col not in master.columns:
                master[col] = ""
        master["ticker"] = master["ticker"].map(_norm_ticker)

        # tenta inferir classe
        cls_col = None
        for c in ["classe", "tipo_ativo", "tipo ativo", "tipo"]:
            if c in master.columns:
                cls_col = c
                break
        if cls_col is None:
            master["classe"] = ""
        else:
            master["classe"] = master[cls_col].map(_norm_classe)

        # setor (para ações)
        setor_col = None
        for c in ["setor", "segmento", "industria", "indústria"]:
            if c in master.columns:
                setor_col = c
                break
        master["setor"] = master[setor_col].astype(str).str.strip() if setor_col else ""

        master = master[["ticker", "classe", "setor"]].copy()

    # merge base
    base = pd.DataFrame({"ticker": [_norm_ticker(t) for t in universe]})
    base["classe"] = cls
    d = base.merge(pos, on="ticker", how="left").merge(cot, on="ticker", how="left").merge(master, on="ticker", how="left", suffixes=("", "_m"))

    # classe final: preferir master se existir
    if "classe_m" in d.columns:
        d["classe"] = d["classe_m"].where(d["classe_m"].astype(str).str.strip() != "", d["classe"])
        d = d.drop(columns=["classe_m"])

    # sanitiza
    d["quantidade"] = d["quantidade"].map(_to_float)
    d["preco_medio"] = d["preco_medio"].map(_to_float)
    d["preco_atual"] = d["preco_atual"].map(_to_float)
    d["setor"] = d.get("setor", "").fillna("").astype(str).str.strip()

    # gating mínimo: sem preço atual ou pm -> revisão (não ranqueia)
    d["has_preco_atual"] = d["preco_atual"].map(_is_num) & (d["preco_atual"] > 0)
    d["has_pm"] = d["preco_medio"].map(_is_num) & (d["preco_medio"] > 0)
    d["gating_ok"] = d["has_preco_atual"] & d["has_pm"]

    return d


# =============================================================================
# Ranking: Borda ponderado discreto (score relativo)
# =============================================================================

def _borda_points_from_rank(rank_series: pd.Series) -> pd.Series:
    """
    rank 1 é o melhor.
    pontos = N - rank
    Empates: rank com método 'average' já cuida.
    """
    n = float(len(rank_series))
    return (n - rank_series).astype(float)

def _rank_metric(values: pd.Series, higher_is_better: bool, neutral_if_nan: bool = True) -> pd.Series:
    """
    Gera rank (1 melhor).
    Se neutral_if_nan: NaN vira rank médio (neutro).
    """
    v = values.astype(float)
    if higher_is_better:
        r = v.rank(ascending=False, method="average")
    else:
        r = v.rank(ascending=True, method="average")

    if neutral_if_nan:
        # coloca NaN como rank médio
        mask = ~v.map(_is_num)
        if mask.any():
            r_mean = float(r.mean()) if len(r) else 1.0
            r.loc[mask] = r_mean
    return r

def _apply_tie_break(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tie-break determinístico (contrato):
      1) menor concentracao (melhor)
      2) maior gap_alocacao (melhor)
      3) maior desconto_pm (melhor)
      4) ticker A-Z
    Se colunas não existirem, ignora o nível.
    """
    d = df.copy()

    # defaults
    if "concentracao" not in d.columns:
        d["concentracao"] = float("nan")
    if "gap_alocacao" not in d.columns:
        d["gap_alocacao"] = float("nan")
    if "desconto_pm" not in d.columns:
        d["desconto_pm"] = float("nan")

    # ordena com NaN indo para o fim
    def _na_last(s: pd.Series, asc: bool):
        tmp = s.copy()
        tmp = tmp.where(tmp.map(_is_num), float("inf") if asc else float("-inf"))
        return tmp

    d["_tb_conc"] = _na_last(d["concentracao"], asc=True)          # menor melhor
    d["_tb_gap"] = _na_last(d["gap_alocacao"], asc=False)          # maior melhor
    d["_tb_desc"] = _na_last(d["desconto_pm"], asc=False)          # maior melhor (desconto mais negativo? ver abaixo)
    # Atenção: desconto_pm aqui será "quanto abaixo do PM" em valor positivo (melhor maior).
    # ticker
    d["_tb_t"] = d["ticker"].astype(str)

    d = d.sort_values(by=["score_base", "_tb_conc", "_tb_gap", "_tb_desc", "_tb_t"],
                      ascending=[False, True, False, False, True]).copy()

    d = d.drop(columns=["_tb_conc", "_tb_gap", "_tb_desc", "_tb_t"])
    return d


# =============================================================================
# Cálculo de métricas por classe (somente sinais permitidos)
# =============================================================================

def compute_metrics_fii(
    df: pd.DataFrame,
    df_prov_norm: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calcula métricas permitidas FII/FIAGRO:
      - desconto_pm (positivo = desconto)
      - ultimo_vpc (último provento recebido)
      - ultimo_yield (condicional: ultimo_vpc / preco_atual)
      - estabilidade_proventos (cv: menor melhor -> transformado para score maior melhor)
    """
    d = df.copy()

    # desconto vs PM: converter em "desconto" positivo (melhor maior)
    # desconto_pm = max(0, (pm - preco_atual) / pm)
    pm = d["preco_medio"].astype(float)
    pa = d["preco_atual"].astype(float)
    d["desconto_pm"] = ((pm - pa) / pm).where((pm > 0) & pm.map(_is_num) & pa.map(_is_num), float("nan"))
    d["desconto_pm"] = d["desconto_pm"].where(d["desconto_pm"].map(_is_num), float("nan"))

    # último vpc recebido (não projetar média)
    d["ultimo_vpc"] = d["ticker"].apply(lambda t: last_vpc(df_prov_norm, t))

    # yield condicional (último pagamento / cotação atual)
    d["ultimo_yield"] = d.apply(
        lambda r: _safe_div(float(r["ultimo_vpc"]), float(r["preco_atual"])) if _is_num(r["ultimo_vpc"]) and _is_num(r["preco_atual"]) and r["preco_atual"] > 0 else float("nan"),
        axis=1
    )

    # estabilidade: usa CV (menor melhor) -> estabilidade_score = 1/(1+cv)
    def _stab(ticker: str) -> float:
        m = stability_metrics(df_prov_norm, ticker, window=6)
        cv = m.get("cv", float("nan"))
        if not _is_num(cv) or cv < 0:
            return float("nan")
        return 1.0 / (1.0 + float(cv))

    d["estabilidade_proventos"] = d["ticker"].apply(_stab)

    return d

def compute_metrics_acoes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas permitidas AÇÕES:
      - desconto_pm (positivo = desconto)
      - diversificacao (proxy: 1/(1+peso_ativo) se peso_ativo existir)
      - gap_alocacao_setor (se existir: alvo_setor - peso_setor)
    """
    d = df.copy()

    pm = d["preco_medio"].astype(float)
    pa = d["preco_atual"].astype(float)
    d["desconto_pm"] = ((pm - pa) / pm).where((pm > 0) & pm.map(_is_num) & pa.map(_is_num), float("nan"))
    d["desconto_pm"] = d["desconto_pm"].where(d["desconto_pm"].map(_is_num), float("nan"))

    # diversificacao: se tiver peso_ativo (%), menor peso -> melhor diversificar
    if "peso_ativo" in d.columns:
        p = d["peso_ativo"].map(_to_float)
        d["diversificacao"] = (1.0 / (1.0 + p)).where(p.map(_is_num), float("nan"))
    else:
        d["diversificacao"] = float("nan")

    # gap de alocação por setor (se tiver colunas)
    if "alvo_setor" in d.columns and "peso_setor" in d.columns:
        alvo = d["alvo_setor"].map(_to_float)
        atual = d["peso_setor"].map(_to_float)
        d["gap_alocacao_setor"] = (alvo - atual).where(alvo.map(_is_num) & atual.map(_is_num), float("nan"))
    else:
        d["gap_alocacao_setor"] = float("nan")

    return d


# =============================================================================
# Score e chips
# =============================================================================

def score_candidates(
    df: pd.DataFrame,
    weights: Dict[str, int],
    criteria_order: List[str],
    higher_is_better_map: Dict[str, bool],
    penalty_positions_col: str = "penalty_positions",
) -> pd.DataFrame:
    """
    Calcula score_base via Borda ponderado discreto.
    Espera colunas de critérios em df.
    """
    d = df.copy()

    # score base
    d["score_base"] = 0.0

    # ranks por critério (para explicação/auditoria)
    for crit in criteria_order:
        w = int(weights.get(crit, 0) or 0)
        if w <= 0:
            continue

        if crit not in d.columns:
            d[crit] = float("nan")

        hib = bool(higher_is_better_map.get(crit, True))
        r = _rank_metric(d[crit], higher_is_better=hib, neutral_if_nan=True)
        d[f"rank_{crit}"] = r

        pts = _borda_points_from_rank(r)
        d["score_base"] += float(w) * pts

    # penalidade (deslocamento)
    if penalty_positions_col not in d.columns:
        d[penalty_positions_col] = 0
    d[penalty_positions_col] = d[penalty_positions_col].fillna(0).astype(int)

    return d

def build_chips_fii(row: pd.Series) -> List[str]:
    chips: List[str] = []
    if row.get("gating_ok") is not True:
        chips.append("REVISÃO: DADOS MÍNIMOS")
        return chips

    # desconto
    desc = row.get("desconto_pm")
    if _is_num(desc) and float(desc) > 0:
        chips.append("DESCONTO vs PM")

    # estabilidade
    stab = row.get("estabilidade_proventos")
    if _is_num(stab):
        chips.append("ESTABILIDADE (RECEBIDOS)")

    # último yield
    uy = row.get("ultimo_yield")
    if _is_num(uy):
        chips.append("ÚLTIMO YIELD (COND.)")

    # alertas
    sev = str(row.get("alert_severity", "") or "").strip().upper()
    pen = int(row.get("penalty_positions", 0) or 0)
    if sev == "AMARELO" and pen > 0:
        chips.append(f"ALERTA AMARELO (+{pen})")
    if sev in ("VERMELHO", "CINZA"):
        chips.append(f"BLOQUEADO ({sev})")

    return chips

def build_chips_acoes(row: pd.Series) -> List[str]:
    chips: List[str] = []
    if row.get("gating_ok") is not True:
        chips.append("REVISÃO: DADOS MÍNIMOS")
        return chips

    desc = row.get("desconto_pm")
    if _is_num(desc) and float(desc) > 0:
        chips.append("DESCONTO vs PM")

    div = row.get("diversificacao")
    if _is_num(div):
        chips.append("DIVERSIFICAÇÃO")

    sev = str(row.get("alert_severity", "") or "").strip().upper()
    pen = int(row.get("penalty_positions", 0) or 0)
    if sev == "AMARELO" and pen > 0:
        chips.append(f"ALERTA AMARELO (+{pen})")
    if sev in ("VERMELHO", "CINZA"):
        chips.append(f"BLOQUEADO ({sev})")

    return chips


# =============================================================================
# Rankers públicos (por classe)
# =============================================================================

def rank_fii_fiagro(
    df_candidates: pd.DataFrame,
    df_proventos: pd.DataFrame,
    weights: Dict[str, int],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retorna (ranked, revisao).
    - ranked: apenas gating_ok=True
    - revisao: gating_ok=False
    """
    d = df_candidates.copy()
    prov_norm = _normalize_proventos(df_proventos)
    d = compute_metrics_fii(d, prov_norm)

    # mapa de "maior é melhor"
    hib = {
        "desconto_pm": True,
        "estabilidade_proventos": True,
        "ultimo_yield": True,
        "gap_alocacao": True,      # se existir no df, maior gap (mais abaixo do alvo) é melhor
        "concentracao": False,     # menor concentração é melhor
    }

    d = score_candidates(
        d,
        weights=weights,
        criteria_order=DEFAULT_CRITERIA_FII,
        higher_is_better_map=hib,
    )

    # chips
    d["chips"] = d.apply(build_chips_fii, axis=1)

    # separa revisão
    revisao = d[d["gating_ok"] != True].copy()

    ranked = d[d["gating_ok"] == True].copy()
    ranked = _apply_tie_break(ranked)

    # aplica deslocamento AMARELO após ordenação (freio emocional)
    ranked["rank_pos"] = range(1, len(ranked) + 1)
    ranked["rank_pos_ajustada"] = ranked["rank_pos"] + ranked["penalty_positions"].fillna(0).astype(int)

    # reordena por rank ajustado + tie-break estável
    ranked = ranked.sort_values(by=["rank_pos_ajustada", "ticker"], ascending=[True, True]).copy()
    ranked["rank_final"] = range(1, len(ranked) + 1)

    return ranked, revisao


def rank_acoes(
    df_candidates: pd.DataFrame,
    weights: Dict[str, int],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retorna (ranked, revisao). AÇÕES sem renda/projeção.
    """
    d = df_candidates.copy()
    d = compute_metrics_acoes(d)

    hib = {
        "desconto_pm": True,
        "diversificacao": True,
        "gap_alocacao_setor": True,
        "concentracao": False,
    }

    d = score_candidates(
        d,
        weights=weights,
        criteria_order=DEFAULT_CRITERIA_ACOES,
        higher_is_better_map=hib,
    )

    d["chips"] = d.apply(build_chips_acoes, axis=1)

    revisao = d[d["gating_ok"] != True].copy()
    ranked = d[d["gating_ok"] == True].copy()
    ranked = _apply_tie_break(ranked)

    ranked["rank_pos"] = range(1, len(ranked) + 1)
    ranked["rank_pos_ajustada"] = ranked["rank_pos"] + ranked["penalty_positions"].fillna(0).astype(int)
    ranked = ranked.sort_values(by=["rank_pos_ajustada", "ticker"], ascending=[True, True]).copy()
    ranked["rank_final"] = range(1, len(ranked) + 1)

    return ranked, revisao


# =============================================================================
# Simulador imutável (não grava, não altera input)
# =============================================================================

@dataclass(frozen=True)
class SimResult:
    ticker: str
    valor_aporte: float
    preco_ref: float
    qtd_antes: float
    pm_antes: float
    qtd_depois: float
    pm_depois: float
    # métricas condicionais (podem ser NaN)
    renda_cond_antes: float
    renda_cond_depois: float


def simulate_aporte(
    df_candidates: pd.DataFrame,
    df_proventos: pd.DataFrame,
    ticker: str,
    valor_aporte: float,
    preco_ref: Optional[float] = None,
) -> SimResult:
    """
    Simula impacto no PM e quantidade usando preço_ref (default: preco_atual do candidato).
    Para FII/FIAGRO calcula renda condicional (ultimo_vpc * qtd).
    Para AÇÕES, renda_cond = NaN (não existe projeção automática).

    Não altera df_candidates.
    """
    t = _norm_ticker(ticker)
    v = float(valor_aporte or 0.0)
    if v <= 0:
        raise ValueError("simulate_aporte: valor_aporte deve ser > 0.")

    d = df_candidates.copy()
    if "ticker" not in d.columns:
        raise ValueError("simulate_aporte: df_candidates precisa ter 'ticker'.")

    d["ticker"] = d["ticker"].map(_norm_ticker)
    row = d[d["ticker"] == t]
    if row.empty:
        raise ValueError(f"simulate_aporte: ticker {t} não encontrado no universo carregado.")

    r0 = row.iloc[0]
    cls = _norm_classe(r0.get("classe", ""))

    qtd0 = float(_coalesce(_to_float(r0.get("quantidade")), 0.0) or 0.0)
    pm0 = float(_coalesce(_to_float(r0.get("preco_medio")), float("nan")))

    p_ref = float(_coalesce(preco_ref, _to_float(r0.get("preco_atual"))))
    if not _is_num(p_ref) or p_ref <= 0:
        raise ValueError("simulate_aporte: preco_ref inválido (sem cotação válida).")

    qtd_add = v / p_ref
    qtd1 = qtd0 + qtd_add

    # pm depois: (custo_antes + aporte) / qtd1
    # custo_antes aproximado = pm0 * qtd0 (sem taxas aqui; é simulação)
    custo0 = pm0 * qtd0 if _is_num(pm0) and pm0 > 0 else float("nan")
    custo1 = (custo0 + v) if _is_num(custo0) else float("nan")
    pm1 = (custo1 / qtd1) if _is_num(custo1) and qtd1 > 0 else float("nan")

    prov_norm = _normalize_proventos(df_proventos)
    lv = last_vpc(prov_norm, t)

    renda0 = float("nan")
    renda1 = float("nan")
    if cls in ("FII", "FIAGRO"):
        # renda condicional: se repetir o último pagamento recebido
        if _is_num(lv) and lv > 0:
            renda0 = lv * qtd0
            renda1 = lv * qtd1

    return SimResult(
        ticker=t,
        valor_aporte=v,
        preco_ref=p_ref,
        qtd_antes=qtd0,
        pm_antes=pm0,
        qtd_depois=qtd1,
        pm_depois=pm1,
        renda_cond_antes=renda0,
        renda_cond_depois=renda1,
    )
