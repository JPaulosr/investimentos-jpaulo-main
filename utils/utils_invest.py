# utils_invest.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd

# =========================
# 0) VERSÃO DO MOTOR
# =========================
MOTOR_VERSION = "0.3.0"

# =========================
# 1) NORMALIZAÇÃO / VALIDADORES
# =========================
TICKER_RE = re.compile(r"^[A-Z]{4,5}\d{1,2}$")

def norm_ticker(t: str) -> str:
    t = (t or "").strip().upper()
    t = t.replace(" ", "")
    return t

def validar_ticker(t: str) -> bool:
    t = norm_ticker(t)
    return bool(TICKER_RE.match(t))

def to_float_br(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("R$", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return None

def to_int(v, default: int = 0) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return default
        s = s.replace(".", "").replace(",", ".")
        return int(float(s))
    except:
        return default

def ensure_columns(df: pd.DataFrame, required: List[str], df_name: str = "df") -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{df_name}: faltam colunas obrigatórias: {missing}. Colunas atuais: {list(df.columns)}")

# =========================
# 2) CÁLCULOS BÁSICOS
# =========================
def calc_total_compra(qtd: float, preco: float, taxa: float = 0.0) -> float:
    qtd = float(qtd or 0)
    preco = float(preco or 0)
    taxa = float(taxa or 0)
    return round(qtd * preco + taxa, 2)

def calc_unitario_provento(total: float, qtd: float) -> float:
    total = float(total or 0)
    qtd = float(qtd or 0)
    if qtd <= 0:
        return 0.0
    return round(total / qtd, 6)

# =========================
# 3) MODELOS (CONTRATOS DO PROJETO)
# =========================
@dataclass(frozen=True)
class InvestConfig:
    # moeda / datas
    base_currency: str = "BRL"
    # metas de alocação por classe (somar ~100)
    alvo_por_classe: Optional[Dict[str, float]] = None
    # limite de concentração (ex: 0.15 = 15%)
    max_peso_ativo: float = 0.20
    # quanto do patrimônio em "caixa" é aceitável
    min_caixa: float = 0.00
    # como tratar taxa
    incluir_taxa_no_preco_medio: bool = True

DEFAULT_CONFIG = InvestConfig(
    alvo_por_classe={
        "RF": 0.40,
        "AÇÕES": 0.35,
        "FII": 0.15,
        "EXTERIOR": 0.10,
    },
    max_peso_ativo=0.20,
)

@dataclass(frozen=True)
class DashboardKPIs:
    patrimonio_total: float
    custo_total: float
    lucro_total: float
    retorno_total_pct: float
    proventos_12m: float
    yield_12m_pct: float

# =========================
# 4) SCHEMAS (NOMES FIXOS DE COLUNAS)
# =========================
# A ideia é: qualquer página que “funcione” vai produzir DataFrames com essas colunas.
# Isso faz o projeto sobreviver a vários chats e várias páginas.

SCHEMA_POSICOES = [
    "ticker",            # ex: "ITSA4"
    "classe",            # "RF" | "AÇÕES" | "FII" | "EXTERIOR" | "CAIXA"
    "qtd",               # quantidade
    "preco_atual",       # preço atual
    "preco_medio",       # preço médio
]

SCHEMA_APORTES = [
    "data",              # date/datetime/str
    "ticker",
    "qtd",
    "preco",
    "taxa",              # opcional: pode ser 0
]

SCHEMA_PROVENTOS = [
    "data",
    "ticker",
    "valor_total",
]

# =========================
# 5) LIMPEZA / PADRONIZAÇÃO
# =========================
def padronizar_posicoes(df: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(df, SCHEMA_POSICOES, "posicoes")
    out = df.copy()

    out["ticker"] = out["ticker"].astype(str).map(norm_ticker)
    out["classe"] = out["classe"].astype(str).str.strip().str.upper()

    out["qtd"] = out["qtd"].map(to_float_br).fillna(0.0).astype(float)
    out["preco_atual"] = out["preco_atual"].map(to_float_br).fillna(0.0).astype(float)
    out["preco_medio"] = out["preco_medio"].map(to_float_br).fillna(0.0).astype(float)

    # remove linhas inúteis
    out = out[(out["ticker"] != "") & (out["qtd"] > 0)]
    return out

def padronizar_aportes(df: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(df, SCHEMA_APORTES, "aportes")
    out = df.copy()

    out["ticker"] = out["ticker"].astype(str).map(norm_ticker)
    out["qtd"] = out["qtd"].map(to_float_br).fillna(0.0).astype(float)
    out["preco"] = out["preco"].map(to_float_br).fillna(0.0).astype(float)
    out["taxa"] = out.get("taxa", 0).map(to_float_br).fillna(0.0).astype(float)

    out["data"] = pd.to_datetime(out["data"], errors="coerce").dt.date
    out = out[(out["ticker"] != "") & (out["qtd"] > 0) & (out["preco"] > 0)]
    return out

def padronizar_proventos(df: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(df, SCHEMA_PROVENTOS, "proventos")
    out = df.copy()

    out["ticker"] = out["ticker"].astype(str).map(norm_ticker)
    out["valor_total"] = out["valor_total"].map(to_float_br).fillna(0.0).astype(float)
    out["data"] = pd.to_datetime(out["data"], errors="coerce").dt.date

    out = out[(out["ticker"] != "") & (out["valor_total"] > 0)]
    return out

# =========================
# 6) MÉTRICAS PRINCIPAIS
# =========================
def calcular_posicoes_enriquecidas(posicoes: pd.DataFrame) -> pd.DataFrame:
    p = padronizar_posicoes(posicoes)
    p["valor_mercado"] = (p["qtd"] * p["preco_atual"]).round(2)
    p["custo"] = (p["qtd"] * p["preco_medio"]).round(2)
    p["lucro"] = (p["valor_mercado"] - p["custo"]).round(2)
    p["retorno_pct"] = p.apply(lambda r: round((r["lucro"] / r["custo"])*100, 4) if r["custo"] > 0 else 0.0, axis=1)
    total = float(p["valor_mercado"].sum() or 0.0)
    p["peso"] = p["valor_mercado"].apply(lambda v: (v / total) if total > 0 else 0.0)
    return p

def calcular_kpis(posicoes: pd.DataFrame, proventos: Optional[pd.DataFrame] = None) -> DashboardKPIs:
    p = calcular_posicoes_enriquecidas(posicoes)

    patrimonio = float(p["valor_mercado"].sum() or 0.0)
    custo = float(p["custo"].sum() or 0.0)
    lucro = float(p["lucro"].sum() or 0.0)
    retorno_pct = round((lucro / custo) * 100, 4) if custo > 0 else 0.0

    prov_12m = 0.0
    if proventos is not None and len(proventos) > 0:
        pr = padronizar_proventos(proventos)
        hoje = date.today()
        inicio = (pd.Timestamp(hoje) - pd.DateOffset(months=12)).date()
        prov_12m = float(pr[(pr["data"] >= inicio) & (pr["data"] <= hoje)]["valor_total"].sum() or 0.0)

    yield_12m = round((prov_12m / patrimonio) * 100, 4) if patrimonio > 0 else 0.0

    return DashboardKPIs(
        patrimonio_total=round(patrimonio, 2),
        custo_total=round(custo, 2),
        lucro_total=round(lucro, 2),
        retorno_total_pct=retorno_pct,
        proventos_12m=round(prov_12m, 2),
        yield_12m_pct=yield_12m,
    )

# =========================
# 7) ALOCAÇÃO E ALERTAS (GESTÃO)
# =========================
def alocacao_por_classe(posicoes: pd.DataFrame) -> pd.DataFrame:
    p = calcular_posicoes_enriquecidas(posicoes)
    g = p.groupby("classe", as_index=False)["valor_mercado"].sum()
    total = float(g["valor_mercado"].sum() or 0.0)
    g["peso"] = g["valor_mercado"].apply(lambda v: (v / total) if total > 0 else 0.0)
    return g.sort_values("valor_mercado", ascending=False)

def alertas_concentracao(posicoes: pd.DataFrame, cfg: InvestConfig = DEFAULT_CONFIG) -> pd.DataFrame:
    p = calcular_posicoes_enriquecidas(posicoes)
    out = p[p["peso"] > float(cfg.max_peso_ativo)].copy()
    out["limite"] = float(cfg.max_peso_ativo)
    out["excesso"] = (out["peso"] - out["limite"])
    return out.sort_values("peso", ascending=False)

# =========================
# 8) MOTOR DE APORTES (DECISÃO)
# =========================
def sugestao_aporte_por_alvo(posicoes: pd.DataFrame, valor_aporte: float, cfg: InvestConfig = DEFAULT_CONFIG) -> pd.DataFrame:
    """
    Sugere distribuição do aporte por CLASSE com base no alvo.
    (Depois você pode evoluir para sugerir por ATIVO.)
    """
    valor_aporte = float(valor_aporte or 0.0)
    if valor_aporte <= 0:
        return pd.DataFrame(columns=["classe", "alvo", "peso_atual", "delta", "aporte_sugerido"])

    alvo = cfg.alvo_por_classe or {}
    a = alocacao_por_classe(posicoes)
    mapa_atual = {str(r["classe"]).upper(): float(r["peso"]) for _, r in a.iterrows()}

    rows = []
    for classe, alvo_pct in alvo.items():
        classe_u = str(classe).upper()
        atual = float(mapa_atual.get(classe_u, 0.0))
        delta = float(alvo_pct) - atual
        rows.append({"classe": classe_u, "alvo": float(alvo_pct), "peso_atual": atual, "delta": delta})

    df = pd.DataFrame(rows)
    # só classes abaixo do alvo recebem aporte
    df["delta_pos"] = df["delta"].apply(lambda x: max(0.0, x))
    soma = float(df["delta_pos"].sum() or 0.0)

    if soma <= 0:
        df["aporte_sugerido"] = 0.0
    else:
        df["aporte_sugerido"] = df["delta_pos"].apply(lambda d: round((d / soma) * valor_aporte, 2))

    return df[["classe", "alvo", "peso_atual", "delta", "aporte_sugerido"]].sort_values("aporte_sugerido", ascending=False)

# =========================
# 9) “SAÍDAS PADRÃO” PARA AS PÁGINAS
# =========================
def gerar_pacote_dashboard(posicoes: pd.DataFrame, proventos: Optional[pd.DataFrame] = None, cfg: InvestConfig = DEFAULT_CONFIG) -> Dict[str, object]:
    """
    Retorna um pacote fixo (contrato) para qualquer página usar.
    Assim você muda a regra aqui e todas as telas seguem iguais.
    """
    p_enr = calcular_posicoes_enriquecidas(posicoes)
    kpis = calcular_kpis(posicoes, proventos)
    alloc = alocacao_por_classe(posicoes)
    alertas = alertas_concentracao(posicoes, cfg)

    return {
        "motor_version": MOTOR_VERSION,
        "kpis": kpis,
        "posicoes": p_enr,
        "alocacao_classe": alloc,
        "alertas_concentracao": alertas,
    }
