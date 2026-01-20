# utils/alerts.py
# -*- coding: utf-8 -*-
"""
ALERTS ENGINE (CONTRATO) — Investimentos MD

✅ Objetivo:
- Aplicar alertas MANUAIS (aba alertas_ativos) como efeitos determinísticos:
  - VERMELHO / CINZA -> BLOQUEIA (remove da elegibilidade)
  - AMARELO          -> PENALIZA (deslocamento em posições, +K)
  - VERDE            -> sem efeito

🔒 Regras:
- Sem UI (sem emojis, sem textos persuasivos)
- Sem heurísticas (não calcula "pagou mais/menos", não usa média)
- Fonte de verdade: Sheets (alertas_ativos)
- Tolerante à ausência: se df_alertas vazio/inválido -> sem efeitos
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd


# -------------------------
# Constantes de contrato
# -------------------------

SEVERIDADES_VALIDAS = {"VERDE", "AMARELO", "VERMELHO", "CINZA"}

DEFAULT_PENALTY_K = 2  # deslocamento padrão para AMARELO (se regra não informar)


# -------------------------
# Helpers
# -------------------------

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()
    d.columns = [str(c).strip().lower() for c in d.columns]
    # aliases tolerantes (caso algum header venha diferente)
    aliases = {
        "ticker": ["ticker", "ativo", "codigo", "código", "cod", "papel"],
        "classe": ["classe", "tipo_ativo", "tipo ativo", "tipo", "class"],
        "severidade": ["severidade", "severity", "nivel", "nível"],
        "motivo": ["motivo", "descricao", "descrição", "obs", "observacao", "observação", "comentario", "comentário"],
        "ativo": ["ativo", "enabled", "habilitado"],
        "criado_em": ["criado_em", "criado em", "data", "data_criacao", "criado"],
        "expira_em": ["expira_em", "expira em", "validade", "ate", "até", "expires"],
    }
    # mapeia colunas existentes para chave padrão
    col_map = {}
    for std, syns in aliases.items():
        for s in syns:
            s0 = str(s).strip().lower()
            for c in d.columns:
                if c == s0:
                    col_map[c] = std
    if col_map:
        d = d.rename(columns=col_map)
    return d


def _to_bool(x) -> bool:
    if x is None:
        return False
    if isinstance(x, bool):
        return bool(x)
    s = str(x).strip().lower()
    return s in ("1", "true", "t", "yes", "y", "sim", "s", "ok", "ativo", "on")


def _norm_ticker(x) -> str:
    return str(x or "").strip().upper().replace(" ", "")


def _norm_classe(x) -> str:
    # mantém o contrato (AÇÕES | FII | FIAGRO) mas tolera variações
    s = str(x or "").strip().upper()
    s = s.replace("ACOES", "AÇÕES").replace("ACAO", "AÇÕES").replace("AÇÕES", "AÇÕES")
    if s in ("FII", "FIIS"):
        return "FII"
    if s in ("FIAGRO", "FIAGROS"):
        return "FIAGRO"
    if s in ("AÇÃO", "AÇÕES", "ACOES"):
        return "AÇÕES"
    return s


def _norm_severidade(x) -> str:
    s = str(x or "").strip().upper()
    # tolerâncias comuns
    s = s.replace("VERMELHA", "VERMELHO").replace("AMARELA", "AMARELO")
    if s not in SEVERIDADES_VALIDAS:
        return ""
    return s


def _safe_str(x) -> str:
    s = str(x or "").strip()
    return s


def _build_alerts_map(df_alertas: pd.DataFrame, classe: Optional[str] = None) -> Dict[str, Dict]:
    """
    Retorna map por ticker com:
      - severidade (prioridade máxima por ticker)
      - motivos (lista)
    Prioridade por severidade:
      VERMELHO/CINZA > AMARELO > VERDE
    """
    if df_alertas is None or df_alertas.empty:
        return {}

    d = _norm_cols(df_alertas)
    if d.empty:
        return {}

    # garante colunas mínimas (tolerante)
    for c in ["ticker", "classe", "severidade", "motivo", "ativo"]:
        if c not in d.columns:
            d[c] = ""

    d["ticker"] = d["ticker"].map(_norm_ticker)
    d["classe"] = d["classe"].map(_norm_classe)
    d["severidade"] = d["severidade"].map(_norm_severidade)
    d["motivo"] = d["motivo"].map(_safe_str)
    d["ativo_flag"] = d["ativo"].map(_to_bool)

    d = d[(d["ativo_flag"] == True)]
    d = d[(d["ticker"] != "")]
    d = d[(d["severidade"] != "")]

    if classe:
        cls = _norm_classe(classe)
        d = d[d["classe"] == cls]

    if d.empty:
        return {}

    priority = {"VERMELHO": 3, "CINZA": 3, "AMARELO": 2, "VERDE": 1}

    out: Dict[str, Dict] = {}
    for _, r in d.iterrows():
        t = r["ticker"]
        sev = r["severidade"]
        motivo = r.get("motivo", "") or ""
        if t not in out:
            out[t] = {"severidade": sev, "motivos": [motivo] if motivo else []}
        else:
            # acumula motivo
            if motivo:
                out[t]["motivos"].append(motivo)
            # escolhe a maior prioridade
            if priority.get(sev, 0) > priority.get(out[t]["severidade"], 0):
                out[t]["severidade"] = sev

    return out


# -------------------------
# API pública
# -------------------------

def apply_alerts(
    df_candidates: pd.DataFrame,
    df_alertas: pd.DataFrame,
    classe: str,
    penalty_k: int = DEFAULT_PENALTY_K,
) -> pd.DataFrame:
    """
    Aplica efeitos determinísticos de alertas na lista de candidatos (UMA classe por vez).
    Entradas:
      - df_candidates: precisa ter coluna 'ticker' (e idealmente 'classe')
      - df_alertas: aba alertas_ativos (Sheets) já carregada (pode estar vazia)
      - classe: "AÇÕES" | "FII" | "FIAGRO"
      - penalty_k: deslocamento de posições para AMARELO (inteiro >=0)

    Saída:
      - DF com colunas:
        - blocked (bool)
        - penalty_positions (int)
        - alert_severity (str)
        - alert_motives (str)  # texto com motivos concatenados
    """
    if df_candidates is None or df_candidates.empty:
        return pd.DataFrame(columns=["ticker", "blocked", "penalty_positions", "alert_severity", "alert_motives"])

    d = df_candidates.copy()
    if "ticker" not in d.columns:
        raise ValueError("apply_alerts: df_candidates precisa ter coluna 'ticker'.")

    d["ticker"] = d["ticker"].map(_norm_ticker)
    cls = _norm_classe(classe)

    # se vier classe no candidates, filtra por segurança (separação absoluta)
    if "classe" in d.columns:
        d["classe"] = d["classe"].map(_norm_classe)
        d = d[d["classe"] == cls].copy()

    # default sem alertas
    d["blocked"] = False
    d["penalty_positions"] = 0
    d["alert_severity"] = ""
    d["alert_motives"] = ""

    amap = _build_alerts_map(df_alertas, classe=cls)
    if not amap:
        return d

    def _apply_row(ticker: str):
        a = amap.get(ticker)
        if not a:
            return (False, 0, "", "")
        sev = a.get("severidade", "") or ""
        motivos = a.get("motivos", []) or []
        motives_txt = " | ".join([m for m in motivos if str(m).strip()])[:600]  # corta para não explodir UI/log

        if sev in ("VERMELHO", "CINZA"):
            return (True, 0, sev, motives_txt)

        if sev == "AMARELO":
            k = int(penalty_k) if penalty_k is not None else DEFAULT_PENALTY_K
            if k < 0:
                k = 0
            return (False, k, sev, motives_txt)

        # VERDE
        return (False, 0, sev, motives_txt)

    applied = d["ticker"].apply(_apply_row)
    d["blocked"] = applied.apply(lambda x: bool(x[0]))
    d["penalty_positions"] = applied.apply(lambda x: int(x[1]))
    d["alert_severity"] = applied.apply(lambda x: str(x[2] or ""))
    d["alert_motives"] = applied.apply(lambda x: str(x[3] or ""))

    return d


def filter_eligible(df_with_alerts: pd.DataFrame) -> pd.DataFrame:
    """
    Remove candidatos bloqueados (blocked=True).
    """
    if df_with_alerts is None or df_with_alerts.empty:
        return pd.DataFrame()
    d = df_with_alerts.copy()
    if "blocked" not in d.columns:
        return d
    return d[d["blocked"] == False].copy()


def get_blocked(df_with_alerts: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna apenas bloqueados.
    """
    if df_with_alerts is None or df_with_alerts.empty:
        return pd.DataFrame()
    d = df_with_alerts.copy()
    if "blocked" not in d.columns:
        return pd.DataFrame()
    return d[d["blocked"] == True].copy()
