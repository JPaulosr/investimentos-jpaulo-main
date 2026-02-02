# utils/posicao.py
# -*- coding: utf-8 -*-

from __future__ import annotations
import re
from typing import Any, Dict, Optional

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore


def _norm_ticker(s: Any) -> str:
    if not s:
        return ""
    s = str(s).strip().upper()
    return re.sub(r"[^A-Z0-9]", "", s)


def _to_float(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    s = re.sub(r"[^0-9,.\-]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _is_venda(tipo: Any) -> bool:
    t = str(tipo or "").strip().upper()
    return t in {"VENDA", "V", "SELL", "S"}


def get_posicao_from_movimentacoes(
    df_mov,
    ticker: str,
    *,
    col_ticker: Optional[str] = None,
    col_qtd: Optional[str] = None,
    col_tipo: Optional[str] = None,
) -> float:
    """
    Retorna a posição atual (quantidade) do ticker somando movimentações.
    - Se tiver coluna de tipo/operação e for venda => subtrai.
    - Se não tiver tipo, assume tudo como compra/entrada.
    """
    if df_mov is None:
        return 0.0
    if pd is not None:
        try:
            if isinstance(df_mov, pd.DataFrame) and df_mov.empty:
                return 0.0
        except Exception:
            pass

    # Mapeia colunas por heurística se não vierem explicitamente
    cols = {}
    try:
        cols = {str(c).lower().strip(): c for c in df_mov.columns}  # type: ignore
    except Exception:
        return 0.0

    c_tk = col_ticker or cols.get("ticker") or cols.get("ativo") or cols.get("codigo")
    c_qt = col_qtd or cols.get("quantidade") or cols.get("qtd") or cols.get("cotas")
    c_tp = col_tipo or cols.get("tipo") or cols.get("operacao") or cols.get("tipo_operacao")

    if not c_tk or not c_qt:
        return 0.0

    tk = _norm_ticker(ticker)
    if not tk:
        return 0.0

    total = 0.0

    # Itera linhas (robusto, não depende de dtype)
    try:
        it = df_mov.to_dict(orient="records")  # type: ignore
    except Exception:
        return 0.0

    for r in it:
        rt = _norm_ticker(r.get(c_tk))
        if rt != tk:
            continue

        qtd = _to_float(r.get(c_qt))
        if c_tp and _is_venda(r.get(c_tp)):
            qtd *= -1.0

        total += qtd

    # Nunca retorna negativo
    return max(0.0, float(total))


def calc_credito_estimado(qtd: Any, valor_por_cota: Any) -> float:
    return max(0.0, _to_float(qtd) * _to_float(valor_por_cota))
