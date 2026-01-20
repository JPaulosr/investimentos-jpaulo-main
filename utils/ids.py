# utils/ids.py
# -*- coding: utf-8 -*-

from datetime import datetime
import secrets

def make_id(ticker: str, tipo: str, when: datetime | None = None) -> str:
    """
    Gera ID único e legível para registros (movimentações/proventos).
    Formato: YYYYMMDD_HHMMSS_micro_TICKER_TIPO_rand
    Ex: 20260117_181923_457812_HSLG11_COMPRA_A3F7
    """
    dt = when or datetime.now()
    ticker = (ticker or "").strip().upper()
    tipo = (tipo or "").strip().upper().replace(" ", "_")

    ts = dt.strftime("%Y%m%d_%H%M%S_%f")  # inclui microssegundos
    rand = secrets.token_hex(2).upper()   # 4 chars hex (ex: A3F7)

    return f"{ts}_{ticker}_{tipo}_{rand}"
