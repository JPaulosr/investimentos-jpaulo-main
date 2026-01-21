# jobs/proventos_job.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import json
import re
import time
import hashlib
from datetime import datetime, date
from typing import Any, Dict, List, Tuple, Optional

import gspread
import requests
from google.oauth2.service_account import Credentials


# =============================================================================
# Path (para importar utils se quiser no futuro)
# =============================================================================
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# =============================================================================
# ENV / Secrets
# =============================================================================
SHEET_ID = os.getenv("SHEET_ID", "").strip()
GCP_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Abas (contrato)
ABA_ATIVOS = "ativos_master"
ABA_ANUNCIADOS = "proventos_anunciados"
ABA_LOGS = "alerts_log"

# Segurança
MAX_DAYS_ALERT = 120  # evita telegram de coisa muito antiga
REQUEST_TIMEOUT = 20


# =============================================================================
# Sheets helpers
# =============================================================================
def _get_client() -> gspread.Client:
    if not GCP_JSON:
        raise RuntimeError("❌ GCP_SERVICE_ACCOUNT_JSON não definido.")
    info = json.loads(GCP_JSON)
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def _safe_get_records(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    # get_all_records exige header na linha 1
    try:
        return ws.get_all_records()
    except Exception:
        return []


def _ensure_sheet_with_header(
    sh: gspread.Spreadsheet,
    title: str,
    header: List[str],
    rows: int = 2000,
    cols: int = 15,
) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)

    vals = ws.get_all_values()
    if not vals:
        ws.append_row(header, value_input_option="USER_ENTERED")
        return ws

    # Se a primeira linha não tiver os campos esperados, insere header no topo
    cur = [str(x).strip() for x in vals[0]]
    want_lower = [c.lower() for c in header]
    cur_lower = [c.lower() for c in cur]

    # caso clássico: aba começou sem header e a 1ª linha virou "header"
    if cur_lower != want_lower:
        if any(c.lower() == "event_hash" for c in header) and "event_hash" not in cur_lower:
            ws.insert_row(header, 1)
        else:
            # força header correto
            ws.delete_rows(1)
            ws.insert_row(header, 1)

    return ws


# =============================================================================
# Normalização / parsing
# =============================================================================
def _norm_ticker(s: Any) -> str:
    if s is None:
        return ""
    s = str(s).strip().upper()
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s


def _safe_float(val: Any) -> float:
    """Converte R$ 1.234,56 / 1,23 / 1.23 para float (agressivo)"""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)

    s = str(val).strip()
    if not s:
        return 0.0

    # remove tudo que não for dígito, vírgula, ponto, hífen
    s = re.sub(r"[^0-9,.\-]", "", s)

    # caso tenha ponto e vírgula: assume ponto milhar e vírgula decimal
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")

    try:
        return float(s)
    except Exception:
        return 0.0


def _norm_date_yyyy_mm_dd(val: Any) -> str:
    if val is None:
        return ""
    if hasattr(val, "strftime"):
        try:
            return val.strftime("%Y-%m-%d")
        except Exception:
            pass

    s = str(val).strip()
    if not s:
        return ""

    # formatos comuns
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue

    # fallback: tenta parse ISO parcial
    try:
        dt = datetime.fromisoformat(s.replace("Z", "").split(".")[0])
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def _now_iso_min() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


# =============================================================================
# Dedup (chave do evento)
# =============================================================================
def _event_key(d: Dict[str, Any]) -> str:
    """
    Chave imutável do anúncio:
    ticker|tipo_pagamento|data_com|data_pagamento|valor_por_cota
    """
    t = _norm_ticker(d.get("ticker", ""))
    tp = str(d.get("tipo_pagamento", "") or "").strip().upper()
    dc = _norm_date_yyyy_mm_dd(d.get("data_com", ""))
    dp = _norm_date_yyyy_mm_dd(d.get("data_pagamento", ""))
    v = _safe_float(d.get("valor_por_cota", 0.0))
    vtxt = f"{float(v):.8f}"
    return f"{t}|{tp}|{dc}|{dp}|{vtxt}"


def _event_hash_from_key(key: str) -> str:
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _build_existing_keys_from_anunciados(df_records: List[Dict[str, Any]]) -> set:
    keys = set()
    for r in df_records:
        d = {
            "ticker": r.get("ticker"),
            "tipo_pagamento": r.get("tipo_pagamento"),
            "data_com": r.get("data_com"),
            "data_pagamento": r.get("data_pagamento"),
            "valor_por_cota": r.get("valor_por_cota"),
        }
        k = _event_key(d)
        if k and not k.startswith("|"):
            keys.add(k)
    return keys


def _build_sent_hashes_from_log(df_logs: List[Dict[str, Any]]) -> set:
    sent = set()
    for r in df_logs:
        h = str(r.get("event_hash") or "").strip()
        if h:
            sent.add(h)
    return sent


# =============================================================================
# Telegram
# =============================================================================
def _send_telegram(msg_html: str) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": msg_html, "parse_mode": "HTML"},
            timeout=REQUEST_TIMEOUT,
        )
    except Exception:
        pass


# =============================================================================
# Fetch (simples): lê os anunciados do próprio Sheets como fonte
# Se você tiver scraping externo, pode plugar aqui e ainda assim a dedup segura.
# =============================================================================
def _load_anunciados_sheet_records(ws_anun: gspread.Worksheet) -> List[Dict[str, Any]]:
    return _safe_get_records(ws_anun)


def _today_yyyy_mm_dd() -> str:
    return date.today().strftime("%Y-%m-%d")


def _days_between(a: str, b: str) -> int:
    try:
        da = datetime.strptime(a, "%Y-%m-%d").date()
        db = datetime.strptime(b, "%Y-%m-%d").date()
        return (db - da).days
    except Exception:
        return 10**9


# =============================================================================
# Main
# =============================================================================
def run() -> None:
    print("🚀 Iniciando Robô (idempotente: sem duplicar planilha/telegram)...")
    if not SHEET_ID:
        raise RuntimeError("❌ SHEET_ID vazio.")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)

    # 1) garante abas com header
    ws_anun = _ensure_sheet_with_header(
        sh,
        ABA_ANUNCIADOS,
        header=[
            "ticker",
            "tipo_ativo",
            "status",
            "tipo_pagamento",
            "data_com",
            "data_pagamento",
            "valor_por_cota",
            "quantidade_ref",
            "fonte_url",
            "capturado_em",
        ],
        rows=5000,
        cols=12,
    )

    ws_logs = _ensure_sheet_with_header(
        sh,
        ABA_LOGS,
        header=["ts", "event_hash", "ticker", "tipo", "status"],
        rows=5000,
        cols=8,
    )

    # 2) carrega estado atual para dedup (planilha é a verdade)
    anunciados_records = _load_anunciados_sheet_records(ws_anun)
    existing_keys = _build_existing_keys_from_anunciados(anunciados_records)

    logs_records = _safe_get_records(ws_logs)
    sent_hashes = _build_sent_hashes_from_log(logs_records)

    # 3) aqui entra sua fonte "nova" (scrape / api / etc).
    # Neste template, vou assumir que você já tem uma lista `novos_eventos`
    # vinda de scraping. Vou colocar como lista vazia e você pluga seu fetch.
    novos_eventos: List[Dict[str, Any]] = []

    # -------------------------------------------------------------------------
    # ✅ PONTO DE PLUG-IN DO SEU FETCH
    #
    # Se você já tinha um bloco que construía eventos (ticker, tipo_pagamento,
    # data_com, data_pagamento, valor_por_cota, etc), você coloca aqui e
    # popula `novos_eventos`.
    #
    # Exemplo de formato de item:
    # {
    #   "ticker": "XPML11",
    #   "tipo_ativo": "FII",
    #   "status": "ANUNCIADO",
    #   "tipo_pagamento": "RENDIMENTO",
    #   "data_com": "2026-01-19",
    #   "data_pagamento": "2026-01-25",
    #   "valor_por_cota": 0.92,
    #   "quantidade_ref": "",
    #   "fonte_url": "https://...",
    #   "capturado_em": "2026-01-20 10:05"
    # }
    # -------------------------------------------------------------------------

    # 4) dedup antes de gravar (idempotência)
    to_append_rows: List[List[Any]] = []
    to_log_rows: List[List[Any]] = []
    telegram_queue: List[Tuple[str, str]] = []  # (msg, ticker)

    today = _today_yyyy_mm_dd()

    for ev in novos_eventos:
        # normaliza campos mínimos
        row = {
            "ticker": _norm_ticker(ev.get("ticker", "")),
            "tipo_ativo": str(ev.get("tipo_ativo", "") or "").strip(),
            "status": str(ev.get("status", "ANUNCIADO") or "ANUNCIADO").strip().upper(),
            "tipo_pagamento": str(ev.get("tipo_pagamento", "") or "").strip().upper(),
            "data_com": _norm_date_yyyy_mm_dd(ev.get("data_com", "")),
            "data_pagamento": _norm_date_yyyy_mm_dd(ev.get("data_pagamento", "")),
            "valor_por_cota": _safe_float(ev.get("valor_por_cota", 0.0)),
            "quantidade_ref": ev.get("quantidade_ref", ""),
            "fonte_url": str(ev.get("fonte_url", "") or "").strip(),
            "capturado_em": str(ev.get("capturado_em", "") or _now_iso_min()),
        }

        if not row["ticker"] or not row["tipo_pagamento"] or not row["data_pagamento"]:
            continue

        key = _event_key(row)
        if key in existing_keys:
            # já está na planilha -> não grava de novo
            continue

        # prepara append na planilha
        to_append_rows.append(
            [
                row["ticker"],
                row["tipo_ativo"],
                row["status"],
                row["tipo_pagamento"],
                row["data_com"],
                row["data_pagamento"],
                row["valor_por_cota"],
                row["quantidade_ref"],
                row["fonte_url"],
                row["capturado_em"],
            ]
        )
        existing_keys.add(key)

        # anti-spam telegram: usa log por event_hash
        ev_hash = _event_hash_from_key(key)
        if ev_hash not in sent_hashes:
            sent_hashes.add(ev_hash)
            to_log_rows.append([_now_iso_min(), ev_hash, row["ticker"], "ANUNCIADO", row["status"]])

            # telegram apenas para pagamentos próximos/hoje (exemplo)
            days_to_pay = _days_between(today, row["data_pagamento"])
            if 0 <= days_to_pay <= MAX_DAYS_ALERT:
                msg = (
                    f"📌 <b>Provento ANUNCIADO</b>\n"
                    f"Ativo: <b>{row['ticker']}</b>\n"
                    f"Tipo: {row['tipo_pagamento']}\n"
                    f"Data com: {row['data_com'] or '-'}\n"
                    f"Pagamento: <b>{row['data_pagamento']}</b>\n"
                    f"Valor/cota: <b>R$ {row['valor_por_cota']:,.4f}</b>\n"
                )
                telegram_queue.append((msg, row["ticker"]))

    # 5) grava em batch (rápido e consistente)
    if to_append_rows:
        # append_rows existe no gspread mais novo; se não tiver, cai no loop
        try:
            ws_anun.append_rows(to_append_rows, value_input_option="USER_ENTERED")
        except Exception:
            for r in to_append_rows:
                ws_anun.append_row(r, value_input_option="USER_ENTERED")

    if to_log_rows:
        try:
            ws_logs.append_rows(to_log_rows, value_input_option="USER_ENTERED")
        except Exception:
            for r in to_log_rows:
                ws_logs.append_row(r, value_input_option="USER_ENTERED")

    # 6) telegram: só manda o que for realmente novo nesta execução
    for msg, _tk in telegram_queue:
        _send_telegram(msg)
        time.sleep(0.4)

    print(f"✅ Novos gravados em {ABA_ANUNCIADOS}: {len(to_append_rows)}")
    print(f"🧾 Novos logs em {ABA_LOGS}: {len(to_log_rows)}")
    print(f"📨 Telegram enviados: {len(telegram_queue)}")


if __name__ == "__main__":
    run()
