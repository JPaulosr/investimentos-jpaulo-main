# jobs/proventos_job.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import json
import re
import time
import hashlib
from datetime import datetime
from typing import Any, Dict, List

import gspread
import requests
from google.oauth2.service_account import Credentials


# =============================================================================
# ENV — TOLERANTE A NOMES DIFERENTES (workflow + streamlit)
# =============================================================================
SHEET_ID = (os.getenv("SHEET_ID") or os.getenv("SHEET_ID_NOVO") or "").strip()

GCP_JSON = (os.getenv("GCP_SERVICE_ACCOUNT_JSON") or "").strip()

TELEGRAM_TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or ""
).strip()

TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

# Abas (contrato)
ABA_ANUNCIADOS = "proventos_anunciados"
ABA_LOGS = "alerts_log"

REQUEST_TIMEOUT = 20


# =============================================================================
# FAIL FAST (infra é contrato)
# =============================================================================
if not SHEET_ID:
    raise RuntimeError("❌ SHEET_ID vazio.")

if not GCP_JSON:
    raise RuntimeError("❌ GCP_SERVICE_ACCOUNT_JSON vazio.")


# =============================================================================
# Sheets helpers
# =============================================================================
def _get_client() -> gspread.Client:
    info = json.loads(GCP_JSON)
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def _ensure_sheet_with_header(sh, title: str, header: List[str]) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=5000, cols=len(header) + 2)

    vals = ws.get_all_values()
    if not vals:
        ws.append_row(header, value_input_option="USER_ENTERED")
        return ws

    cur = [c.strip().lower() for c in vals[0]]
    want = [c.lower() for c in header]

    if cur != want:
        if "event_hash" in want and "event_hash" not in cur:
            ws.insert_row(header, 1)
        else:
            ws.delete_rows(1)
            ws.insert_row(header, 1)

    return ws


def _safe_get_records(ws) -> List[Dict[str, Any]]:
    try:
        return ws.get_all_records()
    except Exception:
        return []


# =============================================================================
# Normalização
# =============================================================================
def _norm_ticker(s: Any) -> str:
    if not s:
        return ""
    s = str(s).upper().strip()
    return re.sub(r"[^A-Z0-9]", "", s)


def _norm_float(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = re.sub(r"[^0-9,.\-]", "", str(v))
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _norm_date(s: Any) -> str:
    if not s:
        return ""
    if hasattr(s, "strftime"):
        return s.strftime("%Y-%m-%d")
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(s), fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return ""


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


# =============================================================================
# Deduplicação determinística
# =============================================================================
def _event_key(d: Dict[str, Any]) -> str:
    return "|".join(
        [
            _norm_ticker(d.get("ticker")),
            str(d.get("tipo_pagamento", "")).upper(),
            _norm_date(d.get("data_com")),
            _norm_date(d.get("data_pagamento")),
            f"{_norm_float(d.get('valor_por_cota')):.8f}",
        ]
    )


def _event_hash(key: str) -> str:
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


# =============================================================================
# Telegram
# =============================================================================
def _send_telegram(msg: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=REQUEST_TIMEOUT,
        )
    except Exception:
        pass


# =============================================================================
# MAIN
# =============================================================================
def run() -> None:
    print("🚀 Robô Proventos — execução idempotente")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)

    ws_anun = _ensure_sheet_with_header(
        sh,
        ABA_ANUNCIADOS,
        [
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
    )

    ws_logs = _ensure_sheet_with_header(
        sh,
        ABA_LOGS,
        ["ts", "event_hash", "ticker", "tipo", "status"],
    )

    existentes = _safe_get_records(ws_anun)
    keys_existentes = {_event_key(r) for r in existentes}

    logs = _safe_get_records(ws_logs)
    hashes_enviados = {str(r.get("event_hash")) for r in logs if r.get("event_hash")}

    # ⚠️ AQUI ENTRA O SEU FETCH REAL
    # Exemplo vazio — plugue seu scraping
    novos_eventos: List[Dict[str, Any]] = []

    rows_append = []
    rows_log = []
    telegram_msgs = []

    for ev in novos_eventos:
        row = {
            "ticker": _norm_ticker(ev.get("ticker")),
            "tipo_ativo": ev.get("tipo_ativo", ""),
            "status": ev.get("status", "ANUNCIADO"),
            "tipo_pagamento": ev.get("tipo_pagamento", ""),
            "data_com": _norm_date(ev.get("data_com")),
            "data_pagamento": _norm_date(ev.get("data_pagamento")),
            "valor_por_cota": _norm_float(ev.get("valor_por_cota")),
            "quantidade_ref": ev.get("quantidade_ref", ""),
            "fonte_url": ev.get("fonte_url", ""),
            "capturado_em": ev.get("capturado_em", _now()),
        }

        key = _event_key(row)
        if not key or key in keys_existentes:
            continue

        keys_existentes.add(key)
        rows_append.append(list(row.values()))

        h = _event_hash(key)
        if h not in hashes_enviados:
            hashes_enviados.add(h)
            rows_log.append([_now(), h, row["ticker"], "ANUNCIADO", row["status"]])
            telegram_msgs.append(
                f"📌 Provento anunciado\n{row['ticker']} — {row['tipo_pagamento']}\n"
                f"Pagamento: {row['data_pagamento']} | R$ {row['valor_por_cota']:.4f}"
            )

    if rows_append:
        ws_anun.append_rows(rows_append, value_input_option="USER_ENTERED")

    if rows_log:
        ws_logs.append_rows(rows_log, value_input_option="USER_ENTERED")

    for msg in telegram_msgs:
        _send_telegram(msg)
        time.sleep(0.4)

    print(f"✅ Novos gravados: {len(rows_append)}")
    print(f"🧾 Logs gravados: {len(rows_log)}")
    print(f"📨 Telegram enviados: {len(telegram_msgs)}")


if __name__ == "__main__":
    run()
