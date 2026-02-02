# jobs/proventos_job.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import json
import re
import hashlib
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials
from pathlib import Path

# =============================================================================
# PATH
# =============================================================================
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.proventos_notify import notify_provento

# =============================================================================
# ENV
# =============================================================================
SHEET_ID = (os.getenv("SHEET_ID") or os.getenv("SHEET_ID_NOVO") or "").strip()
GCP_JSON = (os.getenv("GCP_SERVICE_ACCOUNT_JSON") or "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN") or ""
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or ""

ABA_ANUNCIADOS = "proventos_anunciados"
ABA_LOGS = "alerts_log"
ABA_ATIVOS_MASTER = "ativos_master"
ABA_MOVIMENTACOES = os.getenv("ABA_MOVIMENTACOES_NOVO") or "movimentacoes"

HEADER = [
    "ticker","tipo_ativo","status","tipo_pagamento","data_com","data_pagamento",
    "valor_por_cota","quantidade_ref","fonte_url","capturado_em",
    "event_id","ativo","atualizado_em","version_hash"
]

# =============================================================================
# HELPERS
# =============================================================================
def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")

def sha1(txt: str) -> str:
    return hashlib.sha1(txt.encode()).hexdigest()

def norm_ticker(v): return re.sub(r"[^A-Z0-9]", "", str(v or "").upper())

def norm_date(v):
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    for f in ("%Y-%m-%d", "%d/%m/%Y"):
        try: return datetime.strptime(str(v), f).strftime("%Y-%m-%d")
        except: pass
    return ""

def norm_float(v):
    try: return float(str(v).replace(",", "."))
    except: return None

def event_id(row):
    return sha1("|".join([
        row["ticker"], row["tipo_pagamento"],
        row["data_com"], row.get("data_pagamento","")
    ]))

def version_hash(row):
    return sha1(f"{event_id(row)}|{row.get('valor_por_cota')}|{row['status']}")

# =============================================================================
# GOOGLE
# =============================================================================
def get_client():
    info = json.loads(GCP_JSON)
    info["private_key"] = info["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    return gspread.authorize(creds)

# =============================================================================
# META
# =============================================================================
def build_meta_map(sh):
    ws = sh.worksheet(ABA_ATIVOS_MASTER)
    rows = ws.get_all_records()
    meta = {}
    for r in rows:
        tk = norm_ticker(r.get("ticker") or r.get("ativo"))
        if tk:
            meta[tk] = {
                "tipo_ativo": r.get("tipo_ativo",""),
                "classificacao": r.get("classificacao_capital",""),
                "logo_url": r.get("logo_url",""),
                "acao_sugerida": "Aguardar pagamento"
            }
    return meta

def build_pos_map(sh):
    ws = sh.worksheet(ABA_MOVIMENTACOES)
    rows = ws.get_all_records()
    pos = {}
    for r in rows:
        tk = norm_ticker(r.get("ticker") or r.get("ativo"))
        if not tk: continue
        qtd = norm_float(r.get("quantidade") or 0) or 0
        if str(r.get("tipo_operacao","")).upper() == "VENDA":
            qtd *= -1
        pos[tk] = pos.get(tk, 0) + qtd
    return {k:max(0,v) for k,v in pos.items()}

# =============================================================================
# MAIN
# =============================================================================
def run():
    print("🚀 Robô Proventos iniciado")

    gc = get_client()
    sh = gc.open_by_key(SHEET_ID)

    ws = sh.worksheet(ABA_ANUNCIADOS)
    logs = sh.worksheet(ABA_LOGS)

    meta_map = build_meta_map(sh)
    pos_map = build_pos_map(sh)

    enviados = {r["event_hash"] for r in logs.get_all_records() if r.get("event_hash")}

    from utils.proventos_fetch import fetch_provento_anunciado

    for tk in meta_map.keys():
        eventos = fetch_provento_anunciado(tk) or []
        for ev in eventos:
            row = {
                "ticker": norm_ticker(tk),
                "tipo_ativo": meta_map[tk]["tipo_ativo"],
                "status": ev.get("status","ANUNCIADO"),
                "tipo_pagamento": ev.get("tipo_pagamento"),
                "data_com": norm_date(ev.get("data_com")),
                "data_pagamento": norm_date(ev.get("data_pagamento")),
                "valor_por_cota": norm_float(ev.get("valor_por_cota")),
                "quantidade_ref": pos_map.get(tk,0),
                "fonte_url": ev.get("fonte_url",""),
                "capturado_em": now(),
                "ativo": 1
            }

            row["event_id"] = event_id(row)
            row["version_hash"] = version_hash(row)
            row["atualizado_em"] = now()

            if row["version_hash"] in enviados:
                continue

            notify_provento(
                token=TELEGRAM_TOKEN,
                chat_id=TELEGRAM_CHAT_ID,
                ticker=tk,
                evento=row,
                meta=meta_map[tk],
                posicao=pos_map.get(tk,0)
            )

            logs.append_row([now(), row["version_hash"], tk, "ANUNCIADO", row["status"]])

    print("🏁 Finalizado")

if __name__ == "__main__":
    run()
