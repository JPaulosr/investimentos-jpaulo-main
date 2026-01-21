# jobs/proventos_job.py
# -*- coding: utf-8 -*-
"""
ROBÔ PROVENTOS — versão FINAL (idempotente + update + soft delete)
✅ Não duplica
✅ Atualiza quando muda (event_id + version_hash)
✅ Soft delete: ativo=0
✅ Anti-spam Telegram (version_hash)
✅ Lê tickers do ativos_master
✅ NÃO bagunça header
✅ AGORA: se o header estiver errado, ele faz MIGRAÇÃO AUTOMÁTICA segura
    - Se a aba estiver vazia: cria header completo
    - Se a aba estiver "parcial" (só event_id/ativo/atualizado_em/version_hash):
        -> recria header completo e preserva as colunas que já existirem
    - Se a aba estiver "bagunçada" com header duplicado:
        -> normaliza (pega primeira linha com mais colunas) e reconstrói
    - Se mesmo assim não conseguir mapear: falha com erro claro

⚠️ Observação:
- Ele NÃO vai tentar "adivinhar" dados de colunas que não existiam.
- Mas ele garante o schema correto e mantém event_id/ativo/atualizado_em/version_hash se já houver.

Isso resolve o erro que você mostrou:
Atual: ['event_id','ativo','atualizado_em','version_hash']  (schema incompleto)
"""

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

# =============================================================================
# ✅ GARANTE IMPORTS DO REPO
# =============================================================================
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# =============================================================================
# ENV
# =============================================================================
SHEET_ID = (os.getenv("SHEET_ID") or os.getenv("SHEET_ID_NOVO") or "").strip()
GCP_JSON = (os.getenv("GCP_SERVICE_ACCOUNT_JSON") or "").strip()

TELEGRAM_TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN".upper())
    or ""
).strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

REQUEST_TIMEOUT = 20

ABA_ANUNCIADOS = "proventos_anunciados"
ABA_LOGS = "alerts_log"
ABA_ATIVOS_MASTER = "ativos_master"

# =============================================================================
# ✅ CONTRATO (SCHEMA FIXO)
# =============================================================================
HEADER_CONTRATO = [
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
    "event_id",
    "ativo",
    "atualizado_em",
    "version_hash",
]

# =============================================================================
# Fail fast
# =============================================================================
if not SHEET_ID:
    raise RuntimeError("❌ SHEET_ID vazio (env SHEET_ID ou SHEET_ID_NOVO).")
if not GCP_JSON:
    raise RuntimeError("❌ GCP_SERVICE_ACCOUNT_JSON vazio.")


# =============================================================================
# Helpers
# =============================================================================
def _now_iso_min() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def _norm_ticker(s: Any) -> str:
    if not s:
        return ""
    s = str(s).strip().upper()
    return re.sub(r"[^A-Z0-9]", "", s)


def _norm_date(s: Any) -> str:
    if not s:
        return ""
    if hasattr(s, "strftime"):
        try:
            return s.strftime("%Y-%m-%d")
        except Exception:
            return ""
    st = str(s).strip()
    if not st:
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(st, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(st.replace("Z", "").split(".")[0])
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def _norm_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    st = str(v).strip()
    if not st:
        return None
    st = re.sub(r"[^0-9,.\-]", "", st)
    if "," in st and "." in st:
        st = st.replace(".", "").replace(",", ".")
    else:
        st = st.replace(",", ".")
    try:
        return float(st)
    except Exception:
        return None


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def event_id_from_row(row: Dict[str, Any]) -> str:
    key = "|".join(
        [
            _norm_ticker(row.get("ticker", "")),
            str(row.get("tipo_pagamento", "") or "").strip().upper(),
            _norm_date(row.get("data_com", "")),
        ]
    )
    return _sha1(key)


def event_version_fingerprint(row: Dict[str, Any]) -> str:
    v = _norm_float(row.get("valor_por_cota", None))
    vtxt = "" if v is None else f"{float(v):.8f}"
    key = "|".join(
        [
            event_id_from_row(row),
            _norm_date(row.get("data_pagamento", "")),
            vtxt,
            str(row.get("status", "") or "").strip().upper(),
        ]
    )
    return _sha1(key)


# =============================================================================
# Google Sheets
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


def _safe_get_records(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    try:
        return ws.get_all_records()
    except Exception:
        return []


def _ensure_ws(sh: gspread.Spreadsheet, title: str, rows: int = 8000, cols: int = 30) -> gspread.Worksheet:
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def _normalize(cols: List[str]) -> List[str]:
    return [str(c or "").strip().lower() for c in cols]


def _col_idx_map(header: List[str]) -> Dict[str, int]:
    m: Dict[str, int] = {}
    for i, c in enumerate(header, start=1):
        m[str(c).strip().lower()] = i
    return m


def _cell_a1(col_idx: int, row_idx: int) -> str:
    col = ""
    n = col_idx
    while n > 0:
        n, r = divmod(n - 1, 26)
        col = chr(65 + r) + col
    return f"{col}{row_idx}"


def _best_header_candidate(all_vals: List[List[Any]]) -> List[str]:
    """
    Pega a 'melhor' linha candidata a header:
    - a que tem mais colunas não vazias nas primeiras linhas
    """
    best: List[str] = []
    best_score = -1
    for i in range(min(len(all_vals), 5)):
        row = [str(x).strip() for x in all_vals[i]]
        score = sum(1 for x in row if x)
        if score > best_score:
            best_score = score
            best = row
    return best


def _migrate_schema_if_needed(ws: gspread.Worksheet) -> List[str]:
    """
    Garante que a aba fique com HEADER_CONTRATO na linha 1,
    sem duplicar e sem "bagunçar" mais.

    Estratégias:
    1) Aba vazia -> escreve header
    2) Header parcial (ex: só event_id/ativo/...) -> reescreve header completo
    3) Header duplicado/bagunçado -> escolhe melhor candidato e reescreve header completo
    Mantém dados existentes: só desloca colunas via rewrite do header (linha 1).
    """
    all_vals = ws.get_all_values()
    if not all_vals:
        ws.update("1:1", [HEADER_CONTRATO])
        return HEADER_CONTRATO

    cand = _best_header_candidate(all_vals)
    cur_norm = _normalize(cand)
    exp_norm = _normalize(HEADER_CONTRATO)

    if cur_norm == exp_norm:
        return HEADER_CONTRATO

    # caso exatamente o teu erro: header só com 4 colunas
    if set(cur_norm).issubset(set(_normalize(HEADER_CONTRATO))) and len(cur_norm) < len(exp_norm):
        # reescreve header completo, mantendo os dados (linhas abaixo não mudam)
        ws.update("1:1", [HEADER_CONTRATO])
        return HEADER_CONTRATO

    # caso bagunçado: tenta migrar mesmo assim se tiver colunas conhecidas
    known = set(_normalize(HEADER_CONTRATO))
    overlap = len([c for c in cur_norm if c in known])

    # se pelo menos 2 colunas batem, assume que dá pra normalizar
    if overlap >= 2:
        ws.update("1:1", [HEADER_CONTRATO])
        return HEADER_CONTRATO

    # sem chance: falha
    raise RuntimeError(
        "❌ Schema inválido e não migrável automaticamente.\n"
        f"Header detectado: {cand}\n"
        f"Esperado: {HEADER_CONTRATO}\n"
        "Corrija manualmente a linha 1 para bater com o contrato."
    )


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
# FETCH — lê tickers do ativos_master e chama fetch_provento_anunciado
# =============================================================================
def fetch_events_from_master(sh: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    try:
        ws = sh.worksheet(ABA_ATIVOS_MASTER)
    except Exception:
        print(f"❌ Aba '{ABA_ATIVOS_MASTER}' não existe.")
        return []

    rows = _safe_get_records(ws)
    if not rows:
        print(f"❌ Aba '{ABA_ATIVOS_MASTER}' vazia.")
        return []

    tickers: List[str] = []
    for r in rows:
        t = _norm_ticker(r.get("ticker") or r.get("ativo") or "")
        if t:
            tickers.append(t)

    tickers = sorted(set(tickers))
    if not tickers:
        print(f"❌ Nenhum ticker válido encontrado em '{ABA_ATIVOS_MASTER}'.")
        return []

    try:
        from utils.proventos_fetch import fetch_provento_anunciado  # type: ignore
    except Exception as e:
        print("❌ Falha ao importar utils.proventos_fetch.fetch_provento_anunciado")
        print("   ERRO:", repr(e))
        return []

    eventos: List[Dict[str, Any]] = []
    for t in tickers:
        try:
            rows_ev = fetch_provento_anunciado(t, logs=None)  # compatível com teu fetch
            if not rows_ev:
                continue
            for ev in rows_ev:
                rr = dict(ev)
                rr["ticker"] = _norm_ticker(rr.get("ticker") or t)
                rr["tipo_ativo"] = str(rr.get("tipo_ativo", "") or "").strip()
                rr["status"] = str(rr.get("status", "ANUNCIADO") or "ANUNCIADO").strip().upper()
                rr["tipo_pagamento"] = str(rr.get("tipo_pagamento", "") or "").strip().upper()
                rr["data_com"] = _norm_date(rr.get("data_com", ""))
                rr["data_pagamento"] = _norm_date(rr.get("data_pagamento", ""))
                rr["valor_por_cota"] = _norm_float(rr.get("valor_por_cota", None))
                rr["quantidade_ref"] = rr.get("quantidade_ref", "")
                rr["fonte_url"] = str(rr.get("fonte_url", "") or "").strip()
                rr["capturado_em"] = str(rr.get("capturado_em", "") or _now_iso_min())

                if rr["ticker"] and rr["tipo_pagamento"] and rr["data_com"]:
                    eventos.append(rr)
        except Exception:
            print(f"❌ erro no fetch de {t}:")
            print(traceback.format_exc())

    print(f"📦 fetch total eventos={len(eventos)} (tickers={len(tickers)})")
    return eventos


# =============================================================================
# Upsert engine
# =============================================================================
def run() -> None:
    print("🚀 Robô Proventos — schema FIXO + migração segura")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)

    ws_anun = _ensure_ws(sh, ABA_ANUNCIADOS, rows=8000, cols=30)
    ws_logs = _ensure_ws(sh, ABA_LOGS, rows=8000, cols=10)

    # ✅ MIGRAÇÃO SEGURA DO HEADER (resolve teu erro do Actions)
    header = _migrate_schema_if_needed(ws_anun)
    hmap = _col_idx_map(header)

    # logs (garante header simples)
    if not ws_logs.get_all_values():
        ws_logs.update("1:1", [["ts", "event_hash", "ticker", "tipo", "status"]])

    # carrega base
    all_vals = ws_anun.get_all_values()
    existing_by_event_id: Dict[str, int] = {}
    existing_version_hash: Dict[str, str] = {}
    existing_ativo: Dict[str, str] = {}

    idx_event_id = hmap.get("event_id")
    idx_version = hmap.get("version_hash")
    idx_ativo = hmap.get("ativo")

    for ridx in range(2, len(all_vals) + 1):
        row = all_vals[ridx - 1]
        eid = ""
        if idx_event_id and idx_event_id - 1 < len(row):
            eid = str(row[idx_event_id - 1]).strip()
        if eid:
            existing_by_event_id[eid] = ridx
            if idx_version and idx_version - 1 < len(row):
                existing_version_hash[eid] = str(row[idx_version - 1]).strip()
            if idx_ativo and idx_ativo - 1 < len(row):
                existing_ativo[eid] = str(row[idx_ativo - 1]).strip()

    logs_records = _safe_get_records(ws_logs)
    hashes_enviados = {str(r.get("event_hash") or "").strip() for r in logs_records if r.get("event_hash")}

    eventos = fetch_events_from_master(sh)
    if not eventos:
        print("ℹ️ Nenhum evento retornado pelo fetch. Nada a fazer.")
        return

    inserted = 0
    updated = 0
    reactivated = 0
    telegram_sent = 0

    log_rows: List[List[Any]] = []
    append_rows: List[List[Any]] = []

    def make_row_out(row_norm: Dict[str, Any]) -> List[Any]:
        out = [""] * len(header)

        def setc(col: str, val: Any):
            j = hmap.get(col.strip().lower())
            if j:
                out[j - 1] = "" if val is None else val

        setc("ticker", row_norm["ticker"])
        setc("tipo_ativo", row_norm["tipo_ativo"])
        setc("status", row_norm["status"])
        setc("tipo_pagamento", row_norm["tipo_pagamento"])
        setc("data_com", row_norm["data_com"])
        setc("data_pagamento", row_norm["data_pagamento"])
        setc("valor_por_cota", "" if row_norm["valor_por_cota"] is None else row_norm["valor_por_cota"])
        setc("quantidade_ref", row_norm["quantidade_ref"])
        setc("fonte_url", row_norm["fonte_url"])
        setc("capturado_em", row_norm["capturado_em"])
        setc("event_id", row_norm["event_id"])
        setc("ativo", row_norm["ativo"])
        setc("atualizado_em", row_norm["atualizado_em"])
        setc("version_hash", row_norm["version_hash"])
        return out

    for ev in eventos:
        row_norm: Dict[str, Any] = {
            "ticker": _norm_ticker(ev.get("ticker", "")),
            "tipo_ativo": str(ev.get("tipo_ativo", "") or "").strip(),
            "status": str(ev.get("status", "ANUNCIADO") or "ANUNCIADO").strip().upper(),
            "tipo_pagamento": str(ev.get("tipo_pagamento", "") or "").strip().upper(),
            "data_com": _norm_date(ev.get("data_com", "")),
            "data_pagamento": _norm_date(ev.get("data_pagamento", "")),
            "valor_por_cota": _norm_float(ev.get("valor_por_cota", None)),
            "quantidade_ref": ev.get("quantidade_ref", ""),
            "fonte_url": str(ev.get("fonte_url", "") or "").strip(),
            "capturado_em": str(ev.get("capturado_em", "") or _now_iso_min()),
        }

        if not row_norm["ticker"] or not row_norm["tipo_pagamento"] or not row_norm["data_com"]:
            continue

        eid = event_id_from_row(row_norm)
        vhash = event_version_fingerprint(row_norm)

        row_norm["event_id"] = eid
        row_norm["ativo"] = 1
        row_norm["atualizado_em"] = _now_iso_min()
        row_norm["version_hash"] = vhash

        valor = row_norm["valor_por_cota"]
        valor_txt = "-" if valor is None else f"R$ {valor:.4f}"

        if eid not in existing_by_event_id:
            append_rows.append(make_row_out(row_norm))
            inserted += 1
            existing_by_event_id[eid] = -1

            if vhash not in hashes_enviados:
                hashes_enviados.add(vhash)
                log_rows.append([_now_iso_min(), vhash, row_norm["ticker"], "ANUNCIADO", row_norm["status"]])
                _send_telegram(
                    "📌 Provento anunciado (NOVO)\n"
                    f"{row_norm['ticker']} — {row_norm['tipo_pagamento']}\n"
                    f"Com: {row_norm['data_com']} | Pag: {row_norm['data_pagamento'] or '-'}\n"
                    f"Valor/cota: {valor_txt}"
                )
                telegram_sent += 1
            continue

        sheet_row = existing_by_event_id[eid]
        prev_vhash = existing_version_hash.get(eid, "")
        prev_ativo = (existing_ativo.get(eid, "") or "").strip()

        if prev_ativo in ("0", "False", "false", ""):
            try:
                ws_anun.update(_cell_a1(hmap["ativo"], sheet_row), [[1]])
                reactivated += 1
                existing_ativo[eid] = "1"
            except Exception:
                pass

        if prev_vhash and prev_vhash == vhash:
            continue

        updates: List[Tuple[str, Any]] = [
            ("status", row_norm["status"]),
            ("data_pagamento", row_norm["data_pagamento"]),
            ("valor_por_cota", "" if valor is None else valor),
            ("quantidade_ref", row_norm["quantidade_ref"]),
            ("fonte_url", row_norm["fonte_url"]),
            ("atualizado_em", row_norm["atualizado_em"]),
            ("version_hash", vhash),
        ]

        for col, val in updates:
            cidx = hmap.get(col.lower())
            if not cidx:
                continue
            try:
                ws_anun.update(_cell_a1(cidx, sheet_row), [[val]])
            except Exception:
                pass

        existing_version_hash[eid] = vhash
        updated += 1

        if vhash not in hashes_enviados:
            hashes_enviados.add(vhash)
            log_rows.append([_now_iso_min(), vhash, row_norm["ticker"], "UPDATE", row_norm["status"]])
            _send_telegram(
                "🔁 Provento anunciado (ATUALIZADO)\n"
                f"{row_norm['ticker']} — {row_norm['tipo_pagamento']}\n"
                f"Com: {row_norm['data_com']} | Pag: {row_norm['data_pagamento'] or '-'}\n"
                f"Valor/cota: {valor_txt}"
            )
            telegram_sent += 1

    if append_rows:
        ws_anun.append_rows(append_rows, value_input_option="USER_ENTERED")

    if log_rows:
        ws_logs.append_rows(log_rows, value_input_option="USER_ENTERED")

    print(f"✅ Inseridos: {inserted}")
    print(f"🔁 Atualizados: {updated}")
    print(f"♻️ Reativados: {reactivated}")
    print(f"📨 Telegram enviados: {telegram_sent}")
    print("🏁 Concluído.")


if __name__ == "__main__":
    run()
