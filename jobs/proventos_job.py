# jobs/proventos_job.py
# -*- coding: utf-8 -*-
"""
ROBÔ PROVENTOS — versão robusta (idempotente + update + soft delete)

Garante:
✅ Rodar 2x = não duplica
✅ Se o provento "mudar" (valor/data_pagamento/status) = UPDATE da linha, não INSERT
✅ Soft delete: coluna `ativo` = 0 (não apague linhas)
✅ Reativação automática se o evento reaparecer
✅ Anti-spam Telegram por version_hash
✅ Tolerante a envs: SHEET_ID / SHEET_ID_NOVO, TELEGRAM_TOKEN / TELEGRAM_BOT_TOKEN
✅ Header garantido + colunas novas adicionadas sem quebrar aba antiga

CRÍTICO (fix do "não salvou nada"):
✅ O JOB NÃO depende mais de env TICKERS.
✅ Ele lê os tickers direto da aba "ativos_master" (fonte de verdade).

Requisitos:
- A Service Account precisa ter acesso à planilha.
- A aba "ativos_master" deve existir e conter uma coluna "ticker" (ou "ativo") com os códigos.
- O fetcher está em utils/proventos_fetch.py (função fetch_provento_anunciado).
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
# ✅ GARANTE IMPORTS DO REPO (resolve "No module named utils" em Actions)
# =============================================================================
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# =============================================================================
# ENV — tolerante aos nomes do seu workflow
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
ABA_ATIVOS_MASTER = "ativos_master"  # ✅ fonte única de tickers

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
    """
    ID estável do evento (não muda quando data_pagamento/valor mudam).
    Regra: ticker + tipo_pagamento + data_com.
    """
    key = "|".join(
        [
            _norm_ticker(row.get("ticker", "")),
            str(row.get("tipo_pagamento", "") or "").strip().upper(),
            _norm_date(row.get("data_com", "")),
        ]
    )
    return _sha1(key)


def event_version_fingerprint(row: Dict[str, Any]) -> str:
    """
    Fingerprint da "versão" do evento (muda quando data_pagamento/valor/status mudam).
    """
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


def _ensure_sheet_with_header(
    sh: gspread.Spreadsheet,
    title: str,
    header: List[str],
    rows: int = 5000,
    cols: int = 20,
) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)

    vals = ws.get_all_values()
    if not vals:
        ws.append_row(header, value_input_option="USER_ENTERED")
        return ws

    cur = [str(c).strip().lower() for c in vals[0]]
    want = [str(c).strip().lower() for c in header]

    if cur != want:
        ws.insert_row(header, 1)
    return ws


def _get_header(ws: gspread.Worksheet) -> List[str]:
    vals = ws.get_all_values()
    if not vals:
        return []
    return [str(c).strip() for c in vals[0]]


def _ensure_columns(ws: gspread.Worksheet, required_cols: List[str]) -> List[str]:
    header = _get_header(ws)
    if not header:
        ws.append_row(required_cols, value_input_option="USER_ENTERED")
        return required_cols

    header_set = {c.strip().lower() for c in header}
    to_add = [c for c in required_cols if c.strip().lower() not in header_set]
    if to_add:
        new_header = header + to_add
        ws.update("1:1", [new_header])
        return new_header
    return header


def _col_idx_map(header: List[str]) -> Dict[str, int]:
    m: Dict[str, int] = {}
    for i, c in enumerate(header, start=1):
        m[c.strip().lower()] = i
    return m


def _cell_a1(col_idx: int, row_idx: int) -> str:
    col = ""
    n = col_idx
    while n > 0:
        n, r = divmod(n - 1, 26)
        col = chr(65 + r) + col
    return f"{col}{row_idx}"


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
        print(f"❌ Aba '{ABA_ATIVOS_MASTER}' vazia (sem registros).")
        return []

    # tenta ticker; se não tiver, tenta "ativo"
    tickers: List[str] = []
    for r in rows:
        t = _norm_ticker(r.get("ticker") or r.get("ativo") or "")
        if t:
            tickers.append(t)

    tickers = sorted(set(tickers))
    if not tickers:
        print(f"❌ Nenhum ticker válido encontrado em '{ABA_ATIVOS_MASTER}'")
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
            rows_ev = fetch_provento_anunciado(t, logs=None)  # compatível com seu arquivo
            if rows_ev:
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

                    # mínimos obrigatórios para identidade
                    if rr["ticker"] and rr["tipo_pagamento"] and rr["data_com"]:
                        eventos.append(rr)
                    else:
                        print(
                            f"❌ descartado (faltou mínimo) {t} | "
                            f"ticker={rr.get('ticker')} tipo_pagamento={rr.get('tipo_pagamento')} data_com={rr.get('data_com')}"
                        )

        except Exception:
            print(f"❌ erro no fetch de {t}:")
            print(traceback.format_exc())

    print(f"📦 fetch total eventos={len(eventos)} (tickers={len(tickers)})")
    return eventos


# =============================================================================
# Upsert engine
# =============================================================================
def run() -> None:
    print("🚀 Robô Proventos — idempotente + update + soft delete")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)

    # 1) garante abas e headers base
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
        rows=8000,
        cols=20,
    )

    ws_logs = _ensure_sheet_with_header(
        sh,
        ABA_LOGS,
        header=["ts", "event_hash", "ticker", "tipo", "status"],
        rows=8000,
        cols=10,
    )

    # 2) adiciona colunas novas
    header = _ensure_columns(ws_anun, required_cols=["event_id", "ativo", "atualizado_em", "version_hash"])
    hmap = _col_idx_map(header)

    # 3) carrega base existente
    all_vals = ws_anun.get_all_values()
    if not all_vals:
        ws_anun.append_row(header, value_input_option="USER_ENTERED")
        all_vals = ws_anun.get_all_values()

    existing_by_event_id: Dict[str, int] = {}
    existing_version_hash: Dict[str, str] = {}
    existing_ativo: Dict[str, str] = {}

    idx_event_id = hmap.get("event_id", None)
    idx_version = hmap.get("version_hash", None)
    idx_ativo = hmap.get("ativo", None)

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

    # logs para anti-spam
    logs_records = _safe_get_records(ws_logs)
    hashes_enviados = {str(r.get("event_hash") or "").strip() for r in logs_records if r.get("event_hash")}

    # 4) fetch (agora vem do ativos_master)
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

    def _set_by_header(out: List[Any], col: str, val: Any) -> None:
        j = hmap.get(col.strip().lower())
        if j:
            out[j - 1] = "" if val is None else val

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

        # INSERT
        if eid not in existing_by_event_id:
            out = [""] * len(header)

            _set_by_header(out, "ticker", row_norm["ticker"])
            _set_by_header(out, "tipo_ativo", row_norm["tipo_ativo"])
            _set_by_header(out, "status", row_norm["status"])
            _set_by_header(out, "tipo_pagamento", row_norm["tipo_pagamento"])
            _set_by_header(out, "data_com", row_norm["data_com"])
            _set_by_header(out, "data_pagamento", row_norm["data_pagamento"])
            _set_by_header(out, "valor_por_cota", "" if valor is None else valor)
            _set_by_header(out, "quantidade_ref", row_norm["quantidade_ref"])
            _set_by_header(out, "fonte_url", row_norm["fonte_url"])
            _set_by_header(out, "capturado_em", row_norm["capturado_em"])

            _set_by_header(out, "event_id", eid)
            _set_by_header(out, "ativo", 1)
            _set_by_header(out, "atualizado_em", row_norm["atualizado_em"])
            _set_by_header(out, "version_hash", vhash)

            append_rows.append(out)
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

        # UPDATE
        sheet_row = existing_by_event_id[eid]
        prev_vhash = existing_version_hash.get(eid, "")
        prev_ativo = (existing_ativo.get(eid, "") or "").strip()

        # reativar se estava soft delete
        if prev_ativo in ("0", "False", "false", ""):
            try:
                ws_anun.update(_cell_a1(hmap["ativo"], sheet_row), [[1]])
                reactivated += 1
                existing_ativo[eid] = "1"
            except Exception:
                pass

        # versão igual -> nada
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
            a1 = _cell_a1(cidx, sheet_row)
            try:
                ws_anun.update(a1, [[val]])
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

    # 6) batch inserts
    if append_rows:
        try:
            ws_anun.append_rows(append_rows, value_input_option="USER_ENTERED")
        except Exception:
            for r in append_rows:
                ws_anun.append_row(r, value_input_option="USER_ENTERED")

    # 7) logs
    if log_rows:
        try:
            ws_logs.append_rows(log_rows, value_input_option="USER_ENTERED")
        except Exception:
            for r in log_rows:
                ws_logs.append_row(r, value_input_option="USER_ENTERED")

    print(f"✅ Inseridos: {inserted}")
    print(f"🔁 Atualizados: {updated}")
    print(f"♻️ Reativados: {reactivated}")
    print(f"📨 Telegram enviados: {telegram_sent}")
    print("🏁 Concluído.")


if __name__ == "__main__":
    run()
