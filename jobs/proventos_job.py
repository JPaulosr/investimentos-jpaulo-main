# jobs/proventos_job.py
# -*- coding: utf-8 -*-
"""
ROBÔ PROVENTOS — FINAL (idempotente + update + soft delete + auto-fix sheet)

Resolve de vez:
✅ Lê tickers do ativos_master (sem env TICKERS)
✅ Upsert por event_id + version_hash
✅ Atualiza quando muda (não duplica)
✅ Soft delete (ativo=0) e reativa ao reaparecer
✅ Anti-spam Telegram por version_hash
✅ Header contrato (fixo) — não duplica, não insere header aleatório
✅ AUTO-FIX: se a aba estiver com linhas no layout antigo (A-D = hashes), move para K-N
✅ AUTO-CURA: se existir linha legada só com event_id, preenche ticker/tipo_pagamento/data_com no UPDATE

Observação:
- Se você já corrompeu a aba antes, este script corrige o desalinhamento automaticamente.
- Não precisa apagar nada manualmente.
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
# ✅ GARANTE IMPORTS DO REPO (Actions)
# =============================================================================
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.proventos_notify import notify_provento


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

if not SHEET_ID:
    raise RuntimeError("❌ SHEET_ID vazio (env SHEET_ID ou SHEET_ID_NOVO).")
if not GCP_JSON:
    raise RuntimeError("❌ GCP_SERVICE_ACCOUNT_JSON vazio.")

# =============================================================================
# Helpers
# =============================================================================
_HEX40 = re.compile(r"^[a-f0-9]{40}$", re.I)

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
            _norm_date(row.get("data_pagamento", "")),  # ✅ entra no ID
        ]
    )
    return _sha1(key)

def event_version_fingerprint(row: Dict[str, Any]) -> str:
    v = _norm_float(row.get("valor_por_cota", None))
    vtxt = "" if v is None else f"{float(v):.8f}"
    key = "|".join(
        [
            event_id_from_row(row),
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

def _ensure_ws(sh: gspread.Spreadsheet, title: str, rows: int = 8000, cols: int = 30) -> gspread.Worksheet:
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

def _safe_get_records(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    try:
        return ws.get_all_records()
    except Exception:
        return []

def _normalize(cols: List[Any]) -> List[str]:
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

def _assert_or_init_header(ws: gspread.Worksheet) -> List[str]:
    """
    Header fixo:
    - Se vazio: escreve contrato
    - Se existe e já bate: ok
    - Se existe diferente: reescreve linha 1 com contrato (sem inserir nova linha)
      (isso evita header duplicado)
    """
    vals = ws.get_all_values()
    if not vals:
        ws.update("1:1", [HEADER_CONTRATO])
        return HEADER_CONTRATO

    cur = _normalize(vals[0])
    exp = _normalize(HEADER_CONTRATO)

    if cur != exp:
        ws.update("1:1", [HEADER_CONTRATO])
    return HEADER_CONTRATO

def _looks_like_legacy_row(a: str, b: str, c: str, d: str) -> bool:
    # padrão que você mostrou: A=event_id(hex40), B=0/1, C=timestamp, D=version_hash(hex40)
    if not _HEX40.match(a or ""):
        return False
    if str(b).strip() not in ("0", "1"):
        return False
    # timestamp básico (não precisa ser perfeito)
    if not str(c).strip():
        return False
    if not _HEX40.match(d or ""):
        return False
    return True

def _fix_misaligned_legacy_rows(ws: gspread.Worksheet) -> None:
    """
    Se as linhas estão no layout antigo ocupando A-D, move A-D para K-N e limpa A-J.
    Isso corrige exatamente o print que você mandou (ticker virou hash).
    """
    vals = ws.get_all_values()
    if len(vals) < 2:
        return

    # header já foi forçado para contrato. Agora checa a primeira linha de dados.
    r2 = vals[1] + [""] * (14 - len(vals[1]))
    a, b, c, d = (str(r2[0]).strip(), str(r2[1]).strip(), str(r2[2]).strip(), str(r2[3]).strip())

    # Só aplica se realmente parecer legado e se o event_id (col K) estiver vazio
    # (evita mexer em base já correta)
    colK = r2[10] if len(r2) > 10 else ""
    if not _looks_like_legacy_row(a, b, c, d):
        return
    if str(colK).strip():
        return

    # Determina o intervalo de linhas com esse padrão (até a última linha com A preenchido)
    last_row = len(vals)
    # monta updates em batch (mais rápido e confiável em Actions)
    batch_updates = []

    for ridx in range(2, last_row + 1):
        row = (vals[ridx - 1] + [""] * 14)[:14]
        a, b, c, d = (str(row[0]).strip(), str(row[1]).strip(), str(row[2]).strip(), str(row[3]).strip())
        if not a:
            continue
        if not _looks_like_legacy_row(a, b, c, d):
            # se misturou formatos, não tenta “metade”
            continue

        # K=event_id, L=ativo, M=atualizado_em, N=version_hash
        klnm = [a, b, c, d]
        batch_updates.append(
            {
                "range": f"K{ridx}:N{ridx}",
                "values": [klnm],
            }
        )
        # limpa A-J
        batch_updates.append(
            {
                "range": f"A{ridx}:J{ridx}",
                "values": [[""] * 10],
            }
        )

    if batch_updates:
        ws.batch_update(batch_updates)

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
# FETCH — lê tickers do ativos_master
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
            rows_ev = fetch_provento_anunciado(t, logs=None)
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
# Engine
# =============================================================================
def run() -> None:
    print("🚀 Robô Proventos — schema fixo + auto-fix + batch write")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)

    ws_anun = _ensure_ws(sh, ABA_ANUNCIADOS, rows=8000, cols=30)
    ws_logs = _ensure_ws(sh, ABA_LOGS, rows=8000, cols=10)

    # 1) força header correto (sem inserir linha)
    header = _assert_or_init_header(ws_anun)
    hmap = _col_idx_map(header)

    # 2) corrige base “desalinhada” (teu print)
    _fix_misaligned_legacy_rows(ws_anun)

    # 3) garante header do logs
    if not ws_logs.get_all_values():
        ws_logs.update("1:1", [["ts", "event_hash", "ticker", "tipo", "status"]])

    # 4) carrega base anunciados
    all_vals = ws_anun.get_all_values()

    # ================================
    # MAPEAR ESTADO ATUAL DA PLANILHA
    # ================================
    existing_by_event_id: Dict[str, int] = {}
    existing_version_hash: Dict[str, str] = {}
    existing_ativo: Dict[str, str] = {}

    idx_event_id = hmap["event_id"]
    idx_version = hmap["version_hash"]
    idx_ativo = hmap["ativo"]

    for ridx in range(2, len(all_vals) + 1):
        row = all_vals[ridx - 1]

        eid = ""
        if idx_event_id - 1 < len(row):
            eid = str(row[idx_event_id - 1]).strip()

        if not eid:
            continue

        existing_by_event_id[eid] = ridx
        existing_version_hash[eid] = str(row[idx_version - 1]).strip() if idx_version - 1 < len(row) else ""
        existing_ativo[eid] = str(row[idx_ativo - 1]).strip() if idx_ativo - 1 < len(row) else ""

    # ================================
    # ANTI-SPAM (hashes já enviados)
    # ================================
    logs_records = _safe_get_records(ws_logs)
    hashes_enviados = {str(r.get("event_hash") or "").strip() for r in logs_records if r.get("event_hash")}
    print(f"🧱 Anti-spam: {len(hashes_enviados)} hashes no alerts_log")

    # ================================
    # FETCH REAL
    # ================================
    eventos = fetch_events_from_master(sh)
    if not eventos:
        print("ℹ️ Nenhum evento retornado pelo fetch. Nada a fazer.")
        return

    # ================================
    # CONTADORES + BUFFERS
    # ================================
    inserted = 0
    updated = 0
    reactivated = 0
    telegram_sent = 0

    append_rows: List[List[Any]] = []
    log_rows: List[List[Any]] = []
    cell_updates: List[Dict[str, Any]] = []

    # ================================
    # HELPER: montar linha no layout do header
    # ================================
    def make_row_out(row_norm: Dict[str, Any]) -> List[Any]:
        out = [""] * len(header)

        def setc(col: str, val: Any):
            j = hmap.get(col.strip().lower())
            if j:
                out[j - 1] = "" if val is None else val

        setc("ticker", row_norm.get("ticker"))
        setc("tipo_ativo", row_norm.get("tipo_ativo"))
        setc("status", row_norm.get("status"))
        setc("tipo_pagamento", row_norm.get("tipo_pagamento"))
        setc("data_com", row_norm.get("data_com"))
        setc("data_pagamento", row_norm.get("data_pagamento"))
        setc("valor_por_cota", row_norm.get("valor_por_cota"))
        setc("quantidade_ref", row_norm.get("quantidade_ref"))
        setc("fonte_url", row_norm.get("fonte_url"))
        setc("capturado_em", row_norm.get("capturado_em"))

        setc("event_id", row_norm.get("event_id"))
        setc("ativo", row_norm.get("ativo"))
        setc("atualizado_em", row_norm.get("atualizado_em"))
        setc("version_hash", row_norm.get("version_hash"))
        return out

    # ================================
    # LOOP PRINCIPAL DE EVENTOS
    # ================================
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

        # mínimos obrigatórios
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

        # ----------------
        # INSERT
        # ----------------
        if eid not in existing_by_event_id:
            append_rows.append(make_row_out(row_norm))
            inserted += 1
            existing_by_event_id[eid] = -1

            if vhash not in hashes_enviados:
                hashes_enviados.add(vhash)
                log_rows.append([_now_iso_min(), vhash, row_norm["ticker"], "ANUNCIADO", row_norm["status"]])
                ok, metodo, status, err = notify_provento(
                    token=TELEGRAM_TOKEN,
                    chat_id=TELEGRAM_CHAT_ID,
                    ticker=row_norm["ticker"],
                    evento={
                        "tipo_pagamento": row_norm.get("tipo_pagamento"),
                        "data_com": row_norm.get("data_com"),
                        "data_pagamento": row_norm.get("data_pagamento"),
                        "valor_por_cota": row_norm.get("valor_por_cota"),  # mantém full p/ cálculo, exibição vira R$ X,XX
                    },
                    meta={
                        "tipo_ativo": row_norm.get("tipo_ativo") or "",
                        "classificacao": "",        # deixa vazio por enquanto (sem chute)
                        "acao_sugerida": "Aguardar pagamento",
                        # "logo_url": "SUA_URL_DO_LOGO"  # opcional (melhor se você já tiver)
                    },
                    posicao=None,  # Actions: sem impacto por enquanto (evita IO caro)
                )
                if not ok:
                    print(f"⚠️ Telegram falhou (metodo={metodo}, status={status}): {err}")

                telegram_sent += 1
            continue

        # ----------------
        # UPDATE (BATCH)
        # ----------------
        sheet_row = existing_by_event_id[eid]
        prev_vhash = existing_version_hash.get(eid, "")
        prev_ativo = (existing_ativo.get(eid, "") or "").strip()

        # reativar soft delete
        if prev_ativo in ("0", "False", "false", ""):
            cell_updates.append(
                {"range": _cell_a1(hmap["ativo"], sheet_row), "values": [[1]]}
            )
            existing_ativo[eid] = "1"
            reactivated += 1

        # se versão não mudou, não atualiza
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
            cell_updates.append({"range": _cell_a1(cidx, sheet_row), "values": [[val]]})

        existing_version_hash[eid] = vhash
        updated += 1

        if vhash not in hashes_enviados:
            hashes_enviados.add(vhash)
            log_rows.append([_now_iso_min(), vhash, row_norm["ticker"], "UPDATE", row_norm["status"]])
            ok, metodo, status, err = notify_provento(
                token=TELEGRAM_TOKEN,
                chat_id=TELEGRAM_CHAT_ID,
                ticker=row_norm["ticker"],
                evento={
                    "tipo_pagamento": row_norm.get("tipo_pagamento"),
                    "data_com": row_norm.get("data_com"),
                    "data_pagamento": row_norm.get("data_pagamento"),
                    "valor_por_cota": row_norm.get("valor_por_cota"),
                },
                meta={
                    "tipo_ativo": row_norm.get("tipo_ativo") or "",
                    "classificacao": "",
                    "acao_sugerida": "Aguardar pagamento",
                },
                posicao=None,
            )
            if not ok:
                print(f"⚠️ Telegram falhou (metodo={metodo}, status={status}): {err}")

            telegram_sent += 1

    # ================================
    # GRAVAÇÕES (ANTI-429)
    # ================================
    # INSERTS em chunks
    if append_rows:
        CHUNK = 20
        for i in range(0, len(append_rows), CHUNK):
            ws_anun.append_rows(append_rows[i:i + CHUNK], value_input_option="USER_ENTERED")

    # UPDATES em batch (1 request)
    if cell_updates:
        ws_anun.batch_update(cell_updates)

    # LOGS em chunks
    if log_rows:
        CHUNK = 50
        for i in range(0, len(log_rows), CHUNK):
            ws_logs.append_rows(log_rows[i:i + CHUNK], value_input_option="USER_ENTERED")

    print(f"✅ Inseridos: {inserted}")
    print(f"🔁 Atualizados: {updated}")
    print(f"♻️ Reativados: {reactivated}")
    print(f"📨 Telegram enviados: {telegram_sent}")
    print("🏁 Concluído.")


if __name__ == "__main__":
    run()
