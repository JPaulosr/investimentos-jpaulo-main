# jobs/proventos_job.py
# -*- coding: utf-8 -*-
"""
ROBÔ PROVENTOS — FINAL (idempotente + update + soft delete + auto-fix sheet)

Resolve de vez:
✅ Lê tickers do ativos_master (sem env TICKERS)
✅ Upsert por event_id + version_hash
✅ Atualiza quando muda (não duplica)
✅ Soft delete (ativo=0) e reativa ao reaparecer
✅ Header contrato (fixo) — não duplica, não insere header aleatório
✅ AUTO-FIX: se a aba estiver com linhas no layout antigo (A-D = hashes), move para K-N
✅ AUTO-CURA: se existir linha legada só com event_id, preenche ticker/tipo_pagamento/data_com no UPDATE

Telegram:
✅ INSERT = catch-up: SE ENTROU AGORA NA PLANILHA (event_id novo), manda Telegram SEM depender do alerts_log
✅ UPDATE = anti-spam por version_hash (alerts_log)

+ Resumo no final do lote (Telegram):
✅ Se 2+ ativos realmente notificados no run, manda 1 mensagem com total estimado e detalhe por ativo (top 15).
"""

from __future__ import annotations

import os
import sys
import json
import re
import hashlib
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo
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

ABA_MOVIMENTACOES = os.getenv("ABA_MOVIMENTACOES_NOVO") or os.getenv("ABA_LANCAMENTOS") or "movimentacoes"

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

TZ_SP = ZoneInfo("America/Sao_Paulo")

def _now_sp() -> datetime:
    return datetime.now(tz=TZ_SP)

def _today_sp_iso() -> str:
    return _now_sp().strftime("%Y-%m-%d")

def _now_iso_min() -> str:
    return _now_sp().strftime("%Y-%m-%d %H:%M")

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

def _fmt_money_br(v: float) -> str:
    s = f"{v:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def _fmt_date_br(iso_yyyy_mm_dd: str) -> str:
    """Converte YYYY-MM-DD (ou DD/MM/YYYY) para DD/MM/YYYY. Retorna original se inválido."""
    if not iso_yyyy_mm_dd:
        return ""
    s = str(iso_yyyy_mm_dd).strip()
    d = _norm_date(s)
    if not d:
        return s
    try:
        return datetime.strptime(d, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        return s

def _build_month_context(all_vals: list, hmap: dict, pos_map: dict, hoje_iso: str) -> str:
    """Retorna 1 linha com total estimado do mês corrente (pagamentos no mesmo MM/AAAA de hoje)."""
    try:
        ym = (hoje_iso or "")[:7]  # YYYY-MM
        if not ym:
            return ""
        idx_status = hmap.get("status")
        idx_dp = hmap.get("data_pagamento")
        idx_tk = hmap.get("ticker")
        idx_vpc = hmap.get("valor_por_cota")
        idx_ativo = hmap.get("ativo")
        if not (idx_status and idx_dp and idx_tk and idx_vpc and idx_ativo):
            return ""
        total = 0.0
        eventos = 0
        for ridx in range(2, len(all_vals) + 1):
            row = all_vals[ridx - 1]
            status = str(row[idx_status - 1]).strip().upper() if idx_status - 1 < len(row) else ""
            if status != "ANUNCIADO":
                continue
            ativo = str(row[idx_ativo - 1]).strip() if idx_ativo - 1 < len(row) else ""
            if ativo in ("0", "False", "false"):
                continue
            dp = _norm_date(str(row[idx_dp - 1]).strip() if idx_dp - 1 < len(row) else "")
            if not dp or not dp.startswith(ym):
                continue
            tk = _norm_ticker(row[idx_tk - 1]) if idx_tk - 1 < len(row) else ""
            if not tk:
                continue
            qtd = float(pos_map.get(tk, 0.0) or 0.0)
            if qtd <= 0:
                continue
            vpc = _norm_float(row[idx_vpc - 1] if idx_vpc - 1 < len(row) else None)
            if vpc is None or vpc <= 0:
                continue
            total += float(qtd) * float(vpc)
            eventos += 1
        if eventos <= 0 or total <= 0:
            return ""
        mm_aaaa = datetime.strptime(ym + "-01", "%Y-%m-%d").strftime("%m/%Y")
        return f"📦 Mês ({mm_aaaa}): R$ {_fmt_money_br(total)} estimado | {eventos} eventos"
    except Exception:
        return ""

def _fmt_ddmm(iso_yyyy_mm_dd: str) -> str:
    """Converte YYYY-MM-DD (ou DD/MM/YYYY) para DD/MM."""
    if not iso_yyyy_mm_dd:
        return ""
    s = str(iso_yyyy_mm_dd).strip()
    d = _norm_date(s)
    if not d:
        return ""
    try:
        return datetime.strptime(d, "%Y-%m-%d").strftime("%d/%m")
    except Exception:
        return ""

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
    if not _HEX40.match(a or ""):
        return False
    if str(b).strip() not in ("0", "1"):
        return False
    if not str(c).strip():
        return False
    if not _HEX40.match(d or ""):
        return False
    return True

def _fix_misaligned_legacy_rows(ws: gspread.Worksheet) -> None:
    """
    Se as linhas estão no layout antigo ocupando A-D, move A-D para K-N e limpa A-J.
    """
    vals = ws.get_all_values()
    if len(vals) < 2:
        return

    r2 = vals[1] + [""] * (14 - len(vals[1]))
    a, b, c, d = (str(r2[0]).strip(), str(r2[1]).strip(), str(r2[2]).strip(), str(r2[3]).strip())

    colK = r2[10] if len(r2) > 10 else ""
    if not _looks_like_legacy_row(a, b, c, d):
        return
    if str(colK).strip():
        return

    last_row = len(vals)
    batch_updates = []

    for ridx in range(2, last_row + 1):
        row = (vals[ridx - 1] + [""] * 14)[:14]
        a, b, c, d = (str(row[0]).strip(), str(row[1]).strip(), str(row[2]).strip(), str(row[3]).strip())
        if not a:
            continue
        if not _looks_like_legacy_row(a, b, c, d):
            continue

        klnm = [a, b, c, d]  # K=event_id, L=ativo, M=atualizado_em, N=version_hash
        batch_updates.append({"range": f"K{ridx}:N{ridx}", "values": [klnm]})
        batch_updates.append({"range": f"A{ridx}:J{ridx}", "values": [[""] * 10]})

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
# META (ativos_master) — logo_url / tipo_ativo / classificacao
# =============================================================================
def build_meta_map_from_master(sh: gspread.Spreadsheet) -> Dict[str, Dict[str, Any]]:
    try:
        ws = sh.worksheet(ABA_ATIVOS_MASTER)
    except Exception:
        return {}

    rows = _safe_get_records(ws)
    meta: Dict[str, Dict[str, Any]] = {}

    def _get(r, *keys):
        for k in keys:
            if k in r and str(r.get(k) or "").strip() != "":
                return r.get(k)
        return ""

    for r in rows:
        tk = _norm_ticker(_get(r, "ticker", "ativo", "codigo"))
        if not tk:
            continue

        logo = str(_get(r, "logo_url", "logo", "url_logo") or "").strip()
        if not (logo.startswith("http://") or logo.startswith("https://")):
            logo = ""

        classe = str(_get(r, "classe", "tipo_ativo") or "").strip()
        subtipo = str(_get(r, "subtipo") or "").strip()
        segmento = str(_get(r, "segmento") or "").strip()

        parts: List[str] = []
        for p in [classe, subtipo, segmento]:
            p2 = p.strip()
            if p2 and p2.lower() not in [x.lower() for x in parts]:
                parts.append(p2)
        tipo_ativo = " / ".join(parts).strip()

        classificacao = str(_get(r, "classificacao_capital", "classificacao") or "").strip()

        meta[tk] = {
            "logo_url": logo or None,
            "tipo_ativo": tipo_ativo or classe or "",
            "classificacao": classificacao or "",
            "acao_sugerida": "Aguardar pagamento",
        }

    return meta

# =============================================================================
# POSIÇÃO (movimentacoes) — quantidade por ticker (1 leitura)
# =============================================================================
def build_pos_map_from_movimentacoes(sh: gspread.Spreadsheet) -> Dict[str, float]:
    try:
        ws = sh.worksheet(ABA_MOVIMENTACOES)
    except Exception:
        return {}

    rows = _safe_get_records(ws)
    if not rows:
        return {}

    def _to_float(v: Any) -> float:
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        st = str(v).strip()
        if not st:
            return 0.0
        st = re.sub(r"[^0-9,.\-]", "", st)
        if "," in st and "." in st:
            st = st.replace(".", "").replace(",", ".")
        else:
            st = st.replace(",", ".")
        try:
            return float(st)
        except Exception:
            return 0.0

    def _is_venda(tipo: Any) -> bool:
        t = str(tipo or "").strip().upper()
        return t in {"VENDA", "V", "SELL", "S"}

    pos: Dict[str, float] = {}

    for r in rows:
        tk = _norm_ticker(r.get("ticker") or r.get("ativo") or r.get("codigo") or r.get("papel") or "")
        if not tk:
            continue

        qtd = _to_float(r.get("quantidade") or r.get("qtd") or r.get("cotas") or 0)
        tipo = r.get("tipo_operacao") or r.get("tipo") or r.get("operacao") or r.get("tipo_de_operacao")

        if _is_venda(tipo):
            qtd *= -1.0

        pos[tk] = pos.get(tk, 0.0) + qtd

    for k in list(pos.keys()):
        pos[k] = max(0.0, float(pos[k]))
    return pos

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
    print(f"🔐 TELEGRAM_TOKEN set? {'SIM' if TELEGRAM_TOKEN else 'NAO'} | CHAT_ID set? {'SIM' if TELEGRAM_CHAT_ID else 'NAO'}")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)

    # ✅ Pré-carrega meta e posição (1 leitura de cada)
    meta_map = build_meta_map_from_master(sh)
    pos_map = build_pos_map_from_movimentacoes(sh)
    print(f"🧩 meta_map={len(meta_map)} | pos_map={len(pos_map)} | aba_mov='{ABA_MOVIMENTACOES}'")

    ws_anun = _ensure_ws(sh, ABA_ANUNCIADOS, rows=8000, cols=30)
    ws_logs = _ensure_ws(sh, ABA_LOGS, rows=8000, cols=10)

    # 1) força header correto (sem inserir linha)
    header = _assert_or_init_header(ws_anun)
    hmap = _col_idx_map(header)

    # 2) corrige base “desalinhada”
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

    # ✅ RESUMO FINAL DO LOTE (apenas do que foi realmente notificado)
    resumo_itens: List[Tuple[str, float, str]] = []
    resumo_total: float = 0.0

    # ✅ ALERTA DIÁRIO: "HOJE TEM PAGAMENTO" (consolidado)
    hoje_iso = _today_sp_iso()
    payday_itens: List[Tuple[str, float]] = []
    payday_total: float = 0.0

    # ================================
    # HELPER: montar linha no layout do header
    # ================================
    def _get_sheet_val(row: List[Any], col: str) -> str:
        cidx = hmap.get(col.strip().lower())
        if not cidx:
            return ""
        j = cidx - 1
        return str(row[j]).strip() if j < len(row) else ""

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

        # meta + posição
        meta = meta_map.get(
            row_norm["ticker"],
            {
                "logo_url": None,
                "tipo_ativo": row_norm.get("tipo_ativo") or "",
                "classificacao": "",
                "acao_sugerida": "Aguardar pagamento",
            },
        )

        qtd = float(pos_map.get(row_norm["ticker"], 0.0) or 0.0)
        posicao = {"qtd": qtd} if qtd > 0 else None

        # ✅ REGRA: se não tem posição (qtd<=0), NÃO notifica e NÃO grava.
        #    Se o evento já existe na planilha, faz soft-delete (ativo=0) para não poluir.
        if qtd <= 0:
            eid_tmp = event_id_from_row(row_norm)
            sheet_row_tmp = existing_by_event_id.get(eid_tmp)
            if sheet_row_tmp and sheet_row_tmp > 1:
                prev_ativo_tmp = (existing_ativo.get(eid_tmp, "") or "").strip()
                if prev_ativo_tmp not in ("0", "False", "false"):
                    cell_updates.append({"range": _cell_a1(hmap["ativo"], sheet_row_tmp), "values": [[0]]})
                    existing_ativo[eid_tmp] = "0"
            continue

        # ----------------
        # INSERT  ✅ catch-up: manda SEM depender do alerts_log
        # ----------------
        if eid not in existing_by_event_id:
            append_rows.append(make_row_out(row_norm))
            inserted += 1
            existing_by_event_id[eid] = -1

            # registra hash (idempotência do log)
            hashes_enviados.add(vhash)
            log_rows.append([_now_iso_min(), vhash, row_norm["ticker"], "ANUNCIADO", row_norm["status"]])

            # ✅ acumula resumo (somente se houver valor e posição)
            vpc = row_norm.get("valor_por_cota")
            if qtd > 0 and isinstance(vpc, (int, float)) and vpc > 0:
                total_est = float(qtd) * float(vpc)
                resumo_itens.append((row_norm["ticker"], total_est, row_norm.get("data_pagamento","")))
                resumo_total += total_est
                # ✅ acumula alerta do dia do pagamento
                if row_norm.get("data_pagamento") == hoje_iso:
                    payday_itens.append((row_norm["ticker"], total_est))
                    payday_total += total_est

            print(f"📨 (INSERT) Enviando Telegram: {row_norm['ticker']} vhash={vhash[:8]}")
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
                meta=meta,
                posicao=posicao,
                logo_url=meta.get("logo_url"),
            )
            print(f"📨 (INSERT) Resultado: ok={ok} metodo={metodo} status={status} err={err}")
            if ok:
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
            cell_updates.append({"range": _cell_a1(hmap["ativo"], sheet_row), "values": [[1]]})
            existing_ativo[eid] = "1"
            reactivated += 1

        # se versão não mudou, não atualiza
        if prev_vhash and prev_vhash == vhash:
            continue

        valor = row_norm["valor_por_cota"]

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

        # UPDATE: anti-spam por version_hash
        if vhash in hashes_enviados:
            print(f"🧱 (UPDATE) Anti-spam bloqueou: {row_norm['ticker']} vhash={vhash[:8]}")
            continue

        hashes_enviados.add(vhash)
        log_rows.append([_now_iso_min(), vhash, row_norm["ticker"], "UPDATE", row_norm["status"]])

        vpc = row_norm.get("valor_por_cota")
        if qtd > 0 and isinstance(vpc, (int, float)) and vpc > 0:
            total_est = float(qtd) * float(vpc)
            resumo_itens.append((row_norm["ticker"], total_est, row_norm.get("data_pagamento","")))
            resumo_total += total_est
            # ✅ acumula alerta do dia do pagamento
            if row_norm.get("data_pagamento") == hoje_iso:
                payday_itens.append((row_norm["ticker"], total_est))
                payday_total += total_est

        print(f"📨 (UPDATE) Enviando Telegram: {row_norm['ticker']} vhash={vhash[:8]}")
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
            meta=meta,
            posicao=posicao,
            logo_url=meta.get("logo_url"),
        )
        print(f"📨 (UPDATE) Resultado: ok={ok} metodo={metodo} status={status} err={err}")
        if ok:
            telegram_sent += 1

    # ✅ Complemento: pega também pagamentos de HOJE já existentes na planilha (ANUNCIADOS, ativo=1)
    #    (evita perder alertas se o fetch não retornar algum item já gravado)
    try:
        idx_status = hmap.get("status")
        idx_dp = hmap.get("data_pagamento")
        idx_tk = hmap.get("ticker")
        idx_vpc = hmap.get("valor_por_cota")
        idx_ativo2 = hmap.get("ativo")
        if idx_status and idx_dp and idx_tk and idx_vpc and idx_ativo2:
            for ridx in range(2, len(all_vals) + 1):
                row = all_vals[ridx - 1]
                status = str(row[idx_status - 1]).strip().upper() if idx_status - 1 < len(row) else ""
                dp_raw = str(row[idx_dp - 1]).strip() if idx_dp - 1 < len(row) else ""
                dp = _norm_date(dp_raw)
                tk = _norm_ticker(row[idx_tk - 1]) if idx_tk - 1 < len(row) else ""
                ativo = str(row[idx_ativo2 - 1]).strip() if idx_ativo2 - 1 < len(row) else ""
                if not tk or ativo in ("0", "False", "false"):
                    continue
                if status != "ANUNCIADO" or dp != hoje_iso:
                    continue
                qtd = float(pos_map.get(tk, 0.0) or 0.0)
                if qtd <= 0:
                    continue
                vpc = _norm_float(row[idx_vpc - 1] if idx_vpc - 1 < len(row) else None)
                if vpc is None or vpc <= 0:
                    continue
                total_est = float(qtd) * float(vpc)
                payday_itens.append((tk, total_est))
                payday_total += total_est
    except Exception:
        pass

    # ================================
    # GRAVAÇÕES (ANTI-429)
    # ================================
    if append_rows:
        CHUNK = 20
        for i in range(0, len(append_rows), CHUNK):
            ws_anun.append_rows(append_rows[i:i + CHUNK], value_input_option="USER_ENTERED")

    if cell_updates:
        ws_anun.batch_update(cell_updates)

    if log_rows:
        CHUNK = 50
        for i in range(0, len(log_rows), CHUNK):
            ws_logs.append_rows(log_rows[i:i + CHUNK], value_input_option="USER_ENTERED")

    # ================================
    # ✅ RESUMO FINAL DO LOTE (1 msg)
    # ================================
    if len(resumo_itens) >= 2 and resumo_total > 0:
        agg: Dict[str, float] = {}
        dp_min: Dict[str, str] = {}
        for tk, v, dp in resumo_itens:
            agg[tk] = agg.get(tk, 0.0) + float(v)
            if dp:
                cur = dp_min.get(tk)
                if (not cur) or (dp < cur):
                    dp_min[tk] = dp

        itens_ord = sorted(
            [(tk, val, dp_min.get(tk, "")) for tk, val in agg.items()],
            key=lambda x: (x[2] or "9999-12-31", -x[1]),
        )
        linhas = [
            (f"• {_fmt_ddmm(dp)} — {tk}: R$ {_fmt_money_br(val)}" if dp else f"• {tk}: R$ {_fmt_money_br(val)}")
            for tk, val, dp in itens_ord[:15]
        ]

        prox = next((dp for _, _, dp in itens_ord if dp), "")
        prox_txt = f"

• Próximo pagamento: {_fmt_ddmm(prox)}" if prox else ""
        msg = (
            "📊 Resumo do lote — Proventos anunciados

"
            f"Ativos: {len(itens_ord)}
"
            f"Total estimado a receber: R$ {_fmt_money_br(float(sum(agg.values())))}

"
            + "
".join(linhas)
            + prox_txt
        )
_send_telegram(msg)

    # ================================
    # ✅ ALERTA: HOJE TEM PAGAMENTO (1 msg/dia)
    # ================================
    if payday_itens and payday_total > 0:
        agg2: Dict[str, float] = {}
        for tk, v in payday_itens:
            agg2[tk] = agg2.get(tk, 0.0) + float(v)
        itens_ord2 = sorted(agg2.items(), key=lambda x: x[1], reverse=True)
        hday = _sha1("PAYDAY|" + hoje_iso + "|" + ",".join([x[0] for x in itens_ord2]))
        if hday not in hashes_enviados:
            linhas2 = [f"• {_fmt_ddmm(hoje_iso)} — {tk}: R$ {_fmt_money_br(val)}" for tk, val in itens_ord2[:12]]
            msg2 = (
                f"📬💰 HOJE TEM PAGAMENTO — {_fmt_date_br(hoje_iso)}\n\n"
                f"Ativos pagando hoje: {len(itens_ord2)}\n"
                f"Estimativa (carteira): R$ {_fmt_money_br(float(sum(agg2.values())))}\n\n"
                + "\n".join(linhas2)
                + "\n\n📌 Ação: confira o extrato da corretora / lançamentos no app"
            )
            _send_telegram(msg2)
            hashes_enviados.add(hday)
            log_rows.append([_now_iso_min(), hday, "*", "PAYDAY", hoje_iso])

    print(f"✅ Inseridos: {inserted}")
    print(f"🔁 Atualizados: {updated}")
    print(f"♻️ Reativados: {reactivated}")
    print(f"📨 Telegram enviados: {telegram_sent}")
    print("🏁 Concluído.")


if __name__ == "__main__":
    run()
