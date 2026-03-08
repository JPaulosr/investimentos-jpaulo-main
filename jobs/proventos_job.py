# jobs/proventos_job.py
# -*- coding: utf-8 -*-
"""
ROBÔ PROVENTOS — FINAL (idempotente + update + soft delete + auto-fix sheet) + P/VP

✅ Lê tickers do ativos_master (sem env TICKERS)
✅ Upsert por event_id + version_hash
✅ Atualiza quando muda (não duplica)
✅ Soft delete (ativo=0) e reativa ao reaparecer
✅ Header contrato (fixo) — não duplica, não insere header aleatório
✅ AUTO-FIX: se a aba estiver com linhas no layout antigo (A-D = hashes), move para K-N

P/VP:
✅ Nova coluna "pvp" no FINAL do header (não desloca colunas)
✅ P/VP vem do ativos_master se existir (pvp | p_vp | p/vp | p_vp_atual | p_vpa | p/vpa)
✅ Fallback: busca P/VP no Investidor10 (mais compatível com seu fluxo)
✅ Auto-fill: se existir linha antiga com pvp vazio (ativo=1), preenche via cache 1x por ticker (sem depender de INSERT/UPDATE)

Telegram:
✅ INSERT = catch-up: event_id novo -> manda telegram
✅ UPDATE = anti-spam por version_hash (alerts_log)

+ Resumo no final do lote (Telegram)
+ Alerta diário: HOJE TEM PAGAMENTO (consolidado)
"""

from __future__ import annotations

import os
import sys
import json
import re
import hashlib
import traceback
import time as _time
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# ✅ GARANTE IMPORTS DO REPO (GitHub Actions / execução via jobs/)
# =============================================================================
from pathlib import Path
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import gspread
import requests
from google.oauth2.service_account import Credentials

# ✅ imports do repo (agora funciona)
from utils.snapshot_carteira import atualizar_snapshot_carteira
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
    "valor_por_cota",        # valor líquido por cota (o que cai na conta)
    "valor_bruto_por_cota",  # bruto antes do IR
    "ir_por_cota",           # IR retido por cota
    "quantidade_ref",
    "fonte_url",
    "capturado_em",
    "event_id",
    "ativo",
    "atualizado_em",
    "version_hash",
    "pvp",
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
            _norm_date(row.get("data_pagamento", "")),
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

def _fmt_ddmm(iso_yyyy_mm_dd: str) -> str:
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
# P/VP — Investidor10 (fallback robusto)
# =============================================================================
def fetch_pvp_investidor10(ticker: str) -> Optional[float]:
    """
    Extrai P/VP do Investidor10.
    Tenta primeiro FII e depois Ação.
    Retorna None se falhar (não quebra o robô).
    """
    tk = _norm_ticker(ticker)
    if not tk:
        return None

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }

    urls = [
        f"https://investidor10.com.br/fiis/{tk.lower()}/",
        f"https://investidor10.com.br/acoes/{tk.lower()}/",
    ]

    # regexs “tolerantes” (HTML muda bastante)
    # pega um número próximo de P/VP (com vírgula ou ponto)
    patterns = [
        re.compile(r"P/VP[^0-9]{0,80}([0-9]+[.,][0-9]+)", re.I | re.S),
        re.compile(r"P\s*/\s*VP[^0-9]{0,80}([0-9]+[.,][0-9]+)", re.I | re.S),
        re.compile(r"P\s*/\s*VPA[^0-9]{0,80}([0-9]+[.,][0-9]+)", re.I | re.S),
    ]

    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200:
                continue
            html = r.text

            for pat in patterns:
                m = pat.search(html)
                if not m:
                    continue
                val = _norm_float(m.group(1))
                if val is None:
                    continue
                # sanity
                if 0.0 < float(val) < 50.0:
                    return float(val)
        except Exception:
            continue

    return None

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
    vals = ws.get_all_values()
    if not vals:
        ws.update([HEADER_CONTRATO], "1:1")
        return HEADER_CONTRATO

    cur = _normalize(vals[0])
    exp = _normalize(HEADER_CONTRATO)

    if cur != exp:
        ws.update([HEADER_CONTRATO], "1:1")
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
    Como pvp foi adicionada no final, K-N continuam sendo event_id..version_hash.
    """
    vals = ws.get_all_values()
    if len(vals) < 2:
        return

    L = max(len(HEADER_CONTRATO), 15)
    r2 = vals[1] + [""] * (L - len(vals[1]))
    a, b, c, d = (str(r2[0]).strip(), str(r2[1]).strip(), str(r2[2]).strip(), str(r2[3]).strip())

    colK = r2[10] if len(r2) > 10 else ""
    if not _looks_like_legacy_row(a, b, c, d):
        return
    if str(colK).strip():
        return

    last_row = len(vals)
    batch_updates = []

    for ridx in range(2, last_row + 1):
        row = (vals[ridx - 1] + [""] * L)[:L]
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
        print("⚠️ Telegram: TOKEN ou CHAT_ID não definidos")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=REQUEST_TIMEOUT,
        )
        if not r.ok:
            print(f"⚠️ Telegram erro {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"⚠️ Telegram exception: {e}")

# =============================================================================
# META (ativos_master) — logo_url / tipo_ativo / classificacao / pvp
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

        # ✅ P/VP vindo do master (se existir)
        pvp_raw = _get(r, "pvp", "p_vp", "p/vp", "p_vp_atual", "p_vpa", "p/vpa")
        pvp = _norm_float(pvp_raw)

        meta[tk] = {
            "logo_url": logo or None,
            "tipo_ativo": tipo_ativo or classe or "",
            "classificacao": classificacao or "",
            "acao_sugerida": "Aguardar pagamento",
            "pvp": pvp,
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
def fetch_events_from_master(sh: gspread.Spreadsheet, pos_map: Dict[str, float] = {}) -> List[Dict[str, Any]]:
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
                # Bruto e IR — já calculados pelo proventos_fetch.py (15% para JCP/REND_TRIB, 0% para FII)
                # Só recalcula se o site retornou totais reais (bruto_total + liq_total) — mais preciso que os 15%
                _qtd_ref     = _norm_float(rr.get("quantidade_ref") or pos_map.get(_norm_ticker(rr.get("ticker", "")), None))
                _bruto_total = _norm_float(rr.get("valor_bruto_total"))
                _ir_total    = _norm_float(rr.get("ir_total"))
                _liq_total   = _norm_float(rr.get("valor_liq_total"))

                if _qtd_ref and _qtd_ref > 0 and _bruto_total and _liq_total:
                    # Site retornou totais reais → divide para obter por cota (mais preciso)
                    rr["valor_bruto_por_cota"] = round(_bruto_total / _qtd_ref, 8)
                    rr["valor_por_cota"]       = round(_liq_total   / _qtd_ref, 8)
                    rr["ir_por_cota"]          = round((_ir_total or (_bruto_total - _liq_total)) / _qtd_ref, 8)
                else:
                    # Usa o que o fetch já calculou (15% automático)
                    rr["valor_bruto_por_cota"] = _norm_float(rr.get("valor_bruto_por_cota"))
                    rr["ir_por_cota"]          = _norm_float(rr.get("ir_por_cota"))
                # Se bruto não informado mas temos vpc (líquido), mantém bruto = None (não inventa)
                rr["quantidade_ref"] = rr.get("quantidade_ref", "")
                rr["fonte_url"] = str(rr.get("fonte_url", "") or "").strip()
                rr["capturado_em"] = str(rr.get("capturado_em", "") or _now_iso_min())

                # ✅ FIX 1: data_com pode ser vazia (Statusinvest nem sempre retorna) — não descartar o evento
                if rr["ticker"] and rr["tipo_pagamento"] and rr["data_pagamento"]:
                    eventos.append(rr)
        except Exception:
            print(f"❌ erro no fetch de {t}:")
            print(traceback.format_exc())

    print(f"📦 fetch total eventos={len(eventos)} (tickers={len(tickers)})")
    return eventos

# =============================================================================
# MIGRAÇÃO AUTOMÁTICA DE HEADER — aba proventos
# =============================================================================
HEADER_PROVENTOS = [
    "id",
    "portfolio_id",
    "data",
    "ticker",
    "tipo",
    "valor",                  # líquido (o que entrou na conta)
    "quantidade_na_data",
    "valor_por_cota",         # vpc líquido
    "valor_bruto",            # bruto antes do IR (novo)
    "ir_retido",              # IR retido total (novo)
    "valor_bruto_por_cota",   # vpc bruto (novo)
    "ir_por_cota",            # IR por cota (novo)
    "origem",
    "criado_em",
]

def ensure_proventos_tab(sh: gspread.Spreadsheet) -> None:
    """
    Garante que a aba 'proventos' tem todas as colunas do HEADER_PROVENTOS.
    - Se a aba não existir: cria com o header completo.
    - Se já existir mas faltar colunas: adiciona apenas as colunas novas
      ao final do header existente (sem deslocar dados).
    Idempotente: pode ser chamado toda vez que o robô rodar.
    """
    aba = ABA_MOVIMENTACOES.replace("movimentacoes", "proventos") if "movimentacoes" in ABA_MOVIMENTACOES else "proventos"
    # tenta pegar a aba de proventos pelo env ou usa "proventos"
    aba_prov = (
        os.getenv("ABA_PROVENTOS_NOVO")
        or os.getenv("ABA_PROVENTOS")
        or "proventos"
    ).strip()

    try:
        try:
            ws = sh.worksheet(aba_prov)
        except Exception:
            ws = sh.add_worksheet(title=aba_prov, rows=5000, cols=30)
            ws.update([HEADER_PROVENTOS], "1:1")
            print(f"✅ Aba '{aba_prov}' criada com header completo.")
            return

        # Lê header atual
        cur_header = ws.row_values(1)
        cur_header_norm = [str(h).strip().lower() for h in cur_header]

        # Descobre quais colunas faltam
        missing = [c for c in HEADER_PROVENTOS if c.lower() not in cur_header_norm]

        if not missing:
            print(f"✅ Header da aba '{aba_prov}' já está completo ({len(cur_header)} colunas).")
            return

        # Adiciona colunas faltantes ao final do header
        next_col = len(cur_header) + 1
        updates = []
        for i, col_name in enumerate(missing):
            col_letter = chr(ord("A") + next_col - 1 + i) if (next_col - 1 + i) < 26 else None
            if col_letter:
                updates.append({"range": f"{col_letter}1", "values": [[col_name]]})

        if updates:
            ws.batch_update(updates)
            print(f"✅ Aba '{aba_prov}': {len(missing)} colunas adicionadas → {missing}")
        else:
            print(f"⚠️ Aba '{aba_prov}': colunas faltantes mas não foi possível calcular posição (>26 colunas).")
            # Fallback: reescreve header linha 1 preservando colunas existentes
            novo_header = cur_header + missing
            ws.update([novo_header], "1:1")
            print(f"✅ Aba '{aba_prov}': header reescrito com {len(novo_header)} colunas.")

    except Exception as e:
        print(f"⚠️ ensure_proventos_tab falhou (não crítico): {e}")

# =============================================================================
# Engine
# =============================================================================
def run() -> None:
    print("🚀 Robô Proventos — schema fixo + auto-fix + batch write + P/VP")
    print(f"🔐 TELEGRAM_TOKEN set? {'SIM' if TELEGRAM_TOKEN else 'NAO'} | CHAT_ID set? {'SIM' if TELEGRAM_CHAT_ID else 'NAO'}")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)

    # Garante colunas novas na aba proventos (migração automática, idempotente)
    ensure_proventos_tab(sh)

    # ── Batch read: lê 4 abas em 1 request para não estourar quota ──────────
    _ABA_NAMES = {
        "master": ABA_ATIVOS_MASTER,
        "mov":    ABA_MOVIMENTACOES,
        "anun":   ABA_ANUNCIADOS,
        "logs":   ABA_LOGS,
    }
    _batch = sh.values_batch_get([f"'{v}'!A:Z" for v in _ABA_NAMES.values()])
    _vr = _batch["valueRanges"]

    def _vr_to_records(vr):
        rows = vr.get("values", [])
        if len(rows) < 2: return []
        hdrs = [str(h).strip().lower() for h in rows[0]]
        return [dict(zip(hdrs, r + [""] * (len(hdrs) - len(r)))) for r in rows[1:]]

    _master_records = _vr_to_records(_vr[0])
    _mov_records    = _vr_to_records(_vr[1])
    _logs_vals_raw  = _vr[3]

    def _build_meta(rows):
        meta = {}
        for r in rows:
            tk = _norm_ticker(r.get("ticker") or r.get("ativo") or r.get("codigo") or "")
            if not tk: continue
            pvp = None
            for pk in ["pvp","p_vp","p/vp","p_vp_atual","p_vpa","p/vpa"]:
                raw = r.get(pk)
                if raw and str(raw).strip():
                    try: pvp = float(str(raw).replace(",",".").strip())
                    except: pvp = None
                    break
            _classe   = str(r.get("classe")   or r.get("tipo_ativo") or "").strip()
            _subtipo  = str(r.get("subtipo")  or "").strip()
            _segmento = str(r.get("segmento") or "").strip()
            _parts = []
            for _p in [_classe, _subtipo, _segmento]:
                if _p and _p.lower() not in [_x.lower() for _x in _parts]:
                    _parts.append(_p)
            _tipo_ativo = " / ".join(_parts).strip() or _classe
            meta[tk] = {
                "logo_url": str(r.get("logo_url") or r.get("logo") or "").strip(),
                "tipo_ativo": _tipo_ativo,                                          # ✅ FIX 1
                "classe": _classe,
                "subtipo": _subtipo,
                "segmento": _segmento,
                "classificacao_capital": str(r.get("classificacao_capital") or "").strip(),
                "pvp": pvp,
            }
        return meta

    def _build_pos(rows):
        def _tof(v):
            if v is None: return 0.0
            if isinstance(v,(int,float)): return float(v)
            st = re.sub(r"[^0-9,.\-]","",str(v).strip())
            if "," in st and "." in st: st = st.replace(".","").replace(",",".")
            else: st = st.replace(",",".")
            try: return float(st)
            except: return 0.0
        pos = {}
        for r in rows:
            tk = _norm_ticker(r.get("ticker") or r.get("ativo") or r.get("codigo") or r.get("papel") or "")
            if not tk: continue
            qtd = _tof(r.get("quantidade") or r.get("qtd") or r.get("cotas") or 0)
            tipo = r.get("tipo_operacao") or r.get("tipo") or r.get("operacao") or r.get("tipo_de_operacao")
            if str(tipo or "").strip().upper() in {"VENDA","V","SELL","S"}: qtd *= -1.0
            pos[tk] = pos.get(tk,0.0) + qtd
        return {k: max(0.0,v) for k,v in pos.items()}

    meta_map = _build_meta(_master_records)
    pos_map  = _build_pos(_mov_records)
    print(f"🧩 meta_map={len(meta_map)} | pos_map={len(pos_map)} | aba_mov='{ABA_MOVIMENTACOES}'")

    # 🔧 Corrige linhas existentes com campos vazios (idempotente)
    repair_empty_fields(sh, meta_map, pos_map)

    ws_anun = _ensure_ws(sh, ABA_ANUNCIADOS, rows=8000, cols=30)
    ws_logs = _ensure_ws(sh, ABA_LOGS, rows=8000, cols=10)

    header = _assert_or_init_header(ws_anun)
    hmap = _col_idx_map(header)

    _fix_misaligned_legacy_rows(ws_anun)

    if not _logs_vals_raw.get("values"):
        ws_logs.update("1:1", [["ts", "event_hash", "ticker", "tipo", "status"]])

    all_vals = ws_anun.get_all_values()

    existing_by_event_id: Dict[str, int] = {}
    existing_version_hash: Dict[str, str] = {}
    existing_ativo: Dict[str, str] = {}

    idx_event_id = hmap["event_id"]
    idx_version = hmap["version_hash"]
    idx_ativo = hmap["ativo"]

    for ridx in range(2, len(all_vals) + 1):
        row = all_vals[ridx - 1]
        eid = str(row[idx_event_id - 1]).strip() if (idx_event_id - 1) < len(row) else ""
        if not eid:
            continue
        existing_by_event_id[eid] = ridx
        existing_version_hash[eid] = str(row[idx_version - 1]).strip() if (idx_version - 1) < len(row) else ""
        existing_ativo[eid] = str(row[idx_ativo - 1]).strip() if (idx_ativo - 1) < len(row) else ""

    logs_records = _vr_to_records(_logs_vals_raw)
    hashes_enviados = {str(r.get("event_hash") or "").strip() for r in logs_records if r.get("event_hash")}
    print(f"🧱 Anti-spam: {len(hashes_enviados)} hashes no alerts_log")

    # =============================================================================
    # ✅ SOFT-DELETE PROATIVO: apaga 30 dias após data_pagamento
    # Mantém o registro visível por 30 dias após o pagamento, depois arquiva (ativo=0)
    # =============================================================================
    hoje_iso = _today_sp_iso()
    from datetime import timedelta
    _limite_delete = (datetime.now(tz=TZ_SP) - timedelta(days=30)).strftime("%Y-%m-%d")
    idx_dp_col = hmap.get("data_pagamento")
    softdelete_updates: List[Dict[str, Any]] = []
    if idx_dp_col:
        for ridx in range(2, len(all_vals) + 1):
            row = all_vals[ridx - 1]
            ativo_val = str(row[idx_ativo - 1]).strip() if (idx_ativo - 1) < len(row) else ""
            if ativo_val in ("0", "False", "false"):
                continue
            dp_raw = str(row[idx_dp_col - 1]).strip() if (idx_dp_col - 1) < len(row) else ""
            dp = _norm_date(dp_raw)
            if dp and dp < _limite_delete:
                softdelete_updates.append({"range": _cell_a1(hmap["ativo"], ridx), "values": [[0]]})
                eid_row = str(row[idx_event_id - 1]).strip() if (idx_event_id - 1) < len(row) else ""
                if eid_row:
                    existing_ativo[eid_row] = "0"
        if softdelete_updates:
            ws_anun.batch_update(softdelete_updates)
            print(f"🗑️ Soft-delete proativo: {len(softdelete_updates)} linhas com data_pagamento > 30 dias marcadas como ativo=0")

    # =============================================================================
    # ✅ ALERTA ANTECIPADO: HOJE TEM PAGAMENTO (varre planilha antes do fetch)
    # Garante alerta mesmo que o fetch não retorne eventos novos
    # =============================================================================
    payday_pre: Dict[str, float] = {}
    idx_status_col = hmap.get("status")
    idx_vpc_col = hmap.get("valor_por_cota")
    idx_tk_col = hmap.get("ticker")
    if idx_status_col and idx_dp_col and idx_tk_col and idx_vpc_col:
        for ridx in range(2, len(all_vals) + 1):
            row = all_vals[ridx - 1]
            ativo_val = str(row[idx_ativo - 1]).strip() if (idx_ativo - 1) < len(row) else ""
            if ativo_val in ("0", "False", "false"):
                continue
            dp_raw = str(row[idx_dp_col - 1]).strip() if (idx_dp_col - 1) < len(row) else ""
            dp = _norm_date(dp_raw)
            if dp != hoje_iso:
                continue
            tk_row = _norm_ticker(row[idx_tk_col - 1]) if (idx_tk_col - 1) < len(row) else ""
            if not tk_row:
                continue
            qtd = float(pos_map.get(tk_row, 0.0) or 0.0)
            if qtd <= 0:
                continue
            vpc_raw = row[idx_vpc_col - 1] if (idx_vpc_col - 1) < len(row) else ""
            vpc = _norm_float(vpc_raw)
            if vpc and vpc > 0:
                payday_pre[tk_row] = payday_pre.get(tk_row, 0.0) + float(qtd) * float(vpc)

    if payday_pre:
        agg_pre = dict(sorted(payday_pre.items(), key=lambda x: x[1], reverse=True))
        hday = _sha1("PAYDAY|" + hoje_iso + "|" + ",".join(sorted(agg_pre.keys())))
        if hday not in hashes_enviados:
            linhas_pre = [f"• {_fmt_ddmm(hoje_iso)} — {tk}: R$ {_fmt_money_br(val)}" for tk, val in list(agg_pre.items())[:12]]
            msg_pre = (
                f"📬💰 HOJE TEM PAGAMENTO — {_fmt_date_br(hoje_iso)}\n\n"
                f"Ativos pagando hoje: {len(agg_pre)}\n"
                f"Estimativa (carteira): R$ {_fmt_money_br(float(sum(agg_pre.values())))}\n\n"
                + "\n".join(linhas_pre)
                + "\n\n📌 Ação: confira o extrato da corretora / lançamentos no app"
            )
            _send_telegram(msg_pre)
            hashes_enviados.add(hday)
            print(f"📬 Alerta PAYDAY enviado: {len(agg_pre)} ativos | R$ {_fmt_money_br(sum(agg_pre.values()))}")

    # Busca real dos eventos anunciados a partir do scraper
    eventos = fetch_events_from_master(sh, pos_map)
    if not eventos:
        print("ℹ️ Nenhum evento retornado pelo fetch. Continuando para soft-delete e alerta de pagamento.")

    inserted = 0
    updated = 0
    reactivated = 0
    telegram_sent = 0

    append_rows: List[List[Any]] = []
    log_rows: List[List[Any]] = []
    cell_updates: List[Dict[str, Any]] = []

    resumo_itens: List[Tuple[str, float, str]] = []
    resumo_total: float = 0.0

    # hoje_iso já foi definido acima (soft-delete)
    payday_itens: List[Tuple[str, float]] = []
    payday_total: float = 0.0

    # ✅ Cache de P/VP (master/fallback) — 1x por ticker no run
    pvp_cache: Dict[str, Optional[float]] = {}

    def _get_sheet_cell(row: List[Any], colname: str) -> str:
        cidx = hmap.get(colname.strip().lower())
        if not cidx:
            return ""
        j = cidx - 1
        # get_all_values TRIMA colunas vazias do final -> pode dar OOR
        return str(row[j]).strip() if j < len(row) else ""

    def _resolve_pvp(tk: str, meta: Dict[str, Any]) -> Optional[float]:
        # 1) master
        pv = meta.get("pvp") if isinstance(meta, dict) else None
        if pv is not None:
            try:
                pvf = float(pv)
                if 0.0 < pvf < 50.0:
                    return pvf
            except Exception:
                pass

        # 2) cache / fallback investidor10
        if tk not in pvp_cache:
            pvp_cache[tk] = fetch_pvp_investidor10(tk)
        return pvp_cache[tk]

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
        setc("valor_bruto_por_cota", row_norm.get("valor_bruto_por_cota"))  # ✅ fix bruto
        setc("ir_por_cota", row_norm.get("ir_por_cota"))                    # ✅ fix ir
        setc("quantidade_ref", row_norm.get("quantidade_ref"))
        setc("fonte_url", row_norm.get("fonte_url"))
        setc("capturado_em", row_norm.get("capturado_em"))
        setc("event_id", row_norm.get("event_id"))
        setc("ativo", row_norm.get("ativo"))
        setc("atualizado_em", row_norm.get("atualizado_em"))
        setc("version_hash", row_norm.get("version_hash"))
        setc("pvp", row_norm.get("pvp"))
        return out

    # ================================
    # LOOP PRINCIPAL
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
            "valor_bruto_por_cota": _norm_float(ev.get("valor_bruto_por_cota", None) or ev.get("valor_bruto", None)),  # ✅ fix bruto
            "ir_por_cota": _norm_float(ev.get("ir_por_cota", None) or ev.get("ir_retido_por_cota", None)),              # ✅ fix ir
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

        meta = meta_map.get(
            row_norm["ticker"],
            {
                "logo_url": None,
                "tipo_ativo": row_norm.get("tipo_ativo") or "",
                "classificacao": "",
                "acao_sugerida": "Aguardar pagamento",
                "pvp": None,
            },
        )

        # ✅ FIX 2: garante tipo_ativo e status nunca vazios
        # tipo_ativo: fetch > ativos_master > fallback por tipo_pagamento
        if not row_norm.get("tipo_ativo"):
            _ta = str(meta.get("tipo_ativo") or meta.get("classe") or "").strip()
            if not _ta:
                # fallback inteligente por tipo_pagamento
                _tp = row_norm.get("tipo_pagamento", "")
                if _tp in ("RENDIMENTO",):
                    _ta = "FII"
                elif _tp in ("JCP", "DIVIDENDO"):
                    _ta = "Ação"
            row_norm["tipo_ativo"] = _ta

        # status: sempre ANUNCIADO se vazio
        if not row_norm.get("status"):
            row_norm["status"] = "ANUNCIADO"

        # quantidade_ref: preenche do pos_map se vazio
        if not row_norm.get("quantidade_ref"):
            _qtd_ref = pos_map.get(row_norm["ticker"], 0.0)
            if _qtd_ref and _qtd_ref > 0:
                row_norm["quantidade_ref"] = _qtd_ref

        qtd = float(pos_map.get(row_norm["ticker"], 0.0) or 0.0)
        posicao = {"qtd": qtd} if qtd > 0 else None

        # ✅ não tem posição -> não grava e faz soft delete se já existia
        if qtd <= 0:
            sheet_row_tmp = existing_by_event_id.get(eid)
            if sheet_row_tmp and sheet_row_tmp > 1:
                prev_ativo_tmp = (existing_ativo.get(eid, "") or "").strip()
                if prev_ativo_tmp not in ("0", "False", "false"):
                    cell_updates.append({"range": _cell_a1(hmap["ativo"], sheet_row_tmp), "values": [[0]]})
                    existing_ativo[eid] = "0"
            continue

        # ✅ P/VP resolvido
        pvp_val = _resolve_pvp(row_norm["ticker"], meta)
        row_norm["pvp"] = "" if pvp_val is None else pvp_val

        # ---------------- INSERT
        if eid not in existing_by_event_id:
            append_rows.append(make_row_out(row_norm))
            inserted += 1
            existing_by_event_id[eid] = -1

            hashes_enviados.add(vhash)
            log_rows.append([_now_iso_min(), vhash, row_norm["ticker"], "ANUNCIADO", row_norm["status"]])

            vpc = row_norm.get("valor_por_cota")
            if qtd > 0 and isinstance(vpc, (int, float)) and vpc > 0:
                total_est = float(qtd) * float(vpc)
                resumo_itens.append((row_norm["ticker"], total_est, row_norm.get("data_pagamento", "")))
                resumo_total += total_est
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

        # ---------------- UPDATE
        sheet_row = existing_by_event_id[eid]
        prev_vhash = existing_version_hash.get(eid, "")
        prev_ativo = (existing_ativo.get(eid, "") or "").strip()

        if prev_ativo in ("0", "False", "false", ""):
            cell_updates.append({"range": _cell_a1(hmap["ativo"], sheet_row), "values": [[1]]})
            existing_ativo[eid] = "1"
            reactivated += 1

        # se não mudou versão: ainda assim preenche pvp se estiver vazio na planilha
        if prev_vhash and prev_vhash == vhash:
            idx_pvp = hmap.get("pvp")
            if idx_pvp:
                cur_pvp = _get_sheet_cell(all_vals[sheet_row - 1], "pvp")
                if (not cur_pvp) and row_norm.get("pvp") not in ("", None):
                    cell_updates.append({"range": _cell_a1(idx_pvp, sheet_row), "values": [[row_norm.get("pvp")]]})
            continue

        valor = row_norm["valor_por_cota"]
        updates: List[Tuple[str, Any]] = [
            ("tipo_ativo", row_norm.get("tipo_ativo", "")),                         # ✅ FIX 3
            ("status", row_norm["status"]),
            ("data_pagamento", row_norm["data_pagamento"]),
            ("valor_por_cota", "" if valor is None else valor),
            ("quantidade_ref", row_norm["quantidade_ref"]),
            ("fonte_url", row_norm["fonte_url"]),
            ("atualizado_em", row_norm["atualizado_em"]),
            ("version_hash", vhash),
            ("pvp", row_norm.get("pvp", "")),
        ]

        for col, val in updates:
            cidx = hmap.get(col.lower())
            if not cidx:
                continue
            cell_updates.append({"range": _cell_a1(cidx, sheet_row), "values": [[val]]})

        existing_version_hash[eid] = vhash
        updated += 1

        if vhash in hashes_enviados:
            print(f"🧱 (UPDATE) Anti-spam bloqueou: {row_norm['ticker']} vhash={vhash[:8]}")
            continue

        hashes_enviados.add(vhash)
        log_rows.append([_now_iso_min(), vhash, row_norm["ticker"], "UPDATE", row_norm["status"]])

        vpc = row_norm.get("valor_por_cota")
        if qtd > 0 and isinstance(vpc, (int, float)) and vpc > 0:
            total_est = float(qtd) * float(vpc)
            resumo_itens.append((row_norm["ticker"], total_est, row_norm.get("data_pagamento", "")))
            resumo_total += total_est
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

    # =============================================================================
    # ✅ AUTO-FILL P/VP para linhas antigas (ativo=1, ticker ok, pvp vazio, e com posição)
    # =============================================================================
    idx_tk = hmap.get("ticker")
    idx_ativo2 = hmap.get("ativo")
    idx_pvp = hmap.get("pvp")
    if idx_tk and idx_ativo2 and idx_pvp:
        for ridx in range(2, len(all_vals) + 1):
            row = all_vals[ridx - 1]
            ativo = str(row[idx_ativo2 - 1]).strip() if (idx_ativo2 - 1) < len(row) else ""
            if ativo in ("0", "False", "false"):
                continue

            tk = _norm_ticker(row[idx_tk - 1]) if (idx_tk - 1) < len(row) else ""
            if not tk:
                continue

            # get_all_values pode não ter a coluna pvp se estiver vazia no final -> tratar como vazio
            cur_pvp = str(row[idx_pvp - 1]).strip() if (idx_pvp - 1) < len(row) else ""
            if cur_pvp:
                continue

            qtd = float(pos_map.get(tk, 0.0) or 0.0)
            if qtd <= 0:
                continue

            meta = meta_map.get(tk, {"pvp": None})
            pv = _resolve_pvp(tk, meta)
            if pv is None:
                continue

            cell_updates.append({"range": _cell_a1(idx_pvp, ridx), "values": [[pv]]})

    # =============================================================================
    # Complemento: pagamentos HOJE existentes na planilha (não mexe com P/VP)
    # =============================================================================
    try:
        idx_status = hmap.get("status")
        idx_dp = hmap.get("data_pagamento")
        idx_vpc = hmap.get("valor_por_cota")
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

    # =============================================================================
    # GRAVAÇÕES (ANTI-429)
    # =============================================================================
    if append_rows:
        CHUNK = 20
        for i in range(0, len(append_rows), CHUNK):
            ws_anun.append_rows(append_rows[i:i + CHUNK], value_input_option="RAW")

    if cell_updates:
        ws_anun.batch_update(cell_updates)

    if log_rows:
        CHUNK = 50
        for i in range(0, len(log_rows), CHUNK):
            ws_logs.append_rows(log_rows[i:i + CHUNK], value_input_option="RAW")

    # =============================================================================
    # RESUMO FINAL DO LOTE
    # =============================================================================
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
        prox_txt = f"\n\n• Próximo pagamento: {_fmt_ddmm(prox)}" if prox else ""
        msg = (
            "📊 Resumo do lote — Proventos anunciados\n\n"
            f"Ativos: {len(itens_ord)}\n"
            f"Total estimado a receber: R$ {_fmt_money_br(float(sum(agg.values())))}\n\n"
            + "\n".join(linhas)
            + prox_txt
        )
        _send_telegram(msg)

       # =============================================================================
    # ALERTA: HOJE TEM PAGAMENTO (1 msg/dia)
    # =============================================================================
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

    print(f"✅ Inseridos: {inserted}")
    print(f"🔁 Atualizados: {updated}")
    print(f"♻️ Reativados: {reactivated}")
    print(f"📨 Telegram enviados: {telegram_sent}")

    # =============================================================================
    # 🔄 ATUALIZA SNAPSHOT CARTEIRA (sempre ao final do job)
    # =============================================================================
    print("🔄 Atualizando snapshot carteira...")
    atualizar_snapshot_posicoes(sh, pos_map)

    print("🏁 Concluído.")


# =============================================================================
# SNAPSHOT CARTEIRA (executado após o robô)
# =============================================================================
def atualizar_snapshot_posicoes(sh, pos_map: dict):
    """
    Lê posicoes_snapshot (fonte imutável: ticker, quantidade, preco_medio)
    e grava o resultado calculado em carteira_snapshot (aba separada).
    Nunca sobrescreve posicoes_snapshot para evitar corrupção em cascata.
    """
    import math

    try:
        _time.sleep(5)  # anti-quota antes do bloco de leituras do snapshot
        ws_pos    = sh.worksheet("posicoes_snapshot")
        ws_cot    = sh.worksheet("cotacoes_cache")
        ws_prov   = sh.worksheet("proventos")
        ws_anun   = sh.worksheet("proventos_anunciados")
        ws_master = sh.worksheet("ativos_master")

        # UNFORMATTED_VALUE: retorna número bruto sem locale pt-BR (evita 24.22→2422)
        def _ws_to_df(ws):
            data = ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
            if not data or len(data) < 2:
                return pd.DataFrame()
            headers = [str(h).strip() for h in data[0]]
            return pd.DataFrame(data[1:], columns=headers)

        df_cot    = _ws_to_df(ws_cot)
        df_prov   = _ws_to_df(ws_prov)
        df_anun   = _ws_to_df(ws_anun)
        df_master = _ws_to_df(ws_master)

        # ✅ FIX: UNFORMATTED_VALUE retorna células vazias como int 0 em vez de "".
        # Normaliza colunas específicas de texto/data para evitar TypeError no sort_values.
        def _sanitize_str_cols(df, cols):
            for c in cols:
                if c in df.columns:
                    col = df[c]
                    # se houver coluna duplicada, pega só a primeira
                    if isinstance(col, pd.DataFrame):
                        col = col.iloc[:, 0]
                    df[c] = col.apply(lambda x: "" if (x == 0 or x == "0" or x is None) else str(x)).replace("nan", "")
        _sanitize_str_cols(df_anun, ["capturado_em", "data_com", "data_pagamento", "atualizado_em"])
        _sanitize_str_cols(df_prov, ["data", "data_pagamento", "criado_em"])

        # ── Sincronizar posicoes_snapshot a partir das movimentacoes ─────────
        # pos_map já foi calculado no início do job (fonte de verdade).
        # Preserva preco_medio atual do Sheets, só atualiza quantidade.
        df_pos_atual = _ws_to_df(ws_pos)
        pm_map = {}
        if not df_pos_atual.empty and "ticker" in df_pos_atual.columns and "preco_medio" in df_pos_atual.columns:
            for _, r in df_pos_atual.iterrows():
                tk = str(r["ticker"]).strip()
                try:
                    pm_map[tk] = float(r["preco_medio"])
                except Exception:
                    pass

        pos_rows = [["ticker", "quantidade", "preco_medio"]]
        for tk in sorted(pos_map.keys()):
            qtd = pos_map[tk]
            if qtd <= 0:
                continue  # posição zerada — remove
            pm = pm_map.get(tk, "")
            pos_rows.append([tk, str(qtd), str(pm)])

        ws_pos.clear()
        ws_pos.update(pos_rows, value_input_option="RAW")
        print(f"✅ posicoes_snapshot sincronizado: {len(pos_rows)-1} tickers")

        cols_base = ["ticker", "quantidade", "preco_medio"]
        df_pos = pd.DataFrame(pos_rows[1:], columns=cols_base)

        df_snapshot = atualizar_snapshot_carteira(
            df_pos,
            df_cot,
            df_prov,
            df_anun,
            df_master,
        )

        def _serialize(v):
            if isinstance(v, pd.Timestamp):
                return v.strftime("%Y-%m-%d %H:%M")
            if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                return ""
            if isinstance(v, float):
                return str(v)  # string com ponto decimal explícito — evita locale pt-BR
            if isinstance(v, int):
                return v
            return v

        rows_out = [df_snapshot.columns.tolist()]
        for _, row in df_snapshot.iterrows():
            rows_out.append([_serialize(val) for val in row])

        # Gravar em aba separada — nunca toca em posicoes_snapshot
        ws_cart = _ensure_ws(sh, "carteira_snapshot", rows=500, cols=25)
        ws_cart.clear()
        ws_cart.update(rows_out, value_input_option="RAW")

        print(f"📊 carteira_snapshot atualizado: {len(rows_out)-1} linhas, {len(df_snapshot.columns)} colunas.")

    except Exception as e:
        import traceback
        print("❌ Erro ao atualizar snapshot carteira:", e)
        traceback.print_exc()



# =============================================================================
# 🔧 REPAIR: corrige linhas existentes com tipo_ativo / status / quantidade_ref vazios
# Chamado automaticamente no início de run() — idempotente
# =============================================================================
def repair_empty_fields(sh: gspread.Spreadsheet, meta_map: Dict[str, Any], pos_map: Dict[str, float]) -> None:
    """
    Varre a aba proventos_anunciados e preenche campos vazios nas linhas existentes:
    - tipo_ativo  : vindo do ativos_master (meta_map)
    - status      : força ANUNCIADO se vazio
    - quantidade_ref : vindo do pos_map
    Não altera version_hash (não dispara re-notificação).
    """
    try:
        ws = sh.worksheet(ABA_ANUNCIADOS)
        all_vals = ws.get_all_values()
        if len(all_vals) < 2:
            return

        header = [str(h).strip().lower() for h in all_vals[0]]
        hmap   = _col_idx_map(all_vals[0])

        idx_ticker  = hmap.get("ticker")
        idx_ta      = hmap.get("tipo_ativo")
        idx_status  = hmap.get("status")
        idx_qtd     = hmap.get("quantidade_ref")
        idx_ativo   = hmap.get("ativo")

        if not idx_ticker:
            print("⚠️ repair: coluna 'ticker' não encontrada, pulando.")
            return

        updates = []
        fixed = 0

        for ridx in range(2, len(all_vals) + 1):
            row = all_vals[ridx - 1]

            def _cell(idx):
                return str(row[idx - 1]).strip() if idx and (idx - 1) < len(row) else ""

            tk = _norm_ticker(_cell(idx_ticker))
            if not tk:
                continue

            # Não toca em linhas soft-deletadas
            ativo_val = _cell(idx_ativo)
            if ativo_val in ("0", "False", "false"):
                continue

            row_fixed = False

            # ── tipo_ativo ──
            if idx_ta:
                cur_ta = _cell(idx_ta)
                if not cur_ta:
                    meta  = meta_map.get(tk, {})
                    new_ta = str(meta.get("tipo_ativo") or meta.get("classe") or "").strip()
                    if not new_ta:
                        # fallback por tipo_pagamento
                        idx_tp = hmap.get("tipo_pagamento")
                        tp = _cell(idx_tp) if idx_tp else ""
                        if tp in ("RENDIMENTO",):
                            new_ta = "FII"
                        elif tp in ("JCP", "DIVIDENDO"):
                            new_ta = "Ação"
                    if new_ta:
                        updates.append({"range": _cell_a1(idx_ta, ridx), "values": [[new_ta]]})
                        row_fixed = True

            # ── status ──
            if idx_status:
                cur_st = _cell(idx_status)
                if not cur_st:
                    updates.append({"range": _cell_a1(idx_status, ridx), "values": [["ANUNCIADO"]]})
                    row_fixed = True

            # ── quantidade_ref ──
            if idx_qtd:
                cur_qtd = _cell(idx_qtd)
                if not cur_qtd:
                    qtd_ref = pos_map.get(tk, 0.0)
                    if qtd_ref and qtd_ref > 0:
                        updates.append({"range": _cell_a1(idx_qtd, ridx), "values": [[qtd_ref]]})
                        row_fixed = True

            if row_fixed:
                fixed += 1

        if updates:
            # batch em blocos de 100 para não estourar limite
            CHUNK = 100
            for i in range(0, len(updates), CHUNK):
                ws.batch_update(updates[i:i+CHUNK])
            print(f"🔧 repair_empty_fields: {fixed} linhas corrigidas ({len(updates)} células atualizadas)")
        else:
            print("✅ repair_empty_fields: nenhum campo vazio encontrado")

    except Exception as e:
        print(f"⚠️ repair_empty_fields falhou (não crítico): {e}")

if __name__ == "__main__":
    run()
