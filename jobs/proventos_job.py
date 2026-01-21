# jobs/proventos_job.py
# -*- coding: utf-8 -*-
"""
ROBÔ PROVENTOS — Versão Final Corrigida (Sem Duplicar Header)

Correções aplicadas:
✅ FIX: Removeu a lógica que inseria um novo header se as colunas não batessem exatamente (causa da duplicação).
✅ FEATURE: Lê tickers da aba 'ativos_master' (não depende apenas do .env).
✅ ROBUSTEZ: Mantém idempotência, soft-delete e envio de Telegram.
"""

from __future__ import annotations

import os
import sys
import json
import re
import hashlib
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials

# =============================================================================
# ✅ GARANTE IMPORTS DO REPO (para rodar via Actions ou local)
# =============================================================================
try:
    # Adiciona a raiz do projeto ao PYTHONPATH para importar 'utils'
    REPO_ROOT = Path(__file__).resolve().parents[1]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
except Exception:
    pass

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
ABA_ATIVOS_MASTER = "ativos_master"  # Fonte de verdade dos tickers

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
    """ID estável: ticker + tipo + data_com."""
    key = "|".join(
        [
            _norm_ticker(row.get("ticker", "")),
            str(row.get("tipo_pagamento", "") or "").strip().upper(),
            _norm_date(row.get("data_com", "")),
        ]
    )
    return _sha1(key)


def event_version_fingerprint(row: Dict[str, Any]) -> str:
    """Versão do evento: muda se valor ou data_pagamento mudarem."""
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
    """
    Garante que a aba existe. Se estiver vazia, cria o header.
    CORREÇÃO: Não força insert_row se o header já existir, evitando duplicação.
    """
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)

    vals = ws.get_all_values()
    
    # Se a planilha estiver totalmente vazia, adiciona o header
    if not vals:
        ws.append_row(header, value_input_option="USER_ENTERED")
    
    # REMOVIDO: A checagem "if cur != want" que causava a duplicação do header.
    # Assumimos que se a planilha tem dados, o header está na linha 1.
    # As colunas faltantes serão adicionadas via _ensure_columns.
    
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
        print(f"⚠️ Adicionando colunas novas ao header: {to_add}")
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
    print(f"🔎 Lendo tickers da aba '{ABA_ATIVOS_MASTER}'...")
    try:
        ws = sh.worksheet(ABA_ATIVOS_MASTER)
    except Exception:
        print(f"❌ Aba '{ABA_ATIVOS_MASTER}' não existe. Crie-a com uma coluna 'ticker'.")
        return []

    rows = _safe_get_records(ws)
    
    # Extrai tickers únicos
    tickers: List[str] = []
    for r in rows:
        # Tenta pegar 'ticker', se não 'ativo'
        t = _norm_ticker(r.get("ticker") or r.get("ativo") or "")
        if t:
            tickers.append(t)

    tickers = sorted(set(tickers))
    if not tickers:
        print(f"⚠️ Nenhum ticker encontrado em '{ABA_ATIVOS_MASTER}'.")
        return []

    print(f"📋 Tickers encontrados ({len(tickers)}): {tickers}")

    # Import dinâmico do fetcher
    try:
        from utils.proventos_fetch import fetch_provento_anunciado  # type: ignore
    except Exception as e:
        print("❌ Falha crítica: não foi possível importar utils.proventos_fetch.")
        print(f"   Erro: {e}")
        return []

    eventos: List[Dict[str, Any]] = []
    
    for t in tickers:
        try:
            # Tenta chamar fetcher (suporta logs=None ou sem logs)
            try:
                rows_ev = fetch_provento_anunciado(t, logs=None)
            except TypeError:
                rows_ev = fetch_provento_anunciado(t)

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

                    # Validação mínima
                    if rr["ticker"] and rr["tipo_pagamento"] and rr["data_com"]:
                        eventos.append(rr)
        except Exception:
            print(f"⚠️ Erro ao buscar proventos para {t}. Pulando.")
            # print(traceback.format_exc()) # Descomente para debug pesado

    print(f"📦 Total de eventos capturados: {len(eventos)}")
    return eventos


# =============================================================================
# Upsert engine
# =============================================================================
def run() -> None:
    print("🚀 Robô Proventos — INICIADO")

    gc = _get_client()
    try:
        sh = gc.open_by_key(SHEET_ID)
    except Exception as e:
        print(f"❌ Erro ao abrir planilha ID {SHEET_ID}: {e}")
        return

    # 1) Garante aba e header básico (sem duplicar)
    ws_anun = _ensure_sheet_with_header(
        sh,
        ABA_ANUNCIADOS,
        header=[
            "ticker", "tipo_ativo", "status", "tipo_pagamento", "data_com",
            "data_pagamento", "valor_por_cota", "quantidade_ref", "fonte_url", "capturado_em"
        ]
    )

    ws_logs = _ensure_sheet_with_header(
        sh,
        ABA_LOGS,
        header=["ts", "event_hash", "ticker", "tipo", "status"]
    )

    # 2) Adiciona colunas novas dinamicamente (se não existirem)
    header = _ensure_columns(ws_anun, required_cols=["event_id", "ativo", "atualizado_em", "version_hash"])
    hmap = _col_idx_map(header)

    # 3) Carrega dados existentes para memória
    all_vals = ws_anun.get_all_values()
    
    # Se só tem header, ok
    if not all_vals:
        all_vals = [header]

    existing_by_event_id: Dict[str, int] = {}
    existing_version_hash: Dict[str, str] = {}
    existing_ativo: Dict[str, str] = {}

    idx_event_id = hmap.get("event_id")
    idx_version = hmap.get("version_hash")
    idx_ativo = hmap.get("ativo")

    # Mapeia linhas existentes (ignora header)
    for ridx in range(2, len(all_vals) + 1):
        row = all_vals[ridx - 1]
        eid = ""
        # Verifica bounds para evitar IndexError
        if idx_event_id and idx_event_id - 1 < len(row):
            eid = str(row[idx_event_id - 1]).strip()
        
        if eid:
            existing_by_event_id[eid] = ridx
            
            if idx_version and idx_version - 1 < len(row):
                existing_version_hash[eid] = str(row[idx_version - 1]).strip()
            
            if idx_ativo and idx_ativo - 1 < len(row):
                existing_ativo[eid] = str(row[idx_ativo - 1]).strip()

    # Logs de spam
    logs_records = _safe_get_records(ws_logs)
    hashes_enviados = {str(r.get("event_hash") or "").strip() for r in logs_records if r.get("event_hash")}

    # 4) Busca eventos
    eventos = fetch_events_from_master(sh)
    if not eventos:
        print("ℹ️ Nenhum evento novo encontrado.")
        return

    inserted = 0
    updated = 0
    reactivated = 0
    telegram_sent = 0
    log_rows: List[List[Any]] = []
    append_rows: List[List[Any]] = []

    # Função auxiliar para preencher linha na ordem correta
    def _set_by_header(out_list: List[Any], col_name: str, val: Any) -> None:
        idx = hmap.get(col_name.strip().lower())
        if idx:
            out_list[idx - 1] = "" if val is None else val

    # 5) Processamento
    for ev in eventos:
        eid = event_id_from_row(ev)
        vhash = event_version_fingerprint(ev)

        ev["event_id"] = eid
        ev["ativo"] = 1
        ev["atualizado_em"] = _now_iso_min()
        ev["version_hash"] = vhash

        valor = ev["valor_por_cota"]
        valor_txt = "-" if valor is None else f"R$ {valor:.4f}"

        # --- CASO 1: INSERT (Novo) ---
        if eid not in existing_by_event_id:
            new_row = [""] * len(header)
            
            # Preenche colunas mapeadas
            _set_by_header(new_row, "ticker", ev["ticker"])
            _set_by_header(new_row, "tipo_ativo", ev["tipo_ativo"])
            _set_by_header(new_row, "status", ev["status"])
            _set_by_header(new_row, "tipo_pagamento", ev["tipo_pagamento"])
            _set_by_header(new_row, "data_com", ev["data_com"])
            _set_by_header(new_row, "data_pagamento", ev["data_pagamento"])
            _set_by_header(new_row, "valor_por_cota", "" if valor is None else valor)
            _set_by_header(new_row, "quantidade_ref", ev["quantidade_ref"])
            _set_by_header(new_row, "fonte_url", ev["fonte_url"])
            _set_by_header(new_row, "capturado_em", ev["capturado_em"])
            
            _set_by_header(new_row, "event_id", eid)
            _set_by_header(new_row, "ativo", 1)
            _set_by_header(new_row, "atualizado_em", ev["atualizado_em"])
            _set_by_header(new_row, "version_hash", vhash)

            append_rows.append(new_row)
            inserted += 1
            existing_by_event_id[eid] = -1  # Marca como processado na memória

            # Telegram Novo
            if vhash not in hashes_enviados:
                hashes_enviados.add(vhash)
                log_rows.append([_now_iso_min(), vhash, ev["ticker"], "ANUNCIADO", ev["status"]])
                _send_telegram(
                    "📌 Provento anunciado (NOVO)\n"
                    f"{ev['ticker']} — {ev['tipo_pagamento']}\n"
                    f"Com: {ev['data_com']} | Pag: {ev['data_pagamento'] or '-'}\n"
                    f"Valor/cota: {valor_txt}"
                )
                telegram_sent += 1
            continue

        # --- CASO 2: UPDATE (Existente) ---
        sheet_row = existing_by_event_id[eid]
        
        # Ignora se acabamos de inserir (sheet_row == -1)
        if sheet_row == -1:
            continue

        prev_vhash = existing_version_hash.get(eid, "")
        prev_ativo = (existing_ativo.get(eid, "") or "").strip()

        # Reativar se estava deletado (soft delete)
        if prev_ativo in ("0", "False", "false", ""):
            try:
                ws_anun.update(_cell_a1(idx_ativo, sheet_row), [[1]])
                reactivated += 1
                existing_ativo[eid] = "1"
            except Exception:
                pass

        # Se versão é igual, não faz update
        if prev_vhash == vhash:
            continue

        # Executa Update
        updates_map = [
            ("status", ev["status"]),
            ("data_pagamento", ev["data_pagamento"]),
            ("valor_por_cota", "" if valor is None else valor),
            ("quantidade_ref", ev["quantidade_ref"]),
            ("fonte_url", ev["fonte_url"]),
            ("atualizado_em", ev["atualizado_em"]),
            ("version_hash", vhash),
        ]

        for col_name, col_val in updates_map:
            cidx = hmap.get(col_name.lower())
            if not cidx: continue
            
            try:
                ws_anun.update(_cell_a1(cidx, sheet_row), [[col_val]])
            except Exception:
                pass

        existing_version_hash[eid] = vhash
        updated += 1

        # Telegram Update
        if vhash not in hashes_enviados:
            hashes_enviados.add(vhash)
            log_rows.append([_now_iso_min(), vhash, ev["ticker"], "UPDATE", ev["status"]])
            _send_telegram(
                "🔁 Provento anunciado (ATUALIZADO)\n"
                f"{ev['ticker']} — {ev['tipo_pagamento']}\n"
                f"Com: {ev['data_com']} | Pag: {ev['data_pagamento'] or '-'}\n"
                f"Valor/cota: {valor_txt}"
            )
            telegram_sent += 1

    # 6) Persistência em Lote
    if append_rows:
        print(f"💾 Salvando {len(append_rows)} novas linhas...")
        try:
            ws_anun.append_rows(append_rows, value_input_option="USER_ENTERED")
        except Exception as e:
            print(f"⚠️ Erro ao salvar em lote: {e}. Tentando linha a linha...")
            for row in append_rows:
                try:
                    ws_anun.append_row(row, value_input_option="USER_ENTERED")
                except Exception:
                    print("❌ Erro ao salvar linha avulsa.")

    # 7) Logs
    if log_rows:
        try:
            ws_logs.append_rows(log_rows, value_input_option="USER_ENTERED")
        except Exception:
            pass

    print(f"✅ Inseridos: {inserted}")
    print(f"🔁 Atualizados: {updated}")
    print(f"♻️ Reativados: {reactivated}")
    print(f"📨 Telegram enviados: {telegram_sent}")
    print("🏁 Job concluído com sucesso.")


if __name__ == "__main__":
    run()