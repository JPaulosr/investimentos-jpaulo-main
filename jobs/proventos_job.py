# jobs/proventos_job.py
# -*- coding: utf-8 -*-
"""
ROBÔ PROVENTOS — versão robusta com DIAGNÓSTICO
"""

from __future__ import annotations

import os
import json
import re
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials

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

TICKERS_ENV = (os.getenv("TICKERS") or "").strip()
REQUEST_TIMEOUT = 20
ABA_ANUNCIADOS = "proventos_anunciados"
ABA_LOGS = "alerts_log"

if not SHEET_ID:
    raise RuntimeError("❌ SHEET_ID vazio. Verifique as Secrets do GitHub.")
if not GCP_JSON:
    raise RuntimeError("❌ GCP_SERVICE_ACCOUNT_JSON vazio. Verifique as Secrets.")

# =============================================================================
# Helpers
# =============================================================================
def _now_iso_min() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")

def _norm_ticker(s: Any) -> str:
    if not s: return ""
    s = str(s).strip().upper()
    return re.sub(r"[^A-Z0-9]", "", s)

def _norm_date(s: Any) -> str:
    if not s: return ""
    # Se já for datetime/date
    if hasattr(s, "strftime"):
        return s.strftime("%Y-%m-%d")
    
    st = str(s).strip()
    if not st or st.lower() in ["-", "n/a", "null"]: return ""
    
    # Tenta formatos comuns brasileiros e ISO
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(st, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    try:
        # Tenta ISO com timestamp
        dt = datetime.fromisoformat(st.replace("Z", "").split(".")[0])
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return ""

def _norm_float(v: Any) -> Optional[float]:
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    st = str(v).strip()
    if not st: return None
    st = re.sub(r"[^0-9,.\-]", "", st)
    if "," in st and "." in st:
        st = st.replace(".", "").replace(",", ".") # Ex: 1.000,50 -> 1000.50
    else:
        st = st.replace(",", ".")
    try:
        return float(st)
    except Exception:
        return None

def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def event_id_from_row(row: Dict[str, Any]) -> str:
    key = "|".join([
        _norm_ticker(row.get("ticker", "")),
        str(row.get("tipo_pagamento", "") or "").strip().upper(),
        _norm_date(row.get("data_com", "")),
    ])
    return _sha1(key)

def event_version_fingerprint(row: Dict[str, Any]) -> str:
    v = _norm_float(row.get("valor_por_cota", None))
    vtxt = "" if v is None else f"{float(v):.8f}"
    key = "|".join([
        event_id_from_row(row),
        _norm_date(row.get("data_pagamento", "")),
        vtxt,
        str(row.get("status", "") or "").strip().upper(),
    ])
    return _sha1(key)

# =============================================================================
# Google Sheets
# =============================================================================
def _get_client() -> gspread.Client:
    info = json.loads(GCP_JSON)
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def _ensure_sheet_with_header(sh: gspread.Spreadsheet, title: str, header: List[str]) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=2000, cols=20)
    
    vals = ws.get_all_values()
    if not vals:
        ws.append_row(header, value_input_option="USER_ENTERED")
        return ws
        
    # Verifica header simples (apenas checa se a linha 1 existe)
    return ws

def _ensure_columns(ws: gspread.Worksheet, required_cols: List[str]) -> List[str]:
    header = ws.row_values(1)
    if not header:
        ws.append_row(required_cols, value_input_option="USER_ENTERED")
        return required_cols
        
    header_lower = [str(c).strip().lower() for c in header]
    to_add = [c for c in required_cols if c.strip().lower() not in header_lower]
    
    if to_add:
        print(f"⚠️ Adicionando colunas faltantes: {to_add}")
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
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=REQUEST_TIMEOUT,
        )
    except Exception as e:
        print(f"⚠️ Erro Telegram: {e}")

# =============================================================================
# FETCH
# =============================================================================
def _load_fetcher():
    try:
        from utils.proventos_fetch import fetch_provento_anunciado
        return fetch_provento_anunciado
    except ImportError:
        return None

def fetch_events() -> List[Dict[str, Any]]:
    fetcher = _load_fetcher()
    if fetcher is None:
        print("❌ CRÍTICO: fetcher não encontrado em utils/proventos_fetch.py")
        return []

    tickers = [_norm_ticker(t) for t in TICKERS_ENV.split(",") if _norm_ticker(t)]
    print(f"🔎 Buscando proventos para {len(tickers)} tickers: {tickers}")
    
    out: List[Dict[str, Any]] = []
    
    for t in tickers:
        try:
            # Tenta chamada com logs=None ou sem argumentos
            try:
                rows = fetcher(t, logs=None)
            except TypeError:
                rows = fetcher(t)

            if not rows:
                print(f"   🔹 {t}: Nenhum dado encontrado na fonte.")
                continue

            count_valid = 0
            for r in rows:
                rr = dict(r)
                # Sanitização prévia
                rr["ticker"] = _norm_ticker(rr.get("ticker") or t)
                rr["data_com_raw"] = str(rr.get("data_com", "")) # Guardar original para debug
                rr["data_com"] = _norm_date(rr.get("data_com", ""))
                
                # Validação explicita
                if not rr["ticker"]:
                    continue
                if not rr["data_com"]:
                    # LOG DE DIAGNÓSTICO IMPORTANTE
                    print(f"   ⚠️ IGNORADO {t}: Data Com inválida. Original: '{rr['data_com_raw']}'")
                    continue
                    
                # Preenche defaults
                rr["tipo_pagamento"] = str(rr.get("tipo_pagamento", "")).strip().upper()
                rr["status"] = str(rr.get("status", "ANUNCIADO")).strip().upper()
                rr["data_pagamento"] = _norm_date(rr.get("data_pagamento", ""))
                rr["valor_por_cota"] = _norm_float(rr.get("valor_por_cota", None))
                rr["quantidade_ref"] = rr.get("quantidade_ref", "")
                rr["fonte_url"] = str(rr.get("fonte_url", "")).strip()
                rr["capturado_em"] = str(rr.get("capturado_em", "") or _now_iso_min())
                
                out.append(rr)
                count_valid += 1
            
            print(f"   ✅ {t}: {count_valid} proventos válidos capturados.")

        except Exception as e:
            print(f"❌ Erro no fetch de {t}: {e}")

    return out

# =============================================================================
# MAIN LOGIC
# =============================================================================
def run() -> None:
    print("🚀 Robô Proventos START", flush=True)

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)

    # 1. Setup Abas
    ws_anun = _ensure_sheet_with_header(
        sh, ABA_ANUNCIADOS,
        header=["ticker", "tipo_ativo", "status", "tipo_pagamento", "data_com", "data_pagamento", 
                "valor_por_cota", "quantidade_ref", "fonte_url", "capturado_em"]
    )
    ws_logs = _ensure_sheet_with_header(
        sh, ABA_LOGS, header=["ts", "event_hash", "ticker", "tipo", "status"]
    )

    # 2. Setup Colunas Extras
    header = _ensure_columns(ws_anun, required_cols=["event_id", "ativo", "atualizado_em", "version_hash"])
    hmap = _col_idx_map(header)

    # 3. Mapeia dados existentes
    print("📂 Lendo planilha existente...", flush=True)
    all_vals = ws_anun.get_all_values()
    
    existing_by_event_id = {}
    existing_version_hash = {}
    existing_ativo = {}

    idx_eid = hmap.get("event_id")
    idx_ver = hmap.get("version_hash")
    idx_ati = hmap.get("ativo")

    # Começa da linha 2 (pula header)
    for ridx in range(2, len(all_vals) + 1):
        row = all_vals[ridx - 1]
        eid = str(row[idx_eid - 1]).strip() if idx_eid and (idx_eid-1) < len(row) else ""
        if eid:
            existing_by_event_id[eid] = ridx
            existing_version_hash[eid] = str(row[idx_ver - 1]).strip() if idx_ver and (idx_ver-1) < len(row) else ""
            existing_ativo[eid] = str(row[idx_ati - 1]).strip() if idx_ati and (idx_ati-1) < len(row) else ""

    # 4. Fetch
    eventos = fetch_events()
    if not eventos:
        print("ℹ️ Nenhum evento retornado. Verifique se os tickers estão configurados ou se o site fonte mudou.", flush=True)
        return

    print(f"📊 Processando {len(eventos)} eventos encontrados...", flush=True)

    append_rows = []
    log_rows = []
    
    # Contadores
    stats = {"inserted": 0, "updated": 0, "reactivated": 0, "skipped": 0}
    hashes_enviados = set() # Evita spam na mesma execução

    for ev in eventos:
        eid = event_id_from_row(ev)
        vhash = event_version_fingerprint(ev)
        
        ev["event_id"] = eid
        ev["ativo"] = 1
        ev["version_hash"] = vhash
        ev["atualizado_em"] = _now_iso_min()

        # --- NOVO ---
        if eid not in existing_by_event_id:
            # Prepara linha nova
            new_row = [""] * len(header)
            for col_name, col_idx in hmap.items():
                val = ev.get(col_name)
                # Fallback para chaves que não correspondem direto
                if val is None and col_name in ev: val = ev[col_name]
                new_row[col_idx - 1] = "" if val is None else val
            
            append_rows.append(new_row)
            stats["inserted"] += 1
            existing_by_event_id[eid] = -1 # Marca como processado
            
            # Telegram (Novo)
            if vhash not in hashes_enviados:
                hashes_enviados.add(vhash)
                vtxt = f"R$ {ev['valor_por_cota']:.4f}" if ev['valor_por_cota'] else "-"
                _send_telegram(f"📌 NOVO Provento: {ev['ticker']} ({ev['tipo_pagamento']})\nData Com: {ev['data_com']}\nValor: {vtxt}")
                log_rows.append([_now_iso_min(), vhash, ev['ticker'], "ANUNCIADO", ev['status']])

        # --- EXISTENTE (Update) ---
        else:
            sheet_row = existing_by_event_id[eid]
            
            # Placeholder ignorado (já inserido nesta execução)
            if sheet_row == -1: 
                continue

            prev_vhash = existing_version_hash.get(eid, "")
            prev_ativo = existing_ativo.get(eid, "")

            # Reativar
            if prev_ativo in ("0", "False", "false", ""):
                try:
                    ws_anun.update(_cell_a1(idx_ati, sheet_row), [[1]])
                    stats["reactivated"] += 1
                    existing_ativo[eid] = "1"
                except Exception: pass

            # Update se mudou algo
            if prev_vhash != vhash:
                updates = [
                    ("status", ev["status"]),
                    ("data_pagamento", ev["data_pagamento"]),
                    ("valor_por_cota", ev["valor_por_cota"]),
                    ("version_hash", vhash),
                    ("atualizado_em", ev["atualizado_em"])
                ]
                
                for col, val in updates:
                    cidx = hmap.get(col)
                    if cidx:
                        ws_anun.update(_cell_a1(cidx, sheet_row), [[val]])
                
                stats["updated"] += 1
                
                # Telegram (Update)
                if vhash not in hashes_enviados:
                    hashes_enviados.add(vhash)
                    _send_telegram(f"🔁 ATUALIZADO: {ev['ticker']} ({ev['tipo_pagamento']})\nData Pag: {ev['data_pagamento']}")
                    log_rows.append([_now_iso_min(), vhash, ev['ticker'], "UPDATE", ev['status']])
            else:
                stats["skipped"] += 1

    # 5. Commit Batch
    if append_rows:
        print(f"💾 Salvando {len(append_rows)} novas linhas...", flush=True)
        try:
            ws_anun.append_rows(append_rows, value_input_option="USER_ENTERED")
        except Exception as e:
            print(f"⚠️ Erro no append_rows em lote: {e}. Tentando um por um...", flush=True)
            for r in append_rows:
                try:
                    ws_anun.append_row(r, value_input_option="USER_ENTERED")
                except Exception as ex:
                    print(f"❌ Falha ao salvar linha individual: {ex}")

    if log_rows:
        try:
            ws_logs.append_rows(log_rows, value_input_option="USER_ENTERED")
        except Exception: pass

    print("🏁 RESUMO FINAL:")
    print(f"   Novos (Inseridos): {stats['inserted']}")
    print(f"   Atualizados:       {stats['updated']}")
    print(f"   Reativados:        {stats['reactivated']}")
    print(f"   Sem mudanças:      {stats['skipped']}")
    print("🚀 FIM DO JOB", flush=True)

if __name__ == "__main__":
    run()