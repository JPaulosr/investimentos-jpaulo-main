# jobs/proventos_job.py
# -*- coding: utf-8 -*-
"""
ROBÔ PROVENTOS — versão robusta (idempotente + update + soft delete)
INTEGRADA COM FETCH REAL + CORREÇÃO DE BUGS
"""

from __future__ import annotations

import os
import sys
import json
import re
import time
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials

# --- IMPORTANTE: Adiciona o diretório raiz para importar utils ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from utils.proventos_fetch import fetch_provento_anunciado
except ImportError:
    print("⚠️ AVISO: utils.proventos_fetch não encontrado. O robô não buscará dados reais.")
    def fetch_provento_anunciado(t, logs=None): return []

# =============================================================================
# ENV
# =============================================================================
SHEET_ID = (os.getenv("SHEET_ID") or os.getenv("SHEET_ID_NOVO") or "").strip()
GCP_JSON = (os.getenv("GCP_SERVICE_ACCOUNT_JSON") or "").strip()

TELEGRAM_TOKEN = (
    os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or ""
).strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

REQUEST_TIMEOUT = 20

ABA_ATIVOS = "ativos_master"  # Aba para ler os tickers
ABA_ANUNCIADOS = "proventos_anunciados"
ABA_LOGS = "alerts_log"

if not SHEET_ID:
    raise RuntimeError("❌ SHEET_ID vazio.")
if not GCP_JSON:
    raise RuntimeError("❌ GCP_SERVICE_ACCOUNT_JSON vazio.")


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
    st = str(s).strip()
    if not st: return ""
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
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    st = str(v).strip()
    if not st: return None
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

def _safe_get_records(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    try:
        return ws.get_all_records()
    except Exception:
        # Fallback para leitura manual se headers estiverem zoados
        try:
            vals = ws.get_all_values()
            if not vals: return []
            header = vals[0]
            data = vals[1:]
            out = []
            for row in data:
                item = {}
                for i, col in enumerate(header):
                    if str(col).strip():
                        item[col] = row[i] if i < len(row) else ""
                out.append(item)
            return out
        except:
            return []

def _ensure_sheet_with_header(sh, title, header, rows=1000, cols=20):
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
    
    vals = ws.get_all_values()
    if not vals:
        ws.append_row(header, value_input_option="USER_ENTERED")
        return ws
    
    return ws

def _ensure_columns(ws, required_cols):
    vals = ws.get_all_values()
    if not vals: return required_cols
    header = [str(c).strip() for c in vals[0]]
    header_lower = {h.lower() for h in header}
    
    to_add = [c for c in required_cols if c.lower() not in header_lower]
    if to_add:
        new_header = header + to_add
        ws.update(range_name="1:1", values=[new_header])
        return new_header
    return header

def _col_idx_map(header):
    return {c.strip().lower(): i + 1 for i, c in enumerate(header)}

def _cell_a1(col_idx, row_idx):
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
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=REQUEST_TIMEOUT,
        )
    except: pass

# =============================================================================
# FETCH REAL
# =============================================================================
def fetch_events(sh: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    print("🔎 Lendo ativos para monitorar...")
    try:
        ws_ativos = sh.worksheet(ABA_ATIVOS)
        records = _safe_get_records(ws_ativos)
        tickers = list(set([str(r.get('ticker') or '').strip().upper() for r in records if r.get('ticker')]))
    except Exception as e:
        print(f"⚠️ Erro ao ler {ABA_ATIVOS}: {e}")
        return []

    print(f"📋 Monitorando {len(tickers)} ativos: {tickers}")
    
    novos_eventos = []
    
    for t in tickers:
        if not t: continue
        try:
            resultados = fetch_provento_anunciado(t)
            
            for item in resultados:
                evt = {
                    "ticker": t,
                    "tipo_ativo": "",
                    "status": "ANUNCIADO",
                    "tipo_pagamento": item.get("tipo_pagamento", "RENDIMENTO"),
                    "data_com": item.get("data_com", ""),
                    "data_pagamento": item.get("data_pagamento", ""),
                    "valor_por_cota": item.get("valor_por_cota", 0),
                    "quantidade_ref": "",
                    "fonte_url": item.get("fonte_url", ""),
                    "capturado_em": _now_iso_min(),
                }
                novos_eventos.append(evt)
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Erro ao buscar {t}: {e}")

    print(f"✅ Total de eventos encontrados na web: {len(novos_eventos)}")
    return novos_eventos

# =============================================================================
# Upsert engine
# =============================================================================
def run() -> None:
    print("🚀 Robô Proventos — IDEMPOTENTE + FETCH REAL")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)

    # 1) Setup das abas
    ws_anun = _ensure_sheet_with_header(
        sh, ABA_ANUNCIADOS,
        header=["ticker", "tipo_ativo", "status", "tipo_pagamento", "data_com", 
                "data_pagamento", "valor_por_cota", "quantidade_ref", 
                "fonte_url", "capturado_em"],
    )
    ws_logs = _ensure_sheet_with_header(
        sh, ABA_LOGS, header=["ts", "event_hash", "ticker", "tipo", "status"]
    )

    # 2) Garante colunas de controle
    header = _ensure_columns(ws_anun, required_cols=["event_id", "ativo", "atualizado_em", "version_hash"])
    hmap = _col_idx_map(header)

    # 3) Carrega índices
    print("📚 Indexando base existente...")
    all_vals = ws_anun.get_all_values()
    
    existing_by_event_id = {}
    existing_version_hash = {}
    existing_ativo = {}

    idx_event_id = hmap.get("event_id")
    idx_version = hmap.get("version_hash")
    idx_ativo = hmap.get("ativo")

    for ridx, row in enumerate(all_vals, start=1):
        if ridx == 1: continue 
        
        eid = ""
        if idx_event_id and idx_event_id <= len(row):
            eid = str(row[idx_event_id - 1]).strip()
        
        if eid:
            existing_by_event_id[eid] = ridx
            if idx_version and idx_version <= len(row):
                existing_version_hash[eid] = str(row[idx_version - 1]).strip()
            if idx_ativo and idx_ativo <= len(row):
                existing_ativo[eid] = str(row[idx_ativo - 1]).strip()

    # 4) FETCH REAL
    eventos = fetch_events(sh)
    
    if not eventos:
        print("💤 Nenhum evento encontrado para processar.")
        return

    # 5) Processamento
    inserted = 0
    updated = 0
    reactivated = 0
    telegram_sent = 0
    
    logs_records = _safe_get_records(ws_logs)
    hashes_enviados = {str(r.get("event_hash") or "").strip() for r in logs_records if r.get("event_hash")}
    
    append_rows = []
    log_rows = []

    for ev in eventos:
        row_norm = {
            "ticker": _norm_ticker(ev.get("ticker")),
            "tipo_ativo": str(ev.get("tipo_ativo", "")).strip(),
            "status": str(ev.get("status", "ANUNCIADO")).strip().upper(),
            "tipo_pagamento": str(ev.get("tipo_pagamento", "")).strip().upper(),
            "data_com": _norm_date(ev.get("data_com")),
            "data_pagamento": _norm_date(ev.get("data_pagamento")),
            "valor_por_cota": _norm_float(ev.get("valor_por_cota")),
            "quantidade_ref": ev.get("quantidade_ref", ""),
            "fonte_url": str(ev.get("fonte_url", "")).strip(),
            "capturado_em": str(ev.get("capturado_em", _now_iso_min())),
        }

        if not row_norm["ticker"] or not row_norm["tipo_pagamento"] or not row_norm["data_com"]:
            continue

        eid = event_id_from_row(row_norm)
        vhash = event_version_fingerprint(row_norm)
        
        row_norm["event_id"] = eid
        row_norm["ativo"] = 1
        row_norm["atualizado_em"] = _now_iso_min()
        row_norm["version_hash"] = vhash

        # INSERT
        if eid not in existing_by_event_id:
            out = [""] * len(header)
            def setc(col, val):
                j = hmap.get(col.lower())
                if j: out[j - 1] = "" if val is None else val
            
            for k, v in row_norm.items():
                setc(k, v)
            
            append_rows.append(out)
            inserted += 1
            existing_by_event_id[eid] = -1 

            if vhash not in hashes_enviados:
                hashes_enviados.add(vhash)
                log_rows.append([_now_iso_min(), vhash, row_norm["ticker"], "ANUNCIADO", row_norm["status"]])
                
                val_txt = f"R$ {row_norm['valor_por_cota']:.4f}" if row_norm['valor_por_cota'] else "-"
                msg = (f"📌 <b>Novo Provento: {row_norm['ticker']}</b>\n"
                       f"Tipo: {row_norm['tipo_pagamento']}\n"
                       f"Data Com: {row_norm['data_com']}\n"
                       f"Pagamento: {row_norm['data_pagamento'] or '-'}\n"
                       f"Valor: {val_txt}")
                _send_telegram(msg)
                telegram_sent += 1
            continue

        # UPDATE
        sheet_row = existing_by_event_id[eid]
        prev_vhash = existing_version_hash.get(eid, "")
        prev_ativo = str(existing_ativo.get(eid, "")).strip()

        # Reativar (soft delete)
        if prev_ativo in ("0", "False", "false", ""):
            try:
                # CORREÇÃO DE DEPRECATION E NOME DE VARIÁVEL
                ws_anun.update(range_name=_cell_a1(hmap["ativo"], sheet_row), values=[[1]])
                reactivated += 1
                existing_ativo[eid] = "1"
            except: pass

        # Se mudou valor ou data
        if prev_vhash != vhash:
            updates = [
                ("status", row_norm["status"]),
                ("data_pagamento", row_norm["data_pagamento"]),
                ("valor_por_cota", row_norm["valor_por_cota"]),
                ("quantidade_ref", row_norm["quantidade_ref"]),
                ("atualizado_em", row_norm["atualizado_em"]),
                ("version_hash", vhash)
            ]
            
            for col, val in updates:
                cidx = hmap.get(col)
                if cidx:
                    val_safe = "" if val is None else val
                    try:
                        # CORREÇÃO DE DEPRECATION E NOME DE VARIÁVEL
                        ws_anun.update(range_name=_cell_a1(cidx, sheet_row), values=[[val_safe]])
                    except: pass
            
            existing_version_hash[eid] = vhash
            updated += 1
            
            if vhash not in hashes_enviados:
                hashes_enviados.add(vhash)
                log_rows.append([_now_iso_min(), vhash, row_norm["ticker"], "UPDATE", "Atualizado"])
                
                val_txt = f"R$ {row_norm['valor_por_cota']:.4f}" if row_norm['valor_por_cota'] else "-"
                msg = (f"✏️ <b>Provento Atualizado: {row_norm['ticker']}</b>\n"
                       f"Tipo: {row_norm['tipo_pagamento']}\n"
                       f"Pagamento: {row_norm['data_pagamento'] or '-'}\n"
                       f"Valor: {val_txt}")
                _send_telegram(msg)
                telegram_sent += 1

    # 6) Gravação em Lote (CORREÇÃO DA VARIÁVEL AQUI)
    if append_rows:
        print(f"💾 Salvando {len(append_rows)} novos registros...")
        # AQUI ESTAVA O ERRO: era ws_anunciados, corrigido para ws_anun
        ws_anun.append_rows(append_rows, value_input_option="USER_ENTERED")
    
    if log_rows:
        ws_logs.append_rows(log_rows, value_input_option="USER_ENTERED")

    print(f"✅ Inseridos: {inserted}")
    print(f"🔁 Atualizados: {updated}")
    print(f"♻️ Reativados: {reactivated}")
    print(f"📨 Telegram: {telegram_sent}")

if __name__ == "__main__":
    run()