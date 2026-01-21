# jobs/proventos_job.py
# -*- coding: utf-8 -*-
import os
import sys
import json
import hashlib
import time
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import requests

# Ajuste de path para utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from utils.proventos_fetch import fetch_provento_anunciado
except ImportError:
    def fetch_provento_anunciado(t, logs=None): return []

# --- 1. CONFIGURAÇÃO ROBUSTA (SEM ADIVINHAÇÃO) ---

# Tenta ler SHEET_ID_NOVO, se não tiver, tenta SHEET_ID, se não tiver, vazio.
# O .strip() remove espaços e quebras de linha invisíveis (CRUCIAL no GitHub Actions)
SHEET_ID = (os.getenv("SHEET_ID_NOVO") or os.getenv("SHEET_ID") or "").strip()

print(f"🔎 DEBUG: SHEET_ID length: {len(SHEET_ID)}") # Se for 0, a variável de ambiente não chegou.

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GCP_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON")

ABA_ATIVOS = "ativos_master"
ABA_ANUNCIADOS = "proventos_anunciados"
ABA_LOGS = "alerts_log"

def _get_client():
    if not GCP_JSON:
        raise RuntimeError("❌ GCP_SERVICE_ACCOUNT_JSON não definido.")
    
    info = json.loads(GCP_JSON)
    
    # --- LOG DO E-MAIL EXATO (PARA CONFERIR PERMISSÃO) ---
    email_robo = info.get("client_email", "???")
    print(f"🤖 E-MAIL DA SERVICE ACCOUNT: {email_robo}")
    print("👉 Esse e-mail PRECISA ser 'Editor' na planilha.")
    # -----------------------------------------------------

    if "private_key" in info:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
        
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

def _send_telegram(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", 
                      json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"})
    except: pass

def _generate_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def run():
    print("🚀 Iniciando Robô (Modo Blindado)...")
    
    if not SHEET_ID:
        raise RuntimeError("❌ ERRO FATAL: SHEET_ID está vazio. Verifique os Secrets.")

    gc = _get_client()
    
    # Tenta abrir a planilha
    try:
        sh = gc.open_by_key(SHEET_ID)
        print(f"✅ SUCESSO! Planilha '{sh.title}' aberta.")
    except Exception as e:
        print("\n❌ FALHA AO ABRIR PLANILHA (404/NotFound)")
        print(f"Tentando abrir ID: '{SHEET_ID}' (Tamanho: {len(SHEET_ID)})")
        print("Causas possíveis:")
        print("1. O ID no Secret está errado (tem https:// ou espaços).")
        print("2. O e-mail do robô (acima) não foi convidado como Editor.")
        raise e

    # Verifica se as abas existem
    try:
        ws_anunciados = sh.worksheet(ABA_ANUNCIADOS)
        ws_ativos = sh.worksheet(ABA_ATIVOS)
    except:
        raise RuntimeError(f"❌ Abas '{ABA_ANUNCIADOS}' ou '{ABA_ATIVOS}' não encontradas.")

    # Cria aba de logs se não existir
    try:
        ws_logs = sh.worksheet(ABA_LOGS)
    except:
        ws_logs = sh.add_worksheet(ABA_LOGS, rows=1000, cols=5)
        ws_logs.append_row(["timestamp", "event_hash", "ticker", "tipo", "mensagem"])

    # --- LÓGICA DO ROBÔ ---
    print("📋 Lendo carteira...")
    ativos = ws_ativos.get_all_records()
    tickers = list(set([str(r['ticker']).strip().upper() for r in ativos if r.get('ticker')]))
    print(f"🔎 Verificando {len(tickers)} ativos...")

    exist_records = ws_anunciados.get_all_records()
    existing_keys = set()
    for r in exist_records:
        key = f"{r.get('ticker')}_{r.get('tipo_pagamento')}_{r.get('data_com')}_{r.get('data_pagamento')}_{r.get('valor_por_cota')}"
        existing_keys.add(key)

    logs_db = ws_logs.get_all_records()
    sent_hashes = set(str(r.get('event_hash')) for r in logs_db)
    
    new_db = []
    queue = []
    
    for t in tickers:
        try:
            res = fetch_provento_anunciado(t)
            for item in res:
                val = float(item.get('valor_por_cota', 0))
                if val <= 0: continue
                
                tk = str(item.get('ticker')).upper()
                tp = str(item.get('tipo_pagamento')).upper()
                dc = str(item.get('data_com'))
                dp = str(item.get('data_pagamento'))
                
                db_key = f"{tk}_{tp}_{dc}_{dp}_{val}"
                
                if db_key not in existing_keys:
                    print(f"✨ NOVO: {tk} {val}")
                    new_db.append([
                        tk, "ANUNCIADO", tp, dc, dp, val, 
                        item.get('fonte_url',''), "Robô GitHub", 
                        datetime.now().strftime("%Y-%m-%d %H:%M")
                    ])
                    existing_keys.add(db_key)
                    
                    msg = f"💰 <b>{tk}</b>: R$ {val:,.2f} ({tp})\n📅 Pag: {dp}"
                    h = _generate_hash(msg)
                    if h not in sent_hashes:
                        queue.append((msg, h, tk))
                        sent_hashes.add(h)
            time.sleep(0.5)
        except Exception as e:
            print(f"⚠️ Erro ao ler {t}: {e}")

    if new_db:
        ws_anunciados.append_rows(new_db, value_input_option="USER_ENTERED")
        print(f"💾 Salvou {len(new_db)} registros.")
    
    if queue:
        print(f"📢 Enviando {len(queue)} alertas...")
        log_rows = []
        for m, h, tk in queue:
            _send_telegram(m)
            log_rows.append([datetime.now().strftime("%Y-%m-%d %H:%M"), h, tk, "ALERTA", "Enviado"])
            time.sleep(1)
        ws_logs.append_rows(log_rows, value_input_option="USER_ENTERED")
    else:
        print("🤫 Nenhum alerta novo.")

if __name__ == "__main__":
    run()