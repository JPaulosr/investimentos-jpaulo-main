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
    print("✅ Utils carregado.")
except ImportError:
    print("⚠️ Utils não encontrado (Mock ativo).")
    def fetch_provento_anunciado(t, logs=None): return []

# --- CONFIG ---
SHEET_ID = os.getenv("SHEET_ID_NOVO")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GCP_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON")

ABA_ATIVOS = "ativos_master"
ABA_ANUNCIADOS = "proventos_anunciados"
ABA_LOGS = "alerts_log"

def _get_client_and_debug():
    if not GCP_JSON:
        raise RuntimeError("❌ GCP_JSON vazio.")
    
    info = json.loads(GCP_JSON)
    
    # --- DEBUG: QUEM SOU EU? ---
    email_robo = info.get("client_email", "DESCONHECIDO")
    project_id = info.get("project_id", "DESCONHECIDO")
    print(f"\n🕵️‍♂️ --- MODO DETETIVE ---")
    print(f"🤖 O Robô diz: 'Eu sou o email: {email_robo}'")
    print(f"🏗️ Projeto Google: {project_id}")
    print(f"-----------------------\n")
    # ---------------------------

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
    print("🚀 Iniciando Robô...")
    
    # --- DEBUG: O QUE ESTOU TENTANDO ABRIR? ---
    if not SHEET_ID:
        raise RuntimeError("❌ SHEET_ID_NOVO não configurado.")
    
    # Colocamos aspas simples ao redor para ver se tem espaço em branco
    print(f"📄 Tentando abrir planilha com ID: '{SHEET_ID}'")
    
    gc = _get_client_and_debug()
    
    try:
        sh = gc.open_by_key(SHEET_ID.strip()) # O .strip() remove espaços se houver
        print("✅ SUCESSO! Conexão com planilha estabelecida.")
    except Exception as e:
        print(f"\n❌ FALHA AO ABRIR PLANILHA.")
        print(f"ERRO: {e}")
        print("👉 Verifique se o email que apareceu acima no 'MODO DETETIVE' está na lista de compartilhamento.")
        raise e

    # O resto do código segue normal se conectar...
    try:
        ws_anunciados = sh.worksheet(ABA_ANUNCIADOS)
        ws_ativos = sh.worksheet(ABA_ATIVOS)
    except:
        raise RuntimeError(f"Abas {ABA_ANUNCIADOS} ou {ABA_ATIVOS} não encontradas.")
        
    try:
        ws_logs = sh.worksheet(ABA_LOGS)
    except:
        ws_logs = sh.add_worksheet(ABA_LOGS, rows=1000, cols=5)
        ws_logs.append_row(["timestamp", "event_hash", "ticker", "tipo", "mensagem"])

    # Lógica simplificada de execução
    print("📋 Lendo ativos...")
    ativos = ws_ativos.get_all_records()
    tickers = list(set([str(r['ticker']).strip().upper() for r in ativos if r.get('ticker')]))
    print(f"🔎 {len(tickers)} ativos para verificar.")

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
                
                # Normaliza dados
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
            print(f"Erro {t}: {e}")

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

if __name__ == "__main__":
    run()