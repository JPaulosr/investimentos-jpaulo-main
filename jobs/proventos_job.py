# jobs/proventos_job.py
# -*- coding: utf-8 -*-
import os
import sys
import json
import hashlib
import time
from datetime import datetime
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests

# --- MAGIA AQUI: Adiciona a pasta raiz para conseguir importar seus utils ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importa a SUA função existente (reaproveitamento total)
try:
    from utils.proventos_fetch import fetch_provento_anunciado
    print("✅ Módulo utils.proventos_fetch carregado com sucesso.")
except ImportError:
    raise RuntimeError("❌ Não encontrei utils/proventos_fetch.py. Verifique a estrutura de pastas.")

# ==========================================
# CONFIGURAÇÃO
# ==========================================
SHEET_ID = os.getenv("SHEET_ID_NOVO")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GCP_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON")

ABA_ATIVOS = "ativos_master"
ABA_ANUNCIADOS = "proventos_anunciados"
ABA_LOGS = "alerts_log"

def _get_client():
    if not GCP_JSON:
        raise RuntimeError("❌ Fatal: GCP_SERVICE_ACCOUNT_JSON não encontrado.")
    info = json.loads(GCP_JSON)
    if "private_key" in info:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

def _send_telegram(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"⚠️ Erro Telegram: {e}")

def _generate_hash(texto):
    return hashlib.md5(texto.encode('utf-8')).hexdigest()

def run():
    print("🚀 Iniciando Robô Diário (Integrado ao utils existente)...")
    
    if not SHEET_ID:
        raise RuntimeError("❌ SHEET_ID_NOVO não definido.")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)
    
    # 1. Preparar Abas
    try:
        ws_anunciados = sh.worksheet(ABA_ANUNCIADOS)
        ws_ativos = sh.worksheet(ABA_ATIVOS)
    except:
        raise RuntimeError(f"❌ Abas {ABA_ANUNCIADOS} ou {ABA_ATIVOS} não existem.")

    try:
        ws_logs = sh.worksheet(ABA_LOGS)
    except:
        ws_logs = sh.add_worksheet(ABA_LOGS, rows=1000, cols=5)
        ws_logs.append_row(["timestamp", "event_hash", "ticker", "tipo", "mensagem"])

    # 2. Ler Ativos
    print("📋 Lendo carteira...")
    ativos_records = ws_ativos.get_all_records()
    tickers = sorted(list(set([str(r['ticker']).strip().upper() for r in ativos_records if r.get('ticker')])))
    print(f"🔎 Monitorando {len(tickers)} ativos.")

    # 3. Ler Proventos Existentes (Cache)
    exist_records = ws_anunciados.get_all_records()
    existing_keys = set()
    for r in exist_records:
        # Chave única: TICKER + TIPO + DATA_COM + DATA_PAG + VALOR
        key = f"{r.get('ticker')}_{r.get('tipo_pagamento')}_{r.get('data_com')}_{r.get('data_pagamento')}_{r.get('valor_por_cota')}"
        existing_keys.add(key)

    # 4. Ler Logs de Alerta
    log_records = ws_logs.get_all_records()
    sent_hashes = set(str(r.get('event_hash')) for r in log_records)

    new_db_rows = []
    new_log_rows = []
    alerts_queue = []

    # 5. Loop de Busca (USANDO SEU UTILS)
    logs_captura = [] # Lista dummy para passar para sua função
    
    for ticker in tickers:
        try:
            # AQUI ESTÁ A LIGAÇÃO DIRETA COM SEU CÓDIGO EXISTENTE
            # Sua função retorna uma lista de dicionários já formatados
            resultados = fetch_provento_anunciado(ticker, logs=logs_captura)
            
            for item in resultados:
                # Seus utils retornam chaves como: 'ticker', 'tipo_pagamento', 'data_com', 'data_pagamento', 'valor_por_cota'
                p_ticker = str(item.get('ticker')).upper()
                p_tipo = str(item.get('tipo_pagamento')).upper()
                p_dt_com = str(item.get('data_com'))
                p_dt_pag = str(item.get('data_pagamento'))
                p_val = item.get('valor_por_cota')
                
                try:
                    val_float = float(p_val)
                except:
                    continue

                if val_float <= 0: continue

                # Cria chave para verificar se já existe
                db_key = f"{p_ticker}_{p_tipo}_{p_dt_com}_{p_dt_pag}_{p_val}"
                
                if db_key not in existing_keys:
                    print(f"✨ NOVIDADE: {p_ticker} R$ {val_float}")
                    
                    # Prepara linha para Google Sheets
                    # Ordem: ticker, status, tipo_pagamento, data_com, data_pagamento, valor_por_cota, fonte_url, fonte_nome, capturado_em
                    row_gs = [
                        p_ticker,
                        "ANUNCIADO",
                        p_tipo,
                        p_dt_com,
                        p_dt_pag,
                        val_float,
                        item.get('fonte_url', ''),
                        "Robô Auto (GitHub)",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ]
                    new_db_rows.append(row_gs)
                    existing_keys.add(db_key)

                    # Prepara Alerta Telegram
                    msg = (
                        f"💰 <b>Novo Provento Detectado</b>\n\n"
                        f"🔹 <b>{p_ticker}</b> ({p_tipo})\n"
                        f"💵 <b>R$ {val_float:,.2f}</b>\n"
                        f"📅 Data Com: {p_dt_com}\n"
                        f"📅 Pagamento: {p_dt_pag}"
                    )
                    ev_hash = _generate_hash(msg)
                    
                    if ev_hash not in sent_hashes:
                        alerts_queue.append((msg, ev_hash, p_ticker))
                        sent_hashes.add(ev_hash)
            
            # Pequena pausa para não bloquear IP dos sites
            time.sleep(1.0)

        except Exception as e:
            print(f"⚠️ Erro {ticker}: {e}")

    # 6. Salvar e Avisar
    if new_db_rows:
        print(f"💾 Salvando {len(new_db_rows)} novos registros...")
        ws_anunciados.append_rows(new_db_rows, value_input_option="USER_ENTERED")
    
    if alerts_queue:
        print(f"📢 Enviando {len(alerts_queue)} alertas...")
        for msg, h, tick in alerts_queue:
            _send_telegram(msg)
            new_log_rows.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), h, tick, "ALERTA", "Enviado"])
            time.sleep(1)
        ws_logs.append_rows(new_log_rows, value_input_option="USER_ENTERED")
    else:
        print("🤫 Nenhum alerta novo hoje.")

if __name__ == "__main__":
    run()