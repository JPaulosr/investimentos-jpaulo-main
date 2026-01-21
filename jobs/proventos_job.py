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

# --- CONFIGURAÇÃO ---
# Tenta ler SHEET_ID_NOVO (Prioridade)
SHEET_ID = (os.getenv("SHEET_ID_NOVO") or os.getenv("SHEET_ID") or "").strip()

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
    print("🚀 Iniciando Robô (Versão Anti-Spam & Colunas Corrigidas)...")
    
    if not SHEET_ID:
        raise RuntimeError("❌ ERRO: SHEET_ID vazio.")

    gc = _get_client()
    try:
        sh = gc.open_by_key(SHEET_ID)
    except Exception as e:
        print(f"❌ Erro ao abrir planilha {SHEET_ID}: {e}")
        raise e

    # Carrega abas
    ws_anunciados = sh.worksheet(ABA_ANUNCIADOS)
    ws_ativos = sh.worksheet(ABA_ATIVOS)
    
    try:
        ws_logs = sh.worksheet(ABA_LOGS)
    except:
        ws_logs = sh.add_worksheet(ABA_LOGS, rows=1000, cols=5)
        ws_logs.append_row(["timestamp", "event_hash", "ticker", "tipo", "mensagem"])

    # 1. Mapeamento de Existentes (Para não duplicar)
    print("📋 Lendo base atual para evitar duplicatas...")
    exist_records = ws_anunciados.get_all_records()
    existing_keys = set()
    
    for r in exist_records:
        # Cria chave única: TICKER + DATA_PAG + VALOR
        # Arredondamos o valor para evitar erro de float (0.10 vs 0.1)
        try:
            val_float = float(str(r.get('valor_por_cota', 0)).replace(',','.'))
            val_str = f"{val_float:.4f}"
        except:
            val_str = "0.0000"
            
        key = f"{str(r.get('ticker')).strip().upper()}|{str(r.get('data_pagamento'))}|{val_str}"
        existing_keys.add(key)

    # 2. Ler Ativos para Monitorar
    ativos_raw = ws_ativos.get_all_records()
    tickers = list(set([str(r['ticker']).strip().upper() for r in ativos_raw if r.get('ticker')]))
    print(f"🔎 Monitorando {len(tickers)} ativos.")

    # Listas de execução
    rows_to_save = []
    telegram_lines = []
    log_rows = []
    
    # 3. Busca (Fetch)
    for t in tickers:
        try:
            res = fetch_provento_anunciado(t)
            for item in res:
                # Normalização
                val = float(item.get('valor_por_cota', 0))
                if val <= 0: continue
                
                tk = str(item.get('ticker')).upper()
                tp = str(item.get('tipo_pagamento', 'RENDIMENTO')).upper()
                dc = str(item.get('data_com', ''))
                dp = str(item.get('data_pagamento', ''))
                url = str(item.get('fonte_url', ''))
                
                # Chave para checar duplicidade
                val_check = f"{val:.4f}"
                check_key = f"{tk}|{dp}|{val_check}"
                
                if check_key not in existing_keys:
                    print(f"✨ NOVO: {tk} R$ {val}")
                    
                    # --- CORREÇÃO DAS COLUNAS (A até K) ---
                    # Ordem baseada na sua planilha:
                    # ticker, tipo_ativo, status, tipo_pagamento, data_com, data_pagamento, valor_por_cota, qtd_ref, fonte_url, capturado_em, fonte_nome
                    new_row = [
                        tk,                 # A: ticker
                        "",                 # B: tipo_ativo (deixa vazio)
                        "ANUNCIADO",        # C: status
                        tp,                 # D: tipo_pagamento
                        dc,                 # E: data_com
                        dp,                 # F: data_pagamento
                        val,                # G: valor_por_cota
                        "",                 # H: quantidade_ref
                        url,                # I: fonte_url
                        datetime.now().strftime("%Y-%m-%d %H:%M"), # J: capturado_em
                        "Robô GitHub"       # K: fonte_nome
                    ]
                    
                    rows_to_save.append(new_row)
                    existing_keys.add(check_key) # Adiciona no set para não duplicar no mesmo loop
                    
                    # Adiciona ao resumo do Telegram
                    telegram_lines.append(f"• <b>{tk}</b> ({tp}): R$ {val:,.2f} | Pag: {dp}")

            time.sleep(0.5) # Respeito aos sites
        except Exception as e:
            print(f"⚠️ Erro ao ler {t}: {e}")

    # 4. Salvar em Lote (Batch Save)
    if rows_to_save:
        print(f"💾 Salvando {len(rows_to_save)} novos registros...")
        ws_anunciados.append_rows(rows_to_save, value_input_option="USER_ENTERED")
    else:
        print("✅ Nada novo para salvar.")

    # 5. Enviar Telegram Agrupado (Uma única mensagem)
    if telegram_lines:
        print(f"📢 Enviando resumo com {len(telegram_lines)} itens...")
        
        header = "💰 <b>Novos Proventos Detectados</b>\n\n"
        body = "\n".join(telegram_lines)
        full_msg = header + body
        
        # Gera hash da mensagem inteira para evitar reenvio do mesmo bloco
        msg_hash = _generate_hash(full_msg)
        
        # Verifica logs de envio
        logs_existentes = ws_logs.get_all_records()
        hashes_enviados = set(str(r.get('event_hash')) for r in logs_existentes)
        
        if msg_hash not in hashes_enviados:
            _send_telegram(full_msg)
            
            # Registra log
            ws_logs.append_row([
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                msg_hash,
                "LOTE_DIARIO",
                "RESUMO",
                f"Enviado resumo com {len(telegram_lines)} itens"
            ])
        else:
            print("🔕 Resumo idêntico já enviado anteriormente. Ignorando.")
    else:
        print("🤫 Nenhum alerta para enviar.")

if __name__ == "__main__":
    run()