# jobs/proventos_job.py
# -*- coding: utf-8 -*-
import os
import sys
import json
import hashlib
import time
from datetime import datetime, date
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
SHEET_ID = (os.getenv("SHEET_ID_NOVO") or os.getenv("SHEET_ID") or "").strip()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GCP_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON")

ABA_ATIVOS = "ativos_master"
ABA_ANUNCIADOS = "proventos_anunciados"
ABA_LOGS = "alerts_log"
ABA_POSICOES = "posicoes_snapshot" # Onde lemos a quantidade

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

def _safe_float(val):
    try:
        return float(str(val).replace(',', '.'))
    except:
        return 0.0

def _get_carteira_qtd(sh):
    """
    Tenta ler a quantidade de cotas da aba 'posicoes_snapshot' ou 'ativos_master'.
    Retorna um dicionário: {'PETR4': 100.0, 'VALE3': 50.0}
    """
    carteira = {}
    try:
        # Tenta posicoes_snapshot primeiro (ideal)
        try:
            ws = sh.worksheet(ABA_POSICOES)
        except:
            # Fallback para ativos_master se tiver coluna de quantidade
            ws = sh.worksheet(ABA_ATIVOS)
            
        records = ws.get_all_records()
        for r in records:
            # Tenta achar a chave do ticker e da quantidade (pode variar o nome)
            tk = str(r.get('ticker') or r.get('ativo') or r.get('papel') or '').strip().upper()
            qtd = _safe_float(r.get('quantidade') or r.get('qtd') or r.get('saldo') or r.get('total') or 0)
            
            if tk and qtd > 0:
                carteira[tk] = qtd
                
        print(f"💰 Carteira carregada: {len(carteira)} ativos com saldo.")
    except Exception as e:
        print(f"⚠️ Não foi possível ler saldo da carteira: {e}")
        
    return carteira

def run():
    print("🚀 Iniciando Robô (Anti-Spam + Pagamento Hoje)...")
    
    if not SHEET_ID:
        raise RuntimeError("❌ ERRO: SHEET_ID vazio.")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)

    ws_anunciados = sh.worksheet(ABA_ANUNCIADOS)
    ws_ativos = sh.worksheet(ABA_ATIVOS)
    
    try:
        ws_logs = sh.worksheet(ABA_LOGS)
    except:
        ws_logs = sh.add_worksheet(ABA_LOGS, rows=1000, cols=5)
        ws_logs.append_row(["timestamp", "event_hash", "ticker", "tipo", "mensagem"])

    # 1. Carregar Carteira (Quantidades)
    carteira_qtd = _get_carteira_qtd(sh)

    # 2. Mapeamento de Existentes (Evitar Duplicatas)
    exist_records = ws_anunciados.get_all_records()
    existing_keys = set()
    
    # Prepara verificação de PAGAMENTO HOJE
    hoje_iso = datetime.now().strftime("%Y-%m-%d")
    pagamentos_hoje_queue = []

    for r in exist_records:
        val_float = _safe_float(r.get('valor_por_cota'))
        val_str = f"{val_float:.4f}"
        tk = str(r.get('ticker')).strip().upper()
        dp = str(r.get('data_pagamento'))
        
        # Chave única para o banco
        key = f"{tk}|{dp}|{val_str}"
        existing_keys.add(key)
        
        # --- LÓGICA DE PAGAMENTO HOJE ---
        # Se a data de pagamento for HOJE, calcula e prepara alerta
        if dp == hoje_iso and val_float > 0:
            qtd_ref = _safe_float(r.get('quantidade_ref', 0)) # Se tiver congelado
            if qtd_ref <= 0:
                qtd_ref = carteira_qtd.get(tk, 0.0) # Se não, pega da posição atual
                fonte_qtd = "Posição Atual"
            else:
                fonte_qtd = "Qtd Ref (Congelada)"
            
            if qtd_ref > 0:
                total_receber = qtd_ref * val_float
                msg_pag = (
                    f"💰 <b>Pagamento Hoje: {tk}</b>\n"
                    f"Qtd: {qtd_ref:g} cotas\n"
                    f"Valor/cota: R$ {val_float:,.2f}\n"
                    f"<b>Total: R$ {total_receber:,.2f}</b>\n"
                    f"<i>({fonte_qtd})</i>"
                )
                pagamentos_hoje_queue.append((msg_pag, tk))

    # 3. Fetch Novos Anúncios
    ativos_raw = ws_ativos.get_all_records()
    tickers = list(set([str(r['ticker']).strip().upper() for r in ativos_raw if r.get('ticker')]))
    print(f"🔎 Monitorando {len(tickers)} ativos.")

    rows_to_save = []
    novos_anuncios_lines = []
    
    for t in tickers:
        try:
            res = fetch_provento_anunciado(t)
            for item in res:
                val = _safe_float(item.get('valor_por_cota'))
                if val <= 0: continue
                
                tk = str(item.get('ticker')).upper()
                tp = str(item.get('tipo_pagamento', 'RENDIMENTO')).upper()
                dc = str(item.get('data_com', ''))
                dp = str(item.get('data_pagamento', ''))
                url = str(item.get('fonte_url', ''))
                
                val_check = f"{val:.4f}"
                check_key = f"{tk}|{dp}|{val_check}"
                
                if check_key not in existing_keys:
                    print(f"✨ NOVO: {tk} R$ {val}")
                    
                    # Colunas fixas (A-K)
                    new_row = [
                        tk, "", "ANUNCIADO", tp, dc, dp, val, 
                        "", url, datetime.now().strftime("%Y-%m-%d %H:%M"), "Robô GitHub"
                    ]
                    
                    rows_to_save.append(new_row)
                    existing_keys.add(check_key)
                    
                    # Adiciona ao resumo
                    novos_anuncios_lines.append(f"• <b>{tk}</b> ({tp}): R$ {val:,.2f} | Pag: {dp}")

            time.sleep(0.5)
        except Exception as e:
            print(f"⚠️ Erro ao ler {t}: {e}")

    # 4. Salvar Novos
    if rows_to_save:
        print(f"💾 Salvando {len(rows_to_save)} novos registros...")
        ws_anunciados.append_rows(rows_to_save, value_input_option="USER_ENTERED")

    # 5. Enviar Alertas (Logica de Logs para não repetir)
    logs_existentes = ws_logs.get_all_records()
    hashes_enviados = set(str(r.get('event_hash')) for r in logs_existentes)
    logs_to_append = []

    # A) Alertas de Novos Anúncios (Agrupado)
    if novos_anuncios_lines:
        header = "📢 <b>Novos Proventos Anunciados</b>\n\n"
        body = "\n".join(novos_anuncios_lines)
        full_msg = header + body
        msg_hash = _generate_hash(full_msg)
        
        if msg_hash not in hashes_enviados:
            _send_telegram(full_msg)
            logs_to_append.append([datetime.now().strftime("%Y-%m-%d %H:%M"), msg_hash, "LOTE", "ANUNCIO", "Resumo Enviado"])
            print("📢 Resumo de anúncios enviado.")

    # B) Alertas de Pagamento Hoje (Individual e Detalhado)
    for msg, tk in pagamentos_hoje_queue:
        ph_hash = _generate_hash(msg + datetime.now().strftime("%Y-%m-%d")) # Hash único do dia
        
        if ph_hash not in hashes_enviados:
            _send_telegram(msg)
            logs_to_append.append([datetime.now().strftime("%Y-%m-%d %H:%M"), ph_hash, tk, "PAGAMENTO_HOJE", "Enviado"])
            print(f"💰 Aviso de pagamento enviado para {tk}")
            time.sleep(0.5)

    # Salva logs
    if logs_to_append:
        ws_logs.append_rows(logs_to_append, value_input_option="USER_ENTERED")

if __name__ == "__main__":
    run()