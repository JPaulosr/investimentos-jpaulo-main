# jobs/proventos_job.py
# -*- coding: utf-8 -*-
import os
import sys
import json
import hashlib
import time
import re  # Import necessário para limpar R$
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials
import requests
from collections import defaultdict

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
ABA_POSICOES = "posicoes_snapshot"
MAX_DAYS_ALERT = 120 

def _get_client():
    if not GCP_JSON: raise RuntimeError("❌ GCP_SERVICE_ACCOUNT_JSON não definido.")
    info = json.loads(GCP_JSON)
    if "private_key" in info: info["private_key"] = info["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    return gspread.authorize(creds)

def _send_telegram(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID: return
    try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"})
    except: pass

def _generate_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def _safe_float(val):
    """Converte R$ 1.234,56 ou 1,23 para float de forma agressiva"""
    if val is None: return 0.0
    s = str(val).strip()
    if not s: return 0.0
    # Remove R$ e espaços
    s = re.sub(r'[^\d,-]', '', s) # Mantém apenas números, vírgula e hífen (negativo)
    try:
        return float(s.replace(',', '.'))
    except:
        return 0.0

def _normalize_date(val):
    s = str(val).strip()
    if not s: return ""
    try: return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%d")
    except: pass
    try: return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except: return s 

def _safe_get_records(ws):
    """Lê planilha normalizando cabeçalhos (remove espaços extras)"""
    try:
        vals = ws.get_all_values()
        if not vals: return []
        headers = [str(h).strip() for h in vals[0]] # Limpa cabeçalhos
        data = vals[1:]
        out = []
        for row in data:
            item = {}
            for i, h in enumerate(headers):
                if h: # Ignora coluna sem nome
                    val = row[i] if i < len(row) else ""
                    item[h] = val
            out.append(item)
        return out
    except Exception as e:
        print(f"⚠️ Erro ao ler planilha: {e}")
        return []

def _get_carteira_qtd(sh):
    carteira = {}
    try:
        try: ws = sh.worksheet(ABA_POSICOES)
        except: ws = sh.worksheet(ABA_ATIVOS)
        records = _safe_get_records(ws)
        for r in records:
            tk = str(r.get('ticker') or r.get('ativo') or '').strip().upper()
            qtd = _safe_float(r.get('quantidade') or r.get('qtd') or r.get('saldo') or 0)
            if tk and qtd > 0: carteira[tk] = qtd
    except: pass
    return carteira

def run():
    print("🚀 Iniciando Robô (Versão Anti-Duplicidade R$)...")
    if not SHEET_ID: raise RuntimeError("❌ SHEET_ID vazio.")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)
    ws_anunciados = sh.worksheet(ABA_ANUNCIADOS)
    ws_ativos = sh.worksheet(ABA_ATIVOS)
    try: ws_logs = sh.worksheet(ABA_LOGS)
    except: ws_logs = sh.add_worksheet(ABA_LOGS, rows=1000, cols=5)

    carteira_qtd = _get_carteira_qtd(sh)
    hoje_iso = datetime.now().strftime("%Y-%m-%d")
    
    pagamentos_hoje_queue = []
    novos_anuncios_dict = defaultdict(list)

    # 1. Carrega Base Existente
    print("📋 Lendo registros salvos...")
    exist_records = _safe_get_records(ws_anunciados)
    
    # DEBUG: Mostra o primeiro registro para conferência
    if exist_records:
        print(f"🔎 DEBUG LEITURA (1º Item): {exist_records[0]}")
    
    existing_keys = set()
    
    for r in exist_records:
        val = _safe_float(r.get('valor_por_cota'))
        tk = str(r.get('ticker')).strip().upper()
        dp = _normalize_date(r.get('data_pagamento'))
        
        # Chave estrita
        key = f"{tk}|{dp}|{val:.4f}"
        existing_keys.add(key)
        
        # Check Pagamento Hoje
        if dp == hoje_iso and val > 0:
            _processa_pagamento_hoje(tk, val, r.get('quantidade_ref'), carteira_qtd, pagamentos_hoje_queue)

    print(f"✅ {len(existing_keys)} registros únicos carregados.")

    # 2. Fetch Novos
    ativos_raw = _safe_get_records(ws_ativos)
    tickers = list(set([str(r['ticker']).strip().upper() for r in ativos_raw if r.get('ticker')]))
    rows_to_save = []
    
    print(f"🔎 Verificando {len(tickers)} ativos...")
    for t in tickers:
        try:
            res = fetch_provento_anunciado(t)
            for item in res:
                val = _safe_float(item.get('valor_por_cota'))
                if val <= 0: continue
                
                tk = str(item.get('ticker')).upper()
                tp = str(item.get('tipo_pagamento', 'RENDIMENTO')).upper()
                dc = str(item.get('data_com', ''))
                dp_norm = _normalize_date(item.get('data_pagamento', ''))
                url = str(item.get('fonte_url', ''))
                
                # Chave de comparação
                check_key = f"{tk}|{dp_norm}|{val:.4f}"
                
                if check_key not in existing_keys:
                    print(f"✨ NOVIDADE REAL: {tk} {val} ({dp_norm})")
                    rows_to_save.append([tk, "", "ANUNCIADO", tp, dc, dp_norm, val, "", url, datetime.now().strftime("%Y-%m-%d %H:%M"), "Robô GitHub"])
                    existing_keys.add(check_key)
                    
                    if dp_norm == hoje_iso:
                        _processa_pagamento_hoje(tk, val, 0, carteira_qtd, pagamentos_hoje_queue)
                    else:
                        try:
                            dias = (datetime.strptime(dp_norm, "%Y-%m-%d").date() - datetime.now().date()).days
                            if 0 <= dias <= MAX_DAYS_ALERT:
                                novos_anuncios_dict[tk].append({'val': val, 'dp': dp_norm, 'tp': tp})
                        except:
                            novos_anuncios_dict[tk].append({'val': val, 'dp': dp_norm, 'tp': tp})
            time.sleep(0.5)
        except Exception as e:
            print(f"⚠️ {t}: {e}")

    # 3. Persistência
    if rows_to_save:
        print(f"💾 Salvando {len(rows_to_save)} novos...")
        ws_anunciados.append_rows(rows_to_save, value_input_option="USER_ENTERED")
    else:
        print("✅ Base atualizada. Nenhuma alteração.")

    # 4. Alertas
    logs_existentes = _safe_get_records(ws_logs)
    hashes_enviados = set(str(r.get('event_hash')) for r in logs_existentes)
    logs_to_append = []

    for msg, tk in pagamentos_hoje_queue:
        ph_hash = _generate_hash(msg + hoje_iso)
        if ph_hash not in hashes_enviados:
            _send_telegram(msg)
            logs_to_append.append([datetime.now().strftime("%Y-%m-%d %H:%M"), ph_hash, tk, "PAGAMENTO_HOJE", "Enviado"])

    if novos_anuncios_dict:
        lines = []
        for tk, itens in novos_anuncios_dict.items():
            if len(itens) > 2:
                vals = [i['val'] for i in itens]
                datas = sorted([i['dp'] for i in itens])
                lines.append(f"• <b>{tk}</b>: {len(itens)} eventos\n   R$ {min(vals):,.2f} a R$ {max(vals):,.2f}\n   Pag: {datas[0]} até {datas[-1]}")
            else:
                for i in itens:
                    lines.append(f"• <b>{tk}</b> ({i['tp']}): R$ {i['val']:,.2f} | Pag: {i['dp']}")
        
        if lines:
            full_msg = "📢 <b>Novos Proventos Anunciados</b>\n\n" + "\n".join(lines)
            msg_hash = _generate_hash(full_msg)
            if msg_hash not in hashes_enviados:
                _send_telegram(full_msg)
                logs_to_append.append([datetime.now().strftime("%Y-%m-%d %H:%M"), msg_hash, "LOTE", "ANUNCIO", "Resumo"])

    if logs_to_append:
        ws_logs.append_rows(logs_to_append, value_input_option="USER_ENTERED")

def _processa_pagamento_hoje(tk, val, qtd_ref_raw, carteira, queue):
    try:
        qtd_ref = _safe_float(qtd_ref_raw)
        fonte = "Qtd Congelada"
        if qtd_ref <= 0:
            qtd_ref = carteira.get(tk, 0.0)
            fonte = "Posição Atual"
        
        if qtd_ref > 0:
            total = qtd_ref * val
            msg = (f"💰 <b>Pagamento Hoje: {tk}</b>\nQtd: {qtd_ref:g}\nValor/cota: R$ {val:,.2f}\n<b>Total: R$ {total:,.2f}</b>\n<i>({fonte})</i>")
            queue.append((msg, tk))
    except: pass

if __name__ == "__main__":
    run()