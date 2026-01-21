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
from collections import defaultdict

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
ABA_POSICOES = "posicoes_snapshot"

MAX_DAYS_ALERT = 120 

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

def _normalize_date(val):
    """
    RÉGUA DA PÁGINA MANUAL:
    Garante formato YYYY-MM-DD para comparação exata com o que a página salva.
    """
    s = str(val).strip()
    if not s: return ""
    try:
        # Se vier 31/01/2026 (formato PT-BR do Sheets/Streamlit)
        return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%d")
    except:
        pass
    try:
        # Se já vier 2026-01-31 (formato ISO)
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        return s 

def _get_carteira_qtd(sh):
    carteira = {}
    try:
        try: ws = sh.worksheet(ABA_POSICOES)
        except: ws = sh.worksheet(ABA_ATIVOS)
        records = ws.get_all_records()
        for r in records:
            tk = str(r.get('ticker') or r.get('ativo') or '').strip().upper()
            qtd = _safe_float(r.get('quantidade') or r.get('qtd') or r.get('saldo') or 0)
            if tk and qtd > 0:
                carteira[tk] = qtd
    except: pass
    return carteira

def run():
    print("🚀 Iniciando Robô (Sincronizado com Página Manual)...")
    if not SHEET_ID: raise RuntimeError("❌ SHEET_ID vazio.")

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)
    ws_anunciados = sh.worksheet(ABA_ANUNCIADOS)
    ws_ativos = sh.worksheet(ABA_ATIVOS)
    try: ws_logs = sh.worksheet(ABA_LOGS)
    except: ws_logs = sh.add_worksheet(ABA_LOGS, rows=1000, cols=5)

    carteira_qtd = _get_carteira_qtd(sh)
    hoje_date = datetime.now().date()
    hoje_iso = hoje_date.strftime("%Y-%m-%d")
    
    pagamentos_hoje_queue = []
    novos_anuncios_dict = defaultdict(list)

    # 1. Carrega Base Existente (Usando a Régua da Página)
    print("📋 Mapeando registros existentes...")
    exist_records = ws_anunciados.get_all_records()
    existing_keys = set()
    
    for r in exist_records:
        val = _safe_float(r.get('valor_por_cota'))
        tk = str(r.get('ticker')).strip().upper()
        # Normalização CRÍTICA: Mesma lógica da página manual
        dp = _normalize_date(r.get('data_pagamento'))
        
        # Chave única: Ticker + DataPagamento + Valor
        key = f"{tk}|{dp}|{val:.4f}"
        existing_keys.add(key)
        
        # Check Pagamento Hoje (Base Antiga)
        if dp == hoje_iso and val > 0:
            _processa_pagamento_hoje(tk, val, r.get('quantidade_ref'), carteira_qtd, pagamentos_hoje_queue)

    # 2. Fetch Novos
    ativos_raw = ws_ativos.get_all_records()
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
                # Normaliza a data que vem do site também
                dp_site = str(item.get('data_pagamento', ''))
                dp_norm = _normalize_date(dp_site) 
                
                url = str(item.get('fonte_url', ''))
                
                # AQUI ESTÁ A MÁGICA: Compara usando a mesma chave da página manual
                check_key = f"{tk}|{dp_norm}|{val:.4f}"
                
                if check_key not in existing_keys:
                    print(f"✨ NOVO: {tk} R$ {val} ({dp_norm})")
                    
                    # Salva nas colunas A-K (igual à página manual)
                    rows_to_save.append([
                        tk,                 # ticker
                        "",                 # tipo_ativo (vazio)
                        "ANUNCIADO",        # status
                        tp,                 # tipo_pagamento
                        dc,                 # data_com
                        dp_norm,            # data_pagamento (NORMALIZADA)
                        val,                # valor_por_cota
                        "",                 # quantidade_ref
                        url,                # fonte_url
                        datetime.now().strftime("%Y-%m-%d %H:%M"), # capturado_em
                        "Robô GitHub"       # fonte_nome
                    ])
                    existing_keys.add(check_key)
                    
                    # Check Pagamento Hoje (Base Nova)
                    if dp_norm == hoje_iso:
                        _processa_pagamento_hoje(tk, val, 0, carteira_qtd, pagamentos_hoje_queue)
                    else:
                        # Filtro de Ansiedade
                        try:
                            dt_pag_obj = datetime.strptime(dp_norm, "%Y-%m-%d").date()
                            dias_ate_pag = (dt_pag_obj - hoje_date).days
                            if 0 <= dias_ate_pag <= MAX_DAYS_ALERT:
                                novos_anuncios_dict[tk].append({'val': val, 'dp': dp_norm, 'tp': tp})
                        except:
                            novos_anuncios_dict[tk].append({'val': val, 'dp': dp_norm, 'tp': tp})
            time.sleep(0.5)
        except Exception as e:
            print(f"⚠️ {t}: {e}")

    # 3. Persistência
    if rows_to_save:
        print(f"💾 Salvando {len(rows_to_save)} registros...")
        ws_anunciados.append_rows(rows_to_save, value_input_option="USER_ENTERED")
    else:
        print("✅ Tudo sincronizado. Nenhuma novidade.")

    # 4. Alertas (Logs para não repetir envio)
    logs_existentes = ws_logs.get_all_records()
    hashes_enviados = set(str(r.get('event_hash')) for r in logs_existentes)
    logs_to_append = []

    # A) Pagamentos Hoje
    for msg, tk in pagamentos_hoje_queue:
        ph_hash = _generate_hash(msg + hoje_iso)
        if ph_hash not in hashes_enviados:
            _send_telegram(msg)
            logs_to_append.append([datetime.now().strftime("%Y-%m-%d %H:%M"), ph_hash, tk, "PAGAMENTO_HOJE", "Enviado"])
            time.sleep(0.5)

    # B) Novos Anúncios
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
            msg = (
                f"💰 <b>Pagamento Hoje: {tk}</b>\n"
                f"Qtd: {qtd_ref:g}\n"
                f"Valor/cota: R$ {val:,.2f}\n"
                f"<b>Total: R$ {total:,.2f}</b>\n"
                f"<i>({fonte})</i>"
            )
            queue.append((msg, tk))
    except: pass

if __name__ == "__main__":
    run()