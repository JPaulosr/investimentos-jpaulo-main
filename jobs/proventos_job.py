# jobs/proventos_job.py
# -*- coding: utf-8 -*-
"""
ROBÔ PROVENTOS — MODO DEBUG / FORÇA BRUTA
Salva linha a linha para garantir que erros de formatação não derrubem o lote.
"""

from __future__ import annotations

import os
import json
import re
import hashlib
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials

# =============================================================================
# CONFIGURAÇÕES (ENV)
# =============================================================================
SHEET_ID = (os.getenv("SHEET_ID") or os.getenv("SHEET_ID_NOVO") or "").strip()
GCP_JSON = (os.getenv("GCP_SERVICE_ACCOUNT_JSON") or "").strip()
TICKERS_ENV = (os.getenv("TICKERS") or "").strip()

# Se não tiver tickers no env, usa estes para teste forçado (ajuste se quiser)
if not TICKERS_ENV:
    TICKERS_ENV = "PETR4,VALE3,XPML11"

ABA_ANUNCIADOS = "proventos_anunciados"

if not SHEET_ID or not GCP_JSON:
    raise RuntimeError("❌ ERRO: Faltam SHEET_ID ou GCP_SERVICE_ACCOUNT_JSON no .env/Secrets")

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================
def _now_iso_min() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")

def _norm_ticker(s: Any) -> str:
    if not s: return ""
    return re.sub(r"[^A-Z0-9]", "", str(s).strip().upper())

def _norm_date(s: Any) -> str:
    """Força data para string YYYY-MM-DD ou vazio"""
    if not s: return ""
    st = str(s).strip()
    if len(st) >= 10:
        # Tenta pegar só a parte da data se vier iso com hora
        try:
            return st[:10] 
        except:
            pass
    return st

def _norm_float(v: Any) -> Optional[float]:
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    st = str(v).strip().replace("R$", "").strip()
    if not st: return None
    st = st.replace(",", ".") # simplificado
    try:
        return float(st)
    except:
        return None

def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def event_id_from_row(row: Dict[str, Any]) -> str:
    # ID único: Ticker + Tipo + Data Com
    key = "|".join([
        _norm_ticker(row.get("ticker", "")),
        str(row.get("tipo_pagamento", "")).strip().upper(),
        _norm_date(row.get("data_com", "")),
    ])
    return _sha1(key)

# =============================================================================
# GOOGLE SHEETS E FETCH
# =============================================================================
def _get_client():
    info = json.loads(GCP_JSON)
    if "private_key" in info:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def fetch_events_wrapper():
    """Tenta carregar o fetcher do seu projeto"""
    try:
        from utils.proventos_fetch import fetch_provento_anunciado
        print("✅ Fetcher importado com sucesso.")
    except ImportError:
        print("❌ ERRO: Não achei 'utils/proventos_fetch.py'.")
        return []

    tickers = [t.strip() for t in TICKERS_ENV.split(",") if t.strip()]
    all_data = []
    
    print(f"🔎 Buscando dados para: {tickers}")
    for t in tickers:
        try:
            # Tenta chamar (compatibilidade com versões diferentes do seu fetcher)
            try:
                res = fetch_provento_anunciado(t, logs=None)
            except TypeError:
                res = fetch_provento_anunciado(t)
            
            if res:
                print(f"   -> {t}: Encontrados {len(res)} registros.")
                for r in res:
                    # Garante ticker preenchido
                    r = dict(r)
                    if not r.get("ticker"): r["ticker"] = t
                    all_data.append(r)
            else:
                print(f"   -> {t}: Nenhum registro encontrado.")
        except Exception as e:
            print(f"   -> {t}: Erro ao buscar ({e})")
            
    return all_data

# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================
def run():
    print("🚀 INICIANDO MODO DE GRAVAÇÃO FORÇADA...", flush=True)
    
    gc = _get_client()
    try:
        sh = gc.open_by_key(SHEET_ID)
        print(f"📂 Planilha aberta: {sh.title}")
    except Exception as e:
        print(f"❌ ERRO CRÍTICO: Não consegui abrir a planilha. ID está certo? {e}")
        return

    # 1. Tenta pegar ou criar a aba
    try:
        ws = sh.worksheet(ABA_ANUNCIADOS)
        print(f"📄 Aba '{ABA_ANUNCIADOS}' encontrada.")
    except:
        print(f"⚠️ Aba '{ABA_ANUNCIADOS}' não existe. Criando...")
        ws = sh.add_worksheet(ABA_ANUNCIADOS, rows=1000, cols=20)

    # 2. Verifica Header
    header_esperado = [
        "ticker", "tipo_ativo", "status", "tipo_pagamento", "data_com", 
        "data_pagamento", "valor_por_cota", "quantidade_ref", "fonte_url", 
        "capturado_em", "event_id", "ativo"
    ]
    
    vals = ws.get_all_values()
    if not vals:
        print("📝 Planilha vazia. Criando cabeçalho...")
        ws.append_row(header_esperado)
        vals = [header_esperado]
    
    header_atual = vals[0]
    # Mapeia colunas: ex: 'ticker' está na coluna 1 (indice 0)
    col_map = {name.lower().strip(): i for i, name in enumerate(header_atual)}
    
    # Adiciona colunas faltantes se precisar
    for req in header_esperado:
        if req not in col_map:
            print(f"⚠️ Coluna '{req}' faltando. Adicionando...")
            # Adiciona no final
            header_atual.append(req)
            ws.update("1:1", [header_atual])
            col_map[req] = len(header_atual) - 1

    # 3. TESTE DE ESCRITA (CRUCIAL)
    try:
        print("✍️ Testando permissão de escrita na célula Z1...")
        ws.update_acell("Z1", "TESTE_OK")
        print("✅ Permissão de escrita OK.")
    except Exception as e:
        print(f"❌ ERRO DE PERMISSÃO: Não consigo escrever na planilha. {e}")
        return

    # 4. Mapeia IDs já existentes para não duplicar
    existing_ids = set()
    idx_event_id = col_map.get("event_id")
    
    if idx_event_id is not None:
        # Pula header (linha 1)
        for row in vals[1:]:
            if len(row) > idx_event_id:
                existing_ids.add(str(row[idx_event_id]).strip())

    print(f"📚 {len(existing_ids)} eventos já existem na base.")

    # 5. Busca e Salva
    novos_dados = fetch_events_wrapper()
    salvos_count = 0

    for item in novos_dados:
        # Normaliza dados
        row_dict = {
            "ticker": _norm_ticker(item.get("ticker")),
            "tipo_ativo": str(item.get("tipo_ativo", "")),
            "status": "ANUNCIADO",
            "tipo_pagamento": str(item.get("tipo_pagamento", "")).upper(),
            "data_com": _norm_date(item.get("data_com")),
            "data_pagamento": _norm_date(item.get("data_pagamento")),
            "valor_por_cota": _norm_float(item.get("valor_por_cota")),
            "quantidade_ref": str(item.get("quantidade_ref", "")),
            "fonte_url": str(item.get("fonte_url", "")),
            "capturado_em": _now_iso_min(),
        }
        
        # Gera ID
        eid = event_id_from_row(row_dict)
        row_dict["event_id"] = eid
        row_dict["ativo"] = 1

        # Validação básica
        if not row_dict["ticker"] or not row_dict["data_com"]:
            print(f"⚠️ Pulei registro inválido: {row_dict}")
            continue

        if eid in existing_ids:
            # Se quiser atualizar (UPDATE), a lógica seria aqui.
            # Por enquanto vamos focar em INSERIR O QUE FALTA.
            print(f"⏭️ Já existe: {row_dict['ticker']} - {row_dict['data_com']}")
            continue

        # Prepara a linha (lista) baseada na ordem do header da planilha
        final_row = [""] * len(header_atual)
        for field, val in row_dict.items():
            if field in col_map:
                idx = col_map[field]
                # Converte float para string com ponto ou vírgula conforme excel prefere,
                # mas gspread lida bem com float nativo Python.
                final_row[idx] = val if val is not None else ""

        # GRAVAÇÃO LINHA A LINHA (Blindada)
        try:
            print(f"💾 Salvando NOVO: {row_dict['ticker']} | {row_dict['tipo_pagamento']} | {row_dict['data_com']} ... ", end="")
            ws.append_row(final_row, value_input_option="USER_ENTERED")
            print("OK!")
            salvos_count += 1
            existing_ids.add(eid)
            time.sleep(1) # Delay anti-spam da API do Google
        except Exception as e:
            print(f"❌ FALHA AO SALVAR LINHA: {e}")

    print("-" * 30)
    print(f"🏁 FIM. Total salvo nesta execução: {salvos_count}")

if __name__ == "__main__":
    run()