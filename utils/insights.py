# ARQUIVO: utils/insights.py
# -*- coding: utf-8 -*-

import pandas as pd
from datetime import datetime, date

def to_date_secure(val):
    """
    Converte input para data de forma segura.
    Retorna None se inválido.
    """
    if val is None or str(val).strip() == '': 
        return None
    try:
        # Tenta converter com dia primeiro (Brasil)
        ts = pd.to_datetime(val, dayfirst=True, errors='coerce')
        if pd.isna(ts): 
            return None
        return ts.date()
    except:
        return None

def _to_float(val):
    """Converte string BR ou US para float seguro."""
    if val is None or val == "": return 0.0
    if isinstance(val, (int, float)): return float(val)
    
    s = str(val).strip()
    # Se tiver vírgula e ponto, assume padrão BR (1.000,00) -> remove ponto, troca vírgula
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    # Se só tiver vírgula (10,50), troca por ponto
    elif ',' in s:
        s = s.replace(',', '.')
    
    try:
        return float(s)
    except:
        return 0.0

def get_posicao_na_data(df_movimentacoes: pd.DataFrame, ticker: str, data_alvo) -> float:
    """
    Calcula posição na Data-Com.
    """
    data_alvo = to_date_secure(data_alvo)
    if not data_alvo: return 0.0
    if df_movimentacoes.empty: return 0.0

    df = df_movimentacoes.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    if 'ticker' not in df.columns or 'data' not in df.columns:
        return 0.0

    # Filtra Ticker
    df = df[df['ticker'].astype(str).str.upper().str.strip() == str(ticker).upper().strip()]
    
    # Datas
    df['data_clean'] = df['data'].apply(to_date_secure)
    df = df.dropna(subset=['data_clean'])
    
    # Filtra até Data-Com
    df_filtrado = df[df['data_clean'] <= data_alvo]
    
    qtd_acumulada = 0.0
    for _, row in df_filtrado.iterrows():
        tipo = str(row.get('tipo', '')).upper().strip()
        qtd = _to_float(row.get('quantidade', 0))
        
        if any(x in tipo for x in ['COMPRA', 'BONIF', 'SUBSCR']):
            qtd_acumulada += qtd
        elif 'VENDA' in tipo:
            qtd_acumulada -= qtd
            
    return max(0.0, qtd_acumulada)

def get_anuncios_pendentes(df_anuncios: pd.DataFrame, df_proventos: pd.DataFrame, df_movimentacoes: pd.DataFrame):
    """
    Lista proventos de HOJE ou PASSADO não lançados.
    """
    pendentes = []
    hoje = date.today()
    
    if df_anuncios.empty: return []

    # Prepara deduplicação
    if not df_proventos.empty:
        df_proventos.columns = [str(c).strip().lower() for c in df_proventos.columns]
        if 'data' in df_proventos.columns:
            df_proventos['data_clean'] = df_proventos['data'].apply(to_date_secure)
    
    df_anuncios.columns = [str(c).strip().lower() for c in df_anuncios.columns]

    for _, anuncio in df_anuncios.iterrows():
        # Filtro Básico
        status = str(anuncio.get('status', '')).upper()
        
        # REMOVIDO FILTRO DE 'ATIVO' QUE ESTAVA BLOQUEANDO FIIs
        if status != 'ANUNCIADO': 
            continue
            
        # Datas
        data_pag = to_date_secure(anuncio.get('data_pagamento'))
        data_com = to_date_secure(anuncio.get('data_com'))
        
        if not data_pag or not data_com: continue
        
        # --- REGRA: Só mostra se já venceu ou é hoje ---
        if data_pag > hoje: continue
        # -----------------------------------------------

        ticker = str(anuncio.get('ticker', '')).strip().upper()
        vpc = _to_float(anuncio.get('valor_por_cota', 0))

        # Verifica se já lançou
        ja_existe = False
        if not df_proventos.empty and 'ticker' in df_proventos.columns and 'data_clean' in df_proventos.columns:
            subset = df_proventos[
                (df_proventos['ticker'].astype(str).str.upper() == ticker) & 
                (df_proventos['data_clean'] == data_pag)
            ]
            for _, prov_lancado in subset.iterrows():
                v_lancado = _to_float(prov_lancado.get('valor_por_cota', 0))
                # Se ticker, data e valor baterem, ignora
                if abs(v_lancado - vpc) < 0.005:
                    ja_existe = True
                    break
        
        if ja_existe: continue 
            
        # Calcula Posição
        qtd = get_posicao_na_data(df_movimentacoes, ticker, data_com)
        
        if qtd > 0:
            pendentes.append({
                "ticker": ticker,
                "tipo": anuncio.get('tipo_pagamento', 'RENDIMENTO'),
                "data_pagamento": data_pag,
                "data_com": data_com,
                "valor_por_cota": vpc,
                "quantidade_ref": qtd,
                "valor_total": qtd * vpc,
                "fonte": str(anuncio.get('fonte_url', '')),
                "hash_key": f"{ticker}_{data_pag}_{vpc:.5f}" 
            })
            
    return pendentes