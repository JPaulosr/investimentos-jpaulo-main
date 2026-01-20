# utils/calendar_engine.py
# -*- coding: utf-8 -*-

import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def _to_float(v):
    try:
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace("R$", "").replace("%", "").strip()
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        return float(s)
    except:
        return 0.0

def build_calendar(
    prov_norm: pd.DataFrame,
    positions_enriched: pd.DataFrame,
    prov_anunciados: pd.DataFrame = None,  # <--- Aceita dados do Robô
    window_past_days: int = 365,
    window_future_days: int = 365,
    include_estimates: bool = True
) -> pd.DataFrame:
    """
    Constrói um calendário unificado com:
    1. Histórico (Pago) - Baseado em 'proventos'
    2. Anunciado (Confirmado) - Baseado em 'proventos_anunciados' (Robô)
    3. Estimado (Projeção) - Baseado no histórico do ano anterior
    """
    
    events = []
    hoje = datetime.now().date()
    start_date = hoje - timedelta(days=window_past_days)
    end_date = hoje + timedelta(days=window_future_days)

    # ---------------------------------------------------------
    # 1. HISTÓRICO (O que já está na aba 'proventos')
    # ---------------------------------------------------------
    # Cria uma cópia limpa para uso nas Estimativas depois
    df_hist_clean = pd.DataFrame() 

    if prov_norm is not None and not prov_norm.empty:
        df_hist = prov_norm.copy()
        
        # Correção do KeyError: Busca coluna de data correta de forma dinâmica
        col_pag = None
        # Lista de possíveis nomes para a coluna de data (prioridade)
        # Adicionei 'dt_pagto', 'pgto' e variações para garantir
        candidates = ["data_pagamento", "pagamento", "dt_pagamento", "dt_pagto", "data", "date", "liquidacao", "dt_liquidacao"]
        cols_lower = {c.lower().strip(): c for c in df_hist.columns}
        
        for cand in candidates:
            if cand in cols_lower:
                col_pag = cols_lower[cand]
                break
        
        if col_pag:
            # --- CORREÇÃO PRINCIPAL ---
            # dayfirst=True força o Pandas a entender 15/01 como 15 de Janeiro, não Mês 15.
            df_hist["data_evento_final"] = pd.to_datetime(df_hist[col_pag], dayfirst=True, errors="coerce")
            
            # Remove linhas onde a data não pode ser convertida
            df_hist = df_hist.dropna(subset=["data_evento_final"])
            
            # Salva versão limpa para usar nas estimativas
            df_hist_clean = df_hist.copy()

            # Filtra janela de tempo para exibição
            mask = (df_hist["data_evento_final"].dt.date >= start_date) & (df_hist["data_evento_final"].dt.date <= end_date)
            df_view = df_hist[mask]

            for _, row in df_view.iterrows():
                # Tenta pegar valor total (valor ou valor_liquido ou liquido)
                val = 0.0
                for c_val in ["valor", "valor_liquido", "liquido", "valor_total", "valor_bruto"]:
                    if c_val in cols_lower:
                        val = _to_float(row.get(cols_lower[c_val], 0))
                        if val > 0: break
                
                # Tenta pegar tipo
                tipo = "Rendimento"
                for c_tipo in ["tipo", "tipo_provento", "evento"]:
                    if c_tipo in cols_lower:
                        tipo = str(row.get(cols_lower[c_tipo], "Rendimento")).title()
                        break

                # Tenta pegar ticker
                ticker = ""
                for c_tick in ["ticker", "ativo", "papel", "codigo"]:
                    if c_tick in cols_lower:
                        ticker = str(row.get(cols_lower[c_tick], "")).upper().strip()
                        break

                # Pega valor por cota se existir
                vpc = 0.0
                if "valor_por_cota" in cols_lower:
                    vpc = _to_float(row.get(cols_lower["valor_por_cota"], 0))

                if ticker:
                    # Lógica de status: Se a data é anterior a hoje, considera PAGO
                    dt_ev = row["data_evento_final"].date()
                    status = "PAGO" if dt_ev < hoje else "PROVISIONADO"
                    
                    # Debug mental: Se for 15/01/2026 e hoje é 19/01/2026, status vira "PAGO"
                    
                    events.append({
                        "data_evento": row["data_evento_final"],
                        "ticker": ticker,
                        "valor": val,
                        "tipo": tipo,
                        "fonte": "HISTÓRICO",
                        "status": status,
                        "valor_por_cota": vpc,
                        "data_com": pd.NaT 
                    })

    # ---------------------------------------------------------
    # 2. ANUNCIADOS (Vindo do Robô)
    # ---------------------------------------------------------
    # Cruza o valor anunciado com a quantidade que você tem na carteira
    if prov_anunciados is not None and not prov_anunciados.empty:
        # Mapa de quantidades atuais: { "PETR4": 100, "MXRF11": 50 }
        qtd_map = {}
        if positions_enriched is not None and not positions_enriched.empty:
            for _, pos in positions_enriched.iterrows():
                t = str(pos.get("ticker", "")).upper().strip()
                q = _to_float(pos.get("quantidade", 0))
                if t and q > 0:
                    qtd_map[t] = q

        df_anun = prov_anunciados.copy()
        
        # Garante coluna de data no dataframe do robô
        col_pag_anun = "data_pagamento"
        if col_pag_anun not in df_anun.columns:
             for c in df_anun.columns:
                 if "pagamento" in str(c).lower():
                     col_pag_anun = c
                     break
        
        if col_pag_anun in df_anun.columns:
            # --- CORREÇÃO AQUI TAMBÉM ---
            df_anun["data_pagamento_dt"] = pd.to_datetime(df_anun[col_pag_anun], dayfirst=True, errors="coerce")
            df_anun = df_anun.dropna(subset=["data_pagamento_dt"])
            
            # Filtra janela
            mask_anun = (df_anun["data_pagamento_dt"].dt.date >= start_date) & (df_anun["data_pagamento_dt"].dt.date <= end_date)
            df_anun = df_anun[mask_anun]

            for _, row in df_anun.iterrows():
                ticker = str(row.get("ticker", "")).upper().strip()
                
                if ticker in qtd_map:
                    qtd_atual = qtd_map[ticker]
                    
                    raw_vpc = row.get("valor_por_cota", 0)
                    val_cota = _to_float(raw_vpc)
                    
                    total_previsto = val_cota * qtd_atual
                    
                    if total_previsto > 0:
                        dc = pd.NaT
                        if "data_com" in row:
                            dc = pd.to_datetime(row["data_com"], dayfirst=True, errors="coerce")

                        events.append({
                            "data_evento": row["data_pagamento_dt"],
                            "ticker": ticker,
                            "valor": total_previsto,
                            "tipo": str(row.get("tipo_pagamento", "Rendimento")).upper(),
                            "fonte": "ANUNCIADO (ROBÔ)",
                            "status": "CONFIRMADO",
                            "valor_por_cota": val_cota,
                            "data_com": dc
                        })

    # ---------------------------------------------------------
    # 3. ESTIMATIVAS (Projeção Inteligente)
    # ---------------------------------------------------------
    if include_estimates and not df_hist_clean.empty:
        confirmados = set()
        for e in events:
            if pd.notna(e["data_evento"]):
                dt = e["data_evento"]
                key = f"{e['ticker']}-{dt.month}-{dt.year}"
                confirmados.add(key)

        dt_ref_est_ini = hoje - timedelta(days=365)
        mask_est = (df_hist_clean["data_evento_final"].dt.date >= dt_ref_est_ini) & (df_hist_clean["data_evento_final"].dt.date < hoje)
        df_base_est = df_hist_clean[mask_est]
        
        cols_lower_hist = {c.lower().strip(): c for c in df_base_est.columns}

        for _, row in df_base_est.iterrows():
            try:
                data_original = row["data_evento_final"]
                data_proj = data_original + relativedelta(years=1)
                
                if data_proj.weekday() >= 5:
                    data_proj += timedelta(days=(7 - data_proj.weekday()))

                # Permite estimativas dentro de toda a janela de visualização
                if data_proj.date() >= start_date and data_proj.date() <= end_date:
                    ticker = ""
                    for c_tick in ["ticker", "ativo", "papel", "codigo"]:
                        if c_tick in cols_lower_hist:
                            ticker = str(row.get(cols_lower_hist[c_tick], "")).upper().strip()
                            break
                    
                    if not ticker: continue

                    key_proj = f"{ticker}-{data_proj.month}-{data_proj.year}"
                    
                    if key_proj not in confirmados:
                        val_total_est = 0.0
                        vpc_hist = 0.0
                        if "valor_por_cota" in cols_lower_hist:
                            vpc_hist = _to_float(row.get(cols_lower_hist["valor_por_cota"], 0))
                        
                        val_total_hist = 0.0
                        for c_val in ["valor", "valor_liquido", "liquido", "valor_total"]:
                            if c_val in cols_lower_hist:
                                val_total_hist = _to_float(row.get(cols_lower_hist[c_val], 0))
                                if val_total_hist > 0: break

                        if positions_enriched is not None:
                            pos_atual = positions_enriched[positions_enriched["ticker"] == ticker]
                            if not pos_atual.empty:
                                qtd_atual = _to_float(pos_atual.iloc[0]["quantidade"])
                                if qtd_atual == 0: continue 
                                if vpc_hist > 0:
                                    val_total_est = vpc_hist * qtd_atual
                                else:
                                    val_total_est = val_total_hist
                        else:
                             val_total_est = val_total_hist
                        
                        if val_total_est > 0:
                            events.append({
                                "data_evento": data_proj,
                                "ticker": ticker,
                                "valor": val_total_est,
                                "tipo": "Rendimento (Est)",
                                "fonte": "ESTIMADO",
                                "status": "PREVISÃO",
                                "valor_por_cota": vpc_hist,
                                "data_com": pd.NaT
                            })
            except:
                pass

    # ---------------------------------------------------------
    # 4. FINALIZAÇÃO
    # ---------------------------------------------------------
    if not events:
        return pd.DataFrame(columns=["data_evento", "ticker", "valor", "tipo", "fonte", "status", "valor_por_cota", "data_com", "logo_url", "quantidade"])

    df_cal = pd.DataFrame(events)
    
    if "data_evento" in df_cal.columns:
        df_cal = df_cal.sort_values("data_evento", ascending=True)
    
    return df_cal