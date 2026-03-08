# utils/formatters.py
# -*- coding: utf-8 -*-
from datetime import datetime
from collections import defaultdict

def fmt_brl(v: float) -> str:
    try:
        s = f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except Exception:
        return "R$ 0,00"

def fmt_pct(v: float) -> str:
    try:
        return f"{v:.2f}%".replace(".", ",")
    except:
        return "0,00%"

def fmt_2dp(v: float) -> str:
    try:
        return f"{float(v):.2f}".replace(".", ",")
    except:
        return "0,00"

def fmt_date_br(dt_obj) -> str:
    try:
        if isinstance(dt_obj, str):
            if "-" in dt_obj:
                return datetime.strptime(dt_obj, "%Y-%m-%d").strftime("%d/%m/%Y")
            return dt_obj
        return dt_obj.strftime("%d/%m/%Y")
    except:
        return str(dt_obj)

# --- 1. CARD DE COMPRA ---
def build_trade_msg(
    tipo: str, ticker: str, qtd: float, total_qty: float, preco: float, taxa: float,
    pm: float, est_mes_total: float, vpc_last: float, impacto_mensal: float,
    yoc: float, dy_mensal: float, preco_atual_ref: float,
    metodo: str, custo_total: float, valor_atual: float,
    resultado_fin: float, resultado_pct: float,
    classe: str = "fii"
) -> str:
    
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    financeiro_op = float(qtd) * float(preco) 
    
    sinal_res = "+" if resultado_fin >= 0 else ""
    classe = str(classe).lower().strip()
    is_fii = (classe == "fii" or classe == "fiagro")
    
    bloco_estimativa = ""
    if is_fii and est_mes_total > 0 and vpc_last > 0:
        bloco_estimativa = (
            f"────────────────────\n"
            f"🔮 <b>Expectativa Condicional (Mensal)</b>\n\n"
            f"➡️ Posição ({float(total_qty):g}): <b>{fmt_brl(est_mes_total)}</b>\n"
            f"➡️ Impacto da compra (+{float(qtd):g}): <b>+{fmt_brl(impacto_mensal)}</b>\n"
            f"<i>*Se repetir o último pagamento ({fmt_brl(vpc_last)})</i>\n"
            f"────────────────────\n"
        )
    
    periodo_lbl = "ao mês" if is_fii else "(anual, 12m)"
    lbl_yoc = "YoC (no custo)" if is_fii else "YoC (anual)"
    lbl_dy = "DY (na cotação atual)" if is_fii else "DY (anual)"

    linha_dy = ""
    if dy_mensal > 0 and preco_atual_ref > 0:
        linha_dy = f"📈 {lbl_dy}: <b>{fmt_pct(dy_mensal)} {periodo_lbl}</b> (Cotação: {fmt_brl(preco_atual_ref)})\n"

    metodo_txt = metodo
    if is_fii:
        if "último" in metodo.lower(): metodo_txt = "Último rendimento do fundo"

    return (
        f"📌 <b>OPERAÇÃO REGISTRADA</b>\n"
        f"🕒 {now}\n\n"
        f"🧾 <b>{tipo} — {ticker}</b>\n\n"
        f"🔢 Quantidade: <b>{float(qtd):g}</b>\n"
        f"📦 Total em carteira: <b>{float(total_qty):g}</b>\n"
        f"📊 Preço médio: <b>{fmt_brl(pm)}</b>\n"
        f"💰 Preço unitário: <b>{fmt_brl(preco)}</b>\n\n"
        f"💵 <b>Valor da compra: {fmt_brl(financeiro_op)}</b>\n"
        f"{bloco_estimativa}\n"
        f"💼 <b>Posição Financeira</b>\n"
        f"💰 Custo total: <b>{fmt_brl(custo_total)}</b>\n"
        f"📊 Valor atual: <b>{fmt_brl(valor_atual)}</b>\n"
        f"📈 Resultado: <b>{sinal_res}{fmt_brl(resultado_fin)} ({sinal_res}{fmt_pct(resultado_pct)})</b>\n"
        f"────────────────────\n"
        f"📐 <b>Indicadores ({classe.upper()})</b>\n"
        f"📊 {lbl_yoc}: <b>{fmt_pct(yoc)} {periodo_lbl}</b>\n"
        f"{linha_dy}"
        f"🧠 Método: {metodo_txt}"
    )

# --- 2. CARD DE PROVENTO ---
def build_provento_msg(
    ticker: str, data_ref: str, qtd_total: float, valor_total: float,
    vpc: float, estimativa_total: float,
    yoc: float, dy: float, preco_atual_ref: float,
    metodo: str, status_msg: str,
    classe: str = "fii",
    valor_bruto: float = 0.0,
    ir_retido: float = 0.0,
) -> str:
    
    classe = str(classe).lower().strip()
    is_fii = (classe == "fii" or classe == "fiagro")
    
    lbl_vpc = "Valor por cota" if is_fii else "Valor por ação"
    periodo_lbl = "ao mês" if is_fii else "(anual, 12m)"
    
    linha_dy = ""
    if dy > 0 and preco_atual_ref > 0:
        lbl_dy = "DY (na cotação atual)" if is_fii else "DY (anual)"
        linha_dy = f"📈 {lbl_dy}: <b>{fmt_pct(dy)} {periodo_lbl}</b> (Cotação: {fmt_brl(preco_atual_ref)})\n"

    # Bloco de bruto + IR (só mostra se houver IR retido)
    bloco_ir = ""
    _bruto = float(valor_bruto or 0)
    _ir = float(ir_retido or 0)
    if _ir > 0 and _bruto > 0:
        bloco_ir = (
            f"────────────────────\n"
            f"🧾 <b>Detalhamento Fiscal</b>\n"
            f"📊 Valor bruto: <b>{fmt_brl(_bruto)}</b>\n"
            f"🏛️ IR retido: <b>-{fmt_brl(_ir)}</b>\n"
            f"✅ Valor líquido: <b>{fmt_brl(valor_total)}</b>\n"
        )
    elif _bruto > 0 and abs(_bruto - valor_total) > 0.01:
        bloco_ir = (
            f"────────────────────\n"
            f"🧾 <b>Detalhamento Fiscal</b>\n"
            f"📊 Valor bruto: <b>{fmt_brl(_bruto)}</b>\n"
            f"🏛️ IR retido: <b>-{fmt_brl(_bruto - valor_total)}</b>\n"
            f"✅ Valor líquido: <b>{fmt_brl(valor_total)}</b>\n"
        )

    return (
        f"💰 <b>PROVENTO RECEBIDO (REAL)</b>\n"
        f"🕒 {data_ref}\n\n"
        f"🏷️ Ativo: <b>{ticker}</b>\n\n"
        f"📦 Base: <b>{float(qtd_total):g} cotas</b>\n"
        f"💵 <b>Valor Líquido: {fmt_brl(valor_total)}</b>\n"
        f"📊 {lbl_vpc} (líquido): <b>{fmt_brl(vpc)}</b>\n"
        f"{bloco_ir}"
        f"\n────────────────────\n"
        f"📍 <b>Status vs Último</b>\n"
        f"{status_msg}\n\n"
        f"📐 <b>Indicadores</b>\n"
        f"{linha_dy}"
    )

def build_renda_alert_msg(ticker, ultimo_vpc, media_ref, variacao_pct, window):
    data_hoje = datetime.now().strftime("%d/%m/%Y")
    return (
        f"⚠️ <b>ALERTA DE DESVIO DE RENDA</b>\n"
        f"🕒 {data_hoje}\n\n"
        f"🏷️ Ativo: <b>{ticker}</b>\n\n"
        f"📉 <b>Queda relevante no provento</b>\n\n"
        f"📊 Atual: <b>{fmt_brl(ultimo_vpc)}</b>\n"
        f"📈 Média ({window}m): <b>{fmt_brl(media_ref)}</b>\n"
        f"📉 Variação: <b>{fmt_pct(variacao_pct)}</b>\n"
    )

# --- 3. RESUMO DE OPERAÇÕES (AJUSTADO PARA VISÃO DIA/MÊS) ---
def build_batch_summary_msg(itens: list, tipo_lote: str, impacto_dia_dados: dict = None, total_impacto_mes: float = 0.0) -> str:
    if not itens: return ""

    por_data = defaultdict(list)
    total_caixa = 0.0
    
    # Processa o Lote
    for i in itens:
        d_str = i.get("data_formatada", "Data Desconhecida")
        por_data[d_str].append(i)
        
        if tipo_lote == "OPERACAO":
            tipo = str(i.get("tipo", "")).upper()
            val = float(i.get("valor_total", 0))
            if tipo == "COMPRA": total_caixa -= val
            elif tipo == "VENDA": total_caixa += val
        else:
            val = float(i.get("valor", 0))
            total_caixa += val

    emoji_capa = "🛒" if tipo_lote == "OPERACAO" else "💰"
    titulo = "RESUMO DE OPERAÇÕES" if tipo_lote == "OPERACAO" else "RESUMO DE PROVENTOS"
    
    msg = f"{emoji_capa} <b>{titulo}</b>\n"
    
    # --- LISTA DE OPERAÇÕES (DETALHE) ---
    datas_ordenadas = sorted(por_data.keys())
    data_recente = datas_ordenadas[-1] if datas_ordenadas else datetime.now().strftime("%d/%m/%Y")
    
    for d in datas_ordenadas:
        msg += f"\n📅 <b>{d}</b>\n"
        lista_dia = por_data[d]
        compras, vendas, proventos = [], [], []

        for item in lista_dia:
            if tipo_lote == "OPERACAO":
                tipo = str(item.get("tipo", "")).upper()
                val = float(item.get("valor_total", 0))
                qtd = float(item.get("quantidade", 0))
                tick = item.get("ticker", "")
                
                # Unidade: cotas para FIIs, ações para outros
                classe = str(item.get("classe", "")).lower()
                unidade = "cotas" if classe in ["fii", "fiagro"] else "ações"
                
                if tipo == "COMPRA": compras.append((tick, qtd, val, unidade))
                elif tipo == "VENDA": vendas.append((tick, qtd, val, unidade))
            else:
                tick = item.get("ticker", "")
                val = float(item.get("valor", 0))
                tipo_prov = item.get("tipo", "Provento") 
                proventos.append((tick, tipo_prov, val))

        if tipo_lote == "OPERACAO":
            if compras:
                msg += "🟢 <b>Compras</b>\n"
                for t, q, v, u in compras: msg += f"• {t} → +{q:g} {u} ({fmt_brl(v)})\n"
            if vendas:
                msg += "🔴 <b>Vendas</b>\n"
                for t, q, v, u in vendas: msg += f"• {t} → -{q:g} {u} ({fmt_brl(v)})\n"
        else:
            if proventos:
                msg += "🟢 <b>Entradas</b>\n"
                for t, tp, v in proventos: msg += f"• {t} ({tp}) → {fmt_brl(v)}\n"

    msg += f"\n────────────────────\n"
    
    if tipo_lote == "OPERACAO":
        str_total = fmt_brl(total_caixa)
        if total_caixa < 0: str_total = f"-{fmt_brl(abs(total_caixa))}"
        elif total_caixa > 0: str_total = f"+{fmt_brl(total_caixa)}"
        msg += f"💱 <b>Impacto no caixa: {str_total}</b>"
        
        # --- BLOCO IMPACTO CONDICIONAL (DIA vs MÊS) ---
        # Exibe se houver impacto acumulado no mês OU no dia
        if total_impacto_mes > 0 or (impacto_dia_dados and sum(impacto_dia_dados.values()) > 0):
            msg += "\n\n────────────────────\n"
            msg += "🔮 <b>Impacto Condicional das Compras (estimativa mensal)</b>\n\n"
            
            # 1. Total do Dia (se houver)
            if impacto_dia_dados:
                total_dia = sum(impacto_dia_dados.values())
                if total_dia > 0:
                    msg += f"➡️ No dia ({data_recente}): <b>+{fmt_brl(total_dia)}</b>\n"
            
            # 2. Total do Mês
            # Pega o mês atual para o rótulo (ex: 01/01 -> 18/01)
            mes_ano = "/".join(data_recente.split("/")[1:]) # MM/YYYY
            msg += f"➡️ No mês (01/{mes_ano} → {data_recente}): <b>+{fmt_brl(total_impacto_mes)}</b>\n\n"
            
            # 3. Lista APENAS do Dia (se houver)
            if impacto_dia_dados:
                for tick, val in impacto_dia_dados.items():
                    msg += f"• {tick} → +{fmt_brl(val)}\n"
            
            msg += "\n🧠 <i>Base: último provento conhecido</i>\n"
            msg += "⚠️ <i>Se repetir o último pagamento</i>"
            
    else:
        msg += f"💵 <b>TOTAL RECEBIDO (REAL): {fmt_brl(total_caixa)}</b>"
    
    return msg
