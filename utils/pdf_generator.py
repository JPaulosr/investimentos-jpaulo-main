# utils/pdf_generator.py
# -*- coding: utf-8 -*-

from fpdf import FPDF
from datetime import datetime
import pandas as pd
import io

class ProventosPDF(FPDF):
    def header(self):
        # Logo (se tiver um arquivo local, descomente a linha abaixo)
        # self.image('logo.png', 10, 8, 33)
        self.set_font('Arial', 'B', 15)
        # Move to the right
        self.cell(80)
        # Title
        self.cell(30, 10, 'Resumo de Proventos', 0, 0, 'C')
        # Line break
        self.ln(20)

    def footer(self):
        # Position at 1.5 cm from bottom
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        # Page number
        self.cell(0, 10, 'Página ' + str(self.page_no()) + '/{nb}', 0, 0, 'C')

def gerar_pdf_proventos(df_proventos_real, data_ref):
    """
    Gera um PDF com o resumo mensal e análise de 12 meses.
    Retorna o buffer do PDF (bytes).
    """
    pdf = ProventosPDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_font('Arial', '', 12)

    # Cores
    COR_TITULO = (44, 62, 80)
    COR_TEXTO = (50, 50, 50)
    COR_DESTAQUE = (39, 174, 96) # Verde
    
    # Datas
    mes_atual = data_ref.month
    ano_atual = data_ref.year
    mes_nome = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", 
                "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"][mes_atual]

    # --- 1. DADOS DO MÊS (MTD) ---
    # Filtra dados do mês atual
    df_proventos_real['data_dt'] = pd.to_datetime(df_proventos_real['data'], errors='coerce')
    df_mes = df_proventos_real[
        (df_proventos_real['data_dt'].dt.month == mes_atual) & 
        (df_proventos_real['data_dt'].dt.year == ano_atual)
    ]
    
    total_mes = df_mes['valor'].sum() if not df_mes.empty else 0.0
    eventos_mes = len(df_mes)
    dias_com_entrada = df_mes['data_dt'].nunique()

    # --- 2. DADOS DE 12 MESES (Longo Prazo) ---
    # Filtra últimos 12 meses (incluindo o atual)
    data_inicio_12m = data_ref - pd.DateOffset(months=11)
    # Ajusta para o dia 1 do mês inicial
    data_inicio_12m = data_inicio_12m.replace(day=1)
    
    df_12m = df_proventos_real[df_proventos_real['data_dt'] >= data_inicio_12m]
    total_12m = df_12m['valor'].sum() if not df_12m.empty else 0.0
    media_12m = total_12m / 12  # Média simples de 12 meses

    # Comparação Mês vs Média
    diff_media = total_mes - media_12m
    pct_media = (diff_media / media_12m * 100) if media_12m > 0 else 0.0
    status_media = "Acima da média" if diff_media >= 0 else "Abaixo da média"
    simbolo_media = "(+)" if diff_media >= 0 else "(-)"

    # Quebra por Classe (12m)
    # Assumindo que você tem uma lógica para classificar (aqui simplificado ou precisa vir pronto)
    # Se 'classe' não estiver em proventos, precisaria cruzar com ativos. 
    # Para simplificar este exemplo, vamos assumir que passamos um df já com 'classe' ou ignoramos.
    # Vou deixar placeholder para classe.

    # --- RENDERIZAÇÃO NO PDF ---

    # Título do Mês
    pdf.set_font('Arial', 'B', 16)
    pdf.set_text_color(*COR_TITULO)
    pdf.cell(0, 10, f"Resumo Mensal - {mes_nome}/{ano_atual}", 0, 1, 'L')
    pdf.ln(2)

    # Bloco 1: Visão Geral do Mês
    pdf.set_font('Arial', 'B', 12)
    pdf.set_text_color(*COR_TITULO)
    pdf.cell(0, 10, "1. Situação Atual do Mês", 0, 1, 'L')
    
    pdf.set_font('Arial', '', 11)
    pdf.set_text_color(*COR_TEXTO)
    pdf.cell(0, 8, f"Total Recebido: R$ {total_mes:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), 0, 1)
    pdf.cell(0, 8, f"Dias com entrada: {dias_com_entrada}", 0, 1)
    pdf.cell(0, 8, f"Eventos pagos: {eventos_mes}", 0, 1)
    pdf.ln(5)

    # Bloco 2: Viver de Renda (12 Meses)
    pdf.set_font('Arial', 'B', 12)
    pdf.set_text_color(*COR_TITULO)
    pdf.cell(0, 10, "2. Sustentabilidade da Renda (12 Meses)", 0, 1, 'L')

    pdf.set_font('Arial', '', 11)
    pdf.set_text_color(*COR_TEXTO)
    pdf.cell(0, 8, f"Total Acumulado (12m): R$ {total_12m:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), 0, 1)
    
    # Destaque para a média
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, f"Média Mensal (12m): R$ {media_12m:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), 0, 1)
    
    # Status
    pdf.set_font('Arial', 'I', 10)
    cor_status = (39, 174, 96) if diff_media >= 0 else (192, 57, 43)
    pdf.set_text_color(*cor_status)
    pdf.cell(0, 8, f"{status_media} {simbolo_media} {abs(pct_media):.1f}%", 0, 1)
    pdf.ln(5)

    # Bloco 3: Tendência (Últimos 6 meses simplificado)
    pdf.set_font('Arial', 'B', 12)
    pdf.set_text_color(*COR_TITULO)
    pdf.cell(0, 10, "3. Tendência Recente (Últimos 6 Meses)", 0, 1, 'L')
    
    pdf.set_font('Arial', '', 10)
    pdf.set_text_color(*COR_TEXTO)
    
    # Agrupa por mês
    df_agg = df_12m.groupby(df_12m['data_dt'].dt.to_period('M'))['valor'].sum()
    ultimos_6 = df_agg.tail(6)
    
    for periodo, valor in ultimos_6.items():
        mes_str = periodo.strftime("%b/%Y")
        pdf.cell(50, 7, f"{mes_str}: R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), 0, 1)

    # Gera buffer
    buffer = io.BytesIO()
    pdf.output(buffer)
    buffer.seek(0)
    return buffer