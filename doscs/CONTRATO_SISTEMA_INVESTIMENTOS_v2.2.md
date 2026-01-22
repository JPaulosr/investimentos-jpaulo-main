# 📘 CONTRATO DO SISTEMA DE INVESTIMENTOS — v2.2
(Base única • PDFs • Proventos Anunciados • Alertas • Blindagem Anti‑Regressão)

## 1. Princípios Fundamentais
1. O sistema apoia decisões, nunca as substitui.
2. Nenhuma tela, score, ranking, PDF ou alerta constitui recomendação.
3. Sistema determinístico, auditável e reproduzível.
4. Fonte de verdade: Google Sheets (APP DB).
5. Funcionamento resiliente (offline/cache).
6. Dados externos são auxiliares.

## 2. Separação por Classe de Ativo
AÇÃO ≠ FII / FIAGRO.  
Cada classe possui métricas, regras e análises próprias.

## 3. Proventos
### 3.1 Recebidos (REAL)
Entram em totais, PDFs e renda real.

### 3.2 Anunciados (INFORMATIVO)
Fonte única: aba `proventos_anunciados`.
Não entram em renda nem projeções automáticas.

## 4. Projeções
- FII/FIAGRO: condicional ao último pagamento.
- Ações: apenas quando oficialmente anunciadas.

## 5. PDFs Oficiais
### Tipos
- PDF Executivo
- PDF Auditoria

### Regras
- Motor único: utils/pdf_reports.py
- Funções:
  - build_pdf_executivo
  - build_pdf_auditoria
  - gerar_e_enviar_pdfs
- Nenhuma página gera PDF manualmente.

## 6. Telegram
Canal oficial para alertas e PDFs.
Falhas não quebram o lote.

## 7. Alertas
Baseados exclusivamente em `proventos_anunciados`.

## 8. Legado
Espelhamento best‑effort. Fonte soberana é o APP DB.

## 9. Blindagem
Persistência centralizada.
Pré‑checagens obrigatórias.

## 10. Filosofia
Processo > Palpite  
Clareza > Complexidade

## 11. Status
Versão v2.2 consolidada.
Qualquer mudança exige atualização deste contrato.
