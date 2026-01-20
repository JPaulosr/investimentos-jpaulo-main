📄 CONTRATO DO SISTEMA DE INVESTIMENTOS — v2.1

(Atualização: Página “Momento do Aporte”)

1. Princípios Fundamentais do Sistema

O sistema tem como objetivo auxiliar decisões, nunca substituí-las.

Nenhuma página, métrica ou ranking constitui recomendação de compra ou venda.

O sistema é determinístico, auditável e reproduzível.

A fonte de verdade é o Google Sheets.

O sistema deve funcionar mesmo sem conexão com a internet.

Qualquer dado externo é auxiliar, nunca soberano.

2. Separação Absoluta por Classe de Ativo

AÇÃO ≠ FII / FIAGRO

Nunca misturar métricas, projeções, lógica de renda ou expectativas.

Cada classe possui:

métricas próprias

rankings próprios

regras próprias

páginas próprias

3. Proventos — Regras Invioláveis
3.1 Proventos Recebidos

Apenas valores efetivamente recebidos entram como renda real.

Proventos recebidos são registrados com:

ticker

data

valor

quantidade

valor por cota

3.2 Proventos Anunciados

Proventos anunciados:

não entram em renda

não entram em somatórios

não entram em projeções

Servem exclusivamente para:

calendário futuro

visualização informativa

4. Projeções de Renda
4.1 FII / FIAGRO

Qualquer projeção deve ser condicional.

A base é sempre o último provento recebido.

Linguagem obrigatória:

“Se repetir o último pagamento…”

É proibido:

média automática

estimativa futura sem lastro real

4.2 AÇÕES

Não existe projeção automática de renda.

Dividendos só podem ser exibidos quando:

anunciados oficialmente

com data definida

Ações são tratadas como crescimento patrimonial, não renda mensal.

5. Alocação, Concentração e Risco

O sistema deve:

calcular pesos por ativo

calcular pesos por classe

respeitar limites definidos pelo usuário

Nenhuma página pode ignorar regras de concentração.

Alocação alvo é parâmetro estrutural, não sugestão.

6. Página “Momento do Aporte” — NOVO BLOCO
6.1 Objetivo

A página Momento do Aporte tem como função priorizar candidatos a aporte, jamais recomendar compra ou venda.

Ela existe para:

reduzir decisões emocionais

respeitar regras de alocação

mostrar impactos antes do aporte

explicar riscos e limitações

6.2 Fontes de Dados Permitidas

A página pode utilizar:

preço médio do usuário

custo total

quantidade

posição atual

proventos recebidos

cotações (cache ou manual)

regras de alocação

alertas manuais

É proibido:

qualquer decisão baseada apenas em preço de mercado

dependência exclusiva de dados externos

6.3 Separação por Classe (Obrigatória)

A página deve exibir rankings separados:

FII / FIAGRO

AÇÕES

Métricas nunca podem ser reaproveitadas entre classes.

6.4 Critérios de Priorização — FII / FIAGRO

São permitidos como sinais, nunca como verdade absoluta:

desconto vs preço médio do usuário

estabilidade dos proventos recebidos

último yield recebido

aderência à alocação alvo

impacto positivo na renda condicional

nível de concentração atual

São fatores de penalização:

corte relevante de proventos

ausência prolongada de pagamento

excesso de concentração

alerta manual ativo

6.5 Critérios de Priorização — AÇÕES

São permitidos como sinais:

desconto vs preço médio

contribuição para diversificação

aderência à alocação por setor

ausência de alerta estrutural

São fatores de penalização:

excesso de concentração

tese marcada como inválida

tentativa de “preço médio emocional”

6.6 Score de Aporte

O score é relativo, nunca absoluto.

Serve apenas para ordenar candidatos.

O score:

não é recomendação

não é preço justo

não é previsão

6.7 Alertas e Bloqueios

Ativos com alerta severo podem:

ser penalizados

ou removidos do ranking

Alertas são:

manuais

auditáveis

versionados por data

6.8 Simulador de Aporte (Obrigatório)

Antes de qualquer decisão, a página deve permitir simular:

novo preço médio

novo peso do ativo

impacto na alocação

impacto na concentração

impacto na renda condicional (quando aplicável)

Nenhum aporte é executado automaticamente.

7. Internet e Dados Externos

Dados externos são opcionais.

Devem:

ter cache

ter timestamp

ter fallback local

Nunca podem quebrar o sistema.

Nunca podem assumir papel de verdade absoluta.

8. Filosofia do Sistema

Menos decisões, melhores decisões

Processo vence palpite

Clareza vence complexidade

O sistema protege o usuário dele mesmo
9. Espelhamento Legado (Planilha Principal)

Objetivo

O sistema pode espelhar lançamentos na planilha antiga ("Planilha Principal") por compatibilidade e histórico, mas a fonte de verdade é SEMPRE a planilha do APP (Investimentos_App_DB).

Regras

Base nova (APP DB)

Toda gravação deve ocorrer primeiro na base nova.

Se a gravação na base nova falhar, o item NÃO pode seguir para o legado.

Legado (Planilha Principal)

A gravação no legado é best-effort:

se falhar, o sistema deve avisar (warning/erro) e continuar o lote.

não pode bloquear a gravação na base nova.

Os campos e mapeamentos do legado são rígidos e precisam respeitar validações e dropdowns existentes na planilha antiga.

Operações (Compra/Venda)

Colunas do legado (exemplo):

C = ticker (sempre UPPER)

D = data (dd/mm/aaaa)

E = tipo (EXATO como dropdown: "Compra" / "Venda")

I = quantidade

J = preço unitário

Proventos

Colunas do legado (exemplo):

B = ticker (UPPER)

C = tipo provento (dropdown)

D = data

E = quantidade

F = unitário (NÃO preencher)

G = total líquido

Performance (obrigatório em lote)

Para não demorar, em lotes o sistema deve:

abrir o worksheet do legado apenas uma vez

calcular a próxima linha (next_row) apenas uma vez e ir incrementando

não fazer leituras pesadas dentro do loop (evitar get_all_values por item)

Erros comuns e solução

A. Não gravar no legado por secrets errados (case-sensitive)

Causa

No Streamlit, as chaves em st.secrets são case sensitive. Se o código buscar "SHEET_ID_PRINCIPAL" e o secrets estiver "sheet_id_principal" (ou vice-versa), retorna None e o legado não grava.

Contrato

Os secrets do legado DEVEM existir com estes nomes (recomendado manter exatamente):

SHEET_ID_PRINCIPAL

ABA_PRINCIPAL_MOVIMENTACOES

ABA_PRINCIPAL_PROVENTOS

Fail fast (obrigatório)

Toda página que executa gravação em lote (ex: Central de Lançamentos) deve validar o legado ANTES do usuário clicar em FINALIZAR:

- se faltar qualquer secret do legado, o sistema deve mostrar em tela: LEGADO OFF (com o motivo)
- o lote deve continuar gravando na base nova, mas o usuário deve ser avisado que o espelho não será gravado

Obrigatoriedade de chamada (robô de gravação)

Para o legado receber dados, a página responsável deve:

- importar explicitamente append_movimentacao_legado e append_provento_legado
- chamar o espelho sempre DEPOIS da base nova gravar com sucesso
- tratar falha do legado como warning (best-effort), sem abortar o lote


Blindagem Anti-Regressão (OBRIGATÓRIO)

Objetivo

Evitar que o espelhamento no legado “pare de gravar” por regressão (refatoração, remoção acidental de import/chamada, ou secrets com nome incorreto).

Regra de Arquitetura

Nenhuma página pode implementar gravação no legado diretamente.
Toda gravação (movimentações e proventos) deve passar por UMA ÚNICA função de persistência (single entry-point), localizada em utils/gsheets.py, por exemplo:

- persist_movimentacao(...)
- persist_provento(...)

Comportamento obrigatório dessas funções

1) Validar e explicitar status do legado ANTES da ação:
   - checar presença dos secrets do legado (case-sensitive)
   - checar existência das abas do legado
   - se falhar: marcar LEGADO_OFF (com motivo) e seguir apenas com base nova

2) Gravar SEMPRE primeiro na base nova (APP DB).
   - se falhar: abortar e não tentar legado.

3) Espelhar para o legado apenas após sucesso na base nova (best-effort).
   - se falhar: registrar warning claro (não abortar a base nova).

4) Garantir padronizações invariáveis:
   - ticker sempre UPPER
   - datas no formato requerido
   - tipo exatamente igual ao dropdown do legado

5) Guardrail de Regressão (auto-teste local)

Toda página que grava deve executar, em modo “pré-checagem” (antes do botão FINALIZAR), um self-test rápido:

- consegue abrir planilha nova? (OK/ERRO)
- consegue abrir legado? (OK/LEGADO_OFF)
- funções persist_* importadas? (OK/ERRO)

Se qualquer item falhar, o sistema deve mostrar em tela:

- BASE NOVA: OK/ERRO
- LEGADO: ON/OFF (motivo)

Motivação (histórico)

Problemas recorrentes já ocorridos:

- Secrets do legado com variação de maiúsculas/minúsculas (case-sensitive) ⇒ retornava None e o legado não gravava.
- Remoção acidental do bloco de import/chamada do legado em páginas do robô ⇒ base nova gravava, legado não.

Esta blindagem torna essas falhas imediatamente visíveis e impede repetição de debugging de várias horas.




Se o projeto optar por tolerância, a função de leitura de secrets deve tentar variações de nome (maiúsculas/minúsculas) e registrar claramente qual chave foi usada.

B. Range exceeds grid limits (linhas insuficientes)

Causa

Ao calcular next_row e tentar escrever além do total de linhas da aba, o Google Sheets retorna erro 400 (grid limits).

Contrato

Antes de escrever no legado (ou na base nova, se aplicável), o sistema deve garantir que o worksheet tenha linhas suficientes:

se next_row > ws.row_count, chamar ws.resize(rows=next_row + folga)

A folga recomendada é +50 linhas.

10. Checklist de Diagnóstico (quando não gravar)

Se "não salvou" em qualquer aba:

1) Confirmar secrets: nomes exatos e valores presentes (case-sensitive)

2) Confirmar aba existe e o título bate (sem espaços extras)

3) Confirmar validações/dropdowns do legado ("Compra"/"Venda", "Rendimento"/"Dividendo"/"JCP"/"Amortização")

4) Confirmar próxima linha livre (âncora correta: operações em C, proventos em B)

5) Confirmar limite de linhas (ws.row_count) e aplicar resize se necessário

6) Garantir ordem: base nova primeiro, legado depois (best-effort)

11. Alertas Automatizados de Proventos (Telegram + PDF)

Objetivo

Garantir que o usuário seja informado automaticamente sobre eventos relevantes de proventos,
sem necessidade de consulta manual a plataformas externas.

O sistema deve priorizar clareza, confiabilidade e não gerar ruído ou spam.

11.1 Fonte de Verdade

- A fonte soberana é a aba `proventos_anunciados` do Google Sheets (APP DB).
- Dados externos (Investidor10, StatusInvest, RI, etc.) são utilizados apenas como meio de captura.
- Nenhum dado externo pode sobrescrever registros manuais ou contratos internos.

11.2 Execução do Job

- Os alertas são executados por um job automatizado (ex.: GitHub Actions).
- O job roda no máximo 1 vez por dia, em horário fixo e previsível.
- A execução deve ser idempotente: rodar mais de uma vez não pode duplicar dados nem alertas.
- O job não depende de Streamlit nem de interface gráfica.

11.3 Tipos de Alerta (Gatilhos)

O sistema pode enviar até três tipos de alerta, de forma independente:

A) Novos Anúncios de Proventos
- Gatilho: novos registros inseridos em `proventos_anunciados` desde a última execução.
- Conteúdo mínimo:
  - ticker
  - tipo de pagamento
  - data-com
  - data de pagamento
  - valor por cota
- O sistema deve evitar alertas repetidos para o mesmo anúncio.

B) Data-com Aberta (Janela de Oportunidade)
- Gatilho: ativos com data-com futura dentro de uma janela configurável (ex.: próximos 7 dias).
- Regra segura:
  - considerar data-com aberta apenas se data_com > data atual.
- Objetivo: informar que ainda é possível aportar antes do fechamento da data-com.

C) Pagamentos Previstos no Dia
- Gatilho: registros com data_pagamento == data atual.
- O alerta deve apresentar:
  - lista de ativos pagadores
  - valor por cota
  - total estimado ou congelado
- O método de cálculo deve ser explicitado:
  - quantidade_ref (preferencial), ou
  - quantidade atual em carteira (estimativa).

11.4 Conteúdo e Formato

- O alerta principal deve ser enviado por mensagem no Telegram (texto curto).
- O sistema pode anexar um PDF resumo do dia, contendo:
  - visão consolidada dos eventos
  - detalhamento por ativo
  - notas de confiabilidade dos cálculos
- O PDF é informativo e não constitui recomendação ou decisão automática.

11.5 Anti-Spam e Confiabilidade

- Alertas só devem ser enviados quando houver eventos relevantes.
- O sistema não deve enviar mensagens vazias ou repetitivas.
- Falhas de captura externa não devem gerar alertas falsos.
- O job deve registrar log mínimo de execução para auditoria.

11.6 Escopo e Limites

- O sistema não interpreta comunicados de RI.
- O sistema não faz recomendação de compra ou venda.
- O sistema não executa aportes automaticamente.
- Todo conteúdo enviado é informativo, auditável e rastreável.
