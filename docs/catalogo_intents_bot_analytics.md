# Catálogo de Intents — Bot de Analytics via WhatsApp

**Autor:** Mateus Fabro (Squad 3 — Intelligence Engine)
**Data:** 20/04/2026
**Status:** Rascunho v1 — para discussão com Castrin (Head de Dados)

---

## Contexto

Atualmente, stakeholders (Head, CTO, CGO, gerentes) enviam demandas recorrentes
via WhatsApp — muitas em finais de semana, feriados, horários pontuais. O analista
vira **gargalo humano** de tarefas mecânicas (rodar query, gerar Excel, formatar tabela).

**Hipótese:** ~80% dessas demandas seguem padrões repetíveis e poderiam ser
respondidas por um bot alimentado pela **inteligência acumulada da squad**
(schemas validados, regras de ouro, whitelists, filtros padrão — hoje em `memory/`).

**Canal:** WhatsApp (confirmado — ~100% das demandas chegam por lá).
Arquitetura é agnóstica — se migrar pra Slack/Teams no futuro, mesmo catálogo serve.

**Escopo:** extração e apresentação de dados. **NÃO escopo:** análise crítica,
diagnóstico, decisão (essas continuam humanas).

**Diretriz técnica (confirmada pelo Head):** bot consulta **EXCLUSIVAMENTE
views/matviews do Super Nova DB (PostgreSQL)**. Zero acesso direto ao Athena
ou Super Nova Bet DB. Motivo: proteger Athena de queries pesadas + reusar
camada semântica que já alimenta os produtos SN (matriz financeiro, matriz
aquisição, risco, tab_cassino, tab_sports, etc.).

**Consequência estratégica:** o bot vira **produto SN plug-and-play**. Qualquer
operação futura que implementar o mesmo contrato de views recebe o bot sem
desenvolvimento adicional.

---

## Critérios de priorização

Cada intent é classificada em 3 dimensões:

- **Frequência** (quantas vezes/mês a demanda aparece): Alta / Média / Baixa
- **Complexidade de implementar**: Baixa (script existe) / Média (adaptar) / Alta (criar do zero)
- **Risco LGPD/Segurança**: Baixo (agregado) / Médio (PID público) / Alto (CPF/email/phone)

Priorizar **Alta frequência + Baixa complexidade + Baixo risco** no MVP.

---

## Catálogo — MultiBet BR (Brasil)

### 1. Report de Affiliates (Top 3 IDs)
- **Pergunta exemplo:** *"Manda o report de affiliates de ontem"* / *"Como tá Google, Meta e 477668 essa semana?"*
- **Parâmetros:** período (hoje/ontem/semana/mês), affiliate IDs (default: 3 principais)
- **Script existente:** [`scripts/extract_affiliates_report.py`](../scripts/extract_affiliates_report.py)
- **Formato resposta:** tabela Métrica | Valor + Net Deposit + P&L bold (padrão validado por `feedback_formato_report_affiliates_whatsapp.md`)
- **Filtros padrão:** D-1 obrigatório, test users excluídos
- **Stakeholder:** Head de Tráfego, Castrin, Mauro
- **Operação:** MultiBet BR
- **Frequência:** Alta (diária) | **Complexidade:** Baixa | **Risco:** Baixo

### 2. FTD por dia/canal
- **Pergunta exemplo:** *"Quantos FTDs ontem por canal?"* / *"FTD Meta dos últimos 7 dias"*
- **Parâmetros:** período, canal (all/Meta/Google/Keitaro/orgânico)
- **Fonte confiável:** `ecr_ec2` (intraday, 99% match BQ — ver `feedback_reg_fonte_ecr_ec2.md`)
- **Filtros padrão:** D-1, `is_test = false`, timezone BRT
- **Formato:** tabela Dia × Canal + totalizador
- **Stakeholder:** Head, Tráfego, CGO
- **Operação:** MultiBet BR
- **Frequência:** Alta | **Complexidade:** Baixa | **Risco:** Baixo

### 3. Cadastros D-1 por canal (UTM)
- **Pergunta exemplo:** *"Cadastros de ontem por fonte?"* / *"Quantos cadastros Keitaro hoje?"*
- **Parâmetros:** período, dimensão (utm_source / utm_campaign)
- **Fonte:** `multibet.trackings` + `ps_bi.dim_user` (JOIN por `external_id`)
- **Filtros padrão:** D-1, exclui test users
- **Formato:** tabela canal/fonte × cadastros × %
- **Stakeholder:** Tráfego, Head
- **Operação:** MultiBet BR
- **Frequência:** Alta | **Complexidade:** Baixa | **Risco:** Baixo

### 4. Grandes Ganhos do dia
- **Pergunta exemplo:** *"Teve algum grande ganho hoje?"* / *"Quem ganhou mais de 10K ontem?"*
- **Parâmetros:** período, valor mínimo (default: R$ 10K), produto (casino/sb)
- **Script existente:** [`pipelines/grandes_ganhos.py`](../pipelines/grandes_ganhos.py) (já roda em prod EC2)
- **Formato:** tabela PID/Jogo/Valor/Hora
- **Stakeholder:** Head, Riscos, Operação
- **Frequência:** Média | **Complexidade:** Baixa | **Risco:** Médio (PID público)

### 5. Matriz de Risco v2 — Top flags
- **Pergunta exemplo:** *"Quem está com flag crítico hoje?"* / *"Quantos flags R9?"*
- **Parâmetros:** data (D-1 default), flag específica (opcional)
- **Fonte:** `project_matriz_risco_v2.md` — 21 tags Smartico, push S2S diário 02:30 BRT
- **Formato:** tabela Flag × Count + top 5 PIDs por flag
- **Stakeholder:** Riscos, CTO, Head
- **Frequência:** Alta | **Complexidade:** Baixa | **Risco:** Médio

### 6. Ad Spend multicanal (Google + Meta)
- **Pergunta exemplo:** *"Gasto de ontem Google + Meta?"* / *"ROAS da semana?"*
- **Parâmetros:** período, canal, métrica (spend / ROAS / CPA)
- **Fonte:** pipeline já em prod (`project_ad_spend_multicanal.md`), R$ 8,2M 2.355 rows
- **Formato:** tabela Canal × Campanha × Spend × FTD × CPA
- **Stakeholder:** Tráfego, Head, CGO
- **Frequência:** Alta | **Complexidade:** Baixa | **Risco:** Baixo

### 7. Cohort de aquisição
- **Pergunta exemplo:** *"Retenção da safra de março?"* / *"LTV dos cadastrados em abril"*
- **Parâmetros:** mês/semana de safra, métrica (retenção W1-W4 / LTV / ARPU)
- **Fonte:** `agg_cohort_acquisition.py` + `vw_active_player_retention_weekly`
- **Formato:** tabela safra × week_number × métrica
- **Stakeholder:** Head, CRM, CGO
- **Frequência:** Média | **Complexidade:** Baixa | **Risco:** Baixo

### 8. Alerta Sportsbook
- **Pergunta exemplo:** *"Alguma aposta grande acima da média hoje?"*
- **Parâmetros:** regra (R1-R11), período
- **Fonte:** `project_alerta_sportsbook.md` (R9/R10 em prod, R11 pendente)
- **Formato:** tabela alerta × PID × valor
- **Stakeholder:** Riscos, Sportsbook
- **Frequência:** Alta | **Complexidade:** Média (expor alertas existentes via bot) | **Risco:** Médio

---

## Catálogo — Play4Tune (Paquistão)

### 9. GGR afundando (jogador + jogo)
- **Pergunta exemplo:** *"Quem tá afundando o GGR da P4T?"* / *"Top 10 jogadores GGR negativo últimos 7d"*
- **Parâmetros:** período, top N (default 10)
- **Script existente:** [`scripts/report_players_afundando_ggr_play4.py`](../scripts/report_players_afundando_ggr_play4.py) (criado hoje, 20/04)
- **Formato:** tabela PID/Dias/Apostado/GGR/Payout + top jogo de cada + Excel + WhatsApp
- **Filtros padrão:** 72 test users excluídos, 4 whitelist DP/SQ devolvidas, D-1, moeda BRL + PKR
- **Stakeholder:** Castrin, Gabriel, Riscos
- **Frequência:** Alta (virou rotina) | **Complexidade:** Baixa (script pronto) | **Risco:** Médio

### 10. Top jogos (GGR / turnover / giros)
- **Pergunta exemplo:** *"Top 10 jogos da P4T essa semana"* / *"Qual jogo rodou mais ontem?"*
- **Parâmetros:** período, métrica (GGR / turnover / giros), direção (top / bottom)
- **Script existente:** [`scripts/report_jogos_play4tune_html.py`](../scripts/report_jogos_play4tune_html.py)
- **Formato:** tabela Jogo/RTP/Players/Rodadas/GGR + top player por jogo (opcional)
- **Stakeholder:** Castrin, Produto
- **Frequência:** Alta | **Complexidade:** Baixa | **Risco:** Baixo

### 11. RTP por jogo (7d / 30d / desde D0)
- **Pergunta exemplo:** *"RTP do VORTEX últimos 7d"* / *"Algum jogo com RTP anômalo?"*
- **Parâmetros:** jogo (all / específico), período
- **Script existente:** [`scripts/report_rtp_todos_jogos_desde_d0.py`](../scripts/report_rtp_todos_jogos_desde_d0.py) + [`scripts/report_rtp_padrao_play4.py`](../scripts/report_rtp_padrao_play4.py) (criados hoje, 20/04)
- **Formato:** tabela Jogo/RTP cfg/RTP obs/Delta/Veredito + CSV com legenda
- **Insight integrado:** janelas curtas (7d) detectam evento, janelas longas (30d) diagnosticam produto
- **Stakeholder:** Castrin, Gabriel
- **Frequência:** Alta (agora) | **Complexidade:** Baixa | **Risco:** Baixo

### 12. Deep dive de jogador (PID X)
- **Pergunta exemplo:** *"Me fala sobre o PID XrOZYpJ9Y"* / *"Quem é o maharshani?"*
- **Parâmetros:** PID ou username
- **Fonte:** users + transactions + bets + casino_user_game_metrics (supernova_bet)
- **Formato:** card resumido — cadastro, dias, depósitos totais, saques totais, GGR, jogos favoritos, flags
- **Filtros padrão:** mascarar phone/email em resposta pública
- **Stakeholder:** Head, Riscos, Castrin
- **Frequência:** Alta (incidentes) | **Complexidade:** Média | **Risco:** Alto (dados pessoais — precisa whitelist de número autorizado)

### 13. GGR geral P4T (hoje / semana / mês)
- **Pergunta exemplo:** *"GGR da P4T hoje?"* / *"Como tá a Play4 essa semana?"*
- **Parâmetros:** período
- **Fonte:** `casino_game_metrics` (agregada)
- **Formato:** número + mini-série histórica (últimos 7 dias) + comparativo vs semana anterior
- **Stakeholder:** todos
- **Frequência:** Alta | **Complexidade:** Baixa | **Risco:** Baixo

---

## Catálogo — Cross / Híbrido

### 14. PCR (Player Credit Rating)
- **Pergunta exemplo:** *"Rating do PID X?"* / *"Quem está AAA essa semana?"*
- **Parâmetros:** PID ou tier (D/C/B/A/AA/AAA)
- **Fonte:** `project_pcr_player_credit_rating.md` (HTML v1 pronto, pipeline pendente)
- **Status:** **Requer pipeline antes de expor no bot**
- **Frequência:** Média | **Complexidade:** Alta (pipeline) | **Risco:** Alto

### 15. CRM Report diário
- **Pergunta exemplo:** *"Report CRM de ontem"* / *"Como tá o funil de conversão?"*
- **Parâmetros:** data, etapa do funil (fact_type_id 1-5)
- **Fonte:** `project_crm_report_daily.md` (8 tabelas + HTML já em prod)
- **Formato:** tabela por etapa + % conversão
- **Stakeholder:** CRM, Raphael M., Head
- **Frequência:** Alta (diária) | **Complexidade:** Baixa | **Risco:** Baixo

---

## Regras de ouro (válidas para TODAS intents)

Herdadas das regras da squad (aplicadas automaticamente pelo bot):

1. **D-1 default** — nunca D-0 parcial sem aviso explícito ("dados parciais até XXh").
2. **Test users excluídos** — UNION heurística + lógica dev (Play4Tune) ou `is_test` (MultiBet).
3. **Moeda Play4Tune:** sempre BRL + PKR lado a lado, usando taxa `currency_exchange_rates` do banco.
4. **Dicionário de colunas obrigatório** — todo Excel/CSV acompanha aba Legenda ou `_legenda.txt`.
5. **Fonte sempre declarada** — qual banco, qual tabela, qual período.
6. **Whitelist de números WhatsApp** — só Head/gestores autorizados recebem dados granulares.
7. **Log de auditoria** — toda pergunta + resposta gravada (pro time revisar depois + melhoria contínua).
8. **Quando em dúvida, escalar** — bot tem intent "preciso de análise" que pinga o analista na squad.

---

## Roadmap de implementação sugerido

**MVP (2-3 semanas) — 5 intents Alta frequência + Baixa complexidade:**
1. GGR geral P4T (#13)
2. GGR afundando P4T (#9)
3. Top jogos P4T (#10)
4. FTD por canal MultiBet (#2)
5. Report affiliates (#1)

**Fase 2 (+2 semanas) — mais 5 intents:**
6. RTP por jogo (#11)
7. Cadastros D-1 (#3)
8. Ad spend multicanal (#6)
9. Matriz risco v2 (#5)
10. Grandes ganhos (#4)

**Fase 3 — restantes + deep dive jogador (#12, precisa LGPD reforçado)**

---

## O que eu preciso validar com o Castrin

1. WhatsApp como canal único ou pensar em Slack/Teams paralelo?
2. Provedor WhatsApp — Z-API, Twilio ou API oficial Meta?
3. Orçamento para LLM (Claude/GPT) — uso estimado: R$ 100-300/mês no MVP.
4. Whitelist inicial de números autorizados.
5. Fast-track ou respeitar roadmap (MVP 2-3 semanas)?
6. Responsabilidade — se bot entregar dado errado, quem responde? (Minha sugestão: squad audita 1x cada intent antes de subir, depois é do bot.)

---

## Diferencial vs chatbot genérico

**Não é "ChatGPT com acesso ao SQL".** É a **inteligência acumulada da squad**
produtizada — todos os schemas validados, correções empíricas, whitelists,
regras de filtro, padrões de entrega (hoje em `memory/` com ~50 arquivos)
viram knowledge base do bot.

Cada correção futura (ex: "nova tabela com bug Y", "novo filtro obrigatório Z")
entra como novo `.md` e o bot fica mais inteligente — **não degrada ao longo do tempo**.

---
