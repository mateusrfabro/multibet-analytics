# Roteiro de Apresentacao - Dashboard CRM Performance v0
**Para:** Raphael M. (CRM Leader) + Castrin (Head de Dados)
**Tempo:** 20-30 minutos
**Formato:** Dashboard ao vivo + PDF impresso

---

## 1. Abertura (2 min)
"Construimos um dashboard para dar visibilidade ao desempenho das campanhas CRM.
Hoje vou mostrar a v0 com dados de marco inteiro para coletar feedback de voces
e alinhar o que faz sentido antes de automatizar."

**Deixar claro:** Isso e uma v0. Os dados sao reais, mas precisamos da validacao
do CRM para refinar as metricas e classificacoes.

---

## 2. KPIs Header (3 min)
Mostrar os 6 KPIs e explicar cada um:

| KPI | Valor | O que falar |
|-----|-------|-------------|
| Campanhas | 1.580 | "Tivemos 1.580 campanhas com atividade em marco" |
| Players Ativos | 30.518 | "Desses, 30K efetivamente jogaram" |
| Custo CRM | R$ 738K | "Investimos R$ 738K entre bonus e disparos" |
| GGR | R$ 5,15M | "Geramos R$ 5,15M de receita bruta" |
| ROI | 7.0x | "Para cada R$ 1 investido, voltaram R$ 7" |
| ARPU | R$ 169 | "Cada player gerou em media R$ 169 de receita" |

**Pergunta para o CRM:** "Esses numeros fazem sentido com o que voces veem no dia a dia?"

---

## 3. Funil (3 min)
Mostrar o funil e PAUSAR no gap:

"De 105K que receberam ofertas, 103K completaram (98% - sao popups automaticos).
MAS apenas 34K (32%) tiveram atividade financeira real.
Ou seja, 69K users receberam bonus mas NAO jogaram nem depositaram."

**Pergunta:** "Esses 69K users sao o gap. O que podemos fazer para ativa-los?"

---

## 4. Classificacao de Campanhas (5 min)
Mostrar o grafico "Funil por Tipo" e a tabela:

"Classificamos as campanhas por nome. Challenge (Fortune Tiger, Ox, etc.) e o maior
em volume. Cashback_VIP (IDs 754, 792) e o maior em custo — R$ 353K de bonus."

**Pergunta CRITICA:** "Os IDs 754, 792, 755, 793 — sao os programas de Cashback VIP?
Eles representam 99% do custo de bonus. Precisamos confirmar."

Mostrar a tabela de campanhas com GGR por campanha:
"A campanha 754 sozinha gera R$ 2,9M de GGR. O retorno e enorme."

---

## 5. Custos de Disparo (3 min)
Mostrar a tabela de disparos:

"Gastamos R$ 375K em disparos no mes. SMS Comtele e o mais caro (R$ 173K).
Popup Smartico e gratuito e tem o maior alcance (7M envios)."

**Perguntas:**
- "O custo do Popup realmente e zero?"
- "O 'outro/desconhecido' com 2.5M envios - o que e isso?"
- "Qual a verba mensal aprovada para disparos?"

---

## 6. Top Jogos e VIP (5 min)
**Top Jogos:**
"Os jogos mais jogados pela base CRM sao Fortune Rabbit (R$ 19M turnover),
Fortune Ox (R$ 14.5M), Fortune Tiger (R$ 8M). Sao PG Soft dominando."

**VIP (ACHADO IMPORTANTE):**
"Apenas 559 players (1.5% da base) geram R$ 4,39M de NGR — mais que 100% da
receita liquida. Os outros 36.551 tem NGR NEGATIVO. Ou seja, os VIPs subsidiam
a operacao inteira."

**Perguntar:** "Temos programa VIP dedicado? Account managers para os 99 Elite?"

---

## 7. Pendencias (5 min)
Entregar o PDF com as 7 pendencias e percorrer cada uma.
O mais importante:
1. Confirmar IDs 754/792/755/793
2. bonus_cost_value = BTR ou BG?
3. Verba mensal de disparos

---

## 8. Proximos Passos (2 min)
"Com o feedback de voces hoje, vamos:
- v0.5: Corrigir o que for necessario (3-5 dias)
- v1: Automatizar para rodar diariamente com dados frescos (1-2 semanas)
- v2: Dashboard em producao acessivel por todos (2-3 semanas)"

---

## Materiais para Levar
- [ ] Dashboard rodando no notebook (localhost:5051)
- [ ] PDF impresso (docs/documentacao_dashboard_crm_v0.pdf)
- [ ] Caneta para anotar feedback
- [ ] Lista de pendencias impressa

## Tom da Apresentacao
- **Confiante mas humilde:** "Temos dados solidos, queremos refinar com voces"
- **Orientado a decisao:** Cada secao termina com uma pergunta
- **Honesto sobre limitacoes:** GGR e de toda atividade do player, nao isolado por campanha
- **Proximo passo claro:** "Depois dessa reuniao, corrigimos em 3-5 dias e voltamos"
