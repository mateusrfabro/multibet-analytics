# Matriz de Risco v2 — MultiBet

> **Classificacao comportamental de jogadores baseada em 21 tags com scoring normalizado.**
> Pipeline 100% Athena | Snapshots historicos | Persistencia PostgreSQL

---

## Visao Geral

A Matriz de Risco classifica **100% da base de jogadores** (excluindo test users) em 5 tiers de risco/saude, atribuindo tags comportamentais com scores positivos e negativos. A soma das tags gera um score bruto, normalizado para 0-100.

| Campo | Valor |
|---|---|
| **Escopo** | Todos os jogadores nao-teste |
| **Janela padrao** | 90 dias (rolling) |
| **Fonte de dados** | AWS Athena (Iceberg Data Lake, sa-east-1) |
| **Databases** | ecr_ec2, fund_ec2, cashier_ec2, bonus_ec2, ps_bi |
| **Output** | 1 linha por jogador, 21 colunas de tags + score + tier |
| **Destino** | CSV + PostgreSQL (multibet.risk_tags) |
| **Pipeline** | `risk_matrix_pipeline.py` (auto-discovery de SQLs) |
| **Frequencia** | Sob demanda / Diario (quando automatizado na EC2) |

---

## Tiers de Classificacao

| Tier | Score Norm | Descricao | Acao Sugerida |
|---|---|---|---|
| **Muito Bom** | > 75 | Jogador legitimo, alto engajamento | Monitoramento, considerar pipeline VIP |
| **Bom** | 51 - 75 | Jogador ativo, deposita sem bonus | Manter elegibilidade a promocoes |
| **Mediano** | 26 - 50 | Mistura de comportamentos | Monitoramento passivo, reengajamento opcional |
| **Ruim** | 11 - 25 | Dependente de promos, risco medio | Restringir bonus, monitorar atividade |
| **Muito Ruim** | <= 10 | Multiplas flags criticas | Bloqueio temporario, investigacao completa |
| **SEM SCORE** | — | Nenhuma tag ativa | Jogador sem atividade suficiente para classificar |

---

## Formula de Normalizacao

```
score_norm = (score_bruto + 35) / 85 * 100
```

- Limitado entre **0** e **100**
- P05 empirico = **-35** | P95 empirico = **+50** | Range = **85**
- Pior caso teorico: **-90** | Melhor caso teorico: **+166**

---

## Tags — Visao Consolidada

### Resumo Rapido

| # | Tag | Score | Tipo | Categoria |
|---|---|---|---|---|
| 1 | REGULAR_DEPOSITOR | +10 | Positivo | Depositos |
| 2 | PROMO_ONLY | -15 | Negativo | Depositos |
| 3 | ZERO_RISK_PLAYER | 0 | Neutro | Depositos/Saques |
| 4 | FAST_CASHOUT | -25 | Negativo | Saques |
| 5 | SUSTAINED_PLAYER | +15 | Positivo | Comportamental |
| 6 | NON_BONUS_DEPOSITOR | +10 | Positivo | Depositos |
| 7 | PROMO_CHAINER | -10 | Negativo | Comportamental |
| 8 | CASHOUT_AND_RUN | -25 | Negativo | Comportamental |
| 9 | REINVEST_PLAYER | +15 | Positivo | Depositos |
| 10 | NON_PROMO_PLAYER | +10 | Positivo | Comportamental |
| 11 | ENGAGED_PLAYER | +10 | Positivo | Comportamental |
| 12 | RG_ALERT_PLAYER | -1 | Negativo | Comportamental |
| 13 | BEHAV_RISK_PLAYER | -10 | Negativo | Comportamental |
| 14 | POTENCIAL_ABUSER | -5 | Negativo | Comportamental |
| 15 | PLAYER_REENGAGED | +30 | Positivo | Reativacao |
| 16 | SLEEPER_LOW_PLAYER | +5 | Positivo | Sazonal |
| 17 | VIP_WHALE_PLAYER | +30 | Positivo | Transacional |
| 18 | WINBACK_HI_VAL_PLAYER | +25 | Positivo | Reativacao |
| 19 | BEHAV_SLOTGAMER | +5 | Positivo | Preferencia |
| 20 | MULTI_GAME_PLAYER | -10 | Negativo | Comportamental |
| 21 | ROLLBACK_PLAYER | -15 | Negativo | Transacional |

---

## Detalhamento das Tags

### Tags Positivas (indicam jogador saudavel)

---

### 1. VIP_WHALE_PLAYER — Score: +30

> Jogadores de altissimo valor com GGR expressivo e frequencia elevada.

| Parametro | Valor |
|---|---|
| **GGR minimo** | > R$ 15.000 |
| **Dias ativos minimos** | >= 10 dias distintos |
| **Janela** | 90 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |
| **Calculo GGR** | (soma bets - soma wins) / 100 (centavos para BRL) |
| **Txn types bets** | 27, 28, 41, 43, 59, 127 |
| **Txn types wins** | 45, 80, 112 |

**Logica:** Soma de todas as apostas menos ganhos do jogador no periodo. Se GGR > R$15k E jogador ativo em 10+ dias distintos, qualifica como VIP Whale.

---

### 2. PLAYER_REENGAGED — Score: +30

> Jogadores dormentes que retornaram apos longo periodo de inatividade com engajamento sustentado.

| Parametro | Valor |
|---|---|
| **Gap minimo de inatividade** | >= 30 dias |
| **Dias ativos recentes** | >= 3 dias nos ultimos 14 dias |
| **Janela historica** | 90 dias |
| **Janela recente** | 14 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |

**Logica:** Jogador tinha atividade ha mais de 30 dias, ficou inativo por 30+ dias, e voltou com 3+ dias de atividade nos ultimos 14 dias.

---

### 3. WINBACK_HI_VAL_PLAYER — Score: +25

> Jogadores reativados com GGR expressivo apos periodo de inatividade.

| Parametro | Valor |
|---|---|
| **Gap minimo de inatividade** | >= 30 dias |
| **GGR recente** | R$ 8.000 a R$ 15.000 |
| **Dias ativos recentes** | >= 5 dias nos ultimos 30 dias |
| **Janela historica** | 90 dias |
| **Janela recente** | 30 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |

**Logica:** Jogador reativado com GGR entre R$8k-R$15k e 5+ dias ativos no ultimo mes. Complementa VIP_WHALE para a faixa logo abaixo.

---

### 4. SUSTAINED_PLAYER — Score: +15

> Jogadores que sacam e mantem engajamento por varios dias — sinal de engajamento genuino sustentado.

| Parametro | Valor |
|---|---|
| **Condicao** | >= 3 dias distintos de apostas APOS o ultimo saque |
| **Janela** | 90 dias |
| **Fonte saques** | cashier_ec2.tbl_cashier_cashout |
| **Fonte apostas** | fund_ec2.tbl_real_fund_txn |
| **Txn types considerados** | 27, 28, 41, 43, 59, 127 (bets) |

**Logica:** Jogador fez pelo menos um saque no periodo E continua jogando em pelo menos 3 dias distintos depois disso. Indica engajamento sustentado, nao evento isolado.

> **Mudanca v2.1 (13/05/2026):** antes exigia apenas "qualquer atividade pos-saque", o que ativava em 31.2% da base (qualquer player ativo). Endurecido para >= 3 dias distintos. Reduz cobertura para ~10-15%.

---

### 5. REINVEST_PLAYER — Score: +15

> Jogadores com padrao de reinvestimento — multiplos ciclos saque->deposito.

| Parametro | Valor |
|---|---|
| **Ciclos minimos** | >= 2 ciclos saque->deposito |
| **Janela de reinvestimento** | <= 7 dias apos saque |
| **Janela geral** | 90 dias |
| **Fonte saques** | cashier_ec2.tbl_cashier_cashout |
| **Fonte depositos** | cashier_ec2.tbl_cashier_deposit |

**Logica:** Jogador com pelo menos 2 ciclos distintos saque->deposito (dentro de 7 dias cada). Padrao real de reinvestimento, nao evento unico.

> **Mudanca v2.1 (13/05/2026):** antes exigia apenas 1 ciclo, o que pegava qualquer jogador com fluxo normal de saldo. Endurecido para >= 2 ciclos. Reduz cobertura de 19.3% para ~5-8%.

---

### 6. REGULAR_DEPOSITOR — Score: +10

> Jogadores com depositos regulares e consistentes.

| Parametro | Valor |
|---|---|
| **Frequencia minima** | >= 3 depositos por mes (media) |
| **Janela** | 90 dias |
| **Status deposito** | txn_confirmed_success |
| **Fonte** | cashier_ec2.tbl_cashier_deposit |

**Logica:** Media de depositos por mes >= 3 no periodo de 90 dias. Indica compromisso financeiro consistente.

---

### 7. NON_BONUS_DEPOSITOR — Score: +10

> Jogadores que depositam sem utilizar bonus — engajamento organico.

| Parametro | Valor |
|---|---|
| **Depositos minimos** | >= 3 no periodo |
| **Uso de bonus** | Zero bonus no mesmo periodo |
| **Janela** | 90 dias |
| **Fonte depositos** | cashier_ec2.tbl_cashier_deposit |
| **Fonte bonus** | bonus_ec2.tbl_bonus_pocket_txn |

**Logica:** 3+ depositos confirmados E nenhuma transacao de bonus no periodo. Jogador 100% organico.

---

### 8. NON_PROMO_PLAYER — Score: +10

> Jogadores ativos recentemente sem usar nenhuma promocao.

| Parametro | Valor |
|---|---|
| **Atividade recente** | Apostas nos ultimos 7 dias |
| **Uso de bonus** | Zero bonus nos ultimos 7 dias |
| **Janela** | 7 dias |
| **Fonte apostas** | fund_ec2.tbl_real_fund_txn |
| **Fonte bonus** | bonus_ec2.tbl_bonus_pocket_txn |

**Logica:** Jogador com apostas na ultima semana sem nenhum bonus ativo. Sinal de engajamento independente de promos.

---

### 9. ENGAGED_PLAYER — Score: +10

> Jogadores com frequencia de sessoes saudavel (engajados mas nao compulsivos).

| Parametro | Valor |
|---|---|
| **Sessoes/dia (media)** | >= 3.0 e <= 10.0 |
| **Janela** | 90 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |
| **Metrica** | Sessoes distintas por dia, media no periodo |

**Logica:** Media diaria de sessoes entre 3 e 10. Abaixo de 3 = pouco engajado. Acima de 10 = alerta RG.

---

### 10. BEHAV_SLOTGAMER — Score: +5

> Jogadores predominantemente focados em casino/slots com volume real de apostas.

| Parametro | Valor |
|---|---|
| **Apostas casino minimas** | >= 20 bets |
| **Percentual casino** | >= 90% do total de apostas |
| **Deposito** | Pelo menos 1 deposito no periodo |
| **Janela** | 90 dias |
| **Txn types casino** | 27, 28, 41, 43 |
| **Filtro** | c_product_id = 'CASINO' |
| **Fonte** | fund_ec2.tbl_real_fund_txn, cashier_ec2.tbl_cashier_deposit |

**Logica:** 90%+ das apostas sao em casino E volume minimo de 20 bets E tem deposito. Identifica o slotgamer puro com skin in the game.

> **Mudanca v2.1 (13/05/2026):** thresholds endurecidos (70% -> 90%, 10 bets -> 20 bets). Antes ativava em 60.2% da base (era ruido); agora foca no slotgamer real. Reduz cobertura para ~12-15%.

---

### 11. SLEEPER_LOW_PLAYER — Score: +5

> Jogadores sazonais com atividade limitada mas presentes em eventos.

| Parametro | Valor |
|---|---|
| **Dias ativos** | >= 2 e <= 15 em 90 dias |
| **Bonus** | Pelo menos 1 participacao em bonus |
| **Janela** | 90 dias |
| **Fonte atividade** | fund_ec2.tbl_real_fund_txn |
| **Fonte bonus** | bonus_ec2.tbl_bonus_pocket_txn |

**Logica:** Jogador com pouca atividade (2-15 dias) mas que participa de eventos sazonais/promocoes. Sleeper que pode ser reativado.

---

### 12. RG_ALERT_PLAYER — Score: -1

> Jogadores com frequencia excessiva de sessoes — alerta de jogo responsavel.

| Parametro | Valor |
|---|---|
| **Mediana de sessoes/dia** | > 10.0 (APPROX_PERCENTILE 0.5) |
| **Dias ativos minimos** | >= 5 |
| **Janela** | 90 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |

**Logica:** Mediana diaria de sessoes acima de 10 sustentada por 5+ dias ativos. Mutuamente exclusiva com ENGAGED_PLAYER. Score negativo simbolico (-1) — flag de compliance, impacto numerico negligivel.

> **Mudanca v2.1 (13/05/2026):** score +1 -> -1, atendendo recomendacao Mauro de 07/04. Score positivo em flag de Jogo Responsavel era contradicao regulatoria. Cobertura nao muda (era 61.2%, segue similar); o que muda e o impacto no tier (1 ponto a menos em vez de 1 a mais).

---

### 13. ZERO_RISK_PLAYER — Score: 0

> Jogadores cujo valor medio de saque se aproxima do valor medio de deposito.

| Parametro | Valor |
|---|---|
| **Margem de tolerancia** | <= 50% de diferenca |
| **Formula** | \|avg_cashout - avg_deposit\| / avg_deposit <= 0.50 |
| **Janela** | 90 dias |
| **Fonte** | cashier_ec2.tbl_cashier_deposit, cashier_ec2.tbl_cashier_cashout |

**Logica:** Jogador conservador que saca valores similares ao que deposita. Tag neutra (score 0) — nao beneficia nem penaliza, so descreve perfil.

> **Mudanca v2.1 (13/05/2026):** tolerancia 30% -> 50%. Antes a tag retornava 0 jogadores (era inalcancavel na pratica — saque medio sempre diverge >30% do deposito medio). Alargada para deixar de ser dead code.

---

### Tags Negativas (indicam risco)

---

### 14. FAST_CASHOUT — Score: -25

> Padrao recorrente de deposito-saque em menos de 1h — possivel lavagem ou abuso sistematico.

| Parametro | Valor |
|---|---|
| **Intervalo maximo** | < 1 hora entre deposito e saque |
| **Ocorrencias minimas** | >= 3 pares deposito->saque rapido |
| **Janela** | 90 dias |
| **Status deposito** | txn_confirmed_success |
| **Status saque** | co_success |
| **Fonte** | cashier_ec2.tbl_cashier_deposit, cashier_ec2.tbl_cashier_cashout |
| **Non-stacking** | Se tambem aparecer CASHOUT_AND_RUN, FAST_CASHOUT e suprimido no score final |

**Logica:** Jogador com >= 3 pares deposito-saque rapido (<1h cada). Captura padrao recorrente, nao evento isolado. Possivel lavagem ou abuso sistematico.

> **Mudanca v2.1 (13/05/2026):** (1) threshold de >=1 ocorrencia para >=3 (captura padrao); (2) implementacao com EXISTS no lugar de JOIN cartesiano (reduz custo Athena drasticamente); (3) non-stacking com CASHOUT_AND_RUN no orquestrador (evita penalidade dupla -50). Reduz cobertura de 29.2% para ~5-10%.

---

### 15. CASHOUT_AND_RUN — Score: -25

> Jogador usa bonus, saca, e desaparece por 48+ horas.

| Parametro | Valor |
|---|---|
| **Sequencia** | Bonus -> Saque em ate 1 dia -> Inativo 48h |
| **Inatividade** | Ultima atividade <= data do saque + 2 dias |
| **Janela** | 90 dias |
| **Fonte bonus** | bonus_ec2.tbl_bonus_pocket_txn |
| **Fonte saques** | cashier_ec2.tbl_cashier_cashout |
| **Fonte atividade** | fund_ec2.tbl_real_fund_txn |

**Logica:** Jogador recebe bonus, faz saque no mesmo dia ou proximo, e nao tem atividade nos 2 dias seguintes. Padrao classico de bonus abuse.

> **Nota da avaliacao (Mauro, 07/04):** Logica fragil — compara ultima atividade GLOBAL com UMA data de cashout. Se jogador fez C&R em janeiro mas voltou em marco, nao detecta. Sugestao: verificar atividade apos CADA par bonus-cashout especifico.

---

### 16. PROMO_ONLY — Score: -15

> Jogador deposita quase exclusivamente durante periodos promocionais.

| Parametro | Valor |
|---|---|
| **Percentual minimo** | >= 80% dos depositos em dias de promo |
| **Depositos minimos** | >= 3 depositos totais |
| **Janela** | 90 dias |
| **Fonte depositos** | cashier_ec2.tbl_cashier_deposit |
| **Fonte bonus** | bonus_ec2.tbl_bonus_pocket_txn |

**Logica:** 80%+ dos depositos coincidem com dias em que o jogador recebeu bonus. Indica dependencia de promocoes para depositar.

---

### 17. ROLLBACK_PLAYER — Score: -15

> Taxa elevada de transacoes de rollback — possivel exploit tecnico.

| Parametro | Valor |
|---|---|
| **Rollbacks minimos** | >= 5 transacoes |
| **Taxa minima** | > 10% (rollbacks / total de bets) |
| **Txn types rollback** | 72, 76, 61, 63, 91, 113 |
| **Janela** | 90 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |

**Logica:** Jogador com 5+ rollbacks E taxa de rollback > 10% do total de apostas. Indica possivel exploracaoo de bugs ou tentativa de reverter transacoes.

---

### 18. PROMO_CHAINER — Score: -10

> Jogador que encadeia promocoes sem atividade organica entre elas.

| Parametro | Valor |
|---|---|
| **Dias de bonus distintos** | >= 3 |
| **Atividade em promos** | >= 80% dos dias ativos coincidem com dias de bonus |
| **Janela** | 90 dias |
| **Fonte bonus** | bonus_ec2.tbl_bonus_pocket_txn |
| **Fonte atividade** | fund_ec2.tbl_real_fund_txn |

**Logica:** 3+ dias distintos com bonus E 80%+ da atividade concentrada nesses dias. Jogador que so aparece quando tem promocao.

---

### 19. BEHAV_RISK_PLAYER — Score: -10

> Padroes comportamentais suspeitos em saques — horarios extremos ou valores anomalos.

| Parametro | Valor |
|---|---|
| **Saques minimos** | >= 3 no periodo |
| **Horario extremo** | 2h-5h AM (BRT) |
| **Percentual horario** | >= 30% dos saques em horario extremo |
| **OU desvio de valor** | Desvio padrao > 2x media dos valores |
| **Janela** | 90 dias |
| **Timezone** | AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' |
| **Fonte** | cashier_ec2.tbl_cashier_cashout |

**Logica:** Jogador com 3+ saques E (30%+ em horarios 2-5 AM BRT OU valores altamente variaveis). Ambos sao indicadores de comportamento irregular.

---

### 20. MULTI_GAME_PLAYER — Score: -10

> Sessoes simultaneas indicando possivel bot ou multi-accounting.

| Parametro | Valor |
|---|---|
| **Sessoes simultaneas minimas** | >= 3 na mesma hora |
| **Ocorrencias minimas** | >= 10 vezes no periodo |
| **Janela** | 90 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |

**Logica:** 3+ jogos diferentes na mesma hora, acontecendo 10+ vezes no periodo. Padrao de bot ou uso de multiplas contas.

---

### 21. POTENCIAL_ABUSER — Score: -5

> Conta muito nova — monitoramento preventivo.

| Parametro | Valor |
|---|---|
| **Idade da conta** | < 2 dias (baseado no primeiro deposito) |
| **Janela** | Ultimos 2 dias |
| **Fonte** | cashier_ec2.tbl_cashier_deposit |

**Logica:** Primeiro deposito do jogador ocorreu nos ultimos 2 dias. Tag temporaria — desaparece apos 2 dias se o jogador permanecer ativo.

> **Nota:** Usa primeiro deposito como proxy de data de registro (c_registration_time nao encontrado no ecr_ec2).

---

## Arquitetura Tecnica

### Pipeline

```
1. Auto-descobre SQLs em scripts/sql/risk_matrix/ (1 arquivo por tag)
2. Executa cada tag no Athena (Trino/Presto)
3. Pivota resultados: 1 linha por jogador, 1 coluna por tag
4. Calcula score_bruto (soma), score_norm (0-100), tier
5. Busca user_ext_id no ps_bi.dim_user (cross-ref CRM)
6. Persiste no Super Nova DB (multibet.risk_tags) com snapshot historico
7. Exporta CSV + legenda automatica
```

### Parametros de Execucao

| Parametro | Default | Descricao |
|---|---|---|
| `--date` | Hoje | Data do snapshot |
| `--window_days` | 90 | Janela em dias |
| `--only TAG1 TAG2` | Todas | Executar apenas tags especificas |
| `--dry-run` | False | Apenas CSV, sem gravar no PostgreSQL |

### Colunas de Identificacao

| Coluna | Descricao |
|---|---|
| `label_id` | ID do label/marca (c_partner_id do ECR) |
| `user_id` | ID interno do jogador (c_ecr_id, 18 digitos) |
| `user_ext_id` | ID externo Smartico (para cross-referencia CRM) |
| `snapshot_date` | Data do snapshot |
| `score_bruto` | Soma de todos os scores das tags ativas |
| `score_norm` | Score normalizado 0-100 |
| `tier` | Classificacao final |
| `computed_at` | Timestamp da execucao |

### Destinos

| Destino | Formato | Local |
|---|---|---|
| **CSV** | UTF-8 BOM | `reports/risk_matrix_YYYY-MM-DD_FINAL.csv` |
| **Legenda** | TXT | `reports/risk_matrix_YYYY-MM-DD_legenda.txt` |
| **PostgreSQL** | Tabela | `multibet.risk_tags` (snapshots historicos) |

### Filtros Globais

- `test_user = false` (via ecr_ec2.tbl_ecr_flags)
- `c_partner_id IS NOT NULL`
- Timestamps em UTC (converter para BRT em dashboards)

---

## Distribuicao de Scores

### Faixas Teoricas

| Metrica | Valor |
|---|---|
| **Melhor caso** | +166 (todas as tags positivas) |
| **Pior caso** | -90 (todas as tags negativas) |
| **P05 empirico** | -35 |
| **P95 empirico** | +50 |
| **Range de normalizacao** | 85 pontos |

### Composicao por Tipo

| Tipo | Qtd Tags | Score Min | Score Max |
|---|---|---|---|
| Positivas | 11 | +5 | +30 |
| Neutras | 1 | 0 | 0 |
| Negativas | 9 | -25 | -1 |

---

## Gaps Conhecidos e Melhorias Planejadas

> Avaliacao tecnica realizada em 07/04/2026 (Agente de Riscos — Squad 3).

### Gaps de Cobertura (vs Agente de Riscos R1-R10)

| Regra Agente | Cobertura na Matriz | Status |
|---|---|---|
| R1 Pico de jogo | Nao coberto | GAP |
| R2 Abuso bonus | Parcial (PROMO_ONLY + PROMO_CHAINER) | PARCIAL |
| R3a Zero deposito + saque | Nao coberto | GAP |
| R3b Ratio saque/deposito 5x+ | Nao coberto | GAP |
| R4 Rollbacks | ROLLBACK_PLAYER (binario) | COBERTO |
| R5 Sessoes simultaneas | MULTI_GAME_PLAYER (binario) | COBERTO |
| R6 Velocity check | Nao coberto | GAP |
| R7 Saque rapido pos-registro | Nao coberto | GAP |
| R8 Free spin abuse | Nao coberto | GAP |
| R9 Live delay (sportsbook) | Nao coberto | GAP |
| R10 Cancel/refund SB | Nao coberto | GAP |

### Melhorias Priorizadas

| Prioridade | Acao | Responsavel | Status |
|---|---|---|---|
| ~~P0~~ | ~~Corrigir RG_ALERT score~~ — **resolvido v2.1 (13/05): +1 -> -1** | Mateus | DONE |
| ~~P0~~ | ~~Validar logica CASHOUT_AND_RUN~~ — **MANTER como esta (Mauro 09/04)** | Mauro | DONE |
| ~~P0~~ | ~~ZERO_RISK_PLAYER dead code~~ — **resolvido v2.1 (13/05): tolerancia 30% -> 50%** | Mateus | DONE |
| ~~P1~~ | ~~Endurecer BEHAV_SLOTGAMER (60% da base)~~ — **resolvido v2.1 (13/05): threshold 70% -> 90%** | Mateus | DONE |
| ~~P1~~ | ~~Endurecer SUSTAINED_PLAYER (31% da base)~~ — **resolvido v2.1 (13/05): >=3 dias pos-saque** | Mateus | DONE |
| ~~P1~~ | ~~Endurecer REINVEST_PLAYER (19% da base)~~ — **resolvido v2.1 (13/05): >=2 ciclos** | Mateus | DONE |
| ~~P1~~ | ~~Reescrever FAST_CASHOUT (cartesiano)~~ — **resolvido v2.1 (13/05): EXISTS + >=3 ocorrencias** | Mateus | DONE |
| ~~P1~~ | ~~Non-stacking FC+CR (penalidade dupla -50)~~ — **resolvido v2.1 (13/05): supressao no orquestrador** | Mateus | DONE |
| **P1** | Adicionar 5 tags de fraude do Agente | Mateus + Mauro | TODO |
| **P1** | Separar tags casino vs sportsbook | Mauro | TODO |
| **P2** | Migrar CTE `users` de todas as 21 SQLs para `ps_bi.dim_user` (resolve duplicacao Iceberg + reduz custo) | Mateus | TODO |
| **P2** | GGR via `ps_bi.fct_player_activity_daily` (VIP_WHALE, WINBACK) | Mateus | TODO |
| **P2** | Bonus via `ps_bi.fct_bonus_activity_daily` (PROMO_ONLY, PROMO_CHAINER, NON_BONUS_DEPOSITOR) | Mateus | TODO |
| **P2** | Dividir Mediano em Alto (>37) e Baixo (<=37) | Mateus | TODO |
| **P3** | Implementar scoring gradual (nao binario) | Mateus | TODO |
| **P3** | Dashboard HTML unificado | Mateus | TODO |

### Tags Sugeridas para Adicao

| Tag Proposta | Score | Origem | Descricao |
|---|---|---|---|
| ZERO_DEPOSIT_CASHOUT | -30 | R3a | Zero deposito + saque > R$50 |
| VELOCITY_RISK | -15 | R6 | 5+ transacoes em 1 hora |
| FAST_REGISTRATION_CASH | -20 | R7 | Saque < 24h pos-registro, min R$50 |
| FREESPIN_ABUSER | -25 | R8 | FS wins > R$500 + revenue negativo |
| LIVE_DELAY_EXPLOITER | -20 | R9 | WR >= 55% live + lucro > R$200 |

---

## Repositorio e Arquivos

| Arquivo | Descricao |
|---|---|
| `scripts/risk_matrix_pipeline.py` | Pipeline principal (auto-discovery) |
| `scripts/sql/risk_matrix/*.sql` | 21 queries SQL (1 por tag) |
| `scripts/load_risk_csv_to_pg.py` | Carga manual CSV -> PostgreSQL |
| `reports/risk_matrix_*_FINAL.csv` | CSVs gerados por snapshot |
| `reports/risk_matrix_*_legenda.txt` | Legendas automaticas |
| `reports/avaliacao_risk_matrix_mauro_2026-04-07.txt` | Avaliacao tecnica completa |
| `reports/nota_entrega_matriz_risco.txt` | Nota de entrega ao Mauro |

---

## Historico de Versoes

| Data | Versao | Mudancas |
|---|---|---|
| 2026-04-06 | v2.0 | Migracao completa para Athena. 21 tags implementadas. Pipeline Python com auto-discovery. Persistencia PostgreSQL com snapshots historicos. |
| 2026-04-06 | v2.0 | 12 scores realinhados com Matriz Notion original. 9 SQLs com logica implementada (antes placeholder). 2 tags novas: MULTI_GAME_PLAYER, ROLLBACK_PLAYER. ENGAGED_AND_RG separado em ENGAGED_PLAYER + RG_ALERT_PLAYER. PLAYER_NOT_VALID removido. |
| 2026-04-07 | v2.0 | Avaliacao tecnica pelo Agente de Riscos. 10 pontos de melhoria documentados. Comparacao com R1-R10. Proposta de integracao score unificado. |
| 2026-04-15 | v2.0 | Integracao Smartico S2S em producao. Push diario de 21 tags + 5 tiers para `core_external_markers`. Cron EC2 02:30 BRT. |
| 2026-05-13 | v2.1 | **Auditoria + fixes de discriminacao.** RG_ALERT +1 -> -1 (compliance). BEHAV_SLOTGAMER 70% -> 90% (era 60% base). SUSTAINED_PLAYER exige >=3 dias pos-saque. REINVEST_PLAYER exige >=2 ciclos. FAST_CASHOUT exige >=3 ocorrencias (EXISTS no lugar de cartesiano). ZERO_RISK_PLAYER 30% -> 50% (era dead code). Non-stacking FAST_CASHOUT + CASHOUT_AND_RUN no orquestrador. |

---

> **Responsavel:** Squad 3 — Intelligence Engine
> **Ultima atualizacao:** 2026-05-13
> **Contato:** Mateus Fabro (analista) | Mauro (senior analytics)
