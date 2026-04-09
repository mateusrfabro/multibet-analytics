# Matriz de Risco v2 — MultiBet

> **Classificacao comportamental de jogadores baseada em 21 tags com scoring normalizado.**
> Pipeline 100% Athena | Snapshots historicos | Persistencia PostgreSQL

---

## Visao Geral

A Matriz de Risco classifica jogadores com **atividade financeira** (deposito ou aposta nos ultimos 90 dias) em 5 tiers de risco/saude, atribuindo tags comportamentais com scores positivos e negativos. A soma das tags gera um score bruto, normalizado para 0-100.

| Campo | Valor |
| --- | --- |
| **Escopo** | Jogadores com deposito ou aposta nos ultimos 90 dias (exclui test users e inativos) |
| **Janela padrao** | 90 dias (rolling) |
| **Fonte de dados** | AWS Athena (Iceberg Data Lake, sa-east-1) |
| **Databases** | ecr_ec2, fund_ec2, cashier_ec2, bonus_ec2, ps_bi |
| **Output** | 1 linha por jogador, 21 colunas de tags + score + tier |
| **Destino** | CSV + PostgreSQL (multibet.risk_tags) |
| **Pipeline** | `risk_matrix_pipeline.py` (auto-discovery de SQLs) |
| **Frequencia** | Diario (cron 02:00 BRT — EC2 ETL) |

---

## Tiers de Classificacao

| Tier | Score Norm | Descricao | Acao Sugerida |
| --- | --- | --- | --- |
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
- P05 calibrado = **-35** | P95 calibrado = **+50** | Range = **85**
- Pior caso teorico: **-115** | Melhor caso teorico: **+166**
- Calibracao empirica (09/04): mediana bruto=6 -> norm=48 (Mediano)

| score_bruto | score_norm | tier |
| --- | --- | --- |
| -35 | 0.0 | Muito Ruim |
| -25 | 11.8 | Ruim |
| -10 | 29.4 | Mediano |
| 0 | 41.2 | Mediano |
| +6 (mediana) | 48.2 | Mediano |
| +10 | 52.9 | Bom |
| +20 | 64.7 | Bom |
| +35 | 82.4 | Muito Bom |
| +50 | 100.0 | Muito Bom |

---

## Tags — Visao Consolidada

### Resumo Rapido

| # | Tag | Score | Tipo | Categoria |
| --- | --- | --- | --- | --- |
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
| 12 | RG_ALERT_PLAYER | +1 | Positivo | Comportamental |
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
| --- | --- |
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
| --- | --- |
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
| --- | --- |
| **Gap minimo de inatividade** | >= 30 dias |
| **GGR recente** | R$ 8.000 a R$ 15.000 |
| **Dias ativos recentes** | >= 5 dias nos ultimos 30 dias |
| **Janela historica** | 90 dias |
| **Janela recente** | 30 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |

**Logica:** Jogador reativado com GGR entre R$8k-R$15k e 5+ dias ativos no ultimo mes. Complementa VIP_WHALE para a faixa logo abaixo.

---

### 4. SUSTAINED_PLAYER — Score: +15

> Jogadores que sacam e continuam jogando — sinal de engajamento genuino.

| Parametro | Valor |
| --- | --- |
| **Condicao** | Atividade de apostas APOS o ultimo saque |
| **Janela** | 90 dias |
| **Fonte depositos** | cashier_ec2.tbl_cashier_cashout |
| **Fonte apostas** | fund_ec2.tbl_real_fund_txn |
| **Txn types considerados** | 27, 28, 41, 43, 59, 127 (bets) |

**Logica:** Jogador fez pelo menos um saque no periodo E tem registros de apostas DEPOIS do ultimo saque. Indica que nao "sacou e fugiu".

---

### 5. REINVEST_PLAYER — Score: +15

> Jogadores que sacam e reinvestem depositando novamente dentro de 7 dias.

| Parametro | Valor |
| --- | --- |
| **Janela de reinvestimento** | <= 7 dias apos saque |
| **Janela geral** | 90 dias |
| **Fonte saques** | cashier_ec2.tbl_cashier_cashout |
| **Fonte depositos** | cashier_ec2.tbl_cashier_deposit |

**Logica:** Apos um saque confirmado, o jogador faz um novo deposito dentro de 7 dias. Padrao saudavel de ciclo de vida.

---

### 6. REGULAR_DEPOSITOR — Score: +10

> Jogadores com depositos regulares e consistentes.

| Parametro | Valor |
| --- | --- |
| **Frequencia minima** | >= 3 depositos por mes (media) |
| **Janela** | 90 dias |
| **Status deposito** | txn_confirmed_success |
| **Fonte** | cashier_ec2.tbl_cashier_deposit |

**Logica:** Media de depositos por mes >= 3 no periodo de 90 dias. Indica compromisso financeiro consistente.

---

### 7. NON_BONUS_DEPOSITOR — Score: +10

> Jogadores que depositam sem utilizar bonus — engajamento organico.

| Parametro | Valor |
| --- | --- |
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
| --- | --- |
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
| --- | --- |
| **Sessoes/dia (media)** | >= 3.0 e <= 10.0 |
| **Janela** | 90 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |
| **Metrica** | Sessoes distintas por dia, media no periodo |

**Logica:** Media diaria de sessoes entre 3 e 10. Abaixo de 3 = pouco engajado. Acima de 10 = alerta RG.

---

### 10. BEHAV_SLOTGAMER — Score: +5

> Jogadores focados em casino/slots com historico de deposito.

| Parametro | Valor |
| --- | --- |
| **Apostas casino minimas** | >= 10 bets |
| **Percentual casino** | >= 70% do total de apostas |
| **Deposito** | Pelo menos 1 deposito no periodo |
| **Janela** | 90 dias |
| **Txn types casino** | 27, 28, 41, 43 |
| **Filtro** | c_product_id = 'CASINO' |
| **Fonte** | fund_ec2.tbl_real_fund_txn, cashier_ec2.tbl_cashier_deposit |

**Logica:** 70%+ das apostas sao em jogos de casino E tem deposito. Identifica o perfil "slot player" com skin in the game.

---

### 11. SLEEPER_LOW_PLAYER — Score: +5

> Jogadores sazonais com atividade limitada mas presentes em eventos.

| Parametro | Valor |
| --- | --- |
| **Dias ativos** | >= 2 e <= 15 em 90 dias |
| **Bonus** | Pelo menos 1 participacao em bonus |
| **Janela** | 90 dias |
| **Fonte atividade** | fund_ec2.tbl_real_fund_txn |
| **Fonte bonus** | bonus_ec2.tbl_bonus_pocket_txn |

**Logica:** Jogador com pouca atividade (2-15 dias) mas que participa de eventos sazonais/promocoes. Sleeper que pode ser reativado.

---

### 12. RG_ALERT_PLAYER — Score: +1

> Jogadores com frequencia excessiva de sessoes — alerta de jogo responsavel.

| Parametro | Valor |
| --- | --- |
| **Sessoes/dia (media)** | > 10.0 |
| **Janela** | 90 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |

**Logica:** Media diaria de sessoes acima de 10. Mutuamente exclusiva com ENGAGED_PLAYER. Score positivo minimo (+1) mas sinaliza necessidade de monitoramento RG.

> **Nota da avaliacao (Mauro, 07/04):** Score deveria ser 0 ou -5. Score positivo para jogo excessivo e contraditorio e pode gerar problemas regulatorios.

---

### 13. ZERO_RISK_PLAYER — Score: 0

> Jogadores cujo valor medio de saque se aproxima do valor medio de deposito.

| Parametro | Valor |
| --- | --- |
| **Margem de tolerancia** | <= 30% de diferenca |
| **Formula** | abs(avg_cashout - avg_deposit) / avg_deposit <= 0.30 |
| **Janela** | 90 dias |
| **Fonte** | cashier_ec2.tbl_cashier_deposit, cashier_ec2.tbl_cashier_cashout |

**Logica:** Jogador conservador que saca valores similares ao que deposita. Tag neutra (score 0) — nao beneficia nem penaliza.

---

### Tags Negativas (indicam risco)

---

### 14. FAST_CASHOUT — Score: -25

> Deposito seguido de saque em menos de 1 hora — padrao classico de abuso/lavagem.

| Parametro | Valor |
| --- | --- |
| **Intervalo maximo** | < 1 hora entre deposito e saque |
| **Janela** | 90 dias |
| **Status deposito** | txn_confirmed_success |
| **Status saque** | co_success |
| **Fonte** | cashier_ec2.tbl_cashier_deposit, cashier_ec2.tbl_cashier_cashout |

**Logica:** Qualquer par deposito-saque onde o saque ocorreu dentro de 1 hora apos o deposito. Flag critica de possivel lavagem de dinheiro.

---

### 15. CASHOUT_AND_RUN — Score: -25

> Jogador usa bonus, saca, e desaparece por 48+ horas.

| Parametro | Valor |
| --- | --- |
| **Sequencia** | Bonus -> Saque em ate 1 dia -> Inativo 48h |
| **Inatividade** | Ultima atividade <= data do saque + 2 dias |
| **Janela** | 90 dias |
| **Fonte bonus** | bonus_ec2.tbl_bonus_pocket_txn |
| **Fonte saques** | cashier_ec2.tbl_cashier_cashout |
| **Fonte atividade** | fund_ec2.tbl_real_fund_txn |

**Logica:** Jogador recebe bonus, faz saque no mesmo dia ou proximo, e nao tem atividade nos 2 dias seguintes. Padrao classico de bonus abuse. Detecta quem esta inativo AGORA apos o padrao — se o jogador voltou depois, saiu do comportamento.

> **Validacao (Mauro, 09/04):** Logica mantida como esta. A deteccao de inatividade atual (nao retroativa) e o comportamento operacional correto. Versao por par especifico pode entrar em revisao futura se o negocio precisar do historico.

---

### 16. PROMO_ONLY — Score: -15

> Jogador deposita quase exclusivamente durante periodos promocionais.

| Parametro | Valor |
| --- | --- |
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
| --- | --- |
| **Rollbacks minimos** | >= 5 transacoes |
| **Taxa minima** | > 10% (rollbacks / total de bets) |
| **Txn types rollback** | 72, 76, 61, 63, 91, 113 |
| **Janela** | 90 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |

**Logica:** Jogador com 5+ rollbacks E taxa de rollback > 10% do total de apostas. Indica possivel exploracao de bugs ou tentativa de reverter transacoes.

---

### 18. PROMO_CHAINER — Score: -10

> Jogador que encadeia promocoes sem atividade organica entre elas.

| Parametro | Valor |
| --- | --- |
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
| --- | --- |
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
| --- | --- |
| **Sessoes simultaneas minimas** | >= 3 na mesma hora |
| **Ocorrencias minimas** | >= 10 vezes no periodo |
| **Janela** | 90 dias |
| **Fonte** | fund_ec2.tbl_real_fund_txn |

**Logica:** 3+ jogos diferentes na mesma hora, acontecendo 10+ vezes no periodo. Padrao de bot ou uso de multiplas contas.

> **Correcao (09/04):** SQL corrigido de HAVING > 1 (2+ sessoes) para >= 3, alinhando com a especificacao. Validado pelo Mauro.

---

### 21. POTENCIAL_ABUSER — Score: -5

> Conta muito nova — monitoramento preventivo.

| Parametro | Valor |
| --- | --- |
| **Idade da conta** | < 2 dias (baseado no primeiro deposito) |
| **Janela** | Ultimos 2 dias |
| **Fonte** | cashier_ec2.tbl_cashier_deposit |

**Logica:** Primeiro deposito do jogador ocorreu nos ultimos 2 dias. Tag temporaria — desaparece apos 2 dias se o jogador permanecer ativo.

> **Nota:** Usa primeiro deposito como proxy de data de registro (c_registration_time nao encontrado no ecr_ec2).

---

## Arquitetura Tecnica

### Pipeline

```
1. Auto-descobre SQLs em sql/risk_matrix/ (1 arquivo por tag)
2. Executa cada tag no Athena (Trino/Presto)
3. Pivota resultados: 1 linha por jogador, 1 coluna por tag
4. Calcula score_bruto (soma), score_norm (0-100), tier
5. Busca user_ext_id no ps_bi.dim_user (cross-ref CRM)
6. Persiste no Super Nova DB (multibet.risk_tags) com snapshot historico
7. Exporta CSV + legenda automatica
```

### Parametros de Execucao

| Parametro | Default | Descricao |
| --- | --- | --- |
| `--date` | Hoje | Data do snapshot |
| `--window_days` | 90 | Janela em dias |
| `--only TAG1 TAG2` | Todas | Executar apenas tags especificas |
| `--dry-run` | False | Apenas CSV, sem gravar no PostgreSQL |

### Colunas de Identificacao

| Coluna | Descricao |
| --- | --- |
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
| --- | --- | --- |
| **CSV** | UTF-8 BOM | `output/risk_matrix_YYYY-MM-DD_FINAL.csv` |
| **Legenda** | TXT | `output/risk_matrix_YYYY-MM-DD_legenda.txt` |
| **PostgreSQL** | Tabela | `multibet.risk_tags` (snapshots historicos) |

### Filtros Globais

- `test_user = false` (via ecr_ec2.tbl_ecr_flags)
- `c_partner_id IS NOT NULL`
- Atividade financeira: deposito (`txn_confirmed_success`) ou aposta (`SUCCESS`) nos ultimos 90 dias
- `c_txn_status = 'SUCCESS'` em todas as queries fund_ec2
- Timestamps em UTC (converter para BRT em dashboards)

---

## Distribuicao de Scores

### Resultado atual (09/04/2026) — 177.633 jogadores

| Tier | Jogadores | % |
| --- | --- | --- |
| **Mediano** | 68.481 | 38.6% |
| **Bom** | 59.712 | 33.6% |
| **Muito Bom** | 13.310 | 7.5% |
| **Ruim** | 12.721 | 7.2% |
| **SEM SCORE** | 12.455 | 7.0% |
| **Muito Ruim** | 10.954 | 6.2% |

### Faixas Teoricas

| Metrica | Valor |
| --- | --- |
| **Melhor caso** | +166 (todas as tags positivas) |
| **Pior caso** | -115 (todas as tags negativas) |
| **P05 calibrado** | -35 |
| **P95 calibrado** | +50 |
| **Range de normalizacao** | 85 pontos |

### Composicao por Tipo

| Tipo | Qtd Tags | Score Min | Score Max |
| --- | --- | --- | --- |
| Positivas | 12 | +1 | +30 |
| Neutras | 1 | 0 | 0 |
| Negativas | 8 | -25 | -5 |

---

## Deploy e Automacao

### EC2 ETL (Cron Diario)

| Item | Valor |
| --- | --- |
| **Servidor** | EC2 ETL (54.197.63.138) |
| **Horario** | 02:00 BRT (05:00 UTC) |
| **Crontab** | `0 5 * * * /home/ec2-user/multibet/run_risk_matrix.sh` |
| **Tempo estimado** | 15-30 min (21 queries Athena + COPY PostgreSQL) |
| **Logs** | `pipelines/logs/risk_matrix_YYYY-MM-DD.log` |

### Estrutura na EC2

```
/home/ec2-user/multibet/
├── pipelines/risk_matrix_pipeline.py
├── sql/risk_matrix/ (21 arquivos .sql)
├── run_risk_matrix.sh
└── output/ (CSVs + legendas)
```

---

## Melhorias Pendentes

| Prioridade | Acao | Status | Responsavel |
| --- | --- | --- | --- |
| **P0** | Corrigir RG_ALERT score (+1 para 0 ou -5) | Pendente decisao final | Mateus |
| ~~P0~~ | ~~Validar logica CASHOUT_AND_RUN~~ | Manter como esta (Mauro 09/04) | Mauro |
| **P1** | Adicionar 5 tags de fraude do Agente de Riscos | Pendente | Mateus + Mauro |
| **P1** | Separar tags casino vs sportsbook | Pendente | Mauro |
| **P2** | Implementar scoring gradual (nao binario) | Pendente | Mateus |
| **P2** | Script de merge score unificado | Pendente | Mateus |
| **P3** | Otimizar custo Athena (CTE compartilhada) | Pendente | Gusta |
| **P3** | Dashboard HTML unificado | Pendente | Mateus |

---

## Historico de Versoes

| Data | Mudancas |
| --- | --- |
| 2026-04-06 | v2.0 — Migracao completa para Athena. 21 tags implementadas. Pipeline Python com auto-discovery. Persistencia PostgreSQL com snapshots historicos. |
| 2026-04-06 | 12 scores realinhados com Matriz Notion. 9 SQLs com logica implementada. 2 tags novas: MULTI_GAME_PLAYER, ROLLBACK_PLAYER. |
| 2026-04-07 | Avaliacao tecnica pelo Agente de Riscos. 10 pontos de melhoria documentados. |
| 2026-04-09 | MULTI_GAME_PLAYER corrigido (HAVING >= 3). CASHOUT_AND_RUN validado (manter). Normalizacao recalibrada: range 50 -> 85 (P05=-35, P95=50). Base filtrada por atividade financeira (deposito/aposta 90d). Filtro c_txn_status=SUCCESS adicionado em 4 SQLs. View multibet.matriz_risco atualizada. Deploy EC2 ETL preparado (cron diario 02:00 BRT). |

---

> **Responsavel:** Squad 3 — Intelligence Engine
> **Ultima atualizacao:** 2026-04-09
> **Contato:** Mateus Fabro (analista) | Mauro (senior analytics)
