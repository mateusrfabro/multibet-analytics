# Casos de Fraude e Abuse Observados — MultiBet

> **O que este documento é:** dossiê dos padrões de fraude/abuse que **já aconteceram de fato** na MultiBet, com mecanismo, comportamento do player, IDs reais e valores observados.
> **Período das evidências:** 12/03/2026 a 09/04/2026.
> **Fonte:** investigações próprias em Athena (`fund_ec2`, `vendor_ec2`, `bonus_ec2`, `cashier_ec2`) + deep dives manuais.
> **Autor:** Squad 3 Intelligence Engine.

---

## CASO 1 — "R$1 de teste + Live Bet alto" (Live Delay Exploitation)

### Como funciona (mecanismo)
O jogador descobre um evento ao vivo onde a casa (Altenar/nossa plataforma) tem **delay** entre o que está acontecendo na realidade e a atualização das odds. Esse delay normalmente é de 3 a 10 segundos.

**A sequência exata:**
1. Jogador assiste o evento esportivo ao vivo em outra fonte (TV, stream pirata, feed oficial mais rápido)
2. Faz uma **aposta de teste** muito baixa (R$0,50 – R$5) pra validar se o sistema está aceitando a aposta e se a odd ainda não foi movimentada
3. Quando vê algo acontecer em tempo real (gol iminente, pênalti, cartão), **dispara uma aposta de valor muito alto** (R$500 – R$4.000) antes da odd na casa ser ajustada
4. Pela "janela" do delay, ele está apostando numa odd que **já não deveria existir**
5. Ganha com win rate anormalmente alto. Alguns repetem isso dezenas de vezes no mesmo evento.

Em alguns casos, o jogador também usa o **cashout** do sportsbook pra sair da aposta antes do evento terminar, fixando o lucro com base na odd movimentada (que ainda não refletiu o que ele já viu acontecer).

### Comportamento que delata
- **Concentração 100% live** (nada de pré-live)
- **Win rate absurdo** (≥ 80%, sendo que a média da casa é ~50%)
- **Stake range gigante** (diferença entre a menor e a maior aposta): chega a **600x, 4.000x**
- **Múltiplos eventos "test-bet"**: jogador com 13+ eventos distintos onde fez aposta pequena antes
- **Lucro desproporcional em pouco tempo**

### Casos reais detectados (período 30/03 – 06/04/2026)

| customer_id | Win Rate | Lucro | Stake Range | O que fez |
|---|---|---|---|---|
| **764641775223027** | **100%** | **R$16.005** | — | Lucrou R$16k com win rate 100% em live + 2 refunds de R$7.500 |
| **777971772567301** | **100%** | **R$12.045** | — | Lucrou R$12k WR 100% + 1 refund de R$15.000 num slip só |
| **580921773439598** | 75% | R$1.206 | **4.178x** (!) | Aposta menor de ~R$0,50, aposta maior de ~R$2.100. **Padrão clássico test-then-bet** |
| **145241775222133** | 69,3% | — | **607x** | 13 eventos diferentes com test-bet, 99% concentração live |
| **529971767804388** | 80% | R$5.405 | **600x** | 100% concentração live |
| **359551771766297** | 89,5% | R$5.379 | 99,2x | Padrão idêntico, escala menor |
| **775971774224242** | — | R$9.282 | 200x | Menor volume mas mesmo comportamento |
| **566091775399708** | 66,7% | R$8.901 | — | 100% live, alto lucro |
| **688951774096177** | 63,6% | R$5.995 | — | 91,3% live |

**Conclusão:** 52 jogadores flagados em 7 dias com o padrão. Os 2 "perfeitos" (IDs 764/777) tinham Win Rate 100% — o que estatisticamente é impossível em apostas limpas.

---

## CASO 2 — Micro-Bet Farming de Missões Fortune (12/03/2026)

### Como funciona (mecanismo)
A MultiBet rodava missões tipo "aposte R$150/R$300/R$500 em jogos Fortune (Tiger, Ox, Rabbit, Mouse, etc)". Grupos de jogadores descobriram que dava pra cumprir a missão **usando apostas no valor mínimo**, sem risco real.

**A sequência exata:**
1. Jogador abre o jogo Fortune Tiger (ou qualquer Fortune)
2. Coloca a aposta no **valor mínimo** (R$0,40 – R$0,50)
3. Dispara **300–1.200 apostas seguidas** em poucos minutos
4. Faz isso simultaneamente em 3–6 jogos Fortune diferentes
5. **Bate exatamente R$150 / R$300 / R$500 acumulados** (valor da missão) → recebe a recompensa
6. Saca imediatamente

### Comportamento que delata
- **AVG_BET ≈ R$0,47** (sempre colado no mínimo)
- **100% das apostas no valor mínimo**
- **UNIFORM_BETS** (stddev zero — todas iguais)
- **3–6 jogos Fortune diferentes** na mesma sessão
- **Total wagered NEAR R$150 / R$300 / R$500** (missão batida na bala)
- **Tempo entre apostas: 3–9 segundos** (ritmo de bot/clique frenético)
- Sessão dura 15 minutos a 1 hora, depois some

### Casos reais detectados (arquivo [anti_abuse_20260312_1847.csv](reports/anti_abuse_20260312_1847.csv))

| user_id | Bets | Avg Bet | Wagered | Alvo | Tempo entre bets | Sacou? |
|---|---|---|---|---|---|---|
| **235847142** | 307 | R$0,47 | **R$145,80** | R$150 | 5,1s | ✅ R$599 |
| **275050675** | 1.016 | R$0,50 | **R$505,40** | R$500 | 3,9s | ✅ R$100 |
| **235793116** | 684 | R$0,41 | **R$279,50** | R$300 | 8,9s | ✅ R$100 |
| **244348721** | 653 | R$0,47 | **R$307,30** | R$300 | 6,9s | — |
| **276058784** | 296 | R$0,49 | **R$143,80** | R$150 | 3,1s | ✅ R$50 |
| **265385147** | 1.235 | R$0,47 | **R$581,20** | R$500 | 60,9s | ✅ R$150 |

**Total dos casos ALTO na lista:** dezenas de jogadores farmando o mesmo alvo (R$150/R$300/R$500). O padrão é tão consistente que a assinatura "AVG_BET≈R$0,47 + UNIFORM_BETS + NEAR_R$150 + ALL_6_GAMES" aparece em todas as linhas do CSV.

### Ação tomada
Lista virou o script [analise_desbloqueio_fraudadores.py](scripts/analise_desbloqueio_fraudadores.py) (30/03/2026) que cruzou com GGR/NGR e matriz de risco. Decisão: **DESBLOQUEAR** (jogador valioso), **AVALIAR** (caso a caso) ou **MANTER BLOQUEADO** (conta criada só pra abusar).

---

## CASO 3 — Rollback Farming (buy-in + cancelamento)

### Como funciona (mecanismo)
Parecido com o Caso 2, mas mais **sofisticado**: em vez de apostar valores mínimos, o jogador faz **aposta de valor real + cancela a aposta** (rollback) antes do resultado.

**A sequência exata:**
1. Jogador entra num jogo qualquer
2. Faz buy-in de R$X (valor alto, pode ser R$50/R$100)
3. **Cancela a aposta antes do resultado** (`c_txn_type = 72 = CASINO_BUYIN_CANCEL`)
4. O sistema registra a aposta **e o rollback como duas transações válidas**
5. Na hora de contar turnover/missão, o turnover bruto fica inflado — o jogador bateu o valor da missão **sem jogar nada**
6. Faz isso milhares de vezes

### Comportamento que delata
- **Ratio de rollbacks > 10%** (anormal)
- **Volume de rollbacks gigante** (5.000+ em alguns casos)
- **Zero GGR real** mas **turnover inflado**
- Em contas fraudulentas: depósito = 0 mas rollback_brl = milhões

### Casos reais detectados

Do [risk_top_players_2026-04-06.csv](output/risk_top_players_2026-04-06.csv) — jogadores flagados como "WINNER" mas na verdade são rollback farmers:

| ecr_id | Depósito | Saque | Casino Bets | **Rollbacks** | O que aconteceu |
|---|---|---|---|---|---|
| **767001769095404749** | **R$0** | R$7.500 | R$1.753.302 | **6.937 rollbacks (R$1.75M)** | Zero depósito, turnover "fake" de R$3.5M, sacou R$7.500 |
| **881101771646854148** | **R$0** | R$4.200 | R$1.065.439 | **5.337 rollbacks (R$1.06M)** | Idêntico — zero dep, rollback farming |
| **561651771276152639** | R$200 | R$3.200 | R$1.044.604 | **5.225 rollbacks (R$1.04M)** | Depositou R$200, farmou R$1M em rollbacks, sacou R$3.200 |

**Observação:** estes 3 jogadores somados apostaram **R$3,8M "em casino"** que na verdade era 100% rollback. O GGR reportado na raw é R$7,7M (fake). O saque total foi R$14.900 (tudo lucro líquido extraído).

### Origem do incidente
Descoberto em 20/03/2026 quando Mauro/Castrin identificaram jogadores "batendo missões" sem GGR correspondente. A investigação apontou para o `c_txn_type = 72` como assinatura do abuse.

---

## CASO 4 — Free Spin Abuse (Murilo/Fabiano, abril/2026)

### Como funciona (mecanismo)
Jogadores recebem **campanhas de Free Spin** (rodadas grátis em jogos específicos). O retorno dos FS cai na carteira de bônus e depois pode ser convertido em real cash. A fraude acontece quando o jogador:

**A sequência exata:**
1. Participa de **todas as campanhas de FS** ativas (CRM não bloqueia elegibilidade)
2. Acumula **volume gigante de FS wins** (`c_txn_type = 80 = CASINO_FREESPIN_WIN`)
3. Os FS wins são convertidos em real cash via BTR (`c_txn_type = 20 = ISSUE_BONUS`)
4. O jogador **não deposita valor proporcional** — extrai o dinheiro dos FS sem risco próprio
5. Resultado: **revenue da casa fortemente negativo** com custo operacional alto

### Comportamento que delata
- **FS wins > R$500 no período**
- **Revenue negativo** (bets - wins < 0)
- **Ratio bônus emitido / depósito > 0,5** (ou depósito próximo de zero)
- **Múltiplas campanhas participadas simultaneamente**

### Casos reais detectados (deep dive 06/04/2026)

O Castrin trouxe dois nomes específicos e pediu investigação. Os números do **back-office Pragmatic** (fonte dele) divergem do Athena por 88–112x — indicando que o Athena não captura todas as transações de bônus manuais/campanhas.

**Caso Murilo (ecr_id 849167571791860514)** — dados do back-office:
| Métrica | Valor |
|---|---|
| Depósitos lifetime | 286 depósitos / **R$89.000** |
| Saques lifetime | R$45.612 |
| Casino bets | R$183.470 |
| **Bônus emitidos** | **R$66.500** |
| **Revenue 30d** | **-R$10.105** (Athena) / **-R$636.000** (back-office) |
| FS wins | R$114 (Athena, subestimado) |
| Classificação final | **SUSPECT — Suspender bônus, investigar manualmente** |

**Caso Fabiano (ecr_id 789175681790911033)** — dados do back-office:
| Métrica | Valor |
|---|---|
| Depósitos lifetime | 418 depósitos / **R$70.800** |
| Saques lifetime | R$29.773 |
| **Bônus emitidos** | **R$27.300** |
| **Revenue lifetime** | **-R$93,89** (Athena) / **-R$169.000** (back-office) |

**O gap crítico:** o back-office mostra Murilo com bônus emitidos de R$66.500 e revenue negativo de R$636k. O Athena mostra só R$749 de bônus e revenue próximo de zero. Isso indica que **uma camada inteira de transações de bônus/FS não está sendo replicada para o Iceberg** — e é por isso que esses dois casos só foram pegos quando o Castrin trouxe a lista manual do back-office.

### Ação pendente
Investigar por que Athena não captura os bônus da campanha. Sem resolver isso, **todo free spin abuser passa batido pela nossa detecção automática**.

---

## CASO 5 — Cancel/Refund Abuse em Sportsbook

### Como funciona (mecanismo)
O jogador aposta em eventos esportivos e depois **pede refund sistematicamente** via suporte ou através de mecanismos de cancelamento do sportsbook. Os refunds ficam registrados como `c_operation_type = 'R'` em `vendor_ec2.tbl_sports_book_info`.

**A sequência exata:**
1. Aposta R$X num evento (geralmente valor alto)
2. Durante ou pouco antes do resultado, aciona suporte com alguma justificativa (erro de odd, problema técnico, evento cancelado)
3. Recebe refund integral ou parcial
4. Repete o padrão em múltiplos eventos
5. Em casos extremos: **acerta 100% das apostas que ficam** e "cancela" só as que perdeu

### Comportamento que delata
- **Múltiplos refunds no período** (≥ 3)
- **Valor refundado alto** (≥ R$500 ou aposta individual com 100% refund)
- **Refund ratio > 30% do stake total**

### Casos reais detectados

| customer_id | Qtd Refunds | Valor Refundado | Ratio | Observação |
|---|---|---|---|---|
| **30129291** | **33 refunds** | R$1.449 | 23% | Volume anormal de cancelamentos (micro-valores) |
| **364771774793079** | **29 refunds** | **R$11.940** | **56%** | Refundou mais da metade do que apostou |
| **445481770685333** | 17 refunds | R$1.176 | 35% | |
| **167251774732347** | 11 refunds | — | — | |
| **296421773220517** | 12 refunds | — | — | |
| **545021775239834** | **1 refund** | **R$10.000** | **100%** | Slip único de R$10k, refundado integralmente |
| **738841773515431** | 2 refunds | R$5.000 | **100%** | Mesmo padrão — refund integral |
| **763401769025347** | 4 refunds | **R$14.778** | — | Maior valor absoluto refundado |

**Padrão alarmante:** vários casos com **100% refund ratio** em slip único de valor alto (R$5k–R$15k). Isso indica que o jogador aposta pesado e consegue cancelar via suporte quando vai perder.

---

## CASO 6 — Saque rápido pós-registro (welcome bonus abuse)

### Como funciona (mecanismo)
Contas são **criadas exclusivamente para extrair o welcome bonus** e sumir.

**A sequência exata:**
1. Jogador cria nova conta
2. Faz depósito mínimo (ou nenhum, depende da campanha)
3. Reclama o welcome bonus
4. Cumpre o wagering mínimo possível
5. **Saca em menos de 24h após o registro**
6. Nunca mais retorna à plataforma

### Comportamento que delata
- Saque > R$50 em < 24h após `registration_date`
- Atividade zerada após o saque
- Frequentemente vem de afiliados específicos ou tem nome/dados similares a outras contas (multi-accounting)
- 368 jogadores com esse padrão só num FDS (3 dias)

### Observação de padrão
Esse padrão convive frequentemente com o **Caso 3 (rollback farming)** e o **Caso 2 (micro-bet farming)**: a conta nova bate a missão via rollback/micro-bet, saca o bônus convertido e some.

---

## CASO 7 — Zero depósito LIFETIME + saque (origem suspeita)

### Como funciona (mecanismo)
Jogador **nunca depositou um centavo na vida** e mesmo assim consegue sacar valores relevantes. O saldo apareceu por alguma via não-transparente.

### Fontes do saldo (investigação 03/04/2026)

Dos **39 jogadores** que nunca depositaram mas sacaram:

| Origem | Qtd | Como apareceu o saldo |
|---|---|---|
| **SPORTSBOOK** (txn 59/112) | 19 | Apostaram esportivo com saldo bônus, ganharam, sacaram. Provavelmente welcome bonus sem depósito inicial |
| **ORIGEM DESCONHECIDA** | **12** | **Os mais suspeitos — não há registro de entrada, só saque** |
| **Estorno de saque** (txn 36) | 5 | Saque anterior foi revertido pela operadora, virou "saldo livre" de novo |
| **CASINO sem depósito** | 2 | Saldo casino sem `c_txn_type = 1`. Possível crédito manual da operadora |
| **BONUS** | 1 | Bônus gratuito convertido |

### O enigma dos 12 "ORIGEM DESCONHECIDA"
Sem explicação técnica até o momento. Hipóteses em aberto:
- **Crédito manual do CS** (`c_txn_type = 3 = REAL_CASH_ADDITION_BY_CS`) — suporte liberou saldo e não foi catalogado
- **Transação fora do fund_ec2** — pode ser bônus via back-office que não replica pro Athena
- **Transferência entre contas** — alguma forma de repasse interno não-mapeada

**Pendência crítica:** investigar essas 12 contas individualmente. Se houver um padrão comum (mesmo afiliado, mesma data, mesmo método), é provável indicação de **esquema interno coordenado**.

---

## CASO 8 — Velocity / Structuring (padrão AML)

### Como funciona (mecanismo)
Jogador faz **5+ movimentações financeiras em 1 hora**. Padrão clássico de **structuring** (quebrar grandes operações em várias pequenas pra escapar de thresholds de reporte anti-lavagem).

**Perfis observados:**
1. **Quebra de saque:** pedir 5 saques de R$2.000 em vez de 1 saque de R$10.000 (fugindo do threshold AML da operadora)
2. **Entrada/saída rápida:** depositar, apostar pouco, sacar o mesmo valor poucos minutos depois
3. **Teste de limites:** depositar R$500 várias vezes em sequência pra entender qual é o limite da casa

### Casos reais
866 jogadores flagados no FDS (3 dias) com 5+ transações em janelas de 1 hora. Destes, 13 tinham **combinação tripla R2+R3b+R6** (bônus abuse + saque desproporcional + velocity), indicando fraude coordenada — não comportamento fortuito.

---

## CASO 9 — Promo Only / Promo Chainer

### Como funciona (mecanismo)
Jogador **só deposita quando tem promoção**. 80%+ dos depósitos dele caem em dias de bônus. Quando não tem promo, a conta fica dormente.

### Sub-padrão: Promo Chainer
Jogador encadeia promoções sem atividade orgânica entre elas. Só entra na plataforma durante campanhas, pega o bônus, joga o mínimo pro wagering, saca (ou deixa cair na carteira) e some até a próxima promo.

### Impacto
- **Bônus emitido alto / GGR da casa baixo ou negativo**
- Campanhas CRM "parecem estar engajando" mas na verdade estão subsidiando 1 turno por mês do mesmo abuser

### Exemplo real (top_players)
Vários jogadores na lista de **LOSERS** (GGR negativo pra casa) têm padrão consistente:
- **554941774613627783:** 1 depósito de R$32.500 → **sacou R$55.000** → GGR -R$63.582. Todo o lucro veio de bônus.
- **895909571789770249:** depósito R$2.897 → **sacou R$80.061** → -R$58.631 pra casa (sportsbook puro)
- **485131768941208237:** R$10.891 depositado, R$60.000 sacado, 10 bônus distintos

---

## CASO 10 — Conta Nova + Depósito Alto + Sportsbook (flag operacional)

### Como funciona (mecanismo)
Conta recém-criada (< 7 dias) faz **depósito ≥ R$1.000** e imediatamente faz apostas em sportsbook. Padrão de **lavagem de dinheiro** ou conta **"boi de piranha"** (criada pra camuflar identidade real do apostador).

### Casos observados
Demanda da Izadora (head de riscos) em 07/04/2026 porque vários casos apareciam no back-office. Implementamos como R11 no alerta-ftd.

**Caso específico 08/04/2026:**
- **jogador 494321775567372:** conta nova + depósito de **R$9.000** + 1 bet sportsbook em estado "Open" (não liquidada)
- BKO mostrava aba Sportsbook vazia
- Decisão: manter como alerta — qualquer entrada no SB com esse perfil dispara

**Por que virar alerta:** jogadores legítimos geralmente começam com tickets pequenos e escalam gradualmente. Depósito de R$9k na primeira semana + primeira aposta alta em SB é indício forte de ou (a) teste antes de uma operação grande, ou (b) conta fantoche pra ocultar um high roller suspenso em outro lugar.

---

## Resumo — Como cada caso ataca a plataforma

| # | Caso | Vetor | Como extrai valor | Detecção |
|---|---|---|---|---|
| 1 | **Live Delay** | Sportsbook | Aposta alta pós-evento observado em fonte mais rápida | Win rate + stake range |
| 2 | **Micro-bet Fortune** | Missões Casino | Farmar missão com apostas mínimas uniformes | AVG_BET + UNIFORM_BETS + NEAR_R$X |
| 3 | **Rollback Farming** | Turnover de missões | Buy-in + cancelamento inflando turnover | `txn_type 72` em massa |
| 4 | **Free Spin Abuse** | Bônus/FS | Extrai FS wins sem depósito proporcional | Revenue neg + bônus > dep |
| 5 | **Cancel/Refund** | Sportsbook | Refund sistemático de apostas perdedoras | Qtd refunds / valor refundado |
| 6 | **Welcome Abuse** | Bônus novo cadastro | Conta nova → wagering mínimo → saque 24h | Time-to-cashout pós-registro |
| 7 | **Zero Dep Lifetime** | Origem oculta | Saldo aparece sem depósito rastreável | Saque sem histórico fund |
| 8 | **Velocity/Structuring** | AML | Fragmentar movimentação financeira | 5+ txns em 1h |
| 9 | **Promo Only/Chainer** | Campanhas CRM | Só entra em dia de promo, saca bônus | 80%+ atividade em dia de bonus |
| 10 | **Conta Nova + Dep Alto SB** | SB / Lavagem | Dep alto + 1 bet grande + some | Cruzamento signup × dep × SB |

---

## Gaps conhecidos na nossa detecção

1. **Não temos IP/device no Athena** — impossível correlacionar multi-accounting automático. Precisa vir de logs externos.
2. **Back-office Pragmatic mostra 88–112x mais bônus que o Athena** (caso Murilo/Fabiano). Existe uma camada de transações de bônus/FS que não é replicada pro Iceberg.
3. **Não sabemos a origem dos 12 jogadores "ZERO DEP LIFETIME"** classificados como ORIGEM_DESCONHECIDA.
4. **`txn_type = 3` (REAL_CASH_ADDITION_BY_CS)** nunca foi auditado sistematicamente — pode haver padrão de créditos manuais suspeitos feitos pelo suporte.
5. **Sem histórico de refund justification** — não sabemos POR QUE cada refund do Caso 5 foi concedido, só que foram.
6. **Freespin → BTR:** o valor real do BTR está na `tbl_realcash_sub_fund_txn`, não na `tbl_real_fund_txn`. Qualquer análise que olhe só a tabela principal vê zero — e fraude passa invisível.

---

## Investigações em aberto (o que precisa ser feito)

- [ ] Rodar SQL dedicado nos 12 jogadores "ORIGEM_DESCONHECIDA" do Caso 7 pra ver se compartilham afiliado, país, método, ou data de cadastro
- [ ] Pedir ao Castrin acesso ao relatório que ele usou no caso Murilo/Fabiano pra fechar o gap Athena vs back-office
- [ ] Auditar `c_txn_type = 3` dos últimos 90 dias — quem está recebendo crédito manual e por quê
- [ ] Cruzar refunds do Caso 5 com motivos operacionais (via `csm_ec2.tbl_mst_alert` ou back-office)
- [ ] Verificar se os jogadores do Caso 1 (Live Delay) repetiram o padrão em eventos específicos — pode haver uma liga/torneio onde o delay é sistematicamente explorável
- [ ] Mapear os 3 rollback farmers do Caso 3 (ecr_id 767/881/561) na matriz de risco — confirmar se estão flagados como ROLLBACK_PLAYER

---

**Última atualização:** 10/04/2026
**Referências nos scripts:**
- [scripts/analise_desbloqueio_fraudadores.py](scripts/analise_desbloqueio_fraudadores.py) — Caso 2/3 (micro-bet + rollback farming)
- [scripts/risk_deep_dive_player.py](scripts/risk_deep_dive_player.py) — Caso 4 (Murilo/Fabiano)
- [scripts/risk_sportsbook_alerts.py](scripts/risk_sportsbook_alerts.py) — Caso 1, 5, 10
- [scripts/risk_investigate_txn36.py](scripts/risk_investigate_txn36.py) — Caso 7 (estorno de saque)
- [scripts/risk_top_players.py](scripts/risk_top_players.py) — identificação de winners/losers suspeitos
- [reports/anti_abuse_20260312_1847.csv](reports/anti_abuse_20260312_1847.csv) — lista original do Caso 2
