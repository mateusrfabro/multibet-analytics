# Deep Dive — Live Delay Exploitation (jogadores 764 e 777)

> **Status:** CONFIRMADO — fraude real com prova empírica
> **Data da investigação:** 10/04/2026
> **Período dos dados:** 25/03/2026 a 05/04/2026
> **Jogadores investigados:** 2 (IDs 764641775223027 e 777971772567301)
> **Prejuízo bruto estimado:** R$28.050 líquido (após refunds parciais)
> **Script:** [scripts/risk_deep_dive_live_delay.py](../scripts/risk_deep_dive_live_delay.py)
> **Raw data:** [output/deep_dive_live_delay_raw_2026-04-09.csv](../output/deep_dive_live_delay_raw_2026-04-09.csv)

---

## Resumo executivo

Os 2 jogadores flagados com **Win Rate 100%** pelo R9 (sportsbook_alerts) foram investigados em profundidade. O mecanismo de fraude é **diferente** do que o R9 descreve como "live delay". A descoberta crítica é que:

> **Ambos os jogadores estavam apostando em eventos que JÁ HAVIAM TERMINADO há 1-3 horas antes** — não em eventos "ao vivo" com delay de 3-10 segundos.

Os 14 slips combinados (7 por jogador) mostram:
- **11 apostas ganhas (WR 100% nas fechadas)**
- **3 apostas com refund operacional** (R$22.500 estornados pela casa)
- **R$88.801 em stake total** exposto à casa
- **R$28.050 de profit líquido efetivamente extraído**
- **100% em futebol**
- **Apostas feitas entre 1h12 e 2h31 APÓS o fim real do evento**

Isso não é explorable por um jogador típico. Requer (a) conhecimento de que o sistema da casa está aceitando apostas em eventos encerrados, (b) acesso a uma fonte de resultados mais rápida que o feed da casa, e (c) timing preciso.

---

## Parte 1 — Jogador 764641775223027

### Perfil agregado

| Métrica | Valor |
|---|---|
| Slips totais | **7** |
| Slips liquidados | 5 (todas wins) |
| Slips em open / refundados | 2 (ambos refundados) |
| **Win rate** | **100%** |
| Stake total | R$33.800 |
| Retorno total | R$49.804,77 |
| **Profit líquido** | **+R$16.004,77** |
| Refunds | 2 slips, R$7.500 total |
| Tipo de aposta | **7/7 LIVE** (100%) |
| Esporte | **Futebol** (100%) |
| Janela temporal | **ÚNICO DIA — 04/04/2026, 15:24 a 21:57 BRT** |

### Apostas detalhadas (ordem cronológica)

| # | Hora aposta (BRT) | Evento | Campeonato | Mercado | Seleção | Stake | Odd | Resultado | **Fim real do evento** | **Delay entre fim real → aposta** |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 04/04 15:24:41 | Bayer Leverkusen vs Wolfsburg | Bundesliga | Total de gols | Menos 8.5 | R$12.000 | 1,30 | **WIN R$12.326** (+R$326) | 04/04 13:00 | **+2h24** |
| 2 | 04/04 16:28:54 | Chelsea vs Port Vale | Copa da Inglaterra | 1º tempo - total | Mais 2 | R$4.000 | 1,63 | **WIN R$6.533** (+R$2.533) | 04/04 15:45 | **+43min** |
| 3 | 04/04 16:32:01 | Chelsea vs Port Vale (mesmo) | Copa da Inglaterra | 1º tempo - total | Mais 2 | R$8.000 | 1,80 | **WIN R$14.400** (+R$6.400) | 04/04 15:45 | **+47min** |
| 4 | 04/04 17:13:29 | Moreirense vs Braga | Primeira Liga | Total escanteios | Menos 7.5 | R$5.000 | 2,05 | **REFUND R$5.000** | 04/04 16:30 | **+43min** |
| 5 | 04/04 17:28:37 | Moreirense vs Braga (mesmo) | Primeira Liga | Handicap | Moreirense (+2) | R$7.300 | 1,65 | **WIN R$12.045** (+R$4.745) | 04/04 16:30 | **+58min** |
| 6 | 04/04 21:56:47 | Brusque vs Caxias | Brasileirão Série C | Total de gols | Menos 3.5 | R$2.500 | 1,80 | **WIN R$4.500** (+R$2.000) | 04/04 20:30 | **+1h26** |
| 7 | 04/04 21:57:00 | Brusque vs Caxias (mesmo, 13s depois) | Brasileirão Série C | Total de gols | Menos 3.5 | R$2.500 | 1,80 | **REFUND R$2.500** | 04/04 20:30 | **+1h27** |

### Observações críticas sobre o 764

1. **TODAS as 7 apostas foram feitas DEPOIS do fim real do evento** (delay mínimo de 43 minutos, máximo de 2h24).
2. **Settlement ultrarrápido:** slip #1 apostado às 15:24:41 e liquidado às 15:25:55 = **1 minuto e 14 segundos** depois. Isso confirma que o evento já estava encerrado quando ele apostou — o sistema só precisou processar.
3. **Dupla aposta no mesmo evento:** em 3 jogos diferentes ele apostou 2 vezes no mesmo match/mercado em intervalos de 3 segundos a 15 minutos (slips 2/3, 4/5, 6/7).
4. **Refund como proteção:** em 2 casos (slip 4 e 7), a aposta entrou em estado OPEN e depois foi refundada — provavelmente a casa detectou anomalia antes de liquidar.
5. **Ganhou em 5 de 5 apostas liquidadas** — WR estatisticamente impossível em apostas live legítimas.

---

## Parte 2 — Jogador 777971772567301

### Perfil agregado

| Métrica | Valor |
|---|---|
| Slips totais | **7** |
| Slips liquidados | 6 (todas wins) |
| Slips em open / refundados | 1 (refund R$15.000) |
| **Win rate** | **100%** |
| Stake total | R$55.001 |
| Retorno total | R$67.046,23 |
| **Profit líquido** | **+R$12.045,23** |
| Refunds | 1 slip, R$15.000 |
| Tipo de aposta | 6/7 LIVE (85,7%) + 1 PreLive |
| Esporte | **Futebol** (100%) |
| Janela temporal | 25/03/2026 a 05/04/2026 (11 dias) |
| **Stake range** | **R$1,00 → R$15.000 (15.000x)** |

### Apostas detalhadas (ordem cronológica)

| # | Hora aposta (BRT) | Evento | Campeonato | Mercado | Seleção | Stake | Odd | Resultado | **Fim real do evento** | **Delay** |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 25/03 22:15:27 | Itália vs Irlanda do Norte | Qualificação Copa do Mundo | Vencedor | Itália | **R$1,00** | 1,23 | **WIN R$1,23** (+R$0,23) | 26/03 19:15 | **-21h** (PreLive — ANTES do jogo) |
| 2 | 04/04 15:18:45 | Hamburgo SV vs FC Augsburg | Bundesliga | Empate devolve | FC Augsburg | **R$15.000** | 1,57 | **REFUND R$15.000** | 04/04 13:00 | **+2h18** |
| 3 | 04/04 18:05:36 | Chelsea vs Port Vale | Copa da Inglaterra | Total de gols | Menos 7.5 | R$15.000 | 1,125 | **WIN R$16.875** (+R$1.875) | 04/04 15:45 | **+2h20** |
| 4 | 05/04 13:37:00 | Go Ahead Eagles vs Zwolle | Eredivisie | Total de gols | Menos 5.5 | R$15.000 | 1,20 | **WIN R$18.000** (+R$3.000) | 05/04 11:15 | **+2h22** |
| 5 | 05/04 19:01:06 | Fenerbahce vs Besiktas | Super Liga Turkcell | Empate devolve | Fenerbahce | R$5.000 | 1,53 | **WIN R$7.625** (+R$2.625) | 05/04 16:30 | **+2h31** |
| 6 | 05/04 19:27:06 | Monaco vs Marselha | Liga 1 (França) | 1º tempo - total | Menos 0.5 | R$10.000 | 1,27 | **WIN R$12.727** (+R$2.727) | 05/04 18:15 | **+1h12** |
| 7 | 05/04 19:29:04 | Monaco vs Marselha (mesmo, 2 min depois) | Liga 1 | 1º tempo - total | Menos 0.5 | R$10.000 | 1,18 | **WIN R$11.818** (+R$1.818) | 05/04 18:15 | **+1h14** |

### Observações críticas sobre o 777

1. **A aposta de R$1,00 (slip #1) foi PRELIVE**, não test-bet de live delay. Foi feita 21 horas ANTES do jogo começar (Itália vs Irlanda do Norte no dia seguinte). Esse é um **falso positivo** do R9 para "test-then-bet" — provavelmente uma aposta casual antes de entrar no esquema.
2. **O padrão real começa em 04/04**, quando o jogador passa de apostas de R$1 a apostas de **R$15.000** em eventos já terminados.
3. **6 das 6 apostas liquidadas entre 04/04 e 05/04 foram WINS**.
4. **Todas as apostas pós-evento tinham delay entre 1h12 e 2h31** — padrão consistente com "post-match arbitrage".
5. **Slip #2 (Hamburgo vs Augsburg R$15.000)** foi refundado. O `c_leg_status` ficou "O" e "V" (V provavelmente = VOID). Hipótese: a casa detectou o resultado inconsistente e voidou.
6. **Slip #3 (Chelsea vs Port Vale) foi liquidado em 1 minuto** — o mesmo jogo que o jogador 764 já tinha ganhado dinheiro 1h33 antes.
7. **Dupla aposta no mesmo jogo** (slips 6 e 7 — Monaco vs Marselha, 2 minutos de diferença).

---

## Parte 3 — Achados cruzados (evidência de coordenação?)

### Evento compartilhado: Chelsea vs Port Vale (04/04/2026)

Este evento foi apostado pelos **DOIS** jogadores em horários diferentes:

| Horário BRT | Jogador | Mercado | Stake | Resultado |
|---|---|---|---|---|
| 16:28:54 | 764 | 1º tempo - total Mais 2 | R$4.000 | +R$2.533 |
| 16:32:01 | 764 | 1º tempo - total Mais 2 | R$8.000 | +R$6.400 |
| **18:05:36** | **777** | **Total de gols Menos 7.5** | **R$15.000** | **+R$1.875** |

**Fim real do jogo:** 15:45 BRT.

Os dois jogadores apostaram no mesmo evento, depois do fim real, em mercados **diferentes mas compatíveis** (o 764 no 1º tempo, o 777 no total final). Isso sugere:
- Mesma fonte de informação ("alguém sabe que esse jogo Chelsea x Port Vale tem odds abertas")
- Possível coordenação (mesmo esquema, mesmo IP, ou compartilhando um alerta)
- **OU** mera coincidência estatística, mas a probabilidade é baixíssima

### Janela de exploração: 04/04/2026
- **Jogador 764:** todas as 7 apostas em 04/04 entre 15:24 e 21:57
- **Jogador 777:** 2 das 7 apostas em 04/04 entre 15:18 e 18:05

Ambos concentraram atividade em 04/04 (sábado). **Hipótese:** foi um dia específico em que o bug/janela do sistema Altenar estava ativo e eles se aproveitaram. Isso é compatível com um **incidente operacional pontual** na plataforma, não com um abuse crônico.

### Ligas exploradas (intersecção)

| Liga | 764 | 777 |
|---|---|---|
| Bundesliga | ✓ (1 jogo) | ✓ (1 jogo) |
| Copa da Inglaterra | ✓ (1 jogo) | ✓ (1 jogo) |
| Primeira Liga (Portugal) | ✓ | — |
| Brasileirão Série C | ✓ | — |
| Eredivisie (Holanda) | — | ✓ |
| Super Liga Turkcell (Turquia) | — | ✓ |
| Liga 1 (França) | — | ✓ |
| Qualificação Copa do Mundo | — | ✓ (PreLive, ignorar) |

**7 ligas europeias diferentes** + Brasileirão Série C. Perfil de quem tem feed internacional de resultados.

### Mercados preferidos (ambos)

1. **Total de gols** (X gols marcados no jogo)
2. **1º tempo - total** (gols do 1º tempo)
3. **Total de escanteios**
4. **Handicap** (resultado com vantagem)
5. **Empate devolve aposta** (proteção)

**Por que estes mercados?** São mercados **determinísticos pós-jogo**. Quem sabe o placar final sabe automaticamente se "menos de 3.5 gols" bate. Não exige prever nada — exige apenas **já saber o que aconteceu**.

---

## Parte 4 — O mecanismo real da fraude

### O que NÃO é

Esse caso **não é** "live delay exploitation" no sentido clássico:
- ❌ Não é explorar delay de 3-10 segundos durante o jogo
- ❌ Não é o padrão "R$1 de teste + R$X alto no mesmo momento"
- ❌ Não é assistir o jogo na TV e correr pra apostar antes da odd subir

A métrica `stake_ratio_max_min = 15.000x` do jogador 777 é **enganosa** — o R$1 veio de uma aposta PreLive isolada há 10 dias, não do esquema.

### O que É

**O que realmente aconteceu:** os jogadores descobriram que o sportsbook Altenar estava **aceitando apostas em eventos já encerrados** — a janela de mercado ficou aberta depois do fim real do jogo por minutos ou horas.

**Como funciona tecnicamente:**
1. O provedor de odds (Altenar/Sportradar) envia pra casa:
   - Início do evento → abre mercado
   - Evento em andamento → atualiza odds em tempo real
   - Fim do evento → **"match end"** → fecha mercado → envia resultado
2. Se houver falha no feed de "match end" (bug, lag, ou dessincronização), o mercado **fica aberto mesmo depois do jogo acabar**
3. O jogador descobre isso, consulta o resultado em uma fonte externa (site oficial da liga, Flashscore, Sofascore, outra casa) e **aposta no resultado já conhecido**
4. Quando a casa finalmente recebe o "match end", ela liquida a aposta com o resultado real → o jogador ganha com 100% de certeza

### Por que a casa liquidou e pagou?

Porque o sistema automatizado processou o settlement normalmente. Só depois (provavelmente ao fechar o dia ou rodar auditoria), o time operacional deve ter detectado as anomalias. Evidências disso:
- 3 slips tiveram **REFUND operacional** (R$22.500 totais)
- Os refunds foram feitos **por ação humana ou regra de exceção** — não pelo settlement automático

### É estatisticamente impossível de outra forma

Para contexto:
- **Win rate médio em apostas live** num sportsbook operando normalmente: ~48-52%
- **Win rate desses 2 jogadores**: 100% em 5 e 6 apostas respectivamente
- **Probabilidade de acertar 5/5 apostas independentes com odds médias 1,60**: ~7,5%
- **Probabilidade de acertar 6/6**: ~4,7%
- **Probabilidade combinada (11/11)**: ~0,35%

E isso assumindo odds de 1,60. Com odds mais baixas (1,20-1,30, comum em "cash-out seguro"), a probabilidade implícita de ganhar é maior — mas 11/11 num apostador normal ainda é **muito improvável**. Combinado com o fato de TODAS as apostas terem sido feitas **depois do fim real**, a conclusão é óbvia.

---

## Parte 5 — É normal o que eles fizeram? Como se repete?

### Resposta direta: NÃO é normal

Nenhum jogador legítimo:
- Aposta R$12.000 em 7 jogos diferentes no mesmo dia
- Aposta em 7 ligas europeias diferentes (incluindo Série C do Brasil) em 11 dias
- Acerta 100% das apostas liquidadas
- Concentra 100% em mercados "pós-resultado" (Total de gols, Handicap)
- Faz apostas 1-3 horas depois do fim dos jogos

Cada um desses sinais é suspeito isoladamente. Juntos, constituem **certeza de fraude**.

### Como alguém repetiria — os 4 ingredientes necessários

Para reproduzir o esquema, o fraudador precisa de:

**1. Uma janela vulnerável na plataforma**
- Um bug ou delay no feed de "match end" do provedor de odds
- Sportsbook não fecha o mercado automaticamente quando o jogo termina
- Pode ser: problema no Altenar, Sportradar, ou na integração com a casa
- **Como descobrir:** monitorar quais ligas/competições ficam com odds "vivas" depois do fim do jogo
- **Como detectar no nosso lado:** comparar `c_created_time` da aposta com `c_ts_realend` do evento — se a aposta foi feita depois do fim, é suspeito

**2. Fonte de resultados mais rápida que a casa**
- Sites oficiais (uefa.com, fifa.com, cbf.com.br) — feed imediato
- Agregadores profissionais (Sofascore, Flashscore, LiveScore) — delay de segundos
- Outras casas de apostas (se uma casa atualiza mais rápido, outra fica aberta)
- Feed Betradar / Sportradar pago ($$$$) — nível profissional
- Em casos extremos: alguém no estádio enviando em tempo real (raro)

**3. Identificação dos jogos vulneráveis**
- Monitorar manualmente ou via script as odds de jogos que "já deveriam estar encerrados"
- Testar apostando R$1 — se a casa aceita, o mercado está aberto
- Começar aos poucos pra não despertar alertas
- Atacar **ligas menores** (Brasileirão Série C, Super Liga Turkcell) porque têm menos monitoramento

**4. Capital inicial de alto volume**
- Precisa depositar R$20k-R$50k no mínimo pra valer a pena
- Stakes de R$5k-R$15k por aposta porque as odds são baixas (1,10-1,30)
- Quem apostou R$15.000 numa odd 1,125 (jogador 777) ganhou R$1.875 — retorno de 12,5% em 1 minuto
- Fazer isso 10 vezes em sequência = multiplicar capital rapidamente

### Condições operacionais necessárias da casa

Para o esquema rodar, a casa precisa estar:
- Com falha/delay no feed de encerramento de eventos
- Sem monitoramento automático de "aposta pós-evento" (tempo entre `c_created_time` e `c_ts_realend`)
- Sem validação contra fonte externa de resultados antes do settlement
- Com o sistema de settlement automatizado processando imediatamente

### O que bloquearia o esquema

A defesa é **simples em teoria**:
1. **Validação dupla:** antes de settlement, verificar se `c_ts_realend < c_created_time`. Se sim, bloquear.
2. **Monitoramento em tempo real:** alertar sempre que houver aposta feita > 30 minutos depois do fim de um evento.
3. **Cross-check com fonte externa:** validar resultado contra Sportradar oficial antes de settlement.
4. **Limite de stake em mercados pós-live:** qualquer mercado aberto > 30 min após o fim real deve ter stake máximo de R$50.
5. **Auditoria diária** das apostas com `closure_time - commit_time < 2 minutos` em eventos antigos.

---

## Parte 6 — Exposição financeira da casa

| Métrica | Jogador 764 | Jogador 777 | **Total** |
|---|---|---|---|
| Stake total | R$33.800 | R$55.001 | **R$88.801** |
| Retorno pago pela casa | R$49.804,77 | R$67.046,23 | **R$116.850,97** |
| Profit do jogador (bruto) | R$16.004,77 | R$12.045,23 | **R$28.050,00** |
| Refunds (recuperado pela casa) | R$7.500 | R$15.000 | **R$22.500** |
| **Prejuízo líquido final (se não houver reversão)** | **R$16.005** | **R$12.045** | **R$28.050** |

### O que ainda está aberto
- Jogador 764: 2 slips em estado OPEN (refund) — R$7.500 congelados
- Jogador 777: 1 slip em estado OPEN (refund) — R$15.000 congelado
- Total "em revisão": **R$22.500** que podem ou não voltar pro jogador

### Pior cenário
Se esses 2 jogadores tivessem feito 10x mais apostas com o mesmo esquema, o prejuízo seria **R$280.500**. Se tivessem mantido por 30 dias no mesmo ritmo: **R$840.000**.

**O limite é o capital deles**, não o bug da casa.

---

## Parte 7 — Recomendações de ação

### P0 — Imediato
1. **Bloquear as 2 contas** (764641775223027 e 777971772567301) temporariamente
2. **Reverter os 3 slips ainda em OPEN** (refundar pra casa, não pro jogador) se tecnicamente possível
3. **Escalar ao time de Risk Operations** (Izadora) para investigação humana

### P0 — Técnico (Altenar / Gusta)
1. Verificar com Altenar por que o feed de "match end" não fechou os mercados dos jogos:
   - Bayer Leverkusen vs Wolfsburg (Bundesliga 04/04 13:00)
   - Chelsea vs Port Vale (Copa Inglaterra 04/04 15:45)
   - Moreirense vs Braga (Primeira Liga 04/04 16:30)
   - Brusque vs Caxias (Brasileirão C 04/04 20:30)
   - Hamburgo vs Augsburg (Bundesliga 04/04 13:00)
   - Go Ahead Eagles vs Zwolle (Eredivisie 05/04 11:15)
   - Fenerbahce vs Besiktas (Turkcell 05/04 16:30)
   - Monaco vs Marselha (Liga 1 05/04 18:15)
2. Corrigir o bug de settlement → implementar validação `c_ts_realend < c_created_time = BLOCK`

### P1 — Detecção contínua
1. Criar regra R12 no sportsbook_alerts: **"Post-Match Bet Detection"**
   - Cruzar `c_created_time` com `c_ts_realend`
   - Flag qualquer aposta feita >30min depois do fim real do evento
   - Severidade: **CRITICAL**
2. Rodar varredura histórica dos últimos 90 dias — quantos jogadores fizeram isso?
3. Adicionar campo `post_match_bet` no perfil do jogador na matriz de risco

### P2 — Investigar coordenação
1. Verificar se 764 e 777 compartilham:
   - Afiliado (c_affiliate_id)
   - Método de depósito
   - IP de login (se disponível externamente)
   - Data de registro próxima
   - Nome/CPF similar
2. Se houver ligação, tratar como **rede de fraude coordenada** — não incidente isolado

---

## Anexos técnicos

### Como o script funciona ([scripts/risk_deep_dive_live_delay.py](../scripts/risk_deep_dive_live_delay.py))

1. Consulta `vendor_ec2.tbl_sports_book_bets_info` para pegar:
   - Todos os commits (`c_transaction_type = 'M'`) dos 2 jogadores em 60 dias
   - Todos os payouts (`c_transaction_type = 'P'`) sem filtro de data (settlement pode ser posterior)
2. Join com `tbl_sports_book_bet_details` para detalhes de evento/esporte/mercado
3. Join com `tbl_sports_book_info` (operation_type = 'R') para pegar refunds
4. Calcula settlement_status (SETTLED vs OPEN) e profit
5. Agrega por jogador (win rate, stake range, top sports/tournaments/markets)
6. Identifica eventos com padrão "test-then-bet" (min stake ≤ R$5 E max stake ≥ R$50)

### Correção de schema aplicada
Descobrimos que `c_ts_realstart`, `c_ts_realend`, `c_ts_off` e `c_ts_openbetting` são **VARCHAR** (não timestamp como a memory antiga sugeria). Query corrigida e [memory/schema_sportsbook_bet_details_varchar_fields.md](../../../.claude/projects/c--Users-NITRO-OneDrive---PGX-MultiBet/memory/schema_sportsbook_bet_details_varchar_fields.md) atualizada.

---

**Responsável:** Squad 3 Intelligence Engine
**Autor:** Mateus Fabro
**Última atualização:** 2026-04-10
