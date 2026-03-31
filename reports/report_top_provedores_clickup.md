# Top 10 Provedores por Turnover — Jan a Mar/2026

**Periodo:** 01/01/2026 a 25/03/2026
**Fonte primaria:** Athena (bireports_ec2) | **Validacao cruzada:** BigQuery Smartico
**Test users:** excluidos | **Gerado em:** 25/03/2026

---

## Ranking Top 10 Provedores (Casino)

| # | Provedor | Turnover (R$) | GGR (R$) | Hold Rate | Jogadores |
|---|----------|--------------|----------|-----------|-----------|
| 1 | PG Soft | 171.2M | 31.7M | 18.6% | 78.4K |
| 2 | Pragmatic Play | 111.1M | 14.5M | 13.1% | 97.8K |
| 3 | Spribe | 87.9M | 37.8M | 43.1% | 16.4K |
| 4 | Evolution | 37.0M | 1.1M | 2.9% | 8.3K |
| 5 | Playtech | 28.4M | 701K | 2.5% | 5.0K |
| 6 | Tada Gaming | 4.5M | 9.2K | 0.2% | 9.6K |
| 7 | Wazdan | 2.1M | -37K | -1.8% | 978 |
| 8 | Play'n GO | 1.1M | -65K | -5.8% | 1.1K |
| 9 | Gaming Corps | 764K | 54K | 7.0% | 1.5K |
| 10 | NetEnt | 728K | 20K | 3.7% | 851 |

**Total Top 10 Casino: R$ 444.8M**
**Sportsbook (Altenar): R$ 79.6M** (nao incluso acima — produto separado)

---

## Top 5 Jogos por Provedor (destaques)

### PG Soft (R$ 171.2M) — 38.5% do turnover casino
| Jogo | Turnover | GGR | Players |
|------|----------|-----|---------|
| Fortune Rabbit | R$ 37.6M | R$ 1.2M | 39.3K |
| Fortune Dragon | R$ 28.7M | R$ 1.1M | 28.8K |
| Fortune Tiger | R$ 19.7M | R$ 799K | 44.3K |
| Fortune Ox | R$ 14.7M | R$ 580K | 27.8K |
| Fortune Snake | R$ 7.0M | R$ 386K | 18.4K |

### Pragmatic Play (R$ 111.1M) — 25.0% do turnover casino
| Jogo | Turnover | GGR | Players |
|------|----------|-----|---------|
| Gates of Olympus | R$ 8.1M | R$ 306K | 63.3K |
| Sweet Bonanza 1000 | R$ 6.4M | R$ 116K | 14.1K |
| Gates of Olympus Super Scatter | R$ 4.7M | R$ 392K | 5.6K |
| Sugar Rush 1000 | R$ 4.0M | R$ 126K | 8.3K |
| Tigre Sortudo | R$ 3.9M | R$ 130K | 41.9K |

### Spribe (R$ 87.9M) — 19.8% do turnover casino
| Jogo | Turnover | GGR | Players |
|------|----------|-----|---------|
| **Aviator** | **R$ 47.4M** | R$ 1.7M | 13.6K |
| Mines | R$ 3.3M | R$ 86K | 2.9K |
| Goal | R$ 1.3M | R$ 17K | 1.3K |

### Playtech (R$ 28.4M)
| Jogo | Turnover | GGR | Players |
|------|----------|-----|---------|
| Roleta Brasileira Live | R$ 24.1M | R$ 654K | 3.1K |
| Mega Fire Blaze Roulette | R$ 1.3M | R$ 30K | 236 |
| Adventures Beyond Wonderland | R$ 693K | R$ 37K | 512 |

---

## Validacao Cruzada (3 fontes)

| Fonte | Total Casino | Divergencia |
|-------|-------------|-------------|
| Athena (bireports_ec2) | R$ 447.4M | base |
| BigQuery (Smartico CRM) | R$ 465.6M | +3.9% |
| Athena (daily_bi_summary) | R$ 373.4M | -16.6%* |

*daily_bi_summary usa criterio de liquidacao diferente — nao usar como fonte de turnover.

**Ranking confirmado:** os top 10 sao os mesmos nas 3 fontes. Divergencia Athena vs BigQuery < 5% para os 3 maiores (PG Soft, Pragmatic, Spribe).

Divergencias maiores em Evolution (+16%) e Playtech (+20%) — ambos live casino, onde Smartico registra mais eventos por round.

---

## Insights para o Negocio

1. **PG Soft domina casino com 38.5% do turnover** — serie "Fortune" (Rabbit, Dragon, Tiger, Ox) concentra 63% do volume do provider
2. **Aviator (Spribe) e o jogo com maior volume individual: R$ 47.4M** — apesar de ter apenas 13.6K players, o ticket medio e altissimo
3. **Spribe tem Hold Rate de 43.1%** — o mais alto da plataforma, muito acima da media. Crash games reteem mais
4. **Pragmatic tem a maior base de jogadores (97.8K)** mas nao o maior turnover — players com ticket menor
5. **Wazdan e Play'n GO com GGR negativo** — a casa esta perdendo dinheiro nesses providers. Avaliar se vale manter ou renegociar
6. **Roleta Brasileira (Playtech) concentra 85% do vendor** — dependencia alta de um unico jogo
7. **Tada Gaming tem 9.6K players mas Hold Rate de 0.2%** — quase zero de retencao, alto volume para baixo retorno

---

**Arquivo Excel completo:** `top_provedores_turnover_20260101_20260325_FINAL.xlsx`
(6 abas: Top 10 Provedores, Top 5 Jogos, Validacao Cruzada, Totais por Fonte, Legenda)