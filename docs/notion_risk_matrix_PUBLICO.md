# Matriz de Risco — Classificação Comportamental de Jogadores

> Modelo de scoring que classifica jogadores em 5 níveis de saúde/risco com base em 21 comportamentos observados nos dados transacionais da plataforma.

---

## O que é a Matriz de Risco?

A Matriz de Risco é um sistema automatizado que analisa o comportamento de cada jogador ativo e atribui uma **classificação de risco** baseada em dados reais da operação.

Ela responde à pergunta: **"Este jogador é saudável para a operação ou representa risco?"**

O modelo roda diariamente sobre os dados dos últimos 90 dias, analisando depósitos, saques, apostas, uso de bônus e padrões de sessão.

---

## Como funciona

```
1. Coleta dados transacionais dos últimos 90 dias
2. Avalia 21 comportamentos (tags) para cada jogador
3. Soma os pontos de cada tag ativa (score bruto)
4. Normaliza para escala de 0 a 100
5. Classifica em 5 níveis (tiers)
6. Publica as tags no CRM via API (atualização diária automática)
```

---

## Classificações (Tiers)

| Classificação | Score | Tag no CRM | O que significa | Ação recomendada |
| --- | --- | --- | --- | --- |
| **Muito Bom** | Acima de 75 | `RISK_TIER_MUITO_BOM` | Jogador legítimo, alta frequência e valor | Retenção premium, pipeline VIP |
| **Bom** | 51 a 75 | `RISK_TIER_BOM` | Jogador ativo e saudável | Manter elegibilidade a promoções |
| **Mediano** | 26 a 50 | `RISK_TIER_MEDIANO` | Comportamento misto (positivo e negativo) | Monitoramento, reengajamento |
| **Ruim** | 11 a 25 | `RISK_TIER_RUIM` | Dependente de promoções, sinais de risco | Restringir bônus, monitorar |
| **Muito Ruim** | 10 ou menos | `RISK_TIER_MUITO_RUIM` | Múltiplos comportamentos de risco | Encaminhar para Compliance, suspender bônus |
| **Sem Score** | — | *(não enviada)* | Atividade insuficiente para classificar | Acompanhar |

---

## As 21 Tags Comportamentais

Cada tag é um comportamento identificado automaticamente nos dados. Tags positivas indicam jogador saudável; tags negativas indicam risco. A coluna **Tag no CRM** mostra o nome exato que aparece no Smartico (campo `core_external_markers`).

### Tags Positivas (indicam jogador saudável)

| Tag | Pontos | Tag no CRM | O que identifica |
| --- | --- | --- | --- |
| VIP Whale | +30 | `RISK_VIP_WHALE_PLAYER` | Jogador de altíssimo valor (GGR elevado + alta frequência) |
| Reengajado | +30 | `RISK_PLAYER_REENGAGED` | Voltou após 30+ dias inativo e manteve engajamento |
| Winback Alto Valor | +25 | `RISK_WINBACK_HI_VAL_PLAYER` | Reativado com GGR expressivo |
| Sustentado | +15 | `RISK_SUSTAINED_PLAYER` | Continua jogando após sacar (não "saca e foge") |
| Reinvestidor | +15 | `RISK_REINVEST_PLAYER` | Saca e deposita novamente em até 7 dias |
| Depositante Regular | +10 | `RISK_REGULAR_DEPOSITOR` | Deposita regularmente (3+ vezes por mês) |
| Orgânico (sem bônus) | +10 | `RISK_NON_BONUS_DEPOSITOR` | Deposita sem usar bônus |
| Ativo sem Promo | +10 | `RISK_NON_PROMO_PLAYER` | Ativo na última semana sem usar promoção |
| Engajado | +10 | `RISK_ENGAGED_PLAYER` | 3 a 10 sessões por dia (nível saudável) |
| Slot Player | +5 | `RISK_BEHAV_SLOTGAMER` | Focado em slots com depósito (perfil cassino) |
| Sazonal | +5 | `RISK_SLEEPER_LOW_PLAYER` | Jogador que aparece em eventos/promoções sazonais |
| Alerta RG | +1 | `RISK_RG_ALERT_PLAYER` | 10+ sessões/dia — sinal de jogo responsável |

### Tag Neutra

| Tag | Pontos | Tag no CRM | O que identifica |
| --- | --- | --- | --- |
| Zero Risco | 0 | `RISK_ZERO_RISK_PLAYER` | Valor de saque próximo ao de depósito (conservador) |

### Tags Negativas (sinais de risco)

| Tag | Pontos | Tag no CRM | O que identifica |
| --- | --- | --- | --- |
| Saque Rápido | -25 | `RISK_FAST_CASHOUT` | Deposita e saca em menos de 1 hora |
| Cashout & Run | -25 | `RISK_CASHOUT_AND_RUN` | Usa bônus, saca e desaparece por 48h+ |
| Só Promoção | -15 | `RISK_PROMO_ONLY` | Só deposita quando tem promoção (80%+) |
| Rollback Alto | -15 | `RISK_ROLLBACK_PLAYER` | Taxa de cancelamentos acima de 10% |
| Encadeador de Promos | -10 | `RISK_PROMO_CHAINER` | Encadeia promoções sem jogo orgânico |
| Comportamento Suspeito | -10 | `RISK_BEHAV_RISK_PLAYER` | Saques em horários extremos ou valores anômalos |
| Multi-Sessão | -10 | `RISK_MULTI_GAME_PLAYER` | 3+ jogos simultâneos na mesma hora (possível bot) |
| Conta Nova | -5 | `RISK_POTENCIAL_ABUSER` | Conta com menos de 2 dias (monitoramento preventivo) |

---

## Fórmula de Scoring

### Score bruto

A soma dos pontos de todas as tags ativas do jogador.

**Exemplos:**
- Jogador com Depositante Regular (+10) + Engajado (+10) + Reinvestidor (+15) = **+35 pontos**
- Jogador com Saque Rápido (-25) + Só Promoção (-15) = **-40 pontos**
- Jogador sem nenhuma tag = **Sem Score**

### Normalização (0 a 100)

O score bruto é convertido para uma escala de 0 a 100 usando a fórmula:

```
Score normalizado = (score bruto + 35) / 85 × 100
```

Limitado entre 0 e 100. Calibrado com percentis reais da base.

| Score bruto | Score normalizado | Classificação |
| --- | --- | --- |
| -35 ou menos | 0 | Muito Ruim |
| -25 | 12 | Ruim |
| -10 | 29 | Mediano |
| 0 | 41 | Mediano |
| +6 (mediana típica) | 48 | Mediano |
| +10 | 53 | Bom |
| +20 | 65 | Bom |
| +35 | 82 | Muito Bom |
| +50 ou mais | 100 | Muito Bom |

---

## Regras e Parâmetros

### Base de jogadores

- **Quem entra:** jogadores com pelo menos 1 depósito confirmado OU 1 aposta realizada nos últimos 90 dias.
- **Quem não entra:** contas de teste e jogadores sem atividade financeira no período.

### Filtros de qualidade

- Apenas transações confirmadas/efetivadas (exclui tentativas falhadas).
- Contas de teste excluídas via flag do sistema.
- Timestamps em UTC, convertidos para horário local onde aplicável.

### Janela temporal

- **90 dias rolling** — cada dia a janela avança, sempre olhando os últimos 3 meses.
- Exceções: "Conta Nova" (-5) olha apenas os últimos 2 dias; "Ativo sem Promo" (+10) olha os últimos 7 dias.

---

## Como cada área pode usar

### CRM e Retenção

| Uso | Como |
| --- | --- |
| Segmentação por tier | Criar segmentos automáticos (VIP, Saudável, Monitorar, Restrito, Investigar) |
| Micro-segmentação | Combinar tags (ex.: `RISK_PROMO_ONLY` + `RISK_PROMO_CHAINER` = "Bonus Grinders") |
| Bônus proporcional | Usar score 0–100 para personalizar valor do bônus |
| Reativação | Identificar jogadores com `RISK_PLAYER_REENGAGED` para campanhas dedicadas |

### Riscos e Compliance

| Uso | Como |
| --- | --- |
| Lista de investigação | Filtrar tier `RISK_TIER_MUITO_RUIM` para análise manual |
| Jogo Responsável | Monitorar jogadores com `RISK_RG_ALERT_PLAYER` (sessões excessivas) |
| Antifraude | Cruzar tags `RISK_FAST_CASHOUT` + `RISK_CASHOUT_AND_RUN` + `RISK_ROLLBACK_PLAYER` |
| Auditoria | Snapshots históricos diários permitem rastrear evolução de qualquer jogador |

### Marketing

| Uso | Como |
| --- | --- |
| Exclusão de abusers | Não enviar promos para jogadores com `RISK_PROMO_ONLY` e `RISK_PROMO_CHAINER` |
| Campanhas sazonais | Direcionar para jogadores com `RISK_SLEEPER_LOW_PLAYER` em eventos/feriados |
| Programa orgânico | Reconhecer jogadores com `RISK_NON_BONUS_DEPOSITOR` com benefícios exclusivos |

---

## Exemplos Práticos

### Jogador "Muito Bom" (score 82)

> Deposita regularmente (+10), saca e reinveste (+15), não usa bônus (+10), engajado com 5 sessões/dia (+10), GGR alto (+30), joga slots (+5). Total bruto: +80. Score normalizado: 82.

**Tags no CRM:** `RISK_TIER_MUITO_BOM`, `RISK_REGULAR_DEPOSITOR`, `RISK_REINVEST_PLAYER`, `RISK_NON_BONUS_DEPOSITOR`, `RISK_ENGAGED_PLAYER`, `RISK_VIP_WHALE_PLAYER`, `RISK_BEHAV_SLOTGAMER`

**Ação:** Retenção VIP, cashback premium, account manager.

### Jogador "Mediano" (score 47)

> Deposita regularmente (+10), mas só deposita em dias de promo (-15), usa bônus e continua jogando (+15), sazonal (+5). Total bruto: +15. Score normalizado: 47.

**Tags no CRM:** `RISK_TIER_MEDIANO`, `RISK_REGULAR_DEPOSITOR`, `RISK_PROMO_ONLY`, `RISK_SUSTAINED_PLAYER`, `RISK_SLEEPER_LOW_PLAYER`

**Ação:** Monitorar, testar reengajamento sem bônus para avaliar se joga organicamente.

### Jogador "Muito Ruim" (score 0)

> Depositou e sacou em 30 minutos (-25), usou bônus e sumiu (-25), só deposita em promos (-15), encadeia promos (-10). Total bruto: -75. Score normalizado: 0.

**Tags no CRM:** `RISK_TIER_MUITO_RUIM`, `RISK_FAST_CASHOUT`, `RISK_CASHOUT_AND_RUN`, `RISK_PROMO_ONLY`, `RISK_PROMO_CHAINER`

**Ação:** Suspender bônus, encaminhar para Compliance, investigar possível lavagem.

---

## Integração com CRM (Smartico)

As tags e tiers são publicados automaticamente no CRM via API, permitindo segmentação em tempo real.

### Fluxo completo

```
1. Pipeline roda diariamente (02:00 BRT)
   → Coleta dados dos últimos 90 dias no data lake
   → Calcula 21 tags + score + tier para cada jogador

2. Resultado é salvo no banco com snapshot histórico
   → Cada dia gera um novo snapshot
   → Snapshots anteriores são preservados para auditoria

3. Push automático para o CRM (02:30 BRT)
   → Compara snapshot atual vs. anterior (envia apenas mudanças)
   → Operação atômica: remove tags RISK_* antigas, adiciona as novas
   → Tags de outras integrações são preservadas (não são tocadas)
   → Nenhuma automação/jornada é disparada (somente popula o perfil)

4. Tags ficam disponíveis no BackOffice do CRM
   → Campo: core_external_markers
   → 1 tag de tier (ex.: RISK_TIER_BOM)
   → N tags comportamentais (ex.: RISK_FAST_CASHOUT, RISK_ENGAGED_PLAYER)
   → CRM pode criar segmentos e regras usando essas tags
```

### Exemplo de tags no perfil de um jogador

```
RISK_TIER_BOM
RISK_REGULAR_DEPOSITOR
RISK_ENGAGED_PLAYER
RISK_REINVEST_PLAYER
```

Esse jogador é classificado como "Bom" (score 51–75) e tem 3 comportamentos positivos ativos: deposita regularmente, está engajado e reinveste após saques.

### O que o CRM pode fazer com as tags

| Ação | Exemplo |
| --- | --- |
| Segmentar por tier | Criar grupo "VIPs" = todos com `RISK_TIER_MUITO_BOM` |
| Micro-segmentar | "Bonus Grinders" = `RISK_PROMO_ONLY` + `RISK_PROMO_CHAINER` |
| Excluir de promos | Não enviar bônus para jogadores com `RISK_CASHOUT_AND_RUN` |
| Personalizar valor | Bônus maior para tiers Bom/Muito Bom, menor para Ruim |
| Monitorar risco | Alertas para jogadores com `RISK_FAST_CASHOUT` |
| Rastrear evolução | Comparar tags do jogador semana a semana via snapshots |

---

## Evolução Planejada

| Fase | O que muda | Status |
| --- | --- | --- |
| ~~**v2.3**~~ | ~~Integração automática com CRM via API~~ | **Entregue (abril/2026)** |
| **v2.1** | Adicionar tags específicas de apostas esportivas | Planejado |
| **v2.2** | Adicionar tags de fraude avançada (velocidade de transações, abuso de free spins) | Planejado |
| **v3.0** | Score com suavização temporal (média móvel) + scoring gradual por intensidade | Planejado |

---

## Especificações Técnicas (resumo)

| Item | Valor |
| --- | --- |
| **Atualização** | Diária (automatizada) |
| **Janela de análise** | 90 dias (rolling) |
| **Número de tags** | 21 (12 positivas, 8 negativas, 1 neutra) |
| **Escala do score** | 0 a 100 |
| **Número de tiers** | 5 + Sem Score |
| **Fonte de dados** | Dados transacionais da plataforma (data lake) |
| **Destino CRM** | Smartico — campo `core_external_markers` |
| **Persistência** | Banco de dados relacional + CSV diário |
| **Histórico** | Snapshots diários preservados para auditoria |

---

> **Desenvolvido por:** Squad Intelligence Engine
> **Última atualização:** Abril de 2026
