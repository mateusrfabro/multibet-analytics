# Validação da Integração Smartico (Matriz de Risco v2)

**Responsável:** Mateus Fabro
**Produto:** Push diário de tags RISK_* para o Smartico via API S2S
**Período coberto:** 15/04/2026 (entrega) → 17/04/2026 (validação CRM)
**Status:** Em validação pelo CRM

---

## 1. Contexto

Pipeline diário que classifica 156k+ jogadores ativos (últimos 90 dias) em 21 tags comportamentais + 5 tiers (Muito Bom → Muito Ruim) e sincroniza com o perfil do jogador no CRM Smartico (`core_external_markers`) via External Events API v2.

**Arquivos-chave:**
- Pipeline (calcula): [ec2_deploy/pipelines/risk_matrix_pipeline.py](../ec2_deploy/pipelines/risk_matrix_pipeline.py)
- Push (envia): [ec2_deploy/pipelines/push_risk_to_smartico.py](../ec2_deploy/pipelines/push_risk_to_smartico.py)
- Cliente API: [db/smartico_api.py](../db/smartico_api.py)
- Export de conferência: [ec2_deploy/pipelines/export_smartico_sent_today.py](../ec2_deploy/pipelines/export_smartico_sent_today.py)

---

## 2. Ontem — 15/04/2026 (entrega em produção)

### O que foi feito
Entrega inicial da integração Smartico em 3 fases, com validação incremental.

| Fase | O que | Resultado |
|---|---|---|
| **Fase 1 — Canary** | 1 usuário de teste com tags controladas | OK — tags apareceram no perfil Smartico em ~2s |
| **Fase 2 — Amostra** | 15 jogadores reais selecionados manualmente | OK — 100% sincronizados, sem falhas de API |
| **Fase 3 — Push full** | 155.571 jogadores (base completa ativa) | OK — sent=155571, failed=0 |
| **Automação** | Cron EC2 `30 5 * * *` (02:30 BRT) | Agendado |

### Decisões de design validadas
- **`skip_cjm=True` SEMPRE** — tags só populam o perfil, CRM decide quando disparar automations/journeys
- **Operação atômica** por evento: `^core_external_markers: [RISK_*]` + `+core_external_markers: [tags novas]`
- **Diff apenas** no cron diário — reenvio completo só com flag `--force`
- **Jogadores inativos (>90 dias)** não são "limpos" no Smartico — tags antigas permanecem para facilitar reativação

---

## 3. Hoje — 16/04/2026 (primeiro run automático)

### Execução do cron
| Etapa | Horário (BRT) | Horário (UTC) | Status |
|---|---|---|---|
| Pipeline calcula tags | 02:21 | 05:21 | OK |
| Push Smartico | 02:30 → 02:31 | 05:30 → 05:31 | OK |

### Volumes
| Métrica | Valor |
|---|---|
| Jogadores no snapshot atual (16/04) | 156.190 |
| Jogadores no snapshot anterior (15/04) | 155.571 |
| **Eventos enviados ao Smartico** | **11.806** |
| Falhas | 0 |
| NÃO enviados (tags iguais às de ontem) | 144.384 |

### Breakdown dos 11.806
- **2.766 NOVOS** — jogadores que entraram na base ativa hoje (primeira vez recebendo RISK_*)
- **9.040 MUDOU_TAGS** — já tinham perfil ontem, tags mudaram hoje

### Distribuição por tier (dos 11.806 enviados)
| Tier | Qtd |
|---|---|
| Bom | 5.225 |
| Mediano | 3.526 |
| Muito Bom | 1.904 |
| Ruim | 668 |
| Muito Ruim | 483 |

### Artefatos gerados para conferência CRM
- [reports/smartico_push/smartico_sent_2026-04-16.csv](../reports/smartico_push/smartico_sent_2026-04-16.csv) — lista completa (11.806 linhas)
- [reports/smartico_push/smartico_sent_2026-04-16_LEGENDA.txt](../reports/smartico_push/smartico_sent_2026-04-16_LEGENDA.txt) — dicionário + passo de conferência
- Log EC2: `/home/ec2-user/multibet/pipelines/logs/push_smartico_2026-04-16.log`

### Casos de teste selecionados para conferência manual no Smartico

Seleção de 3 cenários representativos (ganhou, perdeu, trocou tier). Detalhamento completo de cada caso abaixo.

**Caso 1 — Ganhou 1 tag (tier inalterado)**

| Campo | Valor |
|---|---|
| user_ext_id | `765211773344155` |
| Tier | Bom (score 65.9) |
| Diff | `+RISK_NON_PROMO_PLAYER` |
| Tags 15/04 | BEHAV_SLOTGAMER, FAST_CASHOUT, MULTI_GAME_PLAYER, REGULAR_DEPOSITOR, REINVEST_PLAYER, RG_ALERT_PLAYER, SUSTAINED_PLAYER, TIER_BOM |
| Tags 16/04 | BEHAV_SLOTGAMER, FAST_CASHOUT, MULTI_GAME_PLAYER, **NON_PROMO_PLAYER**, REGULAR_DEPOSITOR, REINVEST_PLAYER, RG_ALERT_PLAYER, SUSTAINED_PLAYER, TIER_BOM |

**Caso 2 — Perdeu 1 tag (tier inalterado)**

| Campo | Valor |
|---|---|
| user_ext_id | `28011354` |
| Tier | Bom (score 52.9) |
| Diff | `-RISK_SLEEPER_LOW_PLAYER` |
| Tags 15/04 | ENGAGED_PLAYER, SLEEPER_LOW_PLAYER, TIER_BOM |
| Tags 16/04 | ENGAGED_PLAYER, TIER_BOM |

**Caso 3 — Trocou de tier (Bom → Mediano)**

| Campo | Valor |
|---|---|
| user_ext_id | `945271775656588` |
| Tier | Mediano (score 48.2) |
| Diff | `+TIER_MEDIANO`, `-TIER_BOM`, `-NON_PROMO_PLAYER` |
| Tags 15/04 | BEHAV_SLOTGAMER, NON_PROMO_PLAYER, RG_ALERT_PLAYER, TIER_BOM |
| Tags 16/04 | BEHAV_SLOTGAMER, RG_ALERT_PLAYER, TIER_MEDIANO |

### Critério de validação
Após consulta no Smartico, o `core_external_markers` de cada user_ext_id deve bater **exatamente** com a linha "16/04" acima. Em especial:
- Caso 2: `RISK_SLEEPER_LOW_PLAYER` **não pode** estar no perfil
- Caso 3: `RISK_TIER_BOM` **não pode** estar no perfil (seria indicio de falha do `remove_pattern`)

### Resultado da validação manual no CRM Smartico — 16/04/2026 (tarde)

Evidências em [reports/smartico_push/evidencias_16042026/](../reports/smartico_push/evidencias_16042026/) (prints do painel Smartico capturados por Mateus).

#### Caso 1 — EDNELSON TOBIAS (ganhou 1 tag) — PASS

| Campo | Valor |
|---|---|
| user_ext_id | `765211773344155` |
| Smartico ID | 276105200 |
| Tier | Bom |
| Esperado | 9 tags: BEHAV_SLOTGAMER, FAST_CASHOUT, MULTI_GAME_PLAYER, **NON_PROMO_PLAYER** (nova), REGULAR_DEPOSITOR, REINVEST_PLAYER, RG_ALERT_PLAYER, SUSTAINED_PLAYER, TIER_BOM |
| Observado | 9 tags exatas, todas RISK_* presentes no `External markers` |
| Resultado | **PASS** |

#### Caso 2 — ANDRESSA CARVALHO (perdeu 1 tag) — PASS

| Campo | Valor |
|---|---|
| user_ext_id | `28011354` |
| Smartico ID | 235842086 |
| Tier | Bom |
| Esperado | 2 tags: ENGAGED_PLAYER, TIER_BOM (SLEEPER_LOW_PLAYER removida) |
| Observado | 2 tags exatas — SLEEPER_LOW_PLAYER **ausente** no perfil |
| Resultado | **PASS** |

#### Caso 3 — JOSSIE COSTA (trocou de tier Bom → Mediano) — PASS

| Campo | Valor |
|---|---|
| user_ext_id | `945271775656588` |
| Smartico ID | 278560742 |
| Tier | Mediano |
| Esperado | 3 tags: BEHAV_SLOTGAMER, RG_ALERT_PLAYER, **TIER_MEDIANO** (TIER_BOM e NON_PROMO_PLAYER removidas) |
| Observado | 3 tags exatas — TIER_BOM e NON_PROMO_PLAYER **ausentes** no perfil |
| Resultado | **PASS** |

**Conclusão:** Operação atômica (`^RISK_*` + `+tags_novas`) funciona como desenhado. O push de madrugada sincronizou os 11.806 eventos 1:1 com o snapshot do data lake. Integração **validada empiricamente**.

**Observação lateral útil:** o campo `User markers` no Smartico traz histórico antigo de tagging (`MATRIZ_DE_RISCO_MUITO_BOM_090426`, `MATRIZ_DE_RISCO_MEDIANO_090426` etc). Esses markers vêm de um esquema anterior (manual/ad-hoc) e **não são tocados pelo push automatizado**, que atua exclusivamente no `External markers` com prefixo `RISK_*`. Convém alinhar com o CRM se esse histórico deve ser limpo ou migrado em algum momento.

---

## 4. Amanhã — 17/04/2026 (plano de validação contínua)

### Automação (roda sozinha)
| Cron | Horário | O que faz |
|---|---|---|
| `0 5 * * *` | 02:00 BRT | Pipeline recalcula tags (snapshot 17/04) |
| `30 5 * * *` | 02:30 BRT | Push Smartico diff (17/04 vs 16/04) |

### Tarefas manuais
1. **Monitoramento do run automático:** verificar log `push_smartico_2026-04-17.log` no início da manhã (esperado: diff menor, ordem 3-8k jogadores — dia-a-dia tende a estabilizar após o "pulo" inicial de 11.806).
2. **Gerar CSV de conferência** do dia 17/04 rodando `python3 pipelines/export_smartico_sent_today.py` na EC2 e baixando para `reports/smartico_push/`.
3. **Ampliar bateria de testes no Smartico** (coordenação Mateus + CRM) — meta de 6-10 casos variados:
   - Casos adicionais de **GANHOU tag** (diferentes comportamentais: VIP_WHALE, PLAYER_REENGAGED, WINBACK_HI_VAL)
   - Casos adicionais de **PERDEU tag** (checar se `^RISK_*` remove consistentemente cada tag)
   - Casos de **TROCOU TIER** em sentidos distintos (subida e descida: Muito Ruim → Ruim, Bom → Muito Bom etc)
   - Pelo menos 1 caso **NOVO** (jogador que apareceu pela primeira vez hoje 17/04) — esperado: perfil Smartico não tinha nenhum `RISK_*` antes e ganha as tags atuais
   - Capturar **prints** de cada caso e salvar em `reports/smartico_push/evidencias_17042026/`
4. **Sign-off formal do CRM (Raphael M.):** após 2 dias consecutivos de validação (16/04 + 17/04), integração passa de "em validação" para "operacional".

### Critérios de rollback
Se algum dos 3 casos falhar:
- Pausar cron push (`crontab -e` → comentar linha `30 5 * * *`)
- Investigar logs + payload do evento (Smartico tem audit trail)
- Validar comportamento do `^core_external_markers: ["RISK_*"]` com suporte Smartico

### Próximos passos após validação (P0/P1)
Documentados em [project_matriz_risco_v2.md](../MEMORY.md) — destaque:
- **P0:** `RG_ALERT_PLAYER` score +1 → 0 (risco regulatório — flag Mauro)
- **P0:** `FAST_CASHOUT` sem checar atividade de jogo entre dep/saque (falso positivo)
- **P1:** Tags sportsbook (cobertura zero hoje)
- **P1:** Tags fraude (velocity, FS abuse, disproportionate cashout)
- **P1:** Non-stacking `FAST_CASHOUT` + `CASHOUT_AND_RUN` (73.7% overlap)

---

## 5. Referências

- Projeto: [MEMORY — project_matriz_risco_v2](../memory/project_matriz_risco_v2.md)
- Infra EC2: [MEMORY — ec2_infrastructure](../memory/ec2_infrastructure.md)
- Doc pública Notion (PT-BR): [docs/notion_risk_matrix_PUBLICO.md](notion_risk_matrix_PUBLICO.md)
- Doc técnica: [docs/notion_risk_matrix_v2.md](notion_risk_matrix_v2.md)
- Notion oficial: https://www.notion.so/Matriz-de-Risco-v2-33d985301ab480bdb089e7d351db032b
