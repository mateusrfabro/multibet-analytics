# PCR — Player Credit Rating v1.3 — MultiBet

> **Classificação bancária (D-AAA / E-S) de cada jogador baseada em Player Value Score (PVS) normalizado de 0 a 100.**
> Pipeline Athena + PostgreSQL | Snapshots históricos diários | Push Smartico S2S

---

## Visão Geral

O PCR atribui um **rating no estilo credit score bancário** a cada jogador ativo, combinando valor gerado (GGR, depósitos), risco operacional (margem, dependência de bônus) e outlook (recência, atividade). A saída é 1 tag por jogador (`PCR_RATING_S` até `PCR_RATING_E` + `PCR_RATING_NEW`), publicada no Smartico para segmentação de CRM.

| Campo | Valor |
|---|---|
| **Escopo** | Jogadores `real_user` com atividade nos últimos 90 dias |
| **Janela padrão** | 90 dias (rolling) |
| **Fonte de dados** | AWS Athena (ps_bi.fct_player_activity_daily + dim_user + bireports_ec2.tbl_ecr) |
| **Output** | 1 linha por jogador, com rating + PVS + 20 colunas de métricas |
| **Destino** | PostgreSQL (`multibet.pcr_ratings`) + Smartico (S2S API) |
| **Pipeline principal** | `pipelines/pcr_pipeline.py` |
| **Push CRM** | `scripts/push_pcr_to_smartico.py` |
| **Frequência** | Diária (quando automatizada na EC2) |
| **Histórico** | Preservado por `snapshot_date` (permite backtest e diff) |

---

## Tiers de Classificação

| Rating | PVS (faixa) | Faixa da base | Descrição | Ação de CRM sugerida |
|---|---|---|---|---|
| **S** | Top 1% | ~1% | Whales e jogadores maduros de elite | Atendimento VIP, host dedicado, experiência personalizada |
| **A** | 92 a 99 | ~7% | Alto valor sustentado | Ofertas premium, benefícios exclusivos, cashback elevado |
| **B** | 75 a 92 | ~17% | Valor consistente, engajamento saudável | Cashback, torneios, retenção ativa |
| **C** | 50 a 75 | ~25% | Mediano, padrão regular | Campanhas massivas, free spins, missões |
| **D** | 25 a 50 | ~25% | Baixo valor, porém ativo | Reengajamento, bônus de reativação |
| **E** | Bottom 25% | ~25% | Engajamento mínimo, alto risco de churn | Campanhas de último esforço, possível offboard |
| **NEW** | sem PVS | ~10 a 15% | Novatos (menos de 14 dias ativos OU menos de 3 depósitos) | Jornada de onboarding e boas-vindas |

> **Por que NEW é separado:** a fórmula do PVS usa razões (`margem_ggr`, `bonus_ratio`, `taxa_atividade`) que são estatisticamente instáveis em amostras pequenas. Um FTD recente com 1 depósito cai automaticamente no rating E por construção matemática, recebendo campanha de reativação quando deveria receber a jornada de boas-vindas. Separar garante cálculo justo e jornada correta.

---

## Fórmula do PVS — Player Value Score

```
PVS = 0,25 * score_ggr
    + 0,15 * score_deposit
    + 0,12 * score_recencia
    + 0,10 * score_margem
    + 0,10 * score_num_dep
    + 0,08 * score_dias_ativos
    + 0,05 * score_mix_produto
    + 0,05 * score_taxa_atividade
    - 0,10 * score_bonus_penalidade
```

- Cada `score_*` é o **percentil rank (0 a 100)** da métrica bruta dentro da base de jogadores maduros.
- O PVS final é limitado entre **0 e 100**.
- Os percentis são recalculados a cada snapshot considerando apenas jogadores maduros (os novatos são excluídos do cálculo).

---

## Componentes do PVS

### Valor (40% do peso total)

#### 1. score_ggr — Peso: 25%

> GGR total do jogador nos últimos 90 dias (soma de apostas menos ganhos).

| Parâmetro | Valor |
|---|---|
| **Fonte** | `ps_bi.fct_player_activity_daily` |
| **Colunas** | `casino_realbet_base`, `sb_realbet_base`, `casino_realwin_base`, `sb_realwin_base` |
| **Normalização** | percentil rank 0 a 100 (quanto maior, melhor) |
| **Racional** | GGR é a métrica primária de valor — representa quanto o jogador deixa na casa |

#### 2. score_deposit — Peso: 15%

> Total depositado (em BRL) nos últimos 90 dias.

| Parâmetro | Valor |
|---|---|
| **Fonte** | `ps_bi.fct_player_activity_daily.deposit_success_base` |
| **Normalização** | percentil rank 0 a 100 (quanto maior, melhor) |
| **Racional** | Volume de investimento financeiro do jogador |

### Risco (20% do peso total)

#### 3. score_margem — Peso: 10%

> `margem_ggr = GGR / turnover_total`. Quanto menor (próxima de zero ou negativa), melhor — indica jogador com bom retorno para a casa em relação ao volume movimentado.

| Parâmetro | Valor |
|---|---|
| **Cálculo** | `GGR_total / (casino_real_bet + sb_real_bet)` |
| **Normalização** | percentil rank 0 a 100 **INVERTIDO** (margem menor = score maior) |
| **Racional** | Jogador com margem negativa ou pequena gera GGR sem "sorte" — é mais previsível |

#### 4. score_bonus_penalidade — Peso: -10% (única penalidade)

> `bonus_ratio = bonus_issued / total_deposits`. Penaliza a dependência de bônus em relação ao quanto o jogador deposita.

| Parâmetro | Valor |
|---|---|
| **Cálculo** | `bonus_issued / total_deposits` (0 quando sem depósitos) |
| **Normalização** | percentil rank 0 a 100 (quanto maior a razão, maior a penalidade) |
| **Racional** | Bonus-chaser recebe PVS menor; jogador orgânico recebe peso positivo |

### Outlook (25% do peso total)

#### 5. score_recencia — Peso: 12%

> Dias desde a última atividade (quanto menos, melhor).

| Parâmetro | Valor |
|---|---|
| **Cálculo** | `DATE_DIFF(day, last_active_date, CURRENT_DATE)` |
| **Normalização** | percentil rank 0 a 100 **INVERTIDO** |
| **Racional** | Jogador ativo recentemente tem maior probabilidade de engajar com a campanha |

#### 6. score_dias_ativos — Peso: 8%

> Quantos dias distintos o jogador foi ativo dentro da janela de 90 dias.

| Parâmetro | Valor |
|---|---|
| **Cálculo** | `COUNT(DISTINCT activity_date)` no período |
| **Normalização** | percentil rank 0 a 100 |
| **Racional** | Frequência de engajamento indica hábito, não evento isolado |

#### 7. score_taxa_atividade — Peso: 5%

> Proporção dos 90 dias em que o jogador jogou.

| Parâmetro | Valor |
|---|---|
| **Cálculo** | `days_active / 90` (limitado entre 0 e 1) |
| **Escala** | `(taxa / 1) * 100` (já é 0 a 100, não precisa de rank) |
| **Racional** | Complementa `score_dias_ativos` com base em escala fixa |

### Frequência transacional (10% do peso total)

#### 8. score_num_dep — Peso: 10%

> Quantidade de depósitos no período.

| Parâmetro | Valor |
|---|---|
| **Cálculo** | `SUM(deposit_success_count)` |
| **Normalização** | percentil rank 0 a 100 |
| **Racional** | Complementa `score_deposit` (valor) com frequência (número de transações) |

### Mix de produto (5% do peso total)

#### 9. score_mix_produto — Peso: 5%

> Jogador que joga cassino e sportsbook tem peso maior do que mono-produto.

| Product_type | Score |
|---|---|
| **MISTO** (cassino + sport) | 100 |
| **CASINO** (somente cassino) | 40 |
| **SPORT** (somente sportsbook) | 40 |
| **OUTRO** | 0 |

---

## Fluxo de Execução

```
                      +-------------------------+
                      | pcr_pipeline.py         |
                      +-------------------------+
                                 |
                    1. Extração do Athena (90d)
                                 |
         +-----------------------+-----------------------+
         |                       |                       |
  ps_bi.fct_player_       ps_bi.dim_user         bireports_ec2.tbl_ecr
  activity_daily          (external_id)           (c_category)
         |                       |                       |
         +-----------------------+-----------------------+
                                 |
                    2. Filtros aplicados
         * WHERE is_test = false
         * AND c_category = 'real_user'  (v1.3 — compliance)
         * AND HAVING atividade > 0
                                 |
                    3. atribuir_rating() (v1.3)
                                 |
                 +---------------+---------------+
                 |                               |
            (novatos)                        (maduros)
   days<14 OU num_dep<3                   Os demais
                 |                               |
        rating = "NEW"                 Calcula percentis do PVS
        pvs = NULL                     Atribui S/A/B/C/D/E
                 |                               |
                 +---------------+---------------+
                                 |
                    4. Persistência
                                 |
                      multibet.pcr_ratings
                    (DELETE WHERE snapshot_date
                        + INSERT novos)
                                 |
                    5. Views atualizadas
                       pcr_atual, pcr_resumo
                                 |
                                 v
            +-----------------------------------------+
            |   push_pcr_to_smartico.py (outro job)   |
            +-----------------------------------------+
                                 |
                    6. Diff do snapshot atual vs. anterior
                                 |
                    7. Push S2S ao Smartico
                       (somente quem mudou de rating)
```

---

## Integração com o Smartico

### Mapeamento de Tags

| Rating | Tag Smartico | Shadow Mode |
|---|---|---|
| S | `PCR_RATING_S` | Não |
| A | `PCR_RATING_A` | Não |
| B | `PCR_RATING_B` | Não |
| C | `PCR_RATING_C` | Não |
| D | `PCR_RATING_D` | Não |
| E | `PCR_RATING_E` | Não |
| NEW | `PCR_RATING_NEW` | **SIM — aguardando 2 validações operacionais com o Raphael (CRM): (a) tag precisa pré-registro no painel Smartico? (b) jornada de boas-vindas mapeada?** |

### Operação Atômica

Cada jogador recebe **exatamente 1 tag com o prefixo `PCR_RATING_*`**. O payload remove todas as antigas e adiciona a nova:

```json
{
    "^core_external_markers": ["PCR_RATING_*"],
    "+core_external_markers": ["PCR_RATING_A"]
}
```

Isso preserva tags de outras integrações (RISK_TIER_*, BONUS_*, etc.).

### Dedup por Diff

O push só envia se o rating mudou entre snapshots:

- Snapshot do `dia-1`: o jogador 12345 era `PCR_RATING_B`
- Snapshot do `dia`: o jogador 12345 virou `PCR_RATING_A`
- Payload enviado ao Smartico
- Se não mudou: `skip` (reduz de 80% a 95% do volume diário)

### Rollout em 3 Fases (novos deployments)

Seguindo `memory/feedback_smartico_push_rollout_playbook.md`:

1. **Canary:** 1 usuário seguro (rating B ou C, via `--pick-canary`)
2. **Amostra:** 10 usuários via CSV (`--file amostra.csv`)
3. **Full:** produção completa (`--skip-cjm --confirm`)

Cada fase exige validação do Raphael no painel do Smartico antes de avançar para a próxima.

---

## Persistência

### Tabela `multibet.pcr_ratings`

| Coluna | Tipo | Descrição |
|---|---|---|
| `snapshot_date` | DATE | Data do snapshot (PK composta) |
| `player_id` | BIGINT | ECR ID interno (18 dígitos) (PK composta) |
| `external_id` | BIGINT | ID externo (Smartico user_ext_id) |
| `rating` | VARCHAR(10) | S/A/B/C/D/E/NEW |
| `pvs` | NUMERIC(8,2) | Score 0 a 100 (NULL para NEW) |
| `ggr_total`, `ngr_total` | NUMERIC(15,2) | Performance financeira |
| `total_deposits`, `total_cashouts` | NUMERIC(15,2) | Volumes |
| `num_deposits`, `days_active`, `recency_days` | INTEGER | Frequência |
| `product_type` | VARCHAR(10) | MISTO / CASINO / SPORT / OUTRO |
| `casino_rounds`, `sport_bets` | BIGINT | Atividade por produto |
| `bonus_issued`, `bonus_ratio` | NUMERIC | Bônus |
| `wd_ratio`, `net_deposit` | NUMERIC | W/D e net |
| `margem_ggr`, `ggr_por_dia` | NUMERIC | Métricas derivadas |
| `affiliate_id` | VARCHAR(300) | Origem de aquisição |
| `c_category` | VARCHAR(50) | Status da conta (real_user, etc.) |
| `registration_date` | DATE | Data de cadastro |
| `created_at` | TIMESTAMPTZ | Timestamp da gravação |

### Views

- **`multibet.pcr_atual`:** última snapshot_date (usada pelo CRM)
- **`multibet.pcr_resumo`:** agregado por rating da última snapshot (jogadores, GGR médio, depósito médio, PVS médio) — usada pelo Head para reporting

### Índices

- `idx_pcr_snapshot (snapshot_date DESC)` — consulta da última foto
- `idx_pcr_rating (snapshot_date, rating)` — agregados por tier
- `idx_pcr_category (snapshot_date, c_category)` — filtro por status

### Histórico

Preservado por snapshot_date. Permite:
- **Backtest:** rodar o pipeline com algoritmo alterado e comparar com o histórico
- **Diff temporal:** identificar jogadores que migraram de tier semana a semana
- **Reconciliação:** auditoria do push do Smartico em datas passadas

---

## Métricas de Saúde (monitoramento)

Quando o pipeline rodar diariamente na EC2, validar:

| Métrica | Threshold esperado | Ação se estiver fora |
|---|---|---|
| Jogadores NEW | 10% a 20% da base total | Se > 25%: verificar afluxo de FTDs (possível campanha); se < 5%: verificar pipeline de aquisição |
| Distribuição S/A/B/C/D/E | ~1 / 7 / 17 / 25 / 25 / 25% | Desvios maiores que 5 pp indicam mudança de comportamento ou bug de cálculo |
| `c_category` NOT NULL | > 99% | Se estiver abaixo: bireports_ec2.tbl_ecr pode estar desatualizada |
| Tempo de execução | < 5 min | Se > 10 min: Athena lento ou scan aumentou |
| Tamanho do diff do Smartico | 5% a 15% da base por dia | Se > 30%: ruído nos cortes do PVS (ver 4.3 do README da auditoria) |

---

## Regras Obrigatórias (lessons learned)

Feedbacks registrados na memória após auditoria de 20/04/2026:

1. **`c_category` OBRIGATÓRIO** filtrar `real_user` antes do ranking do PVS
   (evita push do Smartico para conta `rg_closed` — compliance)

2. **Tie-breaker determinístico** em ROW_NUMBER na subquery `ecr_bi`
   (evita rating oscilante entre rodadas)

3. **Amostra mínima para ranking:** novatos separados antes do cálculo
   dos percentis (v1.3 — esta proposta)

4. **Shadow mode obrigatório** para tags novas antes de ativar o push
   (evita que a tag chegue ao Smartico sem a jornada configurada)

5. **Auditor OBRIGATÓRIO** antes de deploy em EC2 ou push de CRM
   (ver `memory/feedback_gatekeeper_deploy_automatizado.md`)

---

## Arquivos Relacionados

| Arquivo | Função |
|---|---|
| `pipelines/pcr_pipeline.py` | Pipeline principal (extração + scoring + persistência) |
| `scripts/push_pcr_to_smartico.py` | Push diário ao Smartico (baseado em diff) |
| `scripts/pcr_scoring.py` | Script legado CSV-only (referência histórica) |
| `ec2_deploy/deploy_pcr_pipeline.sh` | Deploy na EC2 (pendente de aprovação) |
| `ec2_deploy/run_pcr_pipeline.sh` | Cron runner (pendente de aprovação) |
| `docs/pcr_player_credit_rating.md` | Doc conceitual original |
| `docs/proposta_pcr_rating_new_20260420.md` | Proposta da tag NEW |
| `docs/auditoria_sql_pcr_20260420.md` | Relatório da auditoria |
| `docs/README_auditoria_sql_20260420.md` | Visão executiva da auditoria |

---

## Status Atual (21/04/2026)

### Já implementado (aprovado e funcional)

- [x] Pipeline v1.3 separa NEW antes do ranking PVS
- [x] Correções críticas da auditoria aplicadas (c_category + ROW_NUMBER)
- [x] Schema aplicado em produção (rating: VARCHAR(10), views com NEW no ORDER BY)
- [x] Código versionado no git (5 commits na branch main)
- [x] Documentação completa (Notion, README de auditoria, proposta técnica)
- [x] Tag `PCR_RATING_NEW` mapeada no script de push

### Pendente (não depende de aprovação — é validação operacional)

- [ ] **2 perguntas para o Raphael (CRM)** antes de ativar push Smartico:
  - (a) A tag `PCR_RATING_NEW` precisa pré-registro no painel Smartico ou tag arbitrária funciona via S2S?
  - (b) Jornada de boas-vindas mapeada para essa tag já existe ou precisa criar?
- [ ] Após respostas do Raphael: trocar `PUSH_NEW_TAG_ENABLED = False` → `True` + rollout canary 1 user → amostra 10 → full
- [ ] Pesquisa do Smartico sobre `score_norm` (3 perguntas específicas ao Raphael, listadas no README da auditoria)

### Próximo ciclo (não faz parte do escopo atual)

- [ ] Deploy em EC2 (aguardando decisão da arquitetura pós-migração, via Gusta)
- [ ] AVG → mediana em ENGAGED/ZERO_RISK/BEHAV_RISK (bloqueado até NEW + score_norm serem concluídos, para evitar 3 mudanças simultâneas de contrato CRM)
