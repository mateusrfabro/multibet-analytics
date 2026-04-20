# PCR — Player Credit Rating v1.3 — MultiBet

> **Classificacao bancaria (D-AAA / E-S) de cada jogador baseada em Player Value Score (PVS) normalizado 0-100.**
> Pipeline Athena + PostgreSQL | Snapshots historicos diarios | Push Smartico S2S

---

## Visao Geral

O PCR atribui um **rating estilo credit score bancario** a cada jogador ativo, combinando valor gerado (GGR, depositos), risco operacional (margem, dependencia de bonus) e outlook (recencia, atividade). A saida e 1 tag por jogador (`PCR_RATING_S` ate `PCR_RATING_E` + `PCR_RATING_NEW`) publicada no Smartico pra segmentacao de CRM.

| Campo | Valor |
|---|---|
| **Escopo** | Jogadores `real_user` com atividade nos ultimos 90 dias |
| **Janela padrao** | 90 dias (rolling) |
| **Fonte de dados** | AWS Athena (ps_bi.fct_player_activity_daily + dim_user + bireports_ec2.tbl_ecr) |
| **Output** | 1 linha por jogador, rating + PVS + 20 colunas de metricas |
| **Destino** | PostgreSQL (`multibet.pcr_ratings`) + Smartico (S2S API) |
| **Pipeline principal** | `pipelines/pcr_pipeline.py` |
| **Push CRM** | `scripts/push_pcr_to_smartico.py` |
| **Frequencia** | Diaria (quando automatizada na EC2) |
| **Historico** | Preservado por `snapshot_date` (permite backtest e diff) |

---

## Tiers de Classificacao

| Rating | PVS (faixa) | Faixa da base | Descricao | Acao CRM sugerida |
|---|---|---|---|---|
| **S** | Top 1% | ~1% | Whales + jogadores maduros elite | Atendimento VIP, host dedicado, experiencia personalizada |
| **A** | 92 a 99 | ~7% | Alto valor sustentado | Ofertas premium, beneficios exclusivos, cashback elevado |
| **B** | 75 a 92 | ~17% | Valor consistente, engajamento sadio | Cashback, torneios, retencao ativa |
| **C** | 50 a 75 | ~25% | Mediano, padrao regular | Campanhas massivas, free spins, missoes |
| **D** | 25 a 50 | ~25% | Baixo valor, mas ativo | Reengajamento, bonus de reativacao |
| **E** | Bottom 25% | ~25% | Minimo engajamento, alto risco de churn | Campanhas de ultimo esforco, possivel offboard |
| **NEW** | sem PVS | ~10-15% | Novatos (< 14 dias ativos OU < 3 depositos) | Jornada de onboarding / boas-vindas |

> **Por que NEW e separado:** a formula PVS usa ratios (`margem_ggr`, `bonus_ratio`, `taxa_atividade`) que sao estatisticamente instaveis com amostra pequena. FTD recente com 1 deposito cai automaticamente em E por construcao matematica, recebendo campanha de reativacao quando deveria receber boas-vindas. Separar garante calculo justo + jornada correta.

---

## Formula PVS — Player Value Score

```
PVS = 0.25 * score_ggr
    + 0.15 * score_deposit
    + 0.12 * score_recencia
    + 0.10 * score_margem
    + 0.10 * score_num_dep
    + 0.08 * score_dias_ativos
    + 0.05 * score_mix_produto
    + 0.05 * score_taxa_atividade
    - 0.10 * score_bonus_penalidade
```

- Cada `score_*` e o **percentil rank (0-100)** da metrica bruta dentro da base de maduros.
- PVS final e limitado entre **0 e 100**.
- Percentis sao recalculados a cada snapshot apenas sobre jogadores maduros (novatos excluidos).

---

## Componentes do PVS

### Valor (40% do peso total)

#### 1. score_ggr — Peso: 25%

> GGR total do jogador nos 90 dias (soma de bets - wins).

| Parametro | Valor |
|---|---|
| **Fonte** | `ps_bi.fct_player_activity_daily` |
| **Colunas** | `casino_realbet_base`, `sb_realbet_base`, `casino_realwin_base`, `sb_realwin_base` |
| **Normalizacao** | percentil rank 0-100 (maior = melhor) |
| **Racional** | GGR e a metrica primaria de valor — quanto o jogador deixa na casa |

#### 2. score_deposit — Peso: 15%

> Total depositado (em BRL) nos 90 dias.

| Parametro | Valor |
|---|---|
| **Fonte** | `ps_bi.fct_player_activity_daily.deposit_success_base` |
| **Normalizacao** | percentil rank 0-100 (maior = melhor) |
| **Racional** | Volume de investimento financeiro do jogador |

### Risco (20% do peso total)

#### 3. score_margem — Peso: 10%

> `margem_ggr = GGR / turnover_total`. Quanto mais baixa (perto de 0 ou negativo), melhor — indica jogador com bom retorno pra casa em relacao ao volume movimentado.

| Parametro | Valor |
|---|---|
| **Calculo** | `GGR_total / (casino_real_bet + sb_real_bet)` |
| **Normalizacao** | percentil rank 0-100 **INVERTIDO** (margem menor = score maior) |
| **Racional** | Jogador com margem negativa ou pequena gera GGR sem "acaso" — mais previsivel |

#### 4. score_bonus_penalidade — Peso: -10% (unica penalidade)

> `bonus_ratio = bonus_issued / total_deposits`. Penaliza dependencia de bonus em relacao ao quanto o jogador deposita.

| Parametro | Valor |
|---|---|
| **Calculo** | `bonus_issued / total_deposits` (0 se sem depositos) |
| **Normalizacao** | percentil rank 0-100 (maior ratio = mais penalidade) |
| **Racional** | Bonus-chaser recebe PVS menor; jogador organico recebe peso positivo |

### Outlook (25% do peso total)

#### 5. score_recencia — Peso: 12%

> Dias desde a ultima atividade (menor = melhor).

| Parametro | Valor |
|---|---|
| **Calculo** | `DATE_DIFF(day, last_active_date, CURRENT_DATE)` |
| **Normalizacao** | percentil rank 0-100 **INVERTIDO** |
| **Racional** | Jogador ativo recentemente tem mais probabilidade de engajar com campanha |

#### 6. score_dias_ativos — Peso: 8%

> Quantos dias distintos do periodo de 90d o jogador foi ativo.

| Parametro | Valor |
|---|---|
| **Calculo** | `COUNT(DISTINCT activity_date)` no periodo |
| **Normalizacao** | percentil rank 0-100 |
| **Racional** | Frequencia de engajamento indica habito, nao evento isolado |

#### 7. score_taxa_atividade — Peso: 5%

> Proporcao dos 90 dias em que o jogador jogou.

| Parametro | Valor |
|---|---|
| **Calculo** | `days_active / 90` (limitado a 0-1) |
| **Escala** | `(taxa / 1) * 100` (ja e 0-100, nao precisa rank) |
| **Racional** | Complementa `score_dias_ativos` com base escalar fixa |

### Frequencia transacional (10% do peso total)

#### 8. score_num_dep — Peso: 10%

> Quantidade de depositos no periodo.

| Parametro | Valor |
|---|---|
| **Calculo** | `SUM(deposit_success_count)` |
| **Normalizacao** | percentil rank 0-100 |
| **Racional** | Complementa `score_deposit` (valor) com frequencia (n transacoes) |

### Mix de produto (5% do peso total)

#### 9. score_mix_produto — Peso: 5%

> Jogador que joga casino + sportsbook tem peso maior que mono-produto.

| Product_type | Score |
|---|---|
| **MISTO** (casino + sport) | 100 |
| **CASINO** (so casino) | 40 |
| **SPORT** (so sportsbook) | 40 |
| **OUTRO** | 0 |

---

## Fluxo de Execucao

```
                      +-------------------------+
                      | pcr_pipeline.py         |
                      +-------------------------+
                                 |
                    1. Extracao Athena (90d)
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
   days<14 OR num_dep<3                    Os demais
                 |                               |
        rating = "NEW"                Calcula percentis PVS
        pvs = NULL                   Atribui S/A/B/C/D/E
                 |                               |
                 +---------------+---------------+
                                 |
                    4. Persistencia
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
                    6. Diff snapshot atual vs anterior
                                 |
                    7. Push S2S Smartico
                       (so quem mudou de rating)
```

---

## Integracao Smartico

### Mapping de Tags

| Rating | Tag Smartico | Shadow Mode |
|---|---|---|
| S | `PCR_RATING_S` | Nao |
| A | `PCR_RATING_A` | Nao |
| B | `PCR_RATING_B` | Nao |
| C | `PCR_RATING_C` | Nao |
| D | `PCR_RATING_D` | Nao |
| E | `PCR_RATING_E` | Nao |
| NEW | `PCR_RATING_NEW` | **SIM — aguardando aprovacao Raphael + Castrin** |

### Operacao Atomica

Cada jogador recebe **exatamente 1 tag do prefixo `PCR_RATING_*`**. O payload remove todas as antigas + adiciona a nova:

```json
{
    "^core_external_markers": ["PCR_RATING_*"],
    "+core_external_markers": ["PCR_RATING_A"]
}
```

Preserva tags de outras integracoes (RISK_TIER_*, BONUS_*, etc).

### Dedup por Diff

Push so envia se o rating mudou entre snapshots:

- Snapshot `dia-1`: jogador 12345 era `PCR_RATING_B`
- Snapshot `dia`: jogador 12345 virou `PCR_RATING_A`
- Payload enviado ao Smartico
- Se nao mudou: `skip` (reduz 80-95% do volume diario)

### Rollout 3 Fases (novos deployments)

Seguindo `memory/feedback_smartico_push_rollout_playbook.md`:

1. **Canary:** 1 user seguro (rating B ou C, `--pick-canary`)
2. **Amostra:** 10 users via CSV (`--file amostra.csv`)
3. **Full:** producao completa (`--skip-cjm --confirm`)

Cada fase exige validacao no painel Smartico pelo Raphael antes da proxima.

---

## Persistencia

### Tabela `multibet.pcr_ratings`

| Coluna | Tipo | Descricao |
|---|---|---|
| `snapshot_date` | DATE | Data do snapshot (PK composta) |
| `player_id` | BIGINT | ECR ID interno (18 digitos) (PK composta) |
| `external_id` | BIGINT | ID externo (Smartico user_ext_id) |
| `rating` | VARCHAR(10) | S/A/B/C/D/E/NEW |
| `pvs` | NUMERIC(8,2) | Score 0-100 (NULL para NEW) |
| `ggr_total`, `ngr_total` | NUMERIC(15,2) | Performance financeira |
| `total_deposits`, `total_cashouts` | NUMERIC(15,2) | Volumes |
| `num_deposits`, `days_active`, `recency_days` | INTEGER | Frequencia |
| `product_type` | VARCHAR(10) | MISTO / CASINO / SPORT / OUTRO |
| `casino_rounds`, `sport_bets` | BIGINT | Atividade por produto |
| `bonus_issued`, `bonus_ratio` | NUMERIC | Bonus |
| `wd_ratio`, `net_deposit` | NUMERIC | W/D e net |
| `margem_ggr`, `ggr_por_dia` | NUMERIC | Metricas derivadas |
| `affiliate_id` | VARCHAR(300) | Origem aquisicao |
| `c_category` | VARCHAR(50) | Status da conta (real_user, etc) |
| `registration_date` | DATE | Data de cadastro |
| `created_at` | TIMESTAMPTZ | Timestamp da gravacao |

### Views

- **`multibet.pcr_atual`:** ultima snapshot_date (usado pelo CRM)
- **`multibet.pcr_resumo`:** agregado por rating da ultima snapshot (jogadores, GGR medio, deposito medio, PVS medio) — usado pelo Head pra reporting

### Indices

- `idx_pcr_snapshot (snapshot_date DESC)` — consulta da ultima foto
- `idx_pcr_rating (snapshot_date, rating)` — agregados por tier
- `idx_pcr_category (snapshot_date, c_category)` — filtro de status

### Historico

Preservado por snapshot_date. Permite:
- **Backtest:** rodar pipeline com algoritmo alterado e comparar com historico
- **Diff temporal:** identificar jogadores migrando de tier semana-a-semana
- **Reconciliacao:** auditoria de push Smartico em datas passadas

---

## Metricas de Saude (monitoramento)

Quando a pipeline rodar diariamente na EC2, validar:

| Metrica | Threshold esperado | Acao se fora |
|---|---|---|
| Jogadores NEW | 10-20% da base total | Se > 25%: verificar afluxo de FTDs (possivel campanha); se < 5%: verificar pipeline de aquisicao |
| Distribuicao S/A/B/C/D/E | ~1/7/17/25/25/25% | Desvios > 5pp indicam mudanca de comportamento ou bug de calculo |
| `c_category` NOT NULL | > 99% | Se abaixo: bireports_ec2.tbl_ecr com stale |
| Tempo execucao | < 5 min | Se > 10 min: Athena lento ou scan aumentou |
| Tamanho do diff Smartico | 5-15% da base/dia | Se > 30%: ruido nos cortes PVS (ver 4.3 do README auditoria) |

---

## Regras obrigatorias (lessons learned)

Feedbacks registrados na memoria apos auditoria 20/04/2026:

1. **c_category OBRIGATORIO** filtrar `real_user` antes do ranking PVS
   (evita push Smartico para conta `rg_closed` — compliance)

2. **Tie-breaker deterministico** em ROW_NUMBER da subquery `ecr_bi`
   (evita rating oscilante entre rodadas)

3. **Amostra minima pra ranking:** novatos separados antes do calculo
   de percentis (v1.3 — esta proposta)

4. **Shadow mode obrigatorio** para tags novas antes de ativar push
   (evita tag chegar no Smartico sem jornada configurada)

5. **Auditor OBRIGATORIO** antes de deploy EC2 ou push CRM
   (ver `memory/feedback_gatekeeper_deploy_automatizado.md`)

---

## Arquivos relacionados

| Arquivo | Funcao |
|---|---|
| `pipelines/pcr_pipeline.py` | Pipeline principal (extracao + scoring + persistencia) |
| `scripts/push_pcr_to_smartico.py` | Push diario ao Smartico (diff-based) |
| `scripts/pcr_scoring.py` | Script legado CSV-only (referencia historica) |
| `ec2_deploy/deploy_pcr_pipeline.sh` | Deploy na EC2 (pendente aprovacao) |
| `ec2_deploy/run_pcr_pipeline.sh` | Cron runner (pendente aprovacao) |
| `docs/pcr_player_credit_rating.md` | Doc conceitual original |
| `docs/proposta_pcr_rating_new_20260420.md` | Proposta da tag NEW |
| `docs/auditoria_sql_pcr_20260420.md` | Relatorio da auditoria |
| `docs/README_auditoria_sql_20260420.md` | Visao executiva da auditoria |

---

## Status atual (20/04/2026)

- [x] Pipeline v1.3 implementado (separa NEW)
- [x] Fixes criticos da auditoria aplicados (c_category + ROW_NUMBER)
- [x] Shadow mode do NEW ativo (tabela grava, push nao envia)
- [x] Codigo versionado no git (3 commits: onboarding + fixes + docs)
- [ ] Reuniao Raphael + Castrin pra aprovar tag NEW
- [ ] Pesquisa Smartico sobre score_norm confirmada na doc (pergunta operacional pendente com Raphael)
- [ ] Deploy EC2 (aguardando decisao arquitetura pos-migracao)
- [ ] AVG -> mediana em ENGAGED/ZERO_RISK/BEHAV_RISK (bloqueado ate NEW + score_norm fecharem)
