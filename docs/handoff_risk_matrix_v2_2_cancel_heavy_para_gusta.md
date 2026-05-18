# Handoff Gusta — Atualizar `risk_matrix_pipeline` no orquestrador (v2.2 CANCEL_HEAVY_DAILY)

**De:** Mateus
**Para:** Gusta
**Data:** 18/05/2026
**Prioridade:** P1 (tag nova ja aprovada pelo Head Castrin e rodada manual)

---

## TL;DR

Adicionei a tag `CANCEL_HEAVY_DAILY` (-10) na matriz de risco. Aprovada pelo Castrin via WhatsApp em 15/05. Rodei manual em 18/05, snapshot ja esta no DB (`multibet.risk_tags`), CSV gerado. Preciso que voce **substitua o pipeline + SQLs no orquestrador** pra proxima rodada automatica ja sair com a tag nova.

**Commit alvo (HEAD `main`):**
```
37172e82220a5c08098f85f94db6a6a23d8b4fd5
feat(risk_matrix): v2.2 — adiciona tag CANCEL_HEAVY_DAILY (-10)
```

**Repo:** https://github.com/mateusrfabro/multibet-analytics
**Branch:** `main`

---

## Contexto / por que essa mudanca

Em 13/05 detectamos um exploit recorrente (player ext_id 30311442 + outros 590): player faz aposta no MINES, refresh na tela cancela a bet no backend (`c_txn_type=72`), saldo volta. Repete em loop.

A tag `CANCEL_HEAVY_DAILY` captura players com pelo menos 10 cancelamentos (`c_txn_type=72 SUCCESS`) em um unico dia BRT na janela de 90d da matriz.

Calibracao na base de abusers do Castrin (2.102 players validados):
- 596 caem em 13/05 ≈ 591 abusers confirmados pelo Head ("resgataram bonus sem turnover")

Rodada completa hoje 18/05 sobre toda a base ativa (182.243 players):
- **4.370 players** com `cancel_heavy_daily` ativo (2,40% da base)
- **+1.111 players** desceram para Ruim/Muito Ruim (efeito desejado)

---

## O que tem que substituir no orquestrador

### Arquivos que mudaram

| Arquivo | Status | Mudanca |
|---|---|---|
| `ec2_deploy/sql/risk_matrix/CANCEL_HEAVY_DAILY.sql` | **NOVO** | Criar |
| `ec2_deploy/pipelines/risk_matrix_pipeline.py` | substituir | Adicionou tag em TAG_ORDER, TAG_TO_COLUMN, TAG_SCORES + legenda |
| `scripts/sql/risk_matrix/CANCEL_HEAVY_DAILY.sql` | **NOVO** (mirror) | Criar — espelho do canonico em ec2_deploy/ |

### Links raw GitHub

- https://raw.githubusercontent.com/mateusrfabro/multibet-analytics/37172e8/ec2_deploy/sql/risk_matrix/CANCEL_HEAVY_DAILY.sql
- https://raw.githubusercontent.com/mateusrfabro/multibet-analytics/37172e8/ec2_deploy/pipelines/risk_matrix_pipeline.py

---

## Detalhes tecnicos do SQL

**Criterio:**
```sql
WHERE c_txn_type = 72
  AND c_txn_status = 'SUCCESS'
  AND c_start_time BETWEEN <hoje - 90d> AND <hoje>
GROUP BY user_id, dia_brt
HAVING COUNT(*) >= 10
```

**Score:** -10
**Tag CRM Smartico:** `RISK_CANCEL_HEAVY_DAILY`
**Categoria:** Comportamental / Negativa
**Janela:** 90 dias (padrao da matriz)
**Timezone:** BRT (regra ouro CLAUDE.md — `AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'`)
**Filtro test users:** Sim (`f.c_test_user = false`)

---

## Schema do PostgreSQL — atualizacao automatica

O `risk_matrix_pipeline.py` em `save_to_postgres()` ja faz auto-migracao do schema:

1. **ADD COLUMN `cancel_heavy_daily`** — adiciona automaticamente na primeira execucao
2. **DROP COLUMN `player_not_valid`** — limpa coluna legacy (idempotente, ja foi rodado uma vez)

Voce nao precisa rodar `ALTER TABLE` manual. O pipeline faz tudo.

---

## Smartico push

Quando o `push_risk_to_smartico.py` rodar (cron habitual), ele:
- Le snapshot atual vs anterior em `multibet.risk_tags`
- Detecta tag nova `cancel_heavy_daily` automaticamente
- Push de `RISK_CANCEL_HEAVY_DAILY` para os 4.370+ players via S2S
- `skip_cjm=true` (nao dispara jornadas)

---

## Validacao pos-deploy

Apos substituir no orquestrador e rodar a 1a vez no cron, validar:

1. **Log do pipeline:** deve aparecer `[22/22] Executando CANCEL_HEAVY_DAILY...` e o numero deve ficar entre **3.500 e 5.500** flagados (faixa esperada com janela rolante de 90d).

2. **Distribuicao por tier (pos-merge):** procurar no log a linha:
   ```
   cancel_heavy_daily            :   X,XXX ( X.XX%)
   ```
   Deve ficar em **~2,3-2,5%** da base ativa.

3. **DB check:** `SELECT COUNT(*) FROM multibet.risk_tags WHERE snapshot_date = CURRENT_DATE AND cancel_heavy_daily != 0;` — deve casar com o numero do log.

4. **Tier shifts esperados:** primeira rodada apos deploy vai ter aumento em `Ruim` e `Muito Ruim` (~+700 e +400 respectivamente) por conta dos 4K novos players flagados.

---

## Roll back se algo der errado

```sql
-- Reverter ADD COLUMN (vai apagar dados!)
ALTER TABLE multibet.risk_tags DROP COLUMN cancel_heavy_daily;

-- Restaurar player_not_valid se necessario (dropada permanentemente no v2.2)
ALTER TABLE multibet.risk_tags ADD COLUMN player_not_valid INTEGER DEFAULT 0;
```

Mas: **nao precisa reverter** se algo estranho aparecer — basta voltar o pipeline para o commit anterior (`2662176`) que ele para de calcular a tag nova. Os snapshots historicos com a coluna ficam preservados.

---

## Schedule

Mantem cron atual (sem mudanca de horario).

---

## CSVs gerados (mandar via Drive pro Castrin)

Em `output/`:
- `risk_matrix_2026-05-18_FINAL.csv` — matriz completa nova (182.243 linhas, 22 tags)
- `risk_matrix_tier_changes_2026-05-14_vs_2026-05-18.csv` — auditoria (24.427 que mudaram de tier)
- `risk_matrix_cancel_heavy_flagados_2026-05-18.csv` — top 4.370 novos flagados

---

## Apendice — scripts de auditoria/calibracao usados

Todos commitados em `scripts/`:
- `investiga_fraude_30311442.py` — investigacao do player original
- `diag_assinatura_30311442.py` — sequencia minuto-a-minuto
- `confirma_game_category_30311442.py` — c_game_category=158 = MINES
- `calibra_3_tags_base_castrin.py` — cross-check na base do Head
- `diff_matriz_14_vs_15mai.py` — comparativo antes/depois

---

**Proximo passo:** voce substitui no orquestrador. Quando rodar a 1a vez, me da um ping pra eu validar log + Smartico push. Qualquer duvida me chama.
