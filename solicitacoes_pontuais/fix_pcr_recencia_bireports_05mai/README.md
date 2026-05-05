# Fix PCR — migracao de fonte (gap dbt fct_player_activity_daily)

**Data:** 2026-05-05
**Para:** Gusta (orquestrador)
**De:** Mateus

## Resumo (1 linha)

Substituir 1 arquivo no orquestrador para que o PCR pare de usar a fato dbt
`ps_bi.fct_player_activity_daily` (congelada em 06/04/2026) e passe a ler de
`bireports_ec2.tbl_ecr_wise_daily_bi_summary` (raw atualizada D-1).

## Arquivo a substituir

```
pipelines/pcr_pipeline.py
```

Versao corrigida nesta pasta em `pipelines/pcr_pipeline.py`. Sem mudanca em
outros arquivos, sem mudanca de schema, sem nova dependencia.

## Como aplicar

1. Substituir `pipelines/pcr_pipeline.py` no orquestrador pela versao desta pasta
2. **Rodar manualmente uma vez** (nao cron) e me avisar
3. Apos minha validacao no Smartico, religar cron normal

## Cron

Mantido como esta: PCR 03:30 BRT, segmentacao_sa 04:00 BRT (+30min). Sem alteracao.

## Validacao apos deploy (Mateus)

Eu rodo no Super Nova DB:
```sql
SELECT player_id, snapshot_date, rating, recency_days
FROM multibet.pcr_ratings
WHERE player_id = 305245081792208985
ORDER BY snapshot_date DESC LIMIT 5;
```
E peco ao Victor (CRM VIPs) pra comparar o mesmo perfil que ele apontou
no painel Smartico antes/depois — ele tem acesso e e o stakeholder original
do reporte.

## Reversao

Restaurar a versao anterior do `pipelines/pcr_pipeline.py`. Sem efeito colateral
persistente.

## Observacao operacional

A base do PCR vai crescer de ~128k → ~185k jogadores num unico dia. Esse aumento
e correcao do gap (jogadores que existiam mas estavam sumidos da fato congelada),
nao crescimento real. Eu vou alinhar com o squad CRM antes de habilitar o cron.
