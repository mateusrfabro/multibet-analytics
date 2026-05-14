# Handoff — incluir custos Junior Santana e Josias na matriz aquisicao

**Para:** Gusta
**De:** Mateus
**Data:** 14/05/2026
**Commit:** [b70b049](https://github.com/mateusrfabro/multibet-analytics/commit/b70b049)

## Objetivo
Diretoria pediu (via Mauro) que os custos das BMs do **Junior Santana** e **Josias**
apareçam atribuidos a eles na matriz de aquisicao (filtros `Junior santana` e `Josias`).
Pipeline `sync_meta_spend` foi adaptado pra ja gravar `ad_source` + `affiliate_id`
corretos dessas 3 contas.

## O que substituir no orquestrador (2 arquivos)

| De (arquivo neste handoff) | Para (caminho na EC2) |
|---|---|
| `pipelines/sync_meta_spend.py` | `<repo orquestrador>/pipelines/sync_meta_spend.py` |
| `db/meta_ads.py`               | `<repo orquestrador>/db/meta_ads.py` |

Sem mudanca de cron, sem mudanca de wrapper `.sh`. O pipeline continua rodando 5x/dia
com `--days 2`.

## O que adicionar no `.env` da EC2

Anexar 3 IDs ao final de `META_ADS_ACCOUNT_IDS` (manter as 7 ja existentes):

```
,act_507388223796685,act_827977069307885,act_957406645376599
```

## O que muda no DB (multibet.fact_ad_spend)

Pipeline passa a gravar 3 valores distintos em `ad_source`:
- `meta` — contas in-house (Multibet, Multibet Verified...), `affiliate_id = NULL` *(comportamento anterior)*
- `Junior santana` — contas `act_507388223796685` e `act_827977069307885`, `affiliate_id = 457857`
- `Josias` — conta `act_957406645376599`, `affiliate_id = 467185`

DELETE do periodo agora cobre os 3 ad_sources (idempotente em re-runs).
Backfill 120d local ja foi feito (309 linhas Junior, 24 linhas Josias).

## Validacao pos-deploy

Rodar `tail -f` no log do primeiro intraday e conferir:
- linha `Deletados N registros antigos do periodo (ad_sources=['Josias', 'Junior santana', 'meta'])`
- linha `Sync concluido: ... | 9 contas | ...` (8 in-house + 3 novas - 1 sem permissao)

Query de sanity (Super Nova DB):
```sql
SELECT ad_source, affiliate_id, COUNT(*) linhas, ROUND(SUM(cost_brl)::numeric,2) spend
FROM multibet.fact_ad_spend
WHERE dt >= CURRENT_DATE - INTERVAL '2 days'
  AND ad_source IN ('meta','Junior santana','Josias')
GROUP BY ad_source, affiliate_id ORDER BY ad_source;
```
Esperado: 3 linhas, sem `Junior santana`/`Josias` com `affiliate_id` NULL.

## Detalhes tecnicos (ja no codigo, so referencia)

- `pipelines/sync_meta_spend.py` linha ~55: `META_ACCOUNT_OVERRIDES` dict (acrescentar afiliados novos aqui no futuro)
- `db/meta_ads.py` linha ~49: timeout HTTP 30s→90s (libera backfill longo; intraday `--days 2` continua respondendo em <5s)

## Conta com permissao negada (ja era esperado)

`act_846913941192022` ("Multibet sem BM") continua sem permissao no token BM2.
Pipeline tem resiliencia por conta (1 falha = warning, nao derruba) — sem acao necessaria.
