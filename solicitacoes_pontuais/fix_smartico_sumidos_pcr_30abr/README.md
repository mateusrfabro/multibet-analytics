# Fix Smartico — REMOVE puro de jogadores que sumiram da base PCR

**Data:** 30/04/2026
**Para:** Gusta (orquestrador EC2)
**De:** Mateus

## Resumo (1 linha)

Trocar 1 arquivo no orquestrador para que o pipeline de Segmentacao S+A pare de deixar tags `PCR_RATING_*` fantasmas no Smartico para jogadores que sairam da base PCR.

## O bug

A funcao `_carregar_snapshot_anterior` em `pipelines/segmentacao_sa_smartico.py`
buscava o snapshot anterior em `multibet.segmentacao_sa_diaria` (so A+S, ~11k).
Mas o `df_atual` que entra na funcao e a **base PCR completa (~134k)**, vinda
de `multibet.pcr_ratings`.

Resultado: o set difference `ids_anterior - ids_atual` ficava ~0 sempre —
porque praticamente todo A+S de ontem ainda esta na base PCR de hoje, so
mudou de tier. Logo, jogadores que SAIRAM da base PCR (banidos, churned,
fora do filtro D-3y) **nunca recebiam o REMOVE puro** das tags `PCR_RATING_*`.

**Validado em prod 30/04** com 8 perfis no painel Smartico:
- ✅ 6/8 promovidos/rebaixados/estaveis: tag correta
- ❌ 2/8 sumidos da base: continuavam com `PCR_RATING_E` (29559201) e
  `PCR_RATING_D` (29784667) fantasmas

## A correcao

Trocar a fonte do snapshot anterior:

```diff
- FROM multibet.segmentacao_sa_diaria
+ FROM multibet.pcr_ratings
```

Sem mudanca de logica em mais nenhum ponto. So 1 arquivo alterado.

## Arquivo a substituir no orquestrador

```
pipelines/segmentacao_sa_smartico.py
```

Versao corrigida nesta pasta em `pipelines/segmentacao_sa_smartico.py`.

## Como aplicar (Gusta)

1. Substituir o arquivo `pipelines/segmentacao_sa_smartico.py` no
   orquestrador pela versao desta pasta (mesmo caminho).
2. Sem alteracoes em outros arquivos, sem dependencias novas, sem
   mudanca de schema.
3. Cron permanece como esta (04:00 BRT, dispara apos PCR 03:30).

## Validacao apos deploy

Apos a primeira rodada com a correcao (madrugada do dia seguinte ao deploy),
rodar o script ja existente:

```
python scripts/validar_pcr_smartico_diaria.py
```

E spot-check no Smartico nos IDs marcados como `SUMIU_HOJE` no CSV
`reports/validacao_pcr_smartico_<DATA>_spotcheck.csv` — confirmar que
nenhum deles tem mais tag `PCR_RATING_*`.

## Pendencia separada (NAO faz parte deste handoff)

Os ~986 jogadores que sumiram da base PCR no dia 30/04/2026 ficaram com
tag `PCR_RATING_*` fantasma no Smartico. Vou rodar um script one-shot
manualmente pra limpar essas tags retroativamente (REMOVE puro). Nao e
preciso fazer nada no orquestrador para isso.
