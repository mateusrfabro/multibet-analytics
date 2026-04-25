# reports/

Pasta de **entregas** (CSVs, Excel, HTML, PDFs, legendas) geradas por scripts/CLI.

## Convencoes

- Toda entrega final tem sufixo `_FINAL` no nome
- Toda entrega final TEM legenda obrigatoria (`_legenda.txt` ao lado)
- Versoes antigas vao para `_archive/lixeira_<YYYY-MM-DD>/` (nao deletar)
- CSVs/XLSX/HTML estao no `.gitignore` — sao output, nao codigo. So legendas (.txt) e .md vao pro git
- Saidas grandes (> 50 MB) devem ser revisadas antes de commitar

## Como adicionar uma entrega ao log abaixo

Apos rodar uma extracao, adicionar 1 linha em `LOG.md` (proximo do README) ou
no proprio README, no formato:

`YYYY-MM-DD  | <demandante> | <demanda resumida> | <arquivo principal>`

Isso da rastreabilidade — daqui 6 meses sera dificil saber pra quem foi cada CSV.

## Estrutura recomendada (em evolucao)

```
reports/
├── README.md             # este arquivo
├── LOG.md                # log append-only de entregas (criar quando virar habito)
├── *.csv / *.xlsx        # entregas vigentes (nao versionadas no git)
├── *_legenda.txt         # legendas (versionadas)
├── ggr_19abr/            # subpastas por projeto/analise especifica
├── handoff_gabriel_p4t_20260419/
├── afiliados_marco_auditoria/
└── afiliados_affbr_marco_auditoria/
```

## Demandantes recorrentes (referencia)

- **Rapha (CRM Leader):** bases de players para Smartico (matching via `external_id`)
- **Dudu (Trafego):** reports de affiliates, bases de FTD por canal
- **Castrin (Head Dados):** snapshots diarios, resumos executivos
- **Mauro (Sr Analytics):** auditorias de bronze, validacoes
- **Gusta (Sr Infra):** views consumidas em pipelines
- **Rafael Conson (CGO):** P&L afiliados, performance comercial
- **Gabriel Barbosa (CTO):** matriz risco, anti-fraude

## Pipelines que gravam aqui

- `scripts/extract_*.py` — entregas pontuais
- `cli.py affiliate-base / affiliate-daily` — recorrentes (via snova_cli)
- `pipelines/report_tempo_resgate_bonus.py` — mensal
- `pipelines/grandes_ganhos.py` — diario
- `pipelines/pcr_pipeline.py` — semanal/mensal

## Limpeza

- Mensal: revisar arquivos > 50 MB e mover para `_archive/` se nao forem mais consultados
- Trimestral: arquivar entregas finalizadas em `_archive/historico_<trimestre>/`
- Semestral: avaliar deletar conteudo de `_archive/lixeira_*` apos 30+ dias sem necessidade

## Ultima limpeza grande

**24/04/2026** — Fase A+B aprovadas pelo squad (best-practices + auditor):
- 5 arquivos orfaos shell deletados
- 5 scripts `_tmp_/_adhoc_` arquivados
- 6 outputs de teste CLI arquivados
- 16 reports versionados arquivados (`tempo_resgate_bonus_raw_*` antigos, `pcr_ratings_2026-04-08`, etc) — ~544 MB liberados
- 20 scripts versionados antigos arquivados
- Detalhe: `_archive/lixeira_2026-04-25/`
