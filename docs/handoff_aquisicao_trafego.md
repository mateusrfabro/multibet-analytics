# Handoff: Aba "Aquisicao Trafego" no Front

**De:** Squad Intelligence (Mateus F)
**Para:** Front-end (Gusta)
**Data:** 2026-03-27
**Status:** Tabela + View + ETL prontos. Aguardando integracao no front.

---

## Contexto

Nova aba de **Aquisicao por Trafego Pago** para o front `db.supernovagaming.com.br`.
Complementa a aba "Aquisicao" existente (que passa a se chamar **"Aquisicao Safrada"**).

Diferenca:
- **Safrada** = foco em redeposito (STD, TTD, QTD+) por cohort de registro
- **Trafego** = foco em performance do canal (GGR, NGR, conversao, engajamento)

---

## Objetos no Super Nova DB

### Tabela base
```sql
multibet.aquisicao_trafego_diario
```
- 1 linha por dia por canal (PK: `dt` + `channel`)
- Alimentada pelo ETL `pipelines/etl_aquisicao_trafego_diario.py`
- Dados a partir de 2026-03-20

### View de consumo (USAR ESTA NO FRONT)
```sql
multibet.vw_aquisicao_trafego
```
Colunas com nomes em portugues, ordenada por data DESC:

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `data` | DATE | Dia (BRT) |
| `canal` | VARCHAR | Google, Meta (futuro: TikTok, Organico) |
| `cadastros` | INT | Novos registros (REG) |
| `ftd` | INT | Primeiro deposito (FTD) |
| `ftd_amount` | NUMERIC | Valor total dos FTDs (R$) |
| `conversao_ftd_pct` | NUMERIC | Taxa REG->FTD (%) |
| `ticket_medio_ftd` | NUMERIC | Ticket medio do FTD (R$) |
| `deposito` | NUMERIC | Total depositado (R$) |
| `saque` | NUMERIC | Total sacado (R$) |
| `net_deposit` | NUMERIC | Deposito - Saque (R$) |
| `ggr_casino` | NUMERIC | Receita bruta cassino (R$) - somente real cash |
| `ggr_sport` | NUMERIC | Receita bruta esportiva (R$) |
| `ggr_total` | NUMERIC | GGR Casino + Sport (R$) |
| `bonus_cost` | NUMERIC | Custo de bonus emitidos (R$) |
| `ngr` | NUMERIC | Receita liquida = GGR - Bonus (R$) |
| `players_ativos` | INT | Jogadores unicos com atividade no dia |
| `refreshed_at` | TIMESTAMPTZ | Ultima atualizacao do ETL |

---

## Queries de exemplo para o front

### Dados por periodo + canal (igual ao filtro da Safrada)
```sql
SELECT *
FROM multibet.vw_aquisicao_trafego
WHERE data BETWEEN '2026-03-01' AND '2026-03-27'
  AND canal = 'Google'
ORDER BY data DESC;
```

### Dados consolidados (todos os canais somados)
```sql
SELECT
    data,
    'Consolidado' AS canal,
    SUM(cadastros) AS cadastros,
    SUM(ftd) AS ftd,
    SUM(ftd_amount) AS ftd_amount,
    ROUND(SUM(ftd)::numeric / NULLIF(SUM(cadastros), 0) * 100, 1) AS conversao_ftd_pct,
    ROUND(SUM(ftd_amount) / NULLIF(SUM(ftd), 0), 2) AS ticket_medio_ftd,
    SUM(deposito) AS deposito,
    SUM(saque) AS saque,
    SUM(net_deposit) AS net_deposit,
    SUM(ggr_casino) AS ggr_casino,
    SUM(ggr_sport) AS ggr_sport,
    SUM(ggr_total) AS ggr_total,
    SUM(bonus_cost) AS bonus_cost,
    SUM(ngr) AS ngr,
    SUM(players_ativos) AS players_ativos
FROM multibet.vw_aquisicao_trafego
WHERE data BETWEEN '2026-03-01' AND '2026-03-27'
GROUP BY data
ORDER BY data DESC;
```

### KPIs do topo (totais do periodo)
```sql
SELECT
    SUM(cadastros) AS total_cadastros,
    SUM(ftd) AS total_ftd,
    ROUND(SUM(ftd)::numeric / NULLIF(SUM(cadastros), 0) * 100, 1) AS conversao_ftd_pct,
    ROUND(SUM(ftd_amount) / NULLIF(SUM(ftd), 0), 2) AS ticket_medio_ftd,
    SUM(ftd_amount) AS total_ftd_amount,
    SUM(ngr) AS total_ngr,
    SUM(net_deposit) AS total_net_deposit
FROM multibet.vw_aquisicao_trafego
WHERE data BETWEEN '2026-03-01' AND '2026-03-27';
```

---

## Filtro de canal

O dropdown deve listar os canais disponiveis:
```sql
SELECT DISTINCT canal FROM multibet.vw_aquisicao_trafego ORDER BY canal;
```
Atualmente: `Google`, `Meta`. Novos canais serao adicionados automaticamente quando o ETL for atualizado.

Opcao "Todas as fontes" = query consolidada (GROUP BY data, sem filtro de canal).

---

## ETL — como atualizar os dados

### Execucao manual
```bash
# D-1 (padrao)
python pipelines/etl_aquisicao_trafego_diario.py

# Ultimos 7 dias (reprocessar)
python pipelines/etl_aquisicao_trafego_diario.py --days 7

# Carga historica (30 dias)
python pipelines/etl_aquisicao_trafego_diario.py --days 30
```

### Agendamento sugerido (crontab)
```bash
# A cada hora (dados de hoje parciais + D-1 consolidado)
0 * * * * cd /path/to/MultiBet && python pipelines/etl_aquisicao_trafego_diario.py --days 1 >> /var/log/etl_aquisicao.log 2>&1

# Carga historica semanal (reprocessa 7 dias, cobre correcoes retroativas)
0 5 * * 1 cd /path/to/MultiBet && python pipelines/etl_aquisicao_trafego_diario.py --days 7 >> /var/log/etl_aquisicao.log 2>&1
```
(05:00 UTC = 02:00 BRT)

### Dia atual (parcial)
O ETL sempre inclui o dia atual por default (dados parciais ate o momento).
Para desabilitar: `--no-today`.
Cada execucao sobrescreve os dados do dia atual (DELETE + INSERT), mantendo sempre o snapshot mais recente.

### Idempotencia
O ETL faz DELETE + INSERT por (dt, channel). Pode rodar varias vezes sem duplicar dados.

---

## Layout sugerido para a aba

Baseado na aba Safrada existente, com ajustes:

### KPIs no topo (5 cards)
1. CADASTROS (total)
2. FTD (total)
3. CONVERSAO FTD (%)
4. TICKET MEDIO FTD (R$)
5. NGR (R$) — **novo**, principal metrica de ROI

### Tabela principal
Mesma estrutura da Safrada, colunas:
DATA | CADASTROS | FTD | FTD AMOUNT | CONV% | DEPOSITO | SAQUE | NET DEPOSIT | GGR CASINO | GGR SPORT | NGR | PLAYERS ATIVOS

### Filtros
- **Periodo**: date picker (inicio/fim)
- **Canal**: dropdown (Google, Meta, Todas as fontes)

### Graficos (opcionais)
- Funil de aquisicao (CADASTRO -> FTD -> DEPOSITANTE)
- NGR por canal (barras empilhadas)

---

## Notas tecnicas
- Valores ja em BRL (nao dividir por 100)
- GGR = somente dinheiro real (sub-fund isolation, sem bonus)
- NGR = GGR total - Bonus Cost (proxy operacional, nao contabil)
- Test users ja excluidos no ETL
- Conv% nunca deve ultrapassar 100% — FTD = same-day conversion (registrou E depositou no mesmo dia). Corrigido em 27/03/2026: bug anterior contava FTDs de registros antigos
- `refreshed_at` mostra quando o ETL rodou pela ultima vez
- **Players Ativos no consolidado:** a query consolidada (SUM por dia) pode contar o mesmo jogador 2x se ele aparece em Google e Meta. Para precisao exata no consolidado, usar COUNT DISTINCT no Athena em vez de somar os canais. Para o uso diario por canal, o valor e correto
- **c_created_date (financeiro):** campo DATE truncado em UTC, impacto estimado <2.1% em dados de borda de dia. Aceitavel para agregacao diaria

---

## Changelog

### 27/03/2026 — Fix FTD same-day conversion
**Bug:** FTD contava TODOS os primeiros depositos do dia (inclusive de users registrados em dias anteriores), causando Conv% > 100% (ex: 26/03 Google mostrava 110.2%).

**Causa raiz:** `ps_bi.dim_user` tem registros inflados para alguns affiliate_ids (ex: 297657 mostra 1.8x o real). Query original usava dim_user sozinho para contar FTDs.

**Fix:** Query FTD agora usa bireports_ec2.tbl_ecr como base de REGs do dia (validado, sem duplicatas) e JOIN com dim_user apenas para checar ftd_datetime no mesmo dia. Cross-validado com BigQuery (~6% diff aceitavel).

**Impacto:** Todos os dados historicos reprocessados com `--days 30`. Conversao Google caiu de ~65% para ~43%, Meta de ~28% para ~27% (Meta tinha pouco impacto).

---

## Notas para automacao (Mauro)

- **Frequencia recomendada:** a cada hora (dados parciais do dia) + reprocesso semanal --days 7
- **Idempotencia:** DELETE + INSERT por (dt, channel) — seguro rodar varias vezes
- **Cross-database:** query FTD faz JOIN entre bireports_ec2 e ps_bi no Athena (funciona nativamente)
- **Custo Athena:** ~3 queries por canal por dia. Para --days 1 com 3 canais = ~9 queries/execucao
- **Monitoramento:** se Conv% > 80% em qualquer dia, provavelmente ha bug. Range esperado: 25-55%
- **NUNCA usar ps_bi.dim_user sozinho para contar REGs ou FTDs por affiliate** — usar bireports_ec2.tbl_ecr como base