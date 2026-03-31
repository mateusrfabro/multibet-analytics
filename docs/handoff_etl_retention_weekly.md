# Handoff ETL — Active Player Retention vs Repeat Depositors

**Para:** Mauro (ETL) + Gusta (front)
**Data:** 27/03/2026

## Objetivo
Gráfico semanal (dom-sáb) com 4 métricas: depositantes atuais, anteriores, retention % e repeat depositor %.

## O que já está pronto

### Pipeline
- **Arquivo:** `pipelines/vw_active_player_retention_weekly.py`
- **Fonte:** Athena direto (`cashier_ec2` + `bireports_ec2`) — sem depender de bronze
- **Destino:** Super Nova DB → `multibet.etl_active_player_retention_weekly` (tabela)
- **Consumo:** `multibet.vw_active_player_retention_weekly` (view que aponta pra tabela)

### Como rodar
```bash
cd /path/to/MultiBet
python pipelines/vw_active_player_retention_weekly.py
```
- Execução: ~60s (query Athena + carga Super Nova DB)
- Estratégia: TRUNCATE + INSERT completo (22 semanas, carga leve)
- Idempotente: pode rodar quantas vezes quiser

### Schedule sugerido
- **Diário, 06:00 BRT** (dados D-1 consolidados no Athena)

## Colunas da view

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| semana | date | Domingo (início da semana dom-sáb) |
| semana_label | varchar | "Semana DD/MM" — label do eixo X |
| depositantes_semana_atual | int | Depositantes únicos na semana |
| depositantes_semana_anterior | int | Depositantes únicos na semana anterior |
| retidos_da_semana_anterior | int | Depositantes em AMBAS as semanas |
| repeat_depositors | int | Depositantes com 2+ depósitos na mesma semana |
| retention_pct | numeric | retidos / depositantes semana anterior * 100 |
| repeat_depositor_pct | numeric | repeat / total depositantes semana * 100 |

## Para o Gusta (front)

```sql
SELECT * FROM multibet.vw_active_player_retention_weekly ORDER BY semana
```

Mapeamento pro gráfico:
- **Eixo X:** `semana_label`
- **Barra roxo escuro:** `depositantes_semana_anterior`
- **Barra roxo claro:** `depositantes_semana_atual`
- **Linha laranja:** `repeat_depositor_pct` (eixo Y direito, %)
- **Linha verde tracejada:** `retention_pct` (eixo Y direito, %)

Nota: cores invertidas vs report original (pedido do time).

## Validação
- Athena vs Super Nova DB: **0% divergência** (24,569 depositantes, semana 08/03)
- Dados atualizados até D-1 do Athena

## Dependências
- `db/athena.py` (conexão Athena)
- `db/supernova.py` (conexão Super Nova DB via SSH tunnel)
- `.env` com credenciais Athena + Super Nova