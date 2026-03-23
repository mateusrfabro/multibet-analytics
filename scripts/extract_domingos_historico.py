"""
Extracao Historica de Domingos — Previsao para 23/03/2026
=========================================================
Objetivo: Extrair dados historicos de domingos (ultimos 7-8) para prever
           comportamento do proximo domingo (23/03/2026).

Fonte: Athena (fund_ec2, bireports_ec2, ps_bi)
Regras:
  - Timezone BRT obrigatorio (AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
  - Test users excluidos (c_test_user = false / is_test = false)
  - Valores fund_ec2 em centavos (/100.0), ps_bi ja em BRL
  - Sintaxe Presto/Trino

Queries:
  1. Depositos por domingo (7 domingos)
  2. Padrao horario de depositos em domingos
  3. GGR Casino por domingo
  4. Saques por domingo
  5. FTDs e Registros por domingo (via ps_bi.dim_user)
  6. Depositos por dia da semana (ultimos 30 dias, contexto sazonal)

Saida: CSVs em output/
Data: 2026-03-22
Autor: Mateus F. (Squad Intelligence Engine)
"""

import sys
import os
import logging
import time as time_mod

# Garantir path do projeto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from db.athena import query_athena

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# Diretorio de saida
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output"
)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# =============================================================================
# QUERY 1: Depositos por domingo (7 domingos: 02/fev a 16/mar)
# =============================================================================
QUERY_1_DEPOSITOS_DOMINGO = """
-- ============================================================
-- Query 1: Depositos por domingo (ultimos 7 domingos)
-- Comparativo "banana com banana" para previsao do domingo 23/03
-- Fonte: fund_ec2 (centavos /100), join com bireports_ec2 (test users)
-- ============================================================
SELECT
    DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_brt,
    COUNT(*) AS qtd_depositos,
    ROUND(SUM(CAST(f.c_amount_in_ecr_ccy AS DOUBLE) / 100.0), 2) AS total_depositos_brl,
    COUNT(DISTINCT f.c_ecr_id) AS depositantes_unicos,
    ROUND(AVG(CAST(f.c_amount_in_ecr_ccy AS DOUBLE) / 100.0), 2) AS ticket_medio
FROM fund_ec2.tbl_real_fund_txn f
INNER JOIN bireports_ec2.tbl_ecr e
    ON e.c_ecr_id = f.c_ecr_id
    AND e.c_test_user = false
WHERE f.c_txn_type = 1                     -- deposito
  AND f.c_txn_status = 'SUCCESS'            -- apenas confirmados
  AND DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') IN (
        DATE '2026-02-01',   -- domingo
        DATE '2026-02-08',   -- domingo
        DATE '2026-02-15',   -- domingo
        DATE '2026-02-22',   -- domingo
        DATE '2026-03-01',   -- domingo
        DATE '2026-03-08',   -- domingo
        DATE '2026-03-15'    -- domingo
  )
GROUP BY DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
ORDER BY data_brt
"""

# =============================================================================
# QUERY 2: Padrao horario de depositos em domingos (agregado por hora)
# Usa day_of_week() — Presto: 1=Monday, 7=Sunday
# =============================================================================
QUERY_2_PADRAO_HORARIO = """
-- ============================================================
-- Query 2: Depositos por hora nos domingos (padrao horario)
-- Agrega todos os domingos do periodo para entender picos/vales
-- Fonte: fund_ec2, join bireports_ec2
-- day_of_week(date): 1=Monday ... 7=Sunday (Presto/Trino)
-- ============================================================
SELECT
    HOUR(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora_brt,
    COUNT(*) AS qtd_depositos,
    ROUND(SUM(CAST(f.c_amount_in_ecr_ccy AS DOUBLE) / 100.0), 2) AS total_brl,
    COUNT(DISTINCT f.c_ecr_id) AS depositantes,
    ROUND(AVG(CAST(f.c_amount_in_ecr_ccy AS DOUBLE) / 100.0), 2) AS ticket_medio
FROM fund_ec2.tbl_real_fund_txn f
INNER JOIN bireports_ec2.tbl_ecr e
    ON e.c_ecr_id = f.c_ecr_id
    AND e.c_test_user = false
WHERE f.c_txn_type = 1
  AND f.c_txn_status = 'SUCCESS'
  -- Filtro temporal: domingos entre 02/fev e 16/mar (BRT)
  -- Usa offset UTC para cobrir o dia completo em BRT (03:00 UTC = 00:00 BRT)
  AND f.c_start_time >= TIMESTAMP '2026-02-02 03:00:00'
  AND f.c_start_time <  TIMESTAMP '2026-03-17 03:00:00'
  -- Filtra somente domingos (day_of_week = 7)
  AND day_of_week(DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')) = 7
GROUP BY HOUR(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
ORDER BY hora_brt
"""

# =============================================================================
# QUERY 3: GGR Casino por domingo (bets - wins)
# =============================================================================
QUERY_3_GGR_DOMINGO = """
-- ============================================================
-- Query 3: GGR Casino por domingo
-- c_txn_type 27 = Aposta (bet), 45 = Win
-- GGR = Bets - Wins (receita da casa)
-- Fonte: fund_ec2 (centavos /100)
-- ============================================================
SELECT
    DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_brt,
    ROUND(SUM(CASE WHEN f.c_txn_type = 27
        THEN CAST(f.c_amount_in_ecr_ccy AS DOUBLE)/100.0 ELSE 0 END), 2) AS casino_bets,
    ROUND(SUM(CASE WHEN f.c_txn_type = 45
        THEN CAST(f.c_amount_in_ecr_ccy AS DOUBLE)/100.0 ELSE 0 END), 2) AS casino_wins,
    ROUND(
        SUM(CASE WHEN f.c_txn_type = 27
            THEN CAST(f.c_amount_in_ecr_ccy AS DOUBLE)/100.0 ELSE 0 END) -
        SUM(CASE WHEN f.c_txn_type = 45
            THEN CAST(f.c_amount_in_ecr_ccy AS DOUBLE)/100.0 ELSE 0 END),
    2) AS ggr_casino,
    COUNT(CASE WHEN f.c_txn_type = 27 THEN 1 END) AS qtd_bets,
    COUNT(DISTINCT f.c_ecr_id) AS jogadores_unicos
FROM fund_ec2.tbl_real_fund_txn f
INNER JOIN bireports_ec2.tbl_ecr e
    ON e.c_ecr_id = f.c_ecr_id
    AND e.c_test_user = false
WHERE f.c_txn_type IN (27, 45)              -- bets e wins
  AND f.c_txn_status = 'SUCCESS'
  AND DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') IN (
        DATE '2026-02-01', DATE '2026-02-08', DATE '2026-02-15', DATE '2026-02-22',
        DATE '2026-03-01', DATE '2026-03-08', DATE '2026-03-15'
  )
GROUP BY DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
ORDER BY data_brt
"""

# =============================================================================
# QUERY 4: Saques por domingo (para net deposit = depositos - saques)
# =============================================================================
QUERY_4_SAQUES_DOMINGO = """
-- ============================================================
-- Query 4: Saques por domingo
-- c_txn_type = 2 (withdrawal)
-- Para calcular Net Deposit = Depositos - Saques
-- Fonte: fund_ec2 (centavos /100)
-- ============================================================
SELECT
    DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_brt,
    COUNT(*) AS qtd_saques,
    ROUND(SUM(CAST(f.c_amount_in_ecr_ccy AS DOUBLE) / 100.0), 2) AS total_saques_brl,
    COUNT(DISTINCT f.c_ecr_id) AS sacadores_unicos
FROM fund_ec2.tbl_real_fund_txn f
INNER JOIN bireports_ec2.tbl_ecr e
    ON e.c_ecr_id = f.c_ecr_id
    AND e.c_test_user = false
WHERE f.c_txn_type = 2                      -- saque
  AND f.c_txn_status = 'SUCCESS'
  AND DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') IN (
        DATE '2026-02-01', DATE '2026-02-08', DATE '2026-02-15', DATE '2026-02-22',
        DATE '2026-03-01', DATE '2026-03-08', DATE '2026-03-15'
  )
GROUP BY DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
ORDER BY data_brt
"""

# =============================================================================
# QUERY 5: FTDs e Registros por domingo (via ps_bi.dim_user)
# ps_bi.dim_user tem registration_date, ftd_date, has_ftd, is_test
# Valores ja em BRL (sem /100)
# =============================================================================
QUERY_5_FTD_DOMINGO = """
-- ============================================================
-- Query 5: FTDs e Registros por domingo
-- Fonte: ps_bi.dim_user (pre-calculado pelo dbt)
-- registration_date e ftd_date sao tipo DATE (sem hora)
-- has_ftd = flag se fez primeiro deposito
-- ftd_amount_inhouse = valor FTD em BRL (ps_bi ja em BRL)
-- is_test = false para excluir test users
-- ============================================================
WITH domingos AS (
    -- Lista explicita dos domingos para filtro
    SELECT dt FROM (
        VALUES
            DATE '2026-02-01', DATE '2026-02-08', DATE '2026-02-15',
            DATE '2026-02-22', DATE '2026-03-01', DATE '2026-03-08',
            DATE '2026-03-15'
    ) AS t(dt)
)
SELECT
    CAST(u.registration_date AS DATE) AS data_brt,

    -- Total de registros naquele domingo
    COUNT(*) AS total_registros,

    -- Total de FTDs (primeiro deposito) de quem registrou naquele domingo
    -- (pode ter feito o FTD em outro dia, mas registrou no domingo)
    COUNT_IF(u.has_ftd = 1) AS total_ftds,

    -- Taxa de conversao registro -> FTD
    ROUND(
        CAST(COUNT_IF(u.has_ftd = 1) AS DOUBLE)
        / NULLIF(CAST(COUNT(*) AS DOUBLE), 0) * 100.0
    , 2) AS taxa_conversao_pct,

    -- Ticket medio do FTD (ps_bi ja em BRL)
    ROUND(AVG(CASE WHEN u.has_ftd = 1 THEN u.ftd_amount_inhouse END), 2)
        AS ftd_ticket_medio_brl,

    -- Valor total de FTDs
    ROUND(SUM(CASE WHEN u.has_ftd = 1 THEN u.ftd_amount_inhouse ELSE 0 END), 2)
        AS ftd_total_brl

FROM ps_bi.dim_user u
INNER JOIN domingos d ON CAST(u.registration_date AS DATE) = d.dt
WHERE u.is_test = false
GROUP BY CAST(u.registration_date AS DATE)
ORDER BY data_brt
"""

# Fallback se VALUES nao funcionar no Presto/Trino com multiplos valores inline
QUERY_5_FTD_DOMINGO_FALLBACK = """
-- ============================================================
-- Query 5 (fallback): FTDs e Registros por domingo
-- Sem CTE VALUES — usa IN direto
-- ============================================================
SELECT
    CAST(u.registration_date AS DATE) AS data_brt,
    COUNT(*) AS total_registros,
    COUNT_IF(u.has_ftd = 1) AS total_ftds,
    ROUND(
        CAST(COUNT_IF(u.has_ftd = 1) AS DOUBLE)
        / NULLIF(CAST(COUNT(*) AS DOUBLE), 0) * 100.0
    , 2) AS taxa_conversao_pct,
    ROUND(AVG(CASE WHEN u.has_ftd = 1 THEN u.ftd_amount_inhouse END), 2)
        AS ftd_ticket_medio_brl,
    ROUND(SUM(CASE WHEN u.has_ftd = 1 THEN u.ftd_amount_inhouse ELSE 0 END), 2)
        AS ftd_total_brl
FROM ps_bi.dim_user u
WHERE u.is_test = false
  AND CAST(u.registration_date AS DATE) IN (
        DATE '2026-02-01', DATE '2026-02-08', DATE '2026-02-15',
        DATE '2026-02-22', DATE '2026-03-01', DATE '2026-03-08',
        DATE '2026-03-15'
  )
GROUP BY CAST(u.registration_date AS DATE)
ORDER BY data_brt
"""

# =============================================================================
# QUERY 6: Depositos por dia da semana (ultimos 30 dias)
# Para contexto sazonal: como o domingo se compara aos outros dias
# =============================================================================
QUERY_6_DIA_SEMANA = """
-- ============================================================
-- Query 6: Depositos medios por dia da semana (ultimos 30 dias)
-- Contexto sazonal: performance relativa do domingo
-- day_of_week(): 1=Monday ... 7=Sunday
-- Fonte: fund_ec2 (centavos /100)
-- ============================================================
SELECT
    day_of_week(DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')) AS dow,
    CASE day_of_week(DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'))
        WHEN 1 THEN 'Segunda'
        WHEN 2 THEN 'Terca'
        WHEN 3 THEN 'Quarta'
        WHEN 4 THEN 'Quinta'
        WHEN 5 THEN 'Sexta'
        WHEN 6 THEN 'Sabado'
        WHEN 7 THEN 'Domingo'
    END AS dia_semana,
    -- Media de depositos por dia (total / quantidade de dias amostrados)
    COUNT(*) / COUNT(DISTINCT DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'))
        AS avg_qtd,
    ROUND(
        SUM(CAST(f.c_amount_in_ecr_ccy AS DOUBLE) / 100.0)
        / COUNT(DISTINCT DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'))
    , 2) AS avg_valor_brl,
    COUNT(DISTINCT DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'))
        AS dias_amostrados
FROM fund_ec2.tbl_real_fund_txn f
INNER JOIN bireports_ec2.tbl_ecr e
    ON e.c_ecr_id = f.c_ecr_id
    AND e.c_test_user = false
WHERE f.c_txn_type = 1
  AND f.c_txn_status = 'SUCCESS'
  -- Ultimos 30 dias em UTC (offset BRT: 03:00 UTC = 00:00 BRT)
  AND f.c_start_time >= TIMESTAMP '2026-02-20 03:00:00'
  AND f.c_start_time <  TIMESTAMP '2026-03-22 03:00:00'
GROUP BY day_of_week(DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'))
ORDER BY dow
"""


# =============================================================================
# FUNCAO AUXILIAR: executar query com log e salvar CSV
# =============================================================================
def run_and_save(query_name: str, sql: str, csv_filename: str, database: str = "fund_ec2") -> pd.DataFrame:
    """Executa query no Athena, loga resultado resumido e salva CSV."""
    csv_path = os.path.join(OUTPUT_DIR, csv_filename)
    log.info(f"{'='*60}")
    log.info(f"Executando {query_name}...")
    log.info(f"Database: {database}")

    start = time_mod.time()
    try:
        df = query_athena(sql, database=database)
    except Exception as e:
        log.error(f"ERRO em {query_name}: {e}")
        raise
    elapsed = time_mod.time() - start

    log.info(f"Concluido em {elapsed:.1f}s — {len(df)} linhas retornadas")

    if len(df) > 0:
        print(f"\n--- {query_name} ---")
        print(df.to_string(index=False))
        print()

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log.info(f"Salvo em: {csv_path}")
    return df


# =============================================================================
# MAIN — executa todas as 6 queries sequencialmente
# =============================================================================
def main():
    log.info("=" * 60)
    log.info("EXTRACAO HISTORICA DE DOMINGOS — Previsao 23/03/2026")
    log.info("=" * 60)
    log.info(f"Output dir: {OUTPUT_DIR}")
    log.info("")

    results = {}

    # -- Query 1: Depositos por domingo --
    results["depositos"] = run_and_save(
        "Query 1 — Depositos por domingo",
        QUERY_1_DEPOSITOS_DOMINGO,
        "domingos_depositos_historico.csv",
        database="fund_ec2",
    )

    # -- Query 2: Padrao horario --
    results["horario"] = run_and_save(
        "Query 2 — Padrao horario de depositos (domingos)",
        QUERY_2_PADRAO_HORARIO,
        "domingos_padrao_horario.csv",
        database="fund_ec2",
    )

    # -- Query 3: GGR Casino --
    results["ggr"] = run_and_save(
        "Query 3 — GGR Casino por domingo",
        QUERY_3_GGR_DOMINGO,
        "domingos_ggr_historico.csv",
        database="fund_ec2",
    )

    # -- Query 4: Saques --
    results["saques"] = run_and_save(
        "Query 4 — Saques por domingo",
        QUERY_4_SAQUES_DOMINGO,
        "domingos_saques_historico.csv",
        database="fund_ec2",
    )

    # -- Query 5: FTDs e Registros (ps_bi) --
    # Tenta primeiro com VALUES CTE, se falhar usa fallback com IN
    try:
        results["ftd"] = run_and_save(
            "Query 5 — FTDs e Registros por domingo",
            QUERY_5_FTD_DOMINGO,
            "domingos_ftd_historico.csv",
            database="ps_bi",
        )
    except Exception as e:
        log.warning(f"Query 5 com VALUES falhou: {e}")
        log.info("Tentando fallback sem VALUES...")
        results["ftd"] = run_and_save(
            "Query 5 (fallback) — FTDs e Registros por domingo",
            QUERY_5_FTD_DOMINGO_FALLBACK,
            "domingos_ftd_historico.csv",
            database="ps_bi",
        )

    # -- Query 6: Depositos por dia da semana --
    results["dia_semana"] = run_and_save(
        "Query 6 — Depositos por dia da semana (30 dias)",
        QUERY_6_DIA_SEMANA,
        "depositos_por_dia_semana_30d.csv",
        database="fund_ec2",
    )

    # =========================================================================
    # RESUMO CONSOLIDADO
    # =========================================================================
    log.info("")
    log.info("=" * 60)
    log.info("RESUMO CONSOLIDADO")
    log.info("=" * 60)

    # Depositos domingo — media e tendencia
    df_dep = results["depositos"]
    if len(df_dep) > 0:
        media_dep = df_dep["total_depositos_brl"].mean()
        media_qtd = df_dep["qtd_depositos"].mean()
        media_ticket = df_dep["ticket_medio"].mean()
        media_unicos = df_dep["depositantes_unicos"].mean()
        ultimo = df_dep.iloc[-1]

        log.info(f"\n[DEPOSITOS DOMINGO]")
        log.info(f"  Media 7 domingos: R$ {media_dep:,.2f} ({media_qtd:,.0f} txns, {media_unicos:,.0f} unicos)")
        log.info(f"  Ticket medio geral: R$ {media_ticket:,.2f}")
        log.info(f"  Ultimo domingo (16/03): R$ {ultimo.get('total_depositos_brl', 'N/A'):,.2f}")

    # GGR domingo
    df_ggr = results["ggr"]
    if len(df_ggr) > 0:
        media_ggr = df_ggr["ggr_casino"].mean()
        log.info(f"\n[GGR CASINO DOMINGO]")
        log.info(f"  Media 7 domingos: R$ {media_ggr:,.2f}")
        log.info(f"  Ultimo domingo: R$ {df_ggr.iloc[-1].get('ggr_casino', 'N/A'):,.2f}")

    # Net Deposit (depositos - saques)
    df_saq = results["saques"]
    if len(df_dep) > 0 and len(df_saq) > 0:
        merged = pd.merge(
            df_dep[["data_brt", "total_depositos_brl"]],
            df_saq[["data_brt", "total_saques_brl"]],
            on="data_brt",
            how="left",
        )
        merged["total_saques_brl"] = merged["total_saques_brl"].fillna(0)
        merged["net_deposit"] = merged["total_depositos_brl"] - merged["total_saques_brl"]
        media_net = merged["net_deposit"].mean()
        log.info(f"\n[NET DEPOSIT DOMINGO]")
        log.info(f"  Media 7 domingos: R$ {media_net:,.2f}")
        for _, row in merged.iterrows():
            log.info(f"  {row['data_brt']}: Net R$ {row['net_deposit']:,.2f}")

    # FTD
    df_ftd = results["ftd"]
    if len(df_ftd) > 0:
        media_reg = df_ftd["total_registros"].mean()
        media_ftd = df_ftd["total_ftds"].mean()
        media_conv = df_ftd["taxa_conversao_pct"].mean()
        log.info(f"\n[REGISTROS & FTDs DOMINGO]")
        log.info(f"  Media registros: {media_reg:,.0f}")
        log.info(f"  Media FTDs: {media_ftd:,.0f}")
        log.info(f"  Taxa conversao media: {media_conv:.1f}%")

    # Dia da semana — posicao do domingo
    df_dow = results["dia_semana"]
    if len(df_dow) > 0:
        log.info(f"\n[CONTEXTO SAZONAL — Depositos medios por dia da semana (30d)]")
        for _, row in df_dow.iterrows():
            marker = " <<<" if row.get("dia_semana") == "Domingo" else ""
            log.info(f"  {row.get('dia_semana', row.get('dow', '?'))}: "
                     f"R$ {row.get('avg_valor_brl', 0):,.2f} "
                     f"({row.get('avg_qtd', 0):,} txns/dia){marker}")

    # Pico horario
    df_hora = results["horario"]
    if len(df_hora) > 0:
        pico = df_hora.loc[df_hora["total_brl"].idxmax()]
        vale = df_hora.loc[df_hora["total_brl"].idxmin()]
        log.info(f"\n[PADRAO HORARIO DOMINGO]")
        log.info(f"  Pico: {int(pico['hora_brt'])}h — R$ {pico['total_brl']:,.2f}")
        log.info(f"  Vale: {int(vale['hora_brt'])}h — R$ {vale['total_brl']:,.2f}")

    log.info("")
    log.info("=" * 60)
    log.info("Todos os CSVs salvos em output/")
    log.info("Pronto para analise de previsao do domingo 23/03/2026")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
