"""
Validacao Critica — Dados Historicos de 7 Domingos
====================================================
Objetivo: Validar que os dados hardcoded no statistician_previsao_domingo.py
          batem com consultas diretas ao Athena.

Report vai para o CTO — precisao e obrigatoria.

Queries:
  1. Depositos por domingo (fund_ec2, centavos /100)
  2. Saques por domingo (fund_ec2, centavos /100)
  3. GGR Casino por domingo (ps_bi, ja em BRL)
  4. FTDs por domingo (ps_bi.dim_user, ftd_datetime em BRT)

Comparacao:
  - MATCH EXATO: delta = 0
  - DELTA ACEITAVEL: |delta| < 1%
  - DIVERGENCIA: |delta| >= 1%

Saida:
  - Console: tabela comparativa por metrica/domingo
  - CSV: output/validacao_domingos_historicos.csv

Data: 2026-03-22
Autor: Mateus F. (Squad Intelligence Engine)
"""

import sys
import os

# Forcar UTF-8 no stdout/stderr (evitar UnicodeEncodeError no Windows cp1252)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Caminho do projeto
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)

import logging
import pandas as pd
import numpy as np

from db.athena import query_athena

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# Diretorio de saida
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# =============================================================================
# DADOS DO REPORT (hardcoded no statistician_previsao_domingo.py)
# =============================================================================
DOMINGOS = [
    "2026-02-01", "2026-02-08", "2026-02-15", "2026-02-22",
    "2026-03-01", "2026-03-08", "2026-03-15",
]

REPORT_DEPOSITOS = {
    "2026-02-01": {"qtd": 8915, "total_brl": 1037980.43, "unicos": 5348, "ticket": 116.43},
    "2026-02-08": {"qtd": 9622, "total_brl": 1189437.28, "unicos": 5515, "ticket": 123.62},
    "2026-02-15": {"qtd": 8390, "total_brl": 1079345.06, "unicos": 4499, "ticket": 128.65},
    "2026-02-22": {"qtd": 8393, "total_brl": 1095181.67, "unicos": 4766, "ticket": 130.49},
    "2026-03-01": {"qtd": 9312, "total_brl": 1331640.68, "unicos": 5243, "ticket": 143.00},
    "2026-03-08": {"qtd": 11066, "total_brl": 1432130.48, "unicos": 6050, "ticket": 129.42},
    "2026-03-15": {"qtd": 11409, "total_brl": 1536200.59, "unicos": 5847, "ticket": 134.65},
}

REPORT_SAQUES = {
    "2026-02-01": 1094286.31,
    "2026-02-08": 1161913.87,
    "2026-02-15": 1109025.89,
    "2026-02-22": 1426418.76,
    "2026-03-01": 1170929.42,
    "2026-03-08": 1781574.61,
    "2026-03-15": 1890606.37,
}

REPORT_GGR = {
    "2026-02-01": 1151619.92,
    "2026-02-08": 2072286.28,
    "2026-02-15": 3519264.61,
    "2026-02-22": 485523.36,
    "2026-03-01": 206692.25,
    "2026-03-08": 157790.70,
    "2026-03-15": -51668.41,
}

REPORT_FTDS = {
    "2026-02-01": 862,
    "2026-02-08": 804,
    "2026-02-15": 714,
    "2026-02-22": 957,
    "2026-03-01": 932,
    "2026-03-08": 1028,
    "2026-03-15": 670,
}

# Registros do report (para contexto de conversao)
REPORT_REGISTROS = {
    "2026-02-01": 2024,
    "2026-02-08": 1895,
    "2026-02-15": 2287,
    "2026-02-22": 2555,
    "2026-03-01": 2406,
    "2026-03-08": 2485,
    "2026-03-15": 1604,
}


# =============================================================================
# QUERIES
# =============================================================================

# Query 1 - Depositos por domingo (fund_ec2 + bireports_ec2 para filtrar test users)
# Valores em centavos, dividir por 100
QUERY_DEPOSITOS = """
-- Validacao: Depositos por domingo
-- Fonte: fund_ec2 (centavos /100), join bireports_ec2 (excl. test users)
-- Timezone: UTC -> BRT obrigatorio
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
WHERE f.c_txn_type = 1
  AND f.c_txn_status = 'SUCCESS'
  AND DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') IN (
        DATE '2026-02-01', DATE '2026-02-08', DATE '2026-02-15', DATE '2026-02-22',
        DATE '2026-03-01', DATE '2026-03-08', DATE '2026-03-15'
  )
GROUP BY DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
ORDER BY data_brt
"""

# Query 2 - Saques por domingo (fund_ec2 + bireports_ec2)
QUERY_SAQUES = """
-- Validacao: Saques por domingo
-- Fonte: fund_ec2 (centavos /100), join bireports_ec2 (excl. test users)
SELECT
    DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_brt,
    COUNT(*) AS qtd_saques,
    ROUND(SUM(CAST(f.c_amount_in_ecr_ccy AS DOUBLE) / 100.0), 2) AS total_saques_brl
FROM fund_ec2.tbl_real_fund_txn f
INNER JOIN bireports_ec2.tbl_ecr e
    ON e.c_ecr_id = f.c_ecr_id
    AND e.c_test_user = false
WHERE f.c_txn_type = 2
  AND f.c_txn_status = 'SUCCESS'
  AND DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') IN (
        DATE '2026-02-01', DATE '2026-02-08', DATE '2026-02-15', DATE '2026-02-22',
        DATE '2026-03-01', DATE '2026-03-08', DATE '2026-03-15'
  )
GROUP BY DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
ORDER BY data_brt
"""

# Query 3 - GGR Casino por domingo (fund_ec2 — mesma fonte do extract original)
# NOTA: O extract original usou fund_ec2 com c_txn_type 27 (bet) e 45 (win), centavos /100
# A query do pedido usava ps_bi com coluna casino_ggr que NAO EXISTE
QUERY_GGR = """
-- Validacao: GGR Casino por domingo
-- Fonte: fund_ec2 (centavos /100) — MESMA fonte do extract_domingos_historico.py
-- GGR = Bets (tipo 27) - Wins (tipo 45)
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
    2) AS ggr_casino_brl
FROM fund_ec2.tbl_real_fund_txn f
INNER JOIN bireports_ec2.tbl_ecr e
    ON e.c_ecr_id = f.c_ecr_id
    AND e.c_test_user = false
WHERE f.c_txn_type IN (27, 45)
  AND f.c_txn_status = 'SUCCESS'
  AND DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') IN (
        DATE '2026-02-01', DATE '2026-02-08', DATE '2026-02-15', DATE '2026-02-22',
        DATE '2026-03-01', DATE '2026-03-08', DATE '2026-03-15'
  )
GROUP BY DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
ORDER BY data_brt
"""

# Query 3b - GGR Casino via ps_bi (cross-check com colunas corretas)
QUERY_GGR_PSBI = """
-- Cross-check: GGR Casino via ps_bi
-- Colunas corretas: casino_realbet_base, casino_real_win_base (base = local = BRL)
SELECT
    activity_date,
    ROUND(SUM(casino_realbet_base), 2) AS casino_bets_psbi,
    ROUND(SUM(casino_real_win_base), 2) AS casino_wins_psbi,
    ROUND(SUM(casino_realbet_base - casino_real_win_base), 2) AS ggr_casino_psbi
FROM ps_bi.fct_player_activity_daily
WHERE activity_date IN (
        DATE '2026-02-01', DATE '2026-02-08', DATE '2026-02-15', DATE '2026-02-22',
        DATE '2026-03-01', DATE '2026-03-08', DATE '2026-03-15'
  )
GROUP BY activity_date
ORDER BY activity_date
"""

# Query 4 - FTDs por domingo (ps_bi.dim_user — MESMA logica do extract original)
# NOTA: O extract original contou registros POR DATA DE REGISTRO (registration_date)
# e depois contou quantos desses tinham has_ftd = 1 (conversao registro->FTD)
# A query do pedido usava ftd_datetime que conta FTDs POR DATA DO DEPOSITO (diferente!)
QUERY_FTDS = """
-- Validacao: FTDs por domingo — MESMA logica do extract original
-- Registros por registration_date (DATE no ps_bi, UTC truncado)
-- FTDs = registros que converteram (has_ftd = 1), independente de quando fizeram o FTD
SELECT
    CAST(u.registration_date AS DATE) AS data_brt,
    COUNT(*) AS total_registros,
    COUNT_IF(u.has_ftd = 1) AS total_ftds,
    ROUND(
        CAST(COUNT_IF(u.has_ftd = 1) AS DOUBLE)
        / NULLIF(CAST(COUNT(*) AS DOUBLE), 0) * 100.0
    , 2) AS taxa_conversao_pct
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

# Query 4b - FTDs por data do deposito (ftd_datetime) — cross-check alternativo
QUERY_FTDS_BY_DEPOSIT_DATE = """
-- Cross-check: FTDs pela data do DEPOSITO (ftd_datetime convertido BRT)
-- Semantica diferente: conta quantos FTDs aconteceram no domingo
-- (inclui quem registrou antes mas depositou no domingo)
SELECT
    DATE(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ftd_date_brt,
    COUNT(*) AS ftds_by_deposit_date
FROM ps_bi.dim_user
WHERE DATE(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') IN (
        DATE '2026-02-01', DATE '2026-02-08', DATE '2026-02-15', DATE '2026-02-22',
        DATE '2026-03-01', DATE '2026-03-08', DATE '2026-03-15'
  )
  AND is_test = false
GROUP BY DATE(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
ORDER BY ftd_date_brt
"""


# =============================================================================
# FUNCOES AUXILIARES
# =============================================================================

def classificar_delta(report_val, athena_val):
    """
    Classifica a divergencia entre o valor do report e o valor do Athena.
    Retorna (delta_abs, delta_pct, classificacao).
    """
    if report_val is None or athena_val is None:
        return None, None, "SEM DADOS"

    delta_abs = athena_val - report_val
    if report_val == 0:
        if athena_val == 0:
            return 0, 0.0, "MATCH EXATO"
        else:
            return delta_abs, float("inf"), "DIVERGENCIA"

    delta_pct = (delta_abs / abs(report_val)) * 100.0

    if delta_abs == 0:
        classificacao = "MATCH EXATO"
    elif abs(delta_pct) < 1.0:
        classificacao = "DELTA ACEITAVEL"
    else:
        classificacao = "DIVERGENCIA"

    return round(delta_abs, 2), round(delta_pct, 4), classificacao


def print_header(titulo):
    """Imprime cabecalho formatado."""
    print("\n" + "=" * 80)
    print(f"  {titulo}")
    print("=" * 80)


# =============================================================================
# EXECUCAO
# =============================================================================

def main():
    log.info("Iniciando validacao dos 7 domingos historicos...")
    resultados = []  # lista de dicts para o CSV final

    # ------------------------------------------------------------------
    # 1. DEPOSITOS
    # ------------------------------------------------------------------
    print_header("VALIDACAO 1: DEPOSITOS POR DOMINGO")
    try:
        log.info("Executando Query 1 — Depositos (fund_ec2 + bireports_ec2)...")
        df_dep = query_athena(QUERY_DEPOSITOS, database="default")
        log.info(f"Query 1 retornou {len(df_dep)} linhas.")

        # Normalizar coluna de data para string yyyy-mm-dd
        df_dep["data_brt"] = pd.to_datetime(df_dep["data_brt"]).dt.strftime("%Y-%m-%d")

        print(f"\n{'Domingo':<14} {'Report BRL':>16} {'Athena BRL':>16} {'Delta':>12} {'Delta%':>8} {'Status':<18}")
        print("-" * 90)

        for d in DOMINGOS:
            report_val = REPORT_DEPOSITOS[d]["total_brl"]
            row = df_dep[df_dep["data_brt"] == d]
            athena_val = float(row["total_depositos_brl"].iloc[0]) if len(row) > 0 else None
            delta, pct, status = classificar_delta(report_val, athena_val)

            print(f"{d:<14} {report_val:>16,.2f} {athena_val:>16,.2f} {delta:>12,.2f} {pct:>7.2f}% {status:<18}")
            resultados.append({
                "metrica": "depositos_brl",
                "domingo": d,
                "report": report_val,
                "athena": athena_val,
                "delta_abs": delta,
                "delta_pct": pct,
                "status": status,
            })

        # Tambem validar qtd e unicos
        print(f"\n{'Domingo':<14} {'Rep Qtd':>10} {'Ath Qtd':>10} {'Rep Unicos':>12} {'Ath Unicos':>12} {'Rep Ticket':>12} {'Ath Ticket':>12}")
        print("-" * 90)
        for d in DOMINGOS:
            report_qtd = REPORT_DEPOSITOS[d]["qtd"]
            report_unicos = REPORT_DEPOSITOS[d]["unicos"]
            report_ticket = REPORT_DEPOSITOS[d]["ticket"]
            row = df_dep[df_dep["data_brt"] == d]
            if len(row) > 0:
                athena_qtd = int(row["qtd_depositos"].iloc[0])
                athena_unicos = int(row["depositantes_unicos"].iloc[0])
                athena_ticket = float(row["ticket_medio"].iloc[0])
            else:
                athena_qtd = athena_unicos = athena_ticket = None

            print(f"{d:<14} {report_qtd:>10} {athena_qtd:>10} {report_unicos:>12} {athena_unicos:>12} {report_ticket:>12.2f} {athena_ticket:>12.2f}")

            # Classificar qtd
            delta_q, pct_q, status_q = classificar_delta(report_qtd, athena_qtd)
            resultados.append({
                "metrica": "depositos_qtd",
                "domingo": d,
                "report": report_qtd,
                "athena": athena_qtd,
                "delta_abs": delta_q,
                "delta_pct": pct_q,
                "status": status_q,
            })

    except Exception as e:
        log.error(f"Falha na Query 1 (Depositos): {e}")
        print(f"\n*** ERRO ao executar query de depositos: {e} ***")

    # ------------------------------------------------------------------
    # 2. SAQUES
    # ------------------------------------------------------------------
    print_header("VALIDACAO 2: SAQUES POR DOMINGO")
    try:
        log.info("Executando Query 2 — Saques (fund_ec2 + bireports_ec2)...")
        df_saq = query_athena(QUERY_SAQUES, database="default")
        log.info(f"Query 2 retornou {len(df_saq)} linhas.")

        df_saq["data_brt"] = pd.to_datetime(df_saq["data_brt"]).dt.strftime("%Y-%m-%d")

        print(f"\n{'Domingo':<14} {'Report BRL':>16} {'Athena BRL':>16} {'Delta':>12} {'Delta%':>8} {'Status':<18}")
        print("-" * 90)

        for d in DOMINGOS:
            report_val = REPORT_SAQUES[d]
            row = df_saq[df_saq["data_brt"] == d]
            athena_val = float(row["total_saques_brl"].iloc[0]) if len(row) > 0 else None
            delta, pct, status = classificar_delta(report_val, athena_val)

            athena_str = f"{athena_val:>16,.2f}" if athena_val is not None else f"{'N/A':>16}"
            delta_str = f"{delta:>12,.2f}" if delta is not None else f"{'N/A':>12}"
            pct_str = f"{pct:>7.2f}%" if pct is not None else f"{'N/A':>8}"

            print(f"{d:<14} {report_val:>16,.2f} {athena_str} {delta_str} {pct_str} {status:<18}")
            resultados.append({
                "metrica": "saques_brl",
                "domingo": d,
                "report": report_val,
                "athena": athena_val,
                "delta_abs": delta,
                "delta_pct": pct,
                "status": status,
            })

    except Exception as e:
        log.error(f"Falha na Query 2 (Saques): {e}")
        print(f"\n*** ERRO ao executar query de saques: {e} ***")

    # ------------------------------------------------------------------
    # 3. GGR CASINO (fund_ec2 — mesma fonte do extract original)
    # ------------------------------------------------------------------
    print_header("VALIDACAO 3: GGR CASINO POR DOMINGO (fund_ec2)")
    try:
        log.info("Executando Query 3 — GGR Casino (fund_ec2, mesma fonte do extract)...")
        df_ggr = query_athena(QUERY_GGR, database="default")
        log.info(f"Query 3 retornou {len(df_ggr)} linhas.")

        df_ggr["data_brt"] = pd.to_datetime(df_ggr["data_brt"]).dt.strftime("%Y-%m-%d")

        print(f"\n{'Domingo':<14} {'Report BRL':>16} {'Athena BRL':>16} {'Delta':>12} {'Delta%':>8} {'Status':<18}")
        print("-" * 90)

        for d in DOMINGOS:
            report_val = REPORT_GGR[d]
            row = df_ggr[df_ggr["data_brt"] == d]
            athena_val = float(row["ggr_casino_brl"].iloc[0]) if len(row) > 0 else None
            delta, pct, status = classificar_delta(report_val, athena_val)

            athena_str = f"{athena_val:>16,.2f}" if athena_val is not None else f"{'N/A':>16}"
            delta_str = f"{delta:>12,.2f}" if delta is not None else f"{'N/A':>12}"
            pct_str = f"{pct:>7.2f}%" if pct is not None else f"{'N/A':>8}"

            print(f"{d:<14} {report_val:>16,.2f} {athena_str} {delta_str} {pct_str} {status:<18}")
            resultados.append({
                "metrica": "ggr_casino_brl",
                "domingo": d,
                "report": report_val,
                "athena": athena_val,
                "delta_abs": delta,
                "delta_pct": pct,
                "status": status,
            })

        # Mostrar bets e wins para contexto
        if len(df_ggr) > 0:
            print(f"\n  Contexto GGR fund_ec2:")
            print(f"  {'Domingo':<14} {'Casino Bets':>16} {'Casino Wins':>16} {'GGR (Bets-Wins)':>16}")
            print(f"  {'-'*70}")
            for _, row in df_ggr.iterrows():
                print(f"  {row['data_brt']:<14} {row['casino_bets']:>16,.2f} {row['casino_wins']:>16,.2f} {row['ggr_casino_brl']:>16,.2f}")

    except Exception as e:
        log.error(f"Falha na Query 3 (GGR fund_ec2): {e}")
        print(f"\n*** ERRO ao executar query de GGR fund_ec2: {e} ***")

    # ------------------------------------------------------------------
    # 3b. CROSS-CHECK GGR via ps_bi (colunas corretas)
    # ------------------------------------------------------------------
    print_header("CROSS-CHECK 3b: GGR CASINO via ps_bi")
    try:
        log.info("Executando Query 3b — GGR Casino cross-check (ps_bi)...")
        df_ggr_psbi = query_athena(QUERY_GGR_PSBI, database="ps_bi")
        log.info(f"Query 3b retornou {len(df_ggr_psbi)} linhas.")

        df_ggr_psbi["activity_date"] = pd.to_datetime(df_ggr_psbi["activity_date"]).dt.strftime("%Y-%m-%d")

        print(f"\n  {'Domingo':<14} {'fund_ec2 GGR':>16} {'ps_bi GGR':>16} {'Delta fund-psbi':>16}")
        print(f"  {'-'*70}")
        for d in DOMINGOS:
            fund_row = df_ggr[df_ggr["data_brt"] == d] if "df_ggr" in dir() and len(df_ggr) > 0 else pd.DataFrame()
            psbi_row = df_ggr_psbi[df_ggr_psbi["activity_date"] == d]
            fund_val = float(fund_row["ggr_casino_brl"].iloc[0]) if len(fund_row) > 0 else None
            psbi_val = float(psbi_row["ggr_casino_psbi"].iloc[0]) if len(psbi_row) > 0 else None
            delta_cross = round(fund_val - psbi_val, 2) if fund_val is not None and psbi_val is not None else None

            fund_str = f"{fund_val:>16,.2f}" if fund_val is not None else f"{'N/A':>16}"
            psbi_str = f"{psbi_val:>16,.2f}" if psbi_val is not None else f"{'N/A':>16}"
            delta_str = f"{delta_cross:>16,.2f}" if delta_cross is not None else f"{'N/A':>16}"
            print(f"  {d:<14} {fund_str} {psbi_str} {delta_str}")

    except Exception as e:
        log.error(f"Falha na Query 3b (GGR ps_bi cross-check): {e}")
        print(f"\n*** ERRO no cross-check GGR ps_bi: {e} ***")

    # ------------------------------------------------------------------
    # 4. FTDs (por registration_date + has_ftd — mesma logica do extract)
    # ------------------------------------------------------------------
    print_header("VALIDACAO 4: FTDs POR DOMINGO (registration_date + has_ftd)")
    print("  NOTA: FTDs = registros naquele domingo que CONVERTERAM (has_ftd=1)")
    print("  Semantica: 'quantas pessoas que se REGISTRARAM no domingo fizeram FTD?'")
    try:
        log.info("Executando Query 4 — FTDs por registration_date (ps_bi.dim_user)...")
        df_ftd = query_athena(QUERY_FTDS, database="ps_bi")
        log.info(f"Query 4 retornou {len(df_ftd)} linhas.")

        df_ftd["data_brt"] = pd.to_datetime(df_ftd["data_brt"]).dt.strftime("%Y-%m-%d")

        print(f"\n{'Domingo':<14} {'Rep FTDs':>10} {'Ath FTDs':>10} {'Delta':>8} {'Delta%':>8} {'Status':<18} {'Registros':>10} {'Conv%':>8}")
        print("-" * 100)

        for d in DOMINGOS:
            report_val = REPORT_FTDS[d]
            row = df_ftd[df_ftd["data_brt"] == d]
            if len(row) > 0:
                athena_val = int(row["total_ftds"].iloc[0])
                registros = int(row["total_registros"].iloc[0])
                conv_pct = float(row["taxa_conversao_pct"].iloc[0])
            else:
                athena_val = registros = None
                conv_pct = 0.0

            delta, pct, status = classificar_delta(report_val, athena_val)

            athena_str = f"{athena_val:>10}" if athena_val is not None else f"{'N/A':>10}"
            delta_str = f"{delta:>8}" if delta is not None else f"{'N/A':>8}"
            pct_str = f"{pct:>7.2f}%" if pct is not None else f"{'N/A':>8}"
            reg_str = f"{registros:>10}" if registros is not None else f"{'N/A':>10}"

            print(f"{d:<14} {report_val:>10} {athena_str} {delta_str} {pct_str} {status:<18} {reg_str} {conv_pct:>7.2f}%")
            resultados.append({
                "metrica": "ftds",
                "domingo": d,
                "report": report_val,
                "athena": athena_val,
                "delta_abs": delta,
                "delta_pct": pct,
                "status": status,
            })

    except Exception as e:
        log.error(f"Falha na Query 4 (FTDs): {e}")
        print(f"\n*** ERRO ao executar query de FTDs: {e} ***")

    # ------------------------------------------------------------------
    # 4b. CROSS-CHECK FTDs por data do deposito (ftd_datetime)
    # ------------------------------------------------------------------
    print_header("CROSS-CHECK 4b: FTDs POR DATA DO DEPOSITO (ftd_datetime)")
    print("  NOTA: Semantica diferente — 'quantos FTDs ACONTECERAM no domingo?'")
    print("  Inclui quem registrou antes mas fez primeiro deposito no domingo.")
    try:
        log.info("Executando Query 4b — FTDs por ftd_datetime...")
        df_ftd_dep = query_athena(QUERY_FTDS_BY_DEPOSIT_DATE, database="ps_bi")
        log.info(f"Query 4b retornou {len(df_ftd_dep)} linhas.")

        df_ftd_dep["ftd_date_brt"] = pd.to_datetime(df_ftd_dep["ftd_date_brt"]).dt.strftime("%Y-%m-%d")

        print(f"\n  {'Domingo':<14} {'FTDs reg_date':>14} {'FTDs ftd_date':>14} {'Delta':>10}")
        print(f"  {'-'*60}")
        for d in DOMINGOS:
            reg_row = df_ftd[df_ftd["data_brt"] == d] if "df_ftd" in dir() and len(df_ftd) > 0 else pd.DataFrame()
            dep_row = df_ftd_dep[df_ftd_dep["ftd_date_brt"] == d]
            reg_val = int(reg_row["total_ftds"].iloc[0]) if len(reg_row) > 0 else None
            dep_val = int(dep_row["ftds_by_deposit_date"].iloc[0]) if len(dep_row) > 0 else None
            delta_cross = (dep_val - reg_val) if reg_val is not None and dep_val is not None else None

            reg_str = f"{reg_val:>14}" if reg_val is not None else f"{'N/A':>14}"
            dep_str = f"{dep_val:>14}" if dep_val is not None else f"{'N/A':>14}"
            delta_str = f"{delta_cross:>10}" if delta_cross is not None else f"{'N/A':>10}"
            print(f"  {d:<14} {reg_str} {dep_str} {delta_str}")

    except Exception as e:
        log.error(f"Falha na Query 4b (FTDs cross-check): {e}")
        print(f"\n*** ERRO no cross-check FTDs: {e} ***")

    # ------------------------------------------------------------------
    # RESUMO FINAL
    # ------------------------------------------------------------------
    print_header("RESUMO FINAL DA VALIDACAO")

    if resultados:
        df_res = pd.DataFrame(resultados)

        # Contagem por status
        status_counts = df_res["status"].value_counts()
        total = len(df_res)
        print(f"\nTotal de comparacoes: {total}")
        for s, cnt in status_counts.items():
            pct = cnt / total * 100
            print(f"  {s}: {cnt} ({pct:.1f}%)")

        # Divergencias detalhadas
        divergencias = df_res[df_res["status"] == "DIVERGENCIA"]
        if len(divergencias) > 0:
            print(f"\n*** ATENCAO: {len(divergencias)} DIVERGENCIA(S) ENCONTRADA(S) ***")
            print(divergencias[["metrica", "domingo", "report", "athena", "delta_abs", "delta_pct"]].to_string(index=False))
        else:
            print("\nNenhuma divergencia encontrada. Dados do report VALIDADOS.")

        # Salvar CSV
        csv_path = os.path.join(OUTPUT_DIR, "validacao_domingos_historicos.csv")
        df_res.to_csv(csv_path, index=False, encoding="utf-8-sig")
        log.info(f"Resultados salvos em: {csv_path}")
        print(f"\nCSV salvo: {csv_path}")
    else:
        print("\nNenhum resultado para reportar (todas as queries falharam?).")

    log.info("Validacao concluida.")


if __name__ == "__main__":
    main()
