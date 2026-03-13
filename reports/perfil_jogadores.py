"""
Relatório de perfil de jogadores ativos — Redshift
Gera 3 CSVs (30, 60, 90 dias) com:
  1. Jogadores ativos (qtd)
  2. Ticket médio de depósito (BRL)
  3. Turnover médio por sessão (BRL)
  4. Perfil predominante (casual / médio / high roller)

Classificação por ticket médio de aposta do jogador:
  - Casual: < R$ 500
  - Médio: >= R$ 500 e < R$ 1.000
  - High Roller: >= R$ 1.000

Estratégia: 4 queries enxutas por range, toda agregação no Redshift.
"""

import sys
import os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db.redshift import query_redshift
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = os.path.dirname(__file__)


def get_jogadores_ativos(days: int) -> int:
    """Conta jogadores distintos que apostaram no período."""
    sql = f"""
    SELECT COUNT(DISTINCT c_ecr_id) AS jogadores_ativos
    FROM fund.tbl_real_fund_txn
    WHERE c_txn_type = 27
      AND c_txn_status = 'SUCCESS'
      AND c_start_time >= DATEADD(day, -{days}, GETDATE())
    """
    df = query_redshift(sql)
    return int(df.iloc[0]["jogadores_ativos"])


def get_ticket_medio_deposito(days: int) -> float:
    """Ticket médio de depósito em BRL."""
    sql = f"""
    SELECT ROUND(AVG(c_amount) / 100.0, 2) AS ticket_medio_brl
    FROM fund.tbl_fund_deposit_txn
    WHERE c_start_time >= DATEADD(day, -{days}, GETDATE())
    """
    df = query_redshift(sql)
    return float(df.iloc[0]["ticket_medio_brl"])


def get_turnover_medio_sessao(days: int) -> float:
    """Turnover médio por sessão em BRL."""
    sql = f"""
    SELECT ROUND(AVG(turnover_brl), 2) AS turnover_medio_sessao
    FROM (
        SELECT c_session_id,
               SUM(c_amount_in_ecr_ccy) / 100.0 AS turnover_brl
        FROM fund.tbl_real_fund_txn
        WHERE c_txn_type = 27
          AND c_txn_status = 'SUCCESS'
          AND c_start_time >= DATEADD(day, -{days}, GETDATE())
          AND c_session_id IS NOT NULL
        GROUP BY c_session_id
    )
    """
    df = query_redshift(sql)
    return float(df.iloc[0]["turnover_medio_sessao"])


def get_perfil_distribuicao(days: int) -> dict:
    """
    Classifica jogadores por ticket médio de aposta e retorna distribuição.
    Toda a agregação é feita no Redshift.
    """
    sql = f"""
    SELECT
        perfil,
        COUNT(*) AS qtd,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM (
        SELECT
            c_ecr_id,
            CASE
                WHEN SUM(c_amount_in_ecr_ccy) / 100.0 / COUNT(*) < 500   THEN 'Casual'
                WHEN SUM(c_amount_in_ecr_ccy) / 100.0 / COUNT(*) < 1000  THEN 'Medio'
                ELSE 'High Roller'
            END AS perfil
        FROM fund.tbl_real_fund_txn
        WHERE c_txn_type = 27
          AND c_txn_status = 'SUCCESS'
          AND c_start_time >= DATEADD(day, -{days}, GETDATE())
        GROUP BY c_ecr_id
    )
    GROUP BY perfil
    ORDER BY qtd DESC
    """
    df = query_redshift(sql)

    result = {"qtd_casual": 0, "qtd_medio": 0, "qtd_high_roller": 0,
              "pct_casual": 0.0, "pct_medio": 0.0, "pct_high_roller": 0.0,
              "perfil_predominante": "N/A"}

    for _, row in df.iterrows():
        p = row["perfil"]
        q = int(row["qtd"])
        pct = float(row["pct"])
        if p == "Casual":
            result["qtd_casual"] = q
            result["pct_casual"] = pct
        elif p == "Medio":
            result["qtd_medio"] = q
            result["pct_medio"] = pct
        elif p == "High Roller":
            result["qtd_high_roller"] = q
            result["pct_high_roller"] = pct

    # Predominante = primeiro da lista (já ordenada por qtd DESC)
    if len(df) > 0:
        result["perfil_predominante"] = df.iloc[0]["perfil"]

    return result


def run_report():
    """Executa relatório para 30, 60 e 90 dias e salva CSVs."""
    timestamp = datetime.now().strftime("%Y%m%d")

    for days in [30, 60, 90]:
        log.info(f"\n{'='*50}")
        log.info(f"  Processando range de {days} dias...")
        log.info(f"{'='*50}")

        try:
            log.info("  [1/4] Jogadores ativos...")
            ativos = get_jogadores_ativos(days)

            log.info("  [2/4] Ticket medio deposito...")
            ticket_dep = get_ticket_medio_deposito(days)

            log.info("  [3/4] Turnover medio por sessao...")
            turnover = get_turnover_medio_sessao(days)

            log.info("  [4/4] Distribuicao de perfil...")
            perfil = get_perfil_distribuicao(days)

        except Exception as e:
            log.error(f"  ERRO no range {days}d: {e}")
            continue

        # Monta resultado
        result = {
            "range_dias": days,
            "jogadores_ativos": ativos,
            "ticket_medio_deposito_brl": ticket_dep,
            "turnover_medio_sessao_brl": turnover,
            **perfil,
        }
        df_result = pd.DataFrame([result])

        log.info(
            f"\n  RESULTADO — Ultimos {days} dias\n"
            f"  Jogadores ativos:          {ativos:,}\n"
            f"  Ticket medio deposito:     R$ {ticket_dep:,.2f}\n"
            f"  Turnover medio/sessao:     R$ {turnover:,.2f}\n"
            f"  Perfil predominante:       {perfil['perfil_predominante']}\n"
            f"  -- Distribuicao --\n"
            f"    Casual:      {perfil['qtd_casual']:,}  ({perfil['pct_casual']}%)\n"
            f"    Medio:       {perfil['qtd_medio']:,}  ({perfil['pct_medio']}%)\n"
            f"    High Roller: {perfil['qtd_high_roller']:,}  ({perfil['pct_high_roller']}%)"
        )

        filename = f"perfil_jogadores_{days}d_{timestamp}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        df_result.to_csv(filepath, index=False, sep=";", decimal=",")
        log.info(f"  CSV salvo: {filepath}")

    log.info("\nRelatorio finalizado.")


if __name__ == "__main__":
    run_report()