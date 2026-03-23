"""
Analise de Hora Morta — gap horario de depositos (BRT)
=====================================================
Objetivo: Identificar em QUAL HORARIO (BRT) o volume de depositos cai,
para sugerir push notifications ou campanhas CRM nesse horario.

Fonte: fund_ec2.tbl_real_fund_txn (granularidade hora)
Periodo: 14 dias (2026-03-07 a 2026-03-20)
Regras:
  - c_txn_type = 1 (deposito), c_txn_status = 'SUCCESS'
  - Valores em centavos: /100.0
  - Hora em BRT: AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
  - Excluir test users via JOIN bireports_ec2.tbl_ecr
"""

import sys
import os
import logging

# Garante que a raiz do projeto esta no path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.athena import query_athena
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── 1. Query: depositos por hora (BRT), ultimos 14 dias ──────────────────
SQL = """
WITH depositos AS (
    SELECT
        -- Converte timestamp UTC -> BRT e extrai a hora
        HOUR(
            f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
        ) AS hora_brt,
        -- Converte timestamp UTC -> BRT e extrai a data
        DATE(
            f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
        ) AS data_brt,
        -- Valor em BRL (centavos -> reais)
        CAST(f.c_amount_in_ecr_ccy AS DOUBLE) / 100.0 AS valor_brl
    FROM fund_ec2.tbl_real_fund_txn f
    -- Exclui test users
    INNER JOIN bireports_ec2.tbl_ecr e
        ON e.c_ecr_id = f.c_ecr_id
        AND e.c_test_user = false
    WHERE f.c_txn_type = 1              -- Deposito
      AND f.c_txn_status = 'SUCCESS'    -- Confirmado
      -- Periodo: 14 dias atras ate ontem (UTC range cobrindo BRT)
      AND f.c_start_time >= TIMESTAMP '2026-03-07 03:00:00'   -- 07/03 00h BRT = 03h UTC
      AND f.c_start_time <  TIMESTAMP '2026-03-21 03:00:00'   -- 21/03 00h BRT = 03h UTC
),

-- Agregacao por hora e dia (para calcular medias diarias)
por_hora_dia AS (
    SELECT
        hora_brt,
        data_brt,
        COUNT(*) AS qtd_depositos,
        SUM(valor_brl) AS total_brl
    FROM depositos
    GROUP BY hora_brt, data_brt
),

-- Agregacao final por hora (media de 14 dias)
por_hora AS (
    SELECT
        hora_brt,
        -- Media diaria
        AVG(qtd_depositos) AS avg_qtd_depositos,
        AVG(total_brl) AS avg_valor_brl,
        -- Total absoluto (14 dias)
        SUM(qtd_depositos) AS total_qtd_14d,
        SUM(total_brl) AS total_valor_14d,
        -- Contagem de dias com dados nessa hora
        COUNT(DISTINCT data_brt) AS dias_com_dados
    FROM por_hora_dia
    GROUP BY hora_brt
)

SELECT
    hora_brt,
    ROUND(avg_qtd_depositos, 1) AS avg_depositos_dia,
    ROUND(avg_valor_brl, 2) AS avg_valor_brl_dia,
    total_qtd_14d,
    ROUND(total_valor_14d, 2) AS total_valor_14d,
    dias_com_dados,
    -- Percentual do total diario (baseado na soma de 14 dias)
    ROUND(
        total_valor_14d * 100.0 / SUM(total_valor_14d) OVER (),
        2
    ) AS pct_do_total
FROM por_hora
ORDER BY hora_brt
"""

def main():
    log.info("=" * 70)
    log.info("ANALISE HORA MORTA — Depositos por Hora (BRT)")
    log.info("Periodo: 2026-03-07 a 2026-03-20 (14 dias)")
    log.info("=" * 70)

    # ── 2. Executa query ──────────────────────────────────────────────────
    log.info("Executando query no Athena (fund_ec2)...")
    df = query_athena(SQL, database="fund_ec2")
    log.info(f"Resultado: {len(df)} linhas (esperado: 24 horas)")

    if df.empty:
        log.error("Query retornou vazio! Verificar filtros.")
        return

    # ── 3. Analise dos resultados ─────────────────────────────────────────
    print("\n" + "=" * 80)
    print("DEPOSITOS POR HORA (BRT) — Media diaria dos ultimos 14 dias")
    print("=" * 80)
    print(df.to_string(index=False))

    # TOP 3 pico (maior volume medio)
    top3_pico = df.nlargest(3, "avg_valor_brl_dia")
    print("\n" + "-" * 60)
    print("TOP 3 HORAS PICO (maior volume medio diario):")
    print("-" * 60)
    for _, row in top3_pico.iterrows():
        print(f"  {int(row['hora_brt']):02d}h BRT — "
              f"R$ {row['avg_valor_brl_dia']:,.2f}/dia | "
              f"{row['avg_depositos_dia']:.0f} depositos/dia | "
              f"{row['pct_do_total']:.1f}% do total")

    # TOP 3 hora morta (menor volume medio)
    top3_morta = df.nsmallest(3, "avg_valor_brl_dia")
    print("\n" + "-" * 60)
    print("TOP 3 HORAS MORTAS (menor volume medio diario):")
    print("-" * 60)
    for _, row in top3_morta.iterrows():
        print(f"  {int(row['hora_brt']):02d}h BRT — "
              f"R$ {row['avg_valor_brl_dia']:,.2f}/dia | "
              f"{row['avg_depositos_dia']:.0f} depositos/dia | "
              f"{row['pct_do_total']:.1f}% do total")

    # ── 4. Calculo de oportunidade ────────────────────────────────────────
    avg_pico = top3_pico["avg_valor_brl_dia"].mean()
    avg_morta = top3_morta["avg_valor_brl_dia"].mean()
    ratio = avg_morta / avg_pico if avg_pico > 0 else 0

    print("\n" + "-" * 60)
    print("ANALISE DE OPORTUNIDADE:")
    print("-" * 60)
    print(f"  Media hora PICO:  R$ {avg_pico:,.2f}/dia")
    print(f"  Media hora MORTA: R$ {avg_morta:,.2f}/dia")
    print(f"  Ratio morta/pico: {ratio:.1%}")

    # Cenario: se hora morta tivesse 50% do volume da hora pico
    target_50pct = avg_pico * 0.50
    incremento_por_hora = target_50pct - avg_morta
    incremento_3h = incremento_por_hora * 3  # 3 horas mortas

    print(f"\n  CENARIO — Se 3 horas mortas tivessem 50% do volume pico:")
    print(f"    Target por hora:    R$ {target_50pct:,.2f}")
    print(f"    Incremento/hora:    R$ {incremento_por_hora:,.2f}")
    print(f"    Incremento diario:  R$ {incremento_3h:,.2f}")
    print(f"    Incremento mensal:  R$ {incremento_3h * 30:,.2f}")

    # Quanto isso representaria ontem (R$ 1.916.794)
    deposito_ontem = 1_916_794.0
    print(f"\n  CONTEXTO — Depositos de ontem: R$ {deposito_ontem:,.2f}")
    print(f"    Meta R$ 2M: faltaram R$ {2_000_000 - deposito_ontem:,.2f}")
    print(f"    Incremento estimado: R$ {incremento_3h:,.2f}/dia")
    if incremento_3h > 0:
        cobertura = (incremento_3h / (2_000_000 - deposito_ontem)) * 100
        print(f"    Cobertura do gap:   {cobertura:.0f}%")

    # ── 5. Salva CSV ──────────────────────────────────────────────────────
    output_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "output",
        "deposit_hourly_analysis_2026-03-21.csv"
    )
    df.to_csv(output_path, index=False)
    log.info(f"CSV salvo em: {output_path}")

    print("\n" + "=" * 80)
    log.info("Analise concluida com sucesso!")


if __name__ == "__main__":
    main()
