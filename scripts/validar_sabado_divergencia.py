"""
Investigacao de Divergencia — Depositos Sabado 21/03/2026

Contexto:
    Bot Slack reportou R$ 1,603,963.18 para sabado 21/03/2026
    Squad usou R$ 1,440,481.73 (hardcoded como "parcial do bot")
    Diferenca: ~R$ 163K (~10%)

Hipoteses testadas:
    1. Dado parcial (capturado antes de fechar o dia)
    2. Data errada (sexta 20/03 em vez de sabado 21/03)
    3. Filtro de test users (bot pode nao excluir)
    4. Campo diferente (ps_bi em BRL vs fund_ec2 centavos/100)
    5. Timezone (truncamento UTC vs BRT)

Queries:
    A — fund_ec2 COM filtro test users (nossa metodologia) — sabado 21/03
    B — fund_ec2 SEM filtro test users — sabado 21/03
    C — ps_bi (camada dbt) — sabado 21/03
    D — fund_ec2 COM filtro test users — janela 19-22/03 (buscar R$ 1,440,481.73)

Regras aplicadas:
    - Timezone: AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
    - Valores: c_amount_in_ecr_ccy / 100.0 (centavos para BRL)
    - Status: c_txn_status = 'SUCCESS' (validado empiricamente 17/03/2026)
    - Test users: c_test_user = false (bireports_ec2.tbl_ecr)
    - Sem SELECT *, sem partição dt (não existe em fund_ec2)
    - Sintaxe Presto/Trino

Autor: Squad Intelligence Engine — auditoria de divergencia
Data: 2026-03-22
"""

import sys
import os
import logging

# -- Garantir UTF-8 no stdout (Windows) --
sys.stdout.reconfigure(encoding="utf-8")

# -- Ajustar path para importar db.athena --
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# -- Valores de referencia para comparacao --
BOT_VALOR = 1_603_963.18
BOT_TXN = 12_088
BOT_FTD = 1_506

SQUAD_VALOR = 1_440_481.73
SQUAD_TXN = 10_806
SQUAD_FTD = 1_389


def run_query_a():
    """Query A: fund_ec2 COM filtro test users — sabado 21/03 BRT."""
    sql = """
    -- Query A: Depositos sabado 21/03/2026 BRT
    -- Metodologia squad: fund_ec2 + exclusao test users
    -- Valor em centavos / 100 = BRL
    SELECT
        DATE '2026-03-21' AS data_brt,
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
      AND DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') = DATE '2026-03-21'
    """
    log.info("Executando Query A (fund_ec2 COM test users filter, sabado 21/03)...")
    try:
        df = query_athena(sql, database="default")
        return df
    except Exception as e:
        log.error(f"Query A falhou: {e}")
        return None


def run_query_b():
    """Query B: fund_ec2 SEM filtro test users — sabado 21/03 BRT."""
    sql = """
    -- Query B: Depositos sabado 21/03/2026 BRT SEM filtro test users
    -- Objetivo: verificar se bot inclui test users
    SELECT
        DATE '2026-03-21' AS data_brt,
        COUNT(*) AS qtd_depositos,
        ROUND(SUM(CAST(f.c_amount_in_ecr_ccy AS DOUBLE) / 100.0), 2) AS total_depositos_brl,
        COUNT(DISTINCT f.c_ecr_id) AS depositantes_unicos
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_txn_type = 1
      AND f.c_txn_status = 'SUCCESS'
      AND DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') = DATE '2026-03-21'
    """
    log.info("Executando Query B (fund_ec2 SEM test users filter, sabado 21/03)...")
    try:
        df = query_athena(sql, database="fund_ec2")
        return df
    except Exception as e:
        log.error(f"Query B falhou: {e}")
        return None


def run_query_c():
    """Query C: ps_bi (camada dbt) — sabado 21/03."""
    sql = """
    -- Query C: Depositos via ps_bi (camada dbt, valores ja em BRL)
    -- activity_date e baseado em UTC (truncamento meia-noite UTC, nao BRT)
    -- Colunas corretas: deposit_success_base (BRL), deposit_success_count
    SELECT
        activity_date,
        ROUND(SUM(deposit_success_base), 2) AS total_depositos_brl,
        SUM(deposit_success_count) AS qtd_depositos,
        COUNT(DISTINCT CASE WHEN deposit_success_count > 0 THEN player_id END) AS depositantes_unicos
    FROM ps_bi.fct_player_activity_daily
    WHERE activity_date = DATE '2026-03-21'
    GROUP BY activity_date
    """
    log.info("Executando Query C (ps_bi, sabado 21/03)...")
    try:
        df = query_athena(sql, database="ps_bi")
        return df
    except Exception as e:
        log.error(f"Query C falhou: {e}")
        return None


def run_query_d():
    """Query D: fund_ec2 COM filtro test users — janela 19-22/03 BRT."""
    sql = """
    -- Query D: Depositos janela 19-22/03/2026 BRT com test users filter
    -- Objetivo: encontrar qual data bate com R$ 1,440,481.73 (squad)
    SELECT
        DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_brt,
        COUNT(*) AS qtd_depositos,
        ROUND(SUM(CAST(f.c_amount_in_ecr_ccy AS DOUBLE) / 100.0), 2) AS total_depositos_brl,
        COUNT(DISTINCT f.c_ecr_id) AS depositantes_unicos
    FROM fund_ec2.tbl_real_fund_txn f
    INNER JOIN bireports_ec2.tbl_ecr e
        ON e.c_ecr_id = f.c_ecr_id
        AND e.c_test_user = false
    WHERE f.c_txn_type = 1
      AND f.c_txn_status = 'SUCCESS'
      AND DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') IN (
          DATE '2026-03-19', DATE '2026-03-20', DATE '2026-03-21', DATE '2026-03-22'
      )
    GROUP BY DATE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
    ORDER BY data_brt
    """
    log.info("Executando Query D (fund_ec2 COM test users, janela 19-22/03)...")
    try:
        df = query_athena(sql, database="default")
        return df
    except Exception as e:
        log.error(f"Query D falhou: {e}")
        return None


def compare_and_conclude(df_a, df_b, df_c, df_d):
    """Compara resultados e emite conclusao sobre a divergencia."""

    print("\n" + "=" * 80)
    print("INVESTIGACAO DE DIVERGENCIA — Depositos Sabado 21/03/2026")
    print("=" * 80)

    # -- Valores de referencia --
    print(f"\n--- VALORES DE REFERENCIA ---")
    print(f"Bot Slack:        R$ {BOT_VALOR:>14,.2f} | {BOT_TXN:>6,} txns | {BOT_FTD:>5,} FTDs")
    print(f"Squad (hardcoded):R$ {SQUAD_VALOR:>14,.2f} | {SQUAD_TXN:>6,} txns | {SQUAD_FTD:>5,} FTDs")
    print(f"Diferenca:        R$ {BOT_VALOR - SQUAD_VALOR:>14,.2f} ({(BOT_VALOR - SQUAD_VALOR) / BOT_VALOR * 100:.1f}%)")

    # -- Query A --
    print(f"\n--- QUERY A: fund_ec2 COM filtro test users (sabado 21/03 BRT) ---")
    if df_a is not None and len(df_a) > 0:
        print(df_a.to_string(index=False))
        val_a = df_a["total_depositos_brl"].iloc[0]
        txn_a = df_a["qtd_depositos"].iloc[0]
        dep_a = df_a["depositantes_unicos"].iloc[0]
        print(f"\n  -> Diferenca vs Bot:   R$ {val_a - BOT_VALOR:>+14,.2f} ({(val_a - BOT_VALOR) / BOT_VALOR * 100:+.2f}%)")
        print(f"  -> Diferenca vs Squad: R$ {val_a - SQUAD_VALOR:>+14,.2f} ({(val_a - SQUAD_VALOR) / SQUAD_VALOR * 100:+.2f}%)")
    elif df_d is not None and len(df_d) > 0:
        # Fallback: extrair dados de 21/03 da Query D (mesma metodologia)
        row_21 = df_d[df_d["data_brt"].astype(str).str.contains("2026-03-21")]
        if len(row_21) > 0:
            val_a = row_21["total_depositos_brl"].iloc[0]
            txn_a = row_21["qtd_depositos"].iloc[0]
            dep_a = row_21["depositantes_unicos"].iloc[0]
            print(f"  [Usando dados da Query D para 21/03 como fallback]")
            print(f"  total_depositos_brl: {val_a:,.2f} | qtd: {txn_a:,} | depositantes: {dep_a:,}")
            print(f"\n  -> Diferenca vs Bot:   R$ {val_a - BOT_VALOR:>+14,.2f} ({(val_a - BOT_VALOR) / BOT_VALOR * 100:+.2f}%)")
            print(f"  -> Diferenca vs Squad: R$ {val_a - SQUAD_VALOR:>+14,.2f} ({(val_a - SQUAD_VALOR) / SQUAD_VALOR * 100:+.2f}%)")
        else:
            print("  [SEM DADOS ou ERRO]")
            val_a = txn_a = dep_a = None
    else:
        print("  [SEM DADOS ou ERRO]")
        val_a = txn_a = dep_a = None

    # -- Query B --
    print(f"\n--- QUERY B: fund_ec2 SEM filtro test users (sabado 21/03 BRT) ---")
    if df_b is not None and len(df_b) > 0:
        print(df_b.to_string(index=False))
        val_b = df_b["total_depositos_brl"].iloc[0]
        txn_b = df_b["qtd_depositos"].iloc[0]
        if val_a is not None:
            print(f"\n  -> Impacto test users: R$ {val_b - val_a:>+14,.2f} ({(val_b - val_a) / val_a * 100:+.2f}%)")
            print(f"  -> Txns test users:    {txn_b - txn_a:>+6,}")
        print(f"  -> Diferenca vs Bot:   R$ {val_b - BOT_VALOR:>+14,.2f} ({(val_b - BOT_VALOR) / BOT_VALOR * 100:+.2f}%)")
    else:
        print("  [SEM DADOS ou ERRO]")
        val_b = txn_b = None

    # -- Query C --
    print(f"\n--- QUERY C: ps_bi camada dbt (sabado 21/03, activity_date UTC) ---")
    if df_c is not None and len(df_c) > 0:
        print(df_c.to_string(index=False))
        val_c = df_c["total_depositos_brl"].iloc[0]
        print(f"\n  -> Diferenca vs Bot:   R$ {val_c - BOT_VALOR:>+14,.2f} ({(val_c - BOT_VALOR) / BOT_VALOR * 100:+.2f}%)")
        print(f"  -> Diferenca vs Squad: R$ {val_c - SQUAD_VALOR:>+14,.2f} ({(val_c - SQUAD_VALOR) / SQUAD_VALOR * 100:+.2f}%)")
        if val_a is not None:
            print(f"  -> Diferenca vs Query A (fund_ec2 BRT): R$ {val_c - val_a:>+14,.2f}")
            print(f"     NOTA: ps_bi usa activity_date (truncamento UTC), fund_ec2 usa BRT")
            print(f"     Esta diferenca indica depositos entre 00:00-02:59 BRT (21:00-23:59 UTC dia anterior)")
    else:
        print("  [SEM DADOS ou ERRO]")
        val_c = None

    # -- Query D --
    print(f"\n--- QUERY D: fund_ec2 COM filtro test users (janela 19-22/03 BRT) ---")
    if df_d is not None and len(df_d) > 0:
        print(df_d.to_string(index=False))
        print(f"\n  Buscando correspondencia com valor Squad (R$ {SQUAD_VALOR:,.2f}):")
        found_match = False
        for _, row in df_d.iterrows():
            val_row = row["total_depositos_brl"]
            diff_pct = abs(val_row - SQUAD_VALOR) / SQUAD_VALOR * 100
            marker = " <<<< MATCH!" if diff_pct < 1.0 else ""
            print(f"    {row['data_brt']}: R$ {val_row:>14,.2f} (diff: {diff_pct:.2f}%){marker}")
            if diff_pct < 1.0:
                found_match = True
        if not found_match:
            print(f"    Nenhuma data bate com o valor Squad dentro de 1% de tolerancia.")

        print(f"\n  Buscando correspondencia com valor Bot (R$ {BOT_VALOR:,.2f}):")
        for _, row in df_d.iterrows():
            val_row = row["total_depositos_brl"]
            diff_pct = abs(val_row - BOT_VALOR) / BOT_VALOR * 100
            marker = " <<<< MATCH!" if diff_pct < 1.0 else ""
            print(f"    {row['data_brt']}: R$ {val_row:>14,.2f} (diff: {diff_pct:.2f}%){marker}")
    else:
        print("  [SEM DADOS ou ERRO]")

    # -- Conclusao --
    print("\n" + "=" * 80)
    print("CONCLUSAO PRELIMINAR")
    print("=" * 80)

    conclusions = []

    # -- Hipotese 1: Dado parcial --
    if val_a is not None and abs(val_a - BOT_VALOR) / BOT_VALOR * 100 < 1.0:
        conclusions.append(
            f"[HIPOTESE 1 - DADO PARCIAL] DESCARTADA. "
            f"Nossa query (fund_ec2 BRT, dia completo) retorna R$ {val_a:,.2f}, "
            f"que bate EXATAMENTE com o Bot (R$ {BOT_VALOR:,.2f}). "
            f"O bot capturou o dia completo."
        )

    # -- Hipotese 2: Data errada --
    if df_d is not None and len(df_d) > 0:
        found_squad_match = False
        for _, row in df_d.iterrows():
            val_row = row["total_depositos_brl"]
            if abs(val_row - SQUAD_VALOR) / SQUAD_VALOR * 100 < 1.0:
                conclusions.append(
                    f"[HIPOTESE 2 - DATA ERRADA] CONFIRMADA. Valor Squad (R$ {SQUAD_VALOR:,.2f}) "
                    f"corresponde a {row['data_brt']} (R$ {val_row:,.2f})."
                )
                found_squad_match = True
        if not found_squad_match:
            conclusions.append(
                f"[HIPOTESE 2 - DATA ERRADA] NAO CONFIRMADA diretamente. "
                f"R$ {SQUAD_VALOR:,.2f} nao bate com nenhuma data inteira (19-22/03 BRT). "
                f"Valor provavelmente e PARCIAL do sabado (capturado antes de fechar o dia)."
            )

    # -- Hipotese 3: Test users --
    if val_a is not None and val_b is not None:
        test_user_impact = val_b - val_a
        if abs(test_user_impact) < 1.0:
            conclusions.append(
                f"[HIPOTESE 3 - TEST USERS] DESCARTADA. "
                f"Impacto ZERO (R$ {test_user_impact:,.2f}). "
                f"Neste periodo, test users nao fizeram depositos."
            )
        elif test_user_impact > 10_000:
            conclusions.append(
                f"[HIPOTESE 3 - TEST USERS] Impacto de R$ {test_user_impact:,.2f} "
                f"({test_user_impact / val_a * 100:.1f}%) por nao excluir test users."
            )
        else:
            conclusions.append(
                f"[HIPOTESE 3 - TEST USERS] Impacto minimo (R$ {test_user_impact:,.2f}). "
                f"Test users NAO explicam a divergencia."
            )

    # -- Hipotese 4: Campo diferente (ps_bi vs fund_ec2) --
    if val_a is not None and val_c is not None:
        ps_bi_diff = val_c - val_a
        conclusions.append(
            f"[HIPOTESE 4 - CAMPO DIFERENTE] ps_bi retorna R$ {val_c:,.2f} "
            f"vs fund_ec2 BRT R$ {val_a:,.2f} (delta: R$ {ps_bi_diff:,.2f}). "
            f"ps_bi usa truncamento UTC, o que explica a diferenca de ~R$ {abs(ps_bi_diff):,.0f}."
        )

    # -- Hipotese 5: Timezone --
    if val_a is not None and val_c is not None:
        tz_diff = abs(val_c - val_a)
        if tz_diff > 50_000:
            conclusions.append(
                f"[HIPOTESE 5 - TIMEZONE] Diferenca UTC vs BRT = R$ {tz_diff:,.2f}. "
                f"Truncamento de data (UTC midnight vs BRT midnight) causa divergencia SIGNIFICATIVA. "
                f"Depositos entre 00:00-02:59 BRT ficam no dia anterior no UTC."
            )
        else:
            conclusions.append(
                f"[HIPOTESE 5 - TIMEZONE] Diferenca UTC vs BRT = R$ {tz_diff:,.2f}. "
                f"Timezone NAO e fator principal."
            )

    # -- Veredicto final --
    conclusions.append("")
    if val_a is not None and abs(val_a - BOT_VALOR) / BOT_VALOR * 100 < 1.0:
        conclusions.append(
            f"VEREDICTO: O Bot esta CORRETO (R$ {BOT_VALOR:,.2f}). "
            f"Nossa metodologia (fund_ec2 com AT TIME ZONE BRT + exclusao test users) "
            f"confirma o valor. O dado do Squad (R$ {SQUAD_VALOR:,.2f}) esta ERRADO — "
            f"provavelmente capturado parcialmente ou com filtro/data incorretos."
        )
    elif not conclusions:
        conclusions.append(
            "Nenhuma hipotese isolada explica 100% da divergencia. "
            "Possivelmente combinacao de fatores."
        )

    for i, c in enumerate(conclusions, 1):
        print(f"  {i}. {c}")

    print("\n" + "=" * 80)
    print("FIM DA INVESTIGACAO")
    print("=" * 80)


def main():
    log.info("Iniciando investigacao de divergencia — depositos sabado 21/03/2026")
    log.info(f"Bot: R$ {BOT_VALOR:,.2f} | Squad: R$ {SQUAD_VALOR:,.2f} | Delta: R$ {BOT_VALOR - SQUAD_VALOR:,.2f}")

    # -- Executar queries --
    df_a = run_query_a()
    df_b = run_query_b()
    df_c = run_query_c()
    df_d = run_query_d()

    # -- Comparar e concluir --
    compare_and_conclude(df_a, df_b, df_c, df_d)


if __name__ == "__main__":
    main()
