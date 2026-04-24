"""
Investigacao URGENTE: Depositos e GGR em 13/04/2026
=====================================================
Contexto: Saques altos no dia, precisamos entender o cenario de
depositos, balanco liquido (net) e GGR casino nos ultimos 7 dias.

Queries:
  1. Depositos ultimos 7 dias (dia a dia)
  2. Balanco liquido (depositos - saques) por dia
  3. GGR Casino por dia (sub_fund: bet=48, win=49)
  4. Depositos por hora hoje (padrao intraday)

Regras Athena aplicadas:
  - cashier_ec2: c_created_time, status='txn_confirmed_success', valor=c_confirmed_amount_in_ecr_ccy/100
  - cashier_ec2 cashout: status='co_success'
  - fund_ec2 sub_fund: c_start_time, status='SUCCESS', valor=c_amount_in_ecr_ccy/100
  - Timezone: AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
  - 13/04 BRT = 2026-04-13 03:00 UTC ate 2026-04-14 03:00 UTC
  - 7 dias = desde 2026-04-06 03:00 UTC
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from db.athena import query_athena

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 40)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

# ============================================================
# 1) DEPOSITOS - ULTIMOS 7 DIAS (dia a dia)
# ============================================================
print("=" * 80)
print("1) DEPOSITOS CONFIRMADOS - ULTIMOS 7 DIAS (BRT)")
print("=" * 80)

sql_depositos = """
-- Depositos confirmados por dia (BRT), ultimos 7 dias
-- Fonte: cashier_ec2.tbl_cashier_deposit
-- Filtro: txn_confirmed_success, c_created_time convertido para BRT
SELECT
    date(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
    COUNT(*) AS qtd_depositos,
    SUM(c_confirmed_amount_in_ecr_ccy / 100.0) AS total_depositos_brl,
    AVG(c_confirmed_amount_in_ecr_ccy / 100.0) AS ticket_medio,
    COUNT(DISTINCT c_ecr_id) AS depositantes
FROM cashier_ec2.tbl_cashier_deposit
WHERE c_created_time >= TIMESTAMP '2026-04-06 03:00:00'
  AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
  AND c_txn_status = 'txn_confirmed_success'
GROUP BY 1
ORDER BY 1
"""

try:
    df_dep = query_athena(sql_depositos, database="cashier_ec2")
    print(df_dep.to_string(index=False))
    print()
except Exception as e:
    print(f"ERRO na query 1: {e}\n")
    df_dep = None

# ============================================================
# 2) BALANCO LIQUIDO (NET) — Depositos vs Saques por dia
# ============================================================
print("=" * 80)
print("2) BALANCO LIQUIDO (DEPOSITOS - SAQUES) - ULTIMOS 7 DIAS (BRT)")
print("=" * 80)

sql_net = """
-- Balanco liquido: depositos vs saques por dia (BRT)
-- Fonte: cashier_ec2 (deposit + cashout)
-- Status deposit: txn_confirmed_success | Status cashout: co_success
-- CORRECAO: cashout usa c_amount_in_ecr_ccy (nao c_confirmed_amount_in_ecr_ccy)
WITH deps AS (
    SELECT
        date(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
        SUM(c_confirmed_amount_in_ecr_ccy / 100.0) AS total_dep,
        COUNT(*) AS qtd_dep,
        COUNT(DISTINCT c_ecr_id) AS depositantes
    FROM cashier_ec2.tbl_cashier_deposit
    WHERE c_created_time >= TIMESTAMP '2026-04-06 03:00:00'
      AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
      AND c_txn_status = 'txn_confirmed_success'
    GROUP BY 1
),
saques AS (
    SELECT
        date(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
        SUM(c_paid_amount_in_ecr_ccy / 100.0) AS total_saq,
        COUNT(*) AS qtd_saq,
        COUNT(DISTINCT c_ecr_id) AS sacadores
    FROM cashier_ec2.tbl_cashier_cashout
    WHERE c_created_time >= TIMESTAMP '2026-04-06 03:00:00'
      AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
      AND c_txn_status = 'co_success'
    GROUP BY 1
)
SELECT
    COALESCE(d.dia, s.dia) AS dia,
    COALESCE(d.qtd_dep, 0) AS qtd_depositos,
    COALESCE(d.total_dep, 0) AS depositos_brl,
    COALESCE(d.depositantes, 0) AS depositantes,
    COALESCE(s.qtd_saq, 0) AS qtd_saques,
    COALESCE(s.total_saq, 0) AS saques_brl,
    COALESCE(s.sacadores, 0) AS sacadores,
    COALESCE(d.total_dep, 0) - COALESCE(s.total_saq, 0) AS net_brl
FROM deps d
FULL OUTER JOIN saques s ON d.dia = s.dia
ORDER BY 1
"""

try:
    df_net = query_athena(sql_net, database="cashier_ec2")
    print(df_net.to_string(index=False))
    print()
    # Calcular totais do periodo
    if df_net is not None and len(df_net) > 0:
        total_dep = df_net["depositos_brl"].sum()
        total_saq = df_net["saques_brl"].sum()
        total_net = df_net["net_brl"].sum()
        print(f"  TOTAIS PERIODO:")
        print(f"    Depositos: R$ {total_dep:,.2f}")
        print(f"    Saques:    R$ {total_saq:,.2f}")
        print(f"    NET:       R$ {total_net:,.2f}")
        print(f"    Ratio Saq/Dep: {(total_saq/total_dep*100) if total_dep else 0:.1f}%")
        print()
except Exception as e:
    print(f"ERRO na query 2: {e}\n")
    df_net = None

# ============================================================
# 3) GGR CASINO - ULTIMOS 7 DIAS (sub_fund: bet=48, win=49)
# ============================================================
print("=" * 80)
print("3) GGR CASINO - ULTIMOS 7 DIAS (BRT) — fund_ec2.tbl_realcash_sub_fund_txn")
print("=" * 80)

sql_ggr = """
-- GGR Casino por dia (BRT) = bets(27) - wins(45) + rollbacks(72)
-- Fonte: fund_ec2.tbl_real_fund_txn (tabela principal, tem c_txn_status)
-- CORRECAO EMPIRICA: casino usa c_txn_type 27 (bet) e 45 (win), NAO 48/49
-- Rollback (72) = valor devolvido ao jogador em caso de erro
-- Valores: centavos / 100 | Filtro: c_product_id = 'CASINO'
SELECT
    date(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
    SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_bets,
    SUM(CASE WHEN c_txn_type = 45 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_wins,
    SUM(CASE WHEN c_txn_type = 72 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_rollbacks,
    -- GGR = Bets - Wins - Rollbacks
    SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END)
      - SUM(CASE WHEN c_txn_type = 45 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END)
      - SUM(CASE WHEN c_txn_type = 72 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS ggr_brl,
    -- Hold rate = GGR / Bets
    CASE
        WHEN SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy ELSE 0 END) > 0
        THEN (SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy ELSE 0 END)
              - SUM(CASE WHEN c_txn_type = 45 THEN c_amount_in_ecr_ccy ELSE 0 END)
              - SUM(CASE WHEN c_txn_type = 72 THEN c_amount_in_ecr_ccy ELSE 0 END))
             * 100.0 / SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy ELSE 0 END)
        ELSE 0
    END AS hold_rate_pct
FROM fund_ec2.tbl_real_fund_txn
WHERE c_start_time >= TIMESTAMP '2026-04-06 03:00:00'
  AND c_start_time < TIMESTAMP '2026-04-14 03:00:00'
  AND c_txn_status = 'SUCCESS'
  AND c_product_id = 'CASINO'
  AND c_txn_type IN (27, 45, 72)
GROUP BY 1
ORDER BY 1
"""

try:
    df_ggr = query_athena(sql_ggr, database="fund_ec2")
    print(df_ggr.to_string(index=False))
    print()
    if df_ggr is not None and len(df_ggr) > 0:
        total_bets = df_ggr["total_bets"].sum()
        total_wins = df_ggr["total_wins"].sum()
        total_rb = df_ggr["total_rollbacks"].sum()
        total_ggr = df_ggr["ggr_brl"].sum()
        print(f"  TOTAIS PERIODO:")
        print(f"    Bets:      R$ {total_bets:,.2f}")
        print(f"    Wins:      R$ {total_wins:,.2f}")
        print(f"    Rollbacks: R$ {total_rb:,.2f}")
        print(f"    GGR:       R$ {total_ggr:,.2f}")
        print(f"    Hold Rate: {(total_ggr/total_bets*100) if total_bets else 0:.1f}%")
        print()
except Exception as e:
    print(f"ERRO na query 3: {e}\n")
    df_ggr = None

# ============================================================
# 4) DEPOSITOS POR HORA HOJE (padrao intraday)
# ============================================================
print("=" * 80)
print("4) DEPOSITOS POR HORA — HOJE 13/04/2026 (BRT)")
print("=" * 80)

sql_hora = """
-- Depositos por hora hoje (BRT) — padrao intraday
-- Fonte: cashier_ec2.tbl_cashier_deposit
SELECT
    hour(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora_brt,
    COUNT(*) AS qtd,
    SUM(c_confirmed_amount_in_ecr_ccy / 100.0) AS total_brl,
    AVG(c_confirmed_amount_in_ecr_ccy / 100.0) AS ticket_medio,
    COUNT(DISTINCT c_ecr_id) AS depositantes
FROM cashier_ec2.tbl_cashier_deposit
WHERE c_created_time >= TIMESTAMP '2026-04-13 03:00:00'
  AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
  AND c_txn_status = 'txn_confirmed_success'
GROUP BY 1
ORDER BY 1
"""

try:
    df_hora = query_athena(sql_hora, database="cashier_ec2")
    print(df_hora.to_string(index=False))
    print()
    if df_hora is not None and len(df_hora) > 0:
        total_hoje = df_hora["total_brl"].sum()
        total_qtd = df_hora["qtd"].sum()
        print(f"  TOTAL HOJE (ate agora):")
        print(f"    Qtd depositos: {total_qtd:,.0f}")
        print(f"    Total:         R$ {total_hoje:,.2f}")
        print(f"    Ticket medio:  R$ {total_hoje/total_qtd:,.2f}" if total_qtd else "")
        print()
except Exception as e:
    print(f"ERRO na query 4: {e}\n")
    df_hora = None

# ============================================================
# RESUMO EXECUTIVO
# ============================================================
print("=" * 80)
print("RESUMO EXECUTIVO — 13/04/2026")
print("=" * 80)

if df_net is not None and len(df_net) > 0:
    # Hoje (13/04)
    hoje = df_net[df_net["dia"].astype(str) == "2026-04-13"]
    if len(hoje) > 0:
        dep_hoje = hoje["depositos_brl"].values[0]
        saq_hoje = hoje["saques_brl"].values[0]
        net_hoje = hoje["net_brl"].values[0]
        print(f"\n  HOJE (13/04):")
        print(f"    Depositos: R$ {dep_hoje:,.2f}")
        print(f"    Saques:    R$ {saq_hoje:,.2f}")
        print(f"    NET:       R$ {net_hoje:,.2f}")
        if dep_hoje > 0:
            print(f"    Ratio Saq/Dep: {saq_hoje/dep_hoje*100:.1f}%")

    # Media 7 dias (excluindo hoje para comparacao)
    sem_hoje = df_net[df_net["dia"].astype(str) != "2026-04-13"]
    if len(sem_hoje) > 0:
        media_dep = sem_hoje["depositos_brl"].mean()
        media_saq = sem_hoje["saques_brl"].mean()
        media_net = sem_hoje["net_brl"].mean()
        print(f"\n  MEDIA ULTIMOS DIAS (excl. hoje):")
        print(f"    Depositos: R$ {media_dep:,.2f}")
        print(f"    Saques:    R$ {media_saq:,.2f}")
        print(f"    NET:       R$ {media_net:,.2f}")

if df_ggr is not None and len(df_ggr) > 0:
    ggr_hoje = df_ggr[df_ggr["dia"].astype(str) == "2026-04-13"]
    if len(ggr_hoje) > 0:
        print(f"\n  GGR CASINO HOJE:")
        print(f"    Bets:      R$ {ggr_hoje['total_bets'].values[0]:,.2f}")
        print(f"    Wins:      R$ {ggr_hoje['total_wins'].values[0]:,.2f}")
        print(f"    Rollbacks: R$ {ggr_hoje['total_rollbacks'].values[0]:,.2f}")
        print(f"    GGR:       R$ {ggr_hoje['ggr_brl'].values[0]:,.2f}")
        print(f"    Hold Rate: {ggr_hoje['hold_rate_pct'].values[0]:.1f}%")

print("\n" + "=" * 80)
print("FIM DA INVESTIGACAO")
print("=" * 80)
