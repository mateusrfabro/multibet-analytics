"""
Extrator: numeros do dashboard para 20/03/2026.
Executa 4 queries no Athena e imprime resultados reais.
"""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\MultiBet")

from db.athena import query_athena
import traceback

AFFILIATES = "('297657', '445431', '468114')"
DATA = "2026-03-20"

# ---- Query 1: REG ----
q1 = f"""
SELECT COUNT(*) AS reg
FROM bireports_ec2.tbl_ecr
WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
  AND CAST(c_affiliate_id AS VARCHAR) IN {AFFILIATES}
  AND c_test_user = false
"""

# ---- Query 2: FTD ----
q2 = f"""
SELECT COUNT(*) AS ftd, COALESCE(SUM(ftd_amount_inhouse), 0) AS ftd_deposit
FROM ps_bi.dim_user
WHERE ftd_date = DATE '{DATA}'
  AND CAST(affiliate_id AS VARCHAR) IN {AFFILIATES}
  AND is_test = false
"""

# ---- Query 3: Financeiro ----
q3 = f"""
WITH base_players AS (
    SELECT DISTINCT ecr_id
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN {AFFILIATES}
      AND is_test = false
)
SELECT
    COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0 AS dep_amount,
    COALESCE(SUM(s.c_co_success_amount), 0) / 100.0 AS saques,
    COALESCE(SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount), 0) / 100.0 AS ggr_cassino,
    COALESCE(SUM(s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount), 0) / 100.0 AS ggr_sport,
    COALESCE(SUM(s.c_bonus_issued_amount), 0) / 100.0 AS bonus_cost,
    COUNT(DISTINCT s.c_ecr_id) AS players_ativos
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
JOIN base_players p ON s.c_ecr_id = p.ecr_id
WHERE s.c_created_date = DATE '{DATA}'
"""

# ---- Query 4: Breakdown por affiliate ----
q4 = f"""
WITH reg_by_aff AS (
    SELECT CAST(c_affiliate_id AS VARCHAR) AS aff_id, COUNT(*) AS reg
    FROM bireports_ec2.tbl_ecr
    WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
      AND CAST(c_affiliate_id AS VARCHAR) IN {AFFILIATES}
      AND c_test_user = false
    GROUP BY CAST(c_affiliate_id AS VARCHAR)
),
ftd_by_aff AS (
    SELECT CAST(affiliate_id AS VARCHAR) AS aff_id, COUNT(*) AS ftd, COALESCE(SUM(ftd_amount_inhouse), 0) AS ftd_deposit
    FROM ps_bi.dim_user
    WHERE ftd_date = DATE '{DATA}'
      AND CAST(affiliate_id AS VARCHAR) IN {AFFILIATES}
      AND is_test = false
    GROUP BY CAST(affiliate_id AS VARCHAR)
)
SELECT
    COALESCE(r.aff_id, f.aff_id) AS affiliate_id,
    COALESCE(r.reg, 0) AS reg,
    COALESCE(f.ftd, 0) AS ftd,
    COALESCE(f.ftd_deposit, 0) AS ftd_deposit
FROM reg_by_aff r
FULL OUTER JOIN ftd_by_aff f ON r.aff_id = f.aff_id
ORDER BY affiliate_id
"""

def run_query(label, sql, database):
    print(f"\n{'='*50}")
    print(f"EXECUTANDO: {label}")
    print(f"Database: {database}")
    print(f"{'='*50}")
    try:
        df = query_athena(sql, database=database)
        print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()
        return None

if __name__ == "__main__":
    print("=" * 60)
    print(f"EXTRACAO DASHBOARD — {DATA}")
    print("=" * 60)

    df1 = run_query("Query 1 — REG", q1, "bireports_ec2")
    df2 = run_query("Query 2 — FTD", q2, "ps_bi")
    df3 = run_query("Query 3 — Financeiro", q3, "ps_bi")
    df4 = run_query("Query 4 — Breakdown por Affiliate", q4, "ps_bi")

    # Resumo formatado
    print("\n\n")
    print("=" * 60)
    print(f"RESULTADOS ATHENA — {DATA}")
    print("=" * 60)

    if df1 is not None:
        print(f"REG:            {df1['reg'].iloc[0]}")
    else:
        print("REG:            ERRO na query")

    if df2 is not None:
        print(f"FTD:            {df2['ftd'].iloc[0]}")
        print(f"FTD Deposit:    R$ {df2['ftd_deposit'].iloc[0]:.2f}")
    else:
        print("FTD:            ERRO na query")

    if df3 is not None:
        dep = df3['dep_amount'].iloc[0]
        saques = df3['saques'].iloc[0]
        ggr_c = df3['ggr_cassino'].iloc[0]
        ggr_s = df3['ggr_sport'].iloc[0]
        bonus = df3['bonus_cost'].iloc[0]
        players = df3['players_ativos'].iloc[0]
        ngr = ggr_c + ggr_s - bonus

        print(f"Dep Amount:     R$ {dep:,.2f}")
        print(f"Saques:         R$ {saques:,.2f}")
        print(f"GGR Cassino:    R$ {ggr_c:,.2f}")
        print(f"GGR Sport:      R$ {ggr_s:,.2f}")
        print(f"Bonus Cost:     R$ {bonus:,.2f}")
        print(f"NGR (calc):     R$ {ngr:,.2f}")
        print(f"Players ativos: {players}")
    else:
        print("Financeiro:     ERRO na query")

    if df4 is not None:
        print(f"\nBREAKDOWN POR AFFILIATE:")
        print(df4.to_string(index=False))
    else:
        print("\nBREAKDOWN:      ERRO na query")