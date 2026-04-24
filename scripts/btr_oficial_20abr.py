"""
BTR oficial (metodologia do sync_all_aquisicao/btr.sql) -- 20/04/2026 MultiBet
================================================================================
Fonte oficial localizada: GL-Analytics-M-L/sync_all_aquisicao/btr.sql
Popula multibet.tab_btr que alimenta matriz_financeiro.retencao

Query oficial:
  SELECT DATE(t.c_start_time AT TIME ZONE BRT) AS data,
         SUM(COALESCE(r.c_amount_in_house_ccy, 0)) / 100 AS btr_amount_inhouse
  FROM fund_ec2.tbl_real_fund_txn t
  JOIN fund_ec2.tbl_realcash_sub_fund_txn r ON t.c_txn_id = r.c_fund_txn_id
  JOIN ecr_ec2.tbl_ecr_flags f ON t.c_ecr_id = f.c_ecr_id
  WHERE t.c_txn_type = 20
    AND f.c_test_user = FALSE

Diferenca vs minha query anterior:
  - c_amount_in_house_ccy (NAO c_amount_in_ecr_ccy)
  - type=20 na tbl_real_fund_txn (pai), JOIN sub-fund
  - nao filtra op_type nem amount>0
  - exclui test_users via ecr_ec2.tbl_ecr_flags
"""

import sys, os
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.athena import query_athena

pd.set_option("display.max_columns", 40)
pd.set_option("display.width", 260)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

HOJE_UTC_START = "2026-04-20 03:00:00"
HOJE_UTC_END   = "2026-04-21 03:00:00"
SEP = "=" * 100


def run(desc, sql, db="fund_ec2"):
    print(f"\n{SEP}\n  {desc}\n{SEP}")
    try:
        df = query_athena(sql, database=db)
        if df.empty:
            print("  [VAZIO]")
        else:
            print(f"  {len(df)} linhas.\n")
            print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"  [ERRO] {e}")
        return pd.DataFrame()


# 1) replicar a query oficial -- 14d
sql_oficial = """
SELECT
    DATE(CAST(t.c_start_time AT TIME ZONE 'America/Sao_Paulo' AS TIMESTAMP)) AS data,
    ROUND(SUM(COALESCE(r.c_amount_in_house_ccy, 0)) / 100.0, 2) AS btr_oficial_brl,
    COUNT(*) AS qtd_linhas,
    COUNT(DISTINCT t.c_ecr_id) AS jogadores
FROM fund_ec2.tbl_real_fund_txn t
JOIN fund_ec2.tbl_realcash_sub_fund_txn r ON t.c_txn_id = r.c_fund_txn_id
JOIN ecr_ec2.tbl_ecr_flags f ON t.c_ecr_id = f.c_ecr_id
WHERE t.c_txn_type = 20
  AND f.c_test_user = FALSE
  AND t.c_start_time >= TIMESTAMP '2026-04-06 03:00:00'
  AND t.c_start_time <  TIMESTAMP '2026-04-21 03:00:00'
GROUP BY 1
ORDER BY 1 DESC
"""
df = run("BTR oficial (replica btr.sql) -- 14d + hoje", sql_oficial)


# 2) TOP 30 jogadores por BTR oficial hoje
sql_top = f"""
SELECT
    t.c_ecr_id,
    ROUND(SUM(COALESCE(r.c_amount_in_house_ccy, 0)) / 100.0, 2) AS btr_total_brl,
    COUNT(*) AS qtd_linhas_sub,
    ROUND(MAX(COALESCE(r.c_amount_in_house_ccy, 0)) / 100.0, 2) AS btr_max_linha,
    MIN(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro,
    MAX(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo
FROM fund_ec2.tbl_real_fund_txn t
JOIN fund_ec2.tbl_realcash_sub_fund_txn r ON t.c_txn_id = r.c_fund_txn_id
JOIN ecr_ec2.tbl_ecr_flags f ON t.c_ecr_id = f.c_ecr_id
WHERE t.c_txn_type = 20
  AND f.c_test_user = FALSE
  AND t.c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
  AND t.c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
GROUP BY 1
ORDER BY btr_total_brl DESC
LIMIT 30
"""
df_top = run("TOP 30 jogadores pela metodologia OFICIAL hoje", sql_top)


# 3) enriquecer dim_user
if not df_top.empty:
    ids = ",".join(str(int(x)) for x in df_top["c_ecr_id"].tolist())
    sql_perfil = f"""
    SELECT
        ecr_id,
        external_id,
        is_test,
        date(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS signup_dia,
        affiliate_id,
        utm_campaign,
        utm_source
    FROM ps_bi.dim_user
    WHERE ecr_id IN ({ids})
    """
    df_p = run("Perfil top 30", sql_perfil, db="ps_bi")
    if not df_p.empty:
        df_p["ecr_id"] = df_p["ecr_id"].astype(int)
        m = df_top.merge(df_p, left_on="c_ecr_id", right_on="ecr_id", how="left")
        print(f"\n{SEP}\n  TOP 30 CONSOLIDADO (metodologia OFICIAL)\n{SEP}")
        cols = ["external_id","btr_total_brl","qtd_linhas_sub","btr_max_linha",
                "signup_dia","is_test","utm_campaign","affiliate_id","primeiro"]
        print(m[cols].to_string(index=False))
        total_hoje = df[df["data"].astype(str) == "2026-04-20"]["btr_oficial_brl"].values
        total_hoje = total_hoje[0] if len(total_hoje) else 0
        if total_hoje > 0:
            top10_v = m.head(10)["btr_total_brl"].sum()
            top30_v = m["btr_total_brl"].sum()
            print(f"\n  Total BTR oficial hoje: R$ {total_hoje:,.2f}")
            print(f"  Top 10 = R$ {top10_v:,.2f} ({top10_v/total_hoje*100:.1f}%)")
            print(f"  Top 30 = R$ {top30_v:,.2f} ({top30_v/total_hoje*100:.1f}%)")

        # sinais de abuse
        signup_recente = m[m["signup_dia"].astype(str) >= "2026-04-15"]
        if not signup_recente.empty:
            print(f"\n  TOP 30 que cadastraram nos ultimos 5 dias (sinal de farming):")
            print(signup_recente[["external_id","btr_total_brl","signup_dia","utm_campaign","affiliate_id"]].to_string(index=False))

        tu = m[m["is_test"] == True]
        if not tu.empty:
            print(f"\n  ALERTA: {len(tu)} test users no top 30 (matriz ja filtra ecr_ec2 mas ps_bi pode diferir)")
            print(tu[["external_id","btr_total_brl"]].to_string(index=False))


# 4) breakdown op_type hoje pra confirmar hipotese (CR+DR somados)
sql_op = f"""
SELECT
    r.c_op_type,
    COUNT(*) AS qtd,
    ROUND(SUM(COALESCE(r.c_amount_in_house_ccy, 0)) / 100.0, 2) AS soma_brl
FROM fund_ec2.tbl_real_fund_txn t
JOIN fund_ec2.tbl_realcash_sub_fund_txn r ON t.c_txn_id = r.c_fund_txn_id
JOIN ecr_ec2.tbl_ecr_flags f ON t.c_ecr_id = f.c_ecr_id
WHERE t.c_txn_type = 20
  AND f.c_test_user = FALSE
  AND t.c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
  AND t.c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
GROUP BY 1
ORDER BY 1
"""
df_op = run("Breakdown por c_op_type hoje (CR vs DR)", sql_op)

# 5) breakdown house_ccy vs ecr_ccy pra confirmar campo
sql_ccy = f"""
SELECT
    ROUND(SUM(COALESCE(r.c_amount_in_house_ccy, 0)) / 100.0, 2) AS soma_house_ccy,
    ROUND(SUM(COALESCE(r.c_amount_in_ecr_ccy, 0)) / 100.0, 2) AS soma_ecr_ccy,
    ROUND(SUM(COALESCE(CASE WHEN r.c_op_type='CR' THEN r.c_amount_in_house_ccy END, 0)) / 100.0, 2) AS house_CR_only,
    ROUND(SUM(COALESCE(CASE WHEN r.c_op_type='CR' THEN r.c_amount_in_ecr_ccy END, 0)) / 100.0, 2) AS ecr_CR_only
FROM fund_ec2.tbl_real_fund_txn t
JOIN fund_ec2.tbl_realcash_sub_fund_txn r ON t.c_txn_id = r.c_fund_txn_id
JOIN ecr_ec2.tbl_ecr_flags f ON t.c_ecr_id = f.c_ecr_id
WHERE t.c_txn_type = 20
  AND f.c_test_user = FALSE
  AND t.c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
  AND t.c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
"""
df_ccy = run("Comparativo campos house_ccy vs ecr_ccy", sql_ccy)

print(f"\n{SEP}\nFIM\n{SEP}")
