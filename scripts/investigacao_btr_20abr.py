"""
Investigacao BTR (Bonus Turn to Real) em 20/04/2026 (segunda-feira)
====================================================================
Autor: Mateus Fabro | Squad: Intelligence Engine | Data: 2026-04-20

Contexto: Head reportou que BTR ja atingiu 47K hoje. Validar se esta
correto, entender distribuicao e identificar possiveis abusers.

Fonte unica: fund_ec2.tbl_realcash_sub_fund_txn (valor real do BTR)
  - c_txn_type = 20 (ISSUE_BONUS)
  - c_op_type  = 'CR' (credito)
  - tbl_real_fund_txn.c_amount e SEMPRE 0 para type 20 (usar sub-fund)

Janela BRT -> UTC (20/04 BRT = 03:00 UTC de 20 a 03:00 UTC de 21):
  HOJE:  2026-04-20 03:00:00 UTC -> 2026-04-21 03:00:00 UTC
  14D:   2026-04-06 03:00:00 UTC -> 2026-04-21 03:00:00 UTC

D-0 AVISO: dia parcial. Comparar vs media 7d exclusivo (sem hoje).

Uso:
  python scripts/investigacao_btr_20abr.py
"""

import sys
import os
import traceback
from datetime import datetime

import pandas as pd

# -- path setup --
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.athena import query_athena

pd.set_option("display.max_columns", 30)
pd.set_option("display.width", 240)
pd.set_option("display.max_colwidth", 60)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

HOJE_UTC_START = "2026-04-20 03:00:00"
HOJE_UTC_END   = "2026-04-21 03:00:00"

D14_UTC_START  = "2026-04-06 03:00:00"
D14_UTC_END    = "2026-04-21 03:00:00"

SEP = "=" * 90
SUB = "-" * 90


def fmt_brl(v):
    if pd.isna(v):
        return "R$ 0,00"
    return f"R$ {v:,.2f}"


def run_query(desc, sql, database="fund_ec2"):
    print(f"\n{SEP}")
    print(f"  {desc}")
    print(f"{SEP}")
    try:
        df = query_athena(sql, database=database)
        if df.empty:
            print("  [VAZIO]")
        else:
            print(f"  {len(df)} linhas.\n")
            print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"  [ERRO] {e}")
        traceback.print_exc()
        return pd.DataFrame()


print(f"\n{'#' * 90}")
print(f"  INVESTIGACAO BTR -- 20/04/2026 (segunda-feira)")
print(f"  Head reportou: BTR ja em 47K hoje (D-0 parcial)")
print(f"  Executado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'#' * 90}")


# =======================================================================
# Q1: BTR DIARIO ULTIMOS 14 DIAS + HOJE (baseline comparavel)
# =======================================================================
sql_q1 = f"""
SELECT
    date(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
    COUNT(*) AS qtd_btr,
    ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) AS total_btr_brl,
    ROUND(AVG(c_amount_in_ecr_ccy / 100.0), 2) AS ticket_medio,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    ROUND(MAX(c_amount_in_ecr_ccy / 100.0), 2) AS maior_btr
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '{D14_UTC_START}'
  AND c_start_time <  TIMESTAMP '{D14_UTC_END}'
  AND c_txn_type = 20
  AND c_op_type  = 'CR'
  AND c_amount_in_ecr_ccy > 0
GROUP BY 1
ORDER BY 1
"""
df_q1 = run_query("Q1: BTR diario 14d + hoje", sql_q1)

if not df_q1.empty:
    print(f"\n{SUB}")
    print("  ANALISE COMPARATIVA Q1")
    print(f"{SUB}")
    hoje_row = df_q1[df_q1["dia"].astype(str) == "2026-04-20"]
    df_7d = df_q1[(df_q1["dia"].astype(str) >= "2026-04-13") &
                  (df_q1["dia"].astype(str) <  "2026-04-20")]
    media_7d = df_7d["total_btr_brl"].mean() if not df_7d.empty else 0
    media_14d = df_q1[df_q1["dia"].astype(str) < "2026-04-20"]["total_btr_brl"].mean()

    print(f"  Media diaria 7d (13-19/04):  {fmt_brl(media_7d)}")
    print(f"  Media diaria 14d (06-19/04): {fmt_brl(media_14d)}")

    if not hoje_row.empty:
        total_hoje = hoje_row["total_btr_brl"].values[0]
        qtd_hoje = int(hoje_row["qtd_btr"].values[0])
        jog_hoje = int(hoje_row["jogadores"].values[0])
        razao_7d = total_hoje / media_7d * 100 if media_7d > 0 else 0
        razao_14d = total_hoje / media_14d * 100 if media_14d > 0 else 0

        print(f"\n  HOJE (20/04) -- D-0 PARCIAL:")
        print(f"    Total BTR:       {fmt_brl(total_hoje)}")
        print(f"    Qtd transacoes:  {qtd_hoje}")
        print(f"    Jogadores:       {jog_hoje}")
        print(f"    Ticket medio:    {fmt_brl(total_hoje / qtd_hoje if qtd_hoje else 0)}")
        print(f"    vs media 7d:     {razao_7d:.1f}%")
        print(f"    vs media 14d:    {razao_14d:.1f}%")

        if razao_7d > 200:
            print("  >>> ALERTA: BTR hoje ACIMA de 200% da media (mesmo D-0 parcial)")
        elif razao_7d > 150:
            print("  >>> ATENCAO: BTR hoje acima de 150% da media")
        elif razao_7d > 120:
            print("  >>> Elevado mas dentro de variabilidade normal")
        else:
            print("  >>> Dentro do baseline")


# =======================================================================
# Q2: TOP 30 JOGADORES HOJE (concentracao)
# =======================================================================
sql_q2 = f"""
SELECT
    sf.c_ecr_id,
    COUNT(*) AS qtd_btr,
    ROUND(SUM(sf.c_amount_in_ecr_ccy / 100.0), 2) AS total_btr_brl,
    ROUND(MAX(sf.c_amount_in_ecr_ccy / 100.0), 2) AS maior_btr,
    ROUND(AVG(sf.c_amount_in_ecr_ccy / 100.0), 2) AS ticket_medio,
    MIN(sf.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro_brt,
    MAX(sf.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo_brt
FROM fund_ec2.tbl_realcash_sub_fund_txn sf
WHERE sf.c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
  AND sf.c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
  AND sf.c_txn_type = 20
  AND sf.c_op_type  = 'CR'
  AND sf.c_amount_in_ecr_ccy > 0
GROUP BY 1
ORDER BY total_btr_brl DESC
LIMIT 30
"""
df_q2 = run_query("Q2: Top 30 jogadores BTR hoje", sql_q2)


# =======================================================================
# Q3: ENRIQUECER TOP 30 COM dim_user (signup, affiliate, is_test)
# =======================================================================
if not df_q2.empty:
    ecr_ids = ",".join(str(int(x)) for x in df_q2["c_ecr_id"].tolist())
    sql_q3 = f"""
    SELECT
        du.ecr_id,
        du.external_id,
        du.is_test,
        date(du.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia_signup,
        du.affiliate_id,
        du.utm_campaign,
        du.utm_source,
        du.utm_medium
    FROM ps_bi.dim_user du
    WHERE du.ecr_id IN ({ecr_ids})
    """
    df_q3 = run_query("Q3: Perfil dos top 30 (dim_user)", sql_q3, database="ps_bi")

    if not df_q3.empty:
        df_q3["ecr_id"] = df_q3["ecr_id"].astype(int)
        df_m = df_q2.merge(df_q3, left_on="c_ecr_id", right_on="ecr_id", how="left")
        print(f"\n{SUB}")
        print("  Q3b: TOP 30 ENRIQUECIDO (BTR + perfil)")
        print(f"{SUB}")
        cols = ["external_id","total_btr_brl","qtd_btr","dia_signup",
                "is_test","utm_campaign","utm_source","affiliate_id","primeiro_brt"]
        print(df_m[cols].to_string(index=False))

        # concentracao
        total_top10 = df_m.head(10)["total_btr_brl"].sum()
        total_top30 = df_m["total_btr_brl"].sum()
        total_dia = df_q1[df_q1["dia"].astype(str) == "2026-04-20"]["total_btr_brl"].values
        total_dia = total_dia[0] if len(total_dia) else 0
        if total_dia > 0:
            print(f"\n  Top 10 representam: {total_top10/total_dia*100:.1f}% do BTR do dia")
            print(f"  Top 30 representam: {total_top30/total_dia*100:.1f}% do BTR do dia")

        # signup hoje vs antigos
        signup_hoje = df_m[df_m["dia_signup"].astype(str) == "2026-04-20"]
        if not signup_hoje.empty:
            print(f"\n  ALERTA: {len(signup_hoje)} dos top 30 cadastraram HOJE")
            print(signup_hoje[["external_id","total_btr_brl","utm_campaign"]].to_string(index=False))

        # test users no top 30
        test_flag = df_m[df_m["is_test"] == True]
        if not test_flag.empty:
            print(f"\n  {len(test_flag)} test users no top 30 (excluir da leitura)")


# =======================================================================
# Q4: BREAKDOWN POR UTM_CAMPAIGN (hoje)
# =======================================================================
sql_q4 = f"""
WITH btr_hoje AS (
    SELECT c_ecr_id, c_amount_in_ecr_ccy
    FROM fund_ec2.tbl_realcash_sub_fund_txn
    WHERE c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
      AND c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
      AND c_txn_type = 20
      AND c_op_type  = 'CR'
      AND c_amount_in_ecr_ccy > 0
)
SELECT
    COALESCE(du.utm_campaign, '(sem utm)') AS utm_campaign,
    COUNT(*) AS qtd_btr,
    COUNT(DISTINCT bh.c_ecr_id) AS jogadores,
    ROUND(SUM(bh.c_amount_in_ecr_ccy / 100.0), 2) AS total_btr_brl,
    ROUND(AVG(bh.c_amount_in_ecr_ccy / 100.0), 2) AS ticket_medio
FROM btr_hoje bh
LEFT JOIN ps_bi.dim_user du ON du.ecr_id = bh.c_ecr_id
WHERE COALESCE(du.is_test, false) = false
GROUP BY 1
ORDER BY total_btr_brl DESC
LIMIT 20
"""
df_q4 = run_query("Q4: BTR hoje por utm_campaign", sql_q4, database="fund_ec2")


# =======================================================================
# Q5: TIMING -- signup-to-BTR por jogador (sinal de farming)
# =======================================================================
sql_q5 = f"""
WITH btr_hoje AS (
    SELECT
        c_ecr_id,
        MIN(c_start_time) AS primeiro_btr_ts,
        SUM(c_amount_in_ecr_ccy / 100.0) AS total_brl
    FROM fund_ec2.tbl_realcash_sub_fund_txn
    WHERE c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
      AND c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
      AND c_txn_type = 20
      AND c_op_type  = 'CR'
      AND c_amount_in_ecr_ccy > 0
    GROUP BY 1
)
SELECT
    CASE
        WHEN date_diff('day', du.signup_datetime, bh.primeiro_btr_ts) <= 1 THEN '00. mesmo dia'
        WHEN date_diff('day', du.signup_datetime, bh.primeiro_btr_ts) <= 7 THEN '01. 2-7d'
        WHEN date_diff('day', du.signup_datetime, bh.primeiro_btr_ts) <= 30 THEN '02. 8-30d'
        WHEN date_diff('day', du.signup_datetime, bh.primeiro_btr_ts) <= 90 THEN '03. 31-90d'
        ELSE '04. +90d'
    END AS faixa_signup_to_btr,
    COUNT(*) AS qtd_jogadores,
    ROUND(SUM(bh.total_brl), 2) AS total_btr_brl
FROM btr_hoje bh
LEFT JOIN ps_bi.dim_user du ON du.ecr_id = bh.c_ecr_id
WHERE COALESCE(du.is_test, false) = false
GROUP BY 1
ORDER BY 1
"""
df_q5 = run_query("Q5: Signup-to-BTR (timing)", sql_q5, database="fund_ec2")


print(f"\n{SEP}")
print("  FIM DA INVESTIGACAO")
print(f"{SEP}")
