"""
Validar "retencao 47K" da matriz_financeiro vs Athena — 20/04/2026 MultiBet
============================================================================
Autor: Mateus Fabro | Squad: Intelligence Engine | Data: 2026-04-20

DESCOBERTA CRITICA:
  matriz_financeiro.retencao = bonus_cassino + bonus_sportbook
                             = casino_bonus_bet_amount_inhouse
                             + sportsbook_bonus_bet
                             = BONUS WAGERED (apostado com dinheiro de bonus)
  ISSO NAO E BTR!
  BTR real hoje na Athena (type=20) = R$ 26.416 (nao 47K).
  O "47K" que o Head viu e o valor total APOSTADO usando saldo de bonus.

Objetivo deste script:
  1. Confirmar valor retencao hoje (47.109 ja confirmado)
  2. Validar com Athena: SUM(bonus_bet_amount) casino + sportsbook hoje
  3. Identificar TOP jogadores que apostaram bonus hoje
  4. Cruzar com BTR (quem converteu bonus em real)
  5. Cruzar com bonus emitido (qual template de bonus)
  6. Deep dive em anomalias

Schema confirmado:
  bireports_ec2.tbl_ecr_wise_daily_bi_summary (109 cols)
    - c_created_date (DATE, NAO c_activity_date)
    - c_casino_bet_amount (total = real + bonus)
    - c_casino_realcash_bet_amount (apenas real)
    - c_sb_bet_amount / c_sb_realcash_bet_amount
  -> bonus_bet = total - realcash

Uso:
  python scripts/validar_bonus_matriz_vs_athena_20abr.py
"""

import sys
import os
import traceback
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.athena import query_athena
from db.supernova import get_supernova_connection

pd.set_option("display.max_columns", 40)
pd.set_option("display.width", 260)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

HOJE_BRT = "2026-04-20"
HOJE_UTC_START = "2026-04-20 03:00:00"
HOJE_UTC_END   = "2026-04-21 03:00:00"
D14_BRT_START = "2026-04-06"

SEP = "=" * 100


def run_athena(desc, sql, database="fund_ec2"):
    print(f"\n{SEP}\n  {desc}\n{SEP}")
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
        return pd.DataFrame()


def run_pg(desc, sql):
    print(f"\n{SEP}\n  {desc}\n{SEP}")
    tunnel, conn = get_supernova_connection()
    try:
        df = pd.read_sql(sql, conn)
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
    finally:
        conn.close()
        try:
            if tunnel: tunnel.stop()
        except Exception:
            pass


print(f"\n{'#' * 100}")
print(f"  VALIDAR 'RETENCAO 47K' DA MATRIZ_FINANCEIRO -- 20/04/2026 MULTIBET")
print(f"  retencao = bonus_cassino + bonus_sportbook = BONUS APOSTADO (nao BTR)")
print(f"  Executado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'#' * 100}")


# =======================================================================
# PASSO 1: matriz_financeiro -- 14d + hoje (colunas CORRETAS)
# =======================================================================
sql_matriz = f"""
SELECT
    data,
    retencao AS bonus_wagered_total,
    turnover_cassino,
    turnover_sports,
    ggr_cassino,
    ggr_sport,
    ggr_total,
    ngr,
    ativos,
    btr_ggr
FROM multibet.matriz_financeiro
WHERE data >= DATE '{D14_BRT_START}'
  AND data <= DATE '{HOJE_BRT}'
ORDER BY data DESC
"""
df_matriz = run_pg("PASSO 1: matriz_financeiro 14d + hoje (retencao = bonus_wagered)", sql_matriz)

if not df_matriz.empty:
    df_14d = df_matriz[df_matriz["data"].astype(str) < HOJE_BRT]
    media_14d = df_14d["bonus_wagered_total"].mean()
    max_14d   = df_14d["bonus_wagered_total"].max()
    hoje = df_matriz[df_matriz["data"].astype(str) == HOJE_BRT]["bonus_wagered_total"].values
    hoje = hoje[0] if len(hoje) else 0
    print(f"\n  -- SUMARIO --")
    print(f"  Retencao HOJE:       R$ {hoje:,.2f}")
    print(f"  Media 14d anteriores: R$ {media_14d:,.2f}")
    print(f"  Max 14d anteriores:   R$ {max_14d:,.2f}")
    print(f"  vs media:             {hoje/media_14d*100:.1f}%")


# =======================================================================
# PASSO 2: validar valores na Athena (bireports) -- bonus_bet hoje
# =======================================================================
sql_biresumo = f"""
SELECT
    c_created_date AS dia,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    -- Casino
    ROUND(SUM(c_casino_bet_amount - c_casino_realcash_bet_amount) / 100.0, 2) AS bonus_bet_casino,
    ROUND(SUM(c_casino_win_amount - c_casino_realcash_win_amount) / 100.0, 2) AS bonus_win_casino,
    -- Sportsbook
    ROUND(SUM(c_sb_bet_amount - c_sb_realcash_bet_amount) / 100.0, 2) AS bonus_bet_sport,
    ROUND(SUM(c_sb_win_amount - c_sb_realcash_win_amount) / 100.0, 2) AS bonus_win_sport,
    -- Soma
    ROUND(SUM(
          (c_casino_bet_amount - c_casino_realcash_bet_amount)
        + (c_sb_bet_amount     - c_sb_realcash_bet_amount)
    ) / 100.0, 2) AS bonus_bet_total_athena
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
WHERE c_created_date >= DATE '{D14_BRT_START}'
  AND c_created_date <= DATE '{HOJE_BRT}'
GROUP BY c_created_date
ORDER BY c_created_date DESC
"""
df_ath = run_athena("PASSO 2: Athena bireports -- bonus_bet diario (14d + hoje)", sql_biresumo, database="bireports_ec2")


# =======================================================================
# PASSO 3: TOP 30 jogadores por bonus_bet HOJE (no Athena)
# =======================================================================
sql_top = f"""
SELECT
    c_ecr_id,
    ROUND((c_casino_bet_amount - c_casino_realcash_bet_amount) / 100.0, 2) AS bonus_bet_casino,
    ROUND((c_sb_bet_amount     - c_sb_realcash_bet_amount)     / 100.0, 2) AS bonus_bet_sport,
    ROUND((c_casino_bet_amount - c_casino_realcash_bet_amount
         + c_sb_bet_amount     - c_sb_realcash_bet_amount)     / 100.0, 2) AS bonus_bet_total,
    ROUND((c_casino_win_amount - c_casino_realcash_win_amount
         + c_sb_win_amount     - c_sb_realcash_win_amount)     / 100.0, 2) AS bonus_win_total,
    ROUND((c_casino_realcash_bet_amount + c_sb_realcash_bet_amount) / 100.0, 2) AS real_bet_total,
    ROUND(c_deposit_success_amount / 100.0, 2) AS dep_brl,
    ROUND(c_co_success_amount / 100.0, 2) AS saque_brl
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
WHERE c_created_date = DATE '{HOJE_BRT}'
  AND (c_casino_bet_amount - c_casino_realcash_bet_amount
     + c_sb_bet_amount     - c_sb_realcash_bet_amount) > 0
ORDER BY bonus_bet_total DESC
LIMIT 30
"""
df_top = run_athena("PASSO 3: TOP 30 jogadores por bonus_bet HOJE", sql_top, database="bireports_ec2")


# =======================================================================
# PASSO 4: enriquecer perfil dim_user
# =======================================================================
df_merge = pd.DataFrame()
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
    df_perfil = run_athena("PASSO 4: perfil top 30 (ps_bi.dim_user)", sql_perfil, database="ps_bi")
    if not df_perfil.empty:
        df_perfil["ecr_id"] = df_perfil["ecr_id"].astype(int)
        df_merge = df_top.merge(df_perfil, left_on="c_ecr_id", right_on="ecr_id", how="left")
        print(f"\n{SEP}\n  PASSO 4b: TOP 30 CONSOLIDADO\n{SEP}")
        cols = ["external_id","bonus_bet_total","bonus_win_total","bonus_bet_casino","bonus_bet_sport",
                "real_bet_total","dep_brl","saque_brl","signup_dia","is_test","utm_campaign","affiliate_id"]
        print(df_merge[cols].to_string(index=False))

        total_top10 = df_merge.head(10)["bonus_bet_total"].sum()
        total_top30 = df_merge["bonus_bet_total"].sum()
        print(f"\n  Top 10 = R$ {total_top10:,.2f}  |  Top 30 = R$ {total_top30:,.2f}")

        # test users?
        tu = df_merge[df_merge["is_test"] == True]
        if not tu.empty:
            print(f"\n  ATENCAO: {len(tu)} test users no top 30 (INFLAM matriz_financeiro!)")
            print(tu[["external_id","bonus_bet_total","is_test"]].to_string(index=False))
        else:
            print(f"\n  OK: nenhum test user no top 30")

        # signup hoje
        sh = df_merge[df_merge["signup_dia"].astype(str) == HOJE_BRT]
        if not sh.empty:
            print(f"\n  {len(sh)} cadastraram HOJE (sinal de farming)")
            print(sh[["external_id","bonus_bet_total","utm_campaign"]].to_string(index=False))


# =======================================================================
# PASSO 5: BTR dos top 30 hoje (quem converteu bonus em real?)
# =======================================================================
if not df_top.empty:
    ids = ",".join(str(int(x)) for x in df_top["c_ecr_id"].tolist())
    sql_btr = f"""
    SELECT
        c_ecr_id,
        COUNT(*) AS qtd_btr,
        ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) AS btr_total_brl,
        ROUND(MAX(c_amount_in_ecr_ccy / 100.0), 2) AS btr_max,
        MIN(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro_btr_brt
    FROM fund_ec2.tbl_realcash_sub_fund_txn
    WHERE c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
      AND c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
      AND c_txn_type = 20
      AND c_op_type = 'CR'
      AND c_amount_in_ecr_ccy > 0
      AND c_ecr_id IN ({ids})
    GROUP BY 1
    ORDER BY btr_total_brl DESC
    """
    df_btr = run_athena("PASSO 5: BTR hoje dos top 30 (bonus -> real cash)", sql_btr)


# =======================================================================
# PASSO 6: bonus emitido hoje pelos top 30 (qual template)
# =======================================================================
if not df_top.empty:
    ids = ",".join(str(int(x)) for x in df_top["c_ecr_id"].head(15).tolist())
    sql_bonus_emit = f"""
    SELECT
        c_ecr_id,
        c_bonus_template_id,
        ROUND(SUM(c_actual_issued_amount) / 100.0, 2) AS emitido_brl,
        COUNT(*) AS qtd,
        MIN(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro,
        MAX(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo
    FROM bonus_ec2.tbl_bonus_summary_details
    WHERE c_ecr_id IN ({ids})
      AND c_start_time >= TIMESTAMP '2026-04-01 03:00:00'
      AND c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
      AND c_actual_issued_amount > 0
    GROUP BY 1, 2
    ORDER BY c_ecr_id, emitido_brl DESC
    """
    df_bonus = run_athena("PASSO 6: bonus emitido (abril) pelos top 15", sql_bonus_emit, database="bonus_ec2")


print(f"\n{SEP}")
print("  ## CONCLUSAO ##")
print(f"{SEP}")
print(f"""
  1. O '47K' reportado = matriz_financeiro.retencao
  2. retencao = bonus_cassino + bonus_sportbook = BONUS WAGERED (apostado com bonus)
     NAO e BTR (bonus convertido em real). BTR real hoje = R$ 26.416.
  3. Validacao Athena pode divergir por 1-3% (timezone, cut-off) - normal.
  4. Ver PASSO 4b pra perfil dos top 30 que mais apostaram bonus.
  5. Ver PASSO 5 pra quem desses converteu bonus em real.
  6. Ver PASSO 6 pra qual template de bonus foi emitido (origem do dinheiro).
""")
