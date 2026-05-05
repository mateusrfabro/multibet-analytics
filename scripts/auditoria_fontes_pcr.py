"""
Auditoria de fontes alternativas pro PCR.

Objetivo: descobrir qual e a melhor fonte pra substituir
ps_bi.fct_player_activity_daily (congelada em 06/04/2026).

Candidatos:
  A) bireports_ec2.tbl_ecr_wise_daily_bi_summary (raw, ja conhecida)
  B) multibet.fct_player_performance_by_period (silver com GGR/turnover/dep)
  C) Outras views silver/gold que possam servir

Fluxo:
  1) Lista views/silvers em multibet.* no Super Nova DB
  2) Lista freshness das fontes (Athena + Super Nova)
  3) Batimento fato vs candidata pra TOP 50 players num periodo onde fato estava OK
     (jan-mar 2026 = janela 90d antes do gap)
  4) Compara: ggr, turnover, depositos, casino_rounds, sport_bets

Output: reports/auditoria_fontes_pcr_YYYY-MM-DD.csv
"""
import sys
sys.path.insert(0, ".")

from datetime import datetime
from pathlib import Path
import pandas as pd
from db.athena import query_athena
from db.supernova import execute_supernova

SNAPSHOT = datetime.now().strftime("%Y-%m-%d")

print("=" * 70)
print("AUDITORIA DE FONTES PCR — alternativas a fct_player_activity_daily")
print("=" * 70)

# ----------------------------------------------------------------------
# 1) Listar candidatos no Super Nova DB
# ----------------------------------------------------------------------
print("\n[1] Tabelas/views em multibet.* no Super Nova DB")
rows = execute_supernova(
    """
    SELECT table_type, table_name
    FROM information_schema.tables
    WHERE table_schema = 'multibet'
      AND (table_name ILIKE '%player%'
           OR table_name ILIKE '%pcr%'
           OR table_name ILIKE '%performance%'
           OR table_name ILIKE '%activity%'
           OR table_name ILIKE '%ggr%'
           OR table_name ILIKE '%active%')
    ORDER BY table_type, table_name
    """,
    fetch=True,
)
for r in rows:
    print(f"  {r[0]:<10} multibet.{r[1]}")

# ----------------------------------------------------------------------
# 2) Freshness das candidatas
# ----------------------------------------------------------------------
print("\n[2] Freshness das candidatas")
sql_freshness = """
SELECT 'ps_bi.fct_player_activity_daily (atual)' AS fonte,
       CAST(MAX(activity_date) AS VARCHAR) AS ultima_data
FROM ps_bi.fct_player_activity_daily
UNION ALL
SELECT 'bireports_ec2.tbl_ecr_wise_daily_bi_summary',
       CAST(MAX(c_created_date) AS VARCHAR)
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
"""
df = query_athena(sql_freshness)
print(df.to_string(index=False))

# Super Nova DB: candidatos que aparecem no listing
candidatos_sn = [r[1] for r in rows if "performance" in r[1].lower() or "active" in r[1].lower()]
for c in candidatos_sn:
    try:
        rs = execute_supernova(
            f'SELECT MAX(refreshed_at) FROM multibet."{c}" LIMIT 1',
            fetch=True,
        )
        print(f"  multibet.{c}.refreshed_at -> {rs[0][0] if rs else 'vazio'}")
    except Exception as e:
        print(f"  multibet.{c}: erro -> {type(e).__name__}: {str(e)[:80]}")

# ----------------------------------------------------------------------
# 3) Schema da fct_player_performance_by_period (se existir)
# ----------------------------------------------------------------------
print("\n[3] Schema multibet.fct_player_performance_by_period")
try:
    rs = execute_supernova(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'multibet'
          AND table_name = 'fct_player_performance_by_period'
        ORDER BY ordinal_position
        """,
        fetch=True,
    )
    for col, dtype in rs:
        print(f"  {col:<25} {dtype}")
except Exception as e:
    print(f"  erro: {e}")

# Sample dos periodos disponiveis
try:
    rs = execute_supernova(
        """
        SELECT period, period_label, vertical, COUNT(*) AS n_players
        FROM multibet.fct_player_performance_by_period
        GROUP BY period, period_label, vertical
        ORDER BY period, vertical
        """,
        fetch=True,
    )
    print("\n  Periodos x verticais disponiveis:")
    for r in rs:
        print(f"    {r[0]:<12} {r[1]:<15} {r[2]:<10} {r[3]:>10,} players")
except Exception as e:
    print(f"  sample erro: {e}")

# ----------------------------------------------------------------------
# 4) Batimento fato vs bireports — periodo onde fato estava OK
# ----------------------------------------------------------------------
print("\n[4] Batimento fato vs bireports (jan-mar 2026, fato OK ate 06/04)")
print("    Top 50 players por GGR no periodo")

sql_bat = """
WITH fato AS (
    SELECT player_id,
           COUNT(DISTINCT activity_date) AS days_active_fct,
           SUM(ggr_base) AS ggr_fct,
           SUM(casino_realbet_base) AS casino_bet_fct,
           SUM(sb_realbet_base) AS sb_bet_fct,
           SUM(deposit_success_base) AS dep_fct,
           SUM(casino_realbet_count) AS casino_rounds_fct,
           SUM(sb_realbet_count) AS sb_bets_fct
    FROM ps_bi.fct_player_activity_daily
    WHERE activity_date >= DATE '2026-01-06'
      AND activity_date <  DATE '2026-04-06'
    GROUP BY player_id
),
bireports AS (
    SELECT c_ecr_id AS player_id,
           COUNT(DISTINCT c_created_date) AS days_active_bir,
           SUM((c_casino_realcash_bet_amount - c_casino_realcash_win_amount)/100.0
             + (c_sb_realcash_bet_amount     - c_sb_realcash_win_amount    )/100.0
           ) AS ggr_bir,
           SUM(c_casino_realcash_bet_amount/100.0) AS casino_bet_bir,
           SUM(c_sb_realcash_bet_amount/100.0) AS sb_bet_bir,
           SUM(c_deposit_success_amount/100.0) AS dep_bir
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
    WHERE c_created_date >= DATE '2026-01-06'
      AND c_created_date <  DATE '2026-04-06'
    GROUP BY c_ecr_id
),
top_fato AS (
    SELECT player_id FROM fato ORDER BY ggr_fct DESC LIMIT 50
)
SELECT
    f.player_id,
    f.days_active_fct, b.days_active_bir,
    ROUND(f.ggr_fct, 2) AS ggr_fato,
    ROUND(b.ggr_bir, 2) AS ggr_bireports,
    ROUND((b.ggr_bir - f.ggr_fct), 2) AS ggr_delta,
    ROUND(CASE WHEN f.ggr_fct <> 0
               THEN (b.ggr_bir - f.ggr_fct) / f.ggr_fct * 100
               ELSE NULL END, 2) AS ggr_pct,
    ROUND(f.dep_fct, 2) AS dep_fato,
    ROUND(b.dep_bir, 2) AS dep_bireports,
    ROUND(CASE WHEN f.dep_fct <> 0
               THEN (b.dep_bir - f.dep_fct) / f.dep_fct * 100
               ELSE NULL END, 2) AS dep_pct
FROM fato f
JOIN bireports b ON f.player_id = b.player_id
WHERE f.player_id IN (SELECT player_id FROM top_fato)
ORDER BY f.ggr_fct DESC
"""
df_bat = query_athena(sql_bat)
print(f"\n  Linhas: {len(df_bat)}")
if not df_bat.empty:
    print(df_bat.head(15).to_string(index=False))

    print("\n[5] Sumario do batimento")
    print(f"  GGR fato (sum):       R$ {df_bat['ggr_fato'].sum():,.2f}")
    print(f"  GGR bireports (sum):  R$ {df_bat['ggr_bireports'].sum():,.2f}")
    delta_global = (df_bat['ggr_bireports'].sum() - df_bat['ggr_fato'].sum()) / df_bat['ggr_fato'].sum() * 100
    print(f"  Delta global GGR:     {delta_global:+.2f}%")
    print(f"  Delta medio (abs):    {df_bat['ggr_pct'].abs().mean():.2f}%")
    print(f"  Delta mediano:        {df_bat['ggr_pct'].median():+.2f}%")
    print(f"  Players c/ delta>5%:  {(df_bat['ggr_pct'].abs()>5).sum()}/{len(df_bat)}")

    print(f"\n  DEPOSITO fato (sum):  R$ {df_bat['dep_fato'].sum():,.2f}")
    print(f"  DEPOSITO bireports:   R$ {df_bat['dep_bireports'].sum():,.2f}")
    delta_dep = (df_bat['dep_bireports'].sum() - df_bat['dep_fato'].sum()) / df_bat['dep_fato'].sum() * 100
    print(f"  Delta global DEP:     {delta_dep:+.2f}%")

    out = Path("reports") / f"auditoria_fontes_pcr_{SNAPSHOT}.csv"
    out.parent.mkdir(exist_ok=True)
    df_bat.to_csv(out, sep=";", decimal=",", index=False, encoding="utf-8-sig")
    print(f"\n  Evidencia: {out}")

print("\n" + "=" * 70)
print("FIM AUDITORIA")
print("=" * 70)
