"""
Confirma se bireports_ec2.tbl_ecr_wise_daily_bi_summary cobre TODOS os campos
que o PCR precisa pra calcular o rating (nao so recencia).

Campos do PCR atual (player_metrics CTE):
  ggr_total, ngr_total
  casino_bet, casino_win, casino_rounds
  sport_bet, sport_win, sport_bets
  num_deposits, total_deposits
  num_cashouts, total_cashouts
  days_active, last/first_active_date
  bonus_issued, bonus_turned_real
  turnover_total
"""
import sys
sys.path.insert(0, ".")

from db.athena import query_athena

print("[1] Schema completo bireports_ec2.tbl_ecr_wise_daily_bi_summary")
df = query_athena("SHOW COLUMNS FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary")
cols = df.iloc[:, 0].tolist()
print(f"Total: {len(cols)} colunas")
print()
for c in cols:
    if any(k in c for k in ["bet_amount", "win_amount", "deposit", "co_", "bonus",
                             "count", "qty", "session", "ngr", "casino", "sb_",
                             "sport", "turnover", "rounds"]):
        print(f"  {c}")

print("\n[2] Mapeamento PCR <-> bireports")
mapeamento = [
    ("ggr_total",          "(c_casino_realcash_bet - c_casino_realcash_win) + (c_sb_realcash_bet - c_sb_realcash_win)"),
    ("ngr_total",          "ggr_total - bonus_turned_real (precisa calcular)"),
    ("casino_bet",         "c_casino_realcash_bet_amount"),
    ("casino_win",         "c_casino_realcash_win_amount"),
    ("casino_rounds",      "??? procurar count"),
    ("sport_bet",          "c_sb_realcash_bet_amount"),
    ("sport_win",          "c_sb_realcash_win_amount"),
    ("sport_bets",         "??? procurar count"),
    ("num_deposits",       "??? procurar count"),
    ("total_deposits",     "c_deposit_success_amount"),
    ("num_cashouts",       "??? procurar count"),
    ("total_cashouts",     "c_co_success_amount"),
    ("days_active",        "COUNT(DISTINCT c_created_date)"),
    ("last_active_date",   "MAX(c_created_date)"),
    ("first_active_date",  "MIN(c_created_date)"),
    ("bonus_issued",       "c_bonus_issued_amount"),
    ("bonus_turned_real",  "??? procurar"),
    ("turnover_total",     "casino_bet + sport_bet"),
]
for col_pcr, fonte in mapeamento:
    print(f"  {col_pcr:<22} <- {fonte}")

print("\n[3] Counts disponiveis (filtros do PCR HAVING)")
for c in cols:
    if "count" in c or "qty" in c or "_no_" in c or "session" in c:
        print(f"  {c}")
