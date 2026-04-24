"""
Validacao DBeaver-equivalent: tabela e views fact_sports_odds_performance
=========================================================================
Roda 7 queries de sanidade contra o Super Nova DB:
  1. Tabela existe e tem dados
  2. Indices criados
  3. Views existem
  4. Distribuicao por mes (sanity check)
  5. Sample top 5 dias com maior GGR
  6. Detectar buracos no periodo (dias sem dados)
  7. Verificar grain unico (PK funcionando)
"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import execute_supernova

print("=" * 80)
print("VALIDACAO multibet.fact_sports_odds_performance")
print("=" * 80)

# 1. Tabela existe e tem dados
print("\n[1] Estrutura da tabela:")
rows = execute_supernova("""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'multibet'
      AND table_name = 'fact_sports_odds_performance'
    ORDER BY ordinal_position
""", fetch=True)
for r in rows:
    print(f"  {r[0]:<20} {r[1]:<25} nullable={r[2]}")

# 2. Indices
print("\n[2] Indices criados:")
rows = execute_supernova("""
    SELECT indexname, indexdef
    FROM pg_indexes
    WHERE schemaname = 'multibet'
      AND tablename = 'fact_sports_odds_performance'
    ORDER BY indexname
""", fetch=True)
for r in rows:
    print(f"  {r[0]}")

# 3. Views
print("\n[3] Views criadas:")
rows = execute_supernova("""
    SELECT table_name FROM information_schema.views
    WHERE table_schema = 'multibet'
      AND table_name LIKE '%odds%'
    ORDER BY table_name
""", fetch=True)
for r in rows:
    print(f"  multibet.{r[0]}")

# 4. Distribuicao mensal (sanidade)
print("\n[4] Distribuicao mensal (sanity check):")
print(f"  {'Mes':<10} {'Bets':>10} {'Stake (R$)':>15} {'GGR (R$)':>15} {'Hold %':>8}")
rows = execute_supernova("""
    SELECT
        TO_CHAR(dt, 'YYYY-MM') AS mes,
        SUM(total_bets) AS bets,
        SUM(total_stake) AS stake,
        SUM(ggr) AS ggr,
        ROUND(SUM(ggr) * 100.0 / NULLIF(SUM(total_stake), 0), 2) AS hold
    FROM multibet.fact_sports_odds_performance
    GROUP BY 1 ORDER BY 1
""", fetch=True)
for r in rows:
    bets_str = f"{int(r[1]):,}".replace(",", ".")
    stake_str = f"{float(r[2]):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    ggr_str = f"{float(r[3]):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    print(f"  {r[0]:<10} {bets_str:>10} {stake_str:>15} {ggr_str:>15} {float(r[4]):>7}%")

# 5. Top 5 dias com maior GGR
print("\n[5] Top 5 dias com maior GGR:")
rows = execute_supernova("""
    SELECT
        dt,
        SUM(total_bets) AS bets,
        SUM(total_stake) AS stake,
        SUM(ggr) AS ggr,
        ROUND(SUM(ggr) * 100.0 / NULLIF(SUM(total_stake), 0), 2) AS hold
    FROM multibet.fact_sports_odds_performance
    GROUP BY dt
    ORDER BY ggr DESC LIMIT 5
""", fetch=True)
for r in rows:
    print(f"  {r[0]} | bets={int(r[1]):>6} | stake=R$ {float(r[2]):>12,.2f} | "
          f"GGR=R$ {float(r[3]):>10,.2f} | hold={float(r[4]):>5}%")

# 6. Buracos no periodo
print("\n[6] Detectar dias faltantes (buracos no periodo):")
rows = execute_supernova("""
    WITH expected_days AS (
        SELECT generate_series(
            (SELECT MIN(dt) FROM multibet.fact_sports_odds_performance),
            (SELECT MAX(dt) FROM multibet.fact_sports_odds_performance),
            '1 day'::interval
        )::date AS dt
    ),
    actual_days AS (
        SELECT DISTINCT dt FROM multibet.fact_sports_odds_performance
    )
    SELECT e.dt FROM expected_days e
    LEFT JOIN actual_days a ON e.dt = a.dt
    WHERE a.dt IS NULL
    ORDER BY e.dt
""", fetch=True)
if rows:
    print(f"  ATENCAO: {len(rows)} dias faltantes:")
    for r in rows:
        print(f"    {r[0]}")
else:
    print("  OK: nenhum dia faltante no periodo coberto")

# 7. Grain unico (PK funcionando)
print("\n[7] Validacao de grain unico (PK):")
rows = execute_supernova("""
    SELECT
        COUNT(*) AS total_linhas,
        COUNT(DISTINCT (dt, odds_range, bet_mode)) AS combinacoes_unicas
    FROM multibet.fact_sports_odds_performance
""", fetch=True)
for r in rows:
    if r[0] == r[1]:
        print(f"  OK: {r[0]} linhas = {r[1]} combinacoes unicas (PK funcionando)")
    else:
        print(f"  ERRO: {r[0]} linhas mas apenas {r[1]} combinacoes unicas (PK violada!)")

# 8. Sample da view summary
print("\n[8] Sample multibet.vw_odds_performance_summary:")
rows = execute_supernova("""
    SELECT odds_range, bet_mode, total_bets, hold_rate_pct, ggr, dias_cobertos
    FROM multibet.vw_odds_performance_summary
""", fetch=True)
for r in rows:
    print(f"  {r[0]:<14} {r[1]:<10} bets={int(r[2]):>7} hold={float(r[3]):>6}% "
          f"GGR=R$ {float(r[4]):>12,.2f} ({r[5]} dias)")

print("\n" + "=" * 80)
print("VALIDACAO COMPLETA")
print("=" * 80)