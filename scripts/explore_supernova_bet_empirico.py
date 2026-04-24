"""
Investigacao empirica do supernova_bet — mapear valores reais de cada campo critico.
Objetivo: nunca mais errar por assumir valores sem validar.
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

def run():
    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()
    out = []

    def log(msg=""):
        print(msg)
        out.append(msg)

    def section(title):
        log(f"\n{'='*70}")
        log(f"  {title}")
        log(f"{'='*70}")

    def distinct_vals(table, col, extra=""):
        q = f"SELECT {col}, COUNT(*) FROM {table} {extra} GROUP BY 1 ORDER BY 2 DESC"
        cur.execute(q)
        return cur.fetchall()

    log("INVESTIGACAO EMPIRICA — SUPERNOVA_BET")
    log("Objetivo: mapear valores reais de cada campo critico")

    # ============================================
    section("1. TRANSACTIONS — tipos e status")
    for r in distinct_vals("transactions", "type"):
        log(f"  type: {r[0]:<25} {r[1]:>6} txns")
    log("")
    for r in distinct_vals("transactions", "type || ' > ' || status"):
        log(f"  {r[0]:<40} {r[1]:>6}")

    section("2. TRANSACTIONS — campos de valor")
    cur.execute("""
        SELECT type, status,
            ROUND(MIN(amount)::numeric, 2) AS min_amt,
            ROUND(MAX(amount)::numeric, 2) AS max_amt,
            ROUND(AVG(amount)::numeric, 2) AS avg_amt,
            COUNT(*) FILTER (WHERE real_amount > 0) AS has_real_amount,
            COUNT(*) FILTER (WHERE fee_amount > 0) AS has_fee,
            COUNT(*) FILTER (WHERE locked_amount > 0) AS has_locked
        FROM transactions
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    log(f"  {'type':<20} {'status':<12} {'min':>10} {'max':>10} {'avg':>10} {'real_amt':>9} {'fee':>5} {'locked':>7}")
    log(f"  {'-'*90}")
    for r in cur.fetchall():
        log(f"  {r[0]:<20} {r[1]:<12} {r[2]:>10} {r[3]:>10} {r[4]:>10} {r[5]:>9} {r[6]:>5} {r[7]:>7}")

    section("3. BETS — categories e status")
    for r in distinct_vals("bets", "category || ' / ' || status"):
        log(f"  {r[0]:<25} {r[1]:>8} rows")

    cur.execute("""
        SELECT category, status,
            ROUND(MIN(amount)::numeric, 2), ROUND(MAX(amount)::numeric, 2),
            ROUND(MIN(win_amount)::numeric, 2), ROUND(MAX(win_amount)::numeric, 2),
            ROUND(MIN(bonus_amount)::numeric, 2), ROUND(MAX(bonus_amount)::numeric, 2)
        FROM bets GROUP BY 1, 2 ORDER BY 1, 2
    """)
    log(f"\n  {'cat/status':<20} {'amt_min':>8} {'amt_max':>8} {'win_min':>8} {'win_max':>10} {'bonus_min':>10} {'bonus_max':>10}")
    for r in cur.fetchall():
        log(f"  {r[0]+'/'+r[1]:<20} {r[2]:>8} {r[3]:>8} {r[4]:>8} {r[5]:>10} {r[6]:>10} {r[7]:>10}")

    section("4. BETS — como calcular GGR corretamente")
    cur.execute("""
        SELECT
            SUM(amount) FILTER (WHERE category = 'LOSS') AS loss_amt,
            SUM(amount) FILTER (WHERE category = 'WIN') AS win_amt_col,
            SUM(win_amount) FILTER (WHERE category = 'WIN') AS win_winamt_col,
            SUM(amount) FILTER (WHERE category = 'WAGER') AS wager_amt,
            SUM(win_amount) FILTER (WHERE category = 'WAGER') AS wager_winamt,
            -- Tentativas de GGR
            SUM(CASE WHEN category = 'LOSS' THEN amount ELSE 0 END)
            - SUM(CASE WHEN category = 'WIN' THEN win_amount ELSE 0 END) AS ggr_v1,
            SUM(amount) - SUM(win_amount) AS ggr_v2
        FROM bets
    """)
    r = cur.fetchone()
    log(f"  LOSS.amount total:      Rs {r[0]:>12,.2f}")
    log(f"  WIN.amount total:       Rs {r[1]:>12,.2f}")
    log(f"  WIN.win_amount total:   Rs {r[2]:>12,.2f}")
    log(f"  WAGER.amount total:     Rs {r[3] or 0:>12,.2f}")
    log(f"  WAGER.win_amount total: Rs {r[4] or 0:>12,.2f}")
    log(f"  GGR v1 (LOSS.amt - WIN.win_amt):  Rs {r[5]:>12,.2f}")
    log(f"  GGR v2 (SUM amt - SUM win_amt):   Rs {r[6]:>12,.2f}")

    # Cross-check com view
    cur.execute("SELECT SUM(ggr_total) FROM matriz_financeiro")
    ggr_view = cur.fetchone()[0]
    log(f"  GGR matriz_financeiro (view):      Rs {ggr_view:>12,.2f}")
    log(f"  MATCH v1 vs view? {'SIM' if abs(float(r[5]) - float(ggr_view)) < 1000 else 'NAO — diverge ' + str(round(float(r[5]) - float(ggr_view)))}")
    log(f"  MATCH v2 vs view? {'SIM' if abs(float(r[6]) - float(ggr_view)) < 1000 else 'NAO — diverge ' + str(round(float(r[6]) - float(ggr_view)))}")

    section("5. CASINO_USER_GAME_METRICS — cross-check")
    cur.execute("""
        SELECT SUM(total_bet_amount), SUM(total_win_amount), SUM(net_revenue)
        FROM casino_user_game_metrics
    """)
    r = cur.fetchone()
    log(f"  total_bet_amount:  Rs {r[0]:>12,.2f}")
    log(f"  total_win_amount:  Rs {r[1]:>12,.2f}")
    log(f"  net_revenue (GGR): Rs {r[2]:>12,.2f}")
    log(f"  MATCH vs view? {'SIM' if abs(float(r[2]) - float(ggr_view)) < 1000 else 'NAO — diverge ' + str(round(float(r[2]) - float(ggr_view)))}")

    section("6. USERS — campos e distribuicao")
    for col in ['role', 'active', 'blocked', 'is_affiliate']:
        log(f"  {col}:")
        for r in distinct_vals("users", col):
            log(f"    {r[0]!s:<20} {r[1]:>6}")

    cur.execute("SELECT COUNT(*) FILTER (WHERE email IS NOT NULL AND email != ''), COUNT(*) FILTER (WHERE phone IS NOT NULL), COUNT(*) FROM users")
    r = cur.fetchone()
    log(f"\n  Com email: {r[0]}/{r[2]}  |  Com phone: {r[1]}/{r[2]}")

    section("7. WALLETS — tipos e estados")
    for r in distinct_vals("wallets", "type"):
        log(f"  type: {r[0]:<10} {r[1]:>6}")
    cur.execute("SELECT type, COUNT(*) FILTER (WHERE blocked), COUNT(*) FILTER (WHERE balance > 0), ROUND(SUM(balance)::numeric,2) FROM wallets GROUP BY 1")
    for r in cur.fetchall():
        log(f"  {r[0]:<8} bloqueadas:{r[1]}  com_saldo:{r[2]}  total:Rs {r[3]:>12}")

    section("8. BONUS — programas e status")
    cur.execute("SELECT name, type, match_percent, rollover_multiplier, max_withdraw_multiplier, min_deposit_amount, active FROM bonus_programs")
    for r in cur.fetchall():
        log(f"  {r[0]} | type:{r[1]} | match:{r[2]}% | rollover:{r[3]}x | max_saq:{r[4]}x | min_dep:{r[5]} | ativo:{r[6]}")

    for r in distinct_vals("bonus_activations", "status"):
        log(f"  bonus_activations status: {r[0]:<15} {r[1]:>4}")

    section("9. PAYMENT METHODS — config")
    cur.execute("SELECT code, name, operation_type, active, min_amount::numeric(18,2), max_amount::numeric(18,2) FROM payment_methods ORDER BY operation_type, name")
    for r in cur.fetchall():
        log(f"  {r[2]:<8} {r[1]:<25} code:{r[0]:<30} ativo:{r[3]} min:{r[4]} max:{r[5]}")

    section("10. USER_MARKETING_EVENTS — tipos")
    for r in distinct_vals("user_marketing_events", "event_type"):
        log(f"  {r[0]:<20} {r[1]:>6}")

    section("11. WEBHOOK_ENDPOINTS — integrações")
    cur.execute("SELECT description, type, active, events FROM webhook_endpoints")
    for r in cur.fetchall():
        log(f"  {r[0]} | type:{r[1]} | ativo:{r[2]} | events:{r[3]}")

    section("12. TIMESTAMPS — timezone check")
    cur.execute("""
        SELECT
            'transactions' AS tbl,
            MIN(created_at) AS min_ts,
            MAX(created_at) AS max_ts,
            pg_typeof(MIN(created_at))::text AS ts_type
        FROM transactions
        UNION ALL
        SELECT 'bets', MIN(created_at), MAX(created_at), pg_typeof(MIN(created_at))::text FROM bets
        UNION ALL
        SELECT 'users', MIN(created_at), MAX(created_at), pg_typeof(MIN(created_at))::text FROM users
        UNION ALL
        SELECT 'bonus_activations', MIN(created_at), MAX(created_at), pg_typeof(MIN(created_at))::text FROM bonus_activations
    """)
    log(f"  {'tabela':<22} {'tipo':<35} {'min':<28} {'max'}")
    for r in cur.fetchall():
        log(f"  {r[0]:<22} {r[3]:<35} {str(r[1]):<28} {r[2]}")

    # Salvar
    cur.close(); conn.close(); tunnel.stop()
    os.makedirs("reports", exist_ok=True)
    with open("reports/supernova_bet_empirico.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(out))
    log(f"\nSalvo em reports/supernova_bet_empirico.txt")

if __name__ == "__main__":
    run()
