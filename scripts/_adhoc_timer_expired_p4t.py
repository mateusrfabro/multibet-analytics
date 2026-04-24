"""Ad-hoc: timer do gateway (expires_at - created_at) em EXPIRED + tempo ate concluir em COMPLETED."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

def run():
    tunnel, conn = get_supernova_bet_connection()
    try:
        with conn.cursor() as cur:
            # TIMER EXPIRED
            print("\n=== TIMER GATEWAY: quanto tempo o usuario tem para concluir? (EXPIRED) ===")
            cur.execute("""
                SELECT
                    COUNT(*) AS qtd,
                    ROUND(CAST(AVG(EXTRACT(EPOCH FROM (t.expires_at - t.created_at))/60) AS numeric), 2) AS min_medio,
                    ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.expires_at - t.created_at))/60) AS numeric), 2) AS p50,
                    ROUND(CAST(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.expires_at - t.created_at))/60) AS numeric), 2) AS p95,
                    COUNT(*) FILTER (WHERE t.expires_at IS NULL) AS sem_expires
                FROM transactions t
                WHERE t.type='DEPOSIT' AND t.status='EXPIRED'
                  AND t.created_at >= '2026-04-01' AND t.created_at < '2026-04-23'
            """)
            r = cur.fetchone()
            print(f"  EXPIRED total: {r[0]} | media={r[1]}min | mediana={r[2]}min | p95={r[3]}min | sem_expires_at={r[4]}")

            # TEMPO PARA CONCLUIR (COMPLETED)
            print("\n=== TEMPO ATE CONCLUIR (COMPLETED) ===")
            cur.execute("""
                SELECT
                    COUNT(*) AS qtd,
                    ROUND(CAST(AVG(EXTRACT(EPOCH FROM (t.processed_at - t.created_at))/60) AS numeric), 2) AS min_medio,
                    ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.processed_at - t.created_at))/60) AS numeric), 2) AS p50,
                    ROUND(CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.processed_at - t.created_at))/60) AS numeric), 2) AS p75,
                    ROUND(CAST(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.processed_at - t.created_at))/60) AS numeric), 2) AS p90,
                    ROUND(CAST(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.processed_at - t.created_at))/60) AS numeric), 2) AS p95
                FROM transactions t
                WHERE t.type='DEPOSIT' AND t.status='COMPLETED' AND t.processed_at IS NOT NULL
                  AND t.created_at >= '2026-04-01' AND t.created_at < '2026-04-23'
            """)
            r = cur.fetchone()
            print(f"  COMPLETED total: {r[0]} | media={r[1]}min | p50={r[2]}min | p75={r[3]}min | p90={r[4]}min | p95={r[5]}min")

            # USERS QUE EXPIRARAM E NUNCA VOLTARAM (perda direta)
            print("\n=== USERS QUE EXPIRARAM E NUNCA COMPLETARAM DEPOSITO (perda) ===")
            cur.execute("""
                WITH expired_users AS (
                    SELECT DISTINCT user_id
                    FROM transactions
                    WHERE type='DEPOSIT' AND status='EXPIRED'
                      AND created_at >= '2026-04-01' AND created_at < '2026-04-23'
                ),
                completed_users AS (
                    SELECT DISTINCT user_id
                    FROM transactions
                    WHERE type='DEPOSIT' AND status='COMPLETED'
                )
                SELECT
                    COUNT(*) FILTER (WHERE e.user_id NOT IN (SELECT user_id FROM completed_users)) AS nunca_completou,
                    COUNT(*) AS total_expirou
                FROM expired_users e
            """)
            r = cur.fetchone()
            print(f"  Expiraram ao menos uma vez: {r[1]}")
            print(f"  Nunca completaram deposito: {r[0]} ({100*r[0]/r[1]:.1f}%)")

            # PERDA EM PKR dos que nunca completaram
            cur.execute("""
                WITH completed_users AS (
                    SELECT DISTINCT user_id FROM transactions WHERE type='DEPOSIT' AND status='COMPLETED'
                )
                SELECT
                    COUNT(DISTINCT t.user_id) AS users,
                    COUNT(*) AS tentativas,
                    ROUND(SUM(t.amount)::numeric, 2) AS valor_pkr_total
                FROM transactions t
                WHERE t.type='DEPOSIT' AND t.status='EXPIRED'
                  AND t.created_at >= '2026-04-01' AND t.created_at < '2026-04-23'
                  AND t.user_id NOT IN (SELECT user_id FROM completed_users)
            """)
            r = cur.fetchone()
            print(f"\n  PERDA DIRETA (users expirou e nunca voltou):")
            print(f"    Users perdidos   : {r[0]}")
            print(f"    Tentativas perdidas: {r[1]}")
            print(f"    Valor PKR desejado: {float(r[2] or 0):,.0f}")

            # Comparacao JazzCash vs Easypaisa — EXPIRED
            print("\n=== COMPARATIVO JAZZCASH vs EASYPAISA em EXPIRED ===")
            cur.execute("""
                SELECT COALESCE(pm.name, '(sem pm)') AS pm,
                       COUNT(*) FILTER (WHERE t.status='EXPIRED') AS expired,
                       COUNT(*) FILTER (WHERE t.status='COMPLETED') AS completed,
                       COUNT(*) AS total,
                       ROUND(100.0 * COUNT(*) FILTER (WHERE t.status='EXPIRED') / NULLIF(COUNT(*),0), 1) AS pct_expired
                FROM transactions t
                LEFT JOIN payment_methods pm ON pm.id = t.payment_method_id
                WHERE t.type='DEPOSIT'
                  AND t.created_at >= '2026-04-01' AND t.created_at < '2026-04-23'
                GROUP BY 1
                ORDER BY total DESC
            """)
            print(f"  {'payment_method':28} {'expired':>8} {'completed':>10} {'total':>8} {'%expir':>7}")
            for r in cur.fetchall():
                print(f"  {str(r[0])[:28]:28} {r[1]:>8} {r[2]:>10} {r[3]:>8} {str(r[4] or '—'):>7}")

    finally:
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    run()
