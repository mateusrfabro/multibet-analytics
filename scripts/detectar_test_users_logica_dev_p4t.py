"""
Detecta usuarios teste na Play4Tune pela LOGICA OFICIAL DO DEV:
  "Se o usuario teve (a) adicao manual de saldo OU (b) confirmacao manual de deposito,
   ate o momento atual ele e teste."

Mapeamento:
  (a) transactions.type IN ('ADJUSTMENT_CREDIT','ADJUSTMENT_DEBIT')
  (b) transactions.type='DEPOSIT' AND reviewed_by IS NOT NULL

Comparar com filtro heuristico atual (username/email/role) pra ver:
- Quem a heuristica pega e a logica do dev NAO pega (falso positivo atual)
- Quem a logica do dev pega e a heuristica NAO pega (gap atual, precisa adicionar)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.supernova_bet import get_supernova_bet_connection


def run():
    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    try:
        # =================================================================
        # 1. LOGICA OFICIAL DEV — usuarios com manipulacao manual
        # =================================================================
        cur.execute("""
            WITH manual_signals AS (
                SELECT
                    t.user_id,
                    SUM(CASE WHEN t.type = 'ADJUSTMENT_CREDIT' THEN 1 ELSE 0 END) AS adjust_credit_qtd,
                    SUM(CASE WHEN t.type = 'ADJUSTMENT_CREDIT' THEN t.amount ELSE 0 END) AS adjust_credit_valor,
                    SUM(CASE WHEN t.type = 'ADJUSTMENT_DEBIT' THEN 1 ELSE 0 END) AS adjust_debit_qtd,
                    SUM(CASE WHEN t.type = 'ADJUSTMENT_DEBIT' THEN t.amount ELSE 0 END) AS adjust_debit_valor,
                    SUM(CASE WHEN t.type = 'DEPOSIT' AND t.reviewed_by IS NOT NULL THEN 1 ELSE 0 END) AS dep_manual_qtd,
                    SUM(CASE WHEN t.type = 'DEPOSIT' AND t.reviewed_by IS NOT NULL THEN t.amount ELSE 0 END) AS dep_manual_valor,
                    MAX(t.reviewed_by) AS exemplo_reviewed_by
                FROM transactions t
                WHERE t.type IN ('ADJUSTMENT_CREDIT', 'ADJUSTMENT_DEBIT')
                   OR (t.type = 'DEPOSIT' AND t.reviewed_by IS NOT NULL)
                GROUP BY t.user_id
            )
            SELECT
                u.id, u.username, u.public_id, u.role, u.email, u.phone,
                u.created_at::date AS criado_em,
                ms.adjust_credit_qtd, ms.adjust_credit_valor,
                ms.adjust_debit_qtd, ms.adjust_debit_valor,
                ms.dep_manual_qtd, ms.dep_manual_valor,
                ms.exemplo_reviewed_by,
                COALESCE(ROUND(SUM(um.total_bet_amount)::numeric, 2), 0) AS turnover_pkr,
                COALESCE(SUM(um.played_rounds), 0) AS giros
            FROM manual_signals ms
            JOIN users u ON u.id = ms.user_id
            LEFT JOIN casino_user_game_metrics um ON um.user_id = u.id
            GROUP BY u.id, u.username, u.public_id, u.role, u.email, u.phone, u.created_at,
                     ms.adjust_credit_qtd, ms.adjust_credit_valor,
                     ms.adjust_debit_qtd, ms.adjust_debit_valor,
                     ms.dep_manual_qtd, ms.dep_manual_valor, ms.exemplo_reviewed_by
            ORDER BY turnover_pkr DESC, u.username
        """)
        logica_dev = cur.fetchall()
        logica_dev_ids = {r[0] for r in logica_dev}

        print("=" * 150)
        print(f"LOGICA DEV -- usuarios com adicao manual de saldo OU confirmacao manual de deposito")
        print(f"TOTAL: {len(logica_dev)} usuarios")
        print("=" * 150)
        print(f"\n{'Username':<30} {'Role':<12} {'Email':<35} {'AdjCred':>8} {'AdjDeb':>8} {'DepMan':>8} {'Turnover_PKR':>15} {'Giros':>8} {'ReviewedBy'}")
        print("-" * 150)
        for r in logica_dev:
            uid, uname, pid, role, email, phone, criado, ac_q, ac_v, ad_q, ad_v, dm_q, dm_v, rb, turn, giros = r
            email_s = (email or '-')[:33]
            rb_s = (rb or '-')[:30]
            print(f"{uname:<30} {role:<12} {email_s:<35} {int(ac_q):>8} {int(ad_q):>8} {int(dm_q):>8} {float(turn):>15,.2f} {int(giros):>8,} {rb_s}")

        # =================================================================
        # 2. FILTRO HEURISTICO ATUAL (username/email/role)
        # =================================================================
        cur.execute("""
            SELECT u.id, u.username, u.role, u.email
            FROM users u
            WHERE u.role != 'USER'
               OR LOWER(u.username) LIKE '%test%'
               OR LOWER(u.username) LIKE '%teste%'
               OR LOWER(u.username) LIKE '%demo%'
               OR LOWER(u.username) LIKE '%admin%'
               OR LOWER(COALESCE(u.email, '')) LIKE '%@karinzitta%'
               OR LOWER(COALESCE(u.email, '')) LIKE '%@multi.bet%'
               OR LOWER(COALESCE(u.email, '')) LIKE '%@grupo-pgs%'
               OR LOWER(COALESCE(u.email, '')) LIKE '%@supernovagaming%'
               OR LOWER(COALESCE(u.email, '')) LIKE '%@play4tune%'
        """)
        heuristica = cur.fetchall()
        heuristica_ids = {r[0] for r in heuristica}
        heuristica_map = {r[0]: r for r in heuristica}

        # =================================================================
        # 3. COMPARAR OS DOIS FILTROS
        # =================================================================
        print("\n" + "=" * 150)
        print("COMPARACAO: LOGICA DEV (oficial) vs FILTRO HEURISTICO ATUAL (username/email/role)")
        print("=" * 150)
        print(f"\n  Logica DEV:            {len(logica_dev_ids)} usuarios")
        print(f"  Heuristica atual:      {len(heuristica_ids)} usuarios")
        print(f"  Intersecao (ambos):    {len(logica_dev_ids & heuristica_ids)}")
        print(f"  So LOGICA DEV:         {len(logica_dev_ids - heuristica_ids)}  <-- GAP: a heuristica NAO pega")
        print(f"  So HEURISTICA:         {len(heuristica_ids - logica_dev_ids)}  <-- potencial falso positivo da heuristica")

        # A) Quem a logica dev pega mas a heuristica atual NAO (precisa adicionar na base de teste!)
        gap_ids = logica_dev_ids - heuristica_ids
        if gap_ids:
            print("\n" + "=" * 150)
            print("GAP -- usuarios que a LOGICA DEV marca como teste mas a heuristica ATUAL NAO pega:")
            print("(estes estao contando como jogador real hoje no report, mas deveriam ser excluidos)")
            print("=" * 150)
            gap_rows = [r for r in logica_dev if r[0] in gap_ids]
            gap_rows.sort(key=lambda r: -float(r[14]))
            print(f"\n{'Username':<30} {'Role':<12} {'Email':<40} {'Turnover_PKR':>15} {'Giros':>8}")
            print("-" * 110)
            for r in gap_rows:
                uid, uname, pid, role, email, phone, criado, ac_q, ac_v, ad_q, ad_v, dm_q, dm_v, rb, turn, giros = r
                email_s = (email or '-')[:38]
                print(f"{uname:<30} {role:<12} {email_s:<40} {float(turn):>15,.2f} {int(giros):>8,}")
            turn_gap = sum(float(r[14]) for r in gap_rows)
            giros_gap = sum(int(r[15]) for r in gap_rows)
            print(f"\n  IMPACTO se adicionarmos ao filtro:")
            print(f"    Turnover a remover: Rs {turn_gap:,.2f}")
            print(f"    Giros a remover:    {giros_gap:,}")

        # B) Quem a heuristica pega mas a logica dev NAO — essas sao OK (nunca tiveram manipulacao manual),
        #    provavelmente sao admins/dev que so se cadastraram sem depositar/manipular
        print("\n" + "=" * 150)
        print("HEURISTICA pega mas DEV nao -- OK, sao contas de cadastro/admin sem movimento (excluir continua correto)")
        print("=" * 150)
        so_heuristica = heuristica_ids - logica_dev_ids
        print(f"  Total: {len(so_heuristica)} contas (majoritariamente ADMINs cadastrados que nao mexeram saldo manualmente)")

    finally:
        cur.close()
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    run()
