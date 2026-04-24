"""
Mostra as transacoes manuais de um ou mais usernames especificos
-- evidencia de PORQUE foram classificados como teste pela logica do dev.
"""
import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

USERNAMES = [
    'maharshani44377634693',
    'muhammadrehan17657797557',
]


def run():
    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()
    try:
        for uname in USERNAMES:
            print("\n" + "=" * 130)
            print(f"USUARIO: {uname}")
            print("=" * 130)

            cur.execute("""
                SELECT id, username, public_id, role, email, phone,
                       created_at, affiliate_id
                FROM users WHERE username = %s
            """, (uname,))
            u = cur.fetchone()
            if not u:
                print("  usuario nao encontrado")
                continue
            uid, un, pid, role, email, phone, criado, aff = u
            print(f"  ID:           {uid}")
            print(f"  PID:          {pid}")
            print(f"  Role:         {role}")
            print(f"  Email:        {email or '-'}")
            print(f"  Phone:        {phone or '-'}")
            print(f"  Criado:       {criado}")
            print(f"  Affiliate ID: {aff or '-'}")

            # Todas as transacoes manuais
            cur.execute("""
                SELECT type, status, amount, balance_before, balance_after,
                       reviewed_by, reviewed_at, review_note, flagged_for_review,
                       payment_method_id, reference_type, reference_id,
                       metadata, created_at, processed_at,
                       error_reason, rejection_reason
                FROM transactions
                WHERE user_id = %s
                  AND (type IN ('ADJUSTMENT_CREDIT','ADJUSTMENT_DEBIT')
                       OR (type='DEPOSIT' AND reviewed_by IS NOT NULL))
                ORDER BY created_at
            """, (uid,))
            txs = cur.fetchall()

            print(f"\n  TRANSACOES MANUAIS ({len(txs)}):")
            for tx in txs:
                (tp, st, amt, bb, ba, rb, rat, rn, flag, pm, rtp, rid,
                 meta, cat, pat, err, rej) = tx
                print(f"\n    Tipo:            {tp}")
                print(f"    Status:          {st}")
                print(f"    Valor (PKR):     {amt}")
                print(f"    Saldo antes:     {bb}")
                print(f"    Saldo depois:    {ba}")
                print(f"    Created at:      {cat}")
                print(f"    Processed at:    {pat or '-'}")
                print(f"    Reviewed by:     {rb or '-'}")
                print(f"    Reviewed at:     {rat or '-'}")
                print(f"    Review note:     {rn or '-'}")
                print(f"    Flagged:         {flag}")
                print(f"    Payment method:  {pm or '-'}")
                print(f"    Reference type:  {rtp or '-'}")
                print(f"    Reference id:    {rid or '-'}")
                print(f"    Error reason:    {err or '-'}")
                print(f"    Rejection:       {rej or '-'}")
                if meta:
                    try:
                        meta_str = json.dumps(meta, indent=2, ensure_ascii=False)
                    except Exception:
                        meta_str = str(meta)
                    print(f"    Metadata:")
                    for line in str(meta_str).split('\n')[:20]:
                        print(f"      {line}")

            # Quem e o reviewer (se tem ID)
            reviewers = set(tx[5] for tx in txs if tx[5])
            for rev_id in reviewers:
                cur.execute("""
                    SELECT username, role, email
                    FROM users WHERE id::text = %s OR public_id = %s
                """, (rev_id, rev_id))
                rev = cur.fetchone()
                if rev:
                    print(f"\n  REVIEWER '{rev_id}' = {rev[0]} (role={rev[1]}, email={rev[2] or '-'})")

            # Resumo atividade
            cur.execute("""
                SELECT COUNT(*) AS n_tx, COUNT(DISTINCT type) AS tipos
                FROM transactions WHERE user_id = %s
            """, (uid,))
            tot_tx, tipos = cur.fetchone()
            cur.execute("""
                SELECT COUNT(*) FROM bets WHERE user_id = %s
            """, (uid,))
            tot_bets = cur.fetchone()[0]
            print(f"\n  RESUMO:")
            print(f"    Total transacoes:   {tot_tx}")
            print(f"    Tipos distintos:    {tipos}")
            print(f"    Total bets:         {tot_bets}")

    finally:
        cur.close()
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    run()
