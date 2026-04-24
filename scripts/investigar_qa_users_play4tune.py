"""
Investigacao: contas com 'qa' no username na Play4Tune.
Objetivo: classificar cada uma como (a) QA legitimo → whitelist
                                     (b) jogador paquistanes real → manter na base.
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
        # 1. Listar TODAS as contas com 'qa' no username (sem filtro de role)
        cur.execute("""
            SELECT
                u.id,
                u.username,
                u.public_id,
                u.role,
                u.email,
                u.phone,
                u.created_at::date AS criado_em,
                COUNT(DISTINCT um.date) AS dias_ativo,
                COALESCE(ROUND(SUM(um.total_bet_amount)::numeric, 2), 0) AS turnover_pkr,
                COALESCE(ROUND(SUM(um.net_revenue)::numeric, 2), 0) AS ggr_pkr,
                COALESCE(SUM(um.played_rounds), 0) AS giros
            FROM users u
            LEFT JOIN casino_user_game_metrics um ON um.user_id = u.id
            WHERE LOWER(u.username) LIKE '%qa%'
            GROUP BY u.id, u.username, u.public_id, u.role, u.email, u.phone, u.created_at
            ORDER BY turnover_pkr DESC, u.username
        """)
        contas = cur.fetchall()

        print("=" * 130)
        print(f"TOTAL CONTAS COM 'qa' NO USERNAME: {len(contas)}")
        print("=" * 130)

        # Heuristicas de classificacao
        PADROES_QA_EXPLICITO = ['qa_', '_qa_', '_qa', 'qa.', '.qa', 'qateste', 'qatester', 'qauser', 'qatest']
        EMAILS_INTERNOS = ['@karinzitta', '@multi.bet', '@grupo-pgs', '@supernovagaming', '@play4tune']

        def classificar(username, email, role):
            u = (username or '').lower()
            e = (email or '').lower()
            if role != 'USER':
                return 'EXCLUIDO_ROLE'
            if any(p in u for p in PADROES_QA_EXPLICITO):
                return 'QA_SUSPEITO'
            if any(p in e for p in EMAILS_INTERNOS):
                return 'EXCLUIDO_EMAIL'
            # Heuristica: nomes paquistaneses tipicos comecando com Qa
            if u.startswith('qa') and len(u) > 2 and u[2] in 'bcdfghjklmnprstvwxyz':
                # qadir, qamar, qasim, qasem → consoante apos 'qa'
                return 'LEGITIMO_PROVAVEL'
            return 'INDETERMINADO'

        print(f"\n{'ID curto':<10} {'Username':<30} {'Role':<12} {'Email':<35} {'Bets':>8} {'Turnover_PKR':>15} {'Giros':>10} {'Classificacao':<25}")
        print("-" * 145)

        stats = {}
        for c in contas:
            uid, uname, pid, role, email, phone, criado, dias, turnover, ggr, giros = c
            cls = classificar(uname, email, role)
            stats[cls] = stats.get(cls, 0) + 1
            id_short = str(uid)[:8]
            email_short = (email or '—')[:33]
            print(f"{id_short:<10} {uname:<30} {role:<12} {email_short:<35} {dias:>8} {float(turnover):>15,.2f} {int(giros):>10,} {cls:<25}")

        print("\n" + "=" * 130)
        print("RESUMO POR CLASSIFICACAO:")
        print("=" * 130)
        for k, v in sorted(stats.items(), key=lambda x: -x[1]):
            print(f"  {k:<25} {v:>5}")

        # 2. Detalhe dos QA_SUSPEITO e INDETERMINADO (precisam decisao)
        print("\n" + "=" * 130)
        print("CONTAS QUE PRECISAM CLASSIFICACAO MANUAL (QA_SUSPEITO + INDETERMINADO):")
        print("=" * 130)
        para_decidir = []
        for c in contas:
            uid, uname, pid, role, email, phone, criado, dias, turnover, ggr, giros = c
            cls = classificar(uname, email, role)
            if cls in ('QA_SUSPEITO', 'INDETERMINADO'):
                para_decidir.append((uname, pid, role, email, phone, criado, dias, float(turnover), int(giros), cls))

        if not para_decidir:
            print("\n  Nenhuma conta ambigua. Todas ja classificadas automaticamente.")
        else:
            for p in para_decidir:
                print(f"\n  Username: {p[0]}")
                print(f"    PID:            {p[1]}")
                print(f"    Role:           {p[2]}")
                print(f"    Email:          {p[3] or '—'}")
                print(f"    Phone:          {p[4] or '—'}")
                print(f"    Criado em:      {p[5]}")
                print(f"    Dias ativo:     {p[6]}")
                print(f"    Turnover (PKR): {p[7]:,.2f}")
                print(f"    Giros:          {p[8]:,}")
                print(f"    Classificacao:  {p[9]}  ← decidir: WHITELIST ou MANTER")

        # 3. Totais de impacto (se movermos os QA_SUSPEITO/INDETERMINADO pra whitelist)
        if para_decidir:
            turn_total = sum(p[7] for p in para_decidir)
            giros_total = sum(p[8] for p in para_decidir)
            print(f"\n  IMPACTO se TODOS forem pra whitelist:")
            print(f"    Turnover removido: Rs {turn_total:,.2f}")
            print(f"    Giros removidos:   {giros_total:,}")

    finally:
        cur.close()
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    run()
