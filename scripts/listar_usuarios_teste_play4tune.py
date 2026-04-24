"""
Lista as contas EXCLUIDAS como teste pelo filtro atual.
Para mostrar ao time de dev: quais usuarios a gente esta tratando como teste hoje.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.supernova_bet import get_supernova_bet_connection


def qual_regra(username, email, role):
    """Retorna qual regra do filtro capturou esta conta."""
    u = (username or '').lower()
    e = (email or '').lower()
    regras = []
    if role != 'USER':
        regras.append(f"role={role}")
    if 'test' in u:
        regras.append("username~test")
    if 'teste' in u:
        regras.append("username~teste")
    if 'demo' in u:
        regras.append("username~demo")
    if 'admin' in u:
        regras.append("username~admin")
    if '@karinzitta' in e:
        regras.append("email@karinzitta")
    if '@multi.bet' in e:
        regras.append("email@multi.bet")
    if '@grupo-pgs' in e:
        regras.append("email@grupo-pgs")
    if '@supernovagaming' in e:
        regras.append("email@supernovagaming")
    if '@play4tune' in e:
        regras.append("email@play4tune")
    return " + ".join(regras) if regras else "???"


def run():
    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                u.id, u.username, u.public_id, u.role, u.email, u.phone,
                u.created_at::date AS criado_em,
                COUNT(DISTINCT um.date) AS dias_ativo,
                COALESCE(ROUND(SUM(um.total_bet_amount)::numeric, 2), 0) AS turnover_pkr,
                COALESCE(ROUND(SUM(um.net_revenue)::numeric, 2), 0) AS ggr_pkr,
                COALESCE(SUM(um.played_rounds), 0) AS giros
            FROM users u
            LEFT JOIN casino_user_game_metrics um ON um.user_id = u.id
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
            GROUP BY u.id, u.username, u.public_id, u.role, u.email, u.phone, u.created_at
            ORDER BY turnover_pkr DESC, u.role, u.username
        """)
        contas = cur.fetchall()

        print(f"TOTAL CONTAS EXCLUIDAS COMO TESTE: {len(contas)}\n")

        com_bets = [c for c in contas if float(c[8]) > 0]
        sem_bets = [c for c in contas if float(c[8]) == 0]

        print(f"  Com atividade (apostaram): {len(com_bets)}")
        print(f"  Sem atividade:              {len(sem_bets)}")
        print("")
        print("=" * 150)
        print("CONTAS COM ATIVIDADE (apostaram) -- as que impactam nos numeros")
        print("=" * 150)
        print(f"\n{'Username':<35} {'Role':<12} {'Email':<40} {'Criado':<12} {'Giros':>8} {'Turnover_PKR':>15}  {'Regra matched'}")
        print("-" * 150)
        for c in com_bets:
            uid, uname, pid, role, email, phone, criado, dias, turnover, ggr, giros = c
            email_s = (email or '-')[:38]
            regra = qual_regra(uname, email, role)
            print(f"{uname:<35} {role:<12} {email_s:<40} {str(criado):<12} {int(giros):>8,} {float(turnover):>15,.2f}  {regra}")

        print("\n" + "=" * 150)
        print("CONTAS SEM ATIVIDADE -- so cadastradas, nao apostaram (mais provavel serem teste de cadastro)")
        print("=" * 150)
        print(f"\n{'Username':<35} {'Role':<12} {'Email':<40} {'Criado':<12}  {'Regra matched'}")
        print("-" * 150)
        for c in sem_bets[:30]:
            uid, uname, pid, role, email, phone, criado, dias, turnover, ggr, giros = c
            email_s = (email or '-')[:38]
            regra = qual_regra(uname, email, role)
            print(f"{uname:<35} {role:<12} {email_s:<40} {str(criado):<12}  {regra}")

        if len(sem_bets) > 30:
            print(f"\n  ... e mais {len(sem_bets) - 30} contas sem atividade")

        # Quebra por regra
        print("\n" + "=" * 150)
        print("BREAKDOWN POR REGRA DO FILTRO")
        print("=" * 150)
        stats = {}
        for c in contas:
            uid, uname, pid, role, email, phone, criado, dias, turnover, ggr, giros = c
            regra = qual_regra(uname, email, role)
            if regra not in stats:
                stats[regra] = {'count': 0, 'com_bets': 0, 'turn_pkr': 0}
            stats[regra]['count'] += 1
            if float(turnover) > 0:
                stats[regra]['com_bets'] += 1
                stats[regra]['turn_pkr'] += float(turnover)

        print(f"\n{'Regra':<55} {'Total':>8} {'Com Bets':>10} {'Turnover_PKR':>15}")
        print("-" * 90)
        for regra, s in sorted(stats.items(), key=lambda x: -x[1]['count']):
            print(f"{regra:<55} {s['count']:>8} {s['com_bets']:>10} {s['turn_pkr']:>15,.2f}")

    finally:
        cur.close()
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    run()
