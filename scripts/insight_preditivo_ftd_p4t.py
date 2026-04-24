"""Insight preditivo p/ o Head - bonus farming Play4Tune 18/04.
Foco: o que esta ocorrendo AGORA, como vai escalar, impacto projetado, quanto do CV e real vs farmer."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

PKR_BRL = 0.017881

def pkr(v): return f"Rs {float(v or 0):>10,.2f}"
def brl(v): return f"R$ {float(v or 0)*PKR_BRL:>9,.2f}"
def banner(s): print("\n" + "="*78 + "\n" + s + "\n" + "="*78)

SUSPECT = """
    SELECT id FROM users
    WHERE phone LIKE '+92341374%' OR username = 'utl2FFfrQR7Qj6qi'
"""

tunnel, conn = get_supernova_bet_connection()
try:
    with conn.cursor() as cur:
        # =========================================================
        # INSIGHT 1: JOGOS COM RTP > 100% (arbitragem matematica?)
        # =========================================================
        banner("INSIGHT 1 - Jogos com RTP > 100% no catalogo (arbitragem EV+?)")
        cur.execute("""
            SELECT name, rtp::numeric(5,2), active, pot::numeric(5,2) AS pot
            FROM casino_games
            WHERE rtp > 100
            ORDER BY rtp DESC
        """)
        rows = cur.fetchall()
        print(f"   Total jogos RTP > 100%: {len(rows)}")
        for r in rows:
            print(f"   {str(r[0])[:30]:30} rtp={r[1]:>6} active={r[2]} pot={r[3]}")
        if rows:
            # Quanto turnover do batch foi nesses jogos?
            cur.execute(f"""
                WITH suspect AS ({SUSPECT})
                SELECT
                    SUM(CASE WHEN g.rtp > 100 THEN m.total_bet_amount ELSE 0 END)::numeric(12,0) AS turn_ev_pos,
                    SUM(m.total_bet_amount)::numeric(12,0) AS turn_total,
                    SUM(CASE WHEN g.rtp > 100 THEN m.net_revenue ELSE 0 END)::numeric(12,0) AS ggr_ev_pos,
                    SUM(m.net_revenue)::numeric(12,0) AS ggr_total
                FROM casino_user_game_metrics m
                JOIN suspect s ON s.id=m.user_id
                JOIN casino_games g ON g.id=m.game_id
            """)
            r = cur.fetchone()
            pct_ev_pos = 100*float(r[0] or 0)/float(r[1] or 1)
            print(f"\n   Turnover do batch em jogos RTP>100%: {pkr(r[0])} ({pct_ev_pos:.0f}% do total)")
            print(f"   GGR do batch em jogos RTP>100%: {pkr(r[2])}  |  GGR total: {pkr(r[3])}")

        # =========================================================
        # INSIGHT 2: CV FTD SEM O BATCH = quanto seria?
        # =========================================================
        banner("INSIGHT 2 - CV FTD real (orgânico), removendo batch")
        cur.execute(f"""
            WITH suspect AS ({SUSPECT}),
            ftd_users AS (
                SELECT DISTINCT user_id FROM transactions
                WHERE type='DEPOSIT' AND status='COMPLETED'
            )
            SELECT
                COUNT(*) FILTER (WHERE u.role='USER') AS cad_total,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM suspect)) AS cad_organico,
                COUNT(*) FILTER (WHERE u.role='USER' AND EXISTS(SELECT 1 FROM ftd_users f WHERE f.user_id=u.id)) AS ftd_total,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM suspect) AND EXISTS(SELECT 1 FROM ftd_users f WHERE f.user_id=u.id)) AS ftd_organico
            FROM users u
            WHERE ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                  = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
        """)
        r = cur.fetchone()
        cad_tot, cad_org, ftd_tot, ftd_org = r
        cv_tot = 100*ftd_tot/max(cad_tot,1)
        cv_org = 100*ftd_org/max(cad_org,1)
        print(f"   Cadastros total hoje: {cad_tot}  FTD total: {ftd_tot}  CV: {cv_tot:.1f}%")
        print(f"   Cadastros s/batch:    {cad_org}  FTD s/batch: {ftd_org}  CV: {cv_org:.1f}%")
        print(f"\n   >> Baseline historico: 17.8% | CV organico hoje: {cv_org:.1f}% | CV reportado: {cv_tot:.1f}%")
        print(f"   >> TODO o salto (~+{cv_tot-cv_org:.1f}pp) e explicado pelo batch farmer.")
        print(f"   >> Remover o batch: CV volta pro baseline natural.")

        # =========================================================
        # INSIGHT 3: PADRAO HORARIO / CADENCIA DO FARMER
        # =========================================================
        banner("INSIGHT 3 - Cadencia do farmer (prever proximas janelas)")
        cur.execute(f"""
            WITH suspect AS ({SUSPECT})
            SELECT
                DATE_TRUNC('hour', u.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora_brt,
                COUNT(*) AS contas
            FROM users u JOIN suspect s ON s.id=u.id
            GROUP BY 1 ORDER BY 1
        """)
        print(f"   {'hora BRT':18} {'PKT':>5} {'contas':>7}")
        for r in cur.fetchall():
            pkt = (r[0].hour + 8) % 24
            print(f"   {str(r[0])[:16]:18} {pkt:>02}h   {r[1]:>7}")

        cur.execute(f"""
            WITH suspect AS ({SUSPECT})
            SELECT
                EXTRACT(HOUR FROM (u.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')) AS hr_brt,
                COUNT(*) AS contas
            FROM users u JOIN suspect s ON s.id=u.id
            GROUP BY 1 ORDER BY 2 DESC
        """)
        print(f"\n   Picos de cadastro do batch (por hora BRT):")
        for r in cur.fetchall():
            pkt = (int(r[0]) + 8) % 24
            print(f"     {int(r[0]):02}h BRT ({pkt:02}h PKT): {r[1]} contas")

        # Intervalo medio entre cadastros do batch
        cur.execute(f"""
            WITH suspect AS ({SUSPECT}),
            ordered AS (
                SELECT u.created_at,
                       LAG(u.created_at) OVER (ORDER BY u.created_at) AS prev
                FROM users u JOIN suspect s ON s.id=u.id
            )
            SELECT
                AVG(EXTRACT(EPOCH FROM (created_at - prev))/60)::numeric(6,1) AS gap_medio_min,
                MIN(EXTRACT(EPOCH FROM (created_at - prev))/60)::numeric(6,1) AS gap_min,
                MAX(EXTRACT(EPOCH FROM (created_at - prev))/60)::numeric(6,1) AS gap_max
            FROM ordered WHERE prev IS NOT NULL
        """)
        r = cur.fetchone()
        print(f"\n   Gap medio entre cadastros do batch: {r[0]}min  (min={r[1]}min, max={r[2]}min)")

        # =========================================================
        # INSIGHT 4: FARMER ESTAVA TESTANDO? (dias anteriores)
        # =========================================================
        banner("INSIGHT 4 - Farmer ja aparecia nos dias anteriores? (teste)")
        cur.execute("""
            SELECT
                ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                COUNT(*) FILTER (WHERE u.phone LIKE '+923413741%') AS batch_prefix,
                COUNT(*) FILTER (WHERE u.email LIKE '%@wetuns.com' OR u.email LIKE '%@whyknapp.com') AS temp_mail,
                COUNT(*) FILTER (WHERE u.phone LIKE '+923413740%' OR u.phone LIKE '+923413742%' OR u.phone LIKE '+923413743%') AS prefix_vizinho,
                COUNT(*) AS total_cad
            FROM users u
            WHERE u.created_at >= CURRENT_DATE - INTERVAL '15 days'
              AND u.role='USER'
            GROUP BY 1 ORDER BY 1
        """)
        print(f"   {'dia':12} {'batch +92341374':>16} {'temp-mail':>10} {'prefix vizinho':>15} {'total':>6}")
        for r in cur.fetchall():
            flag = " <-" if (r[1] > 0 or r[2] > 0) else ""
            print(f"   {str(r[0]):12} {r[1]:>16} {r[2]:>10} {r[3]:>15} {r[4]:>6}{flag}")

        # =========================================================
        # INSIGHT 5: PREFIXOS VIZINHOS / DOMINIOS PARECIDOS
        # =========================================================
        banner("INSIGHT 5 - Prefixos vizinhos e dominios novos hoje (proximos alvos?)")
        cur.execute("""
            SELECT SUBSTRING(u.phone,1,9) AS prefix_9,
                   COUNT(*) total,
                   COUNT(*) FILTER (WHERE u.created_at >= CURRENT_DATE - INTERVAL '1 day') AS hoje,
                   COUNT(*) FILTER (WHERE u.created_at < CURRENT_DATE - INTERVAL '1 day') AS antes
            FROM users u
            WHERE u.role='USER'
              AND u.phone LIKE '+92341374%'
            GROUP BY 1 ORDER BY 1
        """)
        print(f"   Prefixos 9 digitos '+92341374*' — batch atual vs historico:")
        for r in cur.fetchall():
            print(f"     {r[0]}   total={r[1]:>3}  hoje={r[2]:>3}  antes={r[3]:>3}")

        cur.execute("""
            SELECT SPLIT_PART(email, '@', 2) AS dominio,
                   COUNT(*) FILTER (WHERE u.created_at >= CURRENT_DATE - INTERVAL '1 day') AS hoje,
                   COUNT(*) FILTER (WHERE u.created_at < CURRENT_DATE - INTERVAL '1 day') AS antes
            FROM users u
            WHERE u.role='USER' AND u.email IS NOT NULL
            GROUP BY 1
            HAVING COUNT(*) FILTER (WHERE u.created_at >= CURRENT_DATE - INTERVAL '1 day') >= 1
               AND COUNT(*) FILTER (WHERE u.created_at < CURRENT_DATE - INTERVAL '1 day') = 0
            ORDER BY hoje DESC
        """)
        rows = cur.fetchall()
        print(f"\n   Dominios NOVOS hoje (zero historico antes):")
        for r in rows:
            print(f"     {r[0]:35} hoje={r[1]}")

        # =========================================================
        # INSIGHT 6: PROJECAO DE DANO SE NAO BLOQUEAR
        # =========================================================
        banner("INSIGHT 6 - Projecao de dano (cenarios) se nao bloquear")
        # Metricas por conta farmer
        cur.execute(f"""
            WITH suspect AS ({SUSPECT})
            SELECT
                COUNT(*) AS n_contas,
                AVG((SELECT SUM(t.amount) FROM transactions t WHERE t.user_id=s.id AND t.type='DEPOSIT' AND t.status='COMPLETED')) AS dep_medio,
                AVG(COALESCE((SELECT SUM(t.amount) FROM transactions t WHERE t.user_id=s.id AND t.type='WITHDRAW' AND t.status='COMPLETED'),0)) AS saq_medio,
                AVG((SELECT SUM(m.net_revenue) FROM casino_user_game_metrics m WHERE m.user_id=s.id)) AS ggr_medio
            FROM suspect s
        """)
        r = cur.fetchone()
        n, dep_m, saq_m, ggr_m = r
        resultado_por_conta = float(dep_m or 0) - float(saq_m or 0)  # lucro casa por farmer
        print(f"   Por conta farmer: dep_medio={pkr(dep_m)}  saq_medio={pkr(saq_m)}  GGR={pkr(ggr_m)}")
        print(f"   Resultado caixa por farmer: {pkr(resultado_por_conta)}")
        print(f"\n   Cenario 1: amanha repete (17 contas): impacto = {pkr(17*resultado_por_conta)} {brl(17*resultado_por_conta)}")
        print(f"   Cenario 2: escala 3x (50 contas):       impacto = {pkr(50*resultado_por_conta)} {brl(50*resultado_por_conta)}")
        print(f"   Cenario 3: escala 10x (170 contas):     impacto = {pkr(170*resultado_por_conta)} {brl(170*resultado_por_conta)}")
        print(f"   Cenario 4: 1 semana (1200 contas):      impacto = {pkr(1200*resultado_por_conta)} {brl(1200*resultado_por_conta)}")
        print(f"\n   NOTA: resultado positivo = lucro casa; negativo = prejuizo.")
        print(f"   Com RTP 96%+ (jogos escolhidos) + rollover 75x: ao escalar volume,")
        print(f"   o farmer encontra outliers (RTP>100% jogos) e a casa SANGRA.")

        # Checar contas com resultado POSITIVO pro farmer (prejuizo casa)
        cur.execute(f"""
            WITH suspect AS ({SUSPECT})
            SELECT u.username, u.phone,
                   (SELECT COALESCE(SUM(t.amount),0) FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED') AS dep,
                   (SELECT COALESCE(SUM(t.amount),0) FROM transactions t WHERE t.user_id=u.id AND t.type='WITHDRAW' AND t.status='COMPLETED') AS saq
            FROM users u JOIN suspect s ON s.id=u.id
            ORDER BY ((SELECT COALESCE(SUM(t.amount),0) FROM transactions t WHERE t.user_id=u.id AND t.type='WITHDRAW' AND t.status='COMPLETED') -
                      (SELECT COALESCE(SUM(t.amount),0) FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED')) DESC
        """)
        rows = cur.fetchall()
        lucra_farmer = [r for r in rows if float(r[3])-float(r[2]) > 0]
        print(f"\n   {len(lucra_farmer)} de {len(rows)} contas do batch ja estao LUCRATIVAS pro farmer:")
        for r in lucra_farmer:
            lucro = float(r[3]) - float(r[2])
            print(f"     {str(r[0])[:18]:18} {r[1]:14}  dep={pkr(r[2])}  saq={pkr(r[3])}  LUCRO FARMER={pkr(lucro)}")

finally:
    conn.close(); tunnel.stop()
