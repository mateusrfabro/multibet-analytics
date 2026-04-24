"""
Report de Jogos Play4Tune (P4T) — 3 Rankings
Demanda: Castro/Gestores — entender performance dos jogos da casa.

Reports:
  1. Top jogos por GGR (receita bruta da casa)
  2. Top jogos por Turnover (volume apostado)
  3. Top jogos por Giros (rodadas jogadas)

Cada report inclui detalhamento por jogador (base pequena ~1.4K users).

Fonte confiavel: casino_game_metrics / casino_user_game_metrics
(NAO usar tabela bets direto — GGR da errado, validado 09/04/2026)

Moeda: PKR (Rupia Paquistanesa)
Banco: supernova_bet (PostgreSQL 15.14)
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.supernova_bet import get_supernova_bet_connection


def run_report():
    print("Conectando ao Super Nova Bet DB...")
    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    output = []

    def log(msg=""):
        print(msg)
        output.append(msg)

    # =============================================
    # 0. SANITY CHECK — validar dados antes de tudo
    # =============================================
    cur.execute("SELECT COUNT(*) FROM casino_game_metrics")
    total_metrics = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM casino_user_game_metrics")
    total_user_metrics = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM casino_games WHERE active = true")
    total_games_ativos = cur.fetchone()[0]

    cur.execute("""
        SELECT
            MIN(date) AS primeiro_dia,
            MAX(date) AS ultimo_dia,
            COUNT(DISTINCT date) AS dias,
            ROUND(SUM(net_revenue)::numeric, 2) AS ggr_total,
            ROUND(SUM(total_bet_amount)::numeric, 2) AS turnover_total,
            SUM(played_rounds) AS giros_total
        FROM casino_game_metrics
    """)
    sanity = cur.fetchone()

    log("=" * 80)
    log(f"REPORT JOGOS PLAY4TUNE (P4T)")
    log(f"Data: {ts} | Moeda: PKR (Rupia Paquistanesa)")
    log(f"Fonte: casino_game_metrics + casino_user_game_metrics (supernova_bet)")
    log("=" * 80)
    log(f"\nSanidade: {total_metrics} registros game_metrics | "
        f"{total_user_metrics} registros user_game_metrics | "
        f"{total_games_ativos} jogos ativos")
    log(f"Periodo: {sanity[0]} a {sanity[1]} ({sanity[2]} dias)")
    log(f"Totais: GGR Rs {sanity[3]:,.2f} | Turnover Rs {sanity[4]:,.2f} | "
        f"Giros {sanity[5]:,}")

    # =============================================
    # 1. REPORT 1 — TOP JOGOS POR GGR
    # =============================================
    log(f"\n{'=' * 80}")
    log(f"REPORT 1 — TOP JOGOS POR GGR (Receita Bruta da Casa)")
    log(f"{'=' * 80}")
    log(f"GGR = Apostado - Pago ao jogador. Positivo = casa ganhou. Negativo = casa perdeu.")
    log("")

    cur.execute("""
        SELECT
            g.name AS jogo,
            g.rtp AS rtp_catalogo,
            ROUND(SUM(m.total_bet_amount)::numeric, 2) AS turnover,
            ROUND(SUM(m.total_win_amount)::numeric, 2) AS pago,
            ROUND(SUM(m.net_revenue)::numeric, 2) AS ggr,
            SUM(m.played_rounds) AS giros,
            COUNT(DISTINCT m.date) AS dias_ativo,
            CASE WHEN SUM(m.total_bet_amount) > 0
                 THEN ROUND((SUM(m.net_revenue) / SUM(m.total_bet_amount) * 100)::numeric, 2)
                 ELSE 0 END AS hold_pct,
            CASE WHEN SUM(m.total_bet_amount) > 0
                 THEN ROUND(((1 - SUM(m.total_win_amount) / SUM(m.total_bet_amount)) * 100)::numeric, 2)
                 ELSE 0 END AS rtp_real
        FROM casino_game_metrics m
        JOIN casino_games g ON g.id = m.game_id
        GROUP BY g.name, g.rtp
        HAVING SUM(m.total_bet_amount) > 0
        ORDER BY SUM(m.net_revenue) DESC
    """)
    jogos_ggr = cur.fetchall()

    log(f"{'#':>3} {'Jogo':<25} {'GGR (Rs)':>12} {'Turnover':>12} {'Giros':>8} "
        f"{'Hold%':>7} {'RTP Cat':>7} {'RTP Real':>8} {'Dias':>5}")
    log(f"{'-' * 95}")

    for i, j in enumerate(jogos_ggr, 1):
        rtp_cat = f"{j[1]:.1f}%" if j[1] else "n/a"
        # RTP real = 100 - hold%
        rtp_real = f"{100 - float(j[7]):.1f}%"
        log(f"{i:>3} {j[0]:<25} {j[4]:>12,.2f} {j[2]:>12,.2f} {j[5]:>8,} "
            f"{j[7]:>6.2f}% {rtp_cat:>7} {rtp_real:>8} {j[6]:>5}")

    # Totais
    total_ggr = sum(j[4] for j in jogos_ggr)
    total_turn = sum(j[2] for j in jogos_ggr)
    total_giros = sum(j[5] for j in jogos_ggr)
    log(f"{'-' * 95}")
    log(f"    {'TOTAL':<25} {total_ggr:>12,.2f} {total_turn:>12,.2f} {total_giros:>8,}")

    # Jogos com GGR negativo (casa perdeu)
    negativos = [j for j in jogos_ggr if j[4] < 0]
    if negativos:
        log(f"\n  ATENCAO: {len(negativos)} jogo(s) com GGR negativo (casa PERDEU dinheiro):")
        for j in negativos:
            log(f"    - {j[0]}: Rs {j[4]:,.2f} (turnover Rs {j[2]:,.2f}, {j[5]:,} giros)")

    # Detalhamento por jogador — top 5 jogos por GGR
    log(f"\n--- Detalhamento por jogador (top 5 jogos por GGR) ---")
    top5_nomes_ggr = [j[0] for j in jogos_ggr[:5]]

    cur.execute("""
        SELECT
            g.name AS jogo,
            u.username,
            u.public_id,
            ROUND(SUM(um.total_bet_amount)::numeric, 2) AS turnover,
            ROUND(SUM(um.total_win_amount)::numeric, 2) AS pago,
            ROUND(SUM(um.net_revenue)::numeric, 2) AS ggr,
            SUM(um.played_rounds) AS giros,
            COUNT(DISTINCT um.date) AS dias
        FROM casino_user_game_metrics um
        JOIN casino_games g ON g.id = um.game_id
        JOIN users u ON u.id = um.user_id
        WHERE g.name = ANY(%s)
        GROUP BY g.name, u.username, u.public_id
        ORDER BY g.name, SUM(um.net_revenue) DESC
    """, (top5_nomes_ggr,))
    detalhe_ggr = cur.fetchall()

    jogo_atual = None
    for d in detalhe_ggr:
        if d[0] != jogo_atual:
            jogo_atual = d[0]
            log(f"\n  {jogo_atual}:")
            log(f"    {'Username':<18} {'PID':<10} {'GGR (Rs)':>12} {'Turnover':>12} "
                f"{'Giros':>8} {'Dias':>5}")
            log(f"    {'-' * 70}")
        log(f"    {d[1]:<18} {d[2]:<10} {d[5]:>12,.2f} {d[3]:>12,.2f} "
            f"{d[6]:>8,} {d[7]:>5}")

    # =============================================
    # 2. REPORT 2 — TOP JOGOS POR TURNOVER
    # =============================================
    log(f"\n{'=' * 80}")
    log(f"REPORT 2 — TOP JOGOS POR TURNOVER (Volume Apostado)")
    log(f"{'=' * 80}")
    log(f"Turnover = total apostado pelos jogadores. Quanto maior, mais o jogo engaja.")
    log("")

    # Reordenar por turnover
    jogos_turnover = sorted(jogos_ggr, key=lambda x: x[2], reverse=True)

    log(f"{'#':>3} {'Jogo':<25} {'Turnover (Rs)':>14} {'GGR':>12} {'Giros':>8} "
        f"{'Hold%':>7} {'%Total':>7} {'Dias':>5}")
    log(f"{'-' * 87}")

    for i, j in enumerate(jogos_turnover, 1):
        pct_total = j[2] / total_turn * 100 if total_turn > 0 else 0
        log(f"{i:>3} {j[0]:<25} {j[2]:>14,.2f} {j[4]:>12,.2f} {j[5]:>8,} "
            f"{j[7]:>6.2f}% {pct_total:>6.1f}% {j[6]:>5}")

    log(f"{'-' * 87}")
    log(f"    {'TOTAL':<25} {total_turn:>14,.2f} {total_ggr:>12,.2f} {total_giros:>8,}")

    # Concentracao — top 3 jogos representam quanto?
    top3_turn = sum(j[2] for j in jogos_turnover[:3])
    pct_top3 = top3_turn / total_turn * 100 if total_turn > 0 else 0
    log(f"\n  Concentracao: top 3 jogos = {pct_top3:.1f}% do turnover total")

    # Detalhamento por jogador — top 5 jogos por turnover
    log(f"\n--- Detalhamento por jogador (top 5 jogos por Turnover) ---")
    top5_nomes_turn = [j[0] for j in jogos_turnover[:5]]

    cur.execute("""
        SELECT
            g.name AS jogo,
            u.username,
            u.public_id,
            ROUND(SUM(um.total_bet_amount)::numeric, 2) AS turnover,
            ROUND(SUM(um.net_revenue)::numeric, 2) AS ggr,
            SUM(um.played_rounds) AS giros,
            COUNT(DISTINCT um.date) AS dias,
            ROUND((SUM(um.total_bet_amount) / NULLIF(SUM(um.played_rounds), 0))::numeric, 2) AS ticket_medio
        FROM casino_user_game_metrics um
        JOIN casino_games g ON g.id = um.game_id
        JOIN users u ON u.id = um.user_id
        WHERE g.name = ANY(%s)
        GROUP BY g.name, u.username, u.public_id
        ORDER BY g.name, SUM(um.total_bet_amount) DESC
    """, (top5_nomes_turn,))
    detalhe_turn = cur.fetchall()

    jogo_atual = None
    for d in detalhe_turn:
        if d[0] != jogo_atual:
            jogo_atual = d[0]
            log(f"\n  {jogo_atual}:")
            log(f"    {'Username':<18} {'PID':<10} {'Turnover (Rs)':>14} {'GGR':>12} "
                f"{'Giros':>8} {'Ticket':>8} {'Dias':>5}")
            log(f"    {'-' * 80}")
        ticket = f"{d[7]:,.2f}" if d[7] else "n/a"
        log(f"    {d[1]:<18} {d[2]:<10} {d[3]:>14,.2f} {d[4]:>12,.2f} "
            f"{d[5]:>8,} {ticket:>8} {d[6]:>5}")

    # =============================================
    # 3. REPORT 3 — TOP JOGOS POR GIROS (RODADAS)
    # =============================================
    log(f"\n{'=' * 80}")
    log(f"REPORT 3 — TOP JOGOS POR GIROS (Rodadas Jogadas)")
    log(f"{'=' * 80}")
    log(f"Giros = total de rodadas jogadas. Indica engajamento e popularidade do jogo.")
    log("")

    # Reordenar por giros
    jogos_giros = sorted(jogos_ggr, key=lambda x: x[5], reverse=True)

    log(f"{'#':>3} {'Jogo':<25} {'Giros':>10} {'Turnover':>12} {'GGR':>12} "
        f"{'Ticket Med':>10} {'%Total':>7} {'Dias':>5}")
    log(f"{'-' * 90}")

    for i, j in enumerate(jogos_giros, 1):
        pct_total = j[5] / total_giros * 100 if total_giros > 0 else 0
        ticket = j[2] / j[5] if j[5] > 0 else 0
        log(f"{i:>3} {j[0]:<25} {j[5]:>10,} {j[2]:>12,.2f} {j[4]:>12,.2f} "
            f"{ticket:>10,.2f} {pct_total:>6.1f}% {j[6]:>5}")

    log(f"{'-' * 90}")
    log(f"    {'TOTAL':<25} {total_giros:>10,} {total_turn:>12,.2f} {total_ggr:>12,.2f}")

    # Concentracao — top 3 jogos
    top3_giros = sum(j[5] for j in jogos_giros[:3])
    pct_top3_g = top3_giros / total_giros * 100 if total_giros > 0 else 0
    log(f"\n  Concentracao: top 3 jogos = {pct_top3_g:.1f}% dos giros totais")

    # Detalhamento por jogador — top 5 jogos por giros
    log(f"\n--- Detalhamento por jogador (top 5 jogos por Giros) ---")
    top5_nomes_giros = [j[0] for j in jogos_giros[:5]]

    cur.execute("""
        SELECT
            g.name AS jogo,
            u.username,
            u.public_id,
            SUM(um.played_rounds) AS giros,
            ROUND(SUM(um.total_bet_amount)::numeric, 2) AS turnover,
            ROUND(SUM(um.net_revenue)::numeric, 2) AS ggr,
            COUNT(DISTINCT um.date) AS dias,
            ROUND((SUM(um.total_bet_amount) / NULLIF(SUM(um.played_rounds), 0))::numeric, 2) AS ticket_medio
        FROM casino_user_game_metrics um
        JOIN casino_games g ON g.id = um.game_id
        JOIN users u ON u.id = um.user_id
        WHERE g.name = ANY(%s)
        GROUP BY g.name, u.username, u.public_id
        ORDER BY g.name, SUM(um.played_rounds) DESC
    """, (top5_nomes_giros,))
    detalhe_giros = cur.fetchall()

    jogo_atual = None
    for d in detalhe_giros:
        if d[0] != jogo_atual:
            jogo_atual = d[0]
            log(f"\n  {jogo_atual}:")
            log(f"    {'Username':<18} {'PID':<10} {'Giros':>10} {'Turnover':>12} "
                f"{'GGR':>12} {'Ticket':>8} {'Dias':>5}")
            log(f"    {'-' * 80}")
        ticket = f"{d[7]:,.2f}" if d[7] else "n/a"
        log(f"    {d[1]:<18} {d[2]:<10} {d[3]:>10,} {d[4]:>12,.2f} "
            f"{d[5]:>12,.2f} {ticket:>8} {d[6]:>5}")

    # =============================================
    # 4. ANALISE CRUZADA — Jogos x Jogadores (visao executiva)
    # =============================================
    log(f"\n{'=' * 80}")
    log(f"ANALISE EXECUTIVA — CONCENTRACAO E RISCOS")
    log(f"{'=' * 80}")

    # Top jogadores overall
    cur.execute("""
        SELECT
            u.username,
            u.public_id,
            u.phone,
            ROUND(SUM(um.total_bet_amount)::numeric, 2) AS turnover,
            ROUND(SUM(um.net_revenue)::numeric, 2) AS ggr,
            SUM(um.played_rounds) AS giros,
            COUNT(DISTINCT um.game_id) AS jogos_distintos,
            COUNT(DISTINCT um.date) AS dias_ativo,
            ROUND((SUM(um.total_bet_amount) / NULLIF(SUM(um.played_rounds), 0))::numeric, 2) AS ticket_medio
        FROM casino_user_game_metrics um
        JOIN users u ON u.id = um.user_id
        GROUP BY u.username, u.public_id, u.phone
        ORDER BY SUM(um.total_bet_amount) DESC
        LIMIT 15
    """)
    top_players = cur.fetchall()

    log(f"\nTop 15 jogadores por Turnover (all-time):")
    log(f"{'#':>3} {'Username':<18} {'PID':<10} {'Turnover (Rs)':>14} {'GGR':>12} "
        f"{'Giros':>8} {'Jogos':>6} {'Dias':>5} {'Ticket':>8}")
    log(f"{'-' * 92}")

    for i, p in enumerate(top_players, 1):
        ticket = f"{p[8]:,.2f}" if p[8] else "n/a"
        log(f"{i:>3} {p[0]:<18} {p[1]:<10} {p[3]:>14,.2f} {p[4]:>12,.2f} "
            f"{p[5]:>8,} {p[6]:>6} {p[7]:>5} {ticket:>8}")

    # Concentracao de turnover
    if top_players and total_turn > 0:
        top1_pct = top_players[0][3] / total_turn * 100
        top3_pct = sum(p[3] for p in top_players[:3]) / total_turn * 100
        top5_pct = sum(p[3] for p in top_players[:5]) / total_turn * 100

        log(f"\n  Concentracao de Turnover:")
        log(f"    Top 1 jogador: {top1_pct:.1f}% do total")
        log(f"    Top 3 jogadores: {top3_pct:.1f}% do total")
        log(f"    Top 5 jogadores: {top5_pct:.1f}% do total")

        if top1_pct > 30:
            log(f"\n  ALERTA: {top_players[0][0]} representa {top1_pct:.1f}% do turnover total.")
            log(f"  Risco altissimo de concentracao — se este jogador parar, a operacao sofre.")

    # Jogos que so 1 jogador usa (risco de dependencia)
    cur.execute("""
        SELECT
            g.name,
            COUNT(DISTINCT um.user_id) AS jogadores,
            ROUND(SUM(um.total_bet_amount)::numeric, 2) AS turnover,
            ROUND(SUM(um.net_revenue)::numeric, 2) AS ggr
        FROM casino_user_game_metrics um
        JOIN casino_games g ON g.id = um.game_id
        GROUP BY g.name
        HAVING SUM(um.total_bet_amount) > 1000
        ORDER BY COUNT(DISTINCT um.user_id), SUM(um.total_bet_amount) DESC
    """)
    jogos_concentrados = cur.fetchall()

    log(f"\n  Jogos com poucos jogadores (turnover > Rs 1.000):")
    log(f"    {'Jogo':<25} {'Jogadores':>10} {'Turnover':>12} {'GGR':>12}")
    log(f"    {'-' * 62}")
    for jc in jogos_concentrados:
        flag = " <-- RISCO" if jc[1] <= 2 else ""
        log(f"    {jc[0]:<25} {jc[1]:>10} {jc[2]:>12,.2f} {jc[3]:>12,.2f}{flag}")

    # =============================================
    # 5. TENDENCIA ULTIMOS 7 DIAS
    # =============================================
    log(f"\n{'=' * 80}")
    log(f"TENDENCIA DIARIA (ultimos dias disponiveis)")
    log(f"{'=' * 80}")

    cur.execute("""
        SELECT
            m.date,
            ROUND(SUM(m.total_bet_amount)::numeric, 2) AS turnover,
            ROUND(SUM(m.net_revenue)::numeric, 2) AS ggr,
            SUM(m.played_rounds) AS giros,
            COUNT(DISTINCT m.game_id) AS jogos_ativos
        FROM casino_game_metrics m
        GROUP BY m.date
        ORDER BY m.date
    """)
    tendencia = cur.fetchall()

    log(f"\n{'Data':<12} {'Turnover (Rs)':>14} {'GGR (Rs)':>12} {'Giros':>8} {'Jogos':>6}")
    log(f"{'-' * 56}")
    for t in tendencia:
        log(f"{str(t[0]):<12} {t[1]:>14,.2f} {t[2]:>12,.2f} {t[3]:>8,} {t[4]:>6}")

    # =============================================
    # 6. CROSS-REFERENCE COM VIEW OFICIAL
    # =============================================
    log(f"\n{'=' * 80}")
    log(f"CROSS-REFERENCE — casino_game_metrics vs vw_top_jogos_ggr")
    log(f"{'=' * 80}")

    try:
        cur.execute("""
            SELECT game_name, ggr, total_bet, total_win, total_rounds
            FROM vw_top_jogos_ggr
            ORDER BY ggr DESC
            LIMIT 10
        """)
        view_ggr = cur.fetchall()
        if view_ggr:
            log(f"\nvw_top_jogos_ggr (view oficial — referencia cruzada):")
            log(f"{'Jogo':<25} {'GGR':>12} {'Bet':>12} {'Win':>12} {'Rounds':>8}")
            log(f"{'-' * 72}")
            for v in view_ggr:
                log(f"{v[0]:<25} {v[1]:>12,.2f} {v[2]:>12,.2f} {v[3]:>12,.2f} {v[4]:>8,}")
        else:
            log("  View vw_top_jogos_ggr vazia.")
    except Exception as e:
        log(f"  View vw_top_jogos_ggr nao acessivel: {e}")
        # Tentar com colunas diferentes
        try:
            cur.execute("""
                SELECT * FROM vw_top_jogos_ggr LIMIT 1
            """)
            log(f"  Colunas: {[desc[0] for desc in cur.description]}")
        except:
            log("  View nao existe ou sem acesso.")

    # =============================================
    # 7. LEGENDA / GLOSSARIO
    # =============================================
    log(f"\n{'=' * 80}")
    log(f"LEGENDA / GLOSSARIO")
    log(f"{'=' * 80}")
    log(f"""
Colunas dos reports:
- Jogo: nome do jogo no catalogo casino_games
- GGR (Gross Gaming Revenue): Apostado - Pago = receita bruta da casa
  Positivo = casa ganhou. Negativo = casa perdeu naquele jogo.
- Turnover: total apostado pelos jogadores (volume de bets)
- Giros: total de rodadas jogadas (played_rounds)
- Hold%: GGR / Turnover * 100. Margem da casa.
  Esperado saudavel: 2-5% para slots, variavel para crash/multiplier.
- RTP Catalogo: Return to Player configurado pelo provider (2J Games)
- RTP Real: retorno real ao jogador = 100% - Hold%
- Ticket Medio: Turnover / Giros = valor medio por aposta
- Dias: quantidade de dias com atividade naquele jogo
- %Total: participacao percentual no total

Jogadores:
- Username: nome de login do jogador
- PID: Public ID (identificador curto, uso publico)

Interpretacao:
- Jogo com GGR negativo = casa perdeu dinheiro nesse jogo (jogador ganhou mais)
- Jogo com poucos jogadores + alto turnover = risco de concentracao
- Hold% muito alto (>10%) = pode indicar jogo pouco atrativo (jogador perde rapido)
- Hold% negativo = casa perdeu (evento normal em jogos high-stake/crash)

Moeda: PKR (Rupia Paquistanesa)
Fonte: casino_game_metrics, casino_user_game_metrics, casino_games, users
Banco: supernova_bet (PostgreSQL 15.14)
Periodo: desde o inicio da operacao ate a data do report
""")

    # =============================================
    # SALVAR
    # =============================================
    os.makedirs("reports", exist_ok=True)
    report_date = datetime.now().strftime("%d%m%Y")
    report_path = f"reports/report_jogos_play4tune_{report_date}.txt"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(output))
    log(f"\nReport salvo: {report_path}")

    cur.close()
    conn.close()
    tunnel.stop()
    print("Conexao encerrada.")


if __name__ == "__main__":
    run_report()
