"""
Variante D-0: Top jogadores afundando GGR HOJE (20/04) ate o momento atual.

AVISO: dados PARCIAIS — casino_user_game_metrics esta sendo agregada em tempo quase-real.
Script rodado em 20/04 apos 13h BRT. Dia nao fechou.

Mantem a mesma logica/filtro do script principal (UNION heuristica + dev + whitelist DP/SQ).
"""

import os
import sys
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Reusa modulos do script principal
import report_players_afundando_ggr_play4 as mainmod
from report_players_afundando_ggr_play4 import (
    get_test_user_ids, fetch_fx_rates,
    SQL_SANITY, SQL_TOP_PLAYERS, SQL_JOGOS_DO_PLAYER,
    fmt_brl, fmt_pkr, fmt_brl_pkr, fmt_int,
    gerar_excel,
)
from db.supernova_bet import get_supernova_bet_connection

DATA_INICIO = date(2026, 4, 20)
DATA_FIM    = date(2026, 4, 20)
TOP_N       = 20

OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
XLSX_PATH = os.path.join(OUT_DIR, "play4_players_afundando_ggr_20abr_D0_PARCIAL.xlsx")


def run():
    print("=" * 80)
    print("PLAY4TUNE — D-0 PARCIAL (20/04 ate agora)")
    print(f"Rodado em: {datetime.now().strftime('%d/%m/%Y %H:%M')} BRT")
    print("=" * 80)

    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    try:
        # Taxa PKR->BRL via triangulacao USD (banco)
        pkr_to_brl, fx_at = fetch_fx_rates(cur)
        mainmod.PKR_TO_BRL = pkr_to_brl
        mainmod.FX_FETCHED_AT = fx_at
        print(f"[FX] 1 PKR = R$ {pkr_to_brl:.6f} (snapshot {fx_at} UTC)")

        test_ids, n_test, whitelisted = get_test_user_ids(cur)
        print(f"[Filtro] {n_test} contas teste excluidas | {len(whitelisted)} whitelist DP/SQ")

        cur.execute(SQL_SANITY, (DATA_INICIO, DATA_FIM, test_ids))
        s = cur.fetchone()
        print(f"\n[D-0 20/04] Ativos: {s[3]}")
        print(f"  Turnover: {fmt_brl_pkr(s[4])}")
        print(f"  Ganho:    {fmt_brl_pkr(s[5])}")
        print(f"  GGR casa: {fmt_brl_pkr(s[6])}")

        # Ultima bet do dia (timestamp do corte)
        cur.execute("SELECT MAX(created_at) FROM bets WHERE DATE(created_at) = %s", (DATA_FIM,))
        ultima_bet = cur.fetchone()[0]
        print(f"[Corte] Ultima bet: {ultima_bet}")

        cur.execute(SQL_TOP_PLAYERS, (DATA_FIM, DATA_INICIO, DATA_FIM, test_ids, TOP_N))
        top_players = cur.fetchall()
        print(f"\n[Top {TOP_N}] Jogadores com GGR negativo HOJE:")

        players_detail = []
        for i, p in enumerate(top_players, 1):
            username, pid, phone, cadastro, dias_conta, apostado, ganho, ggr, \
                rodadas, jogos_u, dias_j, payout = p
            print(f"  {i:>2}. PID {pid} | {dias_conta}d | "
                  f"Apostou {fmt_brl_pkr(apostado)} | GGR {fmt_brl_pkr(ggr)} | "
                  f"Payout {payout}%")

            cur.execute(SQL_JOGOS_DO_PLAYER, (DATA_INICIO, DATA_FIM, username))
            jogos = cur.fetchall()
            players_detail.append({
                "rank": i, "username": username, "public_id": pid, "phone": phone,
                "cadastro": cadastro, "dias_conta": dias_conta,
                "apostado_pkr": float(apostado or 0), "ganho_pkr": float(ganho or 0),
                "ggr_pkr": float(ggr or 0), "rodadas": int(rodadas or 0),
                "jogos_unicos": int(jogos_u or 0), "dias_jogou": int(dias_j or 0),
                "payout_pct": float(payout or 0), "jogos": jogos,
            })

        # Reutiliza o Excel do script principal, mas salva em path D-0
        mainmod.XLSX_PATH = XLSX_PATH
        mainmod.DATA_INICIO = DATA_INICIO
        mainmod.DATA_FIM    = DATA_FIM
        mainmod.gerar_excel(players_detail, s)
        print(f"\n[Excel] Salvo em: {XLSX_PATH}")

        # Retorna dados pro formatter de chat
        return s, players_detail, ultima_bet

    finally:
        cur.close()
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    run()
    print("\nCONCLUIDO")
