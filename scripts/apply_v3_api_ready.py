"""
Aplica v3 (shape da categories-api pronto):
  1. ddl_game_image_mapping_v3.sql → +2 cols + view vw_front_api_games
  2. Roda pipeline para popular total_bet_24h/total_wins_24h
  3. Smoke test da view vw_front_api_games com shape GameResponseDto
"""
import sys, os, logging, subprocess
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova, get_supernova_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def aplicar_sql_arquivo(path):
    log.info(f"Aplicando: {os.path.basename(path)}")
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        log.info("  OK")
    finally:
        conn.close(); ssh.close()


def smoke_test():
    log.info("=== SMOKE TEST vw_front_api_games ===")

    # 1. Shape da view (colunas)
    rows = execute_supernova("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='multibet' AND table_name='vw_front_api_games'
        ORDER BY ordinal_position
    """, fetch=True)
    log.info(f"vw_front_api_games: {len(rows)} colunas (shape GameResponseDto)")
    for c, t in rows:
        log.info(f"  {c:<25} {t}")

    # 2. Top 10 mais jogados com totalBet e totalWins populados
    rows = execute_supernova("""
        SELECT "gameId", "name", "provider", "category",
               "totalBets", "uniquePlayers", "totalBet", "totalWins", "rank"
        FROM multibet.vw_front_api_games
        WHERE "rank" IS NOT NULL
        ORDER BY "rank"
        LIMIT 10
    """, fetch=True)
    log.info("\nTop 10 (shape GameResponseDto):")
    for r in rows:
        gid, nm, pv, ct, tb, up, tbet, tw, rk = r
        log.info(f"  #{rk:<3} {nm[:22]:<22} {pv[:14]:<14} {(ct or ''):<6} "
                 f"bets={tb:>6} | bet=R$ {tbet:>10.2f} | wins=R$ {tw:>10.2f}")

    # 3. Validacao totalBet e totalWins — quantos jogos ja tem valor?
    rows = execute_supernova("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN "totalBet" > 0 THEN 1 ELSE 0 END) AS com_bet,
            SUM(CASE WHEN "totalWins" > 0 THEN 1 ELSE 0 END) AS com_wins,
            ROUND(SUM("totalBet")::numeric, 2) AS turnover_total,
            ROUND(SUM("totalWins")::numeric, 2) AS wins_total
        FROM multibet.vw_front_api_games
    """, fetch=True)
    t, cb, cw, tt, wt = rows[0]
    log.info(f"\nView shape GameResponseDto:")
    log.info(f"  Total jogos:       {t}")
    log.info(f"  Com totalBet>0:    {cb}")
    log.info(f"  Com totalWins>0:   {cw}")
    log.info(f"  Turnover total:    R$ {tt:,}")
    log.info(f"  Wins total:        R$ {wt:,}")

    # 4. Gus precisa substituir no GameCachedRepository: mostra SQL equivalente
    log.info("\n--- SQL equivalente que o GameCachedRepository precisa rodar ---")
    log.info("// findMostPlayed:")
    log.info("   SELECT * FROM multibet.vw_front_api_games WHERE \"rank\" IS NOT NULL ORDER BY \"rank\" LIMIT $1 OFFSET $2;")
    log.info("// findMostPaid:")
    log.info("   SELECT * FROM multibet.vw_front_api_games WHERE \"totalWins\" > 0 ORDER BY \"totalWins\" DESC LIMIT $1;")
    log.info("// findByCategory:")
    log.info("   SELECT * FROM multibet.vw_front_api_games WHERE LOWER(\"category\") = LOWER($1);")
    log.info("// findByProvider:")
    log.info("   SELECT * FROM multibet.vw_front_api_games WHERE LOWER(\"provider\") = LOWER($1);")
    log.info("// findLive:")
    log.info("   SELECT * FROM multibet.vw_front_api_games WHERE \"category\" = 'live';")
    log.info("// findFortune:")
    log.info("   SELECT * FROM multibet.vw_front_api_games WHERE \"name\" ILIKE '%fortune%' OR \"name\" ILIKE '%fortuna%';")


def main():
    # 1. DDL v3
    aplicar_sql_arquivo(os.path.join(PROJECT_ROOT, "pipelines", "ddl", "ddl_game_image_mapping_v3.sql"))

    # 2. Pipeline (popular total_bet_24h/total_wins_24h)
    log.info("Rodando game_image_mapper.py para popular total_bet_24h/total_wins_24h...")
    res = subprocess.run(
        [sys.executable, os.path.join(PROJECT_ROOT, "pipelines", "game_image_mapper.py")],
        cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=600
    )
    print(res.stdout[-1500:] if len(res.stdout) > 1500 else res.stdout)
    if res.returncode != 0:
        log.error(f"Pipeline falhou:\n{res.stderr[-1500:]}")
        sys.exit(1)

    # 3. Smoke test
    smoke_test()
    log.info("=== v3 Apply concluido ===")


if __name__ == "__main__":
    main()
