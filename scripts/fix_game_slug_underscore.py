"""
Fix: Atualizar game_slug de hifens (-) para underscores (_)
=============================================================
Pedido do front-end: slugs devem usar _ ao inves de -
Ex: /pb/gameplay/fortune-ox/real-game → /pb/gameplay/fortune_ox/real-game

Atualiza nas tabelas:
  1. multibet.game_image_mapping (fonte)
  2. multibet.grandes_ganhos (destino)

Execucao:
    python scripts/fix_game_slug_underscore.py
"""

import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova, get_supernova_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# SQL: troca hifens por underscores APENAS na parte do nome do jogo
# Formato: /pb/gameplay/{slug}/real-game
# Estrategia: extrair o slug do meio, fazer REPLACE, reconstruir
UPDATE_GAME_IMAGE_MAPPING = """
UPDATE multibet.game_image_mapping
SET game_slug = '/pb/gameplay/' ||
    REPLACE(
        SPLIT_PART(REPLACE(game_slug, '/pb/gameplay/', ''), '/real-game', 1),
        '-', '_'
    ) || '/real-game',
    updated_at = NOW()
WHERE game_slug IS NOT NULL
  AND game_slug LIKE '/pb/gameplay/%/real-game'
  AND game_slug LIKE '%-%'
"""

UPDATE_GRANDES_GANHOS = """
UPDATE multibet.grandes_ganhos
SET game_slug = '/pb/gameplay/' ||
    REPLACE(
        SPLIT_PART(REPLACE(game_slug, '/pb/gameplay/', ''), '/real-game', 1),
        '-', '_'
    ) || '/real-game'
WHERE game_slug IS NOT NULL
  AND game_slug LIKE '/pb/gameplay/%/real-game'
  AND game_slug LIKE '%-%'
"""


def main():
    log.info("=== Fix game_slug: hifens → underscores ===")

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # 1. Verifica antes
            cur.execute("""
                SELECT game_slug FROM multibet.game_image_mapping
                WHERE game_slug LIKE '%-%'
                  AND game_slug LIKE '/pb/gameplay/%/real-game'
                LIMIT 5
            """)
            antes = cur.fetchall()
            log.info(f"Exemplos ANTES (game_image_mapping): {[r[0] for r in antes]}")

            # 2. Atualiza game_image_mapping
            cur.execute(UPDATE_GAME_IMAGE_MAPPING)
            rows_mapping = cur.rowcount
            log.info(f"game_image_mapping: {rows_mapping} registros atualizados")

            # 3. Atualiza grandes_ganhos
            cur.execute(UPDATE_GRANDES_GANHOS)
            rows_ganhos = cur.rowcount
            log.info(f"grandes_ganhos: {rows_ganhos} registros atualizados")

            # 4. Verifica depois
            cur.execute("""
                SELECT game_slug FROM multibet.game_image_mapping
                LIMIT 5
            """)
            depois = cur.fetchall()
            log.info(f"Exemplos DEPOIS (game_image_mapping): {[r[0] for r in depois]}")

        conn.commit()
        log.info("Commit OK.")

    finally:
        conn.close()
        ssh.close()

    log.info(f"Total: {rows_mapping} slugs atualizados em game_image_mapping, "
             f"{rows_ganhos} em grandes_ganhos")
    log.info("=== Concluido ===")


if __name__ == "__main__":
    main()
