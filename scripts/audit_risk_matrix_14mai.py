"""
Spot-check empirico da Matriz de Risco 14/05/2026.

Faz 4 analises:
  1. 10 players random por tier (61 jogadores totais nos 6 tiers)
  2. Top 20 players com MAIOR numero de tags simultaneas (overlap)
  3. Delta tag-a-tag 14/05 vs 13/05 (variacao % e absoluta)
  4. Tags conflitantes (combinacoes que NAO deveriam co-ocorrer)

Output: reports/audit_risk_matrix_2026-05-14.csv + console summary
"""
import sys
import os
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection

HOJE = date(2026, 5, 14)
ONTEM = date(2026, 5, 13)
OUT_DIR = "reports"
os.makedirs(OUT_DIR, exist_ok=True)

# Tags que NAO deveriam co-ocorrer (regra de negocio) — lowercase pq PG salva assim
CONFLITOS = [
    ("vip_whale_player",      "rg_alert_player"),       # VIP nao deve ter alerta RG
    ("zero_risk_player",      "potencial_abuser"),      # zero risco NAO eh abuser
    ("zero_risk_player",      "fast_cashout"),          # zero risco nao saca rapido
    ("promo_only",            "non_promo_player"),      # so promo vs nao promo
    ("promo_only",            "vip_whale_player"),      # so promo nao eh whale
    ("sleeper_low_player",    "engaged_player"),        # dormindo vs ativo
    ("sleeper_low_player",    "vip_whale_player"),      # dormindo nao eh whale
    ("winback_hi_val_player", "engaged_player"),        # winback eh para inativo
    ("cashout_and_run",       "sustained_player"),      # opostos (statistician flagou)
    ("potencial_abuser",      "engaged_player"),        # signup<2d nao deve ter 90d
]


def main() -> int:
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # 1. Descobrir todas as colunas de tags
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='multibet' AND table_name='risk_tags'
                ORDER BY ordinal_position
                """
            )
            cols = [r[0] for r in cur.fetchall()]
            print(f"\n[i] Colunas em multibet.risk_tags: {len(cols)}")

            # PostgreSQL lower-case por padrao. Tag = qualquer coluna nao-meta.
            META = {
                "user_id", "user_ext_id", "snapshot_date", "score_bruto",
                "score_norm", "tier", "external_id", "ecr_id",
                "created_at", "updated_at", "ecr_user_id"
            }
            tags = [c for c in cols if c not in META]
            print(f"[i] Tags detectadas: {len(tags)}")
            print(f"    {tags}")

            # 2. Quantos tiers existem
            cur.execute(
                "SELECT tier, COUNT(*) FROM multibet.risk_tags "
                "WHERE snapshot_date=%s GROUP BY tier ORDER BY 2 DESC",
                (HOJE,),
            )
            tiers = cur.fetchall()
            print(f"\n[i] Tiers (hoje):")
            total = 0
            for t, c in tiers:
                print(f"    {t:15s}: {c:>8,}")
                total += c
            print(f"    {'TOTAL':15s}: {total:>8,}")

            # 3. SPOT-CHECK: 10 players random por tier
            print(f"\n{'='*70}")
            print(f"SPOT-CHECK: 10 players random por tier")
            print(f"{'='*70}")
            tag_cols_sql = ", ".join([f'"{t}"' for t in tags])
            for tier_name, _ in tiers:
                cur.execute(
                    f"""
                    SELECT user_ext_id, score_bruto, score_norm, tier,
                           {tag_cols_sql}
                    FROM multibet.risk_tags
                    WHERE snapshot_date=%s AND tier=%s
                    ORDER BY random()
                    LIMIT 10
                    """,
                    (HOJE, tier_name),
                )
                rows = cur.fetchall()
                print(f"\n--- Tier: {tier_name} ({len(rows)} samples) ---")
                for r in rows:
                    user_ext, sb, sn, tr = r[0], r[1], r[2], r[3]
                    tag_vals = r[4:]
                    tags_ativas = [
                        t for t, v in zip(tags, tag_vals) if v in (1, True)
                    ]
                    print(
                        f"  ext={user_ext}  bruto={sb}  norm={sn}  tags="
                        f"{','.join(tags_ativas) or 'NENHUMA'}"
                    )

            # 4. TOP 20 players com mais tags simultaneas
            tag_sum_sql = " + ".join([f'COALESCE("{t}",0)' for t in tags])
            cur.execute(
                f"""
                SELECT user_ext_id, tier, score_norm,
                       ({tag_sum_sql}) AS n_tags
                FROM multibet.risk_tags
                WHERE snapshot_date=%s
                ORDER BY n_tags DESC, score_norm DESC
                LIMIT 20
                """,
                (HOJE,),
            )
            print(f"\n{'='*70}")
            print(f"TOP 20 OVERLAP — jogadores com mais tags simultaneas")
            print(f"{'='*70}")
            for r in cur.fetchall():
                print(
                    f"  ext={r[0]}  tier={r[1]:15s}  norm={r[2]:>5}  "
                    f"n_tags={r[3]:>3}"
                )

            # Dist do n_tags
            cur.execute(
                f"""
                WITH t AS (
                  SELECT ({tag_sum_sql}) AS n_tags
                  FROM multibet.risk_tags
                  WHERE snapshot_date=%s
                )
                SELECT n_tags, COUNT(*) FROM t GROUP BY n_tags ORDER BY 1
                """,
                (HOJE,),
            )
            print(f"\n[i] Distribuicao de # tags por player:")
            for r in cur.fetchall():
                print(f"    {r[0]:>3} tags: {r[1]:>8,}")

            # 5. Delta tag-a-tag 14/05 vs 13/05
            print(f"\n{'='*70}")
            print(f"DELTA TAG-A-TAG: 14/05 vs 13/05")
            print(f"{'='*70}")
            print(f"  {'tag':30s} {'13/05':>10} {'14/05':>10} {'delta':>10} {'delta%':>8}")
            for tag in tags:
                cur.execute(
                    f'SELECT SUM(CASE WHEN "{tag}"=1 THEN 1 ELSE 0 END) '
                    f"FROM multibet.risk_tags WHERE snapshot_date=%s",
                    (ONTEM,),
                )
                n13 = cur.fetchone()[0] or 0
                cur.execute(
                    f'SELECT SUM(CASE WHEN "{tag}"=1 THEN 1 ELSE 0 END) '
                    f"FROM multibet.risk_tags WHERE snapshot_date=%s",
                    (HOJE,),
                )
                n14 = cur.fetchone()[0] or 0
                delta = n14 - n13
                pct = (delta / n13 * 100) if n13 else 0
                flag = "  <-- ALERTA" if abs(pct) > 30 else ""
                print(f"  {tag:30s} {n13:>10,} {n14:>10,} {delta:>+10,} {pct:>+7.1f}%{flag}")

            # 6. Tags conflitantes
            print(f"\n{'='*70}")
            print(f"TAGS CONFLITANTES (combinacoes proibidas por regra de negocio)")
            print(f"{'='*70}")
            for ta, tb in CONFLITOS:
                if ta not in tags or tb not in tags:
                    print(f"  SKIP: {ta} ou {tb} nao existe na tabela")
                    continue
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM multibet.risk_tags
                    WHERE snapshot_date=%s
                      AND "{ta}"=1 AND "{tb}"=1
                    """,
                    (HOJE,),
                )
                n = cur.fetchone()[0]
                flag = "  <-- ALERTA" if n > 0 else "  OK"
                print(f"  {ta:25s} + {tb:25s}: {n:>6,} players{flag}")

            print(f"\n{'='*70}")
            print("AUDITORIA EMPIRICA CONCLUIDA")
            print(f"{'='*70}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            tunnel.stop()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
