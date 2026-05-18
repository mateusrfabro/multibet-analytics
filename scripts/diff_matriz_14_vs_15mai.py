"""
Diff entre snapshot 2026-05-14 (ANTES — 21 tags) vs 2026-05-15 (DEPOIS — 22 tags).

Output:
  1) Distribuicao de tier ANTES vs DEPOIS
  2) Quantos players ganharam a tag cancel_heavy_daily
  3) Quem mudou de tier (Bom -> Mediano, etc)
  4) CSV com tier_changes pro Castrin auditar
  5) CSV com lista dos players novos flagados (cancel_heavy_daily=1)

NAO altera nada. Apenas leitura.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
from db.supernova import get_supernova_connection

DIA_ANTES = "2026-05-14"
DIA_DEPOIS = "2026-05-18"
OUT_DIR = "output"
os.makedirs(OUT_DIR, exist_ok=True)


def main():
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # 1) Confirma que os 2 snapshots existem
            print("=" * 80)
            print(f"DIFF MATRIZ DE RISCO: {DIA_ANTES} (antes) vs {DIA_DEPOIS} (depois)")
            print("=" * 80)

            cur.execute("""
                SELECT snapshot_date, COUNT(*) AS players
                FROM multibet.risk_tags
                WHERE snapshot_date IN (%s, %s)
                GROUP BY snapshot_date
                ORDER BY snapshot_date
            """, (DIA_ANTES, DIA_DEPOIS))
            for r in cur.fetchall():
                print(f"  snapshot {r[0]}: {r[1]:,} players")

            # 2) Distribuicao de tier ANTES
            print(f"\n--- Tier distribution ANTES ({DIA_ANTES}) ---")
            cur.execute("""
                SELECT tier, COUNT(*) AS qtd
                FROM multibet.risk_tags
                WHERE snapshot_date = %s
                GROUP BY tier
                ORDER BY qtd DESC
            """, (DIA_ANTES,))
            antes = dict(cur.fetchall())
            for t, q in antes.items():
                print(f"  {t:15s}: {q:>8,}")

            print(f"\n--- Tier distribution DEPOIS ({DIA_DEPOIS}) ---")
            cur.execute("""
                SELECT tier, COUNT(*) AS qtd
                FROM multibet.risk_tags
                WHERE snapshot_date = %s
                GROUP BY tier
                ORDER BY qtd DESC
            """, (DIA_DEPOIS,))
            depois = dict(cur.fetchall())
            for t, q in depois.items():
                print(f"  {t:15s}: {q:>8,}")

            # 3) Comparativo lado-a-lado
            print(f"\n--- COMPARATIVO TIER (antes -> depois) ---")
            print(f"  {'tier':15s} {'antes':>10s} {'depois':>10s} {'delta':>10s}")
            for t in ['Muito Bom','Bom','Mediano','Ruim','Muito Ruim','SEM SCORE']:
                a = antes.get(t, 0)
                d = depois.get(t, 0)
                delta = d - a
                marker = " <-- " if abs(delta) > 50 else ""
                print(f"  {t:15s} {a:>10,} {d:>10,} {delta:>+10,}{marker}")

            # 4) Quantos players ganharam a tag cancel_heavy_daily
            print(f"\n--- Players com cancel_heavy_daily ATIVO em {DIA_DEPOIS} ---")
            cur.execute("""
                SELECT COUNT(*) AS qtd,
                       SUM(CASE WHEN cancel_heavy_daily <> 0 THEN 1 ELSE 0 END) AS com_tag
                FROM multibet.risk_tags
                WHERE snapshot_date = %s
            """, (DIA_DEPOIS,))
            total, com_tag = cur.fetchone()
            print(f"  Total players: {total:,}")
            print(f"  Com cancel_heavy_daily ativo: {com_tag:,} ({100*com_tag/total:.2f}%)")

            # 5) Tier change matrix
            print(f"\n--- TIER CHANGES (matrix antes x depois) ---")
            cur.execute("""
                SELECT
                  a.user_ext_id, a.label_id,
                  a.tier AS tier_antes,
                  b.tier AS tier_depois,
                  a.score_bruto AS sb_antes,
                  b.score_bruto AS sb_depois,
                  a.score_norm AS sn_antes,
                  b.score_norm AS sn_depois,
                  COALESCE(b.cancel_heavy_daily, 0) AS cancel_heavy_score
                FROM multibet.risk_tags a
                JOIN multibet.risk_tags b
                  ON a.label_id = b.label_id
                 AND a.user_id = b.user_id
                WHERE a.snapshot_date = %s
                  AND b.snapshot_date = %s
                  AND a.tier <> b.tier
            """, (DIA_ANTES, DIA_DEPOIS))
            tier_changes = cur.fetchall()
            cols = [d[0] for d in cur.description]
            df_tc = pd.DataFrame(tier_changes, columns=cols)
            print(f"\n  Players que mudaram de tier: {len(df_tc):,}")
            if not df_tc.empty:
                # Matriz tier_antes -> tier_depois
                pivot = df_tc.groupby(['tier_antes','tier_depois']).size().reset_index(name='qtd')
                pivot = pivot.pivot(index='tier_antes', columns='tier_depois', values='qtd').fillna(0).astype(int)
                print("\n  Matriz de transicao:")
                print(pivot.to_string())

                # Salva CSV pro Castrin
                tc_path = f"{OUT_DIR}/risk_matrix_tier_changes_{DIA_ANTES}_vs_{DIA_DEPOIS}.csv"
                df_tc.to_csv(tc_path, sep=";", decimal=",", index=False, encoding="utf-8-sig")
                print(f"\n  CSV tier_changes salvo: {tc_path}")

            # 6) Lista de players novos com cancel_heavy_daily ativo
            print(f"\n--- Top 30 players com cancel_heavy_daily ATIVO em {DIA_DEPOIS} ---")
            cur.execute("""
                SELECT b.user_ext_id, b.user_id, b.tier AS tier_novo,
                       a.tier AS tier_antigo,
                       a.score_bruto AS sb_antes, b.score_bruto AS sb_depois,
                       (b.score_bruto - a.score_bruto) AS delta_score
                FROM multibet.risk_tags b
                LEFT JOIN multibet.risk_tags a
                  ON a.label_id = b.label_id
                 AND a.user_id = b.user_id
                 AND a.snapshot_date = %s
                WHERE b.snapshot_date = %s
                  AND b.cancel_heavy_daily <> 0
                ORDER BY (b.score_bruto - COALESCE(a.score_bruto, 0)) ASC
                LIMIT 30
            """, (DIA_ANTES, DIA_DEPOIS))
            top = cur.fetchall()
            cols = [d[0] for d in cur.description]
            df_top = pd.DataFrame(top, columns=cols)
            print(df_top.to_string(index=False))

            # CSV completo dos players cancel_heavy_daily ativo
            print(f"\n--- Salvando CSV completo dos flagados...")
            cur.execute("""
                SELECT b.user_ext_id, b.user_id, b.label_id,
                       a.tier AS tier_antigo, b.tier AS tier_novo,
                       a.score_bruto AS sb_antes, b.score_bruto AS sb_depois,
                       (b.score_bruto - a.score_bruto) AS delta_score
                FROM multibet.risk_tags b
                LEFT JOIN multibet.risk_tags a
                  ON a.label_id = b.label_id
                 AND a.user_id = b.user_id
                 AND a.snapshot_date = %s
                WHERE b.snapshot_date = %s
                  AND b.cancel_heavy_daily <> 0
                ORDER BY (b.score_bruto - COALESCE(a.score_bruto, 0)) ASC
            """, (DIA_ANTES, DIA_DEPOIS))
            all_flag = cur.fetchall()
            cols = [d[0] for d in cur.description]
            df_all = pd.DataFrame(all_flag, columns=cols)
            flag_path = f"{OUT_DIR}/risk_matrix_cancel_heavy_flagados_{DIA_DEPOIS}.csv"
            df_all.to_csv(flag_path, sep=";", decimal=",", index=False, encoding="utf-8-sig")
            print(f"  CSV completo flagados salvo: {flag_path} ({len(df_all):,} linhas)")

            print(f"\n{'='*80}")
            print("DIFF CONCLUIDO")
            print(f"{'='*80}")

    finally:
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    main()
