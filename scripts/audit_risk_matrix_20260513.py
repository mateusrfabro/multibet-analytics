"""
Auditoria do estado atual da Matriz de Risco em producao.
Roda em sequencia ate gerar relatorio em reports/audit_risk_matrix_<date>.md
"""
from __future__ import annotations
import json
import sys
from datetime import date, datetime
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from db.supernova import get_supernova_connection  # noqa: E402


REPORT_PATH = PROJECT_DIR / "reports" / f"audit_risk_matrix_{date.today().isoformat()}.md"
REPORT_PATH.parent.mkdir(exist_ok=True)


def section(buf, title):
    buf.append("")
    buf.append(f"## {title}")
    buf.append("")


def kv(buf, k, v):
    buf.append(f"- **{k}:** {v}")


def fmt_row(row, cols):
    return " | ".join(f"{str(row[c])}" for c in cols)


def main():
    buf = [
        f"# Auditoria Matriz de Risco — {date.today().isoformat()}",
        f"Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    ]

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # 1. Frescor de snapshots
            section(buf, "1. Frescor dos snapshots")
            cur.execute("""
                SELECT snapshot_date,
                       COUNT(*) AS rows_n,
                       COUNT(DISTINCT user_id) AS players_n,
                       MAX(computed_at) AS last_computed
                FROM multibet.risk_tags
                GROUP BY snapshot_date
                ORDER BY snapshot_date DESC
                LIMIT 15
            """)
            rows = cur.fetchall()
            buf.append("| snapshot_date | linhas | jogadores | computed_at |")
            buf.append("|---|---:|---:|---|")
            for r in rows:
                buf.append(f"| {r[0]} | {r[1]:,} | {r[2]:,} | {r[3]} |")

            # Identifica gaps no calendario
            cur.execute("""
                WITH d AS (
                  SELECT DISTINCT snapshot_date FROM multibet.risk_tags
                  WHERE snapshot_date >= CURRENT_DATE - 45
                ),
                gaps AS (
                  SELECT snapshot_date,
                         LAG(snapshot_date) OVER (ORDER BY snapshot_date) AS prev_dt,
                         snapshot_date - LAG(snapshot_date) OVER (ORDER BY snapshot_date) AS gap_dias
                  FROM d
                )
                SELECT prev_dt, snapshot_date, gap_dias
                FROM gaps
                WHERE gap_dias > 1
                ORDER BY snapshot_date
            """)
            gaps = cur.fetchall()
            section(buf, "1.1 Gaps no cron (ultimos 45d)")
            if not gaps:
                buf.append("Nenhum gap (>1d) detectado.")
            else:
                buf.append("| dia anterior | proximo snapshot | gap (dias) |")
                buf.append("|---|---|---:|")
                for g in gaps:
                    buf.append(f"| {g[0]} | {g[1]} | {g[2]} |")

            # 2. Distribuicao por tier (snapshot mais recente)
            cur.execute(
                "SELECT MAX(snapshot_date) FROM multibet.risk_tags"
            )
            latest = cur.fetchone()[0]
            section(buf, f"2. Distribuicao por tier — snapshot {latest}")
            cur.execute("""
                SELECT tier, COUNT(*) AS n
                FROM multibet.risk_tags
                WHERE snapshot_date = %s
                GROUP BY tier
                ORDER BY n DESC
            """, (latest,))
            tiers = cur.fetchall()
            total = sum(r[1] for r in tiers)
            buf.append(f"Total: {total:,} jogadores")
            buf.append("")
            buf.append("| tier | jogadores | % |")
            buf.append("|---|---:|---:|")
            for t, n in tiers:
                buf.append(f"| {t} | {n:,} | {n/total*100:.1f}% |")

            # 3. Estatisticas de score
            section(buf, "3. Distribuicao de score_norm")
            cur.execute("""
                SELECT
                    COUNT(score_norm) AS n_com_score,
                    COUNT(*) FILTER (WHERE score_norm IS NULL) AS n_sem_score,
                    ROUND(AVG(score_norm)::numeric, 1) AS mean,
                    ROUND(STDDEV(score_norm)::numeric, 1) AS sd,
                    MIN(score_norm) AS p_min,
                    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY score_norm) AS p05,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY score_norm) AS p25,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY score_norm) AS p50,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY score_norm) AS p75,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY score_norm) AS p95,
                    MAX(score_norm) AS p_max
                FROM multibet.risk_tags
                WHERE snapshot_date = %s
            """, (latest,))
            r = cur.fetchone()
            kv(buf, "Com score", f"{r[0]:,}")
            kv(buf, "Sem score (NULL)", f"{r[1]:,}")
            kv(buf, "Mean / SD", f"{r[2]} / {r[3]}")
            kv(buf, "Min / P05 / P25", f"{r[4]} / {r[5]} / {r[6]}")
            kv(buf, "Mediana", f"{r[7]}")
            kv(buf, "P75 / P95 / Max", f"{r[8]} / {r[9]} / {r[10]}")

            # 4. Cobertura por tag
            section(buf, "4. Cobertura por tag (% de jogadores com tag ativa != 0)")
            tag_cols = [
                "regular_depositor", "promo_only", "zero_risk_player", "fast_cashout",
                "sustained_player", "non_bonus_depositor", "promo_chainer",
                "cashout_and_run", "reinvest_player", "non_promo_player",
                "engaged_player", "rg_alert_player", "behav_risk_player",
                "potencial_abuser", "player_reengaged", "sleeper_low_player",
                "vip_whale_player", "winback_hi_val_player", "behav_slotgamer",
                "multi_game_player", "rollback_player",
            ]
            select_parts = [
                f"SUM(CASE WHEN {c} <> 0 THEN 1 ELSE 0 END) AS cnt_{c}"
                for c in tag_cols
            ]
            cur.execute(
                f"""SELECT COUNT(*) AS total, {', '.join(select_parts)}
                    FROM multibet.risk_tags
                    WHERE snapshot_date = %s""",
                (latest,),
            )
            r = cur.fetchone()
            total = r[0]
            buf.append("| tag | jogadores | % | score |")
            buf.append("|---|---:|---:|---:|")
            scores = {
                "regular_depositor": 10, "promo_only": -15, "zero_risk_player": 0,
                "fast_cashout": -25, "sustained_player": 15, "non_bonus_depositor": 10,
                "promo_chainer": -10, "cashout_and_run": -25, "reinvest_player": 15,
                "non_promo_player": 10, "engaged_player": 10, "rg_alert_player": 1,
                "behav_risk_player": -10, "potencial_abuser": -5, "player_reengaged": 30,
                "sleeper_low_player": 5, "vip_whale_player": 30,
                "winback_hi_val_player": 25, "behav_slotgamer": 5,
                "multi_game_player": -10, "rollback_player": -15,
            }
            rows = list(zip(tag_cols, r[1:]))
            rows.sort(key=lambda x: x[1], reverse=True)
            for t, n in rows:
                pct = n/total*100 if total else 0
                buf.append(f"| {t} | {n:,} | {pct:.1f}% | {scores[t]:+d} |")

            # 5. Player_ext_id NULL
            section(buf, "5. user_ext_id NULL (impacta push Smartico)")
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE user_ext_id IS NULL) AS sem_ext,
                    COUNT(*) FILTER (WHERE user_ext_id IS NULL AND tier <> 'SEM SCORE') AS sem_ext_com_score
                FROM multibet.risk_tags
                WHERE snapshot_date = %s
            """, (latest,))
            r = cur.fetchone()
            kv(buf, "Total", f"{r[0]:,}")
            kv(buf, "Sem user_ext_id", f"{r[1]:,} ({r[1]/r[0]*100:.1f}%)")
            kv(buf, "Sem user_ext_id mas COM score (perdem push)", f"{r[2]:,}")

            # 6. Evolucao da base nos ultimos snapshots
            section(buf, "6. Variacao da base (jogadores entram/saem entre snapshots)")
            cur.execute("""
                WITH s AS (
                  SELECT snapshot_date,
                         COUNT(DISTINCT user_id) AS players,
                         COUNT(*) FILTER (WHERE tier='Muito Bom') AS mb,
                         COUNT(*) FILTER (WHERE tier='Bom') AS b,
                         COUNT(*) FILTER (WHERE tier='Mediano') AS m,
                         COUNT(*) FILTER (WHERE tier='Ruim') AS r,
                         COUNT(*) FILTER (WHERE tier='Muito Ruim') AS mr,
                         COUNT(*) FILTER (WHERE tier='SEM SCORE') AS ss
                  FROM multibet.risk_tags
                  WHERE snapshot_date >= CURRENT_DATE - 30
                  GROUP BY snapshot_date
                )
                SELECT * FROM s ORDER BY snapshot_date DESC LIMIT 15
            """)
            buf.append("| data | jogadores | M.Bom | Bom | Mediano | Ruim | M.Ruim | SemScore |")
            buf.append("|---|---:|---:|---:|---:|---:|---:|---:|")
            for r in cur.fetchall():
                buf.append("| " + " | ".join(str(x) for x in r) + " |")

            # 7. Spot-check: top 10 VIP_WHALE e top 10 com FAST_CASHOUT
            section(buf, "7. Spot-check: 10 amostras VIP_WHALE_PLAYER (tier mais recente)")
            cur.execute("""
                SELECT user_id, user_ext_id, tier, score_norm,
                       vip_whale_player, engaged_player, sustained_player,
                       fast_cashout, cashout_and_run, promo_only
                FROM multibet.risk_tags
                WHERE snapshot_date = %s AND vip_whale_player <> 0
                ORDER BY score_norm DESC NULLS LAST
                LIMIT 10
            """, (latest,))
            buf.append("| user_id | ext_id | tier | score | VIP | ENG | SUS | FC | CR | PRO |")
            buf.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|")
            for r in cur.fetchall():
                buf.append("| " + " | ".join(str(x) for x in r) + " |")

            section(buf, "8. Spot-check: 10 amostras com FAST_CASHOUT")
            cur.execute("""
                SELECT user_id, user_ext_id, tier, score_norm,
                       fast_cashout, cashout_and_run, promo_only,
                       sustained_player, reinvest_player, regular_depositor
                FROM multibet.risk_tags
                WHERE snapshot_date = %s AND fast_cashout <> 0
                ORDER BY RANDOM()
                LIMIT 10
            """, (latest,))
            buf.append("| user_id | ext_id | tier | score | FC | CR | PRO | SUS | REI | REG |")
            buf.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|")
            for r in cur.fetchall():
                buf.append("| " + " | ".join(str(x) for x in r) + " |")

            # 9. Overlaps
            section(buf, "9. Overlaps relevantes (P1 do roadmap)")
            cur.execute("""
                SELECT
                    SUM(CASE WHEN fast_cashout<>0 AND cashout_and_run<>0 THEN 1 ELSE 0 END) AS fc_and_cr,
                    SUM(CASE WHEN fast_cashout<>0 THEN 1 ELSE 0 END) AS fc_only,
                    SUM(CASE WHEN cashout_and_run<>0 THEN 1 ELSE 0 END) AS cr_only,
                    SUM(CASE WHEN promo_only<>0 AND promo_chainer<>0 THEN 1 ELSE 0 END) AS po_and_pc,
                    SUM(CASE WHEN engaged_player<>0 AND rg_alert_player<>0 THEN 1 ELSE 0 END) AS eng_and_rg
                FROM multibet.risk_tags
                WHERE snapshot_date = %s
            """, (latest,))
            r = cur.fetchone()
            kv(buf, "FAST_CASHOUT ∩ CASHOUT_AND_RUN", f"{r[0]:,} (FC total: {r[1]:,} / CR total: {r[2]:,})")
            kv(buf, "PROMO_ONLY ∩ PROMO_CHAINER", f"{r[3]:,}")
            kv(buf, "ENGAGED ∩ RG_ALERT", f"{r[4]:,}")

            # 10. SEM SCORE: por que? quantos tem 0 tags
            section(buf, "10. Jogadores SEM SCORE (cobertura zero)")
            cur.execute("""
                SELECT COUNT(*) FROM multibet.risk_tags
                WHERE snapshot_date = %s AND tier = 'SEM SCORE'
            """, (latest,))
            ss = cur.fetchone()[0]
            kv(buf, "SEM SCORE", f"{ss:,}")
            buf.append("")
            buf.append("Esses jogadores estão na base de atividade mas NENHUMA das 21 regras os flagrou.")
            buf.append("Indica possivel sub-cobertura das regras ou jogadores muito atípicos (1 deposito, parou).")

    finally:
        conn.close()
        tunnel.stop()

    REPORT_PATH.write_text("\n".join(buf), encoding="utf-8")
    print(f"Relatorio gerado: {REPORT_PATH}")


if __name__ == "__main__":
    main()
