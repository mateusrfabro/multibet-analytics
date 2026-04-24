"""
Diagnostico Trafego Play4Tune — Breakdown de Campanhas e Qualidade
===================================================================

Contexto (23/04/2026):
  Cliente reporta "muito clique nos ads Meta, mas pouco registro no site".
  Meta Ads API sera puxada AMANHA pelo cliente — este script foca 100% no
  lado Play4Tune (banco supernova_bet).

Janela: 01/04 -> 22/04/2026 | 2.393 cadastros | 88,5% com fbclid

Objetivo (questoes prioritarias):
  1. Top-5 utm_campaign por cadastros + CV reg->FTD
  2. Top-5 utm_content (criativos) por cadastros + CV reg->FTD
  3. "Baldes furados" (muito cad, baixa CV) vs "campanhas de ouro" (boa CV)
  4. O que sao utm_source='th' e 'an' (via referrer_url)
  5. Padroes de fbclid anomalos (duplicado/bot-like)
  6. Mix de campanhas por dia — mudanca drastica explica queda CV?

Banco: supernova_bet | Moeda: PKR | Filtro test users obrigatorio
"""
import os
import sys
import csv
import logging
from collections import defaultdict
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.supernova_bet import get_supernova_bet_connection

JANELA_INI = date(2026, 4, 1)
JANELA_FIM = date(2026, 4, 22)

REAL_USERS_WHITELIST = {
    'maharshani44377634693',
    'muhammadrehan17657797557',
    'rehmanzafar006972281',
    'saimkyani15688267',
}

OUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "reports",
)
os.makedirs(OUT_DIR, exist_ok=True)
OUT_TXT = os.path.join(OUT_DIR, "diag_trafego_p4t_campaigns_2026-04-23.txt")
OUT_CSV_CAMPAIGN = os.path.join(OUT_DIR, "diag_trafego_p4t_campaigns.csv")
OUT_CSV_CONTENT = os.path.join(OUT_DIR, "diag_trafego_p4t_content.csv")
OUT_CSV_MIX_DIARIO = os.path.join(OUT_DIR, "diag_trafego_p4t_mix_diario.csv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("diag_trafego_p4t")


# ---------------------------------------------------------------------------
# Filtro test users (logica oficial dev — 16/04)
# ---------------------------------------------------------------------------
def get_test_ids(cur):
    cur.execute("""
        SELECT u.id, u.username
        FROM users u
        WHERE
            u.role != 'USER'
            OR LOWER(u.username) LIKE '%%test%%'
            OR LOWER(u.username) LIKE '%%teste%%'
            OR LOWER(u.username) LIKE '%%demo%%'
            OR LOWER(u.username) LIKE '%%admin%%'
            OR LOWER(COALESCE(u.email, '')) LIKE '%%@karinzitta%%'
            OR LOWER(COALESCE(u.email, '')) LIKE '%%@multi.bet%%'
            OR LOWER(COALESCE(u.email, '')) LIKE '%%@grupo-pgs%%'
            OR LOWER(COALESCE(u.email, '')) LIKE '%%@supernovagaming%%'
            OR LOWER(COALESCE(u.email, '')) LIKE '%%@play4tune%%'
            OR u.id IN (
                SELECT DISTINCT t.user_id FROM transactions t
                WHERE t.type IN ('ADJUSTMENT_CREDIT', 'ADJUSTMENT_DEBIT')
                   OR (t.type = 'DEPOSIT' AND t.reviewed_by IS NOT NULL)
            )
    """)
    rows = cur.fetchall()
    ids = [r[0] for r in rows if r[1] not in REAL_USERS_WHITELIST]
    log.info(f"Usuarios teste filtrados: {len(ids)}")
    return tuple(ids) if ids else ('00000000-0000-0000-0000-000000000000',)


# ---------------------------------------------------------------------------
# Inspecao de schema (evitar erro se coluna nao existir)
# ---------------------------------------------------------------------------
def cols_da_tabela(cur, tabela):
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
    """, (tabela,))
    return {r[0] for r in cur.fetchall()}


# ---------------------------------------------------------------------------
# Consultas principais
# ---------------------------------------------------------------------------
def campaign_breakdown(cur, test_ids):
    """Por utm_source + utm_campaign: cadastros, fbclid, FTD, CV, PKR."""
    cur.execute("""
        WITH regs AS (
          SELECT u.id, u.created_at,
                 COALESCE(u.utm_source,'(null)')   AS src,
                 COALESCE(u.utm_campaign,'(null)') AS camp,
                 COALESCE(u.utm_content,'(null)')  AS cont,
                 u.fbclid
          FROM users u
          WHERE u.created_at >= %(ini)s
            AND u.created_at <  %(fim_plus)s
            AND u.id NOT IN %(tests)s
        ),
        ftd AS (
          SELECT DISTINCT ON (t.user_id)
                 t.user_id, t.created_at AS ftd_ts, t.amount
          FROM transactions t
          WHERE t.type='DEPOSIT' AND t.status='COMPLETED'
            AND t.user_id IN (SELECT id FROM regs)
          ORDER BY t.user_id, t.created_at ASC
        )
        SELECT r.src, r.camp, r.cont,
               COUNT(*)                                               AS cadastros,
               COUNT(r.fbclid)                                        AS com_fbclid,
               COUNT(f.user_id)                                       AS ftds,
               ROUND(100.0*COUNT(f.user_id)::numeric/NULLIF(COUNT(*),0),1) AS cv_ftd,
               ROUND(COALESCE(SUM(f.amount),0)::numeric, 0)           AS pkr_total
        FROM regs r
        LEFT JOIN ftd f ON f.user_id = r.id
        GROUP BY 1,2,3
        ORDER BY cadastros DESC
    """, {
        "ini": JANELA_INI,
        "fim_plus": JANELA_FIM + timedelta(days=1),
        "tests": test_ids,
    })
    return cur.fetchall()


def source_th_an_investigar(cur, test_ids):
    """O que sao utm_source='th' e 'an'? Olhar referrer_url e utm_campaign."""
    cur.execute("""
        SELECT COALESCE(u.utm_source,'(null)') AS src,
               COALESCE(u.utm_campaign,'(null)') AS camp,
               COALESCE(u.utm_medium,'(null)')  AS med,
               COALESCE(u.referrer_url,'(null)') AS ref,
               u.fbclid IS NOT NULL             AS tem_fbclid,
               COUNT(*)                         AS n,
               MIN(u.created_at)                AS primeira,
               MAX(u.created_at)                AS ultima
        FROM users u
        WHERE u.utm_source IN ('th','an')
          AND u.created_at >= %(ini)s
          AND u.created_at <  %(fim_plus)s
          AND u.id NOT IN %(tests)s
        GROUP BY 1,2,3,4,5
        ORDER BY n DESC
    """, {
        "ini": JANELA_INI,
        "fim_plus": JANELA_FIM + timedelta(days=1),
        "tests": test_ids,
    })
    return cur.fetchall()


def fbclid_anomalias(cur, test_ids):
    """fbclid duplicados e comprimento anomalo — sinal de bot/replay."""
    cur.execute("""
        SELECT u.fbclid, COUNT(*) n
        FROM users u
        WHERE u.fbclid IS NOT NULL
          AND u.created_at >= %(ini)s
          AND u.created_at <  %(fim_plus)s
          AND u.id NOT IN %(tests)s
        GROUP BY u.fbclid
        HAVING COUNT(*) > 1
        ORDER BY n DESC
        LIMIT 10
    """, {
        "ini": JANELA_INI,
        "fim_plus": JANELA_FIM + timedelta(days=1),
        "tests": test_ids,
    })
    dups = cur.fetchall()

    cur.execute("""
        SELECT
          COUNT(*)                                        AS total_com_fbclid,
          COUNT(DISTINCT u.fbclid)                        AS distintos,
          MIN(LENGTH(u.fbclid))                           AS min_len,
          MAX(LENGTH(u.fbclid))                           AS max_len,
          ROUND(AVG(LENGTH(u.fbclid))::numeric, 1)        AS avg_len
        FROM users u
        WHERE u.fbclid IS NOT NULL
          AND u.created_at >= %(ini)s
          AND u.created_at <  %(fim_plus)s
          AND u.id NOT IN %(tests)s
    """, {
        "ini": JANELA_INI,
        "fim_plus": JANELA_FIM + timedelta(days=1),
        "tests": test_ids,
    })
    stats = cur.fetchone()
    return dups, stats


def mix_diario(cur, test_ids):
    """Mix de top campanhas por dia — identifica troca abrupta que explique CV."""
    cur.execute("""
        WITH regs AS (
          SELECT u.id,
                 (u.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                 COALESCE(u.utm_campaign,'(null)') AS camp,
                 COALESCE(u.utm_source,'(null)')   AS src
          FROM users u
          WHERE u.created_at >= %(ini)s
            AND u.created_at <  %(fim_plus)s
            AND u.id NOT IN %(tests)s
        ),
        ftd AS (
          SELECT DISTINCT t.user_id
          FROM transactions t
          WHERE t.type='DEPOSIT' AND t.status='COMPLETED'
            AND t.user_id IN (SELECT id FROM regs)
        )
        SELECT r.dia,
               r.src,
               r.camp,
               COUNT(*) AS cad,
               COUNT(f.user_id) AS ftd
        FROM regs r
        LEFT JOIN ftd f ON f.user_id = r.id
        GROUP BY 1,2,3
        ORDER BY r.dia, cad DESC
    """, {
        "ini": JANELA_INI,
        "fim_plus": JANELA_FIM + timedelta(days=1),
        "tests": test_ids,
    })
    return cur.fetchall()


def totais_source(cur, test_ids):
    cur.execute("""
        WITH regs AS (
          SELECT u.id,
                 COALESCE(u.utm_source,'(null)') AS src,
                 u.fbclid
          FROM users u
          WHERE u.created_at >= %(ini)s
            AND u.created_at <  %(fim_plus)s
            AND u.id NOT IN %(tests)s
        ),
        ftd AS (
          SELECT DISTINCT t.user_id
          FROM transactions t
          WHERE t.type='DEPOSIT' AND t.status='COMPLETED'
            AND t.user_id IN (SELECT id FROM regs)
        )
        SELECT r.src,
               COUNT(*) cad,
               COUNT(r.fbclid) com_fbclid,
               COUNT(f.user_id) ftd,
               ROUND(100.0*COUNT(f.user_id)::numeric/NULLIF(COUNT(*),0),1) cv
        FROM regs r LEFT JOIN ftd f ON f.user_id = r.id
        GROUP BY r.src ORDER BY cad DESC
    """, {
        "ini": JANELA_INI,
        "fim_plus": JANELA_FIM + timedelta(days=1),
        "tests": test_ids,
    })
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Relatorio
# ---------------------------------------------------------------------------
def run():
    with open(OUT_TXT, "w", encoding="utf-8") as f:
        def w(s=""):
            f.write(s + "\n")
            print(s)

        w(f"# Diagnostico Trafego Play4Tune — lado P4T apenas")
        w(f"# Janela: {JANELA_INI} -> {JANELA_FIM} (BRT) | Executado 23/04/2026")
        w(f"# NOTA: Meta Ads API sera puxada pelo cliente AMANHA (24/04) — skip nesta rodada")
        w("=" * 100)
        w()

        tunnel, conn = get_supernova_bet_connection()
        try:
            cur = conn.cursor()

            # validar colunas (defensivo)
            cols_users = cols_da_tabela(cur, "users")
            precisas = {"utm_source", "utm_campaign", "utm_content", "utm_medium",
                        "referrer_url", "fbclid"}
            faltando = precisas - cols_users
            if faltando:
                w(f"[WARN] colunas faltando em users: {faltando} — ajustar query se vazio")

            test_ids = get_test_ids(cur)
            w(f"[filtro] {len(test_ids)} users teste excluidos")
            w()

            # --- Totais por source ---
            w("[1] TOTAIS POR utm_source")
            w("-" * 100)
            tot = totais_source(cur, test_ids)
            w(f"  {'source':15s} {'cad':>6s} {'fbclid':>8s} {'ftd':>6s} {'cv_ftd%':>9s}")
            for src, cad, fbc, ftd, cv in tot:
                w(f"  {src:15s} {cad:>6} {fbc:>8} {ftd:>6} {str(cv or 0):>9}")
            w()

            # --- Campaign breakdown ---
            rows = campaign_breakdown(cur, test_ids)
            by_camp = defaultdict(lambda: {"cad":0,"fbc":0,"ftd":0,"pkr":0})
            by_cont = defaultdict(lambda: {"cad":0,"fbc":0,"ftd":0,"pkr":0})
            for src, camp, cont, cad, fbc, ftd, cv, pkr in rows:
                by_camp[(src,camp)]["cad"] += cad
                by_camp[(src,camp)]["fbc"] += fbc
                by_camp[(src,camp)]["ftd"] += ftd
                by_camp[(src,camp)]["pkr"] += float(pkr or 0)
                by_cont[(src,cont)]["cad"] += cad
                by_cont[(src,cont)]["fbc"] += fbc
                by_cont[(src,cont)]["ftd"] += ftd
                by_cont[(src,cont)]["pkr"] += float(pkr or 0)

            # TOP 5 CAMPAIGN por cadastro
            w("[2] TOP 5 utm_campaign POR CADASTROS")
            w("-" * 100)
            w(f"  {'source':6s} {'campaign':50s} {'cad':>5s} {'ftd':>5s} {'cv%':>6s} {'PKR':>10s}")
            top5_camp = sorted(by_camp.items(), key=lambda x: -x[1]["cad"])[:5]
            for (src,camp), v in top5_camp:
                cv = 100*v["ftd"]/v["cad"] if v["cad"] else 0
                w(f"  {src[:6]:6s} {(camp or '(null)')[:50]:50s} {v['cad']:>5} {v['ftd']:>5} {cv:>6.1f} {v['pkr']:>10,.0f}")
            w()

            # TOP 5 CONTENT (criativo)
            w("[3] TOP 5 utm_content (CRIATIVO) POR CADASTROS")
            w("-" * 100)
            w(f"  {'source':6s} {'content':50s} {'cad':>5s} {'ftd':>5s} {'cv%':>6s} {'PKR':>10s}")
            top5_cont = sorted(by_cont.items(), key=lambda x: -x[1]["cad"])[:5]
            for (src,cont), v in top5_cont:
                cv = 100*v["ftd"]/v["cad"] if v["cad"] else 0
                w(f"  {src[:6]:6s} {(cont or '(null)')[:50]:50s} {v['cad']:>5} {v['ftd']:>5} {cv:>6.1f} {v['pkr']:>10,.0f}")
            w()

            # Baldes furados (muito cad, baixa CV) — min 30 cadastros
            w("[4] BALDES FURADOS (cad>=30 e CV<15%) — candidatos a pausar")
            w("-" * 100)
            w(f"  {'source':6s} {'campaign':50s} {'cad':>5s} {'ftd':>5s} {'cv%':>6s}")
            baldes = [((s,c),v, 100*v["ftd"]/v["cad"]) for (s,c),v in by_camp.items()
                      if v["cad"] >= 30 and (100*v["ftd"]/v["cad"] if v["cad"] else 0) < 15]
            for (src,camp),v,cv in sorted(baldes, key=lambda x: x[2]):
                w(f"  {src[:6]:6s} {(camp or '(null)')[:50]:50s} {v['cad']:>5} {v['ftd']:>5} {cv:>6.1f}")
            if not baldes:
                w("  (nenhum caso com cad>=30 e CV<15%)")
            w()

            # Campanhas de ouro (cad>=30 e CV>=25%)
            w("[5] CAMPANHAS DE OURO (cad>=30 e CV>=25%) — candidatas a escalar")
            w("-" * 100)
            w(f"  {'source':6s} {'campaign':50s} {'cad':>5s} {'ftd':>5s} {'cv%':>6s} {'PKR':>10s}")
            ouro = [((s,c),v, 100*v["ftd"]/v["cad"]) for (s,c),v in by_camp.items()
                    if v["cad"] >= 30 and (100*v["ftd"]/v["cad"] if v["cad"] else 0) >= 25]
            for (src,camp),v,cv in sorted(ouro, key=lambda x: -x[2]):
                w(f"  {src[:6]:6s} {(camp or '(null)')[:50]:50s} {v['cad']:>5} {v['ftd']:>5} {cv:>6.1f} {v['pkr']:>10,.0f}")
            if not ouro:
                w("  (nenhum caso com cad>=30 e CV>=25%)")
            w()

            # utm_source th e an
            w("[6] INVESTIGACAO utm_source='th' e 'an'")
            w("-" * 100)
            th_an = source_th_an_investigar(cur, test_ids)
            w(f"  {'src':4s} {'camp':22s} {'med':10s} {'ref':45s} {'fbc':>4s} {'n':>3s} {'primeira':>19s}")
            for src, camp, med, ref, fbc, n, pri, ult in th_an:
                w(f"  {src:4s} {camp[:22]:22s} {med[:10]:10s} {ref[:45]:45s} {str(fbc):>4s} {n:>3} {str(pri)[:19]:>19s}")
            if not th_an:
                w("  (nenhum registro detectado)")
            w()

            # Anomalias fbclid
            w("[7] ANOMALIAS fbclid (duplicados / estatistica)")
            w("-" * 100)
            dups, stats = fbclid_anomalias(cur, test_ids)
            total, distintos, mn, mx, avg = stats
            w(f"  total cadastros com fbclid   : {total}")
            w(f"  fbclid distintos             : {distintos}")
            w(f"  duplicados (mesmo fbclid>1x) : {total - distintos}")
            w(f"  tamanho fbclid (min/avg/max) : {mn} / {avg} / {mx}")
            w()
            w("  TOP 10 fbclid com mais repeticoes:")
            for fbc, n in dups:
                w(f"    {(fbc or '')[:80]:80s} -> {n}x")
            if not dups:
                w("  (sem duplicatas)")
            w()

            # Mix diario
            w("[8] MIX DIARIO — top campanha por dia (so campanhas com cad>=3 no dia)")
            w("-" * 100)
            mix = mix_diario(cur, test_ids)
            por_dia = defaultdict(list)
            for dia, src, camp, cad, ftd in mix:
                por_dia[dia].append((src, camp, cad, ftd))
            w(f"  {'dia':12s} {'source':6s} {'top_campaign':40s} {'cad':>5s} {'ftd':>5s} {'cv%':>6s}")
            for dia in sorted(por_dia):
                top1 = sorted(por_dia[dia], key=lambda x: -x[2])[0]
                src, camp, cad, ftd = top1
                cv = 100*ftd/cad if cad else 0
                w(f"  {str(dia):12s} {src[:6]:6s} {(camp or '(null)')[:40]:40s} {cad:>5} {ftd:>5} {cv:>6.1f}")
            w()

            # Dump CSVs
            with open(OUT_CSV_CAMPAIGN, "w", encoding="utf-8", newline="") as cf:
                wr = csv.writer(cf)
                wr.writerow(["utm_source","utm_campaign","utm_content","cadastros","com_fbclid","ftds","cv_ftd_pct","ftd_pkr_total"])
                wr.writerows(rows)
            with open(OUT_CSV_MIX_DIARIO, "w", encoding="utf-8", newline="") as cf:
                wr = csv.writer(cf)
                wr.writerow(["dia","utm_source","utm_campaign","cadastros","ftds"])
                wr.writerows(mix)
            w(f"  -> CSV detalhado:     {OUT_CSV_CAMPAIGN}")
            w(f"  -> CSV mix diario:    {OUT_CSV_MIX_DIARIO}")
            w()
            w("OK.")

        finally:
            conn.close()
            tunnel.stop()


if __name__ == "__main__":
    run()
