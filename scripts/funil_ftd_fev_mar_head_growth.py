"""
Funil FTD->STD->TTD->QTD+ - Fev/2026 e Mar/2026 (Visao B = casa toda sem afiliado)
Mesma logica do abril: cohort por mes, BRT, test users filtrados.
"""
import os, sys, logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import matplotlib.pyplot as plt
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OUT_DIR = "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/output"
os.makedirs(OUT_DIR, exist_ok=True)

MESES = [
    ("Fevereiro/2026", "2026-02-01", "2026-02-28", "fev"),
    ("Marco/2026",     "2026-03-01", "2026-03-31", "mar"),
]


def sql_funil(data_ini: str, data_fim: str) -> str:
    return f"""
WITH
test_users_ecr AS (
    SELECT DISTINCT CAST(c_ecr_id AS VARCHAR) AS ecr_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = true
),
cohort AS (
    SELECT
        u.ecr_id,
        CASE
            WHEN u.affiliate_id IS NULL
                 OR CAST(u.affiliate_id AS VARCHAR) IN ('0', '')
                THEN 'organic'
            WHEN CAST(u.affiliate_id AS VARCHAR) IN ('468114', '297657', '445431')
                THEN 'google_ads'
            WHEN CAST(u.affiliate_id AS VARCHAR) = '464673' THEN 'meta_ads'
            WHEN CAST(u.affiliate_id AS VARCHAR) = '477668' THEN 'tiktok_ads'
            ELSE 'affiliate_partner'
        END AS source_grupo
    FROM ps_bi.dim_user u
    LEFT JOIN test_users_ecr t ON CAST(u.ecr_id AS VARCHAR) = t.ecr_id
    WHERE u.is_test = false
      AND t.ecr_id IS NULL
      AND u.has_ftd = 1
      AND CAST(u.ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
          BETWEEN DATE '{data_ini}' AND DATE '{data_fim}'
),
deposits_mes AS (
    SELECT d.c_ecr_id, COUNT(*) AS qtd
    FROM cashier_ec2.tbl_cashier_deposit d
    INNER JOIN cohort c ON c.ecr_id = d.c_ecr_id
    WHERE d.c_txn_status = 'txn_confirmed_success'
      AND CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
          BETWEEN DATE '{data_ini}' AND DATE '{data_fim}'
    GROUP BY d.c_ecr_id
),
cwd AS (
    SELECT c.source_grupo, COALESCE(d.qtd, 0) AS qtd
    FROM cohort c
    LEFT JOIN deposits_mes d ON c.ecr_id = d.c_ecr_id
)
SELECT source_grupo,
       COUNT_IF(qtd >= 1) AS ftd,
       COUNT_IF(qtd >= 2) AS std,
       COUNT_IF(qtd >= 3) AS ttd,
       COUNT_IF(qtd >= 4) AS qtd_plus
FROM cwd GROUP BY source_grupo ORDER BY ftd DESC
"""


def gerar_png(nome_mes: str, slug: str, visao_b: dict, breakdown: pd.DataFrame) -> str:
    ftd, std, ttd, q4 = visao_b["ftd"], visao_b["std"], visao_b["ttd"], visao_b["qtd_plus"]
    fmt = lambda n: f"{n:,}".replace(",", ".")
    ps = round(std/ftd*100, 1) if ftd else 0
    pt = round(ttd/ftd*100, 1) if ftd else 0
    pq = round(q4/ftd*100, 1) if ftd else 0
    st_t = round(ttd/std*100, 1) if std else 0
    st_q = round(q4/ttd*100, 1) if ttd else 0
    gargalo = round(100 - ps)

    fig = plt.figure(figsize=(10, 11))
    fig.text(0.5, 0.965, "Funil FTD > STD > TTD > QTD+",
             ha="center", va="top", fontsize=20, weight="bold")
    fig.text(0.5, 0.925, f"{nome_mes}  |  Casa toda - sem afiliado",
             ha="center", va="top", fontsize=12, color="#333")
    fig.text(0.5, 0.898, "Inclui: Organico + Google + Meta + TikTok",
             ha="center", va="top", fontsize=10, style="italic", color="#666")

    ax_tab = fig.add_axes([0.04, 0.52, 0.92, 0.33]); ax_tab.axis("off")
    rows = [
        ["FTD  (1o deposito)",   fmt(ftd), "100,0%", "—"],
        ["STD  (2o deposito)",   fmt(std), f"{ps}%".replace(".", ","),  f"{ps}%".replace(".", ",")],
        ["TTD  (3o deposito)",   fmt(ttd), f"{pt}%".replace(".", ","),  f"{st_t}%".replace(".", ",")],
        ["QTD+ (4o+ depositos)", fmt(q4),  f"{pq}%".replace(".", ","),  f"{st_q}%".replace(".", ",")],
    ]
    cols = ["Etapa", "Jogadores", "% vs FTD", "% vs etapa anterior"]
    tbl = ax_tab.table(cellText=rows, colLabels=cols, cellLoc="center",
                       loc="center", colWidths=[0.36, 0.20, 0.18, 0.26])
    tbl.auto_set_font_size(False); tbl.set_fontsize(12); tbl.scale(1, 2.4)
    for j in range(len(cols)):
        c = tbl[0, j]; c.set_facecolor("#1f4e79")
        c.set_text_props(color="white", weight="bold")
    for i in range(1, len(rows)+1):
        tbl[i, 0].set_facecolor("#f0f4f9")
        tbl[i, 0].set_text_props(weight="bold", ha="left")
        tbl[i, 1].set_text_props(weight="bold")

    leitura = (
        "LEITURA DO FUNIL\n\n"
        f"Dos {fmt(ftd)} jogadores que fizeram o 1o deposito (FTD) em {nome_mes.split('/')[0].lower()},\n"
        f"apenas {round(ps)}% voltaram para um 2o deposito (STD = {fmt(std)}).\n\n"
        f"Desses que voltaram, {round(st_t)}% chegaram ao 3o deposito (TTD = {fmt(ttd)})\n"
        f"- ou seja, {round(pt)}% do total de FTDs do mes.\n\n"
        f"E apenas {round(pq)}% dos FTDs ({fmt(q4)} jogadores) se fidelizaram com\n"
        "4+ depositos no mes (QTD+) - a base recorrente de verdade.\n\n"
        f"GARGALO: a maior perda acontece no FTD > STD ({gargalo}% nao retornam)."
    )
    fig.text(0.5, 0.42, leitura, ha="center", va="top", fontsize=11.5, color="#222",
             bbox=dict(boxstyle="round,pad=0.8", facecolor="#f7f9fc",
                       edgecolor="#1f4e79", linewidth=1.4))

    fig.text(0.5, 0.025,
             f"Coorte: 1o deposito em {nome_mes} (BRT). Funil = 2o/3o/4o+ deposito DENTRO do mes. Casa toda sem afiliado parceiro.\n"
             "Fonte: Athena ps_bi.dim_user + cashier_ec2  |  Gerado em 24/04/2026  |  Squad Intelligence Engine",
             ha="center", va="bottom", fontsize=8, style="italic", color="#888")

    path = f"{OUT_DIR}/funil_ftd_{slug}_FINAL.png"
    plt.savefig(path, dpi=180, bbox_inches="tight", facecolor="white"); plt.close()
    return path


def main():
    resultados = []
    for nome, ini, fim, slug in MESES:
        log.info(f"Rodando {nome}...")
        df = query_athena(sql_funil(ini, fim), database="ps_bi")
        print(f"\n=== {nome} ===")
        print(df.to_string(index=False))

        # Visao B = organic + google_ads + meta_ads + tiktok_ads
        visao_b_df = df[df["source_grupo"].isin(["organic", "google_ads", "meta_ads", "tiktok_ads"])]
        visao_b = {k: int(visao_b_df[k].sum()) for k in ["ftd", "std", "ttd", "qtd_plus"]}

        png = gerar_png(nome, slug, visao_b, df)
        df.to_csv(f"{OUT_DIR}/funil_ftd_{slug}_raw.csv", index=False)
        resultados.append((nome, visao_b, png))
        log.info(f"  PNG: {png}")

    print("\n=== CONSOLIDADO VISAO B ===")
    for nome, v, png in resultados:
        pc_std = round(v["std"]/v["ftd"]*100, 1) if v["ftd"] else 0
        pc_ttd = round(v["ttd"]/v["ftd"]*100, 1) if v["ftd"] else 0
        pc_qp  = round(v["qtd_plus"]/v["ftd"]*100, 1) if v["ftd"] else 0
        print(f"{nome}: FTD={v['ftd']:,} | STD={v['std']:,} ({pc_std}%) | TTD={v['ttd']:,} ({pc_ttd}%) | QTD+={v['qtd_plus']:,} ({pc_qp}%)")


if __name__ == "__main__":
    main()
