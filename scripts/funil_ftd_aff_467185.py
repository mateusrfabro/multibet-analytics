"""Funil FTD->STD->TTD->QTD+ - Fev/Mar/Abr 2026 - APENAS affiliate 467185."""
import os, sys, logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import matplotlib.pyplot as plt
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OUT_DIR = "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/output"
AFF = "467185"

MESES = [
    ("Fevereiro/2026", "2026-02-01", "2026-02-28", "fev", "01 a 28/02"),
    ("Marco/2026",     "2026-03-01", "2026-03-31", "mar", "01 a 31/03"),
    ("Abril/2026",     "2026-04-01", "2026-04-23", "abr", "01 a 23/04 (D-1)*"),
]


def sql(data_ini, data_fim, aff):
    return f"""
WITH
test_users_ecr AS (
    SELECT DISTINCT CAST(c_ecr_id AS VARCHAR) AS ecr_id
    FROM bireports_ec2.tbl_ecr WHERE c_test_user = true
),
cohort AS (
    SELECT u.ecr_id
    FROM ps_bi.dim_user u
    LEFT JOIN test_users_ecr t ON CAST(u.ecr_id AS VARCHAR) = t.ecr_id
    WHERE u.is_test = false
      AND t.ecr_id IS NULL
      AND u.has_ftd = 1
      AND CAST(u.affiliate_id AS VARCHAR) = '{aff}'
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
)
SELECT
    COUNT_IF(COALESCE(d.qtd,0) >= 1) AS ftd,
    COUNT_IF(COALESCE(d.qtd,0) >= 2) AS std,
    COUNT_IF(COALESCE(d.qtd,0) >= 3) AS ttd,
    COUNT_IF(COALESCE(d.qtd,0) >= 4) AS qtd_plus
FROM cohort c
LEFT JOIN deposits_mes d ON c.ecr_id = d.c_ecr_id
"""


fmt = lambda n: f"{n:,}".replace(",", ".")
def pct(a, b, d=1):
    return f"{round(a/b*100, d)}%".replace(".", ",") if b else "-"


def main():
    cores = ["#2a7a3b", "#c88116", "#1f4e79"]
    dados = []
    for nome, ini, fim, slug, periodo in MESES:
        log.info(f"Rodando {nome} aff={AFF}...")
        df = query_athena(sql(ini, fim, AFF), database="ps_bi")
        r = df.iloc[0].to_dict()
        r = {k: int(v or 0) for k, v in r.items()}
        print(f"{nome}: FTD={r['ftd']} STD={r['std']} TTD={r['ttd']} QTD+={r['qtd_plus']}")
        dados.append({"nome": nome, "periodo": periodo, **r})

    fig = plt.figure(figsize=(18, 10))
    fig.text(0.5, 0.965, f"Funil FTD > STD > TTD > QTD+  -  Comparativo 2026",
             ha="center", va="top", fontsize=22, weight="bold")
    fig.text(0.5, 0.930, f"Affiliate {AFF}",
             ha="center", va="top", fontsize=13, color="#333", weight="bold")
    fig.text(0.5, 0.905, "Apenas FTDs trazidos pelo affiliate 467185",
             ha="center", va="top", fontsize=10, style="italic", color="#666")

    for idx, m in enumerate(dados):
        x0 = 0.03 + idx * 0.325
        w  = 0.30
        cor = cores[idx]

        fig.text(x0 + w/2, 0.87, m["nome"], ha="center", va="top",
                 fontsize=17, weight="bold", color=cor)
        fig.text(x0 + w/2, 0.84, m["periodo"], ha="center", va="top",
                 fontsize=10, style="italic", color="#555")

        ax = fig.add_axes([x0, 0.50, w, 0.32]); ax.axis("off")

        ftd, std, ttd, q4 = m["ftd"], m["std"], m["ttd"], m["qtd_plus"]
        ps  = pct(std, ftd); pt = pct(ttd, ftd); pq = pct(q4, ftd)
        stt = pct(ttd, std); stq = pct(q4, ttd)

        rows = [
            ["FTD",  fmt(ftd), "100,0%" if ftd else "-", "—"],
            ["STD",  fmt(std), ps, ps],
            ["TTD",  fmt(ttd), pt, stt],
            ["QTD+", fmt(q4),  pq, stq],
        ]
        cols = ["Etapa", "Jogadores", "% vs FTD", "% vs etapa anterior"]
        tbl = ax.table(cellText=rows, colLabels=cols, cellLoc="center",
                       loc="center", colWidths=[0.18, 0.26, 0.22, 0.34])
        tbl.auto_set_font_size(False); tbl.set_fontsize(11); tbl.scale(1, 2.2)
        for j in range(len(cols)):
            c = tbl[0, j]; c.set_facecolor(cor)
            c.set_text_props(color="white", weight="bold")
        for i in range(1, len(rows)+1):
            tbl[i, 0].set_facecolor("#f0f4f9"); tbl[i, 0].set_text_props(weight="bold")
            tbl[i, 1].set_text_props(weight="bold")

        gargalo = round(100 - std/ftd*100) if ftd else 0
        leitura = (
            f"Dos {fmt(ftd)} FTDs,\n"
            f"{ps} voltaram para STD.\n"
            f"{pq} dos FTDs se fidelizaram\n"
            f"(4+ depositos no mes).\n\n"
            f"GARGALO FTD>STD: {gargalo}% saem."
        )
        fig.text(x0 + w/2, 0.42, leitura, ha="center", va="top", fontsize=10,
                 color="#222",
                 bbox=dict(boxstyle="round,pad=0.5", facecolor="#f7f9fc",
                           edgecolor=cor, linewidth=1.2))

    # Tendencia trimestral
    f = dados[0]; m = dados[1]; a = dados[2]
    if f["ftd"] and a["ftd"]:
        crescimento = round(a["ftd"]/f["ftd"], 2)
        ftd_ratio_text = f"Volume de FTDs variou {crescimento}x de fev para abr."
    else:
        ftd_ratio_text = "Volume de FTDs variou de fev para abr."

    insight = (
        "TENDENCIA TRIMESTRAL (Fev > Mar > Abr):\n\n"
        f"{ftd_ratio_text} "
        f"Fidelizacao (QTD+) saiu de {pct(f['qtd_plus'], f['ftd'])} (fev) > "
        f"{pct(m['qtd_plus'], m['ftd'])} (mar) > {pct(a['qtd_plus'], a['ftd'])} (abr).\n"
        f"Conversao FTD>STD: {pct(f['std'], f['ftd'])} (fev) > "
        f"{pct(m['std'], m['ftd'])} (mar) > {pct(a['std'], a['ftd'])} (abr)."
    )
    fig.text(0.5, 0.21, insight, ha="center", va="top", fontsize=11,
             color="#1f4e79",
             bbox=dict(boxstyle="round,pad=0.8", facecolor="#fff8e7",
                       edgecolor="#c88116", linewidth=1.5))

    fig.text(0.5, 0.03,
             f"*Abril parcial (D-1 fechado em 23/04). Coorte: jogadores cujo 1o deposito ocorreu no mes (apenas affiliate {AFF}). "
             "Funil = 2o/3o/4o+ deposito DENTRO do mesmo mes.  |  "
             "Fonte: Athena ps_bi.dim_user + cashier_ec2  |  Squad Intelligence Engine  |  24/04/2026",
             ha="center", va="bottom", fontsize=8, style="italic", color="#888")

    out = f"{OUT_DIR}/funil_ftd_aff{AFF}_COMPARATIVO_fev_mar_abr.png"
    plt.savefig(out, dpi=170, bbox_inches="tight", facecolor="white")
    print(f"OK: {out}")


if __name__ == "__main__":
    main()
