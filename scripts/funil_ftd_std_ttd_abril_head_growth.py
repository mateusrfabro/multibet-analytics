"""
Funil de recorrencia de deposito - Abril/2026 (01 a 23)
=======================================================
Demanda: Head de Growth - "casa toda esporte e cassino tirando afiliado,
como ta relacao FTD -> STD -> TTD -> QTD+"

Cohort: jogadores cujo 1o deposito (FTD) foi em 01-23/abril/2026
Funil: dos que fizeram FTD, quantos fizeram 2o, 3o, 4o+ deposito DENTRO do mes

Entrega: Duas visoes lado a lado
  Visao A (literal): casa toda sem NADA pago (so organico/direto)
  Visao B (iGaming): casa toda sem AFILIADO de parceria
                     (mantem Google/Meta/TikTok que o head gerencia)

Fonte:
  - ps_bi.dim_user (ftd_date, affiliate_id, is_test)
  - cashier_ec2.tbl_cashier_deposit (contagem de depositos)

Data: 2026-04-24
Autor: Mateus F. (Squad Intelligence Engine)
"""

import sys
import os
import logging
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.table import Table

from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

HOJE = date.today().strftime("%Y-%m-%d")
DATA_INI = "2026-04-01"
DATA_FIM = "2026-04-23"  # fechado, inclusive
DATA_FIM_EXCL = "2026-04-24"  # para filtros < (exclusive)

# IDs oficiais de midia paga (gerenciada pelo time do Head de Growth)
# fonte: pipelines/dim_marketing_mapping_canonical.py
PAID_MEDIA_IDS = ["468114", "297657", "445431", "464673", "477668"]


# =============================================================================
# QUERY UNICA - cohort + funil + breakdown por fonte
# =============================================================================
SQL_FUNIL = f"""
WITH
-- Ajuste 2: test users cruzando is_test (ps_bi) + c_test_user (ecr_ec2)
-- feedback_test_users_filtro_completo.md - divergencia ~3%
test_users_ecr AS (
    SELECT DISTINCT CAST(c_ecr_id AS VARCHAR) AS ecr_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = true
),

-- Cohort: jogadores cujo FTD foi em 01-23/abril/2026 (BRT)
-- Ajuste 1: converter ftd_datetime UTC -> BRT antes de truncar para DATE
--           (evita vazamento/perda de borda ~3h documentado em feedback_timezone_campos_date.md)
cohort AS (
    SELECT
        u.ecr_id,
        CAST(u.affiliate_id AS VARCHAR) AS affiliate_id,
        CASE
            WHEN u.affiliate_id IS NULL
                 OR CAST(u.affiliate_id AS VARCHAR) = '0'
                 OR CAST(u.affiliate_id AS VARCHAR) = ''
                THEN 'organic'
            WHEN CAST(u.affiliate_id AS VARCHAR) IN ('468114', '297657', '445431')
                THEN 'google_ads'
            WHEN CAST(u.affiliate_id AS VARCHAR) = '464673'
                THEN 'meta_ads'
            WHEN CAST(u.affiliate_id AS VARCHAR) = '477668'
                THEN 'tiktok_ads'
            ELSE 'affiliate_partner'
        END AS source_grupo
    FROM ps_bi.dim_user u
    LEFT JOIN test_users_ecr t ON CAST(u.ecr_id AS VARCHAR) = t.ecr_id
    WHERE u.is_test = false
      AND t.ecr_id IS NULL  -- Ajuste 2: exclui test users de ecr_ec2 tambem
      AND u.has_ftd = 1
      AND CAST(
            u.ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
            AS DATE
          ) BETWEEN DATE '{DATA_INI}' AND DATE '{DATA_FIM}'
),

-- Ajuste 1: contagem de depositos com filtro temporal em BRT
deposits_abril AS (
    SELECT
        d.c_ecr_id,
        COUNT(*) AS qtd_depositos_abril
    FROM cashier_ec2.tbl_cashier_deposit d
    INNER JOIN cohort c ON c.ecr_id = d.c_ecr_id
    WHERE d.c_txn_status = 'txn_confirmed_success'
      AND CAST(
            d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
            AS DATE
          ) BETWEEN DATE '{DATA_INI}' AND DATE '{DATA_FIM}'
    GROUP BY d.c_ecr_id
),

cohort_with_deposits AS (
    SELECT
        c.ecr_id,
        c.affiliate_id,
        c.source_grupo,
        COALESCE(d.qtd_depositos_abril, 0) AS qtd
    FROM cohort c
    LEFT JOIN deposits_abril d ON c.ecr_id = d.c_ecr_id
)

-- Agregacao final por source_grupo (para permitir montar as duas visoes)
SELECT
    source_grupo,
    COUNT(*) AS cohort_size,
    COUNT_IF(qtd >= 1) AS ftd,
    COUNT_IF(qtd >= 2) AS std,
    COUNT_IF(qtd >= 3) AS ttd,
    COUNT_IF(qtd >= 4) AS qtd_plus
FROM cohort_with_deposits
GROUP BY source_grupo
ORDER BY ftd DESC
"""


def run_funil():
    log.info("Executando query de funil...")
    df = query_athena(SQL_FUNIL, database="ps_bi")
    log.info(f"Resultado: {len(df)} grupos de fonte")
    print("\n=== BREAKDOWN POR FONTE ===")
    print(df.to_string(index=False))
    return df


def consolida_visoes(df: pd.DataFrame) -> dict:
    """
    Consolida as duas visoes a partir do breakdown.

    Visao A (literal) = so organic
    Visao B (iGaming) = organic + google_ads + meta_ads + tiktok_ads (exclui affiliate_partner)
    Casa toda = todos (referencia)
    """
    def soma(subset: pd.DataFrame) -> dict:
        return {
            "ftd": int(subset["ftd"].sum()),
            "std": int(subset["std"].sum()),
            "ttd": int(subset["ttd"].sum()),
            "qtd_plus": int(subset["qtd_plus"].sum()),
        }

    casa_toda = soma(df)
    visao_a = soma(df[df["source_grupo"] == "organic"])
    visao_b = soma(df[df["source_grupo"].isin(["organic", "google_ads", "meta_ads", "tiktok_ads"])])
    so_afiliado = soma(df[df["source_grupo"] == "affiliate_partner"])

    return {
        "casa_toda": casa_toda,
        "visao_a_organico": visao_a,
        "visao_b_sem_afiliado_parceiro": visao_b,
        "so_afiliado_parceiro": so_afiliado,
    }


def calc_conv(d: dict) -> dict:
    """Calcula % de conversao entre etapas."""
    ftd, std, ttd, q4 = d["ftd"], d["std"], d["ttd"], d["qtd_plus"]
    return {
        **d,
        "pct_std": round(std / ftd * 100, 1) if ftd else 0,
        "pct_ttd": round(ttd / std * 100, 1) if std else 0,
        "pct_qtd_plus": round(q4 / ttd * 100, 1) if ttd else 0,
        # Conversao em relacao ao FTD (funil global)
        "pct_std_ftd": round(std / ftd * 100, 1) if ftd else 0,
        "pct_ttd_ftd": round(ttd / ftd * 100, 1) if ftd else 0,
        "pct_qtd_plus_ftd": round(q4 / ftd * 100, 1) if ftd else 0,
    }


def gerar_print_png(visoes: dict, df_raw: pd.DataFrame, output_path: str):
    """
    Gera PNG compartilhavel via WhatsApp com as 2 visoes lado a lado.
    """
    vA = calc_conv(visoes["visao_a_organico"])
    vB = calc_conv(visoes["visao_b_sem_afiliado_parceiro"])
    casa = calc_conv(visoes["casa_toda"])
    aff = calc_conv(visoes["so_afiliado_parceiro"])

    fig, ax = plt.subplots(figsize=(14, 8))
    ax.axis("off")

    # Titulo
    titulo = "Funil FTD -> STD -> TTD -> QTD+  |  Abril/2026 (01-23, D-1 fechado)"
    subtitulo = "Cohort do mes: jogadores cujo 1o deposito foi entre 01 e 23/04. Funil: dos que fizeram FTD, quantos fizeram 2o, 3o, 4o+ deposito DENTRO do mes."
    ax.text(0.5, 0.97, titulo, ha="center", va="top", fontsize=15, weight="bold", transform=ax.transAxes)
    ax.text(0.5, 0.93, subtitulo, ha="center", va="top", fontsize=9, style="italic", color="#555", transform=ax.transAxes)

    # Tabela principal
    col_labels = ["Etapa", "Visao A\n(literal: so organico)", "Visao B\n(sem afiliado parceiro)", "Casa toda\n(referencia)"]
    row_labels = ["FTD (1o deposito)", "STD (2o deposito)", "TTD (3o deposito)", "QTD+ (4o+ deposito)"]

    def fmt_row(etapa_key, pct_key, data):
        return f"{data[etapa_key]:,}".replace(",", ".") + (f"  ({data[pct_key]}%)" if pct_key and data[etapa_key] else "")

    table_data = [
        [row_labels[0], fmt_row("ftd", None, vA), fmt_row("ftd", None, vB), fmt_row("ftd", None, casa)],
        [row_labels[1], fmt_row("std", "pct_std_ftd", vA), fmt_row("std", "pct_std_ftd", vB), fmt_row("std", "pct_std_ftd", casa)],
        [row_labels[2], fmt_row("ttd", "pct_ttd_ftd", vA), fmt_row("ttd", "pct_ttd_ftd", vB), fmt_row("ttd", "pct_ttd_ftd", casa)],
        [row_labels[3], fmt_row("qtd_plus", "pct_qtd_plus_ftd", vA), fmt_row("qtd_plus", "pct_qtd_plus_ftd", vB), fmt_row("qtd_plus", "pct_qtd_plus_ftd", casa)],
    ]

    # % step-over-step (conversao etapa-a-etapa)
    step_data = [
        ["% step FTD->STD", f"{vA['pct_std']}%", f"{vB['pct_std']}%", f"{casa['pct_std']}%"],
        ["% step STD->TTD", f"{vA['pct_ttd']}%", f"{vB['pct_ttd']}%", f"{casa['pct_ttd']}%"],
        ["% step TTD->QTD+", f"{vA['pct_qtd_plus']}%", f"{vB['pct_qtd_plus']}%", f"{casa['pct_qtd_plus']}%"],
    ]

    all_rows = table_data + [["", "", "", ""]] + step_data

    tbl = ax.table(
        cellText=all_rows,
        colLabels=col_labels,
        cellLoc="center",
        loc="center",
        bbox=[0.05, 0.25, 0.90, 0.58],
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(11)

    # Estilo header
    for i, key in enumerate(col_labels):
        cell = tbl[0, i]
        cell.set_facecolor("#1f4e79")
        cell.set_text_props(color="white", weight="bold")
        cell.set_height(0.08)

    # Estilo coluna etapa (primeira col)
    for i in range(1, len(all_rows) + 1):
        c = tbl[i, 0]
        c.set_facecolor("#f0f0f0")
        c.set_text_props(weight="bold")

    # Rodape com breakdown
    rodape = (
        f"Breakdown por fonte (Casa toda): "
        f"Organico={int(df_raw[df_raw['source_grupo']=='organic']['ftd'].sum()):,} | "
        f"Google={int(df_raw[df_raw['source_grupo']=='google_ads']['ftd'].sum()):,} | "
        f"Meta={int(df_raw[df_raw['source_grupo']=='meta_ads']['ftd'].sum()):,} | "
        f"TikTok={int(df_raw[df_raw['source_grupo']=='tiktok_ads']['ftd'].sum()):,} | "
        f"Afiliado parceiro={int(df_raw[df_raw['source_grupo']=='affiliate_partner']['ftd'].sum()):,}"
    ).replace(",", ".")

    ax.text(0.5, 0.20, rodape, ha="center", va="top", fontsize=9, color="#333", transform=ax.transAxes)

    ax.text(0.5, 0.15,
            "Leitura: Visao B (sem afiliado parceiro) = casa toda mantendo Google/Meta/TikTok que o time de Growth gerencia.",
            ha="center", va="top", fontsize=9, style="italic", color="#1f4e79", transform=ax.transAxes)

    ax.text(0.5, 0.11,
            f"% entre parenteses = conversao cumulativa vs FTD. Ultimas 3 linhas = conversao step-a-step.",
            ha="center", va="top", fontsize=8, color="#777", transform=ax.transAxes)

    ax.text(0.98, 0.03,
            f"Fonte: Athena ps_bi.dim_user + cashier_ec2.tbl_cashier_deposit | Gerado em {HOJE} | Squad Intelligence Engine",
            ha="right", va="bottom", fontsize=7, color="#888", transform=ax.transAxes)

    plt.savefig(output_path, dpi=180, bbox_inches="tight", facecolor="white")
    log.info(f"PNG salvo: {output_path}")


def main():
    df = run_funil()

    # Salva CSV raw
    out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, f"funil_ftd_std_ttd_abril_raw_{HOJE}.csv")
    df.to_csv(csv_path, index=False)
    log.info(f"CSV raw: {csv_path}")

    # Consolida visoes
    visoes = consolida_visoes(df)

    print("\n=== CONSOLIDADO ===")
    for k, v in visoes.items():
        conv = calc_conv(v)
        print(f"\n{k}:")
        print(f"  FTD: {conv['ftd']:,} | STD: {conv['std']:,} ({conv['pct_std']}%) | "
              f"TTD: {conv['ttd']:,} ({conv['pct_ttd']}%) | QTD+: {conv['qtd_plus']:,} ({conv['pct_qtd_plus']}%)")

    # Gera PNG
    png_path = os.path.join(out_dir, f"funil_ftd_std_ttd_abril_{HOJE}.png")
    gerar_print_png(visoes, df, png_path)

    print(f"\n=== ENTREGA ===")
    print(f"  PNG:  {png_path}")
    print(f"  CSV:  {csv_path}")


if __name__ == "__main__":
    main()
