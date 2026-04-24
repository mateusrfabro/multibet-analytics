"""
Pipeline: sync_ad_results (Athena ps_bi.dim_user -> Super Nova DB)
==================================================================
Popula multibet.fact_ad_results com metricas de negocio (REG, FTD, GGR)
por (dia_safra, ad_source). Safra = data de cadastro (signup_datetime BRT).

Granularidade: (dt, ad_source).
Por que nao por campaign_id: dim_campaign_affiliate (mapeamento
ad_api.campaign_id -> affiliate_id) esta vazia — quando for populada,
basta trocar o GROUP BY pra incluir campaign_id.

Fonte dos dados:
    - ps_bi.dim_user (Athena) — tem affiliate_id, signup_datetime, has_ftd,
      ftd_date, ftd_amount_inhouse, is_test nativos
    - multibet.dim_marketing_mapping (Super Nova) — classifica
      affiliate_id -> source_name (google_ads, meta, tiktok, organic, etc.)

Tabela destino:
    multibet.fact_ad_results
        (dt, ad_source, regs, ftds, ftd_amount_brl, refreshed_at)
        PK: (dt, ad_source)

Estrategia: DELETE periodo + INSERT (idempotente).

Execucao:
    python pipelines/sync_ad_results.py              # ultimos 7 dias
    python pipelines/sync_ad_results.py --days 30
    python pipelines/sync_ad_results.py --days 100   # backfill historico

Regras obrigatorias respeitadas (CLAUDE.md / memoria):
    - Timezone: signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
    - is_test: usar ps_bi.dim_user.is_test (nao mais c_test_user)
    - Safra = ps_bi.dim_user.signup_datetime (NAO tab_user_affiliate)
    - Super Nova DB como destino, nunca fonte
"""

import sys
import os
import logging
import argparse
from datetime import date, timedelta, datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection

import psycopg2.extras
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL — tabela destino
# ---------------------------------------------------------------------------
DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fact_ad_results (
    dt                  DATE NOT NULL,
    ad_source           VARCHAR(50) NOT NULL,       -- google_ads, meta, tiktok, organic, other, etc.
    regs                INTEGER DEFAULT 0,           -- cadastros da safra
    ftds                INTEGER DEFAULT 0,           -- FTDs da safra (quantos fizeram FTD em algum momento)
    ftd_amount_brl      NUMERIC(18,2) DEFAULT 0,    -- soma dos valores do primeiro deposito (ftd_amount_inhouse)
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, ad_source)
);
"""
DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_ad_results_source
ON multibet.fact_ad_results (ad_source);
"""


def setup_table():
    log.info("Verificando/criando multibet.fact_ad_results...")
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_INDEX)
    log.info("  OK")


def _query_athena_cohort(start_date: date, end_date: date) -> pd.DataFrame:
    """Busca REG+FTD por (safra_brt, affiliate_id) no Athena.

    Safra = data de cadastro convertida UTC->BRT.
    Filtra is_test=false (excluir usuarios de teste).
    """
    sql = f"""
    SELECT
        DATE(signup_datetime AT TIME ZONE 'America/Sao_Paulo') AS dt_brt,
        COALESCE(affiliate_id, '0') AS affiliate_id,
        COUNT(*) AS regs,
        COALESCE(SUM(has_ftd), 0) AS ftds,
        COALESCE(SUM(ftd_amount_inhouse), 0) AS ftd_amount_brl
    FROM ps_bi.dim_user
    WHERE signup_datetime >= TIMESTAMP '{start_date} 00:00:00 America/Sao_Paulo'
      AND signup_datetime <  TIMESTAMP '{end_date + timedelta(days=1)} 00:00:00 America/Sao_Paulo'
      AND is_test = false
    GROUP BY 1, 2
    """
    log.info(f"Athena: buscando REG+FTD de {start_date} a {end_date}...")
    df = query_athena(sql, database="ps_bi")
    log.info(f"  {len(df):,} linhas (dt_brt, affiliate_id)")
    return df


def _load_affiliate_mapping() -> dict:
    """Carrega affiliate_id -> source_name do dim_marketing_mapping."""
    rows = execute_supernova(
        "SELECT affiliate_id, source_name FROM multibet.dim_marketing_mapping",
        fetch=True,
    )
    mapping = {str(a): s for a, s in rows}
    log.info(f"dim_marketing_mapping: {len(mapping):,} affiliate_ids classificados")
    return mapping


def sync(days: int = 7):
    """Puxa REG/FTD do Athena, classifica source, insere na fact_ad_results."""
    end_date = date.today() - timedelta(days=1)    # D-1
    start_date = end_date - timedelta(days=days - 1)
    log.info(f"Periodo: {start_date} a {end_date} ({days} dias)")

    # 1. Athena: cohort por affiliate_id
    df = _query_athena_cohort(start_date, end_date)
    if df.empty:
        log.warning("Zero linhas do Athena — abortando")
        return

    # Converter ftd_amount_brl de Decimal pra float (evita erro de cast depois)
    df["ftd_amount_brl"] = df["ftd_amount_brl"].astype(float)
    df["affiliate_id"] = df["affiliate_id"].astype(str)

    # 2. Super Nova: mapeamento affiliate_id -> source_name
    mapping = _load_affiliate_mapping()
    # Normalizacao: alinhar com fact_ad_spend (padrao ja usado no front).
    # fact_ad_spend usa canais SEM sufixo '_ads' (ex: 'meta' e nao 'meta_ads'),
    # excecao: 'google_ads'. dim_marketing_mapping usa 'meta_ads', 'tiktok_ads', etc.
    SOURCE_NORMALIZE = {
        "meta_ads": "meta",
        "tiktok_ads": "tiktok",
        "kwai_ads": "kwai",
    }
    df["ad_source"] = df["affiliate_id"].map(
        lambda a: SOURCE_NORMALIZE.get(mapping.get(a, "other"), mapping.get(a, "other"))
    )

    # Diagnostico de cobertura
    total_regs = df["regs"].sum()
    regs_other = df.loc[df["ad_source"] == "other", "regs"].sum()
    log.info(
        f"Cobertura mapping: {100 * (1 - regs_other/total_regs):.1f}% "
        f"({total_regs - regs_other:,} de {total_regs:,} REGs classificados) "
        f"| 'other': {regs_other:,} REGs"
    )

    # 3. Agregar por (dt, ad_source)
    agg = df.groupby(["dt_brt", "ad_source"]).agg(
        regs=("regs", "sum"),
        ftds=("ftds", "sum"),
        ftd_amount_brl=("ftd_amount_brl", "sum"),
    ).reset_index()
    log.info(f"Agregado: {len(agg)} linhas (dt, ad_source)")

    # 4. Resumo por source
    summary = agg.groupby("ad_source").agg(
        dias=("dt_brt", "nunique"),
        regs=("regs", "sum"),
        ftds=("ftds", "sum"),
        ftd_brl=("ftd_amount_brl", "sum"),
    ).reset_index()
    log.info("Resumo por source:")
    for _, r in summary.iterrows():
        log.info(f"  {r['ad_source']:<15} {int(r['dias']):>3} dias | "
                 f"{int(r['regs']):>6,} regs | {int(r['ftds']):>5,} ftds | "
                 f"R$ {r['ftd_brl']:>10,.2f}")

    # 5. DELETE + INSERT
    now_utc = datetime.now(timezone.utc)
    records = [
        (row["dt_brt"], row["ad_source"], int(row["regs"]), int(row["ftds"]),
         float(row["ftd_amount_brl"]), now_utc)
        for _, row in agg.iterrows()
    ]
    insert_sql = """
        INSERT INTO multibet.fact_ad_results
            (dt, ad_source, regs, ftds, ftd_amount_brl, refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM multibet.fact_ad_results WHERE dt BETWEEN %s AND %s",
                (start_date, end_date),
            )
            log.info(f"DELETE: {cur.rowcount} linhas antigas")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
            log.info(f"INSERT: {len(records)} linhas novas")
        conn.commit()
    finally:
        conn.close()
        tunnel.stop()

    log.info("=== SYNC CONCLUIDO ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync REG/FTD por source -> Super Nova DB")
    parser.add_argument("--days", type=int, default=7, help="Dias para sincronizar (default: 7)")
    args = parser.parse_args()

    log.info("=== Pipeline sync_ad_results ===")
    setup_table()
    sync(days=args.days)
    log.info("=== Pipeline concluido ===")
