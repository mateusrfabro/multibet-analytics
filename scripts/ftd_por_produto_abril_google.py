"""
FTDs de abril/2026 filtrados APENAS por Google Ads, segmentados por produto
da primeira aposta (Cassino vs Esporte).

Demanda: gestor de trafego do Google quer validar se o ticket medio do
FTD amount esta baixo por causa de jogadores de Esporte no canal Google.

Abordagem:
- Lista de affiliate_ids Google vem de multibet.dim_marketing_mapping
  (source = 'google_ads', oficiais + forenses via gclid) — NUNCA hardcodado.
- Filtro aplicado em bireports_ec2.tbl_ecr.c_affiliate_id.
- Classificacao do FTD pelo PRODUTO DA PRIMEIRA APOSTA apos deposito.
- Periodo: 01/04/2026 a 22/04/2026 (BRT, parcial D-0).

Fontes:
- multibet.dim_marketing_mapping  (Super Nova DB, PostgreSQL) — lista affiliates Google
- bireports_ec2.tbl_ecr            (filtro test + cadastro affiliate)
- cashier_ec2.tbl_cashier_deposit  (FTD valor e timestamp)
- fund_ec2.tbl_real_fund_txn       (primeira aposta por produto)

Output:
- data/ftd_por_produto_abril_google_agg.csv     (agregado)
- data/ftd_por_produto_abril_google.csv         (granular)
- data/ftd_por_produto_abril_google_legenda.txt
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.athena import query_athena       # noqa: E402
from db.supernova import execute_supernova  # noqa: E402

START_DATE = "2026-04-01"
END_DATE = "2026-04-22"  # parcial

OUT_DIR = Path(__file__).resolve().parent.parent / "data"
OUT_DIR.mkdir(exist_ok=True)


def load_google_affiliate_ids() -> tuple[list[str], list[tuple]]:
    """Le lista de affiliate_ids classificados como google_ads no dim_marketing_mapping.

    Retorna (lista_de_ids, detalhes) onde detalhes e lista de tuplas
    (affiliate_id, partner_name, is_validated) para logging/legenda.
    """
    sql = """
        SELECT DISTINCT affiliate_id,
               COALESCE(partner_name, '(sem nome)') AS partner_name,
               COALESCE(is_validated, FALSE)       AS is_validated
        FROM multibet.dim_marketing_mapping
        WHERE source_name = 'google_ads'
          AND affiliate_id IS NOT NULL
          AND TRIM(affiliate_id) <> ''
        ORDER BY is_validated DESC, affiliate_id
    """
    rows = execute_supernova(sql, fetch=True)
    ids = [str(r[0]) for r in rows]
    detalhes = [(str(r[0]), r[1], bool(r[2])) for r in rows]
    oficiais = [d for d in detalhes if d[2]]
    forenses = [d for d in detalhes if not d[2]]
    print(
        f"[Google Affiliates] {len(ids)} IDs total "
        f"({len(oficiais)} oficiais / {len(forenses)} forenses)"
    )
    for aid, name, _ in oficiais:
        print(f"  OFICIAL  {aid}  {name}")
    return ids, detalhes


def build_sql(google_ids: list[str]) -> tuple[str, str]:
    """Monta SQL Athena com lista de affiliate_ids Google."""
    # Monta IN (...) — sanitiza pra aceitar so digitos (affiliate_id e numerico)
    safe_ids = [i for i in google_ids if i.isdigit()]
    if not safe_ids:
        raise RuntimeError("Nenhum affiliate_id Google valido encontrado")
    in_clause = ", ".join(f"'{i}'" for i in safe_ids)

    base = f"""
WITH
google_affiliates AS (
    -- Lista de affiliate_ids classificados como google_ads em dim_marketing_mapping
    SELECT affiliate_id FROM (
        VALUES {', '.join(f"('{i}')" for i in safe_ids)}
    ) AS t(affiliate_id)
),

-- 1. Jogadores Google (cadastro em bireports_ec2)
google_users AS (
    SELECT
        e.c_ecr_id,
        CAST(e.c_affiliate_id AS VARCHAR) AS c_affiliate_id
    FROM bireports_ec2.tbl_ecr e
    WHERE e.c_test_user = false
      AND CAST(e.c_affiliate_id AS VARCHAR) IN ({in_clause})
),

-- 2. Primeiro deposito confirmado por jogador (FTD)
first_deposits AS (
    SELECT
        c_ecr_id,
        ftd_time_utc,
        CAST(ftd_time_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS ftd_date_brt,
        ftd_amount
    FROM (
        SELECT
            c_ecr_id,
            c_created_time AS ftd_time_utc,
            CAST(c_confirmed_amount_in_ecr_ccy AS DECIMAL(18,2)) / 100.0 AS ftd_amount,
            ROW_NUMBER() OVER (PARTITION BY c_ecr_id ORDER BY c_created_time) AS rn
        FROM cashier_ec2.tbl_cashier_deposit
        WHERE c_txn_status = 'txn_confirmed_success'
    ) t
    WHERE rn = 1
      AND CAST(ftd_time_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
          BETWEEN DATE '{START_DATE}' AND DATE '{END_DATE}'
),

-- 3. Primeira aposta CASSINO
first_casino_bet AS (
    SELECT
        c_ecr_id,
        MIN(c_start_time) AS first_casino_ts_utc
    FROM fund_ec2.tbl_real_fund_txn
    WHERE c_start_time >= TIMESTAMP '{START_DATE} 00:00:00'
      AND c_start_time <  TIMESTAMP '2026-04-23 03:00:00'
      AND c_txn_type IN (27, 28)
      AND c_txn_status = 'SUCCESS'
      AND c_product_id = 'CASINO'
    GROUP BY c_ecr_id
),

-- 4. Primeira aposta SPORTSBOOK
first_sports_bet AS (
    SELECT
        c_ecr_id,
        MIN(c_start_time) AS first_sports_ts_utc
    FROM fund_ec2.tbl_real_fund_txn
    WHERE c_start_time >= TIMESTAMP '{START_DATE} 00:00:00'
      AND c_start_time <  TIMESTAMP '2026-04-23 03:00:00'
      AND c_txn_type = 59
      AND c_txn_status = 'SUCCESS'
      AND c_product_id = 'SPORTS_BOOK'
    GROUP BY c_ecr_id
),

-- 5. Classificacao (apenas jogadores Google)
ftd_classified AS (
    SELECT
        f.c_ecr_id,
        g.c_affiliate_id,
        f.ftd_date_brt,
        f.ftd_amount,
        fc.first_casino_ts_utc,
        fs.first_sports_ts_utc,
        CASE
            WHEN fc.first_casino_ts_utc IS NULL AND fs.first_sports_ts_utc IS NULL
                THEN 'FTD Sem Atividade'
            WHEN fc.first_casino_ts_utc IS NOT NULL AND fs.first_sports_ts_utc IS NULL
                THEN 'FTD Cassino'
            WHEN fs.first_sports_ts_utc IS NOT NULL AND fc.first_casino_ts_utc IS NULL
                THEN 'FTD Esporte'
            WHEN fc.first_casino_ts_utc <= fs.first_sports_ts_utc
                THEN 'FTD Cassino'
            ELSE 'FTD Esporte'
        END AS segmento_ftd
    FROM first_deposits f
    INNER JOIN google_users g ON g.c_ecr_id = f.c_ecr_id
    LEFT JOIN first_casino_bet fc ON fc.c_ecr_id = f.c_ecr_id
    LEFT JOIN first_sports_bet fs ON fs.c_ecr_id = f.c_ecr_id
)
"""

    agg = base + """
SELECT
    segmento_ftd,
    COUNT(*) AS qtd_ftds,
    ROUND(SUM(ftd_amount), 2) AS ftd_amount_total_brl,
    ROUND(AVG(ftd_amount), 2) AS ftd_amount_medio_brl,
    ROUND(APPROX_PERCENTILE(ftd_amount, 0.5), 2) AS ftd_amount_mediano_brl,
    ROUND(MIN(ftd_amount), 2) AS ftd_amount_min_brl,
    ROUND(MAX(ftd_amount), 2) AS ftd_amount_max_brl
FROM ftd_classified
GROUP BY segmento_ftd
ORDER BY qtd_ftds DESC
"""

    # Breakdown por affiliate_id — mostra quais IDs Google sao mais representativos
    by_affiliate = base + """
SELECT
    c_affiliate_id,
    segmento_ftd,
    COUNT(*) AS qtd_ftds,
    ROUND(SUM(ftd_amount), 2) AS ftd_amount_total_brl,
    ROUND(AVG(ftd_amount), 2) AS ftd_amount_medio_brl
FROM ftd_classified
GROUP BY c_affiliate_id, segmento_ftd
ORDER BY c_affiliate_id, qtd_ftds DESC
"""

    return agg, by_affiliate


def build_legenda(n_affiliates: int, detalhes: list[tuple]) -> str:
    oficiais = [d for d in detalhes if d[2]]
    forenses = [d for d in detalhes if not d[2]]
    oficiais_txt = "\n".join(
        f"  - {aid}  {name}" for aid, name, _ in oficiais
    ) or "  (nenhum)"
    forenses_amostra = "\n".join(
        f"  - {aid}  {name}" for aid, name, _ in forenses[:10]
    )
    forenses_txt = (
        f"{len(forenses)} affiliate_ids forenses (is_validated=FALSE), "
        f"inferidos por gclid= em c_reference_url.\nAmostra (10 primeiros):\n{forenses_amostra}"
        if forenses
        else "  (nenhum)"
    )
    return f"""LEGENDA — FTD por Produto (Google Ads, Abril/2026 parcial)
=========================================================

Periodo: {START_DATE} a {END_DATE} (parcial — 22/04 e D-0)
Timezone: America/Sao_Paulo (BRT)
Filtro: {n_affiliates} affiliate_ids classificados como 'google_ads' em dim_marketing_mapping

COLUNAS (agregado)
------------------
- segmento_ftd           — classificacao do FTD (ver Glossario)
- qtd_ftds               — quantidade de jogadores Google com FTD no periodo
- ftd_amount_total_brl   — soma dos FTDs no segmento (R$)
- ftd_amount_medio_brl   — ticket medio do FTD no segmento (R$) [KPI principal]
- ftd_amount_mediano_brl — mediana — menos sensivel a outliers
- ftd_amount_min_brl / max_brl — limites observados

COLUNAS (breakdown por affiliate)
---------------------------------
Mesmas do agregado, quebradas por c_affiliate_id — util para ver qual conta
Google (297657/PMax, 445431/Eyal, 468114/App, etc.) gera cada segmento.

GLOSSARIO
---------
- FTD (First Time Deposit): primeiro deposito confirmado do jogador.
- FTD Cassino: primeira aposta apos FTD foi em casino (fund_ec2 c_product_id='CASINO')
- FTD Esporte: primeira aposta apos FTD foi em sportsbook (fund_ec2 c_product_id='SPORTS_BOOK')
- FTD Sem Atividade: depositou mas nao apostou ate o corte.

FILTRO GOOGLE — COMO FOI MONTADO
--------------------------------
1. Consulta multibet.dim_marketing_mapping (Super Nova DB / PostgreSQL)
2. Filtro: source_name = 'google_ads' (oficiais + forenses via gclid nas URLs)
3. Lista de affiliate_ids inserida no WHERE do cadastro bireports_ec2.tbl_ecr
4. Nao ha hardcode — se Marketing adicionar novo affiliate Google em
   dim_marketing_mapping, o script pega automaticamente na proxima execucao.

AFFILIATES GOOGLE — OFICIAIS (is_validated=TRUE)
------------------------------------------------
{oficiais_txt}

AFFILIATES GOOGLE — FORENSES (is_validated=FALSE)
-------------------------------------------------
{forenses_txt}

RACIONAL
--------
Mesma logica do script geral: FTD nao tem produto direto na origem (e evento
de wallet). Classificamos cada FTD pelo produto da 1a aposta.

COMO LER
--------
Comparar ftd_amount_medio_brl entre "FTD Cassino" e "FTD Esporte" dentro do
canal Google. Se Esporte < Cassino, a hipotese do gestor se confirma pelo
canal dele. Se Esporte >= Cassino, a hipotese nao se sustenta no Google.

COMPARE TAMBEM com o report geral (data/ftd_por_produto_abril_agg.csv) para
saber se o comportamento Google difere da media da casa.

LIMITACOES
----------
- D-0 parcial (22/04). Rodar de novo apos 23/04 (fechamento) e idealmente
  apos 28/04 (para capturar apostas tardias de FTDs de 19-22/04).
- Mapeamento Google usa forense (gclid= em c_reference_url) alem dos 3 IDs
  oficiais. Classificacao forense tem ~5-15% de erro segundo observado em
  reports anteriores — NAO e precisao de cadastro oficial.
- Jogador com c_affiliate_id mudado apos signup pode ser mis-atribuido.
- Validacao empirica sugerida: cruzar com API Google Ads (volume de cliques
  por campanha/affiliate) pra batimento externo.

FONTES
------
- multibet.dim_marketing_mapping  (Super Nova DB)
- bireports_ec2.tbl_ecr
- cashier_ec2.tbl_cashier_deposit
- fund_ec2.tbl_real_fund_txn
"""


def main():
    print(f"[FTD-Google] Periodo: {START_DATE} a {END_DATE}")
    ids, detalhes = load_google_affiliate_ids()

    sql_agg, sql_by_aff = build_sql(ids)

    print("  -> Query agregada (apenas Google)...")
    df_agg = query_athena(sql_agg, database="default")
    print(df_agg)

    print("  -> Breakdown por affiliate_id...")
    df_by_aff = query_athena(sql_by_aff, database="default")

    csv_agg = OUT_DIR / "ftd_por_produto_abril_google_agg.csv"
    csv_by_aff = OUT_DIR / "ftd_por_produto_abril_google_por_affiliate.csv"
    legenda_path = OUT_DIR / "ftd_por_produto_abril_google_legenda.txt"

    df_agg.to_csv(csv_agg, index=False, encoding="utf-8-sig")
    df_by_aff.to_csv(csv_by_aff, index=False, encoding="utf-8-sig")
    legenda_path.write_text(build_legenda(len(ids), detalhes), encoding="utf-8")

    print(f"\n[OK] Agregado:     {csv_agg}")
    print(f"[OK] Por affiliate: {csv_by_aff}")
    print(f"[OK] Legenda:       {legenda_path}")

    oficiais = [d for d in detalhes if d[2]]
    forenses = [d for d in detalhes if not d[2]]
    oficiais_inline = ", ".join(f"{aid} ({name})" for aid, name, _ in oficiais)

    print("\n" + "=" * 90)
    print(f"FTD por Produto — GOOGLE ADS — Abril/2026 (01/04 a 22/04 parcial)")
    print("=" * 90)
    print(
        f"Affiliates considerados: {len(ids)} total "
        f"({len(oficiais)} oficiais + {len(forenses)} forenses via gclid)"
    )
    print(f"Oficiais: {oficiais_inline}")
    print(
        f"Forenses: {len(forenses)} IDs inferidos por gclid= em c_reference_url "
        f"(ver CSV e legenda para lista completa)"
    )
    print("-" * 90)
    print(df_agg.to_string(index=False))
    print("=" * 90)

    cassino = df_agg[df_agg["segmento_ftd"] == "FTD Cassino"]
    esporte = df_agg[df_agg["segmento_ftd"] == "FTD Esporte"]
    if not cassino.empty and not esporte.empty:
        medio_cas = cassino["ftd_amount_medio_brl"].iloc[0]
        medio_esp = esporte["ftd_amount_medio_brl"].iloc[0]
        med_cas = cassino["ftd_amount_mediano_brl"].iloc[0]
        med_esp = esporte["ftd_amount_mediano_brl"].iloc[0]
        print(
            f"\nLeitura (Google):\n"
            f"  Media  -> Esporte R$ {medio_esp:.2f} vs Cassino R$ {medio_cas:.2f} "
            f"(delta {medio_esp-medio_cas:+.2f})\n"
            f"  Mediana-> Esporte R$ {med_esp:.2f} vs Cassino R$ {med_cas:.2f} "
            f"(delta {med_esp-med_cas:+.2f})"
        )

    print("\n[Breakdown por affiliate_id]")
    print(df_by_aff.to_string(index=False))


if __name__ == "__main__":
    main()
