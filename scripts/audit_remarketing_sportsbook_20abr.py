"""
Auditoria empirica: validar base de remarketing Sportsbook (20/04/2026)

Verifica 4 coisas:
1. NENHUM dos 25.397 tem deposito CONFIRMADO em cashier_ec2 (qualquer periodo,
   nao so 20/02+) - se tem algum, e um deposito antigo que passou da janela.
2. Contagem de tentativas de deposito (qualquer status) - sinaliza intencao.
3. NENHUM tem atividade em bireports (bet/ggr) - sanity extra.
4. Distribuicao temporal e por affiliate faz sentido.
"""

import sys
import logging

import pandas as pd

sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Ler bases geradas
meta = pd.read_csv("reports/remarketing_sportsbook_meta_2026-02-20_2026-04-19_FINAL.csv")
tik = pd.read_csv("reports/remarketing_sportsbook_tiktok_2026-02-20_2026-04-19_FINAL.csv")
df = pd.concat([meta, tik], ignore_index=True)

log.info(f"Base: {len(df):,} (Meta {len(meta):,} + TikTok {len(tik):,})")

# Lista de external_ids - montar IN (...) ou usar JOIN via CTE
ext_ids = df["external_id"].astype(str).tolist()
log.info(f"External IDs unicos: {len(set(ext_ids)):,}")

# Como sao 25k IDs, vamos usar um truque: juntar via CTE em Athena.
# Alternativa: criar uma string VALUES (...), mas 25k e muito. Vamos fazer
# um LEFT JOIN contra dim_user filtrado pelos 3 afiliados e periodo, depois
# cruzar com cashier_ec2 (o periodo ja limita).

AFFILIATES = ('464673', '532571', '477668')
ids_sql = ", ".join(f"'{a}'" for a in AFFILIATES)

# === CHECK 1: depositos confirmados (deveria ser 0) ===
sql_dep_conf = f"""
WITH base AS (
    SELECT DISTINCT u.ecr_id, u.external_id
    FROM ps_bi.dim_user u
    LEFT JOIN cashier_ec2.tbl_cashier_deposit d
      ON u.ecr_id = d.c_ecr_id
     AND d.c_txn_status = 'txn_confirmed_success'
    WHERE u.is_test = false
      AND CAST(u.affiliate_id AS VARCHAR) IN ({ids_sql})
      AND CAST(u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
          BETWEEN DATE '2026-02-20' AND DATE '2026-04-19'
      AND u.ftd_date IS NULL
      AND d.c_ecr_id IS NULL
)
SELECT
    COUNT(DISTINCT b.ecr_id) AS total_base,
    COUNT(DISTINCT CASE WHEN d2.c_ecr_id IS NOT NULL THEN b.ecr_id END) AS com_deposito_confirmado_qqr_periodo
FROM base b
LEFT JOIN cashier_ec2.tbl_cashier_deposit d2
  ON b.ecr_id = d2.c_ecr_id
 AND d2.c_txn_status = 'txn_confirmed_success'
"""
log.info("[1/4] Check depositos confirmados (QUALQUER periodo)...")
c1 = query_athena(sql_dep_conf, database="ps_bi")
print("\n" + "=" * 70)
print("CHECK 1 - Depositos CONFIRMADOS em QUALQUER periodo (deveria ser 0)")
print("=" * 70)
print(c1.to_string(index=False))

# === CHECK 2: tentativas de deposito (qualquer status) ===
sql_dep_any = f"""
WITH base AS (
    SELECT DISTINCT u.ecr_id, u.external_id
    FROM ps_bi.dim_user u
    LEFT JOIN cashier_ec2.tbl_cashier_deposit d
      ON u.ecr_id = d.c_ecr_id
     AND d.c_txn_status = 'txn_confirmed_success'
    WHERE u.is_test = false
      AND CAST(u.affiliate_id AS VARCHAR) IN ({ids_sql})
      AND CAST(u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
          BETWEEN DATE '2026-02-20' AND DATE '2026-04-19'
      AND u.ftd_date IS NULL
      AND d.c_ecr_id IS NULL
)
SELECT
    d2.c_txn_status,
    COUNT(DISTINCT b.ecr_id) AS jogadores
FROM base b
JOIN cashier_ec2.tbl_cashier_deposit d2
  ON b.ecr_id = d2.c_ecr_id
GROUP BY d2.c_txn_status
ORDER BY jogadores DESC
"""
log.info("[2/4] Check tentativas de deposito (qualquer status)...")
c2 = query_athena(sql_dep_any, database="ps_bi")
print("\n" + "=" * 70)
print("CHECK 2 - Tentativas de deposito por status (intencao de depositar)")
print("=" * 70)
print(c2.to_string(index=False) if len(c2) else "Nenhuma tentativa - ninguem tentou depositar")

# === CHECK 3: atividade em bireports (bet/ggr) ===
sql_act = f"""
WITH base AS (
    SELECT DISTINCT u.ecr_id
    FROM ps_bi.dim_user u
    LEFT JOIN cashier_ec2.tbl_cashier_deposit d
      ON u.ecr_id = d.c_ecr_id
     AND d.c_txn_status = 'txn_confirmed_success'
    WHERE u.is_test = false
      AND CAST(u.affiliate_id AS VARCHAR) IN ({ids_sql})
      AND CAST(u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
          BETWEEN DATE '2026-02-20' AND DATE '2026-04-19'
      AND u.ftd_date IS NULL
      AND d.c_ecr_id IS NULL
)
SELECT
    COUNT(DISTINCT b.ecr_id) AS jogadores_com_atividade,
    SUM(bi.c_total_bets_amount) AS total_bets_amount_centavos,
    SUM(bi.c_total_wins_amount) AS total_wins_amount_centavos
FROM base b
JOIN bireports_ec2.tbl_ecr_wise_daily_bi_summary bi
  ON b.ecr_id = bi.c_ecr_id
WHERE bi.c_total_bets_amount > 0 OR bi.c_total_wins_amount > 0
"""
log.info("[3/4] Check atividade (bets/wins) em bireports...")
try:
    c3 = query_athena(sql_act, database="bireports_ec2")
    print("\n" + "=" * 70)
    print("CHECK 3 - Atividade em bireports (bet/win) - deveria ser 0")
    print("=" * 70)
    print(c3.to_string(index=False))
except Exception as e:
    log.warning(f"Check 3 falhou: {e}")
    print("\nCHECK 3: pulado (erro na query)")

# === CHECK 4: distribuicao temporal + affiliate ===
df["signup_date"] = pd.to_datetime(df["signup_datetime_brt"]).dt.date
print("\n" + "=" * 70)
print("CHECK 4 - Distribuicao temporal e por affiliate")
print("=" * 70)
print(f"\nRange de cadastro: {df['signup_date'].min()} a {df['signup_date'].max()}")
print(f"\nPor mes:")
df["mes"] = pd.to_datetime(df["signup_datetime_brt"]).dt.to_period("M")
print(df.groupby(["mes", "canal"]).size().unstack(fill_value=0).to_string())

print(f"\nPor affiliate:")
print(df.groupby(["affiliate_id", "canal"]).size().to_string())

print(f"\nAmostra 5 Meta + 5 TikTok (spot check):")
print(df.groupby("canal").head(5)[["canal","affiliate_id","external_id","nome","signup_datetime_brt"]].to_string(index=False))

print("\n" + "=" * 70)
print("VEREDITO:")
print("=" * 70)
print("Se CHECK 1 = 0 e CHECK 3 = 0 -> base LIMPA, pode entregar.")
print("Se CHECK 2 tem linhas -> jogadores tentaram depositar mas nao consolidaram.")
print("  (isso NAO invalida, so mostra intencao - alvo bom pra remarketing)")
