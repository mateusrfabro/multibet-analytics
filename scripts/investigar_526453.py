"""
Investigar affiliate_id 526453 (AFILIADOS BRASIL LTDA / AffiliatesBR) —
ID nao teve players em ps_bi.dim_user. Hipoteses:
 (a) ID correto mas sem players cadastrados (afiliado inativo)
 (b) Nome cadastrado com ID diferente
 (c) 526453 e tracker_id ou banner_id, nao affiliate_id
"""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")
from db.athena import query_athena

print("=" * 70)
print("1) Busca direta por affiliate_id='526453' em ps_bi.dim_user (SEM filtros)")
print("=" * 70)
q1 = """
SELECT CAST(affiliate_id AS VARCHAR) AS affiliate_id,
       affiliate,
       COUNT(*) AS qty_players,
       SUM(CASE WHEN is_test = true THEN 1 ELSE 0 END) AS qty_test_users,
       MIN(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS first_signup_brt,
       MAX(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS last_signup_brt
FROM ps_bi.dim_user
WHERE CAST(affiliate_id AS VARCHAR) = '526453'
GROUP BY 1, 2
"""
df1 = query_athena(q1, database="ps_bi")
print(df1.to_string(index=False) if len(df1) else "  NENHUM resultado — 526453 nao existe em ps_bi.dim_user")
print()

print("=" * 70)
print("2) Busca por nome 'AFILIADOS BRASIL' ou 'AffiliatesBR' em ps_bi.dim_user")
print("=" * 70)
q2 = """
SELECT CAST(affiliate_id AS VARCHAR) AS affiliate_id,
       affiliate,
       COUNT(*) AS qty_players
FROM ps_bi.dim_user
WHERE LOWER(affiliate) LIKE '%afiliados brasil%'
   OR LOWER(affiliate) LIKE '%affiliatesbr%'
   OR LOWER(affiliate) LIKE '%brasil ltda%'
GROUP BY 1, 2
ORDER BY qty_players DESC
"""
df2 = query_athena(q2, database="ps_bi")
print(df2.to_string(index=False) if len(df2) else "  NENHUM resultado — nome nao existe em ps_bi.dim_user")
print()

print("=" * 70)
print("3) Busca em ecr_ec2.tbl_ecr_banner (fonte bruta de affiliate/tracker/banner)")
print("=" * 70)
q3 = """
SELECT CAST(c_affiliate_id AS VARCHAR) AS c_affiliate_id,
       c_affiliate_name,
       COUNT(*) AS qty
FROM ecr_ec2.tbl_ecr_banner
WHERE CAST(c_affiliate_id AS VARCHAR) = '526453'
   OR CAST(c_tracker_id   AS VARCHAR) = '526453'
   OR CAST(c_banner_id    AS VARCHAR) = '526453'
   OR LOWER(c_affiliate_name) LIKE '%afiliados brasil%'
   OR LOWER(c_affiliate_name) LIKE '%affiliatesbr%'
GROUP BY 1, 2
ORDER BY qty DESC
LIMIT 20
"""
df3 = query_athena(q3, database="ecr_ec2")
print(df3.to_string(index=False) if len(df3) else "  NENHUM resultado — nao existe em ecr_ec2.tbl_ecr_banner")
print()

print("=" * 70)
print("4) Busca em ecr_ec2.tbl_ecr (affiliate_id direto no cadastro)")
print("=" * 70)
q4 = """
SELECT CAST(c_affiliate_id AS VARCHAR) AS c_affiliate_id,
       COUNT(*) AS qty_players
FROM ecr_ec2.tbl_ecr
WHERE CAST(c_affiliate_id AS VARCHAR) = '526453'
GROUP BY 1
"""
df4 = query_athena(q4, database="ecr_ec2")
print(df4.to_string(index=False) if len(df4) else "  NENHUM resultado")
