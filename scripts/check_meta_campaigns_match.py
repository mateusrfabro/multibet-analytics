"""
Verifica quantas UTMs/campanhas da lista do gestor de trafego batem com
multibet.fact_ad_spend (Meta API) no periodo 01/03-16/04.
"""
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection
import pandas as pd

UTMS_GESTOR = """Alebahiaodd10 BraboGordinho brabopalmeiras AleFlu AlePalmeiras1 AlePalmeiras
Brabopalmodd13collab Brabopalmdd BraboGordinhoODD10 BraboGordinhoStories brabothiagopalmeiras
Brabocollabflu Braboabertovascoflu BraboGordinho3 AD1_AleEstaticoBahiaPalmeiras domingobrabo10odd
BraboBR31 AleBrazil312 AD2_Brabo_ROAS brabofluminodd13 Brabopalmodd13 AD1_Brabo_ROAS alestoriesBR
AlePalmeiras2 BraboBrasil Braboepj 2classicBraboPalmeiras12 AD1_BraboGordinhoROAS Ale_1
AD2_Brabo_mxconv Ale AleBrasil AleBrazil31 AlePalmeiras12 Ale_2 Ale_3 Ale_5 Alebahia Alermk
Brabo BraboBR BraboBR2 BraboBR312 BraboGordi BraboGordinhoBrasil BraboGordinhoODD1
BraboGordinhoODD101 BraboGordinhoODD11 BraboGordinhobra BraboGordinhobrasil Brabocolla Brabocollab
Brabopal Brabopalm3 Brabopalm3dd Brabopalm3dd3 Brabopalmdd13 ThiagoSoriesBrabo
ThiagoSoriesBraboBotafogo alestories alestoriesB brabo braboflumin braboflumino brabofluminodd
brabothiagopalmeiras2 classicBraboBahiaPalmeiras classicBraboPalmeiras classicBraboPalmeiras12
domingobrabo domingobrabo10 brabothiagopalmeiras AD2_ThiagoPalmeiras AD1_ThiagoPalmeiras
botafogoflamengothiago ThiagoBrasil31 ThiagoBrasil totlivthiago Thiagoobia2 AD1_Thiago_Promocamp
EsporteThiagoSabado Thiago ThiagoBrasil00031 ThiagoSoriesBrabo ThiagoSoriesBraboBotafogo
Thiagonovoproduto Thiagoo Thiagoob Thiagoobia brabothiagopalmeiras2 meta-prante-pxc
prante-sports-march meta-prante-fxi prante-march-keitaro meta-prante-pxj Stories-Esporte
Stories-EsporteBook Sport-teste-quarta""".split()

UTMS_GESTOR = sorted(set(UTMS_GESTOR))
print(f"UTMs unicas na lista do gestor: {len(UTMS_GESTOR)}\n")

tunnel, conn = get_supernova_connection()
cur = conn.cursor()

# 1. Quantos match exato em fact_ad_spend (Meta) no periodo?
cur.execute("""
    SELECT COUNT(DISTINCT campaign_name) AS campanhas_encontradas,
           COUNT(*) AS linhas,
           ROUND(SUM(cost_brl)::numeric,2) AS spend_total
    FROM multibet.fact_ad_spend
    WHERE ad_source = 'meta'
      AND dt BETWEEN '2026-03-01' AND '2026-04-16'
      AND campaign_name = ANY(%s::text[])
""", (UTMS_GESTOR,))
print("[1] Match exato campaign_name = UTM (Meta 01/03-16/04):")
print("   ", cur.fetchone())

# 2. Listar as que bateram
df = pd.read_sql("""
    SELECT campaign_name, COUNT(DISTINCT dt) AS dias_ativa,
           ROUND(SUM(cost_brl)::numeric,2) AS spend_brl,
           SUM(clicks) AS clicks, SUM(conversions) AS conv
    FROM multibet.fact_ad_spend
    WHERE ad_source = 'meta'
      AND dt BETWEEN '2026-03-01' AND '2026-04-16'
      AND campaign_name = ANY(%(utms)s::text[])
    GROUP BY campaign_name
    ORDER BY spend_brl DESC
""", conn, params={"utms": UTMS_GESTOR})
print(f"\n[2] Campanhas da lista que tiveram gasto no periodo: {len(df)}")
print(df.head(15).to_string(index=False))

# 3. Quais da lista NAO apareceram
encontradas = set(df["campaign_name"].tolist())
nao_encontradas = [u for u in UTMS_GESTOR if u not in encontradas]
print(f"\n[3] Da lista NAO encontradas no fact_ad_spend: {len(nao_encontradas)}")
for u in nao_encontradas[:30]: print("   ", u)

# 4. Tamanho total da fact_ad_spend Meta no periodo
cur.execute("""
    SELECT COUNT(DISTINCT campaign_name), ROUND(SUM(cost_brl)::numeric,2)
    FROM multibet.fact_ad_spend
    WHERE ad_source = 'meta'
      AND dt BETWEEN '2026-03-01' AND '2026-04-16'
""")
print("\n[4] fact_ad_spend Meta TOTAL no periodo (todas campanhas):")
print("   ", cur.fetchone())

# 5. Campanhas Meta Sport no periodo (nao batem com lista mas soam esporte)
df2 = pd.read_sql("""
    SELECT campaign_name, ROUND(SUM(cost_brl)::numeric,2) AS spend
    FROM multibet.fact_ad_spend
    WHERE ad_source = 'meta'
      AND dt BETWEEN '2026-03-01' AND '2026-04-16'
      AND (campaign_name ILIKE '%sport%' OR campaign_name ILIKE '%esport%'
           OR campaign_name ILIKE '%ale%' OR campaign_name ILIKE '%brabo%'
           OR campaign_name ILIKE '%thiago%' OR campaign_name ILIKE '%prante%')
    GROUP BY campaign_name
    ORDER BY spend DESC
""", conn)
print(f"\n[5] Todas campanhas Meta com heuristica sports (prefixos): {len(df2)}")

conn.close(); tunnel.stop()
