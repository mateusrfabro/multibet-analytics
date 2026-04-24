"""
Investiga se utm_content (ou outras colunas) da tabela trackings guarda
o nome original do anuncio quando Keitaro reescreveu o utm_campaign.
"""
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection
from db.athena import query_athena
import pandas as pd

UTMS = """josiasbahiaodd10 Alebahiaodd10 BraboGordinho brabopalmeiras AleFlu AlePalmeiras1
AlePalmeiras Brabopalmodd13collab Brabopalmdd BraboGordinhoODD10 BraboGordinhoStories
brabothiagopalmeiras Brabocollabflu Braboabertovascoflu BraboGordinho3
AD1_AleEstaticoBahiaPalmeiras domingobrabo10odd BraboBR31 AleBrazil312 AD2_Brabo_ROAS
brabofluminodd13 Brabopalmodd13 AD1_Brabo_ROAS alestoriesBR AlePalmeiras2 BraboBrasil Braboepj
2classicBraboPalmeiras12 AD1_BraboGordinhoROAS Ale_1 AD2_Brabo_mxconv Ale AleBrasil AleBrazil31
AlePalmeiras12 Ale_2 Ale_3 Ale_5 Alebahia Alermk Brabo BraboBR BraboBR2 BraboBR312 BraboGordi
BraboGordinhoBrasil BraboGordinhoODD1 BraboGordinhoODD101 BraboGordinhoODD11 BraboGordinhobra
BraboGordinhobrasil Brabocolla Brabocollab Brabopal Brabopalm3 Brabopalm3dd Brabopalm3dd3
Brabopalmdd13 ThiagoSoriesBrabo ThiagoSoriesBraboBotafogo alestories alestoriesB brabo
braboflumin braboflumino brabofluminodd brabothiagopalmeiras2 classicBraboBahiaPalmeiras
classicBraboPalmeiras classicBraboPalmeiras12 domingobrabo domingobrabo10 brabothiagopalmeiras
AD2_ThiagoPalmeiras AD1_ThiagoPalmeiras botafogoflamengothiago ThiagoBrasil31 ThiagoBrasil
totlivthiago Thiagoobia2 AD1_Thiago_Promocamp EsporteThiagoSabado Thiago ThiagoBrasil00031
ThiagoSoriesBrabo ThiagoSoriesBraboBotafogo Thiagonovoproduto Thiagoo Thiagoob Thiagoobia
brabothiagopalmeiras2 Empty meta-prante-pxc prante-sports-march meta-prante-fxi
prante-march-keitaro meta-prante-pxj Stories-Esporte Stories-EsporteBook Sport-teste-quarta
1_JosiasBRCR31 josiaspalmeiras2 josias09 josiaspalmodd10 josiasoddvascoxflu josiasbrasil
josiasoddnewcas josiasoddvascoxfluRIO josiasodd13 josiasbrasil2 josiasgalo JosiasJogoQuarta
josiasodd26 josiasestaticobr josiasteste LeadJosiasJogoQuarta josiaspalmeiras josiasmotionbr
AD3_JosiasBRCR31 JosiasnewoDD10 josiaspalmodd13 Josiasdomingo AD1_Josias AD1_JosiasBRCR
AD1_Josias_Influencer_Cassino AD1_Josiastesteganho Josias Josiasarc Josiasnew Josiasnewvideo
Josiasroma josias josiasbahia josiasestatico josiasg josiasglo josiasmotion josiasodd josiasodd3
josiasoddvascoxfluIO josiaspalodd10 josiasvasco josiasvascoestatico""".split()
UTMS = sorted(set([u for u in UTMS if u != "Empty"]))

# cohort Athena
coh = query_athena("""
SELECT ecr_id, external_id, tracker_id
FROM ps_bi.dim_user
WHERE tracker_id IN ('464673','532571')
  AND CAST(signup_datetime AT TIME ZONE 'America/Sao_Paulo' AS DATE) BETWEEN DATE '2026-03-01' AND DATE '2026-04-16'
  AND (is_test IS NULL OR is_test = FALSE)
""", database="ps_bi")
coh["external_id"] = coh["external_id"].astype(str)
ext_ids = coh["external_id"].tolist()
print(f"Cohort Athena: {len(coh)} jogadores")

tunnel, conn = get_supernova_connection()

# 1. Amostra completa do trackings na cohort
df = pd.read_sql("""
    SELECT user_id, utm_campaign, utm_source, utm_content, created_at
    FROM multibet.trackings
    WHERE user_id = ANY(%(ext)s::text[])
""", conn, params={"ext": ext_ids})
print(f"\nLinhas trackings na cohort: {len(df)}")
print(f"Com utm_content preenchido: {df['utm_content'].notna().sum()}")
print(f"Com utm_source preenchido:  {df['utm_source'].notna().sum()}")

# 2. Valores distintos de utm_content (top)
print("\n[utm_content TOP 30 distintos]:")
top_content = df["utm_content"].value_counts().head(30)
print(top_content.to_string())

# 3. Valores distintos de utm_source
print("\n[utm_source TOP 15 distintos]:")
top_source = df["utm_source"].value_counts().head(15)
print(top_source.to_string())

# 4. Quantos utm_content batem com a lista do gestor?
mask_content = df["utm_content"].isin(UTMS)
print(f"\nCadastros com utm_content batendo com a lista do gestor: {mask_content.sum()}")
if mask_content.sum() > 0:
    print(df[mask_content]["utm_content"].value_counts().head(20).to_string())

# 5. Procurar prefixos da lista (Ale/Brabo/Thiago/Josias/Prante) em utm_content
import re
prefixes = ("ale", "brabo", "thiago", "josias", "prante", "stories-esporte", "sport-teste")
def has_prefix(s):
    if s is None: return False
    sl = str(s).lower()
    return any(sl.startswith(p) for p in prefixes)
mask_pref = df["utm_content"].apply(has_prefix)
print(f"\nCadastros com utm_content com prefixo esportivo: {mask_pref.sum()}")
if mask_pref.sum() > 0:
    print(df[mask_pref]["utm_content"].value_counts().head(20).to_string())

conn.close(); tunnel.stop()
