"""
Extrai utm_campaign ORIGINAL do campo c_reference_url de ecr_ec2.tbl_ecr_banner
(antes do Keitaro reescrever) e cruza com a lista do gestor.
"""
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena
import pandas as pd
import re

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

# Puxar tudo de ecr_banner pros dois trackers no periodo
sql = """
SELECT c_ecr_id, c_tracker_id, c_reference_url
FROM ecr_ec2.tbl_ecr_banner
WHERE c_tracker_id IN ('464673','532571')
  AND CAST(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) BETWEEN DATE '2026-03-01' AND DATE '2026-04-16'
  AND c_reference_url IS NOT NULL AND c_reference_url <> ''
"""
df = query_athena(sql, database='ecr_ec2')
print(f"Banners com c_reference_url preenchido: {len(df)}")

# Regex pra extrair utm_campaign
def parse_utm(url):
    m = re.search(r'[?&]utm_campaign=([^&#]+)', str(url))
    return m.group(1) if m else None

df["utm_campaign_original"] = df["c_reference_url"].apply(parse_utm)
df["utm_source_original"]   = df["c_reference_url"].apply(lambda u: (re.search(r'[?&]utm_source=([^&#]+)', str(u)) or [None,None])[1])

com_utm = df[df["utm_campaign_original"].notna()]
print(f"Com utm_campaign parseado: {len(com_utm)} / {len(df)}")

# Top UTMs encontradas
print("\n[Top utm_campaign original nos banners]:")
print(com_utm["utm_campaign_original"].value_counts().head(20).to_string())

# Cruzar com a lista do gestor
mask_bate = com_utm["utm_campaign_original"].isin(UTMS)
print(f"\nBanners com UTM que bate EXATAMENTE com a lista do gestor: {mask_bate.sum()}")
if mask_bate.sum() > 0:
    print("\nUTMs da lista encontradas:")
    print(com_utm[mask_bate]["utm_campaign_original"].value_counts().to_string())

# Case-insensitive match
utms_lower = {u.lower(): u for u in UTMS}
com_utm["_match_ci"] = com_utm["utm_campaign_original"].str.lower().map(utms_lower)
bate_ci = com_utm[com_utm["_match_ci"].notna()]
print(f"\nMatch case-insensitive: {len(bate_ci)}")

# Save amostra
com_utm[["c_ecr_id","c_tracker_id","utm_campaign_original","utm_source_original"]].to_csv(
    "reports/safra_esportivas_464673_532571/_utm_extraida_banner.csv", index=False, encoding="utf-8-sig")
print("\nOK -> reports/safra_esportivas_464673_532571/_utm_extraida_banner.csv")
