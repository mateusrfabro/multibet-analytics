"""
Valida quantas das 136 UTMs da lista do gestor aparecem em multibet.trackings
dentro da safra 01/03-16/04 dos IDs 464673 e 532571 (Meta).
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
print(f"UTMs unicas na lista do gestor: {len(UTMS)}")

# cohort Athena (mesma logica do script principal)
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

# UTMs da cohort
df_utm = pd.read_sql("""
    SELECT utm_campaign, COUNT(DISTINCT user_id) AS cadastros
    FROM multibet.trackings
    WHERE user_id = ANY(%(ext)s::text[])
    GROUP BY utm_campaign
    ORDER BY cadastros DESC
""", conn, params={"ext": ext_ids})
print(f"\nUTMs distintas na safra (trackings): {len(df_utm)}")

# Match
encontradas = df_utm[df_utm["utm_campaign"].isin(UTMS)]
nao_em_trackings = [u for u in UTMS if u not in df_utm["utm_campaign"].tolist()]
print(f"\n[1] UTMs da lista gestor que TIVERAM cadastro na safra: {len(encontradas)} / {len(UTMS)}")
print(encontradas.head(20).to_string(index=False))
print(f"\n[2] UTMs da lista que NAO trouxeram cadastro: {len(nao_em_trackings)}")

# UTMs que trouxeram cadastro mas NAO estao na lista (gestor pode estar perdendo algo)
fora_da_lista = df_utm[~df_utm["utm_campaign"].isin(UTMS) & df_utm["utm_campaign"].notna()]
print(f"\n[3] UTMs que trouxeram cadastro mas NAO estao na lista do gestor: {len(fora_da_lista)}")
print(fora_da_lista.head(15).to_string(index=False))

conn.close(); tunnel.stop()
