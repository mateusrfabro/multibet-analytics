"""
Extracao: base remarketing Sportsbook - Meta e TikTok - 2 meses corridos (20/02 a 19/04/2026)

Demanda do Augusto (gestor de trafego Multibet) em 20/04/2026.
Objetivo: remarketing Meta de quem cadastrou via campanhas de esporte nos ultimos
2 meses mas NAO fez FTD. Augusto pediu arquivos SEPARADOS por canal.

Affiliates (confirmados Augusto 20/04):
  - 464673 -> Meta Sportsbook
  - 532571 -> Meta Sportsbook
  - 477668 -> TikTok Sportsbook

Fonte: ps_bi.dim_user (Athena) + anti-join cashier_ec2 (resolve delay dbt).
Obs: BigQuery Smartico desativado - sem cross-validation real-time.

Padrao de entrega:
  - 1 CSV Meta + legenda
  - 1 CSV TikTok + legenda
  - 1 ZIP com os 4 arquivos
  - Report resumo em stdout (pra copiar no WhatsApp)
"""

import sys
import logging
import zipfile
from datetime import datetime

import pandas as pd
import pytz

sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BRT = pytz.timezone("America/Sao_Paulo")
hoje = datetime.now(BRT).date()

# Janela: 2 meses corridos, D-1 fechado
DATA_INI = "2026-02-20"
DATA_FIM = "2026-04-19"

AFFILIATES = {
    "464673": "Meta",
    "532571": "Meta",
    "477668": "TikTok",
}
IDS_SQL = ", ".join(f"'{k}'" for k in AFFILIATES.keys())

log.info(f"Janela: {DATA_INI} a {DATA_FIM} (2 meses corridos)")
log.info(f"Affiliates: {AFFILIATES}")
log.info(f"Gerado em: {hoje}")

# SQL: cadastros no periodo, sem FTD, anti-join contra depositos confirmados
# Anti-join cobre delay dbt em ftd_date (feedback_base_sem_ftd_anti_join_cashier.md)
sql = f"""
SELECT
    u.ecr_id                                              AS player_id,
    u.external_id,
    u.first_name,
    u.last_name,
    u.email,
    u.mobile_number,
    CAST(u.affiliate_id AS VARCHAR)                       AS affiliate_id,
    u.tracker_id,
    u.signup_channel,
    u.signup_device,
    u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS signup_datetime_brt,
    u.utm_source,
    u.utm_medium,
    u.utm_campaign
FROM ps_bi.dim_user u
LEFT JOIN (
    SELECT DISTINCT c_ecr_id
    FROM cashier_ec2.tbl_cashier_deposit
    WHERE c_txn_status = 'txn_confirmed_success'
      AND c_created_time >= TIMESTAMP '{DATA_INI} 03:00:00'
) d ON u.ecr_id = d.c_ecr_id
WHERE u.is_test = false
  AND CAST(u.affiliate_id AS VARCHAR) IN ({IDS_SQL})
  AND CAST(u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
      BETWEEN DATE '{DATA_INI}' AND DATE '{DATA_FIM}'
  AND u.ftd_date IS NULL
  AND d.c_ecr_id IS NULL
ORDER BY signup_datetime_brt DESC
"""

log.info("Consultando Athena (ps_bi.dim_user + anti-join cashier_ec2)...")
df = query_athena(sql, database="ps_bi")
log.info(f"Athena: {len(df):,} cadastros sem FTD no periodo")

# Enriquecer
df["canal"] = df["affiliate_id"].map(AFFILIATES).fillna("OUTRO")
df["nome"] = (
    df["first_name"].fillna("").str.strip()
    + " "
    + df["last_name"].fillna("").str.strip()
).str.strip()

# Separar por canal
df_meta = df[df["canal"] == "Meta"].copy()
df_tiktok = df[df["canal"] == "TikTok"].copy()

# Sanity checks
def sanity(d, label):
    dup_ext = d["external_id"].duplicated().sum()
    dup_mob = d[d["mobile_number"].notna()]["mobile_number"].duplicated().sum()
    com_mobile = d["mobile_number"].notna().sum()
    com_email = d["email"].notna().sum()
    com_nome = d["nome"].str.len().gt(0).sum()
    return {
        "canal": label,
        "total": len(d),
        "dup_external_id": int(dup_ext),
        "dup_mobile_number": int(dup_mob),
        "com_mobile": f"{com_mobile:,} ({com_mobile/max(len(d),1)*100:.1f}%)",
        "com_email": f"{com_email:,} ({com_email/max(len(d),1)*100:.1f}%)",
        "com_nome": f"{com_nome:,} ({com_nome/max(len(d),1)*100:.1f}%)",
    }

chk_meta = sanity(df_meta, "Meta")
chk_tiktok = sanity(df_tiktok, "TikTok")

# Quebra por affiliate_id dentro de Meta
meta_por_id = df_meta.groupby("affiliate_id").size().to_dict()

# === Output ===
COLS_OUT = [
    "canal", "affiliate_id", "external_id", "nome", "first_name",
    "mobile_number", "email",
    "signup_datetime_brt", "tracker_id", "utm_campaign",
]

def salvar(d, canal_nome, ids_incluidos):
    out_csv = f"reports/remarketing_sportsbook_{canal_nome.lower()}_{DATA_INI}_{DATA_FIM}_FINAL.csv"
    out_leg = f"reports/remarketing_sportsbook_{canal_nome.lower()}_{DATA_INI}_{DATA_FIM}_FINAL_legenda.txt"

    d[COLS_OUT].to_csv(out_csv, index=False, encoding="utf-8-sig")

    legenda = f"""LEGENDA - remarketing_sportsbook_{canal_nome.lower()}_{DATA_INI}_{DATA_FIM}_FINAL.csv
{'=' * 70}
Gerado em: {hoje}
Periodo de cadastro: {DATA_INI} a {DATA_FIM} (2 meses corridos, D-1 fechado BRT)
Canal: {canal_nome}
Affiliates incluidos: {', '.join(ids_incluidos)}
Demanda: Augusto (gestor de trafego Multibet) - remarketing Meta/TikTok de esporte

FILTROS APLICADOS
-----------------
- affiliate_id IN ({', '.join(ids_incluidos)})  -> {canal_nome} Sportsbook
- signup_datetime (BRT) entre {DATA_INI} e {DATA_FIM}
- ftd_date IS NULL  (nunca fez primeiro deposito no ps_bi)
- is_test = false  (exclui contas de teste)
- ANTI-JOIN cashier_ec2: exclui quem tem deposito confirmado
  (c_txn_status='txn_confirmed_success') desde {DATA_INI} 00:00 BRT ate agora -
  resolve delay do dbt em ftd_date

DICIONARIO DE COLUNAS
---------------------
canal                 - {canal_nome}
affiliate_id          - ID do afiliado/canal de trafego
external_id           - ID externo do jogador (identificador Smartico)
nome                  - Nome completo (first + last)
first_name            - Primeiro nome (usar na personalizacao da msg)
mobile_number         - Celular (para disparo WhatsApp/SMS/Meta Ads audience)
email                 - E-mail (para Meta Ads custom audience)
signup_datetime_brt   - Data/hora exata do cadastro em BRT
tracker_id            - Sub-tracker dentro do afiliado (campanha/criativo)
utm_campaign          - Campanha UTM (quando preenchida)

TOTAL: {len(d):,} jogadores
COBERTURA
  Com telefone: {d['mobile_number'].notna().sum():,} ({d['mobile_number'].notna().sum()/max(len(d),1)*100:.1f}%)
  Com email:    {d['email'].notna().sum():,} ({d['email'].notna().sum()/max(len(d),1)*100:.1f}%)
  Com nome:     {d['nome'].str.len().gt(0).sum():,} ({d['nome'].str.len().gt(0).sum()/max(len(d),1)*100:.1f}%)

OBSERVACOES
-----------
1. Base de REMARKETING - todos cadastraram via campanhas de esporte mas NAO
   converteram (sem FTD ate {hoje}).
2. Fuso: cadastro em horario BRT (America/Sao_Paulo).
3. Sobre a lista de criativos (BraboGordinho, Josias, Thiago, etc.): nosso
   sistema nao recebe o nome do criativo puro porque o redirect intermediario
   (Keitaro) reescreve os parametros antes de chegar. Por isso a extracao foi
   feita pelos AFFILIATES de esporte (nivel canal) em vez dos criativos
   individuais - pega todo mundo que caiu via Meta/TikTok esporte no periodo.
4. Validacao cruzada com BigQuery NAO feita (acesso desativado em 19/04).
5. Janela de risco residual: jogadores que depositem ENTRE a geracao e o envio
   ao Meta Ads. Para janela critica, regerar em ate 30min do upload.

GLOSSARIO
---------
FTD = First Time Deposit (primeiro deposito na casa)
"""
    with open(out_leg, "w", encoding="utf-8") as f:
        f.write(legenda)

    log.info(f"Salvo: {out_csv} ({len(d):,} linhas)")
    log.info(f"Salvo: {out_leg}")
    return out_csv, out_leg

meta_csv, meta_leg = salvar(df_meta, "Meta", ["464673", "532571"])
tiktok_csv, tiktok_leg = salvar(df_tiktok, "TikTok", ["477668"])

# ZIP com tudo
zip_path = f"reports/remarketing_sportsbook_{DATA_INI}_{DATA_FIM}_FINAL.zip"
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
    for p in [meta_csv, meta_leg, tiktok_csv, tiktok_leg]:
        z.write(p, arcname=p.split("/")[-1])
log.info(f"ZIP criado: {zip_path}")

# === Resumo no stdout (pro WhatsApp) ===
print("\n" + "=" * 70)
print(f"RESUMO - Remarketing Sportsbook - {DATA_INI} a {DATA_FIM}")
print("=" * 70)
print(f"\nTotal geral: {len(df):,} jogadores sem FTD no periodo")
print(f"\nPor canal:")
print(f"  Meta (464673 + 532571): {len(df_meta):,}")
for aid, qtd in meta_por_id.items():
    print(f"     - {aid}: {qtd:,}")
print(f"  TikTok (477668):        {len(df_tiktok):,}")

print(f"\nSanity check - Meta:")
for k, v in chk_meta.items():
    print(f"  {k:<22}: {v}")

print(f"\nSanity check - TikTok:")
for k, v in chk_tiktok.items():
    print(f"  {k:<22}: {v}")

print(f"\nArquivos gerados:")
print(f"  1. {meta_csv}")
print(f"  2. {meta_leg}")
print(f"  3. {tiktok_csv}")
print(f"  4. {tiktok_leg}")
print(f"  5. {zip_path}  <- mandar esse pro Augusto")
