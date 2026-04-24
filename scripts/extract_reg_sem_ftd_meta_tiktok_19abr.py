"""
Extracao: cadastros de ontem (D-1 = sabado 18/04) sem FTD — Meta e TikTok.
Demanda do Augusto (gestor de trafego Multibet) em 19/04/2026 — disparo WhatsApp/SMS.

Augusto pediu: nome, telefone e external_id.
Campos adicionais incluidos: canal (Meta/TikTok), first_name separado (personalizar msg).

Affiliates:
  - 464673 -> Meta
  - 477668 -> TikTok

Fonte: Athena ps_bi.dim_user (dia fechado, dbt ja carregou).
Obs: BigQuery Smartico foi desativado — sem cross-validation real-time.
"""

import sys
import logging
from datetime import timedelta, datetime

import pandas as pd
import pytz

sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BRT = pytz.timezone("America/Sao_Paulo")
hoje = datetime.now(BRT).date()
d1 = hoje - timedelta(days=1)

AFFILIATES = {
    "464673": "Meta",
    "477668": "TikTok",
}
IDS_SQL = ", ".join(f"'{k}'" for k in AFFILIATES.keys())

log.info(f"Data alvo (D-1 BRT): {d1} | Gerado em: {hoje}")
log.info(f"Affiliates: {AFFILIATES}")

# Anti-join contra cashier_ec2 resolve o delay do dbt em ftd_date (auditor, 19/04):
# quem depositou DEPOIS da ultima carga dbt ainda aparece com ftd_date=NULL no ps_bi.
# Filtramos todos que tem deposito confirmado >= D-1 00:00 BRT (= D-1 03:00 UTC).
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
      AND c_created_time >= TIMESTAMP '{d1} 03:00:00'
) d ON u.ecr_id = d.c_ecr_id
WHERE u.is_test = false
  AND CAST(u.affiliate_id AS VARCHAR) IN ({IDS_SQL})
  AND CAST(u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{d1}'
  AND u.ftd_date IS NULL
  AND d.c_ecr_id IS NULL
ORDER BY signup_datetime_brt DESC
"""

log.info("Consultando Athena (ps_bi.dim_user)...")
df = query_athena(sql, database="ps_bi")
log.info(f"Athena: {len(df)} cadastros sem FTD em {d1}")

# Enriquecer
df["canal"] = df["affiliate_id"].map(AFFILIATES).fillna("OUTRO")
df["nome"] = (
    df["first_name"].fillna("").str.strip()
    + " "
    + df["last_name"].fillna("").str.strip()
).str.strip()

# Estatisticas
por_canal = df.groupby("canal").size().to_dict()
com_mobile = df["mobile_number"].notna().sum()
com_nome = df["nome"].str.len().gt(0).sum()

print("\n" + "=" * 60)
print(f"RESUMO — Cadastros sem FTD em {d1} (Meta + TikTok)")
print("=" * 60)
print(f"Total jogadores:     {len(df):,}")
for canal, qtd in por_canal.items():
    print(f"  - {canal:<7}: {qtd:,}")
print(f"Com telefone:        {com_mobile:,} ({com_mobile/max(len(df),1)*100:.1f}%)")
print(f"Com nome:            {com_nome:,} ({com_nome/max(len(df),1)*100:.1f}%)")

# CSV enxuto — o que o Augusto pediu
COLS_OUT = [
    "canal", "affiliate_id", "external_id", "nome", "first_name",
    "mobile_number", "email",
    "signup_datetime_brt", "tracker_id", "utm_campaign",
]
df_out = df[COLS_OUT].copy()

out_csv = f"reports/cadastros_sem_ftd_meta_tiktok_{d1}_FINAL_v2.csv"
out_leg = f"reports/cadastros_sem_ftd_meta_tiktok_{d1}_FINAL_v2_legenda.txt"

df_out.to_csv(out_csv, index=False, encoding="utf-8-sig")
log.info(f"CSV salvo: {out_csv}")

legenda = f"""LEGENDA — cadastros_sem_ftd_meta_tiktok_{d1}_FINAL_v2.csv
{'=' * 60}
Gerado em: {hoje} (versao auditada)
Periodo de cadastro: {d1} (sabado, dia fechado BRT)
Fonte: ps_bi.dim_user + anti-join cashier_ec2.tbl_cashier_deposit (Athena)
Demanda: Augusto (gestor de trafego Multibet) — disparo WhatsApp/SMS

FILTROS APLICADOS
-----------------
- affiliate_id IN ('464673', '477668')  -> Meta e TikTok
- signup_datetime (BRT) = {d1}
- ftd_date IS NULL  (nunca fez primeiro deposito no ps_bi)
- is_test = false  (exclui contas de teste)
- ANTI-JOIN cashier_ec2: exclui quem tem deposito c_txn_status='txn_confirmed_success'
  desde {d1} 00:00 BRT ate agora — resolve delay do dbt em ftd_date

DICIONARIO DE COLUNAS
---------------------
canal                 - Meta (464673) ou TikTok (477668)
affiliate_id          - ID do afiliado/canal de trafego
external_id           - ID externo do jogador (identificador Smartico)
nome                  - Nome completo (first + last)
first_name            - Primeiro nome — usar na personalizacao da msg
mobile_number         - Celular (para disparo WhatsApp/SMS)
email                 - E-mail (alternativa ou complemento)
signup_datetime_brt   - Data/hora exata do cadastro em BRT
tracker_id            - Sub-tracker dentro do afiliado (campanha)
utm_campaign          - Campanha UTM (quando preenchida)

TOTAL: {len(df_out):,} jogadores
  - Meta:   {por_canal.get('Meta', 0):,}
  - TikTok: {por_canal.get('TikTok', 0):,}

COBERTURA DE CONTATO
--------------------
Com telefone: {com_mobile:,} ({com_mobile/max(len(df),1)*100:.1f}%)
Com nome:     {com_nome:,} ({com_nome/max(len(df),1)*100:.1f}%)

OBSERVACOES
-----------
1. Base de REMARKETING — todos cadastraram nas Super Odds mas NAO converteram (sem FTD).
2. Fuso: cadastro em horario BRT (America/Sao_Paulo), ponto-de-corte 00:00-23:59 de {d1}.
3. v2 (auditada): corrige 16 jogadores que apareciam na v1 mas ja tinham
   deposito hoje entre 07:01-07:33 BRT (delay do dbt em ftd_date). O anti-join
   contra cashier_ec2.tbl_cashier_deposit foi adicionado no SQL.
4. Validacao cruzada com BigQuery NAO foi feita — acesso BQ desativado em 19/04.
5. Janela de risco residual: jogadores que depositem ENTRE a geracao deste arquivo
   e o momento do disparo. Para janela critica, regerar dentro de 30min do envio.

GLOSSARIO
---------
FTD = First Time Deposit (primeiro deposito na casa)
D-1 = ontem (dia fechado, dados consolidados)
"""

with open(out_leg, "w", encoding="utf-8") as f:
    f.write(legenda)
log.info(f"Legenda salva: {out_leg}")

print(f"\nArquivos:")
print(f"  1. {out_csv}  ({len(df_out):,} linhas)")
print(f"  2. {out_leg}")
