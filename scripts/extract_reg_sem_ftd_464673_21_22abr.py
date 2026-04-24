"""
Extracao: cadastros de 21/04 (D-1) e 22/04 (D-0) sem FTD — affiliate 464673 (Meta).
Demanda: gestor de trafego (Augusto) — base de remarketing WhatsApp/SMS.

Padrao canonico: ps_bi.dim_user + anti-join cashier_ec2 (resolve delay dbt no ftd_date).
Referencia: scripts/extract_reg_sem_ftd_meta_tiktok_19abr.py (19/04, auditado).

Ressalva D-0 (22/04): ps_bi.dim_user tem delay do dbt (~horas) — alguns REGs de hoje
podem nao estar consolidados ainda. Para remarketing, subcontar e aceitavel (melhor que
disparar pra quem ja depositou). Para auditoria/KPI, regerar apos carga dbt (~08h BRT D+1).
"""

import re
import sys
import logging
from datetime import datetime

import pytz

sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BRT = pytz.timezone("America/Sao_Paulo")
hoje = datetime.now(BRT).date()

DATA_INI = "2026-04-21"  # D-1 (ontem)
DATA_FIM = "2026-04-22"  # D-0 (hoje)
AFFILIATE_ID = "464673"
CANAL = "Meta"

log.info(f"Periodo: {DATA_INI} ate {DATA_FIM} | Affiliate: {AFFILIATE_ID} ({CANAL})")
log.info(f"Gerado em: {hoje}")

# Anti-join cashier_ec2 cobre ambos os dias (21/04 00:00 BRT = 21/04 03:00 UTC)
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
    CAST(u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS signup_date_brt,
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
LEFT JOIN ecr_ec2.tbl_ecr_flags f
    ON u.ecr_id = f.c_ecr_id
WHERE u.is_test = false
  AND (f.c_test_user = false OR f.c_test_user IS NULL)
  AND CAST(u.affiliate_id AS VARCHAR) = '{AFFILIATE_ID}'
  AND CAST(u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
        BETWEEN DATE '{DATA_INI}' AND DATE '{DATA_FIM}'
  AND u.ftd_date IS NULL
  AND d.c_ecr_id IS NULL
ORDER BY signup_datetime_brt DESC
"""

log.info("Consultando Athena (ps_bi.dim_user + anti-join cashier_ec2)...")
df = query_athena(sql, database="ps_bi")
log.info(f"Total: {len(df)} cadastros sem FTD")

# Enriquecer
df["canal"] = CANAL
df["nome"] = (
    df["first_name"].fillna("").astype(str).str.strip()
    + " "
    + df["last_name"].fillna("").astype(str).str.strip()
).str.strip()


def normalizar_e164_br(telefone):
    """Normaliza telefone brasileiro para padrao E.164: +55DDDNUMERO.

    Regras:
    - Remove caracteres nao numericos (espacos, parenteses, traco, +)
    - Se ja comeca com '55' e tem 12-13 digitos, mantem
    - Se tem 10-11 digitos (DDD+numero), prefixa '55'
    - Caso contrario, retorna None (invalido para WhatsApp API)
    """
    if telefone is None or str(telefone).strip() in ("", "None", "nan"):
        return None
    digits = re.sub(r"\D", "", str(telefone))
    if not digits:
        return None
    if digits.startswith("55") and len(digits) in (12, 13):
        return f"+{digits}"
    if len(digits) in (10, 11):
        return f"+55{digits}"
    return None  # fora do padrao BR valido


df["mobile_e164"] = df["mobile_number"].apply(normalizar_e164_br)
df["mobile_valido"] = df["mobile_e164"].notna()

# Estatisticas por dia
por_dia = df.groupby("signup_date_brt").size().to_dict()
com_mobile = df["mobile_number"].notna().sum()
com_mobile_valido = int(df["mobile_valido"].sum())
com_email = df["email"].notna().sum()
com_nome = df["nome"].str.len().gt(0).sum()

print("\n" + "=" * 60)
print(f"RESUMO — Cadastros sem FTD (21-22/04) | Affiliate {AFFILIATE_ID} ({CANAL})")
print("=" * 60)
print(f"Total jogadores:     {len(df):,}")
for d, qtd in sorted(por_dia.items()):
    print(f"  - {d}: {qtd:,}")
print(f"Com telefone (raw): {com_mobile:,} ({com_mobile/max(len(df),1)*100:.1f}%)")
print(f"Telefone valido E.164: {com_mobile_valido:,} ({com_mobile_valido/max(len(df),1)*100:.1f}%)")
print(f"Com email:           {com_email:,} ({com_email/max(len(df),1)*100:.1f}%)")
print(f"Com nome:            {com_nome:,} ({com_nome/max(len(df),1)*100:.1f}%)")

# CSV enxuto para disparo
COLS_OUT = [
    "canal", "affiliate_id", "external_id", "nome", "first_name",
    "mobile_number", "mobile_e164", "mobile_valido", "email",
    "signup_date_brt", "signup_datetime_brt",
    "tracker_id", "utm_campaign", "utm_source", "utm_medium",
]
df_out = df[COLS_OUT].copy()

out_csv = f"reports/cadastros_sem_ftd_{AFFILIATE_ID}_21_22abr_FINAL.csv"
out_leg = f"reports/cadastros_sem_ftd_{AFFILIATE_ID}_21_22abr_FINAL_legenda.txt"

df_out.to_csv(out_csv, index=False, encoding="utf-8-sig")
log.info(f"CSV salvo: {out_csv}")

legenda = f"""LEGENDA — cadastros_sem_ftd_{AFFILIATE_ID}_21_22abr_FINAL.csv
{'=' * 60}
Gerado em: {hoje}
Periodo de cadastro: {DATA_INI} (D-1) ate {DATA_FIM} (D-0)
Fonte: ps_bi.dim_user + anti-join cashier_ec2.tbl_cashier_deposit (Athena)
Demanda: gestor de trafego — base de remarketing

FILTROS APLICADOS
-----------------
- affiliate_id = '{AFFILIATE_ID}' ({CANAL})
- signup_datetime (BRT) entre {DATA_INI} e {DATA_FIM}
- ftd_date IS NULL  (nunca fez primeiro deposito no ps_bi)
- is_test = false (ps_bi.dim_user) AND c_test_user = false (ecr_ec2.tbl_ecr_flags)
  — duplo filtro de test_users conforme feedback_test_users_filtro_completo
- ANTI-JOIN cashier_ec2: exclui quem tem deposito c_txn_status='txn_confirmed_success'
  desde {DATA_INI} 00:00 BRT ate agora — resolve delay do dbt em ftd_date

DICIONARIO DE COLUNAS
---------------------
canal                 - Meta (affiliate 464673)
affiliate_id          - ID do afiliado/canal de trafego
external_id           - ID externo do jogador (identificador Smartico)
nome                  - Nome completo (first + last)
first_name            - Primeiro nome — usar na personalizacao da msg
mobile_number         - Celular no formato cru (como cadastrado pelo jogador)
mobile_e164           - Celular normalizado E.164 (+55DDDNumero) — USAR ESTE no
                        disparo via WhatsApp Business API / Zenvia / Twilio
mobile_valido         - True se mobile_e164 passou na normalizacao E.164
email                 - E-mail (alternativa ou complemento)
signup_date_brt       - Data do cadastro (BRT)
signup_datetime_brt   - Data/hora exata do cadastro em BRT
tracker_id            - Sub-tracker dentro do afiliado (campanha/banner)
utm_campaign          - Campanha UTM (quando preenchida)
utm_source            - Fonte UTM (quando preenchida)
utm_medium            - Meio UTM (quando preenchida)

FORMATO DE TELEFONE (E.164)
---------------------------
Padrao aceito por WhatsApp Business API, Zenvia, Twilio, Meta Cloud API:
  +55 (DDI BR) + DDD (2 digitos) + numero (8 ou 9 digitos)
  Ex: +5551999998888
mobile_e164 ja entrega nesse formato. Se o disparo for via API que nao aceita
o '+', remover prefixo antes do envio.

TOTAL: {len(df_out):,} jogadores
"""
for d, qtd in sorted(por_dia.items()):
    legenda += f"  - {d}: {qtd:,}\n"

legenda += f"""
COBERTURA DE CONTATO
--------------------
Com telefone (raw):    {com_mobile:,} ({com_mobile/max(len(df),1)*100:.1f}%)
Telefone valido E.164: {com_mobile_valido:,} ({com_mobile_valido/max(len(df),1)*100:.1f}%)
Com email:             {com_email:,} ({com_email/max(len(df),1)*100:.1f}%)
Com nome:              {com_nome:,} ({com_nome/max(len(df),1)*100:.1f}%)

OBSERVACOES
-----------
1. Base de REMARKETING — cadastraram em Meta mas NAO converteram (sem FTD).
2. Fuso: cadastro em horario BRT (America/Sao_Paulo).
3. D-0 (22/04) esta PARCIAL — o dbt do ps_bi tem delay ~horas. Alguns REGs
   recentes de hoje podem nao ter sido carregados ainda. Para remarketing
   e aceitavel (subcontar > disparar pra quem ja depositou). Para KPI
   consolidado, regerar apos a carga dbt (~08h BRT D+1).
4. Janela de risco residual: jogadores que depositem ENTRE a geracao deste
   arquivo e o momento do disparo. Para janela critica, regerar dentro de
   30min do envio.
5. Validacao cruzada com BigQuery NAO foi feita — acesso BQ desativado (19/04).

GLOSSARIO
---------
FTD = First Time Deposit (primeiro deposito na casa)
D-1 = ontem (21/04) — dia fechado, dados consolidados
D-0 = hoje (22/04) — dia corrente, dados parciais
Anti-join = exclusao de registros que existem em outra tabela
"""

with open(out_leg, "w", encoding="utf-8") as f:
    f.write(legenda)
log.info(f"Legenda salva: {out_leg}")

print(f"\nArquivos:")
print(f"  1. {out_csv}  ({len(df_out):,} linhas)")
print(f"  2. {out_leg}")
