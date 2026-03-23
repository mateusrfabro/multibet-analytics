"""
CRM Domingos Analysis — Previsao e Oportunidades para Domingo 23/03/2026
=========================================================================
Objetivo: Analisar dados CRM (Smartico BigQuery) para identificar:
  1. Bonus distribuidos aos domingos (ultimas 4 semanas)
  2. Comunicacoes enviadas aos domingos (push, SMS, email, WhatsApp)
  3. Logins de jogadores aos domingos (engajamento)
  4. Automacoes ativas que potencialmente rodam aos domingos
  5. Depositos aprovados aos domingos (conversao CRM)
  6. Engagements (popups, widgets) aos domingos

Saida: CSVs em output/ + resumo executivo para report ao CTO

Fonte: BigQuery (Smartico CRM) — dataset smartico-bq6.dwh_ext_24105
Schema real validado em 22/03/2026 via INFORMATION_SCHEMA.COLUMNS

Uso:
    cd MultiBet
    python scripts/crm_domingos_analysis.py
"""
import sys
import os
import logging
from datetime import datetime

# Garante que a raiz do projeto esta no path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

import pandas as pd

# Flag de controle: se BigQuery falhar, gera report com analise qualitativa
BIGQUERY_AVAILABLE = True

try:
    from db.bigquery import query_bigquery
    log.info("Modulo BigQuery importado com sucesso")
except Exception as e:
    log.warning(f"BigQuery nao disponivel: {e}")
    BIGQUERY_AVAILABLE = False

# ===========================================================================
# Diretorio de saida
# ===========================================================================
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "output"
)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===========================================================================
# MAPEAMENTO DE IDs SMARTICO (validado via dm_activity_type)
#
# activity_type_id -> canal:
#   30 = Popup, 31 = Inbox, 40 = Push, 50 = Email,
#   60 = SMS, 61 = WhatsApp, 62 = Viber, 63 = CustomIM,
#   64 = Telegram, 100 = Give Bonus, 200 = WebHook
#
# fact_type_id em j_communication -> status do envio:
#   1 = Sent, 2 = Delivered, 3 = Opened/Read,
#   4 = Clicked, 5 = Bounced, 6 = Dismissed,
#   8 = ?, 9 = ?, 10 = ?
#
# bonus_status_id em j_bonuses:
#   1 = Given (bonus concedido)
#   3 = Redeemed (bonus resgatado/usado)
#   4 = Expired (bonus expirado)
# ===========================================================================

# ===========================================================================
# QUERY 1: Bonus distribuidos aos domingos (j_bonuses)
#
# Regra critica (feedback_crm_bonus_isolation):
#   NUNCA filtrar bonus so por template_id — usar duplo filtro
#   entity_id + label_bonus_template_id se necessario isolar campanha.
#   Aqui analisamos TODOS os bonus por domingo, agrupados por
#   entity_id + label_bonus_template_id para manter granularidade.
#
# Schema real (validado 22/03/2026):
#   fact_date (TIMESTAMP), bonus_cost_value (NUMERIC), entity_id (INT64),
#   label_bonus_template_id (INT64), bonus_status_id (INT64),
#   user_ext_id (STRING)
# ===========================================================================
SQL_BONUS_DOMINGOS = """
SELECT
    DATE(fact_date) AS data_domingo,
    entity_id,
    label_bonus_template_id,
    bonus_status_id,
    COUNT(DISTINCT user_ext_id) AS jogadores_unicos,
    COUNT(*) AS total_bonus,
    ROUND(SUM(CAST(bonus_cost_value AS FLOAT64)), 2) AS total_valor_bonus,
    ROUND(AVG(CAST(bonus_cost_value AS FLOAT64)), 2) AS avg_valor_bonus
FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
WHERE
    -- BigQuery: DAYOFWEEK retorna 1=Domingo, 2=Segunda... 7=Sabado
    EXTRACT(DAYOFWEEK FROM fact_date) = 1
    AND DATE(fact_date) >= '2026-02-23'
    AND DATE(fact_date) <= '2026-03-16'
GROUP BY
    DATE(fact_date),
    entity_id,
    label_bonus_template_id,
    bonus_status_id
ORDER BY data_domingo DESC, total_valor_bonus DESC
"""

# ===========================================================================
# QUERY 2: Bonus resumo agregado por domingo (visao macro)
# ===========================================================================
SQL_BONUS_RESUMO = """
SELECT
    DATE(fact_date) AS data_domingo,
    bonus_status_id,
    COUNT(DISTINCT user_ext_id) AS jogadores_com_bonus,
    COUNT(*) AS total_bonus,
    ROUND(SUM(CAST(bonus_cost_value AS FLOAT64)), 2) AS total_valor_bonus,
    ROUND(AVG(CAST(bonus_cost_value AS FLOAT64)), 2) AS avg_valor_bonus,
    COUNT(DISTINCT entity_id) AS campanhas_unicas,
    COUNT(DISTINCT label_bonus_template_id) AS templates_unicos
FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
WHERE
    EXTRACT(DAYOFWEEK FROM fact_date) = 1
    AND DATE(fact_date) >= '2026-02-23'
    AND DATE(fact_date) <= '2026-03-16'
GROUP BY DATE(fact_date), bonus_status_id
ORDER BY data_domingo DESC, bonus_status_id
"""

# ===========================================================================
# QUERY 3: Comunicacoes enviadas aos domingos (j_communication)
#
# Schema real: fact_date (TIMESTAMP), activity_type_id (INT64),
#   fact_type_id (INT64), user_ext_id (STRING)
#
# Mapeamento activity_type_id:
#   30=Popup, 40=Push, 50=Email, 60=SMS, 61=WhatsApp, 100=Give Bonus
# Mapeamento fact_type_id:
#   1=Sent, 2=Delivered, 3=Opened, 4=Clicked, 5=Bounced, 6=Dismissed
# ===========================================================================
SQL_COMUNICACOES_DOMINGOS = """
SELECT
    DATE(fact_date) AS data_domingo,
    activity_type_id,
    CASE activity_type_id
        WHEN 30 THEN 'Popup'
        WHEN 31 THEN 'Inbox'
        WHEN 40 THEN 'Push'
        WHEN 50 THEN 'Email'
        WHEN 60 THEN 'SMS'
        WHEN 61 THEN 'WhatsApp'
        WHEN 64 THEN 'Telegram'
        WHEN 100 THEN 'Give Bonus'
        WHEN 200 THEN 'WebHook'
        ELSE CAST(activity_type_id AS STRING)
    END AS canal,
    fact_type_id,
    CASE fact_type_id
        WHEN 1 THEN 'Sent'
        WHEN 2 THEN 'Delivered'
        WHEN 3 THEN 'Opened'
        WHEN 4 THEN 'Clicked'
        WHEN 5 THEN 'Bounced'
        WHEN 6 THEN 'Dismissed'
        ELSE CAST(fact_type_id AS STRING)
    END AS status_envio,
    COUNT(DISTINCT user_ext_id) AS jogadores_alcancados,
    COUNT(*) AS total_envios
FROM `smartico-bq6.dwh_ext_24105.j_communication`
WHERE
    EXTRACT(DAYOFWEEK FROM fact_date) = 1
    AND DATE(fact_date) >= '2026-02-23'
    AND DATE(fact_date) <= '2026-03-16'
    -- Filtrar apenas canais de comunicacao direta
    AND activity_type_id IN (40, 50, 60, 61, 64, 30, 31)
GROUP BY
    DATE(fact_date),
    activity_type_id,
    fact_type_id
ORDER BY data_domingo DESC, activity_type_id, fact_type_id
"""

# ===========================================================================
# QUERY 4: Logins por domingo (tr_login)
#
# Schema real: event_time (TIMESTAMP), user_id (INT64)
# NOTA: tr_login NAO tem user_ext_id — usar user_id
# ===========================================================================
SQL_LOGINS_DOMINGOS = """
SELECT
    DATE(event_time) AS data_domingo,
    EXTRACT(HOUR FROM event_time) AS hora_utc,
    COUNT(DISTINCT user_id) AS jogadores_logados,
    COUNT(*) AS total_logins
FROM `smartico-bq6.dwh_ext_24105.tr_login`
WHERE
    EXTRACT(DAYOFWEEK FROM event_time) = 1
    AND DATE(event_time) >= '2026-02-23'
    AND DATE(event_time) <= '2026-03-16'
GROUP BY
    DATE(event_time),
    EXTRACT(HOUR FROM event_time)
ORDER BY data_domingo DESC, hora_utc
"""

# ===========================================================================
# QUERY 5: Automacoes CRM ativas (dm_automation_rule)
# ===========================================================================
SQL_AUTOMACOES = """
SELECT
    rule_id,
    rule_name,
    is_active,
    rule_type_id,
    rule_control_group_percents,
    create_date,
    update_date,
    bo_user_email
FROM `smartico-bq6.dwh_ext_24105.dm_automation_rule`
WHERE is_active = true
ORDER BY update_date DESC
LIMIT 100
"""

# ===========================================================================
# QUERY 6: Depositos aprovados aos domingos (tr_acc_deposit_approved)
#
# Schema real: event_time (TIMESTAMP), user_id (INT64),
#   acc_last_deposit_amount (NUMERIC)
# ===========================================================================
SQL_DEPOSITOS_DOMINGOS = """
SELECT
    DATE(event_time) AS data_domingo,
    EXTRACT(HOUR FROM event_time) AS hora_utc,
    COUNT(DISTINCT user_id) AS jogadores_depositantes,
    COUNT(*) AS total_depositos,
    ROUND(SUM(CAST(acc_last_deposit_amount AS FLOAT64)), 2) AS total_valor_depositos,
    ROUND(AVG(CAST(acc_last_deposit_amount AS FLOAT64)), 2) AS avg_valor_deposito
FROM `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved`
WHERE
    EXTRACT(DAYOFWEEK FROM event_time) = 1
    AND DATE(event_time) >= '2026-02-23'
    AND DATE(event_time) <= '2026-03-16'
GROUP BY
    DATE(event_time),
    EXTRACT(HOUR FROM event_time)
ORDER BY data_domingo DESC, hora_utc
"""

# ===========================================================================
# QUERY 7: Engagements por domingo (j_engagements)
#
# Schema real: create_date (TIMESTAMP), user_ext_id (STRING),
#   activity_type_id (INT64), event_type_id (INT64)
# ===========================================================================
SQL_ENGAGEMENTS = """
SELECT
    DATE(create_date) AS data_domingo,
    activity_type_id,
    CASE activity_type_id
        WHEN 30 THEN 'Popup'
        WHEN 31 THEN 'Inbox'
        WHEN 40 THEN 'Push'
        WHEN 50 THEN 'Email'
        WHEN 60 THEN 'SMS'
        WHEN 100 THEN 'Give Bonus'
        ELSE CAST(activity_type_id AS STRING)
    END AS tipo_engagement,
    COUNT(DISTINCT user_ext_id) AS jogadores_unicos,
    COUNT(*) AS total_engagements
FROM `smartico-bq6.dwh_ext_24105.j_engagements`
WHERE
    EXTRACT(DAYOFWEEK FROM create_date) = 1
    AND DATE(create_date) >= '2026-02-23'
    AND DATE(create_date) <= '2026-03-16'
GROUP BY
    DATE(create_date),
    activity_type_id
ORDER BY data_domingo DESC, total_engagements DESC
"""


def execute_query_safe(sql, name):
    """Executa query no BigQuery com tratamento de erro."""
    if not BIGQUERY_AVAILABLE:
        log.warning(f"[{name}] BigQuery indisponivel -- pulando")
        return None
    try:
        log.info(f"[{name}] Executando query...")
        df = query_bigquery(sql)
        log.info(f"[{name}] Retornou {len(df)} linhas")
        return df
    except Exception as e:
        log.error(f"[{name}] Erro: {e}")
        return None


def save_csv(df, filename, label):
    """Salva DataFrame como CSV e loga."""
    if df is not None and not df.empty:
        path = os.path.join(OUTPUT_DIR, filename)
        df.to_csv(path, index=False, encoding="utf-8-sig")
        log.info(f"[{label}] CSV salvo: {path}")
        return path
    return None


def analyze_bonus(df_detail, df_resumo):
    """Analisa bonus distribuidos aos domingos."""
    print("\n" + "=" * 80)
    print("1. BONUS DISTRIBUIDOS AOS DOMINGOS (Ultimas 4 semanas)")
    print("=" * 80)

    # Mapeia bonus_status_id para nomes
    status_map = {1: "Given", 3: "Redeemed", 4: "Expired"}

    if df_resumo is not None and not df_resumo.empty:
        # Filtra apenas bonus "Given" (status_id=1) para visao principal
        df_given = df_resumo[df_resumo['bonus_status_id'] == 1].copy()
        if not df_given.empty:
            print("\nBonus concedidos (Given) por domingo:")
            print("-" * 80)
            for _, row in df_given.iterrows():
                print(
                    f"  {row['data_domingo']} | "
                    f"Jogadores: {row['jogadores_com_bonus']:,} | "
                    f"Bonus: {row['total_bonus']:,} | "
                    f"Valor total: R$ {row['total_valor_bonus']:,.2f} | "
                    f"Avg: R$ {row['avg_valor_bonus']:,.2f} | "
                    f"Campanhas: {row['campanhas_unicas']} | "
                    f"Templates: {row['templates_unicos']}"
                )

            # Tendencia
            if len(df_given) >= 2:
                ultimos = df_given.sort_values('data_domingo', ascending=False)
                u1 = ultimos.iloc[0]
                u2 = ultimos.iloc[1]
                var_val = ((u1['total_valor_bonus'] - u2['total_valor_bonus'])
                           / u2['total_valor_bonus'] * 100) if u2['total_valor_bonus'] > 0 else 0
                var_jog = ((u1['jogadores_com_bonus'] - u2['jogadores_com_bonus'])
                           / u2['jogadores_com_bonus'] * 100) if u2['jogadores_com_bonus'] > 0 else 0
                seta_val = "+" if var_val > 0 else ""
                seta_jog = "+" if var_jog > 0 else ""
                print(f"\n  Tendencia ultimo vs penultimo domingo:")
                print(f"    Valor bonus: {seta_val}{var_val:.1f}%")
                print(f"    Jogadores:   {seta_jog}{var_jog:.1f}%")

        # Mostra todos os status
        print(f"\nResumo completo por status:")
        print("-" * 80)
        for _, row in df_resumo.iterrows():
            sname = status_map.get(row['bonus_status_id'], f"Status {row['bonus_status_id']}")
            print(
                f"  {row['data_domingo']} | {sname:10s} | "
                f"Jogadores: {row['jogadores_com_bonus']:,} | "
                f"Bonus: {row['total_bonus']:,} | "
                f"Valor: R$ {row['total_valor_bonus']:,.2f}"
            )

        save_csv(df_resumo, "crm_bonus_domingos_2026-03-22.csv", "Bonus Resumo")
    else:
        print("  [SEM DADOS] Query de bonus nao retornou resultados")

    if df_detail is not None and not df_detail.empty:
        # Top campanhas (entity_id) por valor de bonus (apenas Given)
        df_given_detail = df_detail[df_detail['bonus_status_id'] == 1].copy()
        if not df_given_detail.empty:
            top_campanhas = (
                df_given_detail.groupby(['entity_id', 'label_bonus_template_id'])
                .agg({
                    'total_valor_bonus': 'sum',
                    'jogadores_unicos': 'sum',
                    'total_bonus': 'sum'
                })
                .sort_values('total_valor_bonus', ascending=False)
                .head(10)
            )
            print("\n  Top 10 campanhas de bonus aos domingos (entity_id + template_id):")
            print("  " + "-" * 80)
            for idx, (ids, row) in enumerate(top_campanhas.iterrows(), 1):
                eid, tid = ids
                print(
                    f"  #{idx:2d} | entity={eid} template={tid} | "
                    f"Jogadores: {int(row['jogadores_unicos']):,} | "
                    f"Bonus: {int(row['total_bonus']):,} | "
                    f"Valor: R$ {row['total_valor_bonus']:,.2f}"
                )

        save_csv(df_detail, "crm_bonus_domingos_detail_2026-03-22.csv", "Bonus Detail")


def analyze_communications(df):
    """Analisa comunicacoes (push, SMS, email) enviadas aos domingos."""
    print("\n" + "=" * 80)
    print("2. COMUNICACOES CRM AOS DOMINGOS (Push, SMS, Email, WhatsApp)")
    print("=" * 80)

    if df is not None and not df.empty:
        # Resumo por canal e status (soma dos 4 domingos)
        resumo_canal = (
            df.groupby(['canal', 'status_envio'])
            .agg({
                'jogadores_alcancados': 'sum',
                'total_envios': 'sum'
            })
            .sort_values('total_envios', ascending=False)
        )
        print("\nResumo por canal e status (soma dos 4 domingos):")
        print("-" * 75)
        for (canal, status), row in resumo_canal.iterrows():
            print(
                f"  {canal:12s} | {status:12s} | "
                f"Jogadores: {int(row['jogadores_alcancados']):>8,} | "
                f"Envios: {int(row['total_envios']):>8,}"
            )

        # Resumo por domingo (volume total)
        resumo_dia = (
            df.groupby('data_domingo')
            .agg({
                'jogadores_alcancados': 'sum',
                'total_envios': 'sum'
            })
            .sort_values('data_domingo', ascending=False)
        )
        print(f"\nVolume total de comunicacoes por domingo:")
        print("-" * 55)
        for data, row in resumo_dia.iterrows():
            print(
                f"  {data} | "
                f"Jogadores alcancados: {int(row['jogadores_alcancados']):>8,} | "
                f"Total envios: {int(row['total_envios']):>8,}"
            )

        # Taxa de conversao por canal (sent -> clicked)
        print(f"\nTaxa de conversao por canal (Sent -> Clicked):")
        print("-" * 75)
        for canal in df['canal'].unique():
            canal_data = df[df['canal'] == canal]
            sent = canal_data[canal_data['fact_type_id'] == 1]['total_envios'].sum()
            delivered = canal_data[canal_data['fact_type_id'] == 2]['total_envios'].sum()
            opened = canal_data[canal_data['fact_type_id'] == 3]['total_envios'].sum()
            clicked = canal_data[canal_data['fact_type_id'] == 4]['total_envios'].sum()
            if sent > 0:
                print(
                    f"  {canal:12s} | "
                    f"Sent: {sent:>8,} | "
                    f"Delivered: {delivered:>8,} ({delivered/sent*100:.1f}%) | "
                    f"Opened: {opened:>8,} ({opened/sent*100:.1f}%) | "
                    f"Clicked: {clicked:>8,} ({clicked/sent*100:.1f}%)"
                )

        save_csv(df, "crm_comunicacoes_domingos_2026-03-22.csv", "Comunicacoes")
    else:
        print("  [SEM DADOS] Query de comunicacoes nao retornou resultados")


def analyze_logins(df):
    """Analisa logins por domingo e identifica horarios de pico."""
    print("\n" + "=" * 80)
    print("3. LOGINS DE JOGADORES AOS DOMINGOS (Engajamento)")
    print("=" * 80)

    if df is not None and not df.empty:
        # Total por domingo
        resumo_dia = (
            df.groupby('data_domingo')
            .agg({
                'jogadores_logados': 'sum',
                'total_logins': 'sum'
            })
            .sort_values('data_domingo', ascending=False)
        )
        print("\nTotal logins por domingo:")
        print("-" * 55)
        for data, row in resumo_dia.iterrows():
            print(
                f"  {data} | "
                f"Jogadores unicos: {int(row['jogadores_logados']):>8,} | "
                f"Total logins: {int(row['total_logins']):>8,}"
            )

        # Horarios de pico (UTC -> BRT = UTC-3)
        pico_hora = (
            df.groupby('hora_utc')
            .agg({
                'total_logins': 'sum',
                'jogadores_logados': 'sum'
            })
            .sort_values('total_logins', ascending=False)
        )
        print(f"\nTop 5 horarios de PICO de login (media dos 4 domingos):")
        print("-" * 60)
        for hora, row in pico_hora.head(5).iterrows():
            hora_brt = (int(hora) - 3) % 24
            print(
                f"  {int(hora):02d}h UTC ({hora_brt:02d}h BRT) | "
                f"Logins: {int(row['total_logins']):>8,} | "
                f"Jogadores: {int(row['jogadores_logados']):>8,}"
            )

        # Horas mortas
        mortas = pico_hora.sort_values('total_logins', ascending=True)
        print(f"\nTop 5 HORAS MORTAS de login:")
        print("-" * 60)
        for hora, row in mortas.head(5).iterrows():
            hora_brt = (int(hora) - 3) % 24
            print(
                f"  {int(hora):02d}h UTC ({hora_brt:02d}h BRT) | "
                f"Logins: {int(row['total_logins']):>8,} | "
                f"Jogadores: {int(row['jogadores_logados']):>8,}"
            )

        save_csv(df, "crm_logins_domingos_2026-03-22.csv", "Logins")
    else:
        print("  [SEM DADOS] Query de logins nao retornou resultados")


def analyze_automations(df):
    """Lista automacoes ativas do CRM."""
    print("\n" + "=" * 80)
    print("4. AUTOMACOES CRM ATIVAS (dm_automation_rule)")
    print("=" * 80)

    if df is not None and not df.empty:
        print(f"\nTotal de automacoes ativas: {len(df)}")
        print("-" * 90)
        for _, row in df.head(25).iterrows():
            name = str(row.get('rule_name', 'N/A'))[:55]
            print(
                f"  ID: {row['rule_id']:>8} | "
                f"{name:55s} | "
                f"Atualizado: {str(row.get('update_date', 'N/A'))[:19]}"
            )
        if len(df) > 25:
            print(f"  ... e mais {len(df) - 25} automacoes ativas")

        save_csv(df, "crm_automacoes_ativas_2026-03-22.csv", "Automacoes")
    else:
        print("  [SEM DADOS] Query de automacoes nao retornou resultados")


def analyze_deposits(df):
    """Analisa depositos por domingo e hora."""
    print("\n" + "=" * 80)
    print("5. DEPOSITOS AOS DOMINGOS (Conversao CRM)")
    print("=" * 80)

    if df is not None and not df.empty:
        resumo_dia = (
            df.groupby('data_domingo')
            .agg({
                'jogadores_depositantes': 'sum',
                'total_depositos': 'sum',
                'total_valor_depositos': 'sum'
            })
            .sort_values('data_domingo', ascending=False)
        )
        print("\nDepositos por domingo:")
        print("-" * 75)
        for data, row in resumo_dia.iterrows():
            print(
                f"  {data} | "
                f"Jogadores: {int(row['jogadores_depositantes']):>7,} | "
                f"Depositos: {int(row['total_depositos']):>7,} | "
                f"Valor total: R$ {row['total_valor_depositos']:>12,.2f}"
            )

        # Horario de pico de depositos
        pico_dep = (
            df.groupby('hora_utc')
            .agg({
                'total_depositos': 'sum',
                'total_valor_depositos': 'sum'
            })
            .sort_values('total_valor_depositos', ascending=False)
        )
        print(f"\nTop 5 horarios de PICO de deposito:")
        print("-" * 65)
        for hora, row in pico_dep.head(5).iterrows():
            hora_brt = (int(hora) - 3) % 24
            print(
                f"  {int(hora):02d}h UTC ({hora_brt:02d}h BRT) | "
                f"Depositos: {int(row['total_depositos']):>7,} | "
                f"Valor: R$ {row['total_valor_depositos']:>12,.2f}"
            )

        # Horas mortas de deposito
        mortas_dep = pico_dep.sort_values('total_valor_depositos', ascending=True)
        print(f"\nTop 5 HORAS MORTAS de deposito:")
        print("-" * 65)
        for hora, row in mortas_dep.head(5).iterrows():
            hora_brt = (int(hora) - 3) % 24
            print(
                f"  {int(hora):02d}h UTC ({hora_brt:02d}h BRT) | "
                f"Depositos: {int(row['total_depositos']):>7,} | "
                f"Valor: R$ {row['total_valor_depositos']:>12,.2f}"
            )

        save_csv(df, "crm_depositos_domingos_2026-03-22.csv", "Depositos")
    else:
        print("  [SEM DADOS] Query de depositos nao retornou resultados")


def analyze_engagements(df):
    """Analisa engagements (popups, widgets) por domingo."""
    print("\n" + "=" * 80)
    print("6. ENGAGEMENTS AOS DOMINGOS (Popups, Inbox, etc.)")
    print("=" * 80)

    if df is not None and not df.empty:
        resumo = (
            df.groupby(['data_domingo', 'tipo_engagement'])
            .agg({
                'jogadores_unicos': 'sum',
                'total_engagements': 'sum'
            })
            .sort_values(['data_domingo', 'total_engagements'], ascending=[False, False])
        )
        print("\nEngagements por domingo e tipo:")
        print("-" * 70)
        for (data, tipo), row in resumo.iterrows():
            print(
                f"  {data} | {tipo:12s} | "
                f"Jogadores: {int(row['jogadores_unicos']):>8,} | "
                f"Total: {int(row['total_engagements']):>8,}"
            )

        save_csv(df, "crm_engagements_domingos_2026-03-22.csv", "Engagements")
    else:
        print("  [SEM DADOS] Query de engagements nao retornou resultados")


def generate_executive_summary(results):
    """Gera resumo executivo com recomendacoes para domingo 23/03/2026."""

    has_data = any(
        v is not None and not v.empty
        for v in results.values()
        if isinstance(v, pd.DataFrame)
    )

    print("\n" + "=" * 80)
    print("=" * 80)
    print("  RESUMO EXECUTIVO -- PREVISAO CRM DOMINGO 23/03/2026")
    print("  Para: CTO (Gabriel Barbosa)")
    print("  De: Squad Intelligence Engine")
    print("  Data: 22/03/2026")
    print("=" * 80)
    print("=" * 80)

    if has_data:
        # Calcular metricas reais
        df_dep = results.get('depositos')
        df_login = results.get('logins')
        df_bonus = results.get('bonus_resumo')
        df_comm = results.get('comunicacoes')

        if df_dep is not None and not df_dep.empty:
            dep_por_domingo = (
                df_dep.groupby('data_domingo')
                .agg({'total_depositos': 'sum', 'total_valor_depositos': 'sum', 'jogadores_depositantes': 'sum'})
            )
            avg_dep = dep_por_domingo['total_valor_depositos'].mean()
            avg_dep_count = dep_por_domingo['total_depositos'].mean()
            avg_dep_players = dep_por_domingo['jogadores_depositantes'].mean()
            print(f"\n  BASELINE DE DOMINGOS (media ultimas 4 semanas):")
            print(f"    Depositos: {avg_dep_count:,.0f} transacoes/domingo")
            print(f"    Valor medio: R$ {avg_dep:,.2f}/domingo")
            print(f"    Jogadores depositantes: {avg_dep_players:,.0f}/domingo")

        if df_login is not None and not df_login.empty:
            login_por_domingo = (
                df_login.groupby('data_domingo')
                .agg({'jogadores_logados': 'sum', 'total_logins': 'sum'})
            )
            avg_logins = login_por_domingo['total_logins'].mean()
            avg_players = login_por_domingo['jogadores_logados'].mean()
            print(f"    Logins: {avg_logins:,.0f} sessoes/domingo")
            print(f"    Jogadores logados: {avg_players:,.0f}/domingo")

        if df_bonus is not None and not df_bonus.empty:
            given = df_bonus[df_bonus['bonus_status_id'] == 1]
            if not given.empty:
                bonus_por_domingo = given.groupby('data_domingo').agg({'total_valor_bonus': 'sum', 'total_bonus': 'sum'})
                avg_bonus_val = bonus_por_domingo['total_valor_bonus'].mean()
                avg_bonus_cnt = bonus_por_domingo['total_bonus'].mean()
                print(f"    Bonus concedidos: {avg_bonus_cnt:,.0f}/domingo (R$ {avg_bonus_val:,.2f})")

        if df_comm is not None and not df_comm.empty:
            comm_por_domingo = df_comm.groupby('data_domingo').agg({'total_envios': 'sum'})
            avg_comm = comm_por_domingo['total_envios'].mean()
            print(f"    Comunicacoes CRM: {avg_comm:,.0f} envios/domingo")

    print("""
--------------------------------------------------------------------------------
OPORTUNIDADES CRM PARA DOMINGO 23/03/2026
--------------------------------------------------------------------------------

1. RE-DEPOSIT CAMPAIGN (Sabado -> Domingo)
   ----------------------------------------
   TARGET: Jogadores que depositaram sabado (22/03) mas nao depositaram
           no domingo anterior (16/03).
   MECANICA: Push notification as 12h BRT + email as 14h BRT
   BONUS: 10-20% cashback no primeiro deposito do domingo (cap R$ 50)
   RACIONAL: Jogador que deposita sabado tem habito de jogo no fim de semana.
             O domingo e a "extensao natural" do comportamento. O cashback
             reduz a barreira para um segundo deposito no mesmo fim de semana.
   TIMING: Enviar entre 11h-13h BRT (almoco, momento de decisao).

2. PUSH NOTIFICATIONS NOS HORARIOS DE PICO DO DOMINGO
   ---------------------------------------------------
   ESTRATEGIA: 3 ondas de push
     - 12h BRT: "Seu domingo de sorte comeca agora" (awareness)
     - 18h BRT: "Bonus flash ate 21h" (urgencia, pico de logins)
     - 21h BRT: "Ultima chance do domingo" (FOMO, encerramento)
   SEGMENTACAO:
     - Onda 1: Base geral ativa (login ultimos 30 dias)
     - Onda 2: Jogadores que logaram hoje mas NAO depositaram
     - Onda 3: Jogadores que depositaram mas pararam de jogar

3. BONUS FLASH PARA HORAS MORTAS
   --------------------------------
   MECANICA: Free Spins instantaneos no login durante horas mortas
     - 05h-08h BRT: 10 Free Spins (valor minimo, custo baixo)
     - 08h-10h BRT: 5 Free Spins + 20% deposit match (cap R$ 30)
   OBJETIVO: Aumentar o volume nas horas de menor atividade.
             Domingo de manha tem potencial por ser dia de folga.
   JOGOS SUGERIDOS: Fortune Tiger, Fortune Dragon (PG Soft,
                    integrados na campanha Multiverso)

4. REATIVACAO DE DORMENTES "JOGADORES DE DOMINGO"
   ------------------------------------------------
   TARGET: Jogadores com padrao historico de login/deposito aos domingos
           que pararam nos ultimos 15-30 dias.
   BONUS SUGERIDO:
     - Dormentes 15-20 dias: R$ 10 Free Bet ou 15 Free Spins
     - Dormentes 20-30 dias: R$ 20 Free Bet ou 25 Free Spins + 30% match
   ESTIMATIVA: Com base no report dormant_whales (21/03), ha centenas de
               whales dormentes. Mesmo 5-10% de conversao gera impacto.

5. CAMPANHA MULTIVERSO -- BOOST DOMINICAL
   ----------------------------------------
   CONTEXTO: Campanha Multiverso (6 Fortune Games, PG Soft) ativa
             desde 13/03/2026 com challenges e quests.
   OPORTUNIDADE: "Boost dominical" para a Multiverso:
     - Progressao 2x nos challenges aos domingos
     - Free Spins extras para quem completar quest no domingo
   RACIONAL: Aproveita infra existente (bot anti-abuse rodando).

6. SEGMENTACAO POR TIER
   ----------------------------------------
   WHALES (dep > R$ 500/dia):
     -> Contato pessoal VIP manager (WhatsApp/ligacao)
     -> Oferta exclusiva: 30% deposit match sem cap
   REGULAR (R$ 100-500/mes):
     -> Push + email automatizado
     -> 15% deposit match (cap R$ 75)
   CASUAL (< R$ 100/mes):
     -> Push generico com Free Spins
     -> Custo por jogador baixo, volume alto
   DORMENTES (15-30 dias sem deposito):
     -> SMS + Push com oferta personalizada

--------------------------------------------------------------------------------
METRICAS A MONITORAR NO DOMINGO 23/03
--------------------------------------------------------------------------------

  KPI                          | Meta sugerida    | Fonte
  -----------------------------|------------------|------------------
  NRC (novos registros)        | > media semanal  | BigQuery j_user
  FTD (primeiro deposito)      | > media semanal  | BigQuery j_bonuses
  UDC (depositantes unicos)    | +10% vs dom ant  | tr_acc_deposit_approved
  Logins unicos                | +5% vs dom ant   | tr_login
  Bonus distribuidos (valor)   | Dentro do budget | j_bonuses
  Taxa abertura push           | > 15%            | j_communication
  Taxa conversao push->dep     | > 3%             | j_communication + tr_acc
  Reativacoes (dormentes)      | > 50 jogadores   | Athena ps_bi

--------------------------------------------------------------------------------
RISCOS E MITIGACOES
--------------------------------------------------------------------------------

  1. ABUSO DE BONUS: Bot anti-abuse Multiverso ja ativo. Monitorar
     novos patterns aos domingos (volume pode ser diferente).

  2. CANIBALISMO DE CANAIS: Nao enviar push + SMS + email no mesmo
     horario. Escalonar: push primeiro, email 2h depois, SMS so
     para quem nao abriu push.

  3. ORCAMENTO DE BONUS: Domingos historicamente tem mais jogadores.
     Garantir que o budget de bonus comporta o volume.
     Sugestao: cap individual + cap global da campanha.

  4. HORARIO DE FUTEBOL: Se houver jogos do Brasileirao no domingo,
     atencao do jogador pode estar no Sportsbook.
     Oportunidade: cross-sell casino durante intervalo dos jogos.
""")


def save_legenda():
    """Salva legenda/dicionario da entrega (padrao obrigatorio CLAUDE.md)."""
    legenda_path = os.path.join(OUTPUT_DIR, "crm_domingos_analysis_2026-03-22_legenda.txt")
    with open(legenda_path, "w", encoding="utf-8") as f:
        f.write("LEGENDA -- Analise CRM Domingos (Previsao 23/03/2026)\n")
        f.write("=" * 60 + "\n\n")
        f.write("FONTE: BigQuery (Smartico CRM) -- dataset smartico-bq6.dwh_ext_24105\n")
        f.write("DATA DE EXTRACAO: 22/03/2026\n")
        f.write("PERIODO ANALISADO: 23/02/2026 a 16/03/2026 (4 domingos)\n")
        f.write("DESTINO: Report para CTO -- previsao domingo 23/03/2026\n\n")
        f.write("ARQUIVOS GERADOS:\n")
        f.write("-" * 60 + "\n")
        f.write("crm_bonus_domingos_2026-03-22.csv\n")
        f.write("  Bonus por domingo (resumo agregado por bonus_status_id)\n")
        f.write("  bonus_status_id: 1=Given, 3=Redeemed, 4=Expired\n")
        f.write("  bonus_cost_value em moeda da label (BRL)\n\n")
        f.write("crm_bonus_domingos_detail_2026-03-22.csv\n")
        f.write("  Bonus por domingo por campanha (entity_id + label_bonus_template_id)\n")
        f.write("  Para analise de quais campanhas performam melhor aos domingos\n\n")
        f.write("crm_comunicacoes_domingos_2026-03-22.csv\n")
        f.write("  Comunicacoes por domingo, canal e status\n")
        f.write("  canal: Push, Email, SMS, WhatsApp, Popup, Inbox\n")
        f.write("  status_envio: Sent, Delivered, Opened, Clicked, Bounced, Dismissed\n\n")
        f.write("crm_logins_domingos_2026-03-22.csv\n")
        f.write("  Logins por domingo e hora (UTC). Converter -3h para BRT.\n\n")
        f.write("crm_automacoes_ativas_2026-03-22.csv\n")
        f.write("  Lista de automacoes CRM ativas no Smartico\n\n")
        f.write("crm_depositos_domingos_2026-03-22.csv\n")
        f.write("  Depositos aprovados por domingo e hora (UTC)\n")
        f.write("  acc_last_deposit_amount em moeda da label (BRL)\n\n")
        f.write("crm_engagements_domingos_2026-03-22.csv\n")
        f.write("  Engagements (popups, inbox) por domingo e tipo\n\n")
        f.write("GLOSSARIO:\n")
        f.write("-" * 60 + "\n")
        f.write("NRC     = New Registered Customers (novos registros)\n")
        f.write("FTD     = First Time Depositors (primeiro deposito)\n")
        f.write("UDC     = Unique Depositing Customers (depositantes unicos)\n")
        f.write("GGR     = Gross Gaming Revenue (apostas - ganhos jogador)\n")
        f.write("entity_id   = ID da campanha/entidade no Smartico\n")
        f.write("label_bonus_template_id = ID do template de bonus\n")
        f.write("REGRA: entity_id + template_id = identificacao unica de campanha\n")
        f.write("       Nunca filtrar so por template_id (caso Multiverso/RETEM).\n\n")
        f.write("MAPEAMENTO activity_type_id (canais Smartico):\n")
        f.write("  30=Popup, 31=Inbox, 40=Push, 50=Email\n")
        f.write("  60=SMS, 61=WhatsApp, 64=Telegram\n")
        f.write("  100=Give Bonus, 200=WebHook\n\n")
        f.write("MAPEAMENTO fact_type_id (status envio j_communication):\n")
        f.write("  1=Sent, 2=Delivered, 3=Opened, 4=Clicked\n")
        f.write("  5=Bounced, 6=Dismissed\n\n")
        f.write("MAPEAMENTO bonus_status_id (j_bonuses):\n")
        f.write("  1=Given (concedido), 3=Redeemed (usado), 4=Expired\n")

    log.info(f"Legenda salva: {legenda_path}")


def main():
    log.info("=" * 80)
    log.info("CRM DOMINGOS ANALYSIS -- Previsao para 23/03/2026")
    log.info("Fonte: BigQuery (Smartico CRM)")
    log.info("Schema validado via INFORMATION_SCHEMA.COLUMNS em 22/03/2026")
    log.info("=" * 80)

    # ===========================================================================
    # Executa todas as queries
    # ===========================================================================
    results = {}

    results['bonus_detail'] = execute_query_safe(SQL_BONUS_DOMINGOS, "Bonus Detalhado")
    results['bonus_resumo'] = execute_query_safe(SQL_BONUS_RESUMO, "Bonus Resumo")
    results['comunicacoes'] = execute_query_safe(SQL_COMUNICACOES_DOMINGOS, "Comunicacoes")
    results['logins'] = execute_query_safe(SQL_LOGINS_DOMINGOS, "Logins")
    results['automacoes'] = execute_query_safe(SQL_AUTOMACOES, "Automacoes")
    results['depositos'] = execute_query_safe(SQL_DEPOSITOS_DOMINGOS, "Depositos")
    results['engagements'] = execute_query_safe(SQL_ENGAGEMENTS, "Engagements")

    # ===========================================================================
    # Analisa e imprime resultados
    # ===========================================================================
    analyze_bonus(results['bonus_detail'], results['bonus_resumo'])
    analyze_communications(results['comunicacoes'])
    analyze_logins(results['logins'])
    analyze_automations(results['automacoes'])
    analyze_deposits(results['depositos'])
    analyze_engagements(results['engagements'])

    # ===========================================================================
    # Resumo executivo com recomendacoes
    # ===========================================================================
    generate_executive_summary(results)

    # ===========================================================================
    # Salva legenda (padrao obrigatorio)
    # ===========================================================================
    save_legenda()

    # ===========================================================================
    # Contagem final
    # ===========================================================================
    csvs_gerados = [
        f for f in os.listdir(OUTPUT_DIR)
        if f.startswith("crm_") and f.endswith("2026-03-22.csv")
    ]
    print(f"\n{'=' * 80}")
    print(f"ANALISE CONCLUIDA")
    print(f"CSVs gerados em output/: {len(csvs_gerados)}")
    for c in sorted(csvs_gerados):
        print(f"  - {c}")
    print(f"Legenda: output/crm_domingos_analysis_2026-03-22_legenda.txt")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
