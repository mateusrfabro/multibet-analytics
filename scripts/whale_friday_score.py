"""
whale_friday_score.py — Weekend Whale Watch: Feature Engineering & Scoring
===========================================================================
Objetivo: Identificar jogadores de alto valor (VIPs/Whales) que depositam
nas sextas-feiras, usando modelo RFM + Behavioral com score composto.

Fonte de dados: Athena (ps_bi + bireports_ec2)
Autor: Squad 3 — Intelligence Engine
Data: 2026-03-20

Racional do modelo:
    O modelo combina 4 dimensoes classicas de segmentacao:
    - Recency (R): quao recente e o comportamento do jogador
    - Frequency (F): quao consistente e o padrao de deposito nas sextas
    - Monetary (M): volume financeiro dos depositos
    - Behavioral (B): sinais de engajamento e valor para a casa

    O score final (0-100) pondera essas dimensoes com pesos calibrados
    para o contexto iGaming, onde Monetary e Frequency nas sextas sao
    os indicadores mais fortes de um "whale de fim de semana".
"""

import pandas as pd
import numpy as np
import logging
import sys
import os
from datetime import datetime, timedelta

# Adiciona raiz do projeto ao path para importar db/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db.athena import query_athena

# ---------------------------------------------------------------------------
# Configuracao de logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ===========================================================================
# CONSTANTES DO MODELO
# ===========================================================================

# Janela de analise: 90 dias atras a partir de hoje
JANELA_DIAS = 90

# Numero de sextas-feiras possiveis em 90 dias (ceil(90/7) = 13)
TOTAL_SEXTAS_90D = 13

# Pesos do score final (somam 100%)
# Racional dos pesos:
#   - Monetary (35%): maior peso porque define o "whale" — volume financeiro
#   - Frequency (30%): consistencia de sexta e o padrao que queremos prever
#   - Behavioral (20%): GGR e ratio mostram valor real para a casa
#   - Recency (15%): importante mas menos diferenciador entre jogadores ativos
PESO_RECENCY = 0.15
PESO_FREQUENCY = 0.30
PESO_MONETARY = 0.35
PESO_BEHAVIORAL = 0.20


# ===========================================================================
# QUERIES SQL (Athena — ps_bi)
# ===========================================================================

def build_sql_features(data_referencia: str, janela_dias: int = JANELA_DIAS) -> str:
    """
    Gera a SQL que extrai todas as features brutas do Athena.
    Usa ps_bi (preferido — valores ja em BRL, pre-agregado).
    """
    dt_ref = datetime.strptime(data_referencia, '%Y-%m-%d')
    dt_inicio = dt_ref - timedelta(days=janela_dias)
    data_inicio = dt_inicio.strftime('%Y-%m-%d')

    # Inicio da semana corrente (segunda-feira)
    dias_desde_segunda = dt_ref.weekday()  # 0=Monday
    dt_inicio_semana = dt_ref - timedelta(days=dias_desde_segunda)
    data_inicio_semana = dt_inicio_semana.strftime('%Y-%m-%d')

    sql = f"""
    -- =========================================================================
    -- Weekend Whale Watch — Feature Extraction
    -- Data referencia: {data_referencia} | Janela: {janela_dias} dias
    -- Fonte: ps_bi (BRL, pre-agregado) + bireports_ec2 (login, test_user)
    -- Regras: is_test=false, valores BRL (sem /100), timezone BRT nos timestamps
    -- =========================================================================

    WITH
    -- -----------------------------------------------------------------
    -- 1. Base de jogadores reais (exclui test users)
    -- Fonte: bireports_ec2.tbl_ecr (c_test_user = boolean)
    -- -----------------------------------------------------------------
    jogadores_reais AS (
        SELECT
            e.c_ecr_id,
            e.c_external_id,
            -- Recency: dias desde ultimo login (convertido para BRT)
            date_diff(
                'day',
                CAST(e.c_last_login_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE),
                DATE '{data_referencia}'
            ) AS dias_desde_ultimo_login
        FROM bireports_ec2.tbl_ecr e
        WHERE e.c_test_user = false
    ),

    -- -----------------------------------------------------------------
    -- 2. Depositos diarios nos ultimos 90 dias (ps_bi — valores em BRL)
    -- Colunas validadas em crm_kpis_marco_2026_corrigido.py
    -- -----------------------------------------------------------------
    depositos AS (
        SELECT
            d.player_id,
            d.created_date,
            -- day_of_week: 1=Monday ... 5=Friday ... 7=Sunday (ISO/Presto)
            day_of_week(CAST(d.created_date AS DATE)) AS dow,
            d.success_amount_local AS valor_deposito_brl,
            d.success_count
        FROM ps_bi.fct_deposits_daily d
        -- Filtro test users via dim_user (is_test validado empiricamente)
        INNER JOIN ps_bi.dim_user u ON d.player_id = u.ecr_id
        WHERE u.is_test = false
          AND CAST(d.created_date AS DATE) >= DATE '{data_inicio}'
          AND CAST(d.created_date AS DATE) <= DATE '{data_referencia}'
          AND d.success_count > 0
    ),

    -- -----------------------------------------------------------------
    -- 3. Metricas de deposito agregadas por jogador
    -- -----------------------------------------------------------------
    metricas_deposito AS (
        SELECT
            dep.player_id,

            -- RECENCY: dias desde ultimo deposito
            date_diff(
                'day',
                MAX(dep.created_date),
                DATE '{data_referencia}'
            ) AS dias_desde_ultimo_deposito,

            -- Depositou nesta semana? (desde segunda)
            MAX(CASE WHEN CAST(dep.created_date AS DATE) >= DATE '{data_inicio_semana}' THEN 1 ELSE 0 END)
                AS depositou_esta_semana,

            -- FREQUENCY: sextas com deposito nos ultimos 90 dias
            COUNT(DISTINCT CASE WHEN dep.dow = 5 THEN dep.created_date END)
                AS total_sextas_com_deposito_90d,

            -- Frequencia semanal media = total depositos / numero de semanas
            CAST(SUM(dep.success_count) AS DOUBLE) / GREATEST(({janela_dias} / 7.0), 1.0)
                AS frequencia_semanal_media,

            -- MONETARY: ticket medio SOMENTE nas sextas
            CASE
                WHEN COUNT(CASE WHEN dep.dow = 5 THEN 1 END) > 0
                THEN SUM(CASE WHEN dep.dow = 5 THEN dep.valor_deposito_brl ELSE 0.0 END)
                     / COUNT(DISTINCT CASE WHEN dep.dow = 5 THEN dep.created_date END)
                ELSE 0.0
            END AS valor_medio_deposito_sexta,

            -- Total depositado no periodo (BRL)
            SUM(dep.valor_deposito_brl) AS valor_total_depositos_90d,

            -- Maior deposito individual
            MAX(dep.valor_deposito_brl) AS valor_max_deposito

        FROM depositos dep
        GROUP BY dep.player_id
    ),

    -- -----------------------------------------------------------------
    -- 4. Atividade consolidada — GGR e saques (ps_bi, valores em BRL)
    -- -----------------------------------------------------------------
    atividade AS (
        SELECT
            a.player_id,
            -- GGR 90d (pre-calculado no ps_bi, ja em BRL)
            SUM(COALESCE(a.ggr_local, 0)) AS ggr_90d,
            -- Total depositado e sacado para ratio
            SUM(COALESCE(a.deposit_success_local, 0)) AS total_dep_brl,
            SUM(COALESCE(a.cashout_success_local, 0)) AS total_saq_brl
        FROM ps_bi.fct_player_activity_daily a
        WHERE CAST(a.activity_date AS DATE) >= DATE '{data_inicio}'
          AND CAST(a.activity_date AS DATE) <= DATE '{data_referencia}'
        GROUP BY a.player_id
    )

    -- -----------------------------------------------------------------
    -- 5. JOIN FINAL — monta dataset com todas as features
    -- -----------------------------------------------------------------
    SELECT
        j.c_ecr_id                                        AS player_id,
        j.c_external_id                                   AS external_id,

        -- RECENCY (R)
        COALESCE(md.dias_desde_ultimo_deposito, {janela_dias}) AS dias_desde_ultimo_deposito,
        COALESCE(j.dias_desde_ultimo_login, {janela_dias})     AS dias_desde_ultimo_login,
        COALESCE(md.depositou_esta_semana, 0)                  AS depositou_esta_semana,

        -- FREQUENCY (F)
        COALESCE(md.total_sextas_com_deposito_90d, 0)          AS total_sextas_com_deposito_90d,
        ROUND(COALESCE(md.total_sextas_com_deposito_90d, 0) * 100.0 / {TOTAL_SEXTAS_90D}, 2)
                                                               AS pct_sextas_com_deposito,
        ROUND(COALESCE(md.frequencia_semanal_media, 0.0), 2)   AS frequencia_semanal_media,

        -- MONETARY (M)
        ROUND(COALESCE(md.valor_medio_deposito_sexta, 0.0), 2) AS valor_medio_deposito_sexta,
        ROUND(COALESCE(md.valor_total_depositos_90d, 0.0), 2)  AS valor_total_depositos_90d,
        ROUND(COALESCE(md.valor_max_deposito, 0.0), 2)         AS valor_max_deposito,

        -- BEHAVIORAL (B)
        ROUND(COALESCE(at.ggr_90d, 0.0), 2)                   AS ggr_90d,
        ROUND(
            CASE
                WHEN COALESCE(at.total_saq_brl, 0) > 0
                THEN COALESCE(at.total_dep_brl, 0.0) / at.total_saq_brl
                ELSE COALESCE(at.total_dep_brl, 0.0)
            END, 2
        )                                                      AS ratio_deposito_saque

    FROM jogadores_reais j
    INNER JOIN metricas_deposito md ON j.c_ecr_id = md.player_id
    LEFT JOIN atividade at ON j.c_ecr_id = at.player_id

    -- Filtra apenas quem tem pelo menos 1 deposito no periodo
    WHERE md.valor_total_depositos_90d > 0

    ORDER BY md.valor_total_depositos_90d DESC
    """
    return sql


# ===========================================================================
# SCORING — Calculo do whale_friday_score (0-100)
# ===========================================================================

def normalize_minmax(series: pd.Series, inverse: bool = False) -> pd.Series:
    """
    Normaliza uma serie para [0, 1] usando Min-Max scaling.
    Se inverse=True, valores MENORES recebem score MAIOR.
    """
    s_min = series.min()
    s_max = series.max()

    if s_max == s_min:
        return pd.Series(0.5, index=series.index)

    normalized = (series - s_min) / (s_max - s_min)

    if inverse:
        normalized = 1.0 - normalized

    return normalized


def calcular_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o whale_friday_score (0-100) a partir das features brutas.

    Formula:
        whale_friday_score = (R * 0.15 + F * 0.30 + M * 0.35 + B * 0.20) * 100

    Cada sub-score e a media das features normalizadas (MinMax 0-1).
    """
    logger.info(f"Calculando scores para {len(df)} jogadores...")

    result = df.copy()

    # --- RECENCY SCORE (0-1) ---
    r1 = normalize_minmax(result['dias_desde_ultimo_deposito'], inverse=True)
    r2 = normalize_minmax(result['dias_desde_ultimo_login'], inverse=True)
    r3 = result['depositou_esta_semana'].astype(float)
    result['r_score'] = (r1 + r2 + r3) / 3.0

    # --- FREQUENCY SCORE (0-1) ---
    f1 = normalize_minmax(result['total_sextas_com_deposito_90d'])
    f2 = normalize_minmax(result['pct_sextas_com_deposito'])
    f3 = normalize_minmax(result['frequencia_semanal_media'])
    result['f_score'] = (f1 + f2 + f3) / 3.0

    # --- MONETARY SCORE (0-1) ---
    m1 = normalize_minmax(result['valor_medio_deposito_sexta'])
    m2 = normalize_minmax(result['valor_total_depositos_90d'])
    m3 = normalize_minmax(result['valor_max_deposito'])
    result['m_score'] = (m1 + m2 + m3) / 3.0

    # --- BEHAVIORAL SCORE (0-1) ---
    b1 = normalize_minmax(result['ggr_90d'])
    # Cap ratio em percentil 99 para evitar outliers extremos
    ratio_capped = result['ratio_deposito_saque'].clip(
        upper=result['ratio_deposito_saque'].quantile(0.99)
    )
    b2 = normalize_minmax(ratio_capped)
    result['b_score'] = (b1 + b2) / 2.0

    # --- WHALE FRIDAY SCORE (0-100) ---
    result['whale_friday_score'] = (
        result['r_score'] * PESO_RECENCY +
        result['f_score'] * PESO_FREQUENCY +
        result['m_score'] * PESO_MONETARY +
        result['b_score'] * PESO_BEHAVIORAL
    ) * 100.0

    result['whale_friday_score'] = result['whale_friday_score'].round(2)

    # --- TIERING ---
    result['whale_tier'] = pd.cut(
        result['whale_friday_score'],
        bins=[-0.01, 20, 40, 60, 80, 100.01],
        labels=['Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond']
    )

    logger.info("Distribuicao por tier:")
    tier_counts = result['whale_tier'].value_counts().sort_index()
    for tier, count in tier_counts.items():
        pct = count / len(result) * 100
        logger.info(f"  {tier}: {count} jogadores ({pct:.1f}%)")

    return result


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

def extrair_features(data_referencia: str = None) -> pd.DataFrame:
    """Extrai features brutas do Athena."""
    if data_referencia is None:
        data_referencia = datetime.now().strftime('%Y-%m-%d')

    logger.info(f"Extraindo features do Athena (data ref: {data_referencia})...")

    sql = build_sql_features(data_referencia)
    logger.info("Executando query no ps_bi...")

    try:
        df = query_athena(sql, database="ps_bi")
    except Exception as e:
        logger.error(f"Erro ao executar query: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        logger.error("Query retornou vazio! Verificar conexao e filtros.")
        return pd.DataFrame()

    logger.info(f"Extraidos {len(df)} jogadores com deposito nos ultimos {JANELA_DIAS} dias.")

    # Garantir tipos corretos
    colunas_float = [
        'valor_medio_deposito_sexta', 'valor_total_depositos_90d',
        'valor_max_deposito', 'ggr_90d', 'ratio_deposito_saque',
        'pct_sextas_com_deposito', 'frequencia_semanal_media'
    ]
    for col in colunas_float:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    colunas_int = [
        'dias_desde_ultimo_deposito', 'dias_desde_ultimo_login',
        'depositou_esta_semana', 'total_sextas_com_deposito_90d'
    ]
    for col in colunas_int:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    return df


def gerar_dataset_whale_friday(data_referencia: str = None,
                                salvar_csv: bool = True) -> pd.DataFrame:
    """Pipeline completo: extrai features + calcula score + salva output."""
    if data_referencia is None:
        data_referencia = datetime.now().strftime('%Y-%m-%d')

    logger.info("=" * 60)
    logger.info("WEEKEND WHALE WATCH — Feature Engineering & Scoring")
    logger.info(f"Data referencia: {data_referencia}")
    logger.info(f"Janela de analise: {JANELA_DIAS} dias")
    logger.info(f"Pesos: R={PESO_RECENCY} F={PESO_FREQUENCY} "
                f"M={PESO_MONETARY} B={PESO_BEHAVIORAL}")
    logger.info("=" * 60)

    # Step 1: Extrair features brutas
    df = extrair_features(data_referencia)
    if df.empty:
        logger.error("Abortando — sem dados para processar.")
        return df

    # Step 2: Calcular scores
    df_scored = calcular_score(df)

    # Step 3: Ordenar por score (top whales primeiro)
    df_scored = df_scored.sort_values('whale_friday_score', ascending=False)
    df_scored = df_scored.reset_index(drop=True)

    # Step 4: Salvar CSV
    if salvar_csv:
        output_dir = os.path.join(os.path.dirname(__file__), '..', 'output')
        os.makedirs(output_dir, exist_ok=True)

        filename = f"whale_friday_score_{data_referencia}.csv"
        filepath = os.path.join(output_dir, filename)
        df_scored.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"Dataset salvo em: {filepath}")

    # Step 5: Log resumo dos top 10
    logger.info("\nTOP 10 WHALES DE SEXTA-FEIRA:")
    logger.info("-" * 80)
    top10 = df_scored.head(10)[
        ['external_id', 'whale_friday_score', 'whale_tier',
         'total_sextas_com_deposito_90d', 'valor_total_depositos_90d',
         'ggr_90d', 'dias_desde_ultimo_deposito']
    ]
    for _, row in top10.iterrows():
        logger.info(
            f"  ID {row['external_id']:>12} | "
            f"Score: {row['whale_friday_score']:6.1f} ({row['whale_tier']}) | "
            f"Sextas: {row['total_sextas_com_deposito_90d']:2.0f}/13 | "
            f"Dep 90d: R$ {row['valor_total_depositos_90d']:>12,.2f} | "
            f"GGR: R$ {row['ggr_90d']:>12,.2f} | "
            f"Ult. dep: {row['dias_desde_ultimo_deposito']:.0f}d"
        )

    logger.info("=" * 60)
    logger.info(f"Total jogadores scorados: {len(df_scored)}")
    logger.info(f"Score medio: {df_scored['whale_friday_score'].mean():.1f}")
    logger.info(f"Score mediano: {df_scored['whale_friday_score'].median():.1f}")
    logger.info(f"Diamond (80+): {len(df_scored[df_scored['whale_friday_score'] >= 80])} jogadores")
    logger.info(f"Platinum (60-80): {len(df_scored[df_scored['whale_friday_score'].between(60, 80)])} jogadores")
    logger.info("=" * 60)

    return df_scored


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == '__main__':
    # Permite passar data como argumento: python whale_friday_score.py 2026-03-20
    data_ref = sys.argv[1] if len(sys.argv) > 1 else None
    df_final = gerar_dataset_whale_friday(data_referencia=data_ref)

    if not df_final.empty:
        logger.info("Pipeline concluido com sucesso!")
    else:
        logger.error("Pipeline falhou — verificar logs acima.")
