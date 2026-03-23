"""
Analise de Conversao Registro -> FTD (First-Time Deposit)
=========================================================
Objetivo: Entender ONDE perdemos conversao nos ultimos 30 dias.
Fonte: ps_bi.dim_user (Athena, valores em BRL, timestamps UTC)
Data: 2026-03-21
Autor: Mateus F. (Squad Intelligence Engine)

Racional:
- dim_user tem registration_date e ftd_date (pre-calculados pelo dbt)
- ps_bi ja esta em BRL (sem necessidade de /100)
- Filtro is_test = false para excluir usuarios de teste
- Timestamps convertidos para BRT (America/Sao_Paulo)
"""

import sys
import os
import logging

# Garantir path do projeto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# =============================================================================
# STEP 0: Descoberta de schema (dim_user)
# =============================================================================
def discover_schema():
    """Descobre colunas de dim_user para validar nomes antes da query principal."""
    log.info("=== STEP 0: Descobrindo schema de ps_bi.dim_user ===")
    sql = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'ps_bi'
      AND table_name = 'dim_user'
    ORDER BY ordinal_position
    """
    df = query_athena(sql, database="ps_bi")
    log.info(f"dim_user tem {len(df)} colunas:")
    print(df.to_string(index=False))
    return df


# =============================================================================
# STEP 1: Query principal — Conversao diaria + tempo medio + dia da semana
# =============================================================================

MAIN_SQL = """
-- ============================================================
-- Analise de Conversao Registro -> FTD (ultimos 30 dias)
-- Fonte: ps_bi.dim_user
-- Regra: is_test = false, timestamps convertidos para BRT
-- ============================================================

WITH registros AS (
    -- Todos os registros dos ultimos 30 dias
    -- registration_date e ftd_date sao tipo DATE (sem hora) - nao usar AT TIME ZONE
    -- ftd_datetime e tipo TIMESTAMP - usar para calculo de tempo
    SELECT
        ecr_id,
        external_id,
        CAST(registration_date AS DATE) AS reg_date_brt,
        signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS reg_ts_brt,
        has_ftd,
        ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ftd_ts_brt,
        CAST(ftd_date AS DATE) AS ftd_date_brt,
        ftd_amount_inhouse AS ftd_amount
    FROM ps_bi.dim_user
    WHERE is_test = false
      AND CAST(registration_date AS DATE)
          BETWEEN DATE '2026-02-19' AND DATE '2026-03-20'
),

-- ============================================================
-- Bloco 1: Metricas diarias
-- ============================================================
daily_metrics AS (
    SELECT
        reg_date_brt AS dia,
        -- Nome do dia da semana (em ingles, depois mapeamos)
        day_of_week(reg_date_brt) AS dow_number,
        COUNT(*) AS total_registros,
        COUNT_IF(has_ftd = 1 AND ftd_date_brt IS NOT NULL) AS total_ftds,
        -- Taxa de conversao
        ROUND(
            CAST(COUNT_IF(has_ftd = 1 AND ftd_date_brt IS NOT NULL) AS DOUBLE)
            / NULLIF(CAST(COUNT(*) AS DOUBLE), 0) * 100, 2
        ) AS taxa_conversao_pct,
        -- Tempo medio entre registro e FTD (em horas)
        ROUND(
            AVG(
                CASE
                    WHEN has_ftd = 1 AND ftd_ts_brt IS NOT NULL
                    THEN date_diff('minute', reg_ts_brt, ftd_ts_brt) / 60.0
                END
            ), 2
        ) AS tempo_medio_ftd_horas,
        -- FTD amount medio (ps_bi ja em BRL)
        ROUND(
            AVG(
                CASE
                    WHEN has_ftd = 1 AND ftd_amount IS NOT NULL
                    THEN ftd_amount
                END
            ), 2
        ) AS ftd_amount_medio_brl,
        -- FTD amount total
        ROUND(
            SUM(
                CASE
                    WHEN has_ftd = 1 AND ftd_amount IS NOT NULL
                    THEN ftd_amount
                    ELSE 0
                END
            ), 2
        ) AS ftd_amount_total_brl
    FROM registros
    GROUP BY reg_date_brt, day_of_week(reg_date_brt)
)

SELECT
    dia,
    CASE dow_number
        WHEN 1 THEN 'Segunda'
        WHEN 2 THEN 'Terca'
        WHEN 3 THEN 'Quarta'
        WHEN 4 THEN 'Quinta'
        WHEN 5 THEN 'Sexta'
        WHEN 6 THEN 'Sabado'
        WHEN 7 THEN 'Domingo'
    END AS dia_semana,
    total_registros,
    total_ftds,
    taxa_conversao_pct,
    tempo_medio_ftd_horas,
    ftd_amount_medio_brl,
    ftd_amount_total_brl
FROM daily_metrics
ORDER BY dia
"""


# =============================================================================
# STEP 2: Query de resumo por dia da semana (agregado)
# =============================================================================

DOW_SQL = """
WITH registros AS (
    SELECT
        ecr_id,
        CAST(registration_date AS DATE) AS reg_date_brt,
        signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS reg_ts_brt,
        has_ftd,
        ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ftd_ts_brt,
        CAST(ftd_date AS DATE) AS ftd_date_brt,
        ftd_amount_inhouse AS ftd_amount
    FROM ps_bi.dim_user
    WHERE is_test = false
      AND CAST(registration_date AS DATE)
          BETWEEN DATE '2026-02-19' AND DATE '2026-03-20'
)

SELECT
    day_of_week(reg_date_brt) AS dow_number,
    CASE day_of_week(reg_date_brt)
        WHEN 1 THEN 'Segunda'
        WHEN 2 THEN 'Terca'
        WHEN 3 THEN 'Quarta'
        WHEN 4 THEN 'Quinta'
        WHEN 5 THEN 'Sexta'
        WHEN 6 THEN 'Sabado'
        WHEN 7 THEN 'Domingo'
    END AS dia_semana,
    -- Quantos dias desse DOW no periodo
    COUNT(DISTINCT reg_date_brt) AS qtd_dias,
    -- Totais
    COUNT(*) AS total_registros,
    COUNT_IF(has_ftd = 1 AND ftd_date_brt IS NOT NULL) AS total_ftds,
    -- Medias diarias
    ROUND(CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT reg_date_brt), 0) AS avg_registros_dia,
    ROUND(CAST(COUNT_IF(has_ftd = 1 AND ftd_date_brt IS NOT NULL) AS DOUBLE) / COUNT(DISTINCT reg_date_brt), 0) AS avg_ftds_dia,
    -- Taxa de conversao
    ROUND(
        CAST(COUNT_IF(has_ftd = 1 AND ftd_date_brt IS NOT NULL) AS DOUBLE)
        / NULLIF(CAST(COUNT(*) AS DOUBLE), 0) * 100, 2
    ) AS taxa_conversao_pct,
    -- Tempo medio ate FTD (horas)
    ROUND(
        AVG(
            CASE
                WHEN has_ftd = 1 AND ftd_ts_brt IS NOT NULL
                THEN date_diff('minute', reg_ts_brt, ftd_ts_brt) / 60.0
            END
        ), 2
    ) AS tempo_medio_ftd_horas,
    -- FTD amount medio
    ROUND(
        AVG(
            CASE WHEN has_ftd = 1 AND ftd_amount IS NOT NULL THEN ftd_amount END
        ), 2
    ) AS ftd_amount_medio_brl
FROM registros
GROUP BY day_of_week(reg_date_brt)
ORDER BY dow_number
"""


# =============================================================================
# STEP 3: Executar, analisar e salvar
# =============================================================================

def main():
    # --- Schema discovery ---
    try:
        schema_df = discover_schema()
        # Verifica se ftd_amount existe
        cols = schema_df['column_name'].tolist()
        has_ftd_amount = 'ftd_amount' in cols
        log.info(f"Coluna ftd_amount existe: {has_ftd_amount}")
        if not has_ftd_amount:
            log.warning("ftd_amount NAO encontrada em dim_user. Verificando alternativas...")
            ftd_cols = [c for c in cols if 'ftd' in c.lower() or 'deposit' in c.lower() or 'amount' in c.lower()]
            log.info(f"Colunas relacionadas a FTD/amount: {ftd_cols}")
    except Exception as e:
        log.error(f"Erro na descoberta de schema: {e}")
        log.info("Continuando com a query principal mesmo assim...")

    # --- Query principal: metricas diarias ---
    log.info("=== STEP 1: Executando query de conversao diaria ===")
    try:
        df_daily = query_athena(MAIN_SQL, database="ps_bi")
        log.info(f"Resultado: {len(df_daily)} dias retornados")
        print("\n" + "="*80)
        print("CONVERSAO DIARIA - REGISTRO -> FTD (ultimos 30 dias)")
        print("="*80)
        print(df_daily.to_string(index=False))
    except Exception as e:
        log.error(f"Erro na query principal: {e}")
        log.error("Verifique se as colunas existem. Possivel ajuste necessario.")
        raise

    # --- Query resumo por dia da semana ---
    log.info("\n=== STEP 2: Executando query por dia da semana ===")
    try:
        df_dow = query_athena(DOW_SQL, database="ps_bi")
        log.info(f"Resultado: {len(df_dow)} dias da semana")
        print("\n" + "="*80)
        print("RESUMO POR DIA DA SEMANA")
        print("="*80)
        print(df_dow.to_string(index=False))
    except Exception as e:
        log.error(f"Erro na query por dia da semana: {e}")
        raise

    # =================================================================
    # STEP 3: Analise e insights
    # =================================================================
    print("\n" + "="*80)
    print("ANALISE E INSIGHTS")
    print("="*80)

    # Metricas globais
    total_regs = df_daily['total_registros'].sum()
    total_ftds = df_daily['total_ftds'].sum()
    taxa_media = round(total_ftds / total_regs * 100, 2) if total_regs > 0 else 0

    print(f"\n--- METRICAS GLOBAIS (30 dias) ---")
    print(f"Total de registros:     {total_regs:,}")
    print(f"Total de FTDs:          {total_ftds:,}")
    print(f"Taxa de conversao media: {taxa_media}%")

    # Tempo medio global de conversao
    if 'tempo_medio_ftd_horas' in df_daily.columns:
        tempo_medio_global = df_daily['tempo_medio_ftd_horas'].mean()
        print(f"Tempo medio registro->FTD: {tempo_medio_global:.1f} horas")

    # Melhor e pior dia da semana
    if len(df_dow) > 0:
        melhor = df_dow.loc[df_dow['taxa_conversao_pct'].idxmax()]
        pior = df_dow.loc[df_dow['taxa_conversao_pct'].idxmin()]

        print(f"\n--- MELHOR DIA (benchmark) ---")
        print(f"  Dia: {melhor['dia_semana']}")
        print(f"  Taxa conversao: {melhor['taxa_conversao_pct']}%")
        print(f"  Avg registros/dia: {melhor['avg_registros_dia']:.0f}")
        print(f"  Avg FTDs/dia: {melhor['avg_ftds_dia']:.0f}")
        if 'tempo_medio_ftd_horas' in df_dow.columns:
            print(f"  Tempo medio FTD: {melhor['tempo_medio_ftd_horas']:.1f}h")
        if 'ftd_amount_medio_brl' in df_dow.columns:
            print(f"  FTD amount medio: R$ {melhor['ftd_amount_medio_brl']:.2f}")

        print(f"\n--- PIOR DIA (oportunidade) ---")
        print(f"  Dia: {pior['dia_semana']}")
        print(f"  Taxa conversao: {pior['taxa_conversao_pct']}%")
        print(f"  Avg registros/dia: {pior['avg_registros_dia']:.0f}")
        print(f"  Avg FTDs/dia: {pior['avg_ftds_dia']:.0f}")
        if 'tempo_medio_ftd_horas' in df_dow.columns:
            print(f"  Tempo medio FTD: {pior['tempo_medio_ftd_horas']:.1f}h")

        # Simulacao: se todos os dias tivessem a taxa do melhor dia
        melhor_taxa = melhor['taxa_conversao_pct'] / 100.0

        print(f"\n--- SIMULACAO: Se todos os dias tivessem {melhor['taxa_conversao_pct']}% de conversao ---")

        ftds_extras_total = 0
        for _, row in df_dow.iterrows():
            ftds_atuais = row['total_ftds']
            regs = row['total_registros']
            ftds_potenciais = round(regs * melhor_taxa)
            delta = ftds_potenciais - ftds_atuais
            dias = row['qtd_dias']

            if delta > 0:
                print(f"  {row['dia_semana']}: +{delta} FTDs extras no periodo ({delta/dias:.0f}/dia)")
                ftds_extras_total += delta

        print(f"\n  TOTAL FTDs extras no periodo: +{ftds_extras_total}")
        print(f"  Media FTDs extras por dia: +{ftds_extras_total/30:.0f}")

        # Estimativa de deposito extra
        if 'ftd_amount_medio_brl' in df_dow.columns:
            avg_ftd_global = df_dow['ftd_amount_medio_brl'].mean()
            deposito_extra = ftds_extras_total * avg_ftd_global
            print(f"\n  Avg FTD amount global: R$ {avg_ftd_global:.2f}")
            print(f"  Deposito extra estimado (periodo): R$ {deposito_extra:,.2f}")
            print(f"  Deposito extra estimado (mensal): R$ {deposito_extra:,.2f}")

    # =================================================================
    # STEP 4: Salvar CSV
    # =================================================================
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, "ftd_conversion_analysis_2026-03-21.csv")

    # Combinar daily + dow em um unico Excel-friendly CSV
    # Adicionar linha separadora
    df_daily_out = df_daily.copy()
    df_daily_out.insert(0, 'secao', 'DIARIO')

    # Salvar diario
    df_daily_out.to_csv(csv_path, index=False)
    log.info(f"CSV salvo: {csv_path}")

    # Salvar DOW em arquivo separado para clareza
    dow_path = os.path.join(output_dir, "ftd_conversion_by_dow_2026-03-21.csv")
    df_dow.to_csv(dow_path, index=False)
    log.info(f"CSV DOW salvo: {dow_path}")

    # =================================================================
    # STEP 5: Legenda (padrao obrigatorio)
    # =================================================================
    legenda_path = os.path.join(output_dir, "ftd_conversion_analysis_2026-03-21_legenda.txt")
    legenda = """
LEGENDA - Analise de Conversao Registro -> FTD
================================================
Data da analise: 2026-03-21
Periodo: 2026-02-19 a 2026-03-20 (30 dias)
Fonte: ps_bi.dim_user (Athena, Iceberg Data Lake)
Filtros: is_test = false (excluindo usuarios de teste)
Fuso horario: BRT (America/Sao_Paulo)

COLUNAS - Arquivo DIARIO:
- secao: Tipo de dado (DIARIO)
- dia: Data do registro (BRT)
- dia_semana: Nome do dia da semana
- total_registros: Quantidade de novos registros no dia
- total_ftds: Quantidade de FTDs (primeiro deposito) realizados por usuarios registrados no dia
- taxa_conversao_pct: (total_ftds / total_registros) * 100
- tempo_medio_ftd_horas: Media de horas entre o registro e o primeiro deposito
- ftd_amount_medio_brl: Valor medio do primeiro deposito (R$)
- ftd_amount_total_brl: Soma total dos primeiros depositos do dia (R$)

COLUNAS - Arquivo POR DIA DA SEMANA:
- dow_number: Numero do dia (1=Segunda, 7=Domingo)
- dia_semana: Nome do dia
- qtd_dias: Quantos dias desse tipo no periodo analisado
- total_registros / total_ftds: Somas acumuladas
- avg_registros_dia / avg_ftds_dia: Medias diarias
- taxa_conversao_pct: Taxa de conversao agregada do dia da semana
- tempo_medio_ftd_horas: Tempo medio de conversao
- ftd_amount_medio_brl: FTD amount medio

GLOSSARIO:
- FTD: First-Time Deposit (primeiro deposito do jogador)
- Taxa de conversao: % de registros que fizeram FTD
- Benchmark: melhor dia da semana em conversao (meta a atingir)
- Oportunidade: pior dia da semana (onde ha mais espaco para melhoria)

NOTA IMPORTANTE:
- A taxa de conversao considera TODOS os FTDs do usuario, mesmo que tenham ocorrido
  dias apos o registro. Ou seja, um usuario que se registrou dia 19/02 e fez FTD
  dia 25/02 conta como convertido no dia 19/02.
- ps_bi ja tem valores em BRL (nao precisa dividir por 100).
"""
    with open(legenda_path, 'w', encoding='utf-8') as f:
        f.write(legenda)
    log.info(f"Legenda salva: {legenda_path}")

    # =================================================================
    # RESUMO FINAL
    # =================================================================
    print("\n" + "="*80)
    print("RESUMO EXECUTIVO PARA O TIME")
    print("="*80)
    print(f"""
ANALISE DE CONVERSAO REGISTRO -> FTD
Periodo: 19/02/2026 a 20/03/2026 (30 dias)

1. SITUACAO ATUAL:
   - {total_regs:,} registros | {total_ftds:,} FTDs | {taxa_media}% conversao media

2. OPORTUNIDADE:
   - Se todos os dias tivessem a taxa do melhor dia ({melhor['dia_semana']}: {melhor['taxa_conversao_pct']}%),
     teriamos +{ftds_extras_total} FTDs extras no periodo.

3. ACOES SUGERIDAS:
   a) Investigar por que {melhor['dia_semana']} converte melhor:
      - Ha campanha especifica nesse dia?
      - O mix de canais de aquisicao muda?
      - Bonus de boas-vindas ativo?

   b) Replicar as condicoes do melhor dia nos piores dias:
      - Push notifications / CRM no dia do registro
      - Bonus de deposito direcionado para D+0/D+1

   c) Reduzir tempo de conversao:
      - Usuarios que demoram >24h para FTD: enviar lembrete
      - Simplificar processo de deposito (PIX em 1 clique?)

4. PROXIMOS PASSOS:
   - Cruzar com canais de aquisicao (tracker_id) para ver qual canal converte melhor
   - Analisar cohort D+0 vs D+1 vs D+7 (conversao por janela)
   - Verificar se tamanho do FTD impacta retencao D+30

Arquivos gerados:
   - output/ftd_conversion_analysis_2026-03-21.csv (diario)
   - output/ftd_conversion_by_dow_2026-03-21.csv (por dia da semana)
   - output/ftd_conversion_analysis_2026-03-21_legenda.txt
""")


if __name__ == "__main__":
    main()
