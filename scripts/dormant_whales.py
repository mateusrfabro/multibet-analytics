"""
Whales Dormentes — Identificar jogadores de alto valor que pararam de depositar.

Contexto: MultiBet fez R$ 1.916.794 em depositos em 20/03/2026.
Faltam R$ 83K para bater a meta de R$ 2M diarios.
Objetivo: reativar depositos dos whales dormentes para fechar esse gap.

Uso:
    cd MultiBet
    python scripts/dormant_whales.py
"""
import sys
import os
import logging

# Garantir que o projeto esta no path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

from db.athena import query_athena
import pandas as pd

# ====================================================================
# QUERY: Whales dormentes nos ultimos 90 dias
#
# Logica passo a passo:
#   1. dep_daily: depositos diarios por player usando fct_deposits_daily
#      - ps_bi: valores JA em BRL (nao dividir por 100)
#      - Periodo: 90 dias (2025-12-21 a 2026-03-20)
#
#   2. whale_days: identifica players que tiveram pelo menos 1 dia
#      com deposito >= R$ 500 — esse e o criterio de "whale"
#
#   3. dep_summary: para esses whales, calcula:
#      - total depositado nos 90 dias
#      - media de deposito por dia ativo (nao por 90 dias corridos)
#      - data do ultimo deposito
#      - dias desde o ultimo deposito ate hoje (21/03/2026)
#
#   4. Filtro de dormencia: ultimo deposito entre 7 e 30 dias atras
#      - < 7 dias = ainda ativo, nao precisa acao urgente
#      - > 30 dias = provavelmente churned, mais dificil reativar
#      - 7-30 dias = janela ideal de reativacao
#
#   5. ggr_90d: GGR acumulado de cada player nos 90 dias
#      - GGR positivo = casa ganhou dinheiro com esse player (mais valioso)
#      - GGR negativo = player ganhou mais do que apostou
#
#   6. dim_user: traz external_id (para acionar no Smartico CRM)
#      + filtra test users (is_test = false)
# ====================================================================

sql = """
WITH dep_daily AS (
    -- Depositos diarios por player nos ultimos 90 dias
    -- ps_bi: valores ja em BRL (nao dividir por 100)
    SELECT
        d.player_id,
        d.created_date,
        d.success_amount_local AS dep_dia_brl
    FROM fct_deposits_daily d
    WHERE d.created_date >= DATE '2025-12-21'
      AND d.created_date <= DATE '2026-03-20'
      AND d.success_amount_local > 0
),

whale_days AS (
    -- Players que tiveram pelo menos 1 dia com deposito >= R$ 500
    SELECT DISTINCT player_id
    FROM dep_daily
    WHERE dep_dia_brl >= 500
),

dep_summary AS (
    -- Resumo de depositos dos whales nos 90 dias
    SELECT
        dd.player_id,
        SUM(dd.dep_dia_brl)                                          AS total_depositos_90d,
        AVG(dd.dep_dia_brl)                                          AS avg_deposito_diario,
        MAX(dd.created_date)                                         AS ultimo_deposito,
        DATE_DIFF('day', MAX(dd.created_date), DATE '2026-03-21')    AS dias_sumido,
        COUNT(DISTINCT dd.created_date)                              AS dias_com_deposito
    FROM dep_daily dd
    INNER JOIN whale_days w ON w.player_id = dd.player_id
    GROUP BY dd.player_id
),

ggr_90d AS (
    -- GGR acumulado 90 dias (positivo = casa ganhou)
    SELECT
        a.player_id,
        SUM(a.ggr_local) AS ggr_90d
    FROM fct_player_activity_daily a
    WHERE a.activity_date >= DATE '2025-12-21'
      AND a.activity_date <= DATE '2026-03-20'
    GROUP BY a.player_id
)

SELECT
    ds.player_id,
    u.external_id,
    ROUND(ds.total_depositos_90d, 2)  AS total_depositos_90d,
    ROUND(ds.avg_deposito_diario, 2)  AS avg_deposito_diario,
    ds.ultimo_deposito,
    ds.dias_sumido,
    ds.dias_com_deposito,
    ROUND(COALESCE(g.ggr_90d, 0), 2) AS ggr_90d
FROM dep_summary ds
-- Join dim_user para external_id e filtro de test users
INNER JOIN dim_user u
    ON u.ecr_id = ds.player_id
    AND u.is_test = false
-- GGR (left join pois pode nao ter atividade de jogo)
LEFT JOIN ggr_90d g ON g.player_id = ds.player_id
-- FILTRO CRITICO: dormentes recentes (7 a 30 dias sem deposito)
WHERE ds.dias_sumido BETWEEN 7 AND 30
ORDER BY ds.avg_deposito_diario DESC
"""


def main():
    log.info("=" * 70)
    log.info("WHALES DORMENTES — Analise de reativacao MultiBet")
    log.info("=" * 70)
    log.info("Executando query no Athena (ps_bi)...")
    log.info("Periodo: 2025-12-21 a 2026-03-20 | Dormencia: 7-30 dias")

    try:
        df = query_athena(sql, database="ps_bi")
        log.info(f"Query retornou {len(df)} jogadores dormentes")
    except Exception as e:
        log.error(f"Erro na query Athena: {e}")
        raise

    if df.empty:
        log.warning("Nenhum whale dormente encontrado. Verificar filtros.")
        return

    # ==================================================================
    # POTENCIAL DE REATIVACAO
    # Soma do avg_deposito_diario = quanto a casa deixa de receber por dia
    # Se reativarmos TODOS, esse e o upside maximo
    # ==================================================================
    potencial_diario = df['avg_deposito_diario'].sum()
    total_depositos_perdidos = df['total_depositos_90d'].sum()
    total_players = len(df)
    ggr_total = df['ggr_90d'].sum()
    gap_meta = 83206.00  # R$ 2M - R$ 1.916.794

    log.info("=" * 70)
    log.info("RESUMO — WHALES DORMENTES")
    log.info("=" * 70)
    log.info(f"Total de whales dormentes:       {total_players}")
    log.info(f"Potencial reativacao/dia:         R$ {potencial_diario:,.2f}")
    log.info(f"Total depositado 90d (todos):     R$ {total_depositos_perdidos:,.2f}")
    log.info(f"GGR 90d acumulado:                R$ {ggr_total:,.2f}")
    log.info(f"Meta gap diario:                  R$ {gap_meta:,.2f}")
    log.info(f"Cobertura do gap:                 {(potencial_diario / gap_meta) * 100:.1f}%")
    log.info("=" * 70)

    # ==================================================================
    # TOP 20 DORMENTES
    # ==================================================================
    top20 = df.head(20)
    log.info("")
    log.info("TOP 20 WHALES DORMENTES (por avg deposito diario):")
    log.info("-" * 90)
    for idx, (_, row) in enumerate(top20.iterrows(), 1):
        log.info(
            f"  #{idx:2d} | Player: {row['player_id']} | "
            f"ExtID: {row['external_id']} | "
            f"Avg/dia: R$ {row['avg_deposito_diario']:,.2f} | "
            f"Total 90d: R$ {row['total_depositos_90d']:,.2f} | "
            f"Sumido: {row['dias_sumido']}d | "
            f"GGR: R$ {row['ggr_90d']:,.2f}"
        )

    # ==================================================================
    # SALVAR CSV (encoding utf-8-sig para Excel BR)
    # ==================================================================
    output_dir = os.path.join(os.path.dirname(__file__), "..", "output")
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, "dormant_whales_2026-03-21.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log.info(f"\nCSV salvo em: {csv_path}")
    log.info(f"Total de linhas: {len(df)}")

    # ==================================================================
    # LEGENDA (padrao de entrega obrigatorio — CLAUDE.md)
    # ==================================================================
    legenda_path = os.path.join(output_dir, "dormant_whales_2026-03-21_legenda.txt")
    with open(legenda_path, "w", encoding="utf-8") as f:
        f.write("LEGENDA — dormant_whales_2026-03-21.csv\n")
        f.write("=" * 60 + "\n\n")
        f.write("FONTE: Athena (ps_bi) — Iceberg Data Lake\n")
        f.write("DATA DE EXTRACAO: 21/03/2026\n")
        f.write("PERIODO ANALISADO: 2025-12-21 a 2026-03-20 (90 dias)\n")
        f.write("FILTRO DORMENCIA: Ultimo deposito entre 7 e 30 dias atras\n")
        f.write("CRITERIO WHALE: Pelo menos 1 dia com deposito >= R$ 500\n")
        f.write("FILTRO TEST USERS: Excluidos (is_test = false)\n\n")
        f.write("COLUNAS:\n")
        f.write("-" * 60 + "\n")
        f.write("player_id           ID interno do jogador (ecr_id no ps_bi)\n")
        f.write("external_id         ID externo para Smartico CRM (user_ext_id)\n")
        f.write("total_depositos_90d Total depositado nos ultimos 90 dias (BRL)\n")
        f.write("avg_deposito_diario Media de deposito por dia ativo (BRL)\n")
        f.write("ultimo_deposito     Data do ultimo deposito realizado\n")
        f.write("dias_sumido         Dias desde o ultimo deposito ate 21/03/2026\n")
        f.write("dias_com_deposito   Qtd de dias com deposito nos 90 dias\n")
        f.write("ggr_90d             GGR acumulado 90 dias (BRL)\n")
        f.write("                    Positivo = casa ganhou com esse jogador\n")
        f.write("                    Negativo = jogador ganhou mais do que apostou\n\n")
        f.write("GLOSSARIO:\n")
        f.write("-" * 60 + "\n")
        f.write("GGR  = Gross Gaming Revenue = Apostas - Ganhos do jogador\n")
        f.write("Whale = Jogador de alto valor (deposito diario >= R$ 500)\n")
        f.write("Dormente = Parou de depositar entre 7-30 dias\n")
        f.write("           (nao e churned, janela ideal de reativacao)\n\n")
        f.write("ACAO SUGERIDA POR SEGMENTO:\n")
        f.write("-" * 60 + "\n")
        f.write("1. Top 20 (avg_deposito_diario mais alto):\n")
        f.write("   -> Contato pessoal do VIP manager\n")
        f.write("   -> Ligacao + oferta exclusiva\n\n")
        f.write("2. Players com GGR positivo (casa lucrou):\n")
        f.write("   -> Bonus de retorno moderado (10-20% do avg deposito)\n")
        f.write("   -> Sao rentaveis, vale investir na reativacao\n\n")
        f.write("3. Players com GGR negativo (jogador ganhou):\n")
        f.write("   -> Bonus mais agressivo (20-30%)\n")
        f.write("   -> Ja lucramos com eles historicamente\n\n")
        f.write("4. Todos:\n")
        f.write("   -> Usar external_id para ativar campanhas no Smartico CRM\n")
        f.write("   -> Push notification + email + SMS em 3 ondas\n\n")
        f.write(f"METRICAS RESUMO:\n")
        f.write(f"Total whales dormentes:    {total_players}\n")
        f.write(f"Potencial reativacao/dia:  R$ {potencial_diario:,.2f}\n")
        f.write(f"Gap meta diaria:           R$ {gap_meta:,.2f}\n")
        f.write(f"Cobertura do gap:          {(potencial_diario / gap_meta) * 100:.1f}%\n")

    log.info(f"Legenda salva em: {legenda_path}")
    log.info("\nConcluido com sucesso!")


if __name__ == "__main__":
    main()
