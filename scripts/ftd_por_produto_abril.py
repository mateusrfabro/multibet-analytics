"""
FTDs de abril/2026 segmentados por produto da primeira aposta.

Demanda: gestor de trafego quer saber se FTDs com ticket medio baixo
sao de jogadores de Esporte (hipotese dele).

Abordagem:
- FTD e evento de wallet — nao tem produto direto no banco.
- Classificamos cada FTD pelo produto da PRIMEIRA APOSTA do jogador apos o deposito.
- Janela de observacao: ate 22/04/2026 (parcial, D-0).

Fontes:
- cashier_ec2.tbl_cashier_deposit  (primeiro deposito confirmado — FTD amount)
- fund_ec2.tbl_real_fund_txn       (primeira aposta por produto)
- bireports_ec2.tbl_ecr             (filtro test user)

Periodo: 01/04/2026 a 22/04/2026 (BRT).

Output:
- data/ftd_por_produto_abril.csv       (dados granulares por jogador)
- data/ftd_por_produto_abril_agg.csv   (agregado por segmento)
- data/ftd_por_produto_abril_legenda.txt
"""

import os
import sys
from pathlib import Path

import pandas as pd

# Adiciona raiz do projeto ao path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.athena import query_athena  # noqa: E402

START_DATE = "2026-04-01"
END_DATE = "2026-04-22"  # parcial, inclusivo

OUT_DIR = Path(__file__).resolve().parent.parent / "data"
OUT_DIR.mkdir(exist_ok=True)

SQL = f"""
WITH params AS (
    SELECT
        TIMESTAMP '{START_DATE} 00:00:00' AS start_ts_utc,
        TIMESTAMP '{END_DATE} 23:59:59' AS end_ts_utc
),

-- 1. Flag de test user (LEFT JOIN depois — se jogador nao estiver em tbl_ecr,
--    trata como nao-teste por default para nao excluir FTDs muito recentes)
test_user_flag AS (
    SELECT
        c_ecr_id,
        c_test_user
    FROM bireports_ec2.tbl_ecr
),

-- 2. Primeiro deposito confirmado por jogador (FTD)
--    Filtro: FTD em abril/2026 no horario BRT
first_deposits AS (
    SELECT
        c_ecr_id,
        ftd_time_utc,
        CAST(ftd_time_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS ftd_date_brt,
        ftd_amount
    FROM (
        SELECT
            c_ecr_id,
            c_created_time AS ftd_time_utc,
            CAST(c_confirmed_amount_in_ecr_ccy AS DECIMAL(18,2)) / 100.0 AS ftd_amount,
            ROW_NUMBER() OVER (PARTITION BY c_ecr_id ORDER BY c_created_time) AS rn
        FROM cashier_ec2.tbl_cashier_deposit
        WHERE c_txn_status = 'txn_confirmed_success'
    ) t
    WHERE rn = 1
      AND CAST(ftd_time_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
          BETWEEN DATE '{START_DATE}' AND DATE '{END_DATE}'
),

-- 3. Primeira aposta CASSINO por jogador (apos o FTD — janela ate end_date+3)
--    c_txn_type 27/28 = CASINO_BUYIN/REBUY; c_product_id = 'CASINO'
first_casino_bet AS (
    SELECT
        c_ecr_id,
        MIN(c_start_time) AS first_casino_ts_utc
    FROM fund_ec2.tbl_real_fund_txn
    WHERE c_start_time >= TIMESTAMP '{START_DATE} 00:00:00'
      AND c_start_time <  TIMESTAMP '2026-04-23 03:00:00'
      AND c_txn_type IN (27, 28)
      AND c_txn_status = 'SUCCESS'
      AND c_product_id = 'CASINO'
    GROUP BY c_ecr_id
),

-- 4. Primeira aposta SPORTSBOOK por jogador
--    c_txn_type 59 = SB_BUYIN; c_product_id = 'SPORTS_BOOK'
first_sports_bet AS (
    SELECT
        c_ecr_id,
        MIN(c_start_time) AS first_sports_ts_utc
    FROM fund_ec2.tbl_real_fund_txn
    WHERE c_start_time >= TIMESTAMP '{START_DATE} 00:00:00'
      AND c_start_time <  TIMESTAMP '2026-04-23 03:00:00'
      AND c_txn_type = 59
      AND c_txn_status = 'SUCCESS'
      AND c_product_id = 'SPORTS_BOOK'
    GROUP BY c_ecr_id
),

-- 5. Classificacao do FTD pelo produto da primeira aposta
ftd_classified AS (
    SELECT
        f.c_ecr_id,
        f.ftd_date_brt,
        f.ftd_amount,
        fc.first_casino_ts_utc,
        fs.first_sports_ts_utc,
        CASE
            -- Nao apostou ate agora
            WHEN fc.first_casino_ts_utc IS NULL AND fs.first_sports_ts_utc IS NULL
                THEN 'FTD Sem Atividade'
            -- So cassino
            WHEN fc.first_casino_ts_utc IS NOT NULL AND fs.first_sports_ts_utc IS NULL
                THEN 'FTD Cassino'
            -- So esporte
            WHEN fs.first_sports_ts_utc IS NOT NULL AND fc.first_casino_ts_utc IS NULL
                THEN 'FTD Esporte'
            -- Apostou nos dois: vence quem veio primeiro
            WHEN fc.first_casino_ts_utc <= fs.first_sports_ts_utc
                THEN 'FTD Cassino'
            ELSE 'FTD Esporte'
        END AS segmento_ftd
    FROM first_deposits f
    LEFT JOIN test_user_flag r ON r.c_ecr_id = f.c_ecr_id
    LEFT JOIN first_casino_bet fc ON fc.c_ecr_id = f.c_ecr_id
    LEFT JOIN first_sports_bet fs ON fs.c_ecr_id = f.c_ecr_id
    -- Jogador sem registro em tbl_ecr (ETL delay) passa por default (NULL -> nao teste)
    WHERE COALESCE(r.c_test_user, false) = false
)

SELECT
    segmento_ftd,
    COUNT(*) AS qtd_ftds,
    ROUND(SUM(ftd_amount), 2) AS ftd_amount_total_brl,
    ROUND(AVG(ftd_amount), 2) AS ftd_amount_medio_brl,
    ROUND(APPROX_PERCENTILE(ftd_amount, 0.5), 2) AS ftd_amount_mediano_brl,
    ROUND(MIN(ftd_amount), 2) AS ftd_amount_min_brl,
    ROUND(MAX(ftd_amount), 2) AS ftd_amount_max_brl
FROM ftd_classified
GROUP BY segmento_ftd
ORDER BY qtd_ftds DESC
"""

# Query granular (para investigacao caso necessario)
SQL_GRANULAR = SQL.replace(
    """SELECT
    segmento_ftd,
    COUNT(*) AS qtd_ftds,
    ROUND(SUM(ftd_amount), 2) AS ftd_amount_total_brl,
    ROUND(AVG(ftd_amount), 2) AS ftd_amount_medio_brl,
    ROUND(APPROX_PERCENTILE(ftd_amount, 0.5), 2) AS ftd_amount_mediano_brl,
    ROUND(MIN(ftd_amount), 2) AS ftd_amount_min_brl,
    ROUND(MAX(ftd_amount), 2) AS ftd_amount_max_brl
FROM ftd_classified
GROUP BY segmento_ftd
ORDER BY qtd_ftds DESC""",
    """SELECT
    c_ecr_id,
    ftd_date_brt,
    ftd_amount,
    segmento_ftd,
    first_casino_ts_utc,
    first_sports_ts_utc
FROM ftd_classified
ORDER BY ftd_date_brt, c_ecr_id""",
)


LEGENDA = f"""LEGENDA — FTD por Produto (Abril/2026 parcial)
==============================================

Periodo: {START_DATE} a {END_DATE} (parcial — hoje 22/04 e D-0, dados podem mudar)
Timezone: America/Sao_Paulo (BRT)

COLUNAS
-------
- segmento_ftd           — classificacao do FTD (ver Glossario)
- qtd_ftds               — quantidade de jogadores que fizeram FTD no periodo
- ftd_amount_total_brl   — soma dos valores de FTD no segmento (R$)
- ftd_amount_medio_brl   — ticket medio do FTD no segmento (R$) [KPI principal]
- ftd_amount_mediano_brl — mediana do FTD no segmento (R$) — menos sensivel a outliers
- ftd_amount_min_brl     — menor FTD observado no segmento
- ftd_amount_max_brl     — maior FTD observado no segmento

GLOSSARIO
---------
- FTD (First Time Deposit): primeiro deposito confirmado do jogador.
- FTD Cassino: jogador cujo primeiro BUYIN apos o FTD foi em jogo de cassino.
- FTD Esporte: jogador cujo primeiro BUYIN apos o FTD foi em aposta esportiva.
- FTD Sem Atividade: jogador que depositou mas nao apostou ate o corte.
- Segmento: e ATRIBUIDO via comportamento, nao existe no ledger.
  Depositos vao para a wallet unica do jogador — nao tem "carteira de produto".

RACIONAL
--------
Um deposito nao carrega produto na origem. Para responder a pergunta
"o FTD veio de cassino ou esporte?" inferimos pelo comportamento:
- Se a primeira aposta do jogador (apos depositar) foi em casino_ec2
  (c_txn_type 27/28, c_product_id='CASINO') -> FTD Cassino
- Se foi em vendor_ec2 / sportsbook (c_txn_type 59, c_product_id='SPORTS_BOOK')
  -> FTD Esporte

COMO LER
--------
Comparar ftd_amount_medio_brl entre "FTD Cassino" e "FTD Esporte":
- Se medio do Esporte < medio do Cassino -> hipotese do gestor confirmada
  (jogadores de esporte depositam menos e puxam o ticket geral para baixo).
- Se medio do Esporte >= medio do Cassino -> hipotese refutada.

Atencao ao segmento "FTD Sem Atividade": dinheiro que entrou mas nao
converteu em aposta — relevante para o time de tracking/CRM.

FONTES
------
- cashier_ec2.tbl_cashier_deposit  (FTD amount e timestamp)
- fund_ec2.tbl_real_fund_txn        (primeira aposta por produto)
- bireports_ec2.tbl_ecr             (flag c_test_user = false)

LIMITACOES
----------
- Dados de 22/04 sao parciais (D-0). Para fechamento, rodar novamente apos 23/04
  e idealmente recomendar ao gestor rodar uma segunda versao apos 28/04
  para capturar apostas tardias de jogadores que deram FTD entre 19-22/04
  e demoraram mais de 24h para apostar pela 1a vez.
- Jogador que apostou no mesmo milissegundo em cassino E esporte: desempata
  pelo cassino (praticamente inexistente).
- Rollback (c_txn_type 72) nao invalida classificacao — aposta contou.
- Desempate por timestamp da primeira aposta pode sub-representar a "intencao":
  jogador que apostou R$5 em cassino e R$500 em esporte entra como Cassino.
  Para analise complementar, pode ser util classificar por produto de maior
  volume nos primeiros 7 dias (nao implementado aqui — sob demanda).
- Bucket "FTD Sem Atividade" e critico: NAO ignorar ao comparar ticket medio
  entre Cassino e Esporte. Esse grupo representa dinheiro que entrou mas
  nao converteu — deve ser analisado separadamente pelo time de CRM/tracking.
"""


def main():
    print(f"[FTD-por-Produto] Rodando Athena — {START_DATE} a {END_DATE}")

    # 1. Agregado
    print("  -> Query agregada...")
    df_agg = query_athena(SQL, database="default")
    print(df_agg)

    # 2. Granular (opcional — util para auditoria)
    print("  -> Query granular (para auditoria)...")
    df_gran = query_athena(SQL_GRANULAR, database="default")

    # 3. Salvar outputs
    csv_agg = OUT_DIR / "ftd_por_produto_abril_agg.csv"
    csv_gran = OUT_DIR / "ftd_por_produto_abril.csv"
    legenda_path = OUT_DIR / "ftd_por_produto_abril_legenda.txt"

    df_agg.to_csv(csv_agg, index=False, encoding="utf-8-sig")
    df_gran.to_csv(csv_gran, index=False, encoding="utf-8-sig")
    legenda_path.write_text(LEGENDA, encoding="utf-8")

    print(f"\n[OK] Agregado:  {csv_agg}")
    print(f"[OK] Granular: {csv_gran}")
    print(f"[OK] Legenda:  {legenda_path}")

    # 4. Print formatado (tabela para o gestor)
    print("\n" + "=" * 80)
    print("FTD por Produto — Abril/2026 (01/04 a 22/04 parcial)")
    print("=" * 80)
    print(df_agg.to_string(index=False))
    print("=" * 80)

    # 5. Leitura para o gestor
    cassino = df_agg[df_agg["segmento_ftd"] == "FTD Cassino"]
    esporte = df_agg[df_agg["segmento_ftd"] == "FTD Esporte"]
    if not cassino.empty and not esporte.empty:
        medio_cas = cassino["ftd_amount_medio_brl"].iloc[0]
        medio_esp = esporte["ftd_amount_medio_brl"].iloc[0]
        delta = medio_esp - medio_cas
        sinal = "MENOR" if delta < 0 else "MAIOR"
        print(
            f"\nLeitura: ticket medio Esporte (R$ {medio_esp:.2f}) e "
            f"{sinal} que Cassino (R$ {medio_cas:.2f}), delta R$ {delta:+.2f}."
        )


if __name__ == "__main__":
    main()
