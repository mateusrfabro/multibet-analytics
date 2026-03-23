"""
validar_gold_vs_raw.py — Validacao Cruzada Gold vs Raw
=======================================================
Processo obrigatorio do Auditor QA antes de entregar qualquer report financeiro.
Compara camada gold (bireports_ec2) vs raw (fund_ec2) para garantir consistencia.

Uso:
    python validacoes/validar_gold_vs_raw.py                           # todos affiliates, ontem
    python validacoes/validar_gold_vs_raw.py --date 2026-03-20         # todos affiliates, data especifica
    python validacoes/validar_gold_vs_raw.py --affiliate 464673        # affiliate especifico, ontem
    python validacoes/validar_gold_vs_raw.py --affiliate 464673 --date 2026-03-20

Margens de divergencia (proposta Auditor QA 21/03/2026):
    < 0.5%  VERDE    — OK, aprovado automaticamente
    0.5-2%  AMARELO  — alerta, aprovado com ressalva
    2-5%    LARANJA  — hold, investigar causa raiz
    > 5%    VERMELHO — bloqueado, escalar para infra

Autor: Squad 3 — Intelligence Engine (Auditor QA)
Data: 21/03/2026
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from db.athena import query_athena

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Margens de divergencia
# ---------------------------------------------------------------------------
THRESHOLD_VERDE = 0.5
THRESHOLD_AMARELO = 2.0
THRESHOLD_LARANJA = 5.0


def classificar_divergencia(pct: float) -> dict:
    """Retorna status e acao baseado na % de divergencia."""
    if pct < THRESHOLD_VERDE:
        return {"status": "VERDE", "acao": "OK — aprovado automaticamente", "bloqueante": False}
    elif pct < THRESHOLD_AMARELO:
        return {"status": "AMARELO", "acao": "Alerta — aprovado com ressalva, documentar", "bloqueante": False}
    elif pct < THRESHOLD_LARANJA:
        return {"status": "LARANJA", "acao": "Hold — investigar causa raiz antes de entregar", "bloqueante": True}
    else:
        return {"status": "VERMELHO", "acao": "Bloqueado — escalar para Gusta/infra", "bloqueante": True}


# ---------------------------------------------------------------------------
# Queries de validacao por metrica
# ---------------------------------------------------------------------------

def _build_sql_dep_amount(affiliate_id: str, target_date: str) -> str:
    """Query gold vs raw para DEP AMOUNT."""
    # Data seguinte para filtro raw (UTC boundary)
    dt_next = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    return f"""
    -- ============================================================
    -- VALIDACAO CRUZADA: Gold vs Raw — DEP AMOUNT
    -- Affiliate: {affiliate_id} | Data: {target_date}
    -- ============================================================

    -- GOLD: bireports_ec2.tbl_ecr_wise_daily_bi_summary
    -- Valores em centavos (/100.0)
    WITH gold_players AS (
        SELECT DISTINCT e.c_ecr_id
        FROM bireports_ec2.tbl_ecr e
        WHERE CAST(e.c_affiliate_id AS VARCHAR) = '{affiliate_id}'
          AND e.c_test_user = false
    ),
    gold AS (
        SELECT
            'GOLD' AS fonte,
            COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0 AS valor_brl,
            COALESCE(SUM(s.c_deposit_success_count), 0) AS txn_count,
            COUNT(DISTINCT s.c_ecr_id) AS players
        FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
        INNER JOIN gold_players p ON s.c_ecr_id = p.c_ecr_id
        WHERE s.c_created_date = DATE '{target_date}'
    ),

    -- RAW: fund_ec2.tbl_real_fund_txn
    -- c_txn_type=1 (deposito), c_txn_status='SUCCESS'
    -- Valores em centavos (/100.0)
    -- Timezone: 03:00 UTC = 00:00 BRT
    raw_players AS (
        SELECT DISTINCT e.c_ecr_id
        FROM ecr_ec2.tbl_ecr e
        WHERE CAST(e.c_affiliate_id AS VARCHAR) = '{affiliate_id}'
          AND e.c_ecr_id NOT IN (
            SELECT b.c_ecr_id FROM bireports_ec2.tbl_ecr b WHERE b.c_test_user = true
          )
    ),
    raw AS (
        SELECT
            'RAW' AS fonte,
            COALESCE(SUM(f.c_amount_in_ecr_ccy), 0) / 100.0 AS valor_brl,
            COUNT(*) AS txn_count,
            COUNT(DISTINCT f.c_ecr_id) AS players
        FROM fund_ec2.tbl_real_fund_txn f
        INNER JOIN raw_players p ON f.c_ecr_id = p.c_ecr_id
        WHERE f.c_txn_type = 1
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_start_time >= TIMESTAMP '{target_date} 03:00:00'
          AND f.c_start_time <  TIMESTAMP '{dt_next} 03:00:00'
    )

    SELECT * FROM gold
    UNION ALL
    SELECT * FROM raw
    """


def _build_sql_saques(affiliate_id: str, target_date: str) -> str:
    """Query gold vs raw para SAQUES."""
    dt_next = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    return f"""
    WITH gold_players AS (
        SELECT DISTINCT e.c_ecr_id
        FROM bireports_ec2.tbl_ecr e
        WHERE CAST(e.c_affiliate_id AS VARCHAR) = '{affiliate_id}'
          AND e.c_test_user = false
    ),
    gold AS (
        SELECT 'GOLD' AS fonte,
            COALESCE(SUM(s.c_co_success_amount), 0) / 100.0 AS valor_brl,
            COALESCE(SUM(s.c_co_success_count), 0) AS txn_count,
            COUNT(DISTINCT s.c_ecr_id) AS players
        FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
        INNER JOIN gold_players p ON s.c_ecr_id = p.c_ecr_id
        WHERE s.c_created_date = DATE '{target_date}'
    ),
    raw_players AS (
        SELECT DISTINCT e.c_ecr_id
        FROM ecr_ec2.tbl_ecr e
        WHERE CAST(e.c_affiliate_id AS VARCHAR) = '{affiliate_id}'
          AND e.c_ecr_id NOT IN (
            SELECT b.c_ecr_id FROM bireports_ec2.tbl_ecr b WHERE b.c_test_user = true
          )
    ),
    raw AS (
        SELECT 'RAW' AS fonte,
            COALESCE(SUM(f.c_amount_in_ecr_ccy), 0) / 100.0 AS valor_brl,
            COUNT(*) AS txn_count,
            COUNT(DISTINCT f.c_ecr_id) AS players
        FROM fund_ec2.tbl_real_fund_txn f
        INNER JOIN raw_players p ON f.c_ecr_id = p.c_ecr_id
        WHERE f.c_txn_type = 2
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_start_time >= TIMESTAMP '{target_date} 03:00:00'
          AND f.c_start_time <  TIMESTAMP '{dt_next} 03:00:00'
    )
    SELECT * FROM gold UNION ALL SELECT * FROM raw
    """


def _build_sql_ggr_casino(affiliate_id: str, target_date: str) -> str:
    """Query gold vs raw para GGR CASSINO."""
    dt_next = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    return f"""
    WITH gold_players AS (
        SELECT DISTINCT e.c_ecr_id
        FROM bireports_ec2.tbl_ecr e
        WHERE CAST(e.c_affiliate_id AS VARCHAR) = '{affiliate_id}'
          AND e.c_test_user = false
    ),
    gold AS (
        SELECT 'GOLD' AS fonte,
            COALESCE(SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount), 0) / 100.0 AS valor_brl,
            0 AS txn_count,
            COUNT(DISTINCT s.c_ecr_id) AS players
        FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
        INNER JOIN gold_players p ON s.c_ecr_id = p.c_ecr_id
        WHERE s.c_created_date = DATE '{target_date}'
    ),
    raw_players AS (
        SELECT DISTINCT e.c_ecr_id
        FROM ecr_ec2.tbl_ecr e
        WHERE CAST(e.c_affiliate_id AS VARCHAR) = '{affiliate_id}'
          AND e.c_ecr_id NOT IN (
            SELECT b.c_ecr_id FROM bireports_ec2.tbl_ecr b WHERE b.c_test_user = true
          )
    ),
    raw AS (
        SELECT 'RAW' AS fonte,
            COALESCE(
                SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy ELSE 0 END)
              - SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy ELSE 0 END)
            , 0) / 100.0 AS valor_brl,
            SUM(CASE WHEN f.c_txn_type IN (27, 45) THEN 1 ELSE 0 END) AS txn_count,
            COUNT(DISTINCT f.c_ecr_id) AS players
        FROM fund_ec2.tbl_real_fund_txn f
        INNER JOIN raw_players p ON f.c_ecr_id = p.c_ecr_id
        WHERE f.c_txn_type IN (27, 45)
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_start_time >= TIMESTAMP '{target_date} 03:00:00'
          AND f.c_start_time <  TIMESTAMP '{dt_next} 03:00:00'
    )
    SELECT * FROM gold UNION ALL SELECT * FROM raw
    """


# Mapeamento de metricas
METRICAS = {
    'dep_amount': {
        'nome': 'DEP AMOUNT',
        'builder': _build_sql_dep_amount,
        'prioridade': 'CRITICA',
    },
    'saques': {
        'nome': 'SAQUES',
        'builder': _build_sql_saques,
        'prioridade': 'CRITICA',
    },
    'ggr_casino': {
        'nome': 'GGR CASSINO',
        'builder': _build_sql_ggr_casino,
        'prioridade': 'ALTA',
    },
}


def validar_metrica(affiliate_id: str, target_date: str, metrica: str) -> dict:
    """
    Executa validacao cruzada gold vs raw para uma metrica.

    Returns:
        dict com gold_value, raw_value, diff_abs, diff_pct, status, acao, bloqueante
    """
    config = METRICAS[metrica]
    sql = config['builder'](affiliate_id, target_date)

    logger.info(f"Validando {config['nome']} — affiliate {affiliate_id}, dia {target_date}")

    try:
        df = query_athena(sql, database='bireports_ec2')
    except Exception as e:
        logger.error(f"Erro ao executar query: {e}")
        return {
            "metrica": config['nome'],
            "gold_value": None, "raw_value": None,
            "diff_abs": None, "diff_pct": None,
            "status": "VERMELHO", "acao": f"Erro na query: {e}",
            "bloqueante": True
        }

    if df is None or len(df) < 2:
        logger.error("Query retornou menos de 2 linhas")
        return {
            "metrica": config['nome'],
            "gold_value": None, "raw_value": None,
            "diff_abs": None, "diff_pct": None,
            "status": "VERMELHO", "acao": "Query retornou dados incompletos",
            "bloqueante": True
        }

    gold = float(df.iloc[0]['valor_brl'] or 0)
    raw = float(df.iloc[1]['valor_brl'] or 0)
    diff = abs(gold - raw)
    pct = diff / gold * 100 if gold > 0 else (0 if raw == 0 else 100)

    classificacao = classificar_divergencia(pct)

    result = {
        "metrica": config['nome'],
        "gold_value": round(gold, 2),
        "raw_value": round(raw, 2),
        "diff_abs": round(diff, 2),
        "diff_pct": round(pct, 4),
        **classificacao
    }

    # Log
    emoji = {"VERDE": "OK", "AMARELO": "!!", "LARANJA": "??", "VERMELHO": "XX"}
    logger.info(
        f"  [{emoji[result['status']]}] {result['status']} — "
        f"Gold: R$ {gold:,.2f} | Raw: R$ {raw:,.2f} | "
        f"Diff: R$ {diff:,.2f} ({pct:.2f}%)"
    )

    return result


def validar_todas_metricas(affiliate_id: str, target_date: str) -> list:
    """Roda validacao para todas as metricas configuradas."""
    resultados = []
    for metrica_key in METRICAS:
        r = validar_metrica(affiliate_id, target_date, metrica_key)
        resultados.append(r)
    return resultados


def imprimir_relatorio(resultados: list, affiliate_id: str, target_date: str):
    """Imprime relatorio consolidado de validacao."""
    print()
    print("=" * 70)
    print(f"VALIDACAO GOLD vs RAW — Affiliate {affiliate_id} | {target_date}")
    print("=" * 70)
    print()
    print(f"{'Metrica':<15} {'Gold':>15} {'Raw':>15} {'Diff':>12} {'%':>8} {'Status':<10}")
    print("-" * 70)

    bloqueantes = 0
    for r in resultados:
        gold_str = f"R$ {r['gold_value']:,.2f}" if r['gold_value'] is not None else "ERRO"
        raw_str = f"R$ {r['raw_value']:,.2f}" if r['raw_value'] is not None else "ERRO"
        diff_str = f"R$ {r['diff_abs']:,.2f}" if r['diff_abs'] is not None else "ERRO"
        pct_str = f"{r['diff_pct']:.2f}%" if r['diff_pct'] is not None else "ERRO"

        print(f"{r['metrica']:<15} {gold_str:>15} {raw_str:>15} {diff_str:>12} {pct_str:>8} {r['status']:<10}")

        if r['bloqueante']:
            bloqueantes += 1

    print("-" * 70)
    print()

    if bloqueantes == 0:
        print("VEREDICTO: APROVADO — todas as metricas dentro da margem")
    else:
        print(f"VEREDICTO: BLOQUEADO — {bloqueantes} metrica(s) fora da margem")
        for r in resultados:
            if r['bloqueante']:
                print(f"  - {r['metrica']}: {r['status']} — {r['acao']}")

    print("=" * 70)

    return bloqueantes == 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Validacao cruzada Gold vs Raw')
    parser.add_argument('--affiliate', type=str, default=None,
                        help='Affiliate ID (default: todos os configurados)')
    parser.add_argument('--date', type=str, default=None,
                        help='Data alvo YYYY-MM-DD (default: ontem)')
    parser.add_argument('--metric', type=str, default=None,
                        choices=list(METRICAS.keys()),
                        help='Metrica especifica (default: todas)')
    args = parser.parse_args()

    # Data padrao: ontem
    if args.date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        target_date = args.date

    # Affiliates padrao (do dashboard)
    if args.affiliate is None:
        affiliates = ['297657', '445431', '468114', '464673']
    else:
        affiliates = [args.affiliate]

    logger.info("=" * 60)
    logger.info("VALIDACAO CRUZADA GOLD vs RAW — Auditor QA")
    logger.info(f"Data: {target_date} | Affiliates: {affiliates}")
    logger.info("=" * 60)

    todos_aprovados = True

    for aff in affiliates:
        if args.metric:
            resultados = [validar_metrica(aff, target_date, args.metric)]
        else:
            resultados = validar_todas_metricas(aff, target_date)

        aprovado = imprimir_relatorio(resultados, aff, target_date)
        if not aprovado:
            todos_aprovados = False

    if todos_aprovados:
        logger.info("RESULTADO FINAL: TODOS APROVADOS")
    else:
        logger.warning("RESULTADO FINAL: BLOQUEIOS DETECTADOS — verificar acima")

    return 0 if todos_aprovados else 1


if __name__ == '__main__':
    sys.exit(main())
