"""
Report: Análise de Promoções CRM — Turnover, UAP e Faixas
Fonte: fund_ec2 (Athena) + validação BigQuery (Smartico)

Demanda CRM — Março 2026:
Para cada promoção (6 promos, 8 jogos):
  1. Quantidade de usuários por faixa de turnover (independente do opt-in)
  2. Turnover total: mês anterior, durante, dia(s) seguinte(s)
  3. UAP: mês anterior, durante, dia(s) seguinte(s)

Regra "dias seguintes" (confirmada com CRM):
  Mesma janela de horas da promoção, no(s) dia(s) imediatamente após o encerramento.

Regra "mês anterior":
  Mesma janela de horas/dias, deslocada 1 mês para trás (fevereiro).

Otimização de custo Athena (conforme instrução do arquiteto):
  - fund_ec2.tbl_real_fund_txn NÃO tem partição dt
  - Filtro de c_start_time OBRIGATÓRIO no início do WHERE (reduz scan S3)
  - Apenas 2 queries no total: (1) DURING+AFTER per-user, (2) BEFORE aggregate
  - Valores em centavos (c_amount_in_ecr_ccy / 100.0)
  - Timestamps em UTC (Athena opera em UTC; BRT = UTC - 3h)
"""

import logging
import os
import sys
import pandas as pd
from datetime import datetime, timedelta

# Adiciona raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# ============================================================
# CONFIGURAÇÃO DAS 6 PROMOÇÕES
# ============================================================
# BRT → UTC: somar 3 horas
# Fim "23h59 BRT" → usar < 03:00:00 UTC dia seguinte (exclusivo)
# Fim "22h BRT"   → usar < 01:00:00 UTC dia seguinte (exclusivo)

PROMOS = [
    {
        "id": "P1",
        "name": "Tigre Sortudo",
        "periodo_brt": "14h 07/03 às 23h59 08/03",
        "game_ids": ["4776"],
        "smr_game_ids": [45838245],
        "during": ("2026-03-07 17:00:00", "2026-03-09 03:00:00"),
        "before": ("2026-02-07 17:00:00", "2026-02-09 03:00:00"),
        "after":  ("2026-03-09 17:00:00", "2026-03-11 03:00:00"),
        "faixas": [
            ("Gire entre R$50 a R$199",    50,    199.99),
            ("Gire entre R$200 a R$499",  200,    499.99),
            ("Gire entre R$500 a R$999",  500,    999.99),
            ("Gire R$1.000 ou mais",     1000, float("inf")),
        ],
    },
    {
        "id": "P2",
        "name": "Fortune Rabbit",
        "periodo_brt": "16h às 23h59 09/03",
        "game_ids": ["8842"],
        "smr_game_ids": [45708862],
        "during": ("2026-03-09 19:00:00", "2026-03-10 03:00:00"),
        "before": ("2026-02-09 19:00:00", "2026-02-10 03:00:00"),
        "after":  ("2026-03-10 19:00:00", "2026-03-11 03:00:00"),
        "faixas": [
            ("Gire entre R$30 a R$99",    30,  99.99),
            ("Gire entre R$100 a R$299", 100, 299.99),
            ("Gire entre R$300 a R$599", 300, 599.99),
            ("Gire R$600 ou mais",       600, float("inf")),
        ],
    },
    {
        "id": "P3",
        "name": "Gates of Olympus",
        "periodo_brt": "11h às 23h59 10/03",
        "game_ids": ["vs20olympgate"],
        "smr_game_ids": [45805477],  # resolvido via BigQuery 20/03
        "during": ("2026-03-10 14:00:00", "2026-03-11 03:00:00"),
        "before": ("2026-02-10 14:00:00", "2026-02-11 03:00:00"),
        "after":  ("2026-03-11 14:00:00", "2026-03-12 03:00:00"),
        "faixas": [
            ("Gire entre R$50 a R$99",    50,  99.99),
            ("Gire entre R$100 a R$299", 100, 299.99),
            ("Gire entre R$300 a R$499", 300, 499.99),
            ("Gire R$500 ou mais",       500, float("inf")),
        ],
    },
    {
        "id": "P4",
        "name": "Sweet Bonanza",
        "periodo_brt": "11h às 23h59 11/03",
        "game_ids": ["vs20fruitsw"],
        "smr_game_ids": [45883879],
        "during": ("2026-03-11 14:00:00", "2026-03-12 03:00:00"),
        "before": ("2026-02-11 14:00:00", "2026-02-12 03:00:00"),
        "after":  ("2026-03-12 14:00:00", "2026-03-13 03:00:00"),
        "faixas": [
            ("Gire entre R$15 e R$49",    15,  49.99),
            ("Gire entre R$50 a R$99",    50,  99.99),
            ("Gire entre R$100 a R$299", 100, 299.99),
            ("Gire R$300 ou mais",       300, float("inf")),
        ],
    },
    {
        "id": "P5",
        "name": "Fortune Ox",
        "periodo_brt": "18h às 22h 12/03",
        "game_ids": ["2603"],
        "smr_game_ids": [45846458],
        "during": ("2026-03-12 21:00:00", "2026-03-13 01:00:00"),
        "before": ("2026-02-12 21:00:00", "2026-02-13 01:00:00"),
        "after":  ("2026-03-13 21:00:00", "2026-03-14 01:00:00"),
        "faixas": [
            ("Gire entre R$30 a R$99",    30,  99.99),
            ("Gire entre R$100 a R$299", 100, 299.99),
            ("Gire entre R$300 a R$599", 300, 599.99),
            ("Gire R$600 ou mais",       600, float("inf")),
        ],
    },
    {
        "id": "P6",
        "name": "Combo FDS (Ratinho + Tigre + Macaco)",
        "periodo_brt": "17h 13/03 às 23h59 15/03",
        "game_ids": ["vs10forwild", "4776", "vs5luckym"],
        "smr_game_ids": [45881668, 45838245, 45872323],  # Ratinho, Tigre, Macaco — resolvido 20/03
        "during": ("2026-03-13 20:00:00", "2026-03-16 03:00:00"),
        "before": ("2026-02-13 20:00:00", "2026-02-16 03:00:00"),
        "after":  ("2026-03-16 20:00:00", "2026-03-19 03:00:00"),
        "faixas": [
            ("Gire entre R$50 a R$199",    50,  199.99),
            ("Gire entre R$200 a R$399",  200,  399.99),
            ("Gire entre R$400 a R$799",  400,  799.99),
            ("Gire entre R$800 a R$999",  800,  999.99),
            ("Gire R$1.000 ou mais",     1000, float("inf")),
        ],
    },
]

# Game IDs consolidados (sem duplicatas)
ALL_GAME_IDS = sorted(set(g for p in PROMOS for g in p["game_ids"]))


# ============================================================
# SQL BUILDERS
# ============================================================

def _game_condition(game_ids: list[str]) -> str:
    """Gera condição SQL para filtro de game_id."""
    if len(game_ids) == 1:
        return f"f.c_game_id = '{game_ids[0]}'"
    ids_str = ", ".join(f"'{g}'" for g in game_ids)
    return f"f.c_game_id IN ({ids_str})"


def _build_case_clauses(promos, period_keys: list[str]) -> str:
    """Gera CASE WHEN para rotular cada transação por promo+período."""
    clauses = []
    for period_key in period_keys:
        for p in promos:
            start, end = p[period_key]
            game_cond = _game_condition(p["game_ids"])
            clauses.append(
                f"        WHEN {game_cond}\n"
                f"             AND f.c_start_time >= TIMESTAMP '{start}'\n"
                f"             AND f.c_start_time <  TIMESTAMP '{end}'\n"
                f"             THEN '{p['id']}_{period_key}'"
            )
    return "CASE\n" + "\n".join(clauses) + "\n    END"


def build_during_after_query() -> str:
    """Query 1: turnover per-user para períodos DURING + AFTER.

    Um único scan de fund_ec2 cobrindo Mar 7 → Mar 19 (UTC).
    DURING clauses vêm primeiro para prioridade correta no CASE WHEN.
    """
    case_expr = _build_case_clauses(PROMOS, ["during", "after"])
    game_ids_str = ", ".join(f"'{g}'" for g in ALL_GAME_IDS)

    # Range mais amplo que cobre todos os during + after
    all_ts = []
    for p in PROMOS:
        all_ts.append(p["during"][0])
        all_ts.append(p["during"][1])
        all_ts.append(p["after"][0])
        all_ts.append(p["after"][1])
    min_start = min(all_ts)
    max_end = max(all_ts)

    return f"""
-- ============================================================
-- Query 1/2: Per-user turnover (DURING + AFTER)
-- Filtro de timestamp PRIMEIRO para minimizar scan fund_ec2
-- Range UTC: {min_start} → {max_end}
-- ============================================================
WITH labeled AS (
    SELECT
        f.c_ecr_id,
        {case_expr} AS promo_period,
        f.c_amount_in_ecr_ccy
    FROM fund_ec2.tbl_real_fund_txn f
    JOIN bireports_ec2.tbl_ecr b ON b.c_ecr_id = f.c_ecr_id
    WHERE f.c_start_time >= TIMESTAMP '{min_start}'
      AND f.c_start_time <  TIMESTAMP '{max_end}'
      AND f.c_game_id IN ({game_ids_str})
      AND f.c_txn_type = 27
      AND f.c_txn_status = 'SUCCESS'
      AND b.c_test_user = false
)
SELECT
    c_ecr_id,
    promo_period,
    SUM(c_amount_in_ecr_ccy) / 100.0 AS turnover_brl
FROM labeled
WHERE promo_period IS NOT NULL
GROUP BY 1, 2
"""


def build_before_query() -> str:
    """Query 2: turnover e UAP agregados para períodos BEFORE.

    Um único scan de fund_ec2 cobrindo Fev 7 → Fev 16 (UTC).
    Retorna apenas 6 linhas (uma por promo).
    """
    case_expr = _build_case_clauses(PROMOS, ["before"])
    game_ids_str = ", ".join(f"'{g}'" for g in ALL_GAME_IDS)

    all_ts = []
    for p in PROMOS:
        all_ts.append(p["before"][0])
        all_ts.append(p["before"][1])
    min_start = min(all_ts)
    max_end = max(all_ts)

    return f"""
-- ============================================================
-- Query 2/2: Aggregate turnover + UAP (BEFORE — mês anterior)
-- Filtro de timestamp PRIMEIRO para minimizar scan fund_ec2
-- Range UTC: {min_start} → {max_end}
-- ============================================================
WITH labeled AS (
    SELECT
        f.c_ecr_id,
        {case_expr} AS promo_period,
        f.c_amount_in_ecr_ccy
    FROM fund_ec2.tbl_real_fund_txn f
    JOIN bireports_ec2.tbl_ecr b ON b.c_ecr_id = f.c_ecr_id
    WHERE f.c_start_time >= TIMESTAMP '{min_start}'
      AND f.c_start_time <  TIMESTAMP '{max_end}'
      AND f.c_game_id IN ({game_ids_str})
      AND f.c_txn_type = 27
      AND f.c_txn_status = 'SUCCESS'
      AND b.c_test_user = false
)
SELECT
    promo_period,
    COUNT(DISTINCT c_ecr_id) AS uap,
    SUM(c_amount_in_ecr_ccy) / 100.0 AS turnover_brl
FROM labeled
WHERE promo_period IS NOT NULL
GROUP BY 1
"""


# ============================================================
# PROCESSAMENTO DE DADOS
# ============================================================

def classify_users(df_users: pd.DataFrame, faixas: list) -> pd.DataFrame:
    """Classifica cada usuário na faixa de turnover correspondente.

    Regra: a faixa é determinada pelo turnover total (soma de bets).
    Se o valor cai entre valor_min e valor_max da faixa, o user pertence a ela.
    Users abaixo da faixa mínima são marcados separadamente.
    """
    min_threshold = min(f[1] for f in faixas)
    # Ordenar faixas por valor_min DESC (maior primeiro) para match rápido
    faixas_sorted = sorted(faixas, key=lambda f: f[1], reverse=True)

    def _classify(turnover):
        for nome, vmin, vmax in faixas_sorted:
            if vmin <= turnover <= vmax:
                return nome
        if turnover < min_threshold:
            return f"Abaixo de R${min_threshold:.0f}"
        return "Sem classificação"

    df = df_users.copy()
    df["faixa"] = df["turnover_brl"].apply(_classify)
    return df


def build_tier_summary(df_classified: pd.DataFrame, faixas: list) -> pd.DataFrame:
    """Gera tabela resumo com distribuição por faixa."""
    faixa_names = [f[0] for f in faixas]

    summary = (
        df_classified.groupby("faixa")
        .agg(
            qty_users=("c_ecr_id", "count"),
            turnover_total=("turnover_brl", "sum"),
        )
        .reset_index()
    )

    # Garantir que todas as faixas apareçam (mesmo com 0)
    for nome in faixa_names:
        if nome not in summary["faixa"].values:
            new_row = pd.DataFrame([{
                "faixa": nome, "qty_users": 0, "turnover_total": 0.0,
            }])
            summary = pd.concat([summary, new_row], ignore_index=True)

    # Ordenar pela sequência original das faixas
    order_map = {nome: i for i, nome in enumerate(faixa_names)}
    summary["_order"] = summary["faixa"].map(order_map).fillna(999)
    summary = summary.sort_values("_order").drop(columns="_order").reset_index(drop=True)

    # Percentuais
    total_u = summary["qty_users"].sum()
    total_t = summary["turnover_total"].sum()
    summary["pct_users"] = (
        (summary["qty_users"] / total_u * 100).round(1) if total_u > 0 else 0
    )
    summary["pct_turnover"] = (
        (summary["turnover_total"] / total_t * 100).round(1) if total_t > 0 else 0
    )

    return summary


# ============================================================
# VALIDAÇÃO BIGQUERY (SMARTICO)
# ============================================================

def resolve_smartico_game_ids() -> dict:
    """Busca Smartico game IDs para jogos sem mapeamento hardcoded."""
    from db.bigquery import query_bigquery

    sql = """
    SELECT smr_game_id, game_name
    FROM `smartico-bq6.dwh_ext_24105.dm_casino_game_name`
    WHERE LOWER(game_name) LIKE '%gates of olympus%'
       OR LOWER(game_name) LIKE '%ratinho sortudo%'
       OR LOWER(game_name) LIKE '%ratinho%lucky%'
       OR LOWER(game_name) LIKE '%macaco sortudo%'
       OR LOWER(game_name) LIKE '%macaco%lucky%'
       OR LOWER(game_name) LIKE '%monkey%lucky%'
       OR LOWER(game_name) LIKE '%lucky monkey%'
       OR LOWER(game_name) LIKE '%fortune wild%'
    ORDER BY game_name
    """
    df = query_bigquery(sql)
    log.info(f"Smartico game catalog (busca IDs faltantes):\n{df.to_string()}")
    return df


def validate_with_bigquery(promos: list, athena_agg: dict) -> pd.DataFrame:
    """Validação cruzada: compara totais DURING do Athena com BigQuery."""
    from db.bigquery import query_bigquery

    results = []
    for p in promos:
        if not p["smr_game_ids"]:
            log.warning(f"  ⚠ Sem Smartico IDs para {p['name']} — pulando validação BQ")
            results.append({
                "Promoção": p["name"],
                "Athena UAP": athena_agg.get(f"{p['id']}_during", {}).get("uap", 0),
                "BQ UAP": "N/A",
                "Diff UAP (%)": "N/A",
                "Athena Turnover (R$)": round(
                    athena_agg.get(f"{p['id']}_during", {}).get("turnover", 0), 2
                ),
                "BQ Turnover (R$)": "N/A",
                "Diff Turnover (%)": "N/A",
                "OK?": "⚠ Sem ID",
            })
            continue

        start_utc, end_utc = p["during"]
        smr_ids = ", ".join(str(g) for g in p["smr_game_ids"])

        sql = f"""
        SELECT
            COUNT(DISTINCT b.user_id) AS uap_bq,
            SUM(CAST(b.casino_last_bet_amount_real AS FLOAT64)) AS turnover_brl_bq
        FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet` b
        WHERE b.casino_last_bet_game_name IN ({smr_ids})
          AND b.event_time >= TIMESTAMP '{start_utc}'
          AND b.event_time <  TIMESTAMP '{end_utc}'
        """
        try:
            df = query_bigquery(sql)
            bq_uap = int(df["uap_bq"].iloc[0]) if len(df) > 0 and pd.notna(df["uap_bq"].iloc[0]) else 0
            bq_turn = float(df["turnover_brl_bq"].iloc[0]) if len(df) > 0 and pd.notna(df["turnover_brl_bq"].iloc[0]) else 0.0
        except Exception as e:
            log.warning(f"  ⚠ BigQuery falhou para {p['name']}: {e}")
            bq_uap = 0
            bq_turn = 0.0

        ath = athena_agg.get(f"{p['id']}_during", {"uap": 0, "turnover": 0})
        diff_uap = abs(ath["uap"] - bq_uap) / max(ath["uap"], 1) * 100
        diff_turn = abs(ath["turnover"] - bq_turn) / max(ath["turnover"], 1) * 100
        ok = diff_uap < 5 and diff_turn < 5

        results.append({
            "Promoção": p["name"],
            "Athena UAP": ath["uap"],
            "BQ UAP": bq_uap,
            "Diff UAP (%)": round(diff_uap, 1),
            "Athena Turnover (R$)": round(ath["turnover"], 2),
            "BQ Turnover (R$)": round(bq_turn, 2),
            "Diff Turnover (%)": round(diff_turn, 1),
            "OK?": "OK" if ok else "DIVERGE",
        })
        log.info(
            f"  {p['name']}: UAP {ath['uap']} vs {bq_uap} "
            f"(diff {diff_uap:.1f}%) | Turnover {ath['turnover']:,.2f} vs {bq_turn:,.2f} "
            f"(diff {diff_turn:.1f}%) → {'OK' if ok else 'DIVERGE'}"
        )

    return pd.DataFrame(results)


# ============================================================
# GERAÇÃO DO EXCEL
# ============================================================

def _pct_change(before: float, after: float) -> str:
    """Calcula variação percentual between → after."""
    if before == 0:
        return "N/A" if after == 0 else "+∞"
    pct = (after - before) / before * 100
    sign = "+" if pct > 0 else ""
    return f"{sign}{pct:.1f}%"


def generate_excel(
    promos: list,
    tier_summaries: dict,
    period_summaries: dict,
    validation_df: pd.DataFrame,
    output_path: str,
):
    """Gera Excel consolidado com todas as análises."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:

        # === Sheet 1: Resumo Consolidado ===
        resumo_rows = []
        for p in promos:
            pid = p["id"]
            bef = period_summaries.get(f"{pid}_before", {"uap": 0, "turnover": 0})
            dur = period_summaries.get(f"{pid}_during", {"uap": 0, "turnover": 0})
            aft = period_summaries.get(f"{pid}_after",  {"uap": 0, "turnover": 0})

            resumo_rows.append({
                "Promoção": p["name"],
                "Período (BRT)": p["periodo_brt"],
                "Turnover Mês Anterior (R$)": round(bef["turnover"], 2),
                "UAP Mês Anterior": bef["uap"],
                "Turnover Durante (R$)": round(dur["turnover"], 2),
                "UAP Durante": dur["uap"],
                "Turnover Dia Seguinte (R$)": round(aft["turnover"], 2),
                "UAP Dia Seguinte": aft["uap"],
                "Var. Turnover Antes→Durante": _pct_change(bef["turnover"], dur["turnover"]),
                "Var. Turnover Durante→Depois": _pct_change(dur["turnover"], aft["turnover"]),
                "Var. UAP Antes→Durante": _pct_change(bef["uap"], dur["uap"]),
                "Var. UAP Durante→Depois": _pct_change(dur["uap"], aft["uap"]),
            })

        df_resumo = pd.DataFrame(resumo_rows)
        df_resumo.to_excel(writer, sheet_name="Resumo", index=False)

        # === Sheets por promoção: distribuição por faixa ===
        for p in promos:
            pid = p["id"]
            # Nome da sheet truncado (max 31 chars no Excel)
            sheet_name = f"{pid} {p['name']}"[:31]

            tier_df = tier_summaries.get(pid)
            if tier_df is not None and not tier_df.empty:
                # Renomear colunas para apresentação
                out = tier_df.rename(columns={
                    "faixa": "Faixa",
                    "qty_users": "Qtd Usuários",
                    "turnover_total": "Turnover Total (R$)",
                    "pct_users": "% Usuários",
                    "pct_turnover": "% Turnover",
                })
                out["Turnover Total (R$)"] = out["Turnover Total (R$)"].round(2)
                out.to_excel(writer, sheet_name=sheet_name, index=False)

        # === Sheet: Validação BigQuery ===
        if validation_df is not None and not validation_df.empty:
            validation_df.to_excel(writer, sheet_name="Validacao BQ", index=False)

    log.info(f"Excel salvo: {output_path}")


# ============================================================
# MAIN
# ============================================================

def main():
    from db.athena import query_athena

    log.info("=" * 60)
    log.info("RELATÓRIO PROMOÇÕES CRM — Mar/2026")
    log.info("Fonte: fund_ec2 (Athena) | Validação: BigQuery (Smartico)")
    log.info("2 queries Athena | 7 game_ids | 6 promoções")
    log.info("=" * 60)

    # ----------------------------------------------------------
    # STEP 1: Query DURING + AFTER (per-user turnover)
    # ----------------------------------------------------------
    log.info("\n[1/5] Query DURING + AFTER no Athena (per-user)...")
    sql_da = build_during_after_query()
    log.info(f"SQL gerado ({len(sql_da)} chars). Executando...")
    df_da = query_athena(sql_da, database="fund_ec2")
    log.info(f"Resultado: {len(df_da):,} linhas (users × promo_period)")

    # ----------------------------------------------------------
    # STEP 2: Query BEFORE (aggregate)
    # ----------------------------------------------------------
    log.info("\n[2/5] Query BEFORE no Athena (aggregate)...")
    sql_bef = build_before_query()
    log.info(f"SQL gerado ({len(sql_bef)} chars). Executando...")
    df_before = query_athena(sql_bef, database="fund_ec2")
    log.info(f"Resultado BEFORE:\n{df_before.to_string(index=False)}")

    # ----------------------------------------------------------
    # STEP 3: Processar dados
    # ----------------------------------------------------------
    log.info("\n[3/5] Processando dados...")

    period_summaries = {}
    tier_summaries = {}

    # 3a. BEFORE — já agregado direto do SQL
    for _, row in df_before.iterrows():
        period_summaries[row["promo_period"]] = {
            "uap": int(row["uap"]),
            "turnover": float(row["turnover_brl"]),
        }

    # 3b. DURING + AFTER — agregar per-user → totais
    for p in PROMOS:
        pid = p["id"]
        for period_key in ["during", "after"]:
            label = f"{pid}_{period_key}"
            df_period = df_da[df_da["promo_period"] == label]
            period_summaries[label] = {
                "uap": int(df_period["c_ecr_id"].nunique()),
                "turnover": float(df_period["turnover_brl"].sum()),
            }

        # 3c. Classificar users em faixas (somente DURING)
        df_during = df_da[df_da["promo_period"] == f"{pid}_during"].copy()
        if not df_during.empty:
            df_classified = classify_users(df_during, p["faixas"])
            tier_summaries[pid] = build_tier_summary(df_classified, p["faixas"])

    # Log resumo dos períodos
    log.info("\n" + "=" * 60)
    log.info("RESUMO POR PROMOÇÃO")
    log.info("=" * 60)
    for p in PROMOS:
        pid = p["id"]
        log.info(f"\n--- {p['name']} ({p['periodo_brt']}) ---")
        for period_label, period_name in [
            ("before", "Mês Anterior"),
            ("during", "Durante"),
            ("after",  "Dia Seguinte"),
        ]:
            d = period_summaries.get(f"{pid}_{period_label}", {"uap": 0, "turnover": 0})
            log.info(
                f"  {period_name:14s}: Turnover R$ {d['turnover']:>12,.2f} | "
                f"UAP {d['uap']:>6,}"
            )
        # Faixas
        if pid in tier_summaries:
            log.info(f"\n  Distribuição por faixa:")
            for _, row in tier_summaries[pid].iterrows():
                log.info(
                    f"    {row['faixa']:30s}: {int(row['qty_users']):>5} users "
                    f"({row['pct_users']:>5.1f}%) | "
                    f"R$ {row['turnover_total']:>12,.2f} ({row['pct_turnover']:>5.1f}%)"
                )

    # ----------------------------------------------------------
    # STEP 4: Validação BigQuery
    # ----------------------------------------------------------
    log.info("\n[4/5] Validação cruzada BigQuery (Smartico)...")
    validation_df = pd.DataFrame()
    try:
        # Resolver Smartico IDs faltantes
        smr_catalog = resolve_smartico_game_ids()
        # Aqui poderíamos atualizar os smr_game_ids dos promos P3 e P6
        # baseado nos resultados do catálogo. Log para análise manual.

        athena_agg = {}
        for p in PROMOS:
            key = f"{p['id']}_during"
            athena_agg[key] = period_summaries.get(key, {"uap": 0, "turnover": 0})

        validation_df = validate_with_bigquery(PROMOS, athena_agg)
        log.info(f"\nResultado validação:\n{validation_df.to_string(index=False)}")
    except Exception as e:
        log.warning(f"Validação BigQuery falhou: {e}")

    # ----------------------------------------------------------
    # STEP 5: Gerar Excel
    # ----------------------------------------------------------
    log.info("\n[5/5] Gerando Excel consolidado...")
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "report_crm_promocoes_mar2026.xlsx")

    generate_excel(PROMOS, tier_summaries, period_summaries, validation_df, output_path)

    log.info(f"\n{'=' * 60}")
    log.info(f"CONCLUÍDO! Excel: {output_path}")
    log.info(f"{'=' * 60}")

    return output_path


if __name__ == "__main__":
    main()
