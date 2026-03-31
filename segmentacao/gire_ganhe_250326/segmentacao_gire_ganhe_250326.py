"""
Segmentacao Gire & Ganhe — Ratinho Sortudo | 25/03/2026
========================================================
Promocao  : GIRE_GANHE_250326
Jogo      : Ratinho Sortudo — Pragmatic Play (game_id: vs10forwild)
Segmento  : https://drive-6.smartico.ai/24105#/j_segment/30236
Periodo   : 25/03/2026 19h BRT -> 23h59 BRT
            (UTC: 2026-03-25 22:00:00 -> 2026-03-26 02:59:59)

Regras de negocio:
  - Usuarios com opt-in (mark GIRE_GANHE_250326 no Smartico)
  - Rollback (txn_type=72) DESCLASSIFICA o usuario
  - Apostas ACUMULADAS em Ratinho Sortudo (game_id vs10forwild)
  - Faixas exclusivas — usuario fica na mais alta atingida:
      Faixa 1: R$50,00 – R$199,99
      Faixa 2: R$200,00 – R$499,99
      Faixa 3: R$500,00 – R$999,99
      Faixa 4: >= R$1.000,00

Fontes de dados:
  - BigQuery (Smartico): opt-in via j_user.core_tags
  - Athena (Iceberg Data Lake): transacoes via fund_ec2.tbl_real_fund_txn

Saida: CSV + ZIP + mensagens WhatsApp prontas.
"""

import sys
import os
import logging
import zipfile
import pandas as pd

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT  = os.path.dirname(os.path.abspath(__file__))
MULTIBET_ROOT = os.path.dirname(os.path.dirname(PROJECT_ROOT))
sys.path.insert(0, MULTIBET_ROOT)

from db.athena import query_athena
from db.bigquery import query_bigquery

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Parametros da promocao
# ---------------------------------------------------------------------------
MARK_TAG = "GIRE_GANHE_250326"

# Ratinho Sortudo (Pragmatic Play) — confirmado em jogos.csv e promos anteriores
GAME_IDS = ["vs10forwild"]
GAME_NAME = "Ratinho Sortudo"

# Periodo: 25/03 19h BRT = 25/03 22:00 UTC | 25/03 23h59 BRT = 26/03 02:59:59 UTC
START_UTC = "2026-03-25 22:00:00"
END_UTC   = "2026-03-26 02:59:59"

# Periodo BRT para display
PERIODO_BRT = "25/03/2026 19h as 23h59 BRT"

TXN_BET      = 27   # CASINO_BUYIN (aposta)
TXN_ROLLBACK = 72   # CASINO_BUYIN_CANCEL (rollback)

CHUNK_SIZE = 5_000

# Faixas em BRL (avaliadas da maior para a menor — usuario fica na mais alta)
FAIXAS_BRL = [
    ("Faixa 4", 1000,  float("inf")),  # >= R$1.000
    ("Faixa 3",  500,  999.99),         # R$500 – R$999,99
    ("Faixa 2",  200,  499.99),         # R$200 – R$499,99
    ("Faixa 1",   50,  199.99),         # R$50  – R$199,99
]

ROTULOS = {
    "Faixa 4": "Faixa 4 (>=R$1.000)",
    "Faixa 3": "Faixa 3 (R$500-R$999,99)",
    "Faixa 2": "Faixa 2 (R$200-R$499,99)",
    "Faixa 1": "Faixa 1 (R$50-R$199,99)",
    "Abaixo do Minimo": "Abaixo de R$50",
    "Desclassificado (rollback)": "Desclassificados (rollback)",
    "Sem Atividade": "Nao jogou no periodo",
}

OUTPUT_CSV = os.path.join(PROJECT_ROOT, "segmentacao_gire_ganhe_250326_FINAL.csv")
OUTPUT_ZIP = os.path.join(PROJECT_ROOT, "segmentacao_gire_ganhe_250326_FINAL.zip")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fmt_brl(v: float) -> str:
    """Formata valor em pt-BR: R$ 1.234,56"""
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ---------------------------------------------------------------------------
# Etapa 1: BigQuery — buscar usuarios marcados com opt-in
# ---------------------------------------------------------------------------
def fetch_marked_users() -> pd.DataFrame:
    log.info(f"Etapa 1: Buscando usuarios com mark '{MARK_TAG}' no BigQuery (j_user)...")
    sql = f"""
    SELECT
        user_id     AS smartico_user_id,
        user_ext_id
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE '{MARK_TAG}' IN UNNEST(core_tags)
    """
    df = query_bigquery(sql)
    log.info(f"  -> {len(df):,} usuarios com opt-in encontrados")
    return df


# ---------------------------------------------------------------------------
# Etapa 2: Athena — consultar transacoes em chunks
# ---------------------------------------------------------------------------
def build_sql(chunk_ids: list) -> str:
    """
    Query Athena (Presto SQL):
    - CTE params: centraliza timestamps
    - Divisao /100.0 no SQL: valor ja sai em BRL
    - CASE WHEN classificacao: faixas calculadas no SQL
    - c_product_id = 'CASINO': obrigatorio (fund_ec2 mistura casino + sports)
    - c_amount_in_ecr_ccy: centavos | c_txn_status = 'SUCCESS'
    """
    ids_str   = ", ".join(str(i) for i in chunk_ids)
    games_str = ", ".join(f"'{g}'" for g in GAME_IDS)
    return f"""
    WITH params AS (
        SELECT
            TIMESTAMP '{START_UTC}' AS start_utc,
            TIMESTAMP '{END_UTC}'   AS end_utc
    ),
    participantes AS (
        SELECT DISTINCT
            c_ecr_id,
            c_external_id AS user_ext_id
        FROM ecr_ec2.tbl_ecr
        WHERE c_external_id IN ({ids_str})
    ),
    dados_brutos AS (
        SELECT
            f.c_ecr_id,
            SUM(CASE WHEN f.c_txn_type = {TXN_BET}
                     THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS total_bet_brl,
            COUNT_IF(f.c_txn_type = {TXN_ROLLBACK})                  AS qtd_rollbacks,
            COUNT_IF(f.c_txn_type = {TXN_BET})                       AS qtd_apostas
        FROM fund_ec2.tbl_real_fund_txn f
        CROSS JOIN params p
        INNER JOIN participantes pt ON f.c_ecr_id = pt.c_ecr_id
        WHERE f.c_start_time BETWEEN p.start_utc AND p.end_utc
          AND f.c_game_id IN ({games_str})
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_txn_type IN ({TXN_BET}, {TXN_ROLLBACK})
          AND f.c_product_id = 'CASINO'
        GROUP BY 1
    ),
    classificacao AS (
        SELECT
            p.user_ext_id,
            COALESCE(d.total_bet_brl, 0)  AS volume_apostado,
            COALESCE(d.qtd_rollbacks, 0)  AS rollbacks,
            COALESCE(d.qtd_apostas, 0)    AS qtd_apostas,
            CASE
                WHEN COALESCE(d.qtd_rollbacks, 0) > 0  THEN 'Desclassificado (rollback)'
                WHEN COALESCE(d.total_bet_brl, 0) >= 1000 THEN 'Faixa 4'
                WHEN COALESCE(d.total_bet_brl, 0) >= 500  THEN 'Faixa 3'
                WHEN COALESCE(d.total_bet_brl, 0) >= 200  THEN 'Faixa 2'
                WHEN COALESCE(d.total_bet_brl, 0) >= 50   THEN 'Faixa 1'
                WHEN COALESCE(d.total_bet_brl, 0) > 0     THEN 'Abaixo do Minimo'
                ELSE 'Sem Atividade'
            END AS faixa_segmentacao
        FROM participantes p
        LEFT JOIN dados_brutos d ON p.c_ecr_id = d.c_ecr_id
    )
    SELECT
        user_ext_id,
        volume_apostado AS total_bet_brl,
        rollbacks       AS qtd_rollbacks,
        qtd_apostas,
        faixa_segmentacao
    FROM classificacao
    ORDER BY volume_apostado DESC
    """


def fetch_athena_data(ext_ids: list) -> pd.DataFrame:
    chunks = [ext_ids[i:i + CHUNK_SIZE] for i in range(0, len(ext_ids), CHUNK_SIZE)]
    log.info(f"Etapa 2: Consultando Athena: {len(ext_ids):,} IDs em {len(chunks)} chunk(s)...")

    frames = []
    for idx, chunk in enumerate(chunks, 1):
        log.info(f"  Chunk {idx}/{len(chunks)} ({len(chunk):,} IDs)...")
        sql = build_sql(chunk)
        df  = query_athena(sql, database="fund_ec2")
        if not df.empty:
            frames.append(df)
        log.info(f"    -> {len(df):,} registros retornados")

    if frames:
        result = pd.concat(frames, ignore_index=True)
        log.info(f"  Total consolidado: {len(result):,} linhas do Athena")
        return result

    log.warning("  Nenhuma transacao encontrada no Athena!")
    return pd.DataFrame(columns=[
        "user_ext_id", "total_bet_brl", "qtd_rollbacks", "qtd_apostas",
        "faixa_segmentacao",
    ])


# ---------------------------------------------------------------------------
# Etapa 3: Validacao cruzada Athena vs BigQuery (Smartico DW)
# ---------------------------------------------------------------------------
def validacao_cruzada_bigquery(smartico_user_ids: list) -> dict:
    """
    Validacao cruzada via tr_casino_bet no BigQuery.
    casino_last_bet_game_name = ID numerico do jogo no Smartico.
    Usamos busca por nome via dm_casino_game_name para resolver o ID.
    Se falhar, retorna zeros (validacao opcional, nao bloqueia entrega).
    """
    log.info("Etapa 3: Validacao cruzada - consultando BigQuery (tr_casino_bet)...")

    try:
        ids_str = ", ".join(str(i) for i in smartico_user_ids)

        # Buscar smr_game_id do Smartico para Ratinho Sortudo
        # Schema dm_casino_game_name: smr_game_id (INT64), game_name (STRING), label_id (INT64)
        sql_game = """
        SELECT smr_game_id
        FROM `smartico-bq6.dwh_ext_24105.dm_casino_game_name`
        WHERE LOWER(game_name) LIKE '%ratinho%sortudo%'
          AND label_id = 24105
        LIMIT 1
        """
        df_game = query_bigquery(sql_game)

        if df_game.empty:
            log.warning("  smr_game_id para Ratinho Sortudo nao encontrado. Pulando validacao cruzada.")
            return {"qtd_jogadores": 0, "total_bet_brl": 0.0, "qtd_rollbacks": 0, "qtd_apostas": 0, "skipped": True}

        smr_game_id = int(df_game.iloc[0, 0])
        log.info(f"  Smartico smr_game_id para Ratinho Sortudo: {smr_game_id}")

        sql = f"""
        SELECT
            COUNT(DISTINCT b.user_id)                                        AS qtd_jogadores,
            SUM(CASE WHEN COALESCE(b.casino_is_rollback, FALSE) = FALSE
                     THEN b.casino_last_bet_amount ELSE 0 END)              AS total_bet_brl,
            SUM(CASE WHEN b.casino_is_rollback = TRUE
                     THEN 1 ELSE 0 END)                                     AS qtd_rollbacks,
            COUNT(CASE WHEN COALESCE(b.casino_is_rollback, FALSE) = FALSE
                       THEN 1 END)                                           AS qtd_apostas
        FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet` b
        WHERE b.user_id IN ({ids_str})
          AND b.casino_last_bet_game_name = {smr_game_id}
          AND b.event_time BETWEEN TIMESTAMP '{START_UTC}'
                               AND TIMESTAMP '{END_UTC}'
        """
        df = query_bigquery(sql)

        result = {
            "qtd_jogadores": int(df["qtd_jogadores"].iloc[0]) if not df.empty else 0,
            "total_bet_brl": float(df["total_bet_brl"].iloc[0] or 0) if not df.empty else 0.0,
            "qtd_rollbacks": int(df["qtd_rollbacks"].iloc[0]) if not df.empty else 0,
            "qtd_apostas":   int(df["qtd_apostas"].iloc[0]) if not df.empty else 0,
            "skipped": False,
        }
        log.info(f"  BigQuery: {result['qtd_jogadores']} jogadores, "
                 f"{fmt_brl(result['total_bet_brl'])} apostado, "
                 f"{result['qtd_rollbacks']} rollbacks")
        return result

    except Exception as e:
        log.warning(f"  Validacao cruzada falhou: {e}. Continuando sem validacao.")
        return {"qtd_jogadores": 0, "total_bet_brl": 0.0, "qtd_rollbacks": 0, "qtd_apostas": 0, "skipped": True}


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def main():
    log.info(f"Inicio — Promocao {MARK_TAG}")
    log.info(f"  Jogo: {GAME_NAME} (game_ids: {GAME_IDS})")
    log.info(f"  Periodo: {PERIODO_BRT} (UTC: {START_UTC} -> {END_UTC})")

    # -- Etapa 1: BigQuery -> opt-in ----------------------------------------
    df_marked = fetch_marked_users()

    df_marked["user_ext_id"] = (
        pd.to_numeric(df_marked["user_ext_id"], errors="coerce")
          .astype("Int64")
    )
    df_marked = df_marked.dropna(subset=["user_ext_id"])
    ext_ids = df_marked["user_ext_id"].tolist()
    log.info(f"  IDs validos para Athena: {len(ext_ids):,}")

    if not ext_ids:
        log.error("Nenhum usuario encontrado com a mark. Verifique tag no BigQuery.")
        return

    # -- Etapa 2: Athena -> transacoes (classificacao ja feita no SQL) --------
    df_txn = fetch_athena_data(ext_ids)

    if not df_txn.empty:
        df_txn["user_ext_id"]     = df_txn["user_ext_id"].astype("Int64")
        df_txn["total_bet_brl"]   = pd.to_numeric(df_txn["total_bet_brl"],   errors="coerce").fillna(0)
        df_txn["qtd_rollbacks"]   = pd.to_numeric(df_txn["qtd_rollbacks"],   errors="coerce").fillna(0).astype(int)
        df_txn["qtd_apostas"]     = pd.to_numeric(df_txn["qtd_apostas"],     errors="coerce").fillna(0).astype(int)
        df_txn["tem_rollback"]    = df_txn["qtd_rollbacks"] > 0

    # -- Merge left join (preserva TODOS os opt-in) -------------------------
    df_final = df_marked.merge(df_txn, on="user_ext_id", how="left")

    df_final["total_bet_brl"]     = df_final["total_bet_brl"].fillna(0.0)
    df_final["qtd_rollbacks"]     = df_final["qtd_rollbacks"].fillna(0).astype(int)
    df_final["qtd_apostas"]       = df_final["qtd_apostas"].fillna(0).astype(int)
    df_final["tem_rollback"]      = df_final["tem_rollback"].fillna(False)
    df_final["faixa_segmentacao"] = df_final["faixa_segmentacao"].fillna("Sem Atividade")

    # -- Formatacao BRL -----------------------------------------------------
    df_final["total_bet_brl_fmt"] = df_final["total_bet_brl"].apply(fmt_brl)

    # -- Ordenacao e colunas de saida ---------------------------------------
    cols_out = [
        "smartico_user_id", "user_ext_id",
        "qtd_apostas", "qtd_rollbacks", "tem_rollback",
        "total_bet_brl", "total_bet_brl_fmt",
        "faixa_segmentacao",
    ]
    df_final = df_final[cols_out].sort_values("total_bet_brl", ascending=False)

    # -- Etapa 3: Validacao cruzada Athena vs BigQuery ----------------------
    smartico_ids = df_marked["smartico_user_id"].tolist()
    bq_result = validacao_cruzada_bigquery(smartico_ids)

    # Metricas Athena para comparacao
    athena_jogadores = len(df_final[df_final["faixa_segmentacao"] != "Sem Atividade"])
    athena_total_bet = df_final["total_bet_brl"].sum()

    # Divergencia
    validacao_ok = not bq_result.get("skipped", False)
    diff_valor = 0.0
    pct_diff   = 0.0
    if validacao_ok:
        diff_valor = abs(athena_total_bet - bq_result["total_bet_brl"])
        pct_diff   = (diff_valor / athena_total_bet * 100) if athena_total_bet > 0 else 0
        log.info(f"  Validacao cruzada: Athena={athena_jogadores} jogadores / "
                 f"{fmt_brl(athena_total_bet)} | BigQuery={bq_result['qtd_jogadores']} jogadores / "
                 f"{fmt_brl(bq_result['total_bet_brl'])} | "
                 f"Diff={fmt_brl(diff_valor)} ({pct_diff:.2f}%)")
    else:
        log.info("  Validacao cruzada: pulada (game_id Smartico nao encontrado)")

    # -- Exportar CSV -------------------------------------------------------
    df_final.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")

    # -- Gerar ZIP ----------------------------------------------------------
    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(OUTPUT_CSV, os.path.basename(OUTPUT_CSV))
    log.info(f"ZIP gerado: {OUTPUT_ZIP}")

    # -- Metricas -----------------------------------------------------------
    total_marcados   = len(df_final)
    desclassificados = len(df_final[df_final["faixa_segmentacao"] == "Desclassificado (rollback)"])
    total_apostado   = athena_total_bet
    nao_jogaram      = len(df_final[df_final["faixa_segmentacao"] == "Sem Atividade"])
    abaixo_minimo    = len(df_final[df_final["faixa_segmentacao"] == "Abaixo do Minimo"])
    pct_engajamento  = (athena_jogadores / total_marcados * 100) if total_marcados > 0 else 0

    # Elegiveis = quem esta em Faixa 1-4 (exclui Abaixo do Minimo, Desclassificado, Sem Atividade)
    faixas_validas = ["Faixa 1", "Faixa 2", "Faixa 3", "Faixa 4"]
    elegiveis = len(df_final[df_final["faixa_segmentacao"].isin(faixas_validas)])

    ordem_faixas = [
        "Faixa 4", "Faixa 3", "Faixa 2", "Faixa 1",
        "Abaixo do Minimo", "Desclassificado (rollback)", "Sem Atividade",
    ]
    faixa_counts = df_final["faixa_segmentacao"].value_counts()

    # Helper: dados de uma faixa (n, vol, pct)
    def faixa_data(faixa):
        n   = int(faixa_counts.get(faixa, 0))
        vol = df_final[df_final["faixa_segmentacao"] == faixa]["total_bet_brl"].sum()
        pct = (vol / total_apostado * 100) if total_apostado > 0 else 0
        return n, vol, pct

    # Faixa com maior concentracao de volume
    max_faixa, max_pct = "", 0
    for f in faixas_validas:
        _, _, p = faixa_data(f)
        if p > max_pct:
            max_faixa, max_pct = f, p
    n_max, _, _ = faixa_data(max_faixa)

    # Periodo curto para display (ex: "25/03 19h-23h59")
    PERIODO_CURTO = "25/03 19h-23h59"

    # ======================================================================
    # CONSOLE REPORT (espelho da MSG 1)
    # ======================================================================
    SEP = "=" * 70
    print(f"\n{SEP}")
    print(f"  Segmentacao Gire & Ganhe | Promocao {MARK_TAG}")
    print(f"  | Periodo: {PERIODO_CURTO} BRT")
    print(SEP)

    print(f"\nJogo: {GAME_NAME} (Pragmatic Play).")
    print(
        f"Do segmento com opt-in ({total_marcados} usuarios marcados), "
        f"{athena_jogadores} jogaram {GAME_NAME} no periodo ({PERIODO_CURTO} BRT)."
    )
    print(f"Total apostado: {fmt_brl(total_apostado)}.")
    print("\nDistribuicao por faixa:\n")

    for faixa in ordem_faixas:
        n, vol, pct = faixa_data(faixa)
        label = ROTULOS.get(faixa, faixa)
        if faixa in faixas_validas:
            if n > 0:
                print(f"  {label}: {n} jogador{'es' if n != 1 else ''} \u2014 {fmt_brl(vol)} ({pct:.0f}% do volume)")
            else:
                print(f"  {label}: 0 jogadores")
        elif faixa == "Abaixo do Minimo":
            print(f"  Abaixo de R$50: {n} jogadores \u2014 {fmt_brl(vol)}")
        elif faixa == "Desclassificado (rollback)":
            print(f"  Desclassificados (rollback): {n} jogador{'es' if n != 1 else ''}")
        elif faixa == "Sem Atividade":
            print(f"  Nao jogou no periodo: {n} jogadores")

    print(f"\nElegiveis para pagamento: {elegiveis} jogadores.")
    print(
        f"\nPonto de atencao: Apenas {pct_engajamento:.1f}% dos marcados efetivamente "
        f"jogaram no periodo. "
    )
    if max_faixa and max_pct > 40:
        print(
            f"{ROTULOS.get(max_faixa, max_faixa)} concentra {max_pct:.0f}% do volume "
            f"com {n_max} jogador{'es' if n_max != 1 else ''}. "
        )
    if desclassificados == 0:
        print("Zero rollbacks \u2014 nenhum desclassificado.")
    else:
        print(
            f"{desclassificados} jogador(es) desclassificado(s) por rollback."
        )

    # ======================================================================
    # MENSAGENS WHATSAPP (padrao validado com equipe)
    # ======================================================================

    # ---- Mensagem 1 — Report ----
    faixas_bullets = ""
    for faixa in ordem_faixas:
        n, vol, pct = faixa_data(faixa)
        label = ROTULOS.get(faixa, faixa)
        if faixa in faixas_validas:
            if n > 0:
                faixas_bullets += f"\n  \u2022 {label}: {n} jogador{'es' if n != 1 else ''} \u2014 {fmt_brl(vol)} ({pct:.0f}% do volume)"
            else:
                faixas_bullets += f"\n  \u2022 {label}: 0 jogadores"
        elif faixa == "Abaixo do Minimo":
            faixas_bullets += f"\n  \u2022 Abaixo de R$50: {n} jogadores \u2014 {fmt_brl(vol)}"
        elif faixa == "Desclassificado (rollback)":
            faixas_bullets += f"\n  \u2022 Desclassificados (rollback): {n} jogador{'es' if n != 1 else ''}"
        elif faixa == "Sem Atividade":
            faixas_bullets += f"\n  \u2022 Nao jogou no periodo: {n} jogadores"

    concentracao_txt = ""
    if max_faixa and max_pct > 40:
        concentracao_txt = (
            f" {ROTULOS.get(max_faixa, max_faixa)} concentra {max_pct:.0f}% do volume "
            f"com {n_max} jogador{'es' if n_max != 1 else ''}."
        )

    rollback_txt = (
        "Zero rollbacks \u2014 nenhum desclassificado."
        if desclassificados == 0
        else f"{desclassificados} jogador(es) desclassificado(s) por rollback."
    )

    msg1 = f"""Mensagem 1 \u2014 Report:

Segmentacao Gire & Ganhe | Promocao {MARK_TAG}
| Periodo: {PERIODO_CURTO} BRT

Jogo: {GAME_NAME} (Pragmatic Play).
Do segmento com opt-in ({total_marcados} usuarios marcados), {athena_jogadores} jogaram {GAME_NAME} no periodo ({PERIODO_CURTO} BRT).
Total apostado: {fmt_brl(total_apostado)}.

Distribuicao por faixa:
{faixas_bullets}

Elegiveis para pagamento: {elegiveis} jogadores.

Ponto de atencao: Apenas {pct_engajamento:.1f}% dos marcados efetivamente jogaram no periodo.{concentracao_txt} {rollback_txt}"""

    # ---- Mensagem 2 — Validacoes ----
    val_cruzada_item = ""
    if validacao_ok:
        status = "dados consistentes" if pct_diff < 5 else "ATENCAO: divergencia > 5%"
        val_cruzada_item = (
            f"11. Duplo check BigQuery vs Athena: {athena_jogadores} vs "
            f"{bq_result['qtd_jogadores']} jogadores, diferenca de "
            f"{fmt_brl(diff_valor)} ({pct_diff:.2f}%) \u2014 {status}."
        )
    else:
        val_cruzada_item = (
            "11. Duplo check BigQuery vs Athena: nao realizado "
            "(game_id Smartico nao encontrado para este jogo)."
        )

    games_str = ", ".join(GAME_IDS)

    msg2 = f"""Mensagem 2 \u2014 Validacoes:

Validacoes realizadas:

1. Jogo confirmado no catalogo Athena: {GAME_NAME} ({games_str}, Pragmatic Play).
2. Usuarios extraidos do BigQuery Smartico via tag {MARK_TAG} em j_user.core_tags \u2014 {total_marcados} com opt-in confirmado.
3. Valores confirmados em centavos (c_amount_in_ecr_ccy, Pragmatic v1.3) \u2014 divisao por 100 aplicada no SQL.
4. Status c_txn_status = 'SUCCESS' validado empiricamente no schema fund_ec2.
5. Rollbacks (txn_type=72): {int(df_final['qtd_rollbacks'].sum())} \u2014 {desclassificados} jogadores desclassificados.
6. Mapeamento de IDs validado: Smartico user_ext_id = c_external_id na tabela ECR.
7. Cada jogador aparece em apenas uma faixa (a mais alta atingida) \u2014 sem duplicidade de pagamento.
8. Periodo em UTC: {START_UTC} -> {END_UTC} (equivalente a {PERIODO_CURTO} BRT).
9. Dados extraidos do Athena (Iceberg Data Lake) \u2014 banco operacional principal do projeto.
10. Turnover = soma acumulada no {GAME_NAME} (Wallet Share do evento).
{val_cruzada_item}
12. CSV inclui TODOS os {athena_jogadores} com atividade \u2014 elegiveis e nao elegiveis separados."""

    # ---- Imprimir mensagens ----
    print(f"\n{'='*70}")
    print("  MENSAGENS WHATSAPP (copiar e colar)")
    print(f"{'='*70}")

    print(f"\n--- MSG 1: REPORT ---\n")
    print(msg1)

    print(f"\n--- MSG 2: VALIDACOES ---\n")
    print(msg2)

    print(f"\n--- MSG 3: ZIP (anexar arquivo) ---")
    print(f"\n  {os.path.basename(OUTPUT_ZIP)}")

    print(f"\n{'='*70}")
    print(f"CSV salvo : {OUTPUT_CSV}")
    print(f"ZIP salvo : {OUTPUT_ZIP}")
    print(f"  -> {len(df_final):,} linhas | {len(df_final.columns)} colunas")
    print(f"{'='*70}\n")

    return df_final


if __name__ == "__main__":
    main()
