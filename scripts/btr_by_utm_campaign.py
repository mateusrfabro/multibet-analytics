"""
BTR (Bonus Turned Real) por utm_campaign
=========================================
Autor: Mateus Fabro | Squad: Intelligence Engine | Data: 2026-04-09
Recorrencia: Manual (candidato a cron diario se produtizado)

Join: multibet.trackings (Super Nova DB) x fund_ec2 BTR (Athena)
Bridge: trackings.user_id -> dim_user.external_id -> dim_user.ecr_id -> fund.c_ecr_id
BTR source: tbl_realcash_sub_fund_txn (c_txn_type=20, c_op_type=CR)
  NOTA: tbl_real_fund_txn.c_amount_in_ecr_ccy e SEMPRE 0 para type 20.

Saidas:
  1. Tabela multibet.agg_btr_by_utm_campaign (Super Nova DB)
  2. JSON em reports/btr_by_utm_campaign_{data}.json

Uso:
  python scripts/btr_by_utm_campaign.py                    # ultimos 90 dias
  python scripts/btr_by_utm_campaign.py --days 180         # ultimos 180 dias
  python scripts/btr_by_utm_campaign.py --explore          # so explorar schema trackings
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime, timedelta

import pandas as pd

# -- path setup ----------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova import get_supernova_connection
from db.athena import query_athena

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# -- BTR: txn_type 20 = ISSUE_BONUS (wagering batido -> bonus vira real)
# Valor esta em tbl_realcash_sub_fund_txn (NAO na tbl_real_fund_txn que e sempre 0)
BTR_TXN_TYPE = 20


# ========================================================================
# STEP 1: Explorar schema da trackings
# ========================================================================
def explore_trackings_schema():
    """Mostra colunas, tipos e sample da multibet.trackings."""
    log.info("Explorando schema de multibet.trackings...")
    tunnel, conn = get_supernova_connection()
    try:
        # Colunas
        sql_cols = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'multibet'
              AND table_name   = 'trackings'
            ORDER BY ordinal_position
        """
        df_cols = pd.read_sql(sql_cols, conn)
        log.info(f"Colunas encontradas: {len(df_cols)}")
        print("\n-- Schema multibet.trackings --")
        print(df_cols.to_string(index=False))

        # Contagem + amostra
        df_count = pd.read_sql("SELECT COUNT(*) AS total FROM multibet.trackings", conn)
        print(f"\nTotal de linhas: {df_count['total'].iloc[0]:,}")

        df_sample = pd.read_sql("SELECT * FROM multibet.trackings LIMIT 5", conn)
        print("\n-- Amostra (5 linhas) --")
        print(df_sample.to_string(index=False))

        # Distinct utm_campaign
        sql_utm = """
            SELECT utm_campaign, COUNT(*) AS qty
            FROM multibet.trackings
            WHERE utm_campaign IS NOT NULL
              AND utm_campaign != ''
            GROUP BY utm_campaign
            ORDER BY qty DESC
            LIMIT 20
        """
        try:
            df_utm = pd.read_sql(sql_utm, conn)
            print("\n-- Top 20 utm_campaign --")
            print(df_utm.to_string(index=False))
        except Exception as e:
            log.warning(f"utm_campaign pode não existir como coluna: {e}")

        return df_cols
    finally:
        conn.close()
        tunnel.stop()


# ========================================================================
# STEP 2: Buscar trackings (Super Nova DB)
# ========================================================================
def fetch_trackings():
    """Busca user_id + campos UTM da multibet.trackings (colunas especificas)."""
    log.info("Buscando trackings do Super Nova DB...")
    tunnel, conn = get_supernova_connection()
    try:
        sql = """
            SELECT user_id, utm_campaign, utm_source, utm_content, created_at
            FROM multibet.trackings
        """
        df = pd.read_sql(sql, conn)
        # De-duplicar por user_id (relacao quase 1:1, mas ~33 duplicados existem)
        antes = len(df)
        df = df.drop_duplicates(subset="user_id", keep="first")
        if antes > len(df):
            log.info(f"De-duplicados: {antes - len(df)} trackings duplicados removidos")
        log.info(f"Trackings carregados: {len(df):,} usuarios unicos")
        return df
    finally:
        conn.close()
        tunnel.stop()


# ========================================================================
# STEP 3: Buscar BTR do Athena (fund_ec2)
# ========================================================================
def fetch_btr(start_date: str, end_date: str):
    """
    Busca transações BTR (Bonus Turned Real) do fund_ec2.
    NOTA: c_amount_in_ecr_ccy na tbl_real_fund_txn é SEMPRE 0 para type 20.
    O valor real do BTR está em tbl_realcash_sub_fund_txn (sub-fund de real cash).
    Filtro obrigatório por c_start_time (partição Iceberg).
    Valores em centavos -> converte pra BRL.
    """
    sql = f"""
        SELECT
            c_ecr_id,
            c_sub_txn_id   AS c_txn_id,
            c_txn_type,
            c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS btr_time_brt,
            c_amount_in_ecr_ccy / 100.0 AS btr_amount_brl,
            c_op_type
        FROM fund_ec2.tbl_realcash_sub_fund_txn
        WHERE c_txn_type = 20
          AND c_op_type = 'CR'
          AND c_amount_in_ecr_ccy > 0
          AND c_start_time >= TIMESTAMP '{start_date}'
          AND c_start_time <  TIMESTAMP '{end_date}'
    """
    log.info(f"Buscando BTR no Athena via tbl_realcash_sub_fund_txn ({start_date} a {end_date})...")
    df = query_athena(sql, database="fund_ec2")
    log.info(f"BTR carregados: {len(df):,} transacoes (com valor > 0)")
    return df


# ========================================================================
# STEP 4: Bridge external_id -> ecr_id (ps_bi.dim_user)
# ========================================================================
def fetch_dim_user_bridge():
    """
    Busca mapeamento external_id -> ecr_id do dim_user.
    trackings.user_id = CAST(external_id AS VARCHAR) -- feedback validado 08/04/2026.
    Exclui test users (is_test = false) -- feedback test_users_filtro_completo.
    """
    sql = """
        SELECT
            CAST(external_id AS VARCHAR) AS external_id,
            ecr_id
        FROM ps_bi.dim_user
        WHERE external_id IS NOT NULL
          AND is_test = false
    """
    log.info("Buscando bridge dim_user (external_id -> ecr_id, excluindo test users)...")
    df = query_athena(sql, database="ps_bi")
    log.info(f"Bridge carregada: {len(df):,} usuarios (sem test users)")
    return df


# ========================================================================
# STEP 5: Join + Clusterização por utm_campaign
# ========================================================================
def join_and_cluster(df_trackings, df_btr, df_bridge):
    """
    1. trackings.user_id -> bridge.external_id (match)
    2. bridge.ecr_id -> btr.c_ecr_id (match)
    3. Agrega por utm_campaign
    """
    log.info("Fazendo join trackings -> dim_user -> BTR...")

    # -- Normalizar user_id para string (trackings) --
    df_trackings["user_id"] = df_trackings["user_id"].astype(str).str.strip()
    df_bridge["external_id"] = df_bridge["external_id"].astype(str).str.strip()

    # -- Join 1: trackings + bridge --
    df_merged = df_trackings.merge(
        df_bridge,
        left_on="user_id",
        right_on="external_id",
        how="inner",
    )
    log.info(f"Match trackings->dim_user: {len(df_merged):,} de {len(df_trackings):,} "
             f"({len(df_merged)/max(len(df_trackings),1)*100:.1f}%)")

    if df_merged.empty:
        log.warning("Nenhum match trackings->dim_user. Verificar user_id vs external_id.")
        return pd.DataFrame(), pd.DataFrame()

    # -- Join 2: merged + BTR --
    df_btr["c_ecr_id"] = df_btr["c_ecr_id"].astype("int64")
    df_merged["ecr_id"] = df_merged["ecr_id"].astype("int64")

    df_full = df_merged.merge(
        df_btr,
        left_on="ecr_id",
        right_on="c_ecr_id",
        how="inner",
    )
    log.info(f"Match com BTR: {len(df_full):,} transações BTR vinculadas a trackings")

    if df_full.empty:
        log.warning("Nenhum match com BTR. Pode ser que esses usuários não tenham BTR no período.")
        return pd.DataFrame(), pd.DataFrame()

    # -- Identificar coluna utm_campaign --
    utm_col = None
    for candidate in ["utm_campaign", "campaign", "utm_campanha"]:
        if candidate in df_full.columns:
            utm_col = candidate
            break

    if utm_col is None:
        log.error(f"Coluna utm_campaign não encontrada. Colunas disponíveis: {list(df_full.columns)}")
        return pd.DataFrame(), df_full

    log.info(f"Usando coluna '{utm_col}' para agrupamento")

    # -- Limpar valores nulos/vazios --
    df_full[utm_col] = df_full[utm_col].fillna("(sem_utm_campaign)").astype(str)
    df_full[utm_col] = df_full[utm_col].replace("", "(sem_utm_campaign)")

    # -- Clusterização (agregação por utm_campaign) --
    agg = df_full.groupby(utm_col).agg(
        total_users=("ecr_id", "nunique"),
        total_btr_events=("c_txn_id", "count"),
        total_btr_brl=("btr_amount_brl", "sum"),
        avg_btr_per_user=("btr_amount_brl", "mean"),
        median_btr=("btr_amount_brl", "median"),
        min_btr=("btr_amount_brl", "min"),
        max_btr=("btr_amount_brl", "max"),
        first_btr=("btr_time_brt", "min"),
        last_btr=("btr_time_brt", "max"),
    ).reset_index()

    agg = agg.rename(columns={utm_col: "utm_campaign"})
    agg["avg_btr_per_user"] = agg["total_btr_brl"] / agg["total_users"]
    agg["total_btr_brl"] = agg["total_btr_brl"].round(2)
    agg["avg_btr_per_user"] = agg["avg_btr_per_user"].round(2)
    agg["median_btr"] = agg["median_btr"].round(2)
    agg["min_btr"] = agg["min_btr"].round(2)
    agg["max_btr"] = agg["max_btr"].round(2)
    agg = agg.sort_values("total_btr_brl", ascending=False).reset_index(drop=True)

    log.info(f"Clusters gerados: {len(agg)} campanhas únicas")

    return agg, df_full


# ========================================================================
# STEP 6: Persistir no Super Nova DB
# ========================================================================
def save_to_supernova(df_agg):
    """Cria/recria tabela multibet.agg_btr_by_utm_campaign no Super Nova DB."""
    if df_agg.empty:
        log.warning("DataFrame vazio — nada a persistir.")
        return

    log.info("Persistindo tabela multibet.agg_btr_by_utm_campaign...")
    tunnel, conn = get_supernova_connection()
    try:
        cur = conn.cursor()

        # DDL — TRUNCATE+INSERT (idempotente)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS multibet.agg_btr_by_utm_campaign (
                utm_campaign       VARCHAR(500),
                total_users        INTEGER,
                total_btr_events   INTEGER,
                total_btr_brl      NUMERIC(14,2),
                avg_btr_per_user   NUMERIC(14,2),
                median_btr         NUMERIC(14,2),
                min_btr            NUMERIC(14,2),
                max_btr            NUMERIC(14,2),
                first_btr          TIMESTAMP,
                last_btr           TIMESTAMP,
                updated_at         TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (utm_campaign)
            )
        """)
        cur.execute("TRUNCATE TABLE multibet.agg_btr_by_utm_campaign")

        # Insert
        insert_sql = """
            INSERT INTO multibet.agg_btr_by_utm_campaign
                (utm_campaign, total_users, total_btr_events, total_btr_brl,
                 avg_btr_per_user, median_btr, min_btr, max_btr, first_btr, last_btr)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = []
        for _, r in df_agg.iterrows():
            rows.append((
                str(r["utm_campaign"])[:500],
                int(r["total_users"]),
                int(r["total_btr_events"]),
                float(r["total_btr_brl"]),
                float(r["avg_btr_per_user"]),
                float(r["median_btr"]),
                float(r["min_btr"]),
                float(r["max_btr"]),
                r["first_btr"] if pd.notna(r["first_btr"]) else None,
                r["last_btr"] if pd.notna(r["last_btr"]) else None,
            ))

        cur.executemany(insert_sql, rows)
        conn.commit()
        log.info(f"Tabela atualizada: {len(rows)} campanhas inseridas")
    finally:
        conn.close()
        tunnel.stop()


# ========================================================================
# STEP 7: Exportar JSON + resumo
# ========================================================================
def export_outputs(df_agg, df_detail):
    """Exporta JSON (pro dev) + CSV (análise) + legenda."""
    ts = datetime.now().strftime("%Y%m%d")
    os.makedirs("reports", exist_ok=True)

    # -- JSON (para o dev) --
    json_path = f"reports/btr_by_utm_campaign_{ts}.json"
    result = {
        "metadata": {
            "descricao": "Clusterização de BTR (Bonus Turned Real) por utm_campaign",
            "fonte_trackings": "Super Nova DB — multibet.trackings",
            "fonte_btr": "Athena -- fund_ec2.tbl_realcash_sub_fund_txn (txn_type 20, op CR, valor > 0)",
            "bridge": "ps_bi.dim_user (external_id -> ecr_id)",
            "gerado_em": datetime.now().isoformat(),
            "total_campanhas": len(df_agg),
        },
        "colunas": {
            "utm_campaign": "Identificador da campanha (Keitaro/UTM)",
            "total_users": "Qtd de jogadores únicos com BTR nessa campanha",
            "total_btr_events": "Qtd total de transações BTR",
            "total_btr_brl": "Soma total BTR em R$ (BRL)",
            "avg_btr_per_user": "Média BTR por jogador (R$)",
            "median_btr": "Mediana do BTR por transação (R$)",
            "min_btr": "Menor BTR individual (R$)",
            "max_btr": "Maior BTR individual (R$)",
            "first_btr": "Data/hora do primeiro BTR (BRT)",
            "last_btr": "Data/hora do último BTR (BRT)",
        },
        "campanhas": json.loads(
            df_agg.to_json(orient="records", date_format="iso", force_ascii=False)
        ),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"JSON exportado: {json_path}")

    # -- CSV detalhado (opcional, para análise interna) --
    if not df_detail.empty:
        csv_path = f"reports/btr_trackings_detail_{ts}.csv"
        df_detail.to_csv(csv_path, index=False, encoding="utf-8-sig")
        log.info(f"CSV detalhado: {csv_path} ({len(df_detail):,} linhas)")

    # -- Legenda --
    legenda_path = f"reports/btr_by_utm_campaign_{ts}_legenda.txt"
    legenda = """
LEGENDA — BTR por utm_campaign
================================

FONTE DOS DADOS:
  - Trackings: Super Nova DB -> multibet.trackings (user_id, utm_campaign)
  - BTR: Athena -> fund_ec2.tbl_realcash_sub_fund_txn (c_txn_type=20, c_op_type=CR, valor>0)
    NOTA: tbl_real_fund_txn.c_amount e SEMPRE 0 para type 20 -- usar sub-fund
  - Bridge: Athena -> ps_bi.dim_user (external_id -> ecr_id, excluindo test users)

O QUE É BTR (Bonus Turned Real):
  Quando um jogador cumpre o wagering de um bônus, o valor restante
  é convertido de bônus para dinheiro real (real cash). É o "lucro" do
  jogador vindo de um bônus — e um custo para a casa.

COLUNAS:
  utm_campaign      Identificador da campanha (Keitaro/UTM original)
  total_users       Jogadores únicos que fizeram BTR nessa campanha
  total_btr_events  Quantidade de transações BTR
  total_btr_brl     Valor total convertido em R$ (custo para a casa)
  avg_btr_per_user  Média de BTR por jogador — indica "generosidade" da conversão
  median_btr        Mediana por transação — menos sensível a outliers
  min_btr / max_btr Faixa de valores individuais
  first_btr         Primeiro BTR dessa campanha (BRT)
  last_btr          Último BTR dessa campanha (BRT)

COMO INTERPRETAR:
  - Campanhas com alto total_btr_brl e poucos users = bônus "caros" por jogador
  - avg_btr_per_user alto pode indicar abuso de bônus ou perfil high-roller
  - Se median << avg, há outliers puxando a média (investigar max_btr)
  - Campanhas sem BTR não aparecem (jogadores não converteram bônus)

AÇÃO SUGERIDA:
  - Avaliar ROI: cruzar BTR com depósitos e GGR do mesmo grupo
  - Identificar campanhas com BTR alto e GGR baixo = custo sem retorno
  - Campanhas com BTR saudável (baixo) e GGR alto = boas para escalar
""".strip()

    with open(legenda_path, "w", encoding="utf-8") as f:
        f.write(legenda)
    log.info(f"Legenda: {legenda_path}")

    return json_path


# ========================================================================
# MAIN
# ========================================================================
def main():
    parser = argparse.ArgumentParser(description="BTR por utm_campaign")
    parser.add_argument("--days", type=int, default=90,
                        help="Período de BTR em dias (default: 90)")
    parser.add_argument("--explore", action="store_true",
                        help="Apenas explorar schema da trackings e sair")
    parser.add_argument("--no-persist", action="store_true",
                        help="Não salvar no Super Nova DB (só JSON)")
    args = parser.parse_args()

    # -- Modo exploração --
    if args.explore:
        explore_trackings_schema()
        return

    # -- Período --
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    log.info(f"Período BTR: {start_date} a {end_date} ({args.days} dias)")

    # -- Fetch dados --
    df_trackings = fetch_trackings()
    df_bridge = fetch_dim_user_bridge()
    df_btr = fetch_btr(start_date, end_date)

    # -- Validações --
    if df_trackings.empty:
        log.error("Tabela trackings vazia. Abortando.")
        return
    if df_btr.empty:
        log.error("Nenhuma transação BTR no período. Tente aumentar --days.")
        return

    # -- Join + cluster --
    df_agg, df_detail = join_and_cluster(df_trackings, df_btr, df_bridge)

    if df_agg.empty:
        log.error("Resultado vazio após join. Verificar compatibilidade dos IDs.")
        return

    # -- Resumo no terminal --
    print("\n" + "=" * 70)
    print("RESUMO — BTR por utm_campaign")
    print("=" * 70)
    print(f"Campanhas com BTR: {len(df_agg)}")
    print(f"Total BTR:         R$ {df_agg['total_btr_brl'].sum():,.2f}")
    print(f"Jogadores únicos:  {df_agg['total_users'].sum():,}")
    print(f"Transações BTR:    {df_agg['total_btr_events'].sum():,}")
    print()
    print("-- Top 15 campanhas (por volume BTR) --")
    top = df_agg.head(15)[["utm_campaign", "total_users", "total_btr_events",
                            "total_btr_brl", "avg_btr_per_user"]].copy()
    top["total_btr_brl"] = top["total_btr_brl"].apply(lambda v: f"R$ {v:,.2f}")
    top["avg_btr_per_user"] = top["avg_btr_per_user"].apply(lambda v: f"R$ {v:,.2f}")
    print(top.to_string(index=False))

    # -- Persistir --
    if not args.no_persist:
        save_to_supernova(df_agg)

    # -- Exportar --
    json_path = export_outputs(df_agg, df_detail)

    print(f"\nArquivos gerados:")
    print(f"  JSON:    {json_path}")
    print(f"  Tabela:  multibet.agg_btr_by_utm_campaign {'(salva)' if not args.no_persist else '(pulou)'}")
    log.info("Concluído!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Erro fatal: {e}", exc_info=True)
        sys.exit(1)
