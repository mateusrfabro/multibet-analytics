"""
Discovery de schemas para o agente de riscos.
Mapeia tabelas e colunas de: bonus_ec2, csm_ec2, risk_ec2, gaming sessions.

Uso:
    python scripts/risk_discovery_schemas.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.athena import query_athena
import pandas as pd

OUTPUT_DIR = "temp/risk_discovery"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def discover_tables(database: str) -> pd.DataFrame:
    """Lista todas as tabelas de um database."""
    print(f"\n{'='*60}")
    print(f"  TABLES IN {database}")
    print(f"{'='*60}")
    df = query_athena(f"SHOW TABLES IN {database}", database=database)
    print(df.to_string(index=False))
    return df


def discover_columns(database: str, table: str) -> pd.DataFrame:
    """Lista todas as colunas de uma tabela."""
    print(f"\n--- COLUMNS: {database}.{table} ---")
    try:
        df = query_athena(
            f"SHOW COLUMNS FROM {table}",
            database=database
        )
        print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"  ERRO: {e}")
        return pd.DataFrame()


def sample_data(database: str, table: str, limit: int = 5) -> pd.DataFrame:
    """Amostra de dados de uma tabela."""
    print(f"\n--- SAMPLE: {database}.{table} (LIMIT {limit}) ---")
    try:
        df = query_athena(
            f"SELECT * FROM {table} LIMIT {limit}",
            database=database
        )
        print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"  ERRO: {e}")
        return pd.DataFrame()


def main():
    results = {}

    # ========================================================
    # 1. BONUS_EC2 — ciclo de vida de bonus
    # ========================================================
    print("\n" + "=" * 70)
    print("  PHASE 1: BONUS_EC2")
    print("=" * 70)

    bonus_tables = discover_tables("bonus_ec2")
    results["bonus_ec2_tables"] = bonus_tables

    # Descobrir colunas das tabelas principais de bonus
    if not bonus_tables.empty:
        col_name = bonus_tables.columns[0]
        for tbl in bonus_tables[col_name].tolist():
            cols = discover_columns("bonus_ec2", tbl)
            results[f"bonus_ec2.{tbl}_cols"] = cols

    # ========================================================
    # 2. CSM_EC2 — alertas de fraude e risco
    # ========================================================
    print("\n" + "=" * 70)
    print("  PHASE 2: CSM_EC2")
    print("=" * 70)

    csm_tables = discover_tables("csm_ec2")
    results["csm_ec2_tables"] = csm_tables

    if not csm_tables.empty:
        col_name = csm_tables.columns[0]
        for tbl in csm_tables[col_name].tolist():
            cols = discover_columns("csm_ec2", tbl)
            results[f"csm_ec2.{tbl}_cols"] = cols

    # ========================================================
    # 3. RISK_EC2 — score de risco
    # ========================================================
    print("\n" + "=" * 70)
    print("  PHASE 3: RISK_EC2")
    print("=" * 70)

    risk_tables = discover_tables("risk_ec2")
    results["risk_ec2_tables"] = risk_tables

    if not risk_tables.empty:
        col_name = risk_tables.columns[0]
        for tbl in risk_tables[col_name].tolist():
            cols = discover_columns("risk_ec2", tbl)
            results[f"risk_ec2.{tbl}_cols"] = cols
            # Sample dos dados de risco
            sample = sample_data("risk_ec2", tbl, limit=5)
            results[f"risk_ec2.{tbl}_sample"] = sample

    # ========================================================
    # 4. GAMING SESSIONS — sessoes individuais
    # ========================================================
    print("\n" + "=" * 70)
    print("  PHASE 4: GAMING SESSIONS (bireports_ec2)")
    print("=" * 70)

    sessions_cols = discover_columns("bireports_ec2", "tbl_ecr_gaming_sessions")
    results["gaming_sessions_cols"] = sessions_cols

    # Sample de sessoes (com filtro de data pra nao escanear tudo)
    print("\n--- SAMPLE: gaming sessions (1 dia recente) ---")
    try:
        sessions_sample = query_athena("""
            SELECT *
            FROM tbl_ecr_gaming_sessions
            WHERE c_start_time >= TIMESTAMP '2026-04-02'
              AND c_start_time < TIMESTAMP '2026-04-03'
            LIMIT 5
        """, database="bireports_ec2")
        print(sessions_sample.to_string(index=False))
        results["gaming_sessions_sample"] = sessions_sample
    except Exception as e:
        print(f"  ERRO: {e}")

    # ========================================================
    # 5. FUND_EC2 — tipos de transacao relevantes pra fraude
    # ========================================================
    print("\n" + "=" * 70)
    print("  PHASE 5: FUND_EC2 — c_txn_type distribution")
    print("=" * 70)

    print("\n--- Distribuicao de c_txn_type (1 dia) ---")
    try:
        txn_types = query_athena("""
            SELECT
                c_txn_type,
                COUNT(*) AS qty,
                SUM(c_amount_in_ecr_ccy) / 100.0 AS total_brl
            FROM tbl_real_fund_txn
            WHERE c_start_time >= TIMESTAMP '2026-04-02'
              AND c_start_time < TIMESTAMP '2026-04-03'
              AND c_txn_status = 'SUCCESS'
            GROUP BY c_txn_type
            ORDER BY qty DESC
        """, database="fund_ec2")
        print(txn_types.to_string(index=False))
        results["fund_txn_types"] = txn_types
    except Exception as e:
        print(f"  ERRO: {e}")

    # ========================================================
    # SALVAR RESULTADOS
    # ========================================================
    print("\n" + "=" * 70)
    print("  SALVANDO RESULTADOS")
    print("=" * 70)

    for key, df in results.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            path = os.path.join(OUTPUT_DIR, f"{key}.csv")
            df.to_csv(path, index=False)
            print(f"  Salvo: {path} ({len(df)} rows)")

    print("\nDiscovery concluido!")


if __name__ == "__main__":
    main()
