"""
Carrega o CSV da Matriz de Risco no Super Nova DB via COPY.
Uso: python scripts/load_risk_csv_to_pg.py
"""
import io
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db.supernova import get_supernova_connection

CSV_PATH = Path(__file__).resolve().parent.parent / "reports" / "risk_matrix_2026-04-06_FINAL.csv"
PG_SCHEMA = "multibet"
PG_TABLE = "risk_tags"


def main():
    df = pd.read_csv(CSV_PATH)
    print(f"CSV: {len(df)} linhas, {len(df.columns)} colunas")

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # Colunas existentes na tabela
            cur.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{PG_SCHEMA}' AND table_name = '{PG_TABLE}'"
            )
            table_cols = set(r[0] for r in cur.fetchall())
            print(f"Colunas na tabela: {len(table_cols)}")

            # Filtra colunas do CSV que existem na tabela
            insert_cols = [c for c in df.columns if c in table_cols]
            df_insert = df[insert_cols].copy()

            # Deduplica pela PK (label_id, user_id, snapshot_date)
            before = len(df_insert)
            df_insert = df_insert.drop_duplicates(subset=["label_id", "user_id", "snapshot_date"], keep="first")
            after = len(df_insert)
            if before != after:
                print(f"Deduplicado: {before} -> {after} ({before - after} duplicatas removidas)")
            print(f"Colunas para insert: {len(insert_cols)}")

            # Deleta apenas snapshot de hoje (preserva historico)
            cur.execute(
                f"DELETE FROM {PG_SCHEMA}.{PG_TABLE} WHERE snapshot_date = '2026-04-06'"
            )
            print("Truncado.")

            # COPY FROM STDIN com tab delimiter
            buffer = io.StringIO()
            df_insert.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
            buffer.seek(0)

            cols_str = ", ".join([f'"{c}"' for c in insert_cols])
            copy_sql = (
                f"COPY {PG_SCHEMA}.{PG_TABLE} ({cols_str}) "
                f"FROM STDIN WITH DELIMITER '\t' NULL '\\N'"
            )
            print(f"Executando COPY ({len(df)} linhas)...")
            cur.copy_expert(copy_sql, buffer)

        conn.commit()

        # Verifica
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {PG_SCHEMA}.{PG_TABLE}")
            count = cur.fetchone()[0]
        print(f"SUCESSO: {count} linhas em {PG_SCHEMA}.{PG_TABLE}")

    except Exception as e:
        conn.rollback()
        print(f"ERRO: {e}")
        raise
    finally:
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    main()
