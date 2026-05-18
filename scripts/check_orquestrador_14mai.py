"""
Verifica se PCR, Matriz de Risco e Segmentacao A+S rodaram pelo orquestrador
na madrugada de 14/05/2026.

Consulta MAX(snapshot_date) e contagem de linhas das 3 tabelas no Super Nova DB.
"""
import sys
import os
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection  # noqa: E402

HOJE = date(2026, 5, 14)

CHECKS = [
    ("PCR",          "multibet.pcr_atual",              "snapshot_date"),
    ("MATRIZ_RISCO", "multibet.matriz_risco",           "snapshot_date"),
    ("SEGMENTACAO",  "multibet.segmentacao_sa_diaria",  "snapshot_date"),
]


def main() -> int:
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            print(f"\n{'='*70}")
            print(f"VERIFICACAO ORQUESTRADOR — alvo: {HOJE.isoformat()}")
            print(f"{'='*70}\n")

            faltam = []
            for label, tabela, col in CHECKS:
                # Existencia
                cur.execute(
                    "SELECT to_regclass(%s) IS NOT NULL", (tabela,)
                )
                existe = cur.fetchone()[0]
                if not existe:
                    print(f"[{label:13s}] TABELA NAO EXISTE: {tabela}")
                    faltam.append(label)
                    continue

                # MAX snapshot + count do dia
                cur.execute(
                    f"""
                    SELECT
                        MAX({col})                              AS ultimo,
                        COUNT(*) FILTER (WHERE {col} = %s)      AS rows_hoje,
                        COUNT(*) FILTER (WHERE {col} = %s - INTERVAL '1 day') AS rows_ontem
                    FROM {tabela}
                    """,
                    (HOJE, HOJE),
                )
                ultimo, rows_hoje, rows_ontem = cur.fetchone()
                rodou_hoje = (ultimo == HOJE)
                tag = "OK" if rodou_hoje else "NAO RODOU HOJE"
                print(
                    f"[{label:13s}] ultimo={ultimo}  rows_hoje={rows_hoje:>8}  "
                    f"rows_ontem={rows_ontem:>8}  -> {tag}"
                )
                if not rodou_hoje:
                    faltam.append(label)

            print(f"\n{'='*70}")
            if not faltam:
                print("RESULTADO: 3/3 rodaram hoje pelo orquestrador.")
                return 0
            print(f"RESULTADO: NAO RODOU HOJE -> {', '.join(faltam)}")
            return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            tunnel.stop()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
