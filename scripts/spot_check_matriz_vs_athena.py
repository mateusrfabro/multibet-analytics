"""
Spot-check 14/04/2026: matriz_financeiro (Super Nova DB) vs cashier_ec2 (Athena).
Validacao cruzada pedida pelo auditor antes de mandar entrega ao CTO.

Fontes:
- Super Nova DB: multibet.matriz_financeiro (view gold, campo deposit)
- Athena: cashier_ec2.tbl_cashier_deposit (c_confirmed_amount_in_ecr_ccy / 100)

Filtro: 1 dia UTC (14/04/2026) - matriz_financeiro.data e UTC truncado.
"""
import sys
sys.path.insert(0, ".")

from db.supernova import execute_supernova
from db.athena import query_athena


SQL_SNDB = """
SELECT data, deposit
FROM multibet.matriz_financeiro
WHERE data = DATE '2026-04-14';
"""

SQL_ATHENA = """
SELECT
    date(c_created_time) AS dt_utc,
    ROUND(SUM(c_confirmed_amount_in_ecr_ccy) / 100.0, 2) AS deposit_brl,
    COUNT(*) AS qty
FROM cashier_ec2.tbl_cashier_deposit
WHERE c_created_time >= TIMESTAMP '2026-04-14'
  AND c_created_time <  TIMESTAMP '2026-04-15'
  AND c_txn_status = 'txn_confirmed_success'
GROUP BY date(c_created_time);
"""


def main():
    print("=" * 70)
    print("SPOT-CHECK 14/04/2026 - matriz_financeiro vs cashier_ec2")
    print("=" * 70)

    # --- SNDB ---
    print("\n[1/2] Super Nova DB (multibet.matriz_financeiro)...")
    rows = execute_supernova(SQL_SNDB, fetch=True)
    if not rows:
        print("      SEM DADOS para 14/04 na matriz_financeiro!")
        return
    data_sndb, dep_sndb = rows[0]
    qty_sndb = None
    print(f"      data:    {data_sndb}")
    print(f"      deposit: R${float(dep_sndb):,.2f}")

    # --- Athena ---
    print("\n[2/2] Athena (cashier_ec2.tbl_cashier_deposit)...")
    df = query_athena(SQL_ATHENA, database="cashier_ec2")
    if df is None or df.empty:
        print("      SEM DADOS no Athena!")
        return
    row = df.iloc[0]
    dep_ath = float(row["deposit_brl"])
    qty_ath = int(row["qty"])
    print(f"      dt_utc:  {row['dt_utc']}")
    print(f"      deposit: R${dep_ath:,.2f}")
    print(f"      qty:     {qty_ath}")

    # --- Comparativo ---
    print("\n" + "=" * 70)
    print("COMPARATIVO")
    print("=" * 70)
    delta_abs = dep_ath - float(dep_sndb)
    delta_pct = (delta_abs / float(dep_sndb)) * 100 if dep_sndb else 0

    print(f"{'Fonte':<28} {'Deposit BRL':>18} {'Qty':>10}")
    print("-" * 58)
    print(f"{'Super Nova DB (matriz)':<28} {float(dep_sndb):>18,.2f} {'-':>10}")
    print(f"{'Athena (cashier_ec2)':<28} {dep_ath:>18,.2f} {qty_ath:>10,}")
    print("-" * 58)
    print(f"{'DELTA (Athena - SNDB)':<28} {delta_abs:>18,.2f}")
    print(f"{'DELTA %':<28} {delta_pct:>17,.2f}%")

    # --- Veredicto ---
    print("\n" + "=" * 70)
    if abs(delta_pct) < 2:
        print(f"VEREDICTO: MARGEM OK (<2%) - {delta_pct:.2f}%. Matriz validada.")
    elif abs(delta_pct) < 5:
        print(f"VEREDICTO: ALERTA (2-5%) - {delta_pct:.2f}%. Investigar diferenca.")
    else:
        print(f"VEREDICTO: BLOQUEIO (>5%) - {delta_pct:.2f}%. Divergencia material.")
    print("=" * 70)


if __name__ == "__main__":
    main()
