"""
Teste local das queries sportsbook R9/R10/R11 antes de deploy na EC2.
Usa db/athena.py do MultiBet (conexao validada).
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "alerta-ftd"))

from db.athena import query_athena
from sportsbook_alerts import (
    fetch_live_delay,
    fetch_cancel_refund,
    fetch_new_account_high_dep,
    build_sportsbook_section,
)

DAYS = 2


def _query_fn(sql: str):
    """Wrapper que usa db/athena.py do MultiBet."""
    return query_athena(sql)


def test_r10():
    print("=" * 60)
    print("R10 — Cancelamentos/Refund (com jogo/evento)")
    print("=" * 60)
    df = fetch_cancel_refund(_query_fn, days=DAYS)
    if df.empty:
        print("Nenhum resultado (OK — sem refunds no periodo)")
    else:
        print(f"Linhas: {len(df)}")
        print(f"Colunas: {list(df.columns)}")
        print(df.to_string(index=False))
    print()
    return df


def test_r11():
    print("=" * 60)
    print("R11 — Conta Nova + Deposito Alto + SB")
    print("=" * 60)
    df = fetch_new_account_high_dep(_query_fn, days=DAYS)
    if df.empty:
        print("Nenhum resultado (OK — sem contas novas com dep alto + SB)")
    else:
        print(f"Linhas: {len(df)}")
        print(f"Colunas: {list(df.columns)}")
        print(df.to_string(index=False))
    print()
    return df


def test_full_report():
    print("=" * 60)
    print("REPORT COMPLETO (como vai aparecer no Slack)")
    print("=" * 60)
    section = build_sportsbook_section(_query_fn, days=DAYS)
    if section:
        print(section)
        print(f"\n[Tamanho: {len(section)} chars / limite Slack: 3000]")
    else:
        print("Nenhum alerta de sportsbook no periodo.")
    print()


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    print(f"\nTestando sportsbook alerts (ultimos {DAYS} dias)...\n")

    try:
        test_r10()
    except Exception as e:
        print(f"ERRO R10: {e}\n")

    try:
        test_r11()
    except Exception as e:
        print(f"ERRO R11: {e}\n")

    try:
        test_full_report()
    except Exception as e:
        print(f"ERRO REPORT: {e}\n")
