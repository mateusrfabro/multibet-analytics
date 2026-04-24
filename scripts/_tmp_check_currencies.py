"""Investigar estrutura de moeda no supernova_bet schema public."""
import os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

tunnel, conn = get_supernova_bet_connection()
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor()
try:
    # 1. Tabelas relacionadas a moeda/currency/fx
    print("=" * 80)
    print("1. Tabelas com 'currency', 'fx', 'rate', 'exchange', 'conversion' no nome")
    print("=" * 80)
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ('public','platform')
          AND (
              table_name ILIKE '%currenc%' OR
              table_name ILIKE '%fx%' OR
              table_name ILIKE '%rate%' OR
              table_name ILIKE '%exchange%' OR
              table_name ILIKE '%conversion%' OR
              table_name ILIKE '%forex%'
          )
        ORDER BY 1,2
    """)
    for r in cur.fetchall():
        print(" ", r)

    # 2. Colunas com 'currency' em qualquer tabela
    print("\n" + "=" * 80)
    print("2. Colunas ILIKE '%currenc%' ou '%fx%' em public/platform")
    print("=" * 80)
    cur.execute("""
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema IN ('public','platform')
          AND (
              column_name ILIKE '%currenc%' OR
              column_name ILIKE '%fx%' OR
              column_name ILIKE '%exchange%' OR
              column_name ILIKE 'rate' OR
              column_name ILIKE '%brl%' OR
              column_name ILIKE '%usd%' OR
              column_name ILIKE '%pkr%'
          )
        ORDER BY 1,2,3
    """)
    for r in cur.fetchall():
        print(" ", r)

    # 3. Se existir currencies, listar valores
    print("\n" + "=" * 80)
    print("3. Dados da tabela currencies (se existir)")
    print("=" * 80)
    cur.execute("""
        SELECT EXISTS(
          SELECT 1 FROM information_schema.tables
          WHERE table_schema='public' AND table_name='currencies'
        )
    """)
    if cur.fetchone()[0]:
        cur.execute("SELECT id, code, type, name, symbol, decimals, active FROM currencies ORDER BY active DESC, code")
        for row in cur.fetchall():
            print(" ", row)
    else:
        print("  [nao existe]")

    # 3b. currency_exchange_rates completa
    print("\n" + "=" * 80)
    print("3b. Conteudo de currency_exchange_rates")
    print("=" * 80)
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='currency_exchange_rates' ORDER BY ordinal_position")
    print("Colunas:", cur.fetchall())
    cur.execute("SELECT * FROM currency_exchange_rates ORDER BY 1 LIMIT 50")
    for row in cur.fetchall():
        print(" ", row)

    # 4. Distinct currency_id em wallets e transactions
    print("\n" + "=" * 80)
    print("4. currency_id distinct em wallets/transactions/bets")
    print("=" * 80)
    for tbl in ['wallets', 'transactions', 'bets']:
        try:
            cur.execute(f"SELECT currency_id, COUNT(*) FROM {tbl} GROUP BY currency_id LIMIT 20")
            rows = cur.fetchall()
            print(f"  {tbl}: {rows}")
        except Exception as e:
            print(f"  {tbl}: sem currency_id ({e})")
            conn.rollback()
            conn.set_session(readonly=True, autocommit=True)

    # 5. Qualquer tabela com rate/conversion
    print("\n" + "=" * 80)
    print("5. Todas tabelas public (listagem rapida)")
    print("=" * 80)
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_type='BASE TABLE'
        ORDER BY table_name
    """)
    names = [r[0] for r in cur.fetchall()]
    # procura por qualquer nome suspeito
    suspeitos = [n for n in names if any(k in n.lower() for k in
        ['money','coin','token','crypto','wallet','balance','pay','deposit','withdraw'])]
    print("  Suspeitos (pagamento/carteira):", suspeitos)

finally:
    cur.close(); conn.close(); tunnel.stop()
