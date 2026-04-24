"""
Validacao cruzada: meu relatorio de tempo_resgate vs relatorio Mar/2026 da foto.

Foto relata (cruzamento Athena == Redshift, margem 0%):
- Bonus Concedido:  R$ 1.399.100,67
- Bonus Convertido: R$ 1.369.470,89
- Bonus Dropped:    R$   107.565,84
- Bonus Expirado:   R$    16.388,82
- Issue Drop Debit: R$    17.834,56
- Freespin Wins:    R$   272.115,04

Meu relatorio Mar/2026 (cohort por emissao):
- Valor BTR: R$ 1,50M (para bonus EMITIDOS em marco)

Testes:
1) R$ "convertido" POR MES DA TRANSACAO (nao emissao), via sub_fund type 20
   Deve bater com R$ 1,37M da foto.
2) R$ "convertido" POR MES DA EMISSAO do bonus
   Deve bater com R$ 1,50M do meu relatorio.
3) Diferenca entre as duas = bonus emitidos em jan/fev que converteram em mar
   = evidencia direta de "resgate dias/meses depois"
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.athena import query_athena


# 1) Valor BTR em Marco/2026 por MES DA TRANSACAO (timestamp da conversao)
sql_por_txn = """
SELECT
    date_trunc('month', s.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS mes_txn,
    COUNT(*) AS qtd_btr,
    SUM(s.c_amount_in_ecr_ccy)/100.0 AS valor_btr_brl
FROM fund_ec2.tbl_realcash_sub_fund_txn s
WHERE s.c_start_time >= TIMESTAMP '2026-01-01'
  AND s.c_start_time <  TIMESTAMP '2026-04-18'
  AND s.c_txn_type = 20
  AND s.c_op_type  = 'CR'
  AND s.c_amount_in_ecr_ccy > 0
GROUP BY 1
ORDER BY 1
"""


# 2) BTRs em Marco: emitidos em que mes? (prova da dinamica de "resgate meses depois")
# Cada BTR em sub_fund_txn aponta c_fund_txn_id que referencia uma fund_txn.
# A emissao original do bonus nao esta no sub_fund — precisamos linkar via
# c_ecr_bonus_id da bonus_ec2.
#
# Simplificacao: compara BTRs feitos em marco (pelo timestamp da sub_fund)
# com data do registro do bonus correspondente (tbl_real_fund_txn).
sql_btr_mar_por_cohort = """
WITH btr_mar AS (
    SELECT
        s.c_fund_txn_id,
        s.c_ecr_id,
        s.c_amount_in_ecr_ccy / 100.0 AS valor_btr_brl,
        (s.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_btr
    FROM fund_ec2.tbl_realcash_sub_fund_txn s
    WHERE s.c_start_time >= TIMESTAMP '2026-03-01'
      AND s.c_start_time <  TIMESTAMP '2026-04-01'
      AND s.c_txn_type = 20
      AND s.c_op_type  = 'CR'
      AND s.c_amount_in_ecr_ccy > 0
),
fund_link AS (
    -- Busca a data original (data do registro da txn bonus no fund)
    -- Normalmente c_start_time da fund_txn = emissao/uso do bonus
    SELECT
        f.c_txn_id,
        (f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_fund
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_start_time >= TIMESTAMP '2025-10-01'
      AND f.c_start_time <  TIMESTAMP '2026-04-18'
      AND f.c_txn_type = 20
)
SELECT
    date_trunc('month', fl.data_fund) AS mes_origem_bonus,
    COUNT(*) AS qtd_btr,
    SUM(b.valor_btr_brl) AS valor_brl
FROM btr_mar b
LEFT JOIN fund_link fl ON fl.c_txn_id = b.c_fund_txn_id
GROUP BY 1
ORDER BY 1
"""


# 3) Comparacao direta: valor total emitido vs convertido em cada mes
sql_emitido_vs_convertido = """
WITH emitido_por_mes AS (
    SELECT
        date_trunc('month', b.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS mes,
        SUM(COALESCE(s.c_actual_issued_amount, 0))/100.0 AS valor_emitido
    FROM bonus_ec2.tbl_ecr_bonus_details_inactive b
    LEFT JOIN bonus_ec2.tbl_bonus_summary_details s
      ON b.c_ecr_bonus_id = s.c_ecr_bonus_id
    WHERE b.c_created_time >= TIMESTAMP '2026-01-01'
      AND b.c_created_time <  TIMESTAMP '2026-04-18'
      AND b.c_bonus_status = 'BONUS_ISSUED_OFFER'
    GROUP BY 1
),
convertido_por_mes_txn AS (
    SELECT
        date_trunc('month', s.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS mes,
        SUM(s.c_amount_in_ecr_ccy)/100.0 AS valor_convertido_subfund
    FROM fund_ec2.tbl_realcash_sub_fund_txn s
    WHERE s.c_start_time >= TIMESTAMP '2026-01-01'
      AND s.c_start_time <  TIMESTAMP '2026-04-18'
      AND s.c_txn_type = 20
      AND s.c_op_type = 'CR'
      AND s.c_amount_in_ecr_ccy > 0
    GROUP BY 1
)
SELECT
    COALESCE(e.mes, c.mes) AS mes,
    e.valor_emitido,
    c.valor_convertido_subfund,
    (c.valor_convertido_subfund - e.valor_emitido) AS diff,
    CASE WHEN e.valor_emitido > 0
         THEN ROUND((c.valor_convertido_subfund - e.valor_emitido) / e.valor_emitido * 100, 1)
    END AS diff_pct
FROM emitido_por_mes e
FULL OUTER JOIN convertido_por_mes_txn c ON e.mes = c.mes
ORDER BY mes
"""


def run(nome, sql):
    print(f"\n{'=' * 75}")
    print(f"  {nome}")
    print(f"{'=' * 75}")
    df = query_athena(sql)
    print(df.to_string(index=False))
    return df


if __name__ == "__main__":
    print("\nREFERENCIA — relatorio Mar/2026 (foto, Athena=Redshift, 0% diff):")
    print("  Bonus Convertido: R$ 1.369.470,89")
    print("  Bonus Concedido:  R$ 1.399.100,67")

    run("1) BTR (sub_fund type 20) por MES DA TRANSACAO", sql_por_txn)
    run("2) BTRs em MARCO: qual cohort de emissao (via fund_txn)?", sql_btr_mar_por_cohort)
    run("3) Valor emitido (bonus cohort) vs Convertido (sub_fund txn)", sql_emitido_vs_convertido)
