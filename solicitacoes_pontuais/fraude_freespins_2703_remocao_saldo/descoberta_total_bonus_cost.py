"""
Descoberta empirica: qual coluna/combinacao equivale ao "Total Bonus Cost" do BKO Pragmatic
para 27/03/2026 (fraude de freespins).

Colunas validadas via SHOW COLUMNS bonus_ec2.tbl_bonus_summary_details:
  - c_actual_issued_amount    -- valor real emitido (centavos)
  - c_freespin_win            -- ganho dos freespins (centavos)
  - c_free_spin_wager_amount  -- volume apostado em freespins
  - c_offered_crp/rrp/drp     -- pockets oferecidos
  - c_issued_crp/wrp/rrp/drp  -- pockets emitidos
  - c_balance_target_value    -- alvo de saldo (target)
  - c_bonus_id                -- (nao c_pre_offer_id)
"""
import sys
sys.path.insert(0, ".")
from db.athena import query_athena

print("="*80)
print("DESCOBERTA: Total Bonus Cost em bonus_ec2.tbl_bonus_summary_details")
print("Filtro: c_issue_date BRT = 2026-03-27")
print("="*80)

# 1. Somas globais de TODAS as colunas de valor em 27/03
print("\n[1/3] Somatorios em 27/03/2026 BRT (toda a base)")
sql_sums = """
SELECT
    COUNT(*)                                                                 AS qtd_registros,
    COUNT(DISTINCT c_ecr_id)                                                 AS qtd_jogadores,
    COUNT(DISTINCT c_bonus_id)                                               AS qtd_bonus_ids,
    SUM(c_actual_issued_amount)/100.0                                        AS h1_actual_issued_brl,
    SUM(c_freespin_win)/100.0                                                AS h2_freespin_win_brl,
    SUM(c_free_spin_wager_amount)/100.0                                      AS h3_freespin_wager_brl,
    SUM(c_offered_crp)/100.0                                                 AS sum_offered_crp_brl,
    SUM(c_offered_rrp)/100.0                                                 AS sum_offered_rrp_brl,
    SUM(c_offered_drp)/100.0                                                 AS sum_offered_drp_brl,
    SUM(c_issued_crp)/100.0                                                  AS sum_issued_crp_brl,
    SUM(c_issued_wrp)/100.0                                                  AS sum_issued_wrp_brl,
    SUM(c_issued_rrp)/100.0                                                  AS sum_issued_rrp_brl,
    SUM(c_issued_drp)/100.0                                                  AS sum_issued_drp_brl,
    SUM(c_balance_target_value)/100.0                                        AS sum_balance_target_brl,
    SUM(c_actual_issued_amount + COALESCE(c_freespin_win, 0))/100.0          AS h4_issued_plus_freespin_brl
FROM bonus_ec2.tbl_bonus_summary_details
WHERE DATE(c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') = DATE '2026-03-27'
"""
sums = query_athena(sql_sums, database="bonus_ec2")
print(sums.T.to_string())

# 2. Quebra por c_bonus_id (qual campanha concentra o volume?)
print("\n[2/3] Top 15 c_bonus_id em 27/03 (identificar a promo de freespins fraudada)")
sql_offers = """
SELECT
    c_bonus_id,
    COUNT(*)                                                                 AS qtd_emissoes,
    COUNT(DISTINCT c_ecr_id)                                                 AS qtd_jogadores,
    SUM(c_actual_issued_amount)/100.0                                        AS actual_issued_brl,
    SUM(c_freespin_win)/100.0                                                AS freespin_win_brl,
    SUM(c_free_spin_wager_amount)/100.0                                      AS freespin_wager_brl
FROM bonus_ec2.tbl_bonus_summary_details
WHERE DATE(c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') = DATE '2026-03-27'
GROUP BY c_bonus_id
ORDER BY qtd_emissoes DESC
LIMIT 15
"""
offers = query_athena(sql_offers, database="bonus_ec2")
print(offers.to_string(index=False))

# 3. Quebra por tipo: freespin vs cash bonus
print("\n[3/3] Distribuicao tipos em 27/03")
sql_split = """
SELECT
    CASE
        WHEN COALESCE(c_freespin_win, 0) > 0 AND c_actual_issued_amount > 0 THEN 'freespin_E_issued'
        WHEN COALESCE(c_freespin_win, 0) > 0 AND c_actual_issued_amount = 0 THEN 'so_freespin_win'
        WHEN c_actual_issued_amount > 0 THEN 'so_actual_issued'
        ELSE 'zero'
    END                                                          AS tipo,
    COUNT(*)                                                     AS qtd,
    COUNT(DISTINCT c_ecr_id)                                     AS jogadores,
    SUM(c_actual_issued_amount)/100.0                            AS issued_brl,
    SUM(c_freespin_win)/100.0                                    AS freespin_win_brl
FROM bonus_ec2.tbl_bonus_summary_details
WHERE DATE(c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') = DATE '2026-03-27'
GROUP BY 1
ORDER BY qtd DESC
"""
split = query_athena(sql_split, database="bonus_ec2")
print(split.to_string(index=False))

print("\n" + "="*80)
print("INTERPRETACAO DAS HIPOTESES")
print("="*80)
print("h1 = SUM(c_actual_issued_amount)         -> bonus convertido em cash real")
print("h2 = SUM(c_freespin_win)                 -> ganho gerado pelos freespins")
print("h3 = SUM(c_free_spin_wager_amount)       -> volume apostado nos freespins")
print("h4 = h1 + h2                             -> issued + ganho do spin")
print("\nO 'Total Bonus Cost' do BKO eh tipicamente:")
print("- Se incluir SO o cash credit emitido -> h1")
print("- Se incluir freespin (caso da fraude) -> h2 ou h4")
print("- Se for o turnover dos spins -> h3")
