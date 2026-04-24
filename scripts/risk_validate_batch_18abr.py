"""
Risk validation: endorsement of bonus abuse diagnosis (18/04/2026).
Tests 5 risk signals empirically on supernova_bet (Play4Tune):

1. IP/device fingerprint: contas do batch compartilham IP / user_agent?
2. Velocity: histograma reg->FTD (segundos), checar quem eh sub-2min (bot-like)
3. FAILED withdrawals reviewed_by: escalacao humana acontecendo?
4. Perfil fora do batch: os 15 FTDs que NAO sao do batch +9234137419xx -
   sao legitimos ou ha outros batches/padroes suspeitos?
5. Historico: mesmo prefixo / mesmo domain email viram antes dos ultimos 15d?
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import execute_supernova_bet

print("="*70)
print("RISK VALIDATION - Play4Tune batch 18/04/2026")
print("="*70)

# 1. Lista as 31 contas FTD de hoje e verifica IP/device fingerprint
print("\n[1] IP/device/session fingerprint - batch sequencial vs outros FTDs")
try:
    sql1 = """
    WITH ftds_hoje AS (
        SELECT DISTINCT u.id AS user_id, u.username, u.phone, u.email,
               u.created_at, t.ip_address, u.affiliate_code, u.referred_by,
               (u.phone LIKE '+92341374198%' OR u.phone LIKE '+92341374199%') AS is_batch
        FROM users u
        JOIN transactions t ON t.user_id = u.id
        WHERE DATE(u.created_at) = '2026-04-18'
          AND t.type = 'DEPOSIT' AND t.status = 'COMPLETED'
    )
    SELECT is_batch,
           COUNT(*) AS contas,
           COUNT(DISTINCT ip_address) AS ips_distintos,
           COUNT(DISTINCT SPLIT_PART(email,'@',2)) AS dominios_distintos,
           STRING_AGG(DISTINCT SPLIT_PART(email,'@',2), ', ') AS dominios
    FROM ftds_hoje
    GROUP BY is_batch
    ORDER BY is_batch DESC;
    """
    r = execute_supernova_bet(sql1, fetch=True)
    for row in r:
        print(f"  batch={row[0]}: {row[1]} contas, {row[2]} IPs distintos, {row[3]} dominios | dominios: {row[4]}")
except Exception as e:
    print(f"  ERRO (coluna pode nao existir): {e}")

# 2. Velocity registro -> FTD (segundos)
print("\n[2] Velocity registro -> FTD")
try:
    sql2 = """
    WITH ftd AS (
        SELECT u.id, u.phone, u.created_at AS reg,
               MIN(t.created_at) AS ftd,
               EXTRACT(EPOCH FROM (MIN(t.created_at) - u.created_at)) AS segs,
               (u.phone LIKE '+92341374198%' OR u.phone LIKE '+92341374199%') AS is_batch
        FROM users u
        JOIN transactions t ON t.user_id = u.id
        WHERE DATE(u.created_at) = '2026-04-18'
          AND t.type = 'DEPOSIT' AND t.status = 'COMPLETED'
        GROUP BY u.id, u.phone, u.created_at
    )
    SELECT is_batch,
           COUNT(*) AS n,
           ROUND(MIN(segs)::numeric,0) AS min_seg,
           ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY segs)::numeric,0) AS p50_seg,
           ROUND(MAX(segs)::numeric,0) AS max_seg,
           SUM(CASE WHEN segs < 120 THEN 1 ELSE 0 END) AS sub_2min
    FROM ftd
    GROUP BY is_batch ORDER BY is_batch DESC;
    """
    r = execute_supernova_bet(sql2, fetch=True)
    for row in r:
        print(f"  batch={row[0]}: n={row[1]}, min={row[2]}s, p50={row[3]}s, max={row[4]}s, sub2min={row[5]}")
except Exception as e:
    print(f"  ERRO: {e}")

# 3. Saques FAILED com reviewed_by (escalacao humana)
print("\n[3] Saques FAILED / reviewed_by - escalacao humana manual")
try:
    sql3 = """
    SELECT u.phone, u.email, t.amount, t.status, t.created_at,
           t.reviewed_by, t.review_note
    FROM transactions t
    JOIN users u ON u.id = t.user_id
    WHERE DATE(u.created_at) = '2026-04-18'
      AND t.type IN ('WITHDRAW','WITHDRAWAL')
      AND (t.status IN ('FAILED','REJECTED','PENDING') OR t.reviewed_by IS NOT NULL)
    ORDER BY t.created_at;
    """
    r = execute_supernova_bet(sql3, fetch=True)
    print(f"  {len(r)} saques FAILED/reviewed encontrados:")
    for row in r[:20]:
        print(f"    {row[0]} | {row[1]} | Rs {row[2]} | {row[3]} | reviewed_by={row[5]} | note={row[6]}")
except Exception as e:
    print(f"  ERRO: {e}")

# 4. FTDs FORA do batch - perfil/risco
print("\n[4] FTDs FORA do batch +9234137419xx - perfil")
try:
    sql4 = """
    SELECT u.username, u.phone, u.email, u.created_at,
           (SELECT SUM(amount) FROM transactions WHERE user_id=u.id AND type='DEPOSIT' AND status='COMPLETED') AS dep,
           (SELECT SUM(amount) FROM transactions WHERE user_id=u.id AND type IN ('WITHDRAW','WITHDRAWAL') AND status IN ('COMPLETED','PENDING')) AS saq,
           SPLIT_PART(u.email,'@',2) AS dominio
    FROM users u
    JOIN transactions t ON t.user_id=u.id
    WHERE DATE(u.created_at)='2026-04-18'
      AND t.type='DEPOSIT' AND t.status='COMPLETED'
      AND NOT (u.phone LIKE '+92341374198%' OR u.phone LIKE '+92341374199%')
    GROUP BY u.id, u.username, u.phone, u.email, u.created_at
    ORDER BY u.created_at;
    """
    r = execute_supernova_bet(sql4, fetch=True)
    print(f"  {len(r)} FTDs fora do batch:")
    for row in r:
        saq = row[5] if row[5] is not None else 0
        print(f"    {str(row[0])[:14]:14s} | {row[1]:16s} | {str(row[6])[:20]:20s} | dep=Rs{float(row[4]):.0f} | saq=Rs{float(saq):.0f}")
except Exception as e:
    print(f"  ERRO: {e}")

# 5. Historico: prefixo +9234137419 / dominios temp mail - aparecem antes?
print("\n[5] Historico 15d: prefixo e dominios descartaveis antes de hoje?")
try:
    sql5 = """
    SELECT DATE(created_at) AS dia,
           SUM(CASE WHEN phone LIKE '+92341374198%' OR phone LIKE '+92341374199%' THEN 1 ELSE 0 END) AS prefixo_batch,
           SUM(CASE WHEN email LIKE '%@wetuns.com' OR email LIKE '%@whyknapp.com' THEN 1 ELSE 0 END) AS temp_mail,
           SUM(CASE WHEN email ~ '@(mailinator|guerrillamail|tempmail|10minutemail|yopmail|throwaway)' THEN 1 ELSE 0 END) AS outros_temp,
           COUNT(*) AS total_cad
    FROM users
    WHERE created_at >= '2026-04-03' AND created_at < '2026-04-19'
    GROUP BY DATE(created_at)
    ORDER BY dia DESC;
    """
    r = execute_supernova_bet(sql5, fetch=True)
    print(f"  dia          | prefixo_batch | wetuns/whyknapp | outros_temp | total_cad")
    for row in r:
        print(f"  {row[0]} | {row[1]:13d} | {row[2]:15d} | {row[3]:11d} | {row[4]}")
except Exception as e:
    print(f"  ERRO: {e}")

print("\n" + "="*70)
print("FIM VALIDACAO")
print("="*70)
