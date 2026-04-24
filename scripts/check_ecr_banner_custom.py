"""Verifica cobertura de campos custom em tbl_ecr_banner pros trackers Meta."""
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena
import pandas as pd
pd.set_option('display.max_columns', None); pd.set_option('display.width', 300)

sql = """
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN c_custom1       IS NOT NULL AND c_custom1       <> '' THEN 1 ELSE 0 END) AS com_custom1,
  SUM(CASE WHEN c_custom2       IS NOT NULL AND c_custom2       <> '' THEN 1 ELSE 0 END) AS com_custom2,
  SUM(CASE WHEN c_custom3       IS NOT NULL AND c_custom3       <> '' THEN 1 ELSE 0 END) AS com_custom3,
  SUM(CASE WHEN c_custom4       IS NOT NULL AND c_custom4       <> '' THEN 1 ELSE 0 END) AS com_custom4,
  SUM(CASE WHEN c_reference_url IS NOT NULL AND c_reference_url <> '' THEN 1 ELSE 0 END) AS com_ref_url
FROM ecr_ec2.tbl_ecr_banner
WHERE c_tracker_id IN ('464673','532571')
  AND CAST(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) BETWEEN DATE '2026-03-01' AND DATE '2026-04-16'
"""
print("Coverage campos potencialmente com UTM original:")
print(query_athena(sql, database='ecr_ec2').to_string())

sql2 = """
SELECT c_ecr_id, c_tracker_id, c_custom1, c_custom2, c_custom3, c_custom4, c_reference_url
FROM ecr_ec2.tbl_ecr_banner
WHERE c_tracker_id IN ('464673','532571')
  AND (
       (c_custom1       IS NOT NULL AND c_custom1       <> '')
    OR (c_custom2       IS NOT NULL AND c_custom2       <> '')
    OR (c_custom3       IS NOT NULL AND c_custom3       <> '')
    OR (c_custom4       IS NOT NULL AND c_custom4       <> '')
    OR (c_reference_url IS NOT NULL AND c_reference_url <> '')
  )
LIMIT 10
"""
print("\nAmostra (se existir):")
print(query_athena(sql2, database='ecr_ec2').to_string())
