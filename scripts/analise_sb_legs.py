"""Legs detalhados dos top bilhetes SB 19/04."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db.athena import query_athena
import pandas as pd
pd.set_option("display.max_columns", 80)
pd.set_option("display.width", 400)
pd.set_option("display.max_colwidth", 50)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

def show(label, sql):
    print(f"\n{'='*120}\n[{label}]\n{'='*120}")
    try:
        df = query_athena(sql); print(df.to_string(index=False)); return df
    except Exception as e:
        print(f"ERRO: {e}")
        return None

# Top 25 bilhetes — sem conversao timestamp varchar
sql = """
SELECT
    d.c_bet_slip_id,
    d.c_sport_type_name AS esporte,
    d.c_tournament_name AS torneio,
    d.c_event_name AS evento,
    d.c_market_name AS mercado,
    d.c_selection_name AS selecao,
    d.c_odds AS odd_leg,
    d.c_leg_status AS st,
    d.c_is_live AS live,
    d.c_vs_participant_home AS home,
    d.c_vs_participant_away AS away
FROM vendor_ec2.tbl_sports_book_bet_details d
WHERE d.c_bet_slip_id IN (
    'altenar_4850821402','altenar_4853674584','altenar_4846970620',
    'altenar_4849091563','altenar_4847858497','altenar_4849354862',
    'altenar_4851012321','altenar_4852540268','altenar_4849370233',
    'altenar_4844114944','altenar_4849954429','altenar_4853471292',
    'altenar_4852688594','altenar_4852130912','altenar_4845411028',
    'altenar_4850892869','altenar_4844141549','altenar_4853662802',
    'altenar_4851181537','altenar_4851050488','altenar_4850890738',
    'altenar_4848764425','altenar_4847730656','altenar_4840830969',
    'altenar_4841211902'
)
ORDER BY d.c_bet_slip_id
"""
df = show("LEGS dos 25 maiores bilhetes vencedores", sql)

outdir = ROOT / "reports" / "ggr_19abr"
if df is not None:
    df.to_csv(outdir / "sb_legs_top25.csv", index=False)
    print(f"\nCSV legs: {outdir / 'sb_legs_top25.csv'}")
