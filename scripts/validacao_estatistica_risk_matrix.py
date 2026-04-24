"""
Validacao Estatistica Rigorosa - Matriz de Risco v2
====================================================
Analise: distribuicao, normalidade, correlacoes, tiers, outliers
Autor: Statistician Agent (PhD Stats)
Data: 2026-04-09
"""
import pandas as pd
import numpy as np
from scipy import stats
from collections import Counter
import sys

# Carregar CSV
CSV = r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet\reports\risk_matrix_2026-04-09_FINAL.csv"
df = pd.read_csv(CSV)
N = len(df)

tag_cols = [
    'regular_depositor','promo_only','zero_risk_player','fast_cashout',
    'sustained_player','non_bonus_depositor','promo_chainer','cashout_and_run',
    'reinvest_player','non_promo_player','engaged_player','rg_alert_player',
    'behav_risk_player','potencial_abuser','player_reengaged','sleeper_low_player',
    'vip_whale_player','winback_hi_val_player','behav_slotgamer',
    'multi_game_player','rollback_player'
]

tag_scores = {
    'regular_depositor':10, 'promo_only':-15, 'zero_risk_player':0,
    'fast_cashout':-25, 'sustained_player':15, 'non_bonus_depositor':10,
    'promo_chainer':-10, 'cashout_and_run':-25, 'reinvest_player':15,
    'non_promo_player':10, 'engaged_player':10, 'rg_alert_player':1,
    'behav_risk_player':-10, 'potencial_abuser':-5, 'player_reengaged':30,
    'sleeper_low_player':5, 'vip_whale_player':30, 'winback_hi_val_player':25,
    'behav_slotgamer':5, 'multi_game_player':-10, 'rollback_player':-15
}

sb = df['score_bruto']
sn = df['score_norm']

print("="*70)
print("VALIDACAO ESTATISTICA RIGOROSA - MATRIZ DE RISCO v2")
print("="*70)
print(f"N = {N:,} jogadores | Snapshot: 2026-04-09")
print()

# =====================================================================
# 1. DISTRIBUICAO DO SCORE_BRUTO
# =====================================================================
print("="*70)
print("1. DISTRIBUICAO DO SCORE_BRUTO")
print("="*70)
print(f"  Media:    {sb.mean():.4f}")
print(f"  Mediana:  {sb.median():.1f}")
print(f"  Moda:     {sb.mode().values}")
print(f"  Std:      {sb.std():.4f}")
print(f"  Min:      {sb.min()}")
print(f"  Max:      {sb.max()}")
print(f"  Skewness: {sb.skew():.4f}")
print(f"  Kurtosis: {sb.kurtosis():.4f} (excess/Fisher)")
print()

# Interpretacao skewness
skew_val = sb.skew()
if abs(skew_val) < 0.5:
    skew_interp = "aproximadamente simetrica"
elif skew_val > 0:
    skew_interp = f"positivamente assimetrica (cauda direita longa) — skew={skew_val:.2f}"
else:
    skew_interp = f"negativamente assimetrica (cauda esquerda longa) — skew={skew_val:.2f}"
print(f"  Interpretacao Skew: {skew_interp}")

# Interpretacao kurtosis (Fisher: normal=0)
kurt_val = sb.kurtosis()
if kurt_val > 1:
    kurt_interp = f"leptocurtica (caudas pesadas, picos) — kurtosis={kurt_val:.2f}"
elif kurt_val < -1:
    kurt_interp = f"platicurtica (caudas leves, achatada) — kurtosis={kurt_val:.2f}"
else:
    kurt_interp = f"mesocurtica (proxima da normal) — kurtosis={kurt_val:.2f}"
print(f"  Interpretacao Kurt: {kurt_interp}")
print()

# Teste de normalidade (Kolmogorov-Smirnov, pois N>5000 invalida Shapiro)
ks_stat, ks_pval = stats.kstest(sb, 'norm', args=(sb.mean(), sb.std()))
print(f"  Teste KS (normalidade): stat={ks_stat:.6f}, p-value={ks_pval:.2e}")
print(f"  Resultado: {'REJEITA normalidade' if ks_pval < 0.05 else 'Nao rejeita'} (alpha=0.05)")
print()

# Top 20 valores mais frequentes
print("  --- Top 20 score_bruto mais frequentes ---")
val_counts = sb.value_counts().head(20)
for val, cnt in val_counts.items():
    print(f"    score={val:>6}: {cnt:>8,} jogadores ({cnt/N*100:5.1f}%)")
print()

# =====================================================================
# 2. VALIDACAO PERCENTIS P05/P95
# =====================================================================
print("="*70)
print("2. VALIDACAO PERCENTIS P05 e P95")
print("="*70)
percentis = [1, 2.5, 5, 10, 25, 50, 75, 90, 95, 97.5, 99]
print("  Percentis recalculados do score_bruto:")
for p in percentis:
    val = np.percentile(sb, p)
    marker = ""
    if p == 5: marker = f"  <-- documentado: -35, {'OK' if val == -35 else f'DIVERGE (real={val})'}"
    if p == 95: marker = f"  <-- documentado: 50, {'OK' if val == 50 else f'DIVERGE (real={val})'}"
    print(f"    P{p:5.1f}: {val:>8.1f}{marker}")

# Percentis SEM zeros (excluir SEM SCORE)
sb_nonzero = sb[sb != 0]
print()
print(f"  Percentis EXCLUINDO SEM SCORE (score_bruto=0): N={len(sb_nonzero):,}")
for p in [5, 25, 50, 75, 95]:
    val = np.percentile(sb_nonzero, p)
    print(f"    P{p}: {val:.1f}")
print()

# IQR analysis
q1 = np.percentile(sb, 25)
q3 = np.percentile(sb, 75)
iqr = q3 - q1
print(f"  IQR: Q1={q1}, Q3={q3}, IQR={iqr}")
print(f"  Limites outlier (Tukey): [{q1 - 1.5*iqr}, {q3 + 1.5*iqr}]")
n_outliers_low = (sb < q1 - 1.5*iqr).sum()
n_outliers_high = (sb > q3 + 1.5*iqr).sum()
print(f"  Outliers baixo: {n_outliers_low:,} ({n_outliers_low/N*100:.1f}%)")
print(f"  Outliers alto:  {n_outliers_high:,} ({n_outliers_high/N*100:.1f}%)")
print()

# =====================================================================
# 3. ANALISE DA NORMALIZACAO
# =====================================================================
print("="*70)
print("3. ANALISE DA NORMALIZACAO")
print("="*70)
# Verificar se a formula esta correta
df['score_norm_calc'] = ((sb + 35) / 85 * 100).clip(0, 100).round(1)
diff = (df['score_norm'] - df['score_norm_calc']).abs()
print(f"  Formula: score_norm = (score_bruto + 35) / 85 * 100, clamp [0, 100]")
print(f"  Verificacao: max diferenca entre CSV e recalculado = {diff.max():.4f}")
print(f"  Jogadores com score_bruto < -35 (truncados a 0):  {(sb < -35).sum():,}")
print(f"  Jogadores com score_bruto > 50 (truncados a 100): {(sb > 50).sum():,}")
pct_truncated = ((sb < -35).sum() + (sb > 50).sum()) / N * 100
print(f"  Total truncados: {pct_truncated:.2f}% da base")
print()

# Concentracao no meio
pct_40_60 = ((sn >= 40) & (sn <= 60)).sum() / N * 100
pct_20_80 = ((sn >= 20) & (sn <= 80)).sum() / N * 100
print(f"  Concentracao score_norm 40-60: {pct_40_60:.1f}% (zona central)")
print(f"  Concentracao score_norm 20-80: {pct_20_80:.1f}%")
print(f"  Score_norm distribuicao:")
for bucket in [(0,10), (10,20), (20,30), (30,40), (40,50), (50,60), (60,70), (70,80), (80,90), (90,100)]:
    lo, hi = bucket
    if hi == 100:
        n_bucket = ((sn >= lo) & (sn <= hi)).sum()
    else:
        n_bucket = ((sn >= lo) & (sn < hi)).sum()
    print(f"    [{lo:>3}-{hi:>3}): {n_bucket:>8,} ({n_bucket/N*100:5.1f}%)")
print()

# =====================================================================
# 4. DISTRIBUICAO DOS TIERS
# =====================================================================
print("="*70)
print("4. DISTRIBUICAO DOS TIERS")
print("="*70)
tier_order = ['Muito Bom', 'Bom', 'Mediano', 'Ruim', 'Muito Ruim', 'SEM SCORE']
for t in tier_order:
    n_t = (df['tier'] == t).sum()
    pct = n_t / N * 100
    # Score ranges
    tier_sb = sb[df['tier'] == t]
    print(f"  {t:15s}: {n_t:>8,} ({pct:5.1f}%) | score_bruto: [{tier_sb.min():>5}, {tier_sb.max():>5}] | media={tier_sb.mean():.1f}")

# Gini/concentracao
print()
tier_counts = [((df['tier'] == t).sum()) for t in tier_order[:5]]  # excl SEM SCORE
total_scored = sum(tier_counts)
print(f"  Jogadores COM score: {total_scored:,}")
print(f"  Entropia (H) dos 5 tiers (max=2.32 para uniforme):")
probs = [c/total_scored for c in tier_counts]
entropy = -sum(p * np.log2(p) for p in probs if p > 0)
print(f"    H = {entropy:.4f} (max uniforme = {np.log2(5):.4f})")
print(f"    Uniformidade = {entropy/np.log2(5)*100:.1f}%")
print()

# Chi-square test: tiers distribuidos uniformemente?
expected_uniform = [total_scored/5]*5
chi2, chi2_p = stats.chisquare(tier_counts, f_exp=expected_uniform)
print(f"  Chi-square (tiers uniformes?): chi2={chi2:.1f}, p={chi2_p:.2e}")
print(f"  Resultado: {'REJEITA uniformidade' if chi2_p < 0.05 else 'Nao rejeita'}")
print()

# =====================================================================
# 5. PREVALENCIA E CORRELACAO ENTRE TAGS
# =====================================================================
print("="*70)
print("5. PREVALENCIA E CORRELACAO ENTRE TAGS")
print("="*70)

# Tag activation: converter para binario (0/1)
tag_binary = pd.DataFrame()
for col in tag_cols:
    tag_binary[col] = (df[col] != 0).astype(int)

print("  --- Prevalencia (% de jogadores com tag ativa) ---")
for col in sorted(tag_cols, key=lambda c: tag_binary[c].sum(), reverse=True):
    n_active = tag_binary[col].sum()
    print(f"    {col:25s}: {n_active:>8,} ({n_active/N*100:5.1f}%) | score={tag_scores[col]:>+4}")

print()
print("  --- Numero de tags por jogador ---")
n_tags = tag_binary.sum(axis=1)
for i in range(0, 15):
    cnt = (n_tags == i).sum()
    if cnt > 0:
        print(f"    {i:2} tags: {cnt:>8,} ({cnt/N*100:5.1f}%)")

print()
print("  --- Top 15 correlacoes entre tags (Phi/tetrachorica) ---")
corr_matrix = tag_binary.corr()
# Extrair pares unicos
pairs = []
for i in range(len(tag_cols)):
    for j in range(i+1, len(tag_cols)):
        r = corr_matrix.iloc[i, j]
        pairs.append((tag_cols[i], tag_cols[j], r))
pairs.sort(key=lambda x: abs(x[2]), reverse=True)
for t1, t2, r in pairs[:15]:
    print(f"    {t1:25s} x {t2:25s}: r={r:+.4f}")

# FAST_CASHOUT vs CASHOUT_AND_RUN especificamente
r_fc_car = corr_matrix.loc['fast_cashout', 'cashout_and_run']
both = ((tag_binary['fast_cashout']==1) & (tag_binary['cashout_and_run']==1)).sum()
only_fc = ((tag_binary['fast_cashout']==1) & (tag_binary['cashout_and_run']==0)).sum()
only_car = ((tag_binary['fast_cashout']==0) & (tag_binary['cashout_and_run']==1)).sum()
neither = ((tag_binary['fast_cashout']==0) & (tag_binary['cashout_and_run']==0)).sum()
print()
print(f"  --- FAST_CASHOUT x CASHOUT_AND_RUN (detalhe) ---")
print(f"    Correlacao: r={r_fc_car:+.4f}")
print(f"    Ambas ativas:       {both:>8,} ({both/N*100:.1f}%)")
print(f"    So FAST_CASHOUT:    {only_fc:>8,} ({only_fc/N*100:.1f}%)")
print(f"    So CASHOUT_AND_RUN: {only_car:>8,} ({only_car/N*100:.1f}%)")
print(f"    Nenhuma:            {neither:>8,} ({neither/N*100:.1f}%)")
# Chi-square
ct = pd.crosstab(tag_binary['fast_cashout'], tag_binary['cashout_and_run'])
chi2_fc, p_fc, dof_fc, exp_fc = stats.chi2_contingency(ct)
print(f"    Chi-square: chi2={chi2_fc:.1f}, p={p_fc:.2e}, Cramer's V={np.sqrt(chi2_fc/N):.4f}")
print()

# =====================================================================
# 6. ANALISE SEM SCORE
# =====================================================================
print("="*70)
print("6. ANALISE SEM SCORE (jogadores com 0 tags)")
print("="*70)
sem_score = df[df['tier'] == 'SEM SCORE']
com_score = df[df['tier'] != 'SEM SCORE']
print(f"  SEM SCORE: {len(sem_score):,} ({len(sem_score)/N*100:.1f}%)")
# Verificar se SEM SCORE = score_bruto == 0
sem_score_bruto_0 = (sem_score['score_bruto'] == 0).all()
print(f"  Todos score_bruto=0? {sem_score_bruto_0}")
# Verificar se ha jogadores com score_bruto=0 que NAO sao SEM SCORE
bruto_0_nao_sem = df[(df['score_bruto'] == 0) & (df['tier'] != 'SEM SCORE')]
print(f"  score_bruto=0 mas tier != SEM SCORE: {len(bruto_0_nao_sem):,}")
# Verificar se todas as tags sao 0
all_tags_zero = (df[tag_cols].sum(axis=1) == 0)
n_all_zero = all_tags_zero.sum()
print(f"  Jogadores com TODAS as tags = 0: {n_all_zero:,}")
n_sem_score_csv = (df['tier'] == 'SEM SCORE').sum()
print(f"  SEM SCORE no tier: {n_sem_score_csv:,}")
print(f"  Divergencia: {abs(n_all_zero - n_sem_score_csv):,}")
print()

# Score bruto de SEM SCORE: deveria ser 0 e norm 41.2
print(f"  Score norm dos SEM SCORE: {sem_score['score_norm'].unique()}")
print(f"  ATENCAO: score_bruto=0 gera norm=(0+35)/85*100 = {(0+35)/85*100:.1f}")
print(f"  Isso coloca SEM SCORE na faixa Mediano (26-50) pela formula!")
print(f"  MAS o pipeline trata como excecao e rotula SEM SCORE")
print()

# =====================================================================
# 7. ROBUSTEZ DO RANGE 85 (INFLACAO FUTURA)
# =====================================================================
print("="*70)
print("7. ROBUSTEZ DO RANGE 85 E INFLACAO FUTURA")
print("="*70)
# Score maximo e minimo teorico
max_pos = sum(v for v in tag_scores.values() if v > 0)
max_neg = sum(v for v in tag_scores.values() if v < 0)
print(f"  Score teorico maximo: +{max_pos} (todas positivas)")
print(f"  Score teorico minimo: {max_neg} (todas negativas)")
print(f"  Range teorico: {max_pos - max_neg}")
print(f"  Range usado (P05-P95): 85 ({85/(max_pos-max_neg)*100:.1f}% do teorico)")
print()

# Simulacao: adicionar 5 tags positivas (+10 cada) ou 3 negativas (-15 cada)
new_max = max_pos + 5*10  # +50
new_min = max_neg + 3*(-15)  # -45
print(f"  Simulacao: +5 tags positivas (+10 cada) + 3 negativas (-15 cada)")
print(f"    Novo max teorico: +{new_max}")
print(f"    Novo min teorico: {new_min}")
print(f"    Com range 85 fixo:")
print(f"      score_bruto=+{new_max} -> norm={(new_max+35)/85*100:.1f} (truncado a 100)")
print(f"      score_bruto={new_min} -> norm={(new_min+35)/85*100:.1f} (truncado a 0)")
print(f"    Mais jogadores truncados = PERDA DE DISCRIMINACAO")
print()

# Qual % da base atual ja esta no ceiling/floor?
print(f"  Jogadores no ceiling (100.0): {(sn == 100.0).sum():,} ({(sn == 100.0).sum()/N*100:.1f}%)")
print(f"  Jogadores no floor (0.0):     {(sn == 0.0).sum():,} ({(sn == 0.0).sum()/N*100:.1f}%)")
print()

# =====================================================================
# 8. SANIDADE DOS EXTREMOS
# =====================================================================
print("="*70)
print("8. SANIDADE DOS EXTREMOS")
print("="*70)

# Top 10 maiores
print("  --- Top 10 score_bruto MAIS ALTO ---")
top10_high = df.nlargest(10, 'score_bruto')[['user_id','score_bruto','score_norm','tier'] + tag_cols]
for _, row in top10_high.iterrows():
    active = [c for c in tag_cols if row[c] != 0]
    print(f"    bruto={row['score_bruto']:>4} | norm={row['score_norm']:>5.1f} | tags: {', '.join(active)}")

print()
print("  --- Top 10 score_bruto MAIS BAIXO ---")
top10_low = df.nsmallest(10, 'score_bruto')[['user_id','score_bruto','score_norm','tier'] + tag_cols]
for _, row in top10_low.iterrows():
    active = [c for c in tag_cols if row[c] != 0]
    print(f"    bruto={row['score_bruto']:>4} | norm={row['score_norm']:>5.1f} | tags: {', '.join(active)}")

print()

# =====================================================================
# 9. CO-OCORRENCIA DE TAGS POSITIVAS + NEGATIVAS
# =====================================================================
print("="*70)
print("9. CO-OCORRENCIA POSITIVA + NEGATIVA")
print("="*70)
pos_tags = [c for c in tag_cols if tag_scores[c] > 0]
neg_tags = [c for c in tag_cols if tag_scores[c] < 0]

n_pos = tag_binary[pos_tags].sum(axis=1)
n_neg = tag_binary[neg_tags].sum(axis=1)

print(f"  Jogadores com tags positivas E negativas: {((n_pos>0)&(n_neg>0)).sum():,} ({((n_pos>0)&(n_neg>0)).sum()/N*100:.1f}%)")
print(f"  Jogadores SOMENTE positivas: {((n_pos>0)&(n_neg==0)).sum():,} ({((n_pos>0)&(n_neg==0)).sum()/N*100:.1f}%)")
print(f"  Jogadores SOMENTE negativas: {((n_pos==0)&(n_neg>0)).sum():,} ({((n_pos==0)&(n_neg>0)).sum()/N*100:.1f}%)")
print(f"  Jogadores sem nenhuma tag: {((n_pos==0)&(n_neg==0)).sum():,} ({((n_pos==0)&(n_neg==0)).sum()/N*100:.1f}%)")
print()

# =====================================================================
# 10. ALTERNATIVAS DE NORMALIZACAO
# =====================================================================
print("="*70)
print("10. ALTERNATIVAS DE NORMALIZACAO")
print("="*70)

# Quantile-based (rank percentile)
rank_norm = sb.rank(method='average') / N * 100
print(f"  Quantile-based (rank percentile):")
print(f"    Entropia Tiers com rank-norm: calcular...")
# Aplicar os mesmos cortes: >75, 51-75, 26-50, 11-25, <=10
def tier_from_norm(s):
    if s > 75: return 'Muito Bom'
    if s > 50: return 'Bom'
    if s > 25: return 'Mediano'
    if s > 10: return 'Ruim'
    return 'Muito Ruim'

rank_tiers = rank_norm.apply(tier_from_norm)
print("    Distribuicao com quantile-based norm (mesmas faixas):")
for t in ['Muito Bom','Bom','Mediano','Ruim','Muito Ruim']:
    n_t = (rank_tiers == t).sum()
    print(f"      {t:15s}: {n_t:>8,} ({n_t/N*100:.1f}%)")

print()
# Min-max sobre range real
sb_min_real, sb_max_real = sb.min(), sb.max()
minmax_norm = ((sb - sb_min_real) / (sb_max_real - sb_min_real) * 100).clip(0, 100)
print(f"  Min-Max sobre range real [{sb_min_real}, {sb_max_real}]:")
print(f"    Media norm: {minmax_norm.mean():.1f}, Std: {minmax_norm.std():.1f}")
minmax_tiers = minmax_norm.apply(tier_from_norm)
for t in ['Muito Bom','Bom','Mediano','Ruim','Muito Ruim']:
    n_t = (minmax_tiers == t).sum()
    print(f"      {t:15s}: {n_t:>8,} ({n_t/N*100:.1f}%)")

print()
print("="*70)
print("FIM DA VALIDACAO")
print("="*70)
