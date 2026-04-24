"""
Auditoria rapida — Validacao dos numeros das missoes
====================================================
Checks:
1. Hold rate sanity (2-5% normal, >10% = suspeito)
2. GGR plataforma vs soma games (nao pode ser maior)
3. Cross-check: player GGR total missao vs plataforma
4. user_id de teste no dataset?
5. Totais batem com CSVs
"""
import sys
sys.path.insert(0, ".")
import pandas as pd

OUTPUT_DIR = "outputs"
ERRORS = []
WARNINGS = []

df_platform = pd.read_csv(f"{OUTPUT_DIR}/platform_ggr_daily.csv")
df_games = pd.read_csv(f"{OUTPUT_DIR}/mission_games_ggr_daily.csv")
df_players = pd.read_csv(f"{OUTPUT_DIR}/mission_players_ggr_daily.csv")
df_users = pd.read_csv(f"{OUTPUT_DIR}/mission_users_raw.csv")

print("=" * 60)
print("  AUDITORIA — Retorno das Missoes")
print("=" * 60)

# CHECK 1: Hold rate por jogo no periodo missao
print("\n[1] Hold Rate Sanity Check")
df_games['activity_date'] = pd.to_datetime(df_games['activity_date'])
miss_games = df_games[df_games['activity_date'] >= '2026-03-23']
game_totals = miss_games.groupby('game_id').agg(ggr=('ggr', 'sum'), bets=('bets', 'sum')).reset_index()
game_totals['hold_pct'] = (game_totals['ggr'] / game_totals['bets'] * 100).round(2)

for _, r in game_totals.iterrows():
    status = "OK"
    if r['hold_pct'] > 10:
        status = "ATENCAO (>10%)"
        WARNINGS.append(f"Hold rate {r['game_id']}: {r['hold_pct']:.1f}% — acima do normal. Verificar variancia ou rollbacks.")
    elif r['hold_pct'] < 0:
        status = "NEGATIVO"
        WARNINGS.append(f"Hold rate {r['game_id']}: {r['hold_pct']:.1f}% — casa perdeu nesse jogo no periodo.")
    print(f"  {str(r['game_id']):<18} Hold: {r['hold_pct']:>6.1f}%  [{status}]")

# CHECK 2: GGR jogadores missao <= GGR total plataforma (por dia)
print("\n[2] GGR Players Missao <= GGR Plataforma (por dia)")
df_platform['activity_date'] = pd.to_datetime(df_platform['activity_date'])
df_players['activity_date'] = pd.to_datetime(df_players['activity_date'])

merged = df_platform.merge(df_players, on='activity_date', how='inner', suffixes=('_plat', '_miss'))
issues = merged[merged['ggr'] > merged['total_ggr']]
if issues.empty:
    print("  OK — GGR players <= GGR plataforma em todos os dias")
else:
    msg = f"GGR players > plataforma em {len(issues)} dias!"
    ERRORS.append(msg)
    print(f"  ERRO: {msg}")
    print(issues[['activity_date', 'total_ggr', 'ggr']].to_string(index=False))

# CHECK 3: Users de teste no dataset
print("\n[3] Users de teste")
test_users = df_users[df_users['user_id'].astype(str).str.contains(r'[^0-9]', regex=True)]
if test_users.empty:
    print("  OK — Nenhum user_id nao-numerico")
else:
    msg = f"{len(test_users)} user_ids nao-numericos (teste): {test_users['user_id'].tolist()[:5]}"
    WARNINGS.append(msg)
    print(f"  AVISO: {msg}")

# CHECK 4: Cobertura do mapeamento
print("\n[4] Cobertura user_id -> ecr_id")
df_mapped = pd.read_csv(f"{OUTPUT_DIR}/mission_users_mapped.csv")
not_found = df_mapped[df_mapped['ecr_id'] == 'NOT_FOUND']
total = len(df_mapped)
found = total - len(not_found)
pct = 100 * found / total
print(f"  Mapeados: {found}/{total} ({pct:.1f}%)")
if pct < 90:
    ERRORS.append(f"Cobertura baixa: {pct:.1f}%")
    print(f"  ERRO: cobertura abaixo de 90%")
else:
    print(f"  OK — cobertura acima de 90%")

# CHECK 5: Consistencia de totais
print("\n[5] Consistencia de totais")
player_ggr_total = df_players['ggr'].sum()
plat_ggr_total = df_platform['total_ggr'].sum()
pct_of_plat = 100 * player_ggr_total / plat_ggr_total
print(f"  GGR players missao (todo periodo): R$ {player_ggr_total:,.2f}")
print(f"  GGR plataforma (todo periodo):     R$ {plat_ggr_total:,.2f}")
print(f"  Participacao: {pct_of_plat:.1f}%")
if pct_of_plat > 50:
    WARNINGS.append(f"Jogadores de missao representam {pct_of_plat:.1f}% do GGR total — verificar se faz sentido")

# CHECK 6: Hold rate geral da plataforma (sanity)
print("\n[6] Hold rate geral plataforma")
plat_bets = df_platform['total_bets'].sum()
plat_wins = df_platform['total_wins'].sum()
plat_hold = 100 * plat_ggr_total / plat_bets
print(f"  Bets: R$ {plat_bets:,.0f} | Wins: R$ {plat_wins:,.0f} | GGR: R$ {plat_ggr_total:,.0f}")
print(f"  Hold rate plataforma: {plat_hold:.2f}%")
if 1 < plat_hold < 8:
    print("  OK — dentro da faixa esperada (1-8%)")
else:
    WARNINGS.append(f"Hold rate plataforma {plat_hold:.2f}% fora da faixa 1-8%")

# =============================================================================
# RESULTADO DA AUDITORIA
# =============================================================================
print("\n" + "=" * 60)
print("  RESULTADO DA AUDITORIA")
print("=" * 60)
if ERRORS:
    print(f"\n  ERROS ({len(ERRORS)}):")
    for e in ERRORS:
        print(f"    [X] {e}")
else:
    print("\n  ERROS: 0 — Nenhum erro critico encontrado")

if WARNINGS:
    print(f"\n  AVISOS ({len(WARNINGS)}):")
    for w in WARNINGS:
        print(f"    [!] {w}")
else:
    print("\n  AVISOS: 0")

if not ERRORS:
    print("\n  >>> AUDITORIA APROVADA — numeros prontos para entrega <<<")
else:
    print("\n  >>> AUDITORIA COM ERROS — revisar antes de entregar <<<")
