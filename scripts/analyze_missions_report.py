"""
Analise e Report — Retorno das Missoes (GGR)
=============================================
Agrega dados extraidos e gera report para o Head (Castrin).
"""
import sys, os
sys.path.insert(0, ".")
import pandas as pd
import numpy as np

OUTPUT_DIR = "outputs"

# =============================================================================
# Carregar dados
# =============================================================================
df_platform = pd.read_csv(f"{OUTPUT_DIR}/platform_ggr_daily.csv")
df_games = pd.read_csv(f"{OUTPUT_DIR}/mission_games_ggr_daily.csv")
df_players = pd.read_csv(f"{OUTPUT_DIR}/mission_players_ggr_daily.csv")
df_missions = pd.read_csv(f"{OUTPUT_DIR}/missions_summary.csv")

# Converter datas
df_platform['activity_date'] = pd.to_datetime(df_platform['activity_date'])
df_games['activity_date'] = pd.to_datetime(df_games['activity_date'])
df_players['activity_date'] = pd.to_datetime(df_players['activity_date'])

# Periodos
MISSION_START = pd.Timestamp("2026-03-23")
MISSION_END   = pd.Timestamp("2026-04-02")
CONTROL_START = pd.Timestamp("2026-03-13")
CONTROL_END   = pd.Timestamp("2026-03-22")

# Mapa limpo de nomes
GAME_NAMES = {
    '4776': 'Fortune Tiger', '13097': 'Fortune Dragon', '8842': 'Fortune Rabbit',
    '2603': 'Fortune Ox', '833': 'Fortune Mouse', '18949': 'Fortune Snake',
    'vs20fruitsw': 'Sweet Bonanza', 'vs20caramsort': 'Vira-Lata Caramelo',
    'vs5luckytig': 'Tigre Sortudo', 'vs10forwild': 'Ratinho Sortudo',
    '3221': 'Wild Bandito'
}

# =============================================================================
# 1. GGR dos JOGOS de missao (todos os jogadores, nao so mission)
# =============================================================================
print("=" * 70)
print("  ANALISE DE RETORNO DAS MISSOES")
print("  Periodo: 23/03/2026 a 02/04/2026 (11 dias)")
print("  Controle: 13/03/2026 a 22/03/2026 (10 dias)")
print("=" * 70)

# Agregar por periodo
df_games['game_name'] = df_games['game_id'].astype(str).map(GAME_NAMES).fillna(df_games['game_id'].astype(str))
df_games['periodo'] = np.where(df_games['activity_date'] >= MISSION_START, 'missao', 'controle')

game_agg = df_games.groupby(['game_name', 'periodo']).agg(
    ggr=('ggr', 'sum'),
    bets=('bets', 'sum'),
    wins=('wins', 'sum'),
    players_avg=('players', 'mean'),
    rounds=('rounds', 'sum'),
    dias=('activity_date', 'nunique')
).reset_index()

game_agg['ggr_dia'] = game_agg['ggr'] / game_agg['dias']
game_agg['hold_rate'] = (game_agg['ggr'] / game_agg['bets'] * 100).round(2)

print("\n--- 1. GGR por Jogo (TODOS os jogadores) ---")
print(f"{'Jogo':<22} {'Periodo':<10} {'GGR Total':>14} {'GGR/dia':>14} {'Hold%':>7} {'Players/dia':>12} {'Rounds':>12}")
print("-" * 92)
for _, r in game_agg.sort_values(['game_name', 'periodo']).iterrows():
    print(f"{r['game_name']:<22} {r['periodo']:<10} R${r['ggr']:>12,.0f} R${r['ggr_dia']:>12,.0f} {r['hold_rate']:>6.1f}% {r['players_avg']:>11,.0f} {r['rounds']:>11,.0f}")

# Totais por periodo (games de missao)
total_game = game_agg.groupby('periodo').agg(ggr=('ggr', 'sum'), bets=('bets', 'sum')).reset_index()

ggr_game_ctrl = total_game.loc[total_game['periodo'] == 'controle', 'ggr'].values[0]
ggr_game_miss = total_game.loc[total_game['periodo'] == 'missao', 'ggr'].values[0]
days_ctrl = 10
days_miss = 11

print(f"\n  TOTAL games de missao:")
print(f"    Controle: R$ {ggr_game_ctrl:,.2f} ({days_ctrl} dias) | Media/dia: R$ {ggr_game_ctrl/days_ctrl:,.2f}")
print(f"    Missao:   R$ {ggr_game_miss:,.2f} ({days_miss} dias) | Media/dia: R$ {ggr_game_miss/days_miss:,.2f}")
if ggr_game_ctrl > 0:
    uplift_game = ((ggr_game_miss/days_miss - ggr_game_ctrl/days_ctrl) / (ggr_game_ctrl/days_ctrl)) * 100
    print(f"    Uplift GGR/dia games: {uplift_game:+.1f}%")

# =============================================================================
# 2. GGR dos JOGADORES de missao (somente participantes)
# =============================================================================
print("\n--- 2. GGR dos Jogadores de Missao (12.574 mapeados) ---")

df_players['periodo'] = np.where(df_players['activity_date'] >= MISSION_START, 'missao', 'controle')

player_agg = df_players.groupby('periodo').agg(
    ggr=('ggr', 'sum'),
    deposits=('deposits', 'sum'),
    bets=('casino_bets', 'sum'),
    wins=('casino_wins', 'sum'),
    players_avg=('active_players', 'mean'),
    rounds=('rounds', 'sum'),
    dias=('activity_date', 'nunique')
).reset_index()

for _, r in player_agg.iterrows():
    print(f"\n  {r['periodo'].upper()} ({int(r['dias'])} dias):")
    print(f"    GGR Total:      R$ {r['ggr']:>14,.2f}   (media/dia: R$ {r['ggr']/r['dias']:>12,.2f})")
    print(f"    Depositos:      R$ {r['deposits']:>14,.2f}   (media/dia: R$ {r['deposits']/r['dias']:>12,.2f})")
    print(f"    Bets Casino:    R$ {r['bets']:>14,.2f}   (media/dia: R$ {r['bets']/r['dias']:>12,.2f})")
    print(f"    Players ativos/dia: {r['players_avg']:>10,.0f}")
    print(f"    Rounds totais:  {r['rounds']:>14,.0f}   (media/dia: {r['rounds']/r['dias']:>12,.0f})")

ctrl = player_agg[player_agg['periodo'] == 'controle'].iloc[0]
miss = player_agg[player_agg['periodo'] == 'missao'].iloc[0]

print(f"\n  UPLIFTS (media/dia):")
metrics = [
    ('GGR', 'ggr'), ('Depositos', 'deposits'),
    ('Bets', 'bets'), ('Rounds', 'rounds')
]
for name, col in metrics:
    ctrl_avg = ctrl[col] / ctrl['dias']
    miss_avg = miss[col] / miss['dias']
    if ctrl_avg > 0:
        up = ((miss_avg - ctrl_avg) / ctrl_avg) * 100
        print(f"    {name:<12} {up:>+8.1f}%   (R$ {ctrl_avg:>12,.0f} -> R$ {miss_avg:>12,.0f}/dia)")
    else:
        print(f"    {name:<12} N/A")

players_ctrl_avg = ctrl['players_avg']
players_miss_avg = miss['players_avg']
up_players = ((players_miss_avg - players_ctrl_avg) / players_ctrl_avg) * 100
print(f"    {'Players':<12} {up_players:>+8.1f}%   ({players_ctrl_avg:>12,.0f} -> {players_miss_avg:>12,.0f}/dia)")

# =============================================================================
# 3. Participacao no GGR total da plataforma
# =============================================================================
print("\n--- 3. Participacao no GGR Total da Plataforma ---")

df_platform['periodo'] = np.where(df_platform['activity_date'] >= MISSION_START, 'missao', 'controle')
plat_agg = df_platform.groupby('periodo').agg(ggr=('total_ggr', 'sum')).reset_index()

plat_ctrl = plat_agg.loc[plat_agg['periodo'] == 'controle', 'ggr'].values[0]
plat_miss = plat_agg.loc[plat_agg['periodo'] == 'missao', 'ggr'].values[0]

pct_ctrl = (ggr_game_ctrl / plat_ctrl * 100) if plat_ctrl > 0 else 0
pct_miss = (ggr_game_miss / plat_miss * 100) if plat_miss > 0 else 0

# Player GGR como % do total
player_ggr_miss = miss['ggr']
pct_player_miss = (player_ggr_miss / plat_miss * 100) if plat_miss > 0 else 0

print(f"  GGR total plataforma (controle): R$ {plat_ctrl:>14,.2f}")
print(f"  GGR total plataforma (missao):   R$ {plat_miss:>14,.2f}")
print(f"  GGR games de missao (controle):  R$ {ggr_game_ctrl:>14,.2f}  ({pct_ctrl:.1f}% do total)")
print(f"  GGR games de missao (missao):    R$ {ggr_game_miss:>14,.2f}  ({pct_miss:.1f}% do total)")
print(f"  GGR jogadores de missao (missao): R$ {player_ggr_miss:>14,.2f}  ({pct_player_miss:.1f}% do total)")

# =============================================================================
# 4. Resumo das missoes
# =============================================================================
print("\n--- 4. Resumo das Missoes ---")

# Agrupar por jogo (nao por quest)
df_missions['game_name_clean'] = df_missions['game_path'].astype(str).map(GAME_NAMES).fillna(df_missions['game_path'].astype(str))
# Pegar quest1 apenas para contagem de usuarios (quest1 = opt-in inicial)
quest1 = df_missions[df_missions['mission_code'].str.contains('quest1', na=False)]
quest1_agg = quest1.groupby('game_name_clean').agg(
    users=('total_users', 'sum'),
    completaram=('completaram', 'sum')
).reset_index()
quest1_agg['taxa_conclusao'] = (quest1_agg['completaram'] / quest1_agg['users'] * 100).round(1)

print(f"\n{'Jogo':<22} {'Opt-ins Q1':>10} {'Completaram':>12} {'Taxa':>6}")
print("-" * 54)
for _, r in quest1_agg.sort_values('users', ascending=False).iterrows():
    print(f"{r['game_name_clean']:<22} {r['users']:>10,} {r['completaram']:>12,} {r['taxa_conclusao']:>5.1f}%")

total_users = quest1_agg['users'].sum()
total_completed = quest1_agg['completaram'].sum()
print(f"{'TOTAL':<22} {total_users:>10,} {total_completed:>12,} {100*total_completed/total_users:.1f}%")

# =============================================================================
# 5. Top jogos por GGR no periodo de missao
# =============================================================================
print("\n--- 5. Top Jogos por GGR (periodo de missao) ---")
miss_games = game_agg[game_agg['periodo'] == 'missao'].sort_values('ggr', ascending=False)
print(f"\n{'Jogo':<22} {'GGR':>14} {'Bets':>14} {'Hold%':>7}")
print("-" * 60)
for _, r in miss_games.iterrows():
    print(f"{r['game_name']:<22} R${r['ggr']:>12,.0f} R${r['bets']:>12,.0f} {r['hold_rate']:>6.1f}%")

total_miss_ggr = miss_games['ggr'].sum()
total_miss_bets = miss_games['bets'].sum()
print(f"{'TOTAL':<22} R${total_miss_ggr:>12,.0f} R${total_miss_bets:>12,.0f} {100*total_miss_ggr/total_miss_bets:.1f}%")

# =============================================================================
# 6. Conclusao executiva
# =============================================================================
print("\n" + "=" * 70)
print("  CONCLUSAO EXECUTIVA PARA O HEAD")
print("=" * 70)
print(f"""
RETORNO DAS MISSOES — 23/03 a 02/04/2026

1. IMPACTO FINANCEIRO
   - GGR gerado pelos {total_users:,} jogadores de missao: R$ {player_ggr_miss:,.0f}
   - Isso representa {pct_player_miss:.1f}% do GGR total da plataforma no periodo
   - Media diaria GGR desses jogadores: R$ {miss['ggr']/miss['dias']:,.0f}/dia

2. COMPARATIVO ANTES vs DURANTE MISSOES (mesmos jogadores)
   - GGR/dia: R$ {ctrl['ggr']/ctrl['dias']:,.0f} (antes) -> R$ {miss['ggr']/miss['dias']:,.0f} (durante)
   - Depositos/dia: R$ {ctrl['deposits']/ctrl['dias']:,.0f} (antes) -> R$ {miss['deposits']/miss['dias']:,.0f} (durante)
   - Players ativos/dia: {players_ctrl_avg:,.0f} (antes) -> {players_miss_avg:,.0f} (durante)

3. ENGAJAMENTO
   - {total_users:,} jogadores fizeram opt-in nas missoes
   - {total_completed:,} completaram pelo menos a quest 1 ({100*total_completed/total_users:.0f}%)
   - Fortune Tiger foi o mais popular: {quest1_agg[quest1_agg['game_name_clean']=='Fortune Tiger']['users'].values[0]:,} opt-ins

4. DESTAQUES POR JOGO
   - Fortune Rabbit: maior GGR absoluto (puxado por volume de bets)
   - Fortune Dragon: consistente em GGR positivo
   - Fortune Tiger: maior base de jogadores

5. PONTOS DE ATENCAO
   - Alguns dias com GGR negativo (normal em casino — variancia)
   - Hold rate geral dos jogos de missao: {100*total_miss_ggr/total_miss_bets:.1f}%
   - Dados ate D-1 (02/04). Sweet Bonanza e Gates of Olympus ainda ativas.
""")

print("=" * 70)
print("  Analise gerada pelo Squad Intelligence Engine")
print("  Fonte: missions.* (Super Nova DB) + ps_bi (Athena)")
print("  Data: 2026-04-03")
print("=" * 70)
