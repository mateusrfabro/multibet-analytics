"""
Analise de Retorno das Missoes — GGR para a casa
=================================================
Demanda: Head (Castrin) quer saber quanto de GGR as missoes estao gerando.
Fonte: missions.event_log + missions.user_progress (Super Nova DB)
       + ps_bi.fct_casino_activity_daily / fct_player_activity_daily (Athena)

Autor: Squad Intelligence Engine
Data: 2026-04-03
"""
import sys, os, csv
sys.path.insert(0, ".")

import pandas as pd
from db.supernova import execute_supernova
from db.athena import query_athena

OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# STEP 1: Extrair dados de missoes do Super Nova DB
# =============================================================================
print("=" * 60)
print("STEP 1: Extraindo missoes do Super Nova DB")
print("=" * 60)

# Missoes de producao (excluir testes)
EXCLUDE_PATTERNS = (
    "test-%", "teste-%", "xxxxxxxxxxxxxxxxxxx%", "one-by-one%",
    "single-active%", "lightning-%", "2aec18e9%", "dia%-jornada%"
)
exclude_sql = " AND ".join([f"mission_code NOT LIKE '{p}'" for p in EXCLUDE_PATTERNS])

# 1a. Resumo por missao
print("\n[1a] Resumo por missao...")
mission_summary = execute_supernova(f"""
    SELECT
        mission_code,
        game_path,
        COUNT(DISTINCT user_id) as total_users,
        SUM(CASE WHEN current_value >= value_target THEN 1 ELSE 0 END) as completaram,
        COUNT(*) as total_entries,
        MIN(accepted_at) as primeiro_optin,
        MAX(accepted_at) as ultimo_optin
    FROM missions.user_progress
    WHERE {exclude_sql}
      AND mission_code IS NOT NULL
      AND game_path IS NOT NULL
      AND game_path != 'daily-minigame-local'
    GROUP BY mission_code, game_path
    ORDER BY mission_code
""", fetch=True)

print(f"  {len(mission_summary)} missoes encontradas")

# Mapa de nomes
game_name_map = {}
for row in mission_summary:
    mc, gp = row[0], row[1]
    if gp and gp != '0':
        name = mc.replace("challenge-", "").split("-quest")[0].replace("-", " ").title()
        game_name_map[gp] = name

# 1b. Lista de user_ids unicos
print("\n[1b] Extraindo user_ids unicos...")
mission_users = execute_supernova(f"""
    SELECT DISTINCT user_id, MIN(accepted_at) as first_optin
    FROM missions.user_progress
    WHERE {exclude_sql}
      AND mission_code IS NOT NULL
      AND game_path IS NOT NULL
      AND game_path != 'daily-minigame-local'
      AND accepted_at IS NOT NULL
    GROUP BY user_id
""", fetch=True)

print(f"  {len(mission_users)} jogadores unicos")

short_ids = [r for r in mission_users if len(str(r[0])) <= 10]
long_ids  = [r for r in mission_users if len(str(r[0])) > 10]
print(f"  IDs curtos (<=10 dig): {len(short_ids)}")
print(f"  IDs longos (>10 dig):  {len(long_ids)}")

# Salvar user_ids
with open(f"{OUTPUT_DIR}/mission_users_raw.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["user_id", "first_optin", "id_length"])
    for uid, dt in mission_users:
        w.writerow([uid, dt, len(str(uid))])

# =============================================================================
# STEP 2: Identificar formato do user_id no Athena
# =============================================================================
print("\n" + "=" * 60)
print("STEP 2: Testando formato do user_id contra dim_user (Athena)")
print("=" * 60)

sample_short = [str(r[0]) for r in short_ids[:5]]
sample_long  = [str(r[0]) for r in long_ids[:5]]

# external_id e BIGINT no Athena — usar numeros sem aspas
all_numeric = sample_short + sample_long
in_clause_num = ",".join(all_numeric)

# Teste 1: external_id (BIGINT)
print(f"\n  Teste 1: user_id = external_id (BIGINT)?")
print(f"  Amostra: {all_numeric}")
try:
    df_ext = query_athena(f"""
        SELECT external_id, ecr_id
        FROM ps_bi.dim_user
        WHERE external_id IN ({in_clause_num})
    """, database="ps_bi")
    print(f"  Match external_id: {len(df_ext)} resultados")
    if not df_ext.empty:
        print(df_ext.head().to_string(index=False))
    match_external = not df_ext.empty
except Exception as e:
    print(f"  ERRO: {e}")
    match_external = False

# Teste 2: ecr_id (BIGINT)
print(f"\n  Teste 2: user_id = ecr_id (BIGINT)?")
try:
    df_ecr = query_athena(f"""
        SELECT external_id, ecr_id
        FROM ps_bi.dim_user
        WHERE ecr_id IN ({in_clause_num})
    """, database="ps_bi")
    print(f"  Match ecr_id: {len(df_ecr)} resultados")
    if not df_ecr.empty:
        print(df_ecr.head().to_string(index=False))
    match_ecr = not df_ecr.empty
except Exception as e:
    print(f"  ERRO: {e}")
    match_ecr = False

# Determinar coluna de join
if match_external and not match_ecr:
    join_col = "external_id"
elif match_ecr and not match_external:
    join_col = "ecr_id"
elif match_external and match_ecr:
    join_col = "external_id"  # preferivel
else:
    join_col = "unknown"

print(f"\n  >>> Coluna de join: {join_col}")

# =============================================================================
# STEP 3: GGR por jogo das missoes — Athena
# =============================================================================
print("\n" + "=" * 60)
print("STEP 3: GGR por jogo das missoes (Athena)")
print("=" * 60)

mission_games = set()
for row in mission_summary:
    gp = row[1]
    if gp and gp != '0':
        mission_games.add(gp)

MISSION_START = "2026-03-23"
MISSION_END   = "2026-04-02"  # D-1
CONTROL_START = "2026-03-13"
CONTROL_END   = "2026-03-22"

# 3a. GGR total diario da plataforma
print("\n[3a] GGR diario TOTAL da plataforma...")
try:
    df_total = query_athena(f"""
        SELECT
            activity_date,
            SUM(ggr_base) as total_ggr,
            SUM(real_bet_amount_base) as total_bets,
            SUM(real_win_amount_base) as total_wins,
            COUNT(DISTINCT player_id) as unique_players,
            SUM(bet_count) as total_rounds
        FROM ps_bi.fct_casino_activity_daily
        WHERE activity_date >= DATE '{CONTROL_START}'
          AND activity_date <= DATE '{MISSION_END}'
        GROUP BY activity_date
        ORDER BY activity_date
    """, database="ps_bi")
    print(f"  {len(df_total)} dias retornados")
    df_total.to_csv(f"{OUTPUT_DIR}/platform_ggr_daily.csv", index=False)
except Exception as e:
    print(f"  ERRO: {e}")
    df_total = pd.DataFrame()

# 3b. Verificar formato de game_id
print("\n[3b] Amostra game_id no fct_casino_activity_daily...")
try:
    df_gid = query_athena("""
        SELECT DISTINCT game_id
        FROM ps_bi.fct_casino_activity_daily
        WHERE activity_date = DATE '2026-04-01'
        LIMIT 30
    """, database="ps_bi")
    print(f"  Game IDs amostra: {df_gid['game_id'].tolist()[:15]}")
except Exception as e:
    print(f"  ERRO: {e}")

# 3c. GGR dos jogos de missao
# Tentar match com game_id como VARCHAR e como numerico
game_list = list(mission_games)
game_in_str = ",".join([f"'{g}'" for g in game_list])

print(f"\n[3c] GGR dos jogos de missao (game_id string match)...")
try:
    df_games = query_athena(f"""
        SELECT
            activity_date,
            game_id,
            SUM(ggr_base) as ggr,
            SUM(real_bet_amount_base) as bets,
            SUM(real_win_amount_base) as wins,
            COUNT(DISTINCT player_id) as players,
            SUM(bet_count) as rounds
        FROM ps_bi.fct_casino_activity_daily
        WHERE activity_date >= DATE '{CONTROL_START}'
          AND activity_date <= DATE '{MISSION_END}'
          AND CAST(game_id AS VARCHAR) IN ({game_in_str})
        GROUP BY activity_date, game_id
        ORDER BY activity_date, game_id
    """, database="ps_bi")
    print(f"  {len(df_games)} registros (dia x jogo)")
    if not df_games.empty:
        # Adicionar nome do jogo
        df_games['game_name'] = df_games['game_id'].astype(str).map(game_name_map).fillna('Unknown')
        df_games.to_csv(f"{OUTPUT_DIR}/mission_games_ggr_daily.csv", index=False)
        print(f"  Jogos encontrados: {df_games['game_id'].unique().tolist()}")
    else:
        print("  Nenhum resultado com game_id direto.")
except Exception as e:
    print(f"  ERRO: {e}")
    df_games = pd.DataFrame()

# Se nao encontrou, tentar sub_vendor_id
if df_games.empty:
    print("\n  Tentando via sub_vendor_id...")
    try:
        df_games = query_athena(f"""
            SELECT
                activity_date,
                sub_vendor_id as game_id,
                SUM(ggr_base) as ggr,
                SUM(real_bet_amount_base) as bets,
                SUM(real_win_amount_base) as wins,
                COUNT(DISTINCT player_id) as players,
                SUM(bet_count) as rounds
            FROM ps_bi.fct_casino_activity_daily
            WHERE activity_date >= DATE '{CONTROL_START}'
              AND activity_date <= DATE '{MISSION_END}'
              AND CAST(sub_vendor_id AS VARCHAR) IN ({game_in_str})
            GROUP BY activity_date, sub_vendor_id
            ORDER BY activity_date, sub_vendor_id
        """, database="ps_bi")
        print(f"  Via sub_vendor_id: {len(df_games)} registros")
        if not df_games.empty:
            df_games['game_name'] = df_games['game_id'].astype(str).map(game_name_map).fillna('Unknown')
            df_games.to_csv(f"{OUTPUT_DIR}/mission_games_ggr_daily.csv", index=False)
    except Exception as e:
        print(f"  ERRO sub_vendor: {e}")

# =============================================================================
# STEP 4: GGR dos jogadores de missao
# =============================================================================
print("\n" + "=" * 60)
print("STEP 4: GGR dos jogadores de missao (player-level)")
print("=" * 60)

if join_col != "unknown":
    all_user_ids = [str(r[0]) for r in mission_users]
    BATCH_SIZE = 500
    batches = [all_user_ids[i:i+BATCH_SIZE] for i in range(0, len(all_user_ids), BATCH_SIZE)]
    print(f"  {len(all_user_ids)} users em {len(batches)} batches")

    # 4a. Mapear user_ids -> ecr_ids
    print(f"\n[4a] Mapeando user_ids -> ecr_ids via dim_user.{join_col}...")
    ecr_map = {}
    for i, batch in enumerate(batches):
        in_cl = ",".join(batch)  # sem aspas, BIGINT
        try:
            df_map = query_athena(f"""
                SELECT CAST({join_col} AS VARCHAR) as uid, CAST(ecr_id AS VARCHAR) as eid
                FROM ps_bi.dim_user
                WHERE {join_col} IN ({in_cl})
            """, database="ps_bi")
            for _, row in df_map.iterrows():
                ecr_map[str(row['uid'])] = str(row['eid'])
            print(f"    Batch {i+1}/{len(batches)}: {len(df_map)} matches")
        except Exception as e:
            print(f"    Batch {i+1} ERRO: {e}")

    match_pct = 100 * len(ecr_map) / max(len(all_user_ids), 1)
    print(f"\n  Mapeados: {len(ecr_map)} de {len(all_user_ids)} ({match_pct:.1f}%)")

    # Salvar mapeamento
    with open(f"{OUTPUT_DIR}/mission_users_mapped.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "ecr_id", "first_optin"])
        for uid, dt in mission_users:
            ecr = ecr_map.get(str(uid), "NOT_FOUND")
            w.writerow([uid, ecr, dt])

    # 4b. GGR diario dos jogadores de missao
    ecr_ids = list(ecr_map.values())
    if ecr_ids:
        ecr_batches = [ecr_ids[i:i+BATCH_SIZE] for i in range(0, len(ecr_ids), BATCH_SIZE)]
        print(f"\n[4b] GGR diario dos {len(ecr_ids)} jogadores de missao...")

        all_dfs = []
        for i, batch in enumerate(ecr_batches):
            in_cl = ",".join(batch)  # ecr_id numerico
            try:
                df_b = query_athena(f"""
                    SELECT
                        activity_date,
                        SUM(ggr_base) as ggr,
                        SUM(deposit_success_base) as deposits,
                        SUM(casino_realbet_base) as casino_bets,
                        SUM(casino_real_win_base) as casino_wins,
                        COUNT(DISTINCT player_id) as active_players,
                        SUM(casino_realbet_count) as rounds
                    FROM ps_bi.fct_player_activity_daily
                    WHERE activity_date >= DATE '{CONTROL_START}'
                      AND activity_date <= DATE '{MISSION_END}'
                      AND player_id IN ({in_cl})
                    GROUP BY activity_date
                """, database="ps_bi")
                all_dfs.append(df_b)
                print(f"    Batch {i+1}/{len(ecr_batches)}: {len(df_b)} dias")
            except Exception as e:
                print(f"    Batch {i+1} ERRO: {e}")

        if all_dfs:
            df_player = pd.concat(all_dfs, ignore_index=True)
            # Re-agregar por dia (somar batches)
            df_player = df_player.groupby('activity_date').agg({
                'ggr': 'sum',
                'deposits': 'sum',
                'casino_bets': 'sum',
                'casino_wins': 'sum',
                'active_players': 'sum',
                'rounds': 'sum'
            }).reset_index().sort_values('activity_date')

            print(f"\n  GGR diario dos jogadores de missao:")
            print(f"  {'Data':<12} {'GGR (BRL)':>14} {'Depositos':>14} {'Bets':>14} {'Players':>8} {'Rounds':>10}")
            for _, r in df_player.iterrows():
                print(f"  {str(r['activity_date']):<12} R${r['ggr']:>12,.2f} R${r['deposits']:>12,.2f} R${r['casino_bets']:>12,.2f} {int(r['active_players']):>8,} {int(r['rounds']):>10,}")

            df_player.to_csv(f"{OUTPUT_DIR}/mission_players_ggr_daily.csv", index=False)

            # Resumo: controle vs missao
            df_player['activity_date'] = pd.to_datetime(df_player['activity_date'])
            mask_mission = df_player['activity_date'] >= pd.Timestamp(MISSION_START)
            mask_control = df_player['activity_date'] < pd.Timestamp(MISSION_START)

            if mask_control.any():
                ggr_control = df_player.loc[mask_control, 'ggr'].sum()
                days_control = mask_control.sum()
                avg_control = ggr_control / days_control if days_control else 0
            else:
                ggr_control = 0
                avg_control = 0

            if mask_mission.any():
                ggr_mission = df_player.loc[mask_mission, 'ggr'].sum()
                days_mission = mask_mission.sum()
                avg_mission = ggr_mission / days_mission if days_mission else 0
            else:
                ggr_mission = 0
                avg_mission = 0

            print(f"\n  === COMPARATIVO ===")
            print(f"  Periodo Controle ({CONTROL_START} a {CONTROL_END}):")
            print(f"    GGR total: R$ {ggr_control:,.2f} | Media/dia: R$ {avg_control:,.2f}")
            print(f"  Periodo Missoes ({MISSION_START} a {MISSION_END}):")
            print(f"    GGR total: R$ {ggr_mission:,.2f} | Media/dia: R$ {avg_mission:,.2f}")
            if avg_control > 0:
                uplift = ((avg_mission - avg_control) / avg_control) * 100
                print(f"  Uplift GGR/dia: {uplift:+.1f}%")
    else:
        print("  Nenhum ecr_id mapeado.")
else:
    print("  Formato user_id desconhecido. Pulando player-level.")

# =============================================================================
# STEP 5: Resumo das missoes
# =============================================================================
print("\n" + "=" * 60)
print("STEP 5: Salvando resumo das missoes")
print("=" * 60)

with open(f"{OUTPUT_DIR}/missions_summary.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["mission_code", "game_path", "game_name", "total_users", "completaram",
                "total_entries", "primeiro_optin", "ultimo_optin"])
    for row in mission_summary:
        name = game_name_map.get(row[1], row[1])
        w.writerow([row[0], row[1], name, row[2], row[3], row[4], row[5], row[6]])

print(f"  Arquivos salvos em {OUTPUT_DIR}/")
print("\nEXTRACAO COMPLETA!")
