"""
Cruzamento: Sharp Bettors Live 1.01-2.00 x Matriz de Risco
============================================================
Cruza os jogadores identificados como winners no Live odds baixas
com sua classificacao na matriz de risco (multibet.matriz_risco).
Fontes:
  - reports/sharp_bettors_live_1_2.csv (resultado da investigacao anterior)
  - multibet.matriz_risco (Super Nova DB, view sobre risk_tags)
  - multibet.risk_tags (para pegar tags individuais)
"""

import sys, os, logging
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import execute_supernova, get_supernova_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def main():
    log.info("=" * 70)
    log.info("CRUZAMENTO: Sharp Bettors x Matriz de Risco")
    log.info("=" * 70)

    # --- 1. Carregar sharp bettors do CSV ---
    df_sharp = pd.read_csv("reports/sharp_bettors_live_1_2.csv")
    log.info(f"Sharp bettors carregados: {len(df_sharp)} jogadores (min 10 bets)")

    # Separar winners
    winners = df_sharp[df_sharp["player_pnl"] > 0].copy()
    log.info(f"Winners (PnL > 0): {len(winners)} jogadores")

    # Pegar todos os player_ids (winners) para query
    winner_ids = winners["player_id"].astype(str).tolist()

    # --- 2. Buscar classificacao na matriz de risco ---
    log.info("Consultando multibet.matriz_risco no Super Nova DB...")

    # Query: pegar classificacao + score + tags para todos os winners
    # user_ext_id na matriz = c_customer_id (external_id) do sportsbook
    # Usar CAST para garantir match (alguns sao bigint, outros varchar)

    query_risk = """
    SELECT
        mr.user_ext_id,
        mr.classificacao,
        mr.score_bruto,
        mr.score_norm,
        mr.snapshot_date
    FROM multibet.matriz_risco mr
    WHERE mr.user_ext_id IN ({placeholders})
    """

    # Query tags individuais do jogador
    query_tags = """
    SELECT
        rt.user_ext_id,
        rt.label_id
    FROM multibet.risk_tags rt
    WHERE rt.snapshot_date = (SELECT MAX(snapshot_date) FROM multibet.risk_tags)
      AND rt.user_ext_id IN ({placeholders})
    ORDER BY rt.user_ext_id
    """

    # Executar queries
    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # Query classificacao
            placeholders = ",".join(["%s"] * len(winner_ids))
            sql_risk = query_risk.format(placeholders=placeholders)
            cur.execute(sql_risk, winner_ids)
            risk_rows = cur.fetchall()
            risk_cols = [desc[0] for desc in cur.description]

            # Query tags
            sql_tags = query_tags.format(placeholders=placeholders)
            cur.execute(sql_tags, winner_ids)
            tag_rows = cur.fetchall()
            tag_cols = [desc[0] for desc in cur.description]
    finally:
        conn.close()
        ssh.close()

    df_risk = pd.DataFrame(risk_rows, columns=risk_cols)
    df_tags = pd.DataFrame(tag_rows, columns=tag_cols)

    log.info(f"Encontrados na matriz de risco: {len(df_risk)} de {len(winner_ids)} winners")

    if df_risk.empty:
        log.warning("Nenhum winner encontrado na matriz de risco!")
        # Tentar com user_ext_id como string
        log.info("Tentando match alternativo...")
        return

    # --- 3. Consolidar tags por jogador ---
    if not df_tags.empty:
        tags_agg = df_tags.groupby("user_ext_id")["label_id"].apply(
            lambda x: ", ".join(sorted(x.unique()))
        ).reset_index()
        tags_agg.columns = ["user_ext_id", "risk_tags"]
    else:
        tags_agg = pd.DataFrame(columns=["user_ext_id", "risk_tags"])

    # --- 4. Merge: sharp bettors + risk classification + tags ---
    # Converter para mesmo tipo para join
    winners["player_id_str"] = winners["player_id"].astype(str)
    df_risk["user_ext_id_str"] = df_risk["user_ext_id"].astype(str)
    tags_agg["user_ext_id_str"] = tags_agg["user_ext_id"].astype(str)

    merged = winners.merge(
        df_risk[["user_ext_id_str", "classificacao", "score_bruto", "score_norm"]],
        left_on="player_id_str", right_on="user_ext_id_str", how="left"
    )
    merged = merged.merge(
        tags_agg[["user_ext_id_str", "risk_tags"]],
        left_on="player_id_str", right_on="user_ext_id_str", how="left"
    )

    # Flag: encontrado na matriz?
    merged["na_matriz"] = merged["classificacao"].notna()

    # --- 5. Output ---
    print("\n" + "=" * 140)
    print("SHARP BETTORS LIVE 1.01-2.00 x MATRIZ DE RISCO")
    print("=" * 140)

    # Estatisticas de match
    total_winners = len(merged)
    found = merged["na_matriz"].sum()
    not_found = total_winners - found
    print(f"\nMatch rate: {found}/{total_winners} winners encontrados na matriz ({found/total_winners*100:.1f}%)")
    print(f"Nao encontrados: {not_found} (podem ser SEM SCORE ou inativos >90d)")

    # Distribuicao por classificacao
    if found > 0:
        print("\n--- Distribuicao por Classificacao de Risco ---")
        class_dist = merged[merged["na_matriz"]].groupby("classificacao").agg(
            jogadores=("player_id", "count"),
            pnl_total=("player_pnl", "sum"),
            stake_total=("total_stake", "sum"),
            avg_win_rate=("player_win_rate", "mean"),
            avg_ticket=("avg_ticket", "mean"),
        ).sort_values("pnl_total", ascending=False)

        for tier, row in class_dist.iterrows():
            pct = row["jogadores"] / found * 100
            print(f"\n  {tier}:")
            print(f"    Jogadores: {int(row['jogadores'])} ({pct:.1f}%)")
            print(f"    PnL total: R$ {row['pnl_total']:,.2f}")
            print(f"    Stake total: R$ {row['stake_total']:,.2f}")
            print(f"    Win rate medio: {row['avg_win_rate']:.1f}%")
            print(f"    Ticket medio: R$ {row['avg_ticket']:,.2f}")

        # Top 20 detalhado
        print("\n" + "=" * 140)
        print("TOP 20 SHARP BETTORS — DETALHADO COM RISCO")
        print("=" * 140)

        top20 = merged.sort_values("player_pnl", ascending=False).head(20)
        cols = ["player_id", "total_bets", "player_win_rate", "player_pnl",
                "total_stake", "avg_ticket", "classificacao", "score_norm", "risk_tags",
                "first_bet", "last_bet"]
        # Formatar para display
        display_df = top20[cols].copy()
        display_df["player_pnl"] = display_df["player_pnl"].apply(lambda x: f"R$ {x:,.2f}")
        display_df["total_stake"] = display_df["total_stake"].apply(lambda x: f"R$ {x:,.2f}")
        display_df["avg_ticket"] = display_df["avg_ticket"].apply(lambda x: f"R$ {x:,.2f}")
        display_df["score_norm"] = display_df["score_norm"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
        )
        display_df["classificacao"] = display_df["classificacao"].fillna("FORA DA MATRIZ")
        display_df["risk_tags"] = display_df["risk_tags"].fillna("N/A")
        print(display_df.to_string(index=False))

        # Alerta: sharp bettors classificados como "Muito Bom" ou "Bom"
        bom_ou_muito_bom = merged[
            (merged["na_matriz"]) &
            (merged["classificacao"].isin(["Muito Bom", "Bom"])) &
            (merged["player_pnl"] > 5000)
        ].sort_values("player_pnl", ascending=False)

        if len(bom_ou_muito_bom) > 0:
            print("\n" + "=" * 140)
            print("ALERTA: SHARP BETTORS COM CLASSIFICACAO 'BOM' OU 'MUITO BOM' (PnL > R$ 5K)")
            print("Esses jogadores estao ganhando da casa mas a matriz os classifica positivamente!")
            print("=" * 140)
            for _, row in bom_ou_muito_bom.iterrows():
                print(f"  Player {int(row['player_id'])}: PnL R$ {row['player_pnl']:,.2f} | "
                      f"{int(row['total_bets'])} bets | Win rate {row['player_win_rate']:.1f}% | "
                      f"Classificacao: {row['classificacao']} (score {row['score_norm']:.1f}) | "
                      f"Tags: {row.get('risk_tags', 'N/A')}")
            print(f"\n  TOTAL: {len(bom_ou_muito_bom)} jogadores classificados Bom/Muito Bom "
                  f"com PnL > R$ 5K contra a casa no Live 1.01-2.00")

        # Jogadores FORA da matriz (SEM SCORE ou inativos)
        fora = merged[~merged["na_matriz"]].sort_values("player_pnl", ascending=False)
        if len(fora) > 0:
            print(f"\n--- {len(fora)} winners FORA da matriz de risco (SEM SCORE/inativos >90d) ---")
            print(f"PnL total desses jogadores: R$ {fora['player_pnl'].sum():,.2f}")
            top5_fora = fora.head(5)
            for _, row in top5_fora.iterrows():
                print(f"  Player {int(row['player_id'])}: PnL R$ {row['player_pnl']:,.2f} | "
                      f"{int(row['total_bets'])} bets")

    # --- 6. Salvar ---
    merged.to_csv("reports/sharp_bettors_vs_risk_matrix.csv", index=False)
    log.info("Salvo: reports/sharp_bettors_vs_risk_matrix.csv")

    print("\n" + "=" * 140)
    print("CONCLUSAO")
    print("=" * 140)
    print("1. Se sharp bettors estao como 'Bom/Muito Bom', a matriz NAO captura risco de sportsbook")
    print("2. Isso confirma o P1 pendente: 'Tags sportsbook (cobertura zero hoje)'")
    print("3. Recomendacao: criar tag SHARP_BETTOR_LIVE com score negativo (-20)")


if __name__ == "__main__":
    main()
