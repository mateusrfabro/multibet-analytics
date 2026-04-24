"""
FRAUDE "Recarregue e Ganhe Fortune Tiger" — Investigacao 21/04/2026 (D-0)

Fluxo:
  PARTE 1  — identifica c_bonus_id das campanhas emitidas hoje + cruza com
             tbl_bonus_profile pra achar nome/descricao (filtro LIKE
             "recarregue", "fortune tiger", "FT", "reload").
  PARTE 2  — monta cohort de beneficiados + classifica gameplay (FT vs
             Mines vs Outros) + detecta rollbacks.
  PARTE 3  — gera CSV + _legenda.txt + HTML executivo.

Regras validadas (memory/MEMORY.md, feedback_athena_sql_rules.md):
  - Valor: c_amount_in_ecr_ccy / 100.0 (centavos BRL)
  - Status: 'SUCCESS' em fund_ec2 (NAO 'txn_confirmed_success')
  - Sem SELECT * em producao; CTE ao inves de TEMP TABLE
  - Timezone: AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
  - is_test = false (ps_bi); c_test_user = false (ecr_ec2)
  - c_product_id = 'CASINO' obrigatorio em fund_ec2
  - c_actual_issued_amount em bonus_ec2 (NAO c_total_bonus_offered)

ACAO DO AUDITOR antes de executar:
  1. Confirmar se a janela BRT 2026-04-21 00:00 → 2026-04-22 00:00 esta correta.
  2. Revisar a Parte 1 (identificacao de campanha) — filtro LIKE pode
     matchear campanha errada.
  3. Sign-off antes de push pra stakeholder (Castrin).

Autor: extractor | squad Intelligence Engine
"""
from __future__ import annotations

import os
import sys
import csv
from datetime import datetime
from pathlib import Path

import pandas as pd

# Garantir import do db/athena.py
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db.athena import query_athena  # noqa: E402

# ----------------------------------------------------------------------------
# CONSTANTES
# ----------------------------------------------------------------------------
DATA_REF_BRT = "2026-04-21"
UTC_START = "2026-04-21 03:00:00"  # 21/04 00:00 BRT
UTC_END   = "2026-04-22 03:00:00"  # 22/04 00:00 BRT

# Termos pra filtrar nome da campanha na tbl_bonus_profile
TERMOS_CAMPANHA = ["recarregue", "fortune tiger", "reload", " ft ", "ft_", "_ft"]

# Fortune Tiger (memory/feedback_smartico_game_ids.md)
FORTUNE_TIGER_GAME_ID = "45838245"

OUT_DIR = ROOT / "reports" / f"fraude_recarregue_ganhe_ft_{DATA_REF_BRT.replace('-', '')}"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_PATH      = OUT_DIR / f"cohort_fraude_FT_{DATA_REF_BRT}.csv"
LEGENDA_PATH  = OUT_DIR / f"cohort_fraude_FT_{DATA_REF_BRT}_legenda.txt"
HTML_PATH     = OUT_DIR / f"report_exec_fraude_FT_{DATA_REF_BRT}_FINAL.html"


# ============================================================================
# PARTE 1 — IDENTIFICAR CAMPANHA
# ============================================================================
def identificar_campanhas_hoje() -> pd.DataFrame:
    """Retorna DataFrame com c_bonus_id candidatos + nome vindo de tbl_bonus_profile."""
    print("\n[PARTE 1] Listando bonus emitidos hoje (BRT 21/04)...")

    sql_bonus_hoje = f"""
    -- Bonus emitidos hoje (21/04 BRT) — agregado por c_bonus_id
    SELECT
        c_bonus_id,
        COUNT(*) AS qtd_emissoes_hoje,
        COUNT(DISTINCT c_ecr_id) AS jogadores_distintos,
        ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_emitido_brl,
        ROUND(AVG(c_actual_issued_amount / 100.0), 2) AS ticket_medio_brl
    FROM bonus_ec2.tbl_bonus_summary_details
    WHERE c_issue_date >= TIMESTAMP '{UTC_START}'
      AND c_issue_date <  TIMESTAMP '{UTC_END}'
    GROUP BY c_bonus_id
    ORDER BY total_emitido_brl DESC
    LIMIT 100
    """
    df_bonus = query_athena(sql_bonus_hoje, database="bonus_ec2")
    print(f"  {len(df_bonus)} campanhas emitiram bonus hoje.")

    if df_bonus.empty:
        print("  [VAZIO] Nenhum bonus emitido hoje. Investigar janela/credenciais.")
        return df_bonus

    # Cruzar com tbl_bonus_profile pra pegar nome/descricao
    bonus_ids = [str(int(x)) for x in df_bonus["c_bonus_id"].tolist() if pd.notna(x)]
    ids_sql = ",".join(bonus_ids)

    sql_profile = f"""
    -- Perfil/descricao das campanhas (pra filtrar por nome)
    SELECT *
    FROM bonus_ec2.tbl_bonus_profile
    WHERE c_bonus_id IN ({ids_sql})
    """
    try:
        df_profile = query_athena(sql_profile, database="bonus_ec2")
        print(f"  tbl_bonus_profile retornou {len(df_profile)} linhas.")
        print(f"  Colunas tbl_bonus_profile: {list(df_profile.columns)}")
    except Exception as e:
        print(f"  [AVISO] tbl_bonus_profile falhou: {e}")
        df_profile = pd.DataFrame()

    # Merge
    if not df_profile.empty:
        df_merged = df_bonus.merge(df_profile, on="c_bonus_id", how="left")
    else:
        df_merged = df_bonus.copy()

    # Filtrar campanhas que batem com "Recarregue e Ganhe FT"
    # Busca em TODAS as colunas textuais pra nao perder (nao sabemos nome exato da coluna de descricao)
    def match_campanha(row) -> bool:
        texto = " ".join(str(v) for v in row.values if isinstance(v, str)).lower()
        return any(termo.lower() in texto for termo in TERMOS_CAMPANHA)

    if not df_profile.empty:
        df_merged["match_termo"] = df_merged.apply(match_campanha, axis=1)
        candidatas = df_merged[df_merged["match_termo"]]
    else:
        candidatas = df_merged  # sem profile, retorna todos pra auditor decidir

    print(f"\n  Campanhas candidatas (LIKE Recarregue/FT/Reload): {len(candidatas)}")
    if not candidatas.empty:
        print(candidatas[["c_bonus_id", "qtd_emissoes_hoje", "total_emitido_brl"]].to_string(index=False))
    return df_merged


# ============================================================================
# PARTE 2 — COHORT + GAMEPLAY + ROLLBACK
# ============================================================================
def montar_cohort(bonus_ids: list[int]) -> pd.DataFrame:
    """Roda query consolidada (cohort + gameplay + rollback)."""
    if not bonus_ids:
        print("  [AVISO] Lista vazia de c_bonus_id — abortando Parte 2.")
        return pd.DataFrame()

    ids_sql = ",".join(str(int(x)) for x in bonus_ids)
    print(f"\n[PARTE 2] Rodando cohort para bonus_ids: {ids_sql}")

    sql = f"""
    WITH cohort_bonus AS (
        SELECT
            bsd.c_ecr_id,
            bsd.c_bonus_id,
            bsd.c_ecr_bonus_id,
            MIN(bsd.c_actual_issued_amount / 100.0) AS bonus_issued_amount_brl,
            MIN(bsd.c_issue_date) AS bonus_issued_at_utc
        FROM bonus_ec2.tbl_bonus_summary_details bsd
        WHERE bsd.c_issue_date >= TIMESTAMP '{UTC_START}'
          AND bsd.c_issue_date <  TIMESTAMP '{UTC_END}'
          AND bsd.c_bonus_id IN ({ids_sql})
        GROUP BY bsd.c_ecr_id, bsd.c_bonus_id, bsd.c_ecr_bonus_id
    ),
    cohort_perfil AS (
        SELECT
            du.ecr_id,
            du.external_id,
            du.c_email_id AS email,
            ep.c_mobile_number AS mobile_number,
            du.is_test
        FROM ps_bi.dim_user du
        LEFT JOIN ecr_ec2.tbl_ecr_profile ep ON ep.c_ecr_id = du.ecr_id
        WHERE du.ecr_id IN (SELECT c_ecr_id FROM cohort_bonus)
          AND du.is_test = false
    ),
    txn_casino_hoje AS (
        SELECT
            f.c_ecr_id,
            f.c_game_id,
            f.c_txn_type,
            f.c_amount_in_ecr_ccy / 100.0 AS valor_brl,
            f.c_start_time
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_start_time >= TIMESTAMP '{UTC_START}'
          AND f.c_start_time <  TIMESTAMP '{UTC_END}'
          AND f.c_product_id = 'CASINO'
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_txn_type IN (27, 72)
          AND f.c_ecr_id IN (SELECT c_ecr_id FROM cohort_bonus)
    ),
    jogos AS (
        SELECT
            CAST(c_game_id AS VARCHAR) AS c_game_id,
            c_game_desc,
            c_sub_vendor_id
        FROM bireports_ec2.tbl_vendor_games_mapping_data
    ),
    txn_classificada AS (
        SELECT
            t.c_ecr_id,
            t.c_txn_type,
            t.valor_brl,
            CASE
                WHEN t.c_game_id = '{FORTUNE_TIGER_GAME_ID}'
                  OR LOWER(COALESCE(j.c_game_desc, '')) LIKE '%fortune tiger%'
                    THEN 'FORTUNE_TIGER'
                WHEN LOWER(COALESCE(j.c_game_desc, '')) LIKE '%mines%'
                    THEN 'MINES'
                ELSE 'OUTROS'
            END AS bucket_jogo
        FROM txn_casino_hoje t
        LEFT JOIN jogos j ON j.c_game_id = t.c_game_id
    ),
    agg_jogador AS (
        SELECT
            c_ecr_id,
            COUNT_IF(c_txn_type = 27 AND bucket_jogo = 'FORTUNE_TIGER') AS rounds_fortune_tiger,
            COALESCE(SUM(CASE WHEN c_txn_type = 27 AND bucket_jogo = 'FORTUNE_TIGER' THEN valor_brl END), 0) AS stake_ft_brl,
            COUNT_IF(c_txn_type = 27 AND bucket_jogo = 'MINES') AS rounds_mines,
            COALESCE(SUM(CASE WHEN c_txn_type = 27 AND bucket_jogo = 'MINES' THEN valor_brl END), 0) AS stake_mines_brl,
            COUNT_IF(c_txn_type = 27 AND bucket_jogo = 'OUTROS') AS rounds_outros,
            COALESCE(SUM(CASE WHEN c_txn_type = 27 AND bucket_jogo = 'OUTROS' THEN valor_brl END), 0) AS stake_outros_brl,
            COUNT_IF(c_txn_type = 72) AS rollback_count_24h,
            COALESCE(SUM(CASE WHEN c_txn_type = 72 THEN valor_brl END), 0) AS rollback_amount_brl,
            COUNT_IF(c_txn_type = 72 AND bucket_jogo = 'MINES') AS rollback_count_mines
        FROM txn_classificada
        GROUP BY c_ecr_id
    )
    SELECT
        cp.ecr_id,
        cp.external_id,
        cp.email,
        cp.mobile_number,
        CAST(cb.c_bonus_id AS VARCHAR) AS bonus_template_id,
        ROUND(cb.bonus_issued_amount_brl, 2) AS bonus_issued_amount_brl,
        cb.bonus_issued_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS bonus_issued_at_brt,
        COALESCE(aj.rounds_fortune_tiger, 0) AS rounds_fortune_tiger,
        ROUND(COALESCE(aj.stake_ft_brl, 0), 2) AS stake_ft_brl,
        COALESCE(aj.rounds_mines, 0) AS rounds_mines,
        ROUND(COALESCE(aj.stake_mines_brl, 0), 2) AS stake_mines_brl,
        COALESCE(aj.rounds_outros, 0) AS rounds_outros,
        ROUND(COALESCE(aj.stake_outros_brl, 0), 2) AS stake_outros_brl,
        COALESCE(aj.rollback_count_24h, 0) AS rollback_count_24h,
        ROUND(COALESCE(aj.rollback_amount_brl, 0), 2) AS rollback_amount_brl,
        COALESCE(aj.rollback_count_mines, 0) AS rollback_count_mines,
        CASE
            WHEN COALESCE(aj.stake_mines_brl, 0) > COALESCE(aj.stake_ft_brl, 0)
              OR COALESCE(aj.rollback_count_24h, 0) > 0
                THEN TRUE
            ELSE FALSE
        END AS flag_suspeito,
        SUBSTR(cp.mobile_number, 1, 7) AS mobile_prefix_7
    FROM cohort_bonus cb
    JOIN cohort_perfil cp ON cp.ecr_id = cb.c_ecr_id
    LEFT JOIN agg_jogador aj ON aj.c_ecr_id = cb.c_ecr_id
    ORDER BY flag_suspeito DESC, stake_mines_brl DESC
    """
    df = query_athena(sql, database="bonus_ec2")
    print(f"  Cohort: {len(df)} jogadores.")
    return df


# ============================================================================
# PARTE 3 — DETECCAO DE CLUSTER (pos-processamento Python)
# ============================================================================
def detectar_clusters(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona coluna cluster_size (contas no mesmo prefixo mobile)."""
    if df.empty or "mobile_prefix_7" not in df.columns:
        return df
    cluster_map = df.groupby("mobile_prefix_7").size().to_dict()
    df["cluster_size"] = df["mobile_prefix_7"].map(cluster_map).fillna(1).astype(int)
    df["cluster_alto_risco"] = df["cluster_size"] >= 5
    return df


# ============================================================================
# SAIDA: CSV + LEGENDA + HTML
# ============================================================================
LEGENDA_TEXT = """\
LEGENDA — Cohort Fraude "Recarregue e Ganhe Fortune Tiger" (21/04/2026)
========================================================================

FONTE
  - bonus_ec2.tbl_bonus_summary_details   (bonus emitidos, valor em centavos)
  - bonus_ec2.tbl_bonus_profile           (config da campanha)
  - ps_bi.dim_user                        (email, is_test, external_id)
  - ecr_ec2.tbl_ecr_profile               (mobile_number)
  - fund_ec2.tbl_real_fund_txn            (apostas/rollbacks casino, centavos)
  - bireports_ec2.tbl_vendor_games_mapping_data (catalogo jogos — Fortune Tiger, Mines)

JANELA
  BRT 2026-04-21 00:00:00 → 2026-04-22 00:00:00
  UTC 2026-04-21 03:00:00 → 2026-04-22 03:00:00

COLUNAS
  ecr_id                   ID interno do jogador (18 digitos)
  external_id              ID externo = user_ext_id no Smartico (15 digitos)
  email                    Email do cadastro
  mobile_number            Telefone (para deteccao de cluster)
  bonus_template_id        c_bonus_id da campanha
  bonus_issued_amount_brl  Valor do bonus emitido (BRL)
  bonus_issued_at_brt      Timestamp da emissao (BRT)

  rounds_fortune_tiger     # de apostas no Fortune Tiger (c_txn_type=27)
  stake_ft_brl             Soma das apostas no Fortune Tiger (BRL)

  rounds_mines             # de apostas em Mines (qualquer provider)
  stake_mines_brl          Soma das apostas em Mines (BRL)

  rounds_outros            # de apostas em outros jogos casino
  stake_outros_brl         Soma das apostas em outros jogos (BRL)

  rollback_count_24h       # de rollbacks (c_txn_type=72) no dia
  rollback_amount_brl      Valor total estornado (BRL)
  rollback_count_mines     # de rollbacks concentrados em Mines

  flag_suspeito            TRUE se stake_mines > stake_ft OU rollback_count > 0
  mobile_prefix_7          Primeiros 7 digitos do celular (p/ cluster)
  cluster_size             Qtd contas com mesmo prefixo mobile
  cluster_alto_risco       TRUE se cluster_size >= 5 (bonus farming coordenado)

GLOSSARIO
  Rollback: estorno de aposta cassino (c_txn_type = 72 = CASINO_BUYIN_CANCEL).
           Legitimo em casos de erro/timeout; padrao de fraude quando
           concentrado em 1 jogo, muitos eventos em pouco tempo, ou 1 player.

  Stake: valor apostado (buyin = c_txn_type 27).

  Bonus farming: multiplas contas criadas pra explorar a mesma promo.
                 Detectavel via cluster de mobile sequenciais.

ACAO SUGERIDA
  flag_suspeito=TRUE + cluster_alto_risco=TRUE
    -> BLOQUEIO IMEDIATO + investigacao manual + reporte risco/fraude

  flag_suspeito=TRUE + cluster_alto_risco=FALSE
    -> monitorar 48h + validar KYC + segunda opiniao CRM

  flag_suspeito=FALSE
    -> comportamento dentro do esperado

NIVEL ALERTA GERAL
  ALTO RISCO se:
    * Rollback ratio (rollback_count_24h > 0) > 10% do cohort
    * OU stake_mines_medio > 3x stake_ft_medio
    * OU >=1 cluster com cluster_size >= 5

REGRAS ATHENA VALIDADAS EMPIRICAMENTE (31/03/2026, 20/03/2026):
  - fund_ec2 valor: c_amount_in_ecr_ccy / 100.0 (centavos BRL)
  - fund_ec2 status: 'SUCCESS' (NAO 'txn_confirmed_success')
  - c_confirmed_amount_in_inhouse_ccy NAO EXISTE
  - bonus_ec2 valor: c_actual_issued_amount (c_total_bonus_offered NAO EXISTE)
  - c_product_id = 'CASINO' obrigatorio
  - Timezone: AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
"""


def salvar_csv(df: pd.DataFrame) -> None:
    df.to_csv(CSV_PATH, index=False, quoting=csv.QUOTE_MINIMAL, encoding="utf-8-sig")
    LEGENDA_PATH.write_text(LEGENDA_TEXT, encoding="utf-8")
    print(f"\n[SAIDA] CSV:     {CSV_PATH}")
    print(f"[SAIDA] Legenda: {LEGENDA_PATH}")


def gerar_html(df: pd.DataFrame) -> None:
    """HTML executivo em linguagem de negocio (memory/feedback_relatorio_linguagem_executiva.md)."""
    if df.empty:
        resumo_exec = "Nenhum jogador no cohort. Verificar se a campanha esta ativa ou se os filtros estao corretos."
        tabela_html = "<p><em>Sem dados.</em></p>"
        metricas = {}
    else:
        total = len(df)
        suspeitos = int(df["flag_suspeito"].sum())
        rollback_players = int((df["rollback_count_24h"] > 0).sum())
        stake_mines_total = float(df["stake_mines_brl"].sum())
        stake_ft_total = float(df["stake_ft_brl"].sum())
        stake_mines_medio = stake_mines_total / max(total, 1)
        stake_ft_medio = stake_ft_total / max(total, 1)
        ratio_mines_ft = (stake_mines_total / stake_ft_total) if stake_ft_total > 0 else float("inf")
        cluster_risco = 0
        if "cluster_alto_risco" in df.columns:
            cluster_risco = int(df["cluster_alto_risco"].sum())

        # Avaliar nivel de alerta
        alerta_alto = (
            (rollback_players / max(total, 1)) > 0.10
            or stake_mines_medio > 3 * stake_ft_medio
            or cluster_risco > 0
        )
        alerta_txt = "ALTO RISCO" if alerta_alto else "MODERADO"
        cor_alerta = "#b91c1c" if alerta_alto else "#b45309"

        resumo_exec = (
            f"Cohort de <strong>{total}</strong> jogadores recebeu o bonus hoje. "
            f"<strong>{suspeitos}</strong> apresentam sinais suspeitos "
            f"({suspeitos / max(total,1) * 100:.1f}% do cohort). "
            f"<strong>{rollback_players}</strong> jogadores executaram rollback. "
            f"Ratio Mines/FT = <strong>{ratio_mines_ft:.2f}x</strong>. "
            f"Clusters mobile >=5 contas: <strong>{cluster_risco}</strong>."
        )
        metricas = {
            "total": total,
            "suspeitos": suspeitos,
            "rollback_players": rollback_players,
            "stake_mines_total": stake_mines_total,
            "stake_ft_total": stake_ft_total,
            "ratio_mines_ft": ratio_mines_ft,
            "cluster_risco": cluster_risco,
            "alerta_txt": alerta_txt,
            "cor_alerta": cor_alerta,
        }

        # Top 30 suspeitos
        cols_show = [
            "ecr_id", "external_id", "email", "mobile_number",
            "bonus_issued_amount_brl",
            "rounds_fortune_tiger", "stake_ft_brl",
            "rounds_mines", "stake_mines_brl",
            "rollback_count_24h", "rollback_amount_brl",
            "flag_suspeito", "cluster_size",
        ]
        cols_show = [c for c in cols_show if c in df.columns]
        tabela_html = (
            df.sort_values(["flag_suspeito", "stake_mines_brl"], ascending=[False, False])
              .head(30)[cols_show]
              .to_html(index=False, classes="tbl", border=0)
        )

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<title>Fraude Recarregue e Ganhe FT — 21/04/2026</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 40px auto; max-width: 1100px; color: #111; }}
  h1 {{ border-bottom: 3px solid #111; padding-bottom: 8px; }}
  h2 {{ margin-top: 32px; color: #1f2937; }}
  .alerta {{ font-size: 22px; font-weight: 700; padding: 12px 20px; border-radius: 6px; color: white; display: inline-block; }}
  .resumo {{ background: #f3f4f6; padding: 16px 20px; border-left: 4px solid #2563eb; border-radius: 4px; }}
  .tbl {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-top: 12px; }}
  .tbl th, .tbl td {{ border: 1px solid #e5e7eb; padding: 6px 10px; text-align: left; }}
  .tbl th {{ background: #f9fafb; }}
  .tbl tr:nth-child(even) {{ background: #fafafa; }}
  .como-ler {{ background: #fffbeb; border-left: 4px solid #f59e0b; padding: 14px 18px; border-radius: 4px; }}
  .meta {{ color: #6b7280; font-size: 13px; margin-bottom: 20px; }}
</style>
</head>
<body>
<h1>Fraude "Recarregue e Ganhe Fortune Tiger"</h1>
<div class="meta">
  Data de referencia: 21/04/2026 (BRT, D-0) | Gerado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
  Analise de risco/fraude near-real-time — autorizado per memory/feedback_risco_near_realtime.md
</div>

<div class="como-ler">
  <strong>Como ler este relatorio</strong><br>
  O bonus "Recarregue e Ganhe FT" paga giros no Fortune Tiger, mas o padrao suspeito
  e usar o saldo pra jogar Mines e executar rollback (estorno de aposta). Este relatorio
  cruza quem recebeu o bonus hoje com o que esta sendo jogado e sinaliza quem pode estar
  fraudando. Detalhes tecnicos: arquivo _legenda.txt na mesma pasta.
</div>

<h2>Resumo Executivo</h2>
<p class="resumo">{resumo_exec}</p>
{"<p><span class='alerta' style='background: " + metricas['cor_alerta'] + "'>Alerta: " + metricas['alerta_txt'] + "</span></p>" if metricas else ""}

<h2>Top 30 jogadores suspeitos</h2>
{tabela_html}

<h2>Proximos passos</h2>
<ol>
  <li>Auditor valida c_bonus_id da campanha (Parte 1 do script).</li>
  <li>Bloquear contas com flag_suspeito=TRUE + cluster_alto_risco=TRUE (imediato).</li>
  <li>Escalar para Raphael (CRM) + Gabriel (CTO) se alerta = ALTO RISCO.</li>
  <li>Investigar rollbacks concentrados em Mines (provider + game_id especifico).</li>
  <li>Considerar suspender a campanha se cohort contaminado &gt; 20%.</li>
</ol>

<h2>Fontes</h2>
<ul>
  <li>bonus_ec2.tbl_bonus_summary_details, tbl_bonus_profile</li>
  <li>fund_ec2.tbl_real_fund_txn (c_txn_type 27/72, c_product_id=CASINO)</li>
  <li>ps_bi.dim_user (is_test=false) + ecr_ec2.tbl_ecr_profile (mobile)</li>
  <li>bireports_ec2.tbl_vendor_games_mapping_data (catalogo Fortune Tiger, Mines)</li>
</ul>

</body>
</html>
"""
    HTML_PATH.write_text(html, encoding="utf-8")
    print(f"[SAIDA] HTML:    {HTML_PATH}")


# ============================================================================
# MAIN
# ============================================================================
def main() -> int:
    print("=" * 78)
    print(f"  FRAUDE 'Recarregue e Ganhe FT' — {DATA_REF_BRT} (D-0 risco)")
    print("=" * 78)

    # PARTE 1
    df_campanhas = identificar_campanhas_hoje()
    if df_campanhas.empty:
        print("\n[ERRO] Nenhuma campanha identificada. Abortando.")
        return 1

    # Auditor deve decidir qual c_bonus_id corresponde A campanha alvo.
    # Por default, pega os que tem match_termo=True; se nao houver, abortar.
    if "match_termo" in df_campanhas.columns:
        ids_alvo = df_campanhas.loc[df_campanhas["match_termo"] == True, "c_bonus_id"].tolist()
    else:
        ids_alvo = []

    if not ids_alvo:
        print("\n[HANDOFF AUDITOR] Nenhuma campanha matcheou LIKE 'recarregue/FT/reload'.")
        print("  Revisar manualmente as campanhas retornadas acima e setar BONUS_IDS_CONFIRMADOS.")
        print(f"  Candidatas salvas em: {OUT_DIR / 'campanhas_candidatas.csv'}")
        df_campanhas.to_csv(OUT_DIR / "campanhas_candidatas.csv", index=False, encoding="utf-8-sig")
        return 2

    print(f"\n[OK] IDs alvo (match termo): {ids_alvo}")

    # PARTE 2
    df_cohort = montar_cohort(ids_alvo)
    if df_cohort.empty:
        print("\n[ERRO] Cohort vazio. Verificar se bonus foi emitido / janela correta.")
        return 3

    # PARTE 3
    df_final = detectar_clusters(df_cohort)
    salvar_csv(df_final)
    gerar_html(df_final)

    print("\n[OK] Artefatos gerados. Passar para o auditor antes de enviar ao Head.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
