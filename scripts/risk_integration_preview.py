"""
Previa da Integracao: Matriz de Risco + Agente de Fraude
==========================================================
Gera uma PREVIA (sem alterar banco ou tabelas) de como ficaria
a integracao entre a Matriz de Risco (Mauro) e o Agente de Fraude (Squad 3).

O que faz:
  1. Le a Matriz (CSV com 21 tags, score_norm 0-100, tier)
  2. Le o Agente Casino (CSV com R1-R8, risk_score, risk_tier)
  3. Le o Agente Sportsbook (CSV com R9-R10, severidade)
  4. Cruza por user_id (ecr_id) e customer_id (external_id)
  5. Calcula score unificado: matriz * 0.4 + agente * 0.6
  6. Detecta conflitos (Matriz "Bom/Muito Bom" vs Agente "HIGH")
  7. Simula 5 novas tags derivadas do Agente
  8. Gera CSV de previa + relatorio HTML explicativo

NAO ALTERA: banco, tabelas, pipeline, nada. So gera outputs de previa.

Uso:
    python scripts/risk_integration_preview.py

Saida:
    output/risk_integration_preview_YYYY-MM-DD.csv
    output/risk_integration_preview_YYYY-MM-DD.html

Autor: Squad 3 — Intelligence Engine
Data: 2026-04-07
"""

import sys
import os
import json
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_matrix(path: str) -> pd.DataFrame:
    """Carrega CSV da Matriz de Risco."""
    df = pd.read_csv(path)
    df["user_id"] = df["user_id"].astype(str).str.strip()
    if "user_ext_id" in df.columns:
        df["user_ext_id"] = df["user_ext_id"].astype(str).str.strip()
    return df


def load_agent_casino(path: str) -> pd.DataFrame:
    """Carrega CSV do Agente Casino (R1-R8)."""
    df = pd.read_csv(path)
    df["c_ecr_id"] = df["c_ecr_id"].astype(str).str.strip()
    return df


def load_agent_sportsbook(path: str) -> pd.DataFrame:
    """Carrega CSV do Agente Sportsbook (R9-R10)."""
    df = pd.read_csv(path)
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    return df


def main():
    # ------------------------------------------------------------------
    # 1. Localizar arquivos mais recentes
    # ------------------------------------------------------------------
    # Matriz
    matrix_candidates = sorted(Path("reports").glob("risk_matrix_*_FINAL.csv"), reverse=True)
    if not matrix_candidates:
        print("ERRO: Nenhum CSV de matriz encontrado em reports/")
        return
    matrix_path = str(matrix_candidates[0])

    # Agente Casino — preferir o CSV mais completo (03/04 tem 1670, 06/04 tem 2)
    casino_candidates = sorted(Path(OUTPUT_DIR).glob("risk_fraud_alerts_*.csv"), reverse=True)
    # Escolher o maior arquivo (mais completo)
    if casino_candidates:
        casino_path = str(max(casino_candidates, key=lambda p: p.stat().st_size))
    else:
        casino_path = None

    # Agente Sportsbook
    sb_candidates = sorted(Path(OUTPUT_DIR).glob("risk_sportsbook_alerts_*.csv"), reverse=True)
    sb_path = str(sb_candidates[0]) if sb_candidates else None

    print(f"Matriz:     {matrix_path}")
    print(f"Casino:     {casino_path}")
    print(f"Sportsbook: {sb_path}")

    # ------------------------------------------------------------------
    # 2. Carregar dados
    # ------------------------------------------------------------------
    df_matrix = load_matrix(matrix_path)
    print(f"Matriz: {len(df_matrix):,} jogadores")

    df_casino = load_agent_casino(casino_path) if casino_path else pd.DataFrame()
    print(f"Casino (Agente): {len(df_casino):,} alertas")

    df_sb = load_agent_sportsbook(sb_path) if sb_path else pd.DataFrame()
    print(f"Sportsbook (Agente): {len(df_sb):,} alertas")

    # ------------------------------------------------------------------
    # 3. Normalizar IDs para cruzamento
    # ------------------------------------------------------------------
    # Matriz usa user_id (ecr_id) e user_ext_id (external_id)
    # Agente Casino usa c_ecr_id (ecr_id)
    # Agente SB usa customer_id (external_id)

    # Casino: join por ecr_id
    casino_merged = pd.DataFrame()
    if not df_casino.empty:
        casino_merged = df_matrix.merge(
            df_casino[["c_ecr_id", "regras_violadas", "qty_regras", "risk_score", "risk_tier", "evidencias"]],
            left_on="user_id",
            right_on="c_ecr_id",
            how="inner",
            suffixes=("_matrix", "_agent")
        )
        print(f"Casino cruzados com Matriz: {len(casino_merged):,}")

    # Sportsbook: join por external_id
    sb_merged = pd.DataFrame()
    if not df_sb.empty:
        sb_merged = df_matrix.merge(
            df_sb[["customer_id", "regras_violadas", "qty_regras", "max_severidade", "todas_evidencias"]],
            left_on="user_ext_id",
            right_on="customer_id",
            how="inner",
            suffixes=("_matrix", "_agent")
        )
        print(f"Sportsbook cruzados com Matriz: {len(sb_merged):,}")

    # ------------------------------------------------------------------
    # 4. Calcular score unificado
    # ------------------------------------------------------------------
    # Formula: score_unificado = matriz_norm * 0.4 + (100 - fraud_score_norm) * 0.6
    # Onde fraud_score_norm = risk_score do agente normalizado para 0-100

    results = []

    # Casino matches
    if not casino_merged.empty:
        for _, row in casino_merged.iterrows():
            # Normalizar fraud score: max possivel = ~120 (sum all rules)
            fraud_score_raw = row.get("risk_score", 0)
            fraud_score_norm = min(100, fraud_score_raw * 100 / 120)
            # Score da matriz invertido: 100 = bom, 0 = ruim
            # Score do agente invertido: 0 = bom (sem fraude), 100 = ruim (muita fraude)
            # Unificado: alto = bom
            score_unificado = row["score_norm"] * 0.4 + (100 - fraud_score_norm) * 0.6

            results.append({
                "user_id": row["user_id"],
                "user_ext_id": row.get("user_ext_id", ""),
                # Matriz
                "matrix_score_norm": row["score_norm"],
                "matrix_tier": row["tier"],
                "matrix_tags_positivas": sum(1 for col in [
                    "regular_depositor", "sustained_player", "non_bonus_depositor",
                    "reinvest_player", "non_promo_player", "engaged_player",
                    "player_reengaged", "sleeper_low_player", "vip_whale_player",
                    "winback_hi_val_player", "behav_slotgamer"
                ] if row.get(col, 0) > 0),
                "matrix_tags_negativas": sum(1 for col in [
                    "promo_only", "fast_cashout", "promo_chainer", "cashout_and_run",
                    "behav_risk_player", "potencial_abuser", "multi_game_player",
                    "rollback_player"
                ] if row.get(col, 0) != 0),
                # Agente
                "agent_risk_score": fraud_score_raw,
                "agent_risk_tier": row["risk_tier"],
                "agent_regras": row["regras_violadas"],
                "agent_qty_regras": row["qty_regras"],
                "agent_evidencias": str(row["evidencias"])[:150],
                # Unificado
                "score_unificado": round(score_unificado, 1),
                "fonte": "Casino (R1-R8)",
                # Conflito
                "conflito": "SIM" if (
                    row["tier"] in ("Bom", "Muito Bom") and
                    row["risk_tier"] in ("HIGH", "CRITICAL")
                ) else "NAO",
            })

    # Sportsbook matches
    if not sb_merged.empty:
        sev_to_score = {"HIGH": 80, "MEDIUM": 40, "LOW": 15}
        for _, row in sb_merged.iterrows():
            fraud_score_raw = sev_to_score.get(row.get("max_severidade", "LOW"), 15)
            fraud_score_norm = min(100, fraud_score_raw)
            score_unificado = row["score_norm"] * 0.4 + (100 - fraud_score_norm) * 0.6

            results.append({
                "user_id": row["user_id"],
                "user_ext_id": row.get("user_ext_id", ""),
                "matrix_score_norm": row["score_norm"],
                "matrix_tier": row["tier"],
                "matrix_tags_positivas": 0,
                "matrix_tags_negativas": 0,
                "agent_risk_score": fraud_score_raw,
                "agent_risk_tier": row["max_severidade"],
                "agent_regras": row.get("regras_violadas", ""),
                "agent_qty_regras": row.get("qty_regras", 0),
                "agent_evidencias": str(row.get("todas_evidencias", ""))[:150],
                "score_unificado": round(score_unificado, 1),
                "fonte": "Sportsbook (R9-R10)",
                "conflito": "SIM" if (
                    row["tier"] in ("Bom", "Muito Bom") and
                    row["max_severidade"] == "HIGH"
                ) else "NAO",
            })

    if not results:
        print("Nenhum cruzamento encontrado. Verifique IDs.")
        return

    df_result = pd.DataFrame(results)

    # Dedup por user_id — manter pior caso
    df_result = df_result.sort_values("score_unificado").drop_duplicates("user_id", keep="first")
    df_result = df_result.sort_values("score_unificado")

    print(f"\nTotal cruzados: {len(df_result):,}")
    print(f"Conflitos detectados: {len(df_result[df_result['conflito'] == 'SIM'])}")

    # ------------------------------------------------------------------
    # 5. Simular 5 novas tags derivadas do Agente
    # ------------------------------------------------------------------
    # Nao altera a Matriz — apenas mostra como ficaria
    new_tags_spec = {
        "ZERO_DEPOSIT_CASHOUT": {
            "score": -30,
            "regra": "R3a",
            "descricao": "NUNCA depositou na vida + sacou > R$50",
            "tipo": "Derivada do Agente R3a",
        },
        "VELOCITY_RISK": {
            "score": -15,
            "regra": "R6",
            "descricao": "5+ depositos/saques em 1 hora",
            "tipo": "Derivada do Agente R6",
        },
        "FAST_REG_CASH": {
            "score": -20,
            "regra": "R7",
            "descricao": "Sacou < 24h apos registro",
            "tipo": "Derivada do Agente R7",
        },
        "FREESPIN_ABUSER": {
            "score": -25,
            "regra": "R8",
            "descricao": "Revenue negativo + bonus alto + freespin wins",
            "tipo": "Derivada do Agente R8",
        },
        "LIVE_DELAY_EXPLOIT": {
            "score": -20,
            "regra": "R9",
            "descricao": "Win rate alto em live + padrao test-then-bet",
            "tipo": "Derivada do Agente R9",
        },
    }

    # ------------------------------------------------------------------
    # 6. Salvar CSV
    # ------------------------------------------------------------------
    today = datetime.now().strftime("%Y-%m-%d")
    csv_path = os.path.join(OUTPUT_DIR, f"risk_integration_preview_{today}.csv")
    df_result.to_csv(csv_path, index=False)
    print(f"CSV salvo: {csv_path}")

    # ------------------------------------------------------------------
    # 7. Gerar HTML de previa
    # ------------------------------------------------------------------
    # Stats
    total = len(df_result)
    conflitos = df_result[df_result["conflito"] == "SIM"]
    n_conflitos = len(conflitos)

    # Conflito table rows
    conflito_rows = ""
    for _, r in conflitos.sort_values("score_unificado").iterrows():
        conflito_rows += f"""
        <tr class="conflict">
            <td class="mono">{r['user_id']}</td>
            <td class="mono">{r['user_ext_id']}</td>
            <td><span class="badge good">{r['matrix_tier']}</span></td>
            <td class="center">{r['matrix_score_norm']}</td>
            <td><span class="badge bad">{r['agent_risk_tier']}</span></td>
            <td class="center">{r['agent_risk_score']}</td>
            <td>{r['agent_regras']}</td>
            <td class="center score-uni">{r['score_unificado']}</td>
            <td class="evidence">{r['agent_evidencias']}...</td>
        </tr>"""

    # Top 30 worst unified scores
    worst_rows = ""
    for _, r in df_result.head(30).iterrows():
        cls = "conflict" if r["conflito"] == "SIM" else ""
        worst_rows += f"""
        <tr class="{cls}">
            <td class="mono">{r['user_id']}</td>
            <td class="mono">{r['user_ext_id']}</td>
            <td>{r['matrix_tier']}</td>
            <td class="center">{r['matrix_score_norm']}</td>
            <td>{r['agent_risk_tier']}</td>
            <td class="center">{r['agent_risk_score']}</td>
            <td>{r['agent_regras']}</td>
            <td class="center score-uni">{r['score_unificado']}</td>
            <td>{r['fonte']}</td>
        </tr>"""

    # New tags table
    tags_rows = ""
    for tag_name, spec in new_tags_spec.items():
        tags_rows += f"""
        <tr>
            <td class="mono">{tag_name}</td>
            <td class="center" style="color:var(--red)">{spec['score']}</td>
            <td>{spec['regra']}</td>
            <td>{spec['descricao']}</td>
            <td>{spec['tipo']}</td>
        </tr>"""

    # Distribution of unified scores
    bins = [0, 20, 40, 60, 80, 100]
    labels_bins = ["0-20 (Critico)", "20-40 (Ruim)", "40-60 (Mediano)", "60-80 (Bom)", "80-100 (Otimo)"]
    score_dist = pd.cut(df_result["score_unificado"], bins=bins, labels=labels_bins, right=True).value_counts().sort_index()
    dist_rows = ""
    for label, count in score_dist.items():
        pct = round(count / total * 100, 1) if total else 0
        dist_rows += f"<tr><td>{label}</td><td class='center'>{count}</td><td class='center'>{pct}%</td></tr>\n"

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Previa Integracao - Matriz + Agente</title>
<style>
:root {{
    --bg: #0f0f1a; --card: #1a1a2e; --border: #2d2d44;
    --purple: #8b5cf6; --red: #ef4444; --green: #10b981; --yellow: #eab308; --orange: #f97316; --cyan: #06b6d4;
    --text: #ffffff; --text2: #9ca3af; --text3: #6b7280;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ background:var(--bg); color:var(--text); font-family:'Inter',system-ui,sans-serif; font-size:14px; line-height:1.6; }}
.main {{ max-width:1400px; margin:0 auto; padding:24px 32px; }}
h1 {{ font-size:24px; margin-bottom:8px; }}
h2 {{ font-size:18px; margin:28px 0 12px; color:var(--purple); }}
h3 {{ font-size:15px; margin:20px 0 8px; }}
.subtitle {{ color:var(--text2); font-size:13px; margin-bottom:24px; }}
.meta {{ color:var(--text3); font-size:12px; margin-bottom:20px; }}

/* Cards */
.kpi-row {{ display:grid; grid-template-columns:repeat(4, 1fr); gap:16px; margin-bottom:24px; }}
.kpi {{ background:var(--card); border:1px solid var(--border); border-radius:10px; padding:20px; text-align:center; }}
.kpi.alert {{ border-color:var(--red); background:rgba(239,68,68,0.08); }}
.kpi-label {{ color:var(--text2); font-size:11px; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px; }}
.kpi-value {{ font-size:28px; font-weight:700; }}
.kpi-sub {{ color:var(--text3); font-size:11px; margin-top:4px; }}

/* Tables */
.tbl {{ background:var(--card); border:1px solid var(--border); border-radius:10px; overflow-x:auto; margin-bottom:24px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th {{ background:#111122; color:var(--text2); text-transform:uppercase; font-size:11px; padding:12px 14px; text-align:left; border-bottom:1px solid var(--border); white-space:nowrap; }}
td {{ padding:10px 14px; border-bottom:1px solid var(--border); }}
tr:nth-child(even) td {{ background:#111122; }}
tr.conflict td {{ background:rgba(239,68,68,0.08); }}
.mono {{ font-family:'Courier New',monospace; font-size:11px; color:var(--purple); }}
.center {{ text-align:center; }}
.evidence {{ font-size:11px; color:var(--text2); max-width:300px; white-space:normal; line-height:1.4; }}
.score-uni {{ font-weight:700; color:var(--cyan); }}

.badge {{ padding:3px 10px; border-radius:12px; font-size:11px; font-weight:600; }}
.badge.good {{ background:rgba(16,185,129,0.2); color:var(--green); }}
.badge.bad {{ background:rgba(239,68,68,0.2); color:var(--red); }}

/* Callout */
.callout {{ background:var(--card); border-left:4px solid var(--purple); border-radius:0 10px 10px 0; padding:16px 20px; margin:16px 0; }}
.callout.warn {{ border-left-color:var(--orange); }}
.callout.danger {{ border-left-color:var(--red); }}
.callout h4 {{ font-size:14px; margin-bottom:6px; }}
.callout p {{ font-size:12px; color:var(--text2); }}
.callout code {{ background:rgba(139,92,246,0.15); padding:2px 6px; border-radius:4px; font-size:12px; color:var(--purple); }}

/* Proposal */
.proposal {{ background:var(--card); border:1px solid var(--cyan); border-radius:10px; padding:20px; margin:20px 0; }}
.proposal h3 {{ color:var(--cyan); margin-top:0; }}
.proposal-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-top:12px; }}
.proposal-col {{ font-size:12px; color:var(--text2); }}
.proposal-col h4 {{ color:var(--text); font-size:12px; margin-bottom:6px; text-transform:uppercase; }}
.proposal-col p {{ margin-bottom:4px; }}

.footer {{ text-align:center; color:var(--text3); font-size:11px; margin-top:32px; padding-top:16px; border-top:1px solid var(--border); }}
@media (max-width: 1024px) {{ .kpi-row {{ grid-template-columns:repeat(2, 1fr); }} .proposal-grid {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
<div class="main">

<h1>Previa: Integracao Matriz de Risco + Agente de Fraude</h1>
<div class="subtitle">Documento para validacao do time de dados — NENHUMA alteracao foi feita no banco ou tabelas</div>
<div class="meta">Gerado: {datetime.now().strftime("%Y-%m-%d %H:%M")} | Matriz: {matrix_path} | Agente: {casino_path} + {sb_path}</div>

<!-- ============================================================ -->
<h2>1. O que esta sendo proposto</h2>

<div class="proposal">
    <h3>Proposta de Integracao</h3>
    <div class="proposal-grid">
        <div class="proposal-col">
            <h4>Hoje (separados)</h4>
            <p><strong>Matriz de Risco (Mauro):</strong> 21 tags, score 0-100, 5 tiers. Classifica TODO jogador. Foco: CRM + retencao + risco comportamental.</p>
            <p><strong>Agente de Fraude (Squad 3):</strong> 10 regras (R1-R10), score 0-120, 4 tiers. Detecta SUSPEITOS. Foco: antifraude + compliance.</p>
            <p style="color:var(--red); font-weight:600;">Problema: jogador pode ser "Muito Bom" na Matriz e "HIGH" no Agente ao mesmo tempo.</p>
        </div>
        <div class="proposal-col">
            <h4>Proposta (unificado)</h4>
            <p><strong>Score Unificado:</strong> <code>matriz_norm * 0.4 + (100 - fraud_norm) * 0.6</code></p>
            <p>Peso 60% no agente de fraude porque fraude e binaria (ou e ou nao e), enquanto perfil comportamental admite nuances.</p>
            <p><strong>5 novas tags</strong> derivadas do Agente, adicionadas a Matriz.</p>
            <p><strong>Flag de conflito</strong> automatico quando os dois sistemas discordam.</p>
        </div>
    </div>
</div>

<!-- ============================================================ -->
<h2>2. Numeros do cruzamento</h2>

<div class="kpi-row">
    <div class="kpi">
        <div class="kpi-label">Jogadores na Matriz</div>
        <div class="kpi-value" style="color:var(--purple)">{len(df_matrix):,}</div>
        <div class="kpi-sub">Base completa (90 dias)</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Cruzados com Agente</div>
        <div class="kpi-value" style="color:var(--cyan)">{total}</div>
        <div class="kpi-sub">Casino + Sportsbook</div>
    </div>
    <div class="kpi alert">
        <div class="kpi-label">Conflitos Detectados</div>
        <div class="kpi-value" style="color:var(--red)">{n_conflitos}</div>
        <div class="kpi-sub">Matriz "Bom/Muito Bom" vs Agente "HIGH"</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Pior Score Unificado</div>
        <div class="kpi-value" style="color:var(--orange)">{df_result['score_unificado'].min():.0f}</div>
        <div class="kpi-sub">Escala 0-100 (menor = pior)</div>
    </div>
</div>

<!-- ============================================================ -->
<h2>3. Conflitos: Matriz diz "Bom" mas Agente diz "HIGH"</h2>

<div class="callout danger">
    <h4>Por que isso e critico?</h4>
    <p>Esses {n_conflitos} jogadores passariam despercebidos em qualquer analise que use APENAS a Matriz.
    A Matriz os classifica como jogadores saudaveis, mas o Agente detectou padroes claros de fraude.
    Sem integracao, decisoes de CRM (dar bonus, aumentar limites) podem beneficiar fraudadores.</p>
</div>

<div class="tbl">
<table>
    <thead><tr>
        <th>ECR ID</th><th>Ext ID</th><th>Tier Matriz</th><th>Score Matriz</th>
        <th>Tier Agente</th><th>Score Agente</th><th>Regras</th><th>Unificado</th><th>Evidencias</th>
    </tr></thead>
    <tbody>
        {conflito_rows if conflito_rows else '<tr><td colspan="9" class="center" style="color:var(--green);">Nenhum conflito detectado</td></tr>'}
    </tbody>
</table>
</div>

<!-- ============================================================ -->
<h2>4. Distribuicao do Score Unificado</h2>

<div class="callout">
    <h4>Formula: <code>score_unificado = matriz_norm * 0.4 + (100 - fraud_norm) * 0.6</code></h4>
    <p>Valores altos = bom (jogador saudavel). Valores baixos = alerta (fraude detectada pelo agente pesa mais).
    O peso 60% no agente garante que fraude detectada SEMPRE derruba o score, mesmo se a Matriz diz "Muito Bom".</p>
</div>

<div class="tbl">
<table>
    <thead><tr><th>Faixa</th><th>Jogadores</th><th>%</th></tr></thead>
    <tbody>{dist_rows}</tbody>
</table>
</div>

<!-- ============================================================ -->
<h2>5. Top 30 piores scores unificados</h2>

<div class="tbl">
<table>
    <thead><tr>
        <th>ECR ID</th><th>Ext ID</th><th>Matriz Tier</th><th>Matriz Score</th>
        <th>Agente Tier</th><th>Agente Score</th><th>Regras</th><th>Unificado</th><th>Fonte</th>
    </tr></thead>
    <tbody>{worst_rows}</tbody>
</table>
</div>

<!-- ============================================================ -->
<h2>6. 5 Novas Tags Propostas (derivadas do Agente)</h2>

<div class="callout">
    <h4>O que muda na Matriz?</h4>
    <p>Adicionar 5 colunas novas na tabela <code>multibet.risk_tags</code> no Super Nova DB.
    Cada tag e populada pelo Agente de Fraude e entra no calculo do score_bruto da Matriz.
    Isso CORRIGE a maior fraqueza da Matriz: ausencia de regras de fraude especificas.</p>
</div>

<div class="tbl">
<table>
    <thead><tr><th>Tag</th><th>Score</th><th>Regra Origem</th><th>Descricao</th><th>Tipo</th></tr></thead>
    <tbody>{tags_rows}</tbody>
</table>
</div>

<div class="callout warn">
    <h4>Impacto estimado na tabela</h4>
    <p><strong>Alteracao DDL:</strong> 5 novas colunas INTEGER DEFAULT 0 em <code>multibet.risk_tags</code></p>
    <p><strong>Alteracao no pipeline:</strong> Adicionar 5 novos SQLs em <code>scripts/sql/risk_matrix/</code></p>
    <p><strong>Recalculo:</strong> score_bruto, score_norm e tier precisam ser recalculados incluindo as novas tags</p>
    <p><strong>Risco:</strong> BAIXO — novas colunas com DEFAULT 0 nao quebram queries existentes</p>
</div>

<!-- ============================================================ -->
<h2>7. Proximos passos (se aprovado)</h2>

<div class="proposal">
    <div class="proposal-grid">
        <div class="proposal-col">
            <h4>Alteracoes necessarias</h4>
            <p>1. ALTER TABLE multibet.risk_tags ADD COLUMN (5 colunas)</p>
            <p>2. Criar 5 SQLs novos em scripts/sql/risk_matrix/</p>
            <p>3. Atualizar TAG_SCORES no pipeline com novos pesos</p>
            <p>4. Re-rodar pipeline para recalcular scores</p>
            <p>5. Validar com amostra antes de produtizar</p>
        </div>
        <div class="proposal-col">
            <h4>O que NAO muda</h4>
            <p>- As 21 tags existentes continuam iguais</p>
            <p>- O pipeline continua rodando da mesma forma</p>
            <p>- A normalizacao (0-100) continua a mesma</p>
            <p>- O Super Nova DB continua como destino</p>
            <p>- Queries que leem a tabela continuam funcionando</p>
        </div>
    </div>
</div>

<div class="footer">
    Squad 3 — Intelligence Engine | Previa de Integracao | Documento para validacao — nenhuma alteracao realizada
</div>

</div>
</body>
</html>"""

    html_path = os.path.join(OUTPUT_DIR, f"risk_integration_preview_{today}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML salvo: {html_path}")

    # Resumo
    print("\n=== RESUMO ===")
    print(f"Total cruzados: {total}")
    print(f"Conflitos (Matriz Bom + Agente HIGH): {n_conflitos}")
    print(f"Score unificado min/max: {df_result['score_unificado'].min():.1f} / {df_result['score_unificado'].max():.1f}")


if __name__ == "__main__":
    main()
