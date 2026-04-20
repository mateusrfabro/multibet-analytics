"""
Gera docs/schema_multibet_database_v2.0.md a partir do JSON de colunas.

Entrada: reports/schema_columns_multibet_YYYYMMDD.json (mais recente)
Saida : docs/schema_multibet_database_v2.0.md
        docs/schema_play4_supernova.md

Organiza os objetos por categoria (fact/agg/dim/silver/tab/crm/risk+pcr+segment/
operational/views/matviews) e gera tabela de colunas por objeto.

Tambem imprime docs/schema_play4_supernova.md espelhando as 10 foreign tables.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
DOCS = ROOT / "docs"

latest = sorted(REPORTS.glob("schema_columns_multibet_*.json"))[-1]
data = json.loads(latest.read_text(encoding="utf-8"))
generated_at = data["generated_at"]

# --- carrega inventario para metadados auxiliares (linhas estimadas/tamanho) ---
inv_file = sorted(REPORTS.glob("inventario_schema_refresh_*.json"))[-1]
inv = json.loads(inv_file.read_text(encoding="utf-8"))
meta = {}
for schema, items in inv["schemas"].items():
    for it in items:
        meta[(schema, it["name"])] = it


# --- categorias (nome -> regex-like prefix/exact match) ---
def categorize(name: str, otype: str) -> str:
    if otype == "matview":
        return "matview"
    if otype == "foreign_table":
        return "foreign"
    if otype == "view":
        return "view"
    # tabelas
    if name.startswith("fact_") or name.startswith("fct_"):
        return "fact"
    if name.startswith("agg_"):
        return "agg"
    if name.startswith("dim_") or name == "game_image_mapping":
        return "dim"
    if name.startswith("silver_"):
        return "silver"
    if name.startswith("tab_"):
        return "tab"
    if name.startswith("crm_"):
        return "crm"
    if name.startswith("risk_") or name.startswith("pcr_") or name.startswith("segment_"):
        return "risk"
    if name.startswith("mv_"):
        return "mv_legacy"
    if name in ("grandes_ganhos", "trackings", "aquisicao_trafego_diario",
                "etl_active_player_retention_weekly", "etl_control", "migrations"):
        return "operational"
    return "other"


CATEGORY_ORDER = [
    ("matview",    "Materialized views"),
    ("fact",       "Fact / Fct (produto, player, aquisicao)"),
    ("agg",        "Agregacoes"),
    ("dim",        "Dimensoes & mapeamento"),
    ("silver",     "Silver / staging"),
    ("tab",        "Tabelas auxiliares (matrizes financeiras)"),
    ("crm",        "CRM"),
    ("risk",       "Risco / PCR / Segmentacao"),
    ("mv_legacy",  "Tabelas com prefixo mv_ (legado)"),
    ("operational","Operacionais / ETL / utilitarios"),
    ("other",      "Outros"),
    ("view",       "Views"),
    ("foreign",    "Foreign tables"),
]


def format_cell(text):
    if text is None:
        return "—"
    s = str(text).replace("|", "\\|").replace("\n", " ")
    if len(s) > 80:
        s = s[:77] + "..."
    return s


def fmt_num(n):
    if n is None:
        return "—"
    try:
        return f"{int(n):,}".replace(",", ".")
    except Exception:
        return str(n)


def fmt_bytes(b):
    if b is None or b == 0:
        return "—"
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.0f} {unit}" if unit == "B" else f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def render_table(schema_key, objects_map):
    buckets = {key: [] for key, _ in CATEGORY_ORDER}
    for obj_name, cols in sorted(objects_map.items()):
        m = meta.get((schema_key, obj_name), {})
        otype = m.get("type", "table")
        cat = categorize(obj_name, otype)
        buckets[cat].append((obj_name, cols, m))
    out = []
    for cat_key, cat_label in CATEGORY_ORDER:
        if not buckets.get(cat_key):
            continue
        out.append(f"\n## {cat_label}\n")
        for obj_name, cols, m in buckets[cat_key]:
            rows_est = fmt_num(m.get("rows_estimate"))
            size = fmt_bytes(m.get("size_bytes"))
            n_cols = len(cols)
            otype = m.get("type", "?")
            out.append(f"### `{obj_name}`")
            out.append(f"**Tipo:** {otype} &nbsp;&nbsp; **Linhas (est.):** {rows_est} &nbsp;&nbsp; **Tamanho:** {size} &nbsp;&nbsp; **Colunas:** {n_cols}\n")
            out.append("| # | Coluna | Tipo | PK | Null | Default |")
            out.append("|---|--------|------|----|------|---------|")
            for col in cols:
                pk = "PK" if col["pk"] else ""
                null = "Y" if col["nullable"] else "N"
                default = format_cell(col["default"])
                out.append(f"| {col['pos']} | `{col['name']}` | {col['type']} | {pk} | {null} | {default} |")
            out.append("")
    return "\n".join(out)


# ---- Documento multibet ----
mb = data["schemas"]["multibet"]["tables"]

header = f"""# Schema do Banco de Dados MultiBet — v2.0

**Schema:** `multibet`
**Banco:** Super Nova DB (PostgreSQL — AWS RDS)
**Host:** `supernova-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com:5432`
**Versao:** 2.0 (refresh automatico)
**Data:** {generated_at[:10]} (substitui v1.1 de 19/03/2026)
**Responsavel:** Mateus Fabro — Squad Intelligence Engine
**Fonte:** gerado por `scripts/generate_schema_doc_v2.py` a partir do JSON em `reports/schema_columns_multibet_{generated_at[:10].replace('-','')}.json`

---

## Resumo

| Categoria | Quantidade |
|-----------|-----------|
| Tabelas BASE (incluindo `mv_*` legadas) | 74 |
| Views                                    | 43 |
| Materialized views (`mv_*` reais)        | 3  |
| **Total** | **120 objetos no schema multibet** |

**Mudancas vs v1.1:**
- Camada **Bronze descontinuada** (24 tabelas `bronze_*` removidas). Pipelines leem Athena direto.
- Novas tabelas: `risk_tags`, `risk_tags_pgs`, `pcr_ratings`, `segment_tags`, `silver_*` (5), `tab_user_daily`, `tab_dep_user`, `tab_hour_*` (5), `tab_user_affiliate`, `tab_with_user`, `tab_affiliate`, `tab_btr`, `fact_ad_spend`, `fact_sports_odds_performance`, `fact_affiliate_revenue`, `fct_player_performance_by_period`, `fct_active_players_by_period`, `dim_affiliate_source`, `game_image_mapping`, `etl_control`, `migrations`.
- 3 novas matviews: `mv_aquisicao`, `mv_cohort_aquisicao`, `mv_cohort_retencao_ftd`.
- +35 views: `vw_front_*` (6), `matriz_*` (6), `vw_odds_performance_*` (2), `vw_ad_spend_*`, `vw_roi_*`, `vw_ltv_cac_ratio`, `vw_segmentacao_hibrida`, `pcr_atual`, `pcr_resumo`, varias de live ops.

**Arquivos complementares:**
- [docs/inventario_schema_multibet.md](inventario_schema_multibet.md) — inventario resumido (tabelas x camadas, pipelines, dependencias)
- [docs/schema_play4_supernova.md](schema_play4_supernova.md) — foreign tables do schema `play4`
- [docs/supernova_bet_guide.md](supernova_bet_guide.md) — Super Nova Bet Paquistao (outro DB)
- [docs/_migration/schema_bronze_multibet_v1.0.md](_migration/schema_bronze_multibet_v1.0.md) — bronze arquivado (referencia historica)

---

## Regras gerais

- **Somente destino.** Super Nova DB nao e fonte para entregas ao negocio — usar apenas Athena como fonte. Regra oficial desde 2026-03.
- **UTC → BRT:** timestamps sao armazenados em UTC. Converter usando `AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'` nas views/relatorios.
- **Upsert:** maioria das tabelas fact usa `INSERT ... ON CONFLICT DO UPDATE`.
- **Full reload:** algumas fact (casino/sports) fazem TRUNCATE+INSERT.
- **JSONB:** `fact_crm_daily_performance` usa colunas JSONB (funil, financeiro, comparativo).
- **LGPD:** `grandes_ganhos` hasheia nomes (ex: "Ri***s").
"""

body_mb = render_table("multibet", mb)
(DOCS / "schema_multibet_database_v2.0.md").write_text(header + "\n" + body_mb, encoding="utf-8")
print(f"OK: docs/schema_multibet_database_v2.0.md ({len(mb)} objetos)")


# ---- Documento play4 ----
p4 = data["schemas"]["play4"]["tables"]

header_p4 = f"""# Schema `play4` — Super Nova DB (foreign tables para Play4Tune)

**Schema:** `play4`
**Banco host:** Super Nova DB (`supernova_db`)
**Banco remoto:** `supernova-bet-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com` / `supernova_bet`
**Mecanismo:** foreign data wrapper (`postgres_fdw` via server `supernova_bet_server`)
**Moeda:** PKR (Paquistao) | **Provider:** 2J Games | 100% Casino
**Versao:** 2.0
**Data:** {generated_at[:10]} (atualizado com `vw_ggr_player_game_daily`)

---

## Resumo

| Tipo | Quantidade |
|------|-----------|
| Foreign tables | {len(p4)} |

## Observacoes

- Foreign tables sao **somente leitura** — escritas devem ir direto no DB remoto (usar `db/supernova_bet.py`).
- Dados **agregados** (hora/dia). Para granularidade completa (transacoes, apostas, jogadores) usar acesso direto ao `supernova_bet`.
- **Nova tabela em 2026-04:** `vw_ggr_player_game_daily` (31 colunas) — GGR granular por jogador/jogo/dia com flags de outlier.
- Criacao do schema e foreign tables: ver projeto [project_play4tune.md](../../../.claude/projects/c--Users-NITRO-OneDrive---PGX-MultiBet/memory/project_play4tune.md).

---

## Foreign tables
"""

body_p4 = render_table("play4", p4)
(DOCS / "schema_play4_supernova.md").write_text(header_p4 + body_p4, encoding="utf-8")
print(f"OK: docs/schema_play4_supernova.md ({len(p4)} objetos)")
