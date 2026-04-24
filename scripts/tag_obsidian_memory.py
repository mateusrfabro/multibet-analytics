"""
Adiciona tags YAML nos arquivos de memoria pro Obsidian filtrar por tema.
Preserva frontmatter existente (name, description, type).
Tags aninhadas: feedback/athena, project/crm, schema/ps_bi, etc.
"""
import re
from pathlib import Path

MEMORY_DIR = Path(r"C:\Users\NITRO\.claude\projects\c--Users-NITRO-OneDrive---PGX-MultiBet\memory")

def topic_for(name: str) -> list[str]:
    """Retorna lista de tags (tipo + topicos) a partir do nome do arquivo."""
    n = name.lower().removesuffix(".md")

    if n == "memory":
        return []

    tags = []
    if n.startswith("feedback_"):
        tags.append("feedback")
        body = n.removeprefix("feedback_")
    elif n.startswith("project_"):
        tags.append("project")
        body = n.removeprefix("project_")
    elif n.startswith("schema_") or n.startswith("schemas_"):
        tags.append("schema")
        body = n.removeprefix("schema_").removeprefix("schemas_")
    elif n.startswith("reference_"):
        tags.append("reference")
        body = n.removeprefix("reference_")
    elif n.startswith("ec2_"):
        tags.append("infra")
        body = n
    elif n.startswith("alinhamento_"):
        tags.append("meeting")
        body = n
    elif n in ("estrutura_squads", "user_agent_squad"):
        tags.append("squad")
        return tags
    elif n == "glossario_kpis":
        tags.append("glossario")
        return tags
    elif n.startswith("bigquery_") or n.startswith("redshift_"):
        tags.append("reference")
        body = n
    elif n.startswith("campanha_"):
        tags.append("project")
        body = n.removeprefix("campanha_")
    else:
        body = n

    topic_rules = [
        (["athena", "fund_ec2", "cashier_ec2", "bireports", "timezone"], "athena"),
        (["bigquery", "smartico"], "smartico"),
        (["supernova_bet", "p4t", "play4tune", "paquistan", "paquistaneses"], "supernova-bet"),
        (["supernova"], "supernova"),
        (["ec2", "deploy", "flask", "infra"], "ec2"),
        (["crm", "bonus_isolation", "funil"], "crm"),
        (["ggr"], "ggr"),
        (["ftd"], "ftd"),
        (["btr"], "btr"),
        (["risk", "risco", "farming", "matriz_risco"], "risk"),
        (["affiliate", "affiliates", "trackings", "keitaro"], "affiliates"),
        (["marketing", "ad_spend", "google_ads", "dim_marketing"], "marketing"),
        (["sportsbook", "fact_sports"], "sportsbook"),
        (["report", "relatorio", "tooltip", "dashboard", "views_front", "views_casino"], "report"),
        (["test_users", "qa_nomes"], "test-users"),
        (["python", "import"], "python"),
        (["squad", "workflow", "memoria", "framing", "investigar", "auditoria", "validacao", "validar"], "workflow"),
        (["entrega", "legenda", "naming", "padrao", "linguagem"], "entrega"),
        (["ps_bi"], "ps_bi"),
        (["cohort", "gatekeeper"], "cohort"),
        (["ml_approach"], "ml"),
        (["pcr"], "pcr"),
        (["poolcompras"], "operacao"),
        (["csv", "tokens", "economia", "qualidade"], "workflow"),
        (["d_menos_1", "snapshot", "dia_semana"], "workflow"),
        (["inativo"], "definicao"),
    ]
    for keywords, topic in topic_rules:
        if any(kw in body for kw in keywords):
            if topic and topic not in tags:
                tags.append(topic)
            break

    return tags

def process_file(path: Path) -> tuple[bool, str]:
    """Adiciona/atualiza campo tags no frontmatter. Retorna (modificado, tags)."""
    content = path.read_text(encoding="utf-8")
    tags = topic_for(path.name)

    if not tags:
        return False, ""

    tags_line = "tags:\n" + "\n".join(f"  - {t}" for t in tags)

    fm_match = re.match(r"^---\n(.*?)\n---\n", content, re.DOTALL)
    if fm_match:
        fm_body = fm_match.group(1)
        if re.search(r"^tags:", fm_body, re.MULTILINE):
            fm_body_new = re.sub(
                r"^tags:(?:\n  - .*)*",
                tags_line,
                fm_body,
                count=1,
                flags=re.MULTILINE,
            )
        else:
            fm_body_new = fm_body.rstrip() + "\n" + tags_line
        new_content = f"---\n{fm_body_new}\n---\n" + content[fm_match.end():]
    else:
        new_content = f"---\n{tags_line}\n---\n\n" + content

    if new_content != content:
        path.write_text(new_content, encoding="utf-8")
        return True, ", ".join(tags)
    return False, ", ".join(tags)

def main():
    files = sorted(MEMORY_DIR.glob("*.md"))
    print(f"Processando {len(files)} arquivos...\n")
    modified = 0
    skipped = 0
    for f in files:
        changed, tags = process_file(f)
        if changed:
            modified += 1
            print(f"  OK {f.name:60s} -> [{tags}]")
        else:
            skipped += 1
    print(f"\nResultado: {modified} modificados, {skipped} pulados (ja tinham tags iguais ou MEMORY.md)")

if __name__ == "__main__":
    main()
