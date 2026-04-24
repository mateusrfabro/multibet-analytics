# Teste de Migração PCR: `core_external_markers` → `core_external_segment`

> **Data:** 21/04/2026
> **Analista:** Mateus Fabro
> **Revisor CRM:** Raphael Miranda
> **Sistema afetado:** PCR (Player Credit Rating)
> **Objetivo:** Validar empiricamente a migração de tags `PCR_RATING_*` do bucket `core_external_markers` para `core_external_segment` no Smartico, conforme alinhamento com o time de CRM.

---

## Contexto

### O problema

Em 20/04/2026, a v1.2 do pipeline PCR publicou tags de rating (`PCR_RATING_S`, `PCR_RATING_A`, ..., `PCR_RATING_E`) no bucket `core_external_markers` do Smartico. Esse bucket é usado para **tags transacionais/operacionais** (ex: `RISK_*` da Matriz de Risco, `WHATSAPP_OPTIN`, etc.).

O Raphael (Lead CRM) identificou que **tags comportamentais** (como ratings) devem ir para o bucket `core_external_segment`, que é separado no painel Smartico (seção "External Segments") e serve justamente para segmentação comportamental.

### A solução (v1.4)

1. **Pipeline regular** (`push_pcr_to_smartico.py`) agora escreve direto em `core_external_segment`
2. **Script one-shot** (`migrate_pcr_markers_to_segment.py`) migra jogadores que foram publicados no bucket errado ontem
3. **Payload atômico por jogador:**
   ```json
   {
     "-core_external_markers": [
       "PCR_RATING_S", "PCR_RATING_A", "PCR_RATING_B",
       "PCR_RATING_C", "PCR_RATING_D", "PCR_RATING_E", "PCR_RATING_NEW"
     ],
     "+core_external_segment": ["PCR_RATING_B"]
   }
   ```

### Rollout (alinhado com `feedback_smartico_push_rollout_playbook.md`)

| Fase | Usuários | Validação | Status |
|---|---|---|---|
| **Canário** | 1 | Visual no painel Smartico com Raphael | Em execução |
| **Amostra** | 10 | Mesmos 10 da Fase 2 de ontem | Pendente |
| **Full** | Todos do snapshot com rating PCR | Pendente |

---

## Escopo do push de ontem (baseline da migração)

Ontem (20/04/2026) foram publicados 21 jogadores em 3 execuções no bucket **incorreto** (`core_external_markers`):

- `14:23 BRT` — Fase 1 Canário: 1 jogador
- `17:39 BRT` — Fase 2 Amostra: 10 jogadores
- `17:58 BRT` — Fase 2 Retry/segundo lote: 10 jogadores

**Fase 3 (Full) nunca foi executada.** Portanto, o universo de jogadores "contaminados" no bucket `core_external_markers` é pequeno (~21 jogadores, com possível sobreposição entre os lotes de 17:39 e 17:58).

Os 10 jogadores da amostra constam em [`reports/smartico_pcr_amostra_20260420.csv`](../../reports/smartico_pcr_amostra_20260420.csv).

---

## Canário escolhido

| Campo | Valor |
|---|---|
| `user_ext_id` | `30352025` |
| `rating atual` | `B` |
| `PVS` | `58.3` |
| `player_id` | `478865121792075369` |
| `Justificativa da escolha` | ID curto (fácil de pesquisar no painel), rating intermediário (não-extremo), **está na amostra de ontem** (tem `PCR_RATING_B` em `core_external_markers` — permite ver visualmente o flip) |

---

## Protocolo de Validação (3 passos)

### Passo 1 — Estado ANTES do push

**Ação do analista:**
- Abrir perfil do jogador `30352025` no painel Smartico (BackOffice)
- Tirar print da página completa do perfil
- Salvar em `prints/01_antes_push_30352025.png`

**Validação esperada:**
- ✅ Seção `External Markers` **contém** `PCR_RATING_B`
- ✅ Seção `External Segments` **NÃO contém** `PCR_RATING_*`
- ✅ Outras tags (`RISK_*`, `CL02_*`, etc.) permanecem intocadas (nota pro diff do Passo 3)

### Passo 2 — Execução do push

**Comando a ser executado:**
```bash
python scripts/migrate_pcr_markers_to_segment.py --user 30352025 --skip-cjm --confirm
```

**Observações:**
- `--skip-cjm` evita disparar automations / jornadas (só popula o perfil)
- `--confirm` obriga envio real (sem essa flag, cai em dry-run por segurança)
- Payload salvo em `reports/smartico_pcr_migration_push_YYYYMMDD_HHMMSS.json`

**Log esperado:**
```
Snapshot carregado: N jogadores elegiveis
1 eventos montados
Resultado: sent=1 failed=0 total=1
```

### Passo 3 — Estado DEPOIS do push

**Ação do analista:**
- Atualizar (F5) a página do perfil do jogador `30352025` no painel
- Tirar print da página completa
- Salvar em `prints/02_depois_push_30352025.png`

**Validação esperada:**
- ✅ Seção `External Markers` **NÃO contém** mais `PCR_RATING_B` (e nenhuma outra `PCR_RATING_*`)
- ✅ Seção `External Segments` **APARECE** no painel e **contém** `PCR_RATING_B`
- ✅ Outras tags (`RISK_*`, `CL02_*`, etc.) continuam inalteradas
- ✅ Nenhuma automation/jornada foi disparada (verificar via Smartico → Player → Events)

---

## Critérios de Aprovação

Para avançar da fase Canário para Amostra (10 jogadores), todos os 3 critérios abaixo devem ser verdadeiros:

- [ ] Tag `PCR_RATING_B` removida do bucket `core_external_markers`
- [ ] Tag `PCR_RATING_B` presente no bucket `core_external_segment`
- [ ] Nenhuma outra tag do perfil foi afetada (idempotência da operação confirmada)

---

## Registro de Execução

| Evento | Timestamp | Observação |
|---|---|---|
| Canário selecionado (dry-run) | 21/04/2026 10:57 BRT | Payload JSON gerado para revisão |
| Aprovação visual do payload | — | Aguardando |
| Push real executado | — | Aguardando |
| Validação visual no Smartico | — | Aguardando |
| Amostra (10 jogadores) executada | — | Pendente |
| Full executado | — | Pendente |

---

## Entregáveis finais desta validação

1. **PDF técnico** com:
   - Payload JSON enviado
   - Prints ANTES e DEPOIS do canário
   - Prints ANTES e DEPOIS da amostra (10 jogadores)
   - Logs de execução
   - Diff visual (markers → segment)
2. **Notion público** no padrão da Matriz de Risco v2: já criado em [`docs/notion_pcr_PUBLICO.md`](../notion_pcr_PUBLICO.md) — link a ser compartilhado com Castrin após validação
3. **Atualização no `README_auditoria_sql_20260420.md`** marcando item A (migração) como concluído

---

## Arquivos relacionados

- Script de migração: [`scripts/migrate_pcr_markers_to_segment.py`](../../scripts/migrate_pcr_markers_to_segment.py)
- Push regular (já v1.4): [`scripts/push_pcr_to_smartico.py`](../../scripts/push_pcr_to_smartico.py)
- Cliente Smartico com suporte a `core_external_segment`: [`db/smartico_api.py`](../../db/smartico_api.py)
- Payload do canário (dry-run): [`reports/smartico_pcr_migration_dryrun_20260421_105734.json`](../../reports/smartico_pcr_migration_dryrun_20260421_105734.json)
- Amostra da Fase 2 de ontem: [`reports/smartico_pcr_amostra_20260420.csv`](../../reports/smartico_pcr_amostra_20260420.csv)
