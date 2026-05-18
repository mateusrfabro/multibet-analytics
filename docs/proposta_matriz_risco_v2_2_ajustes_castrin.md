# Matriz de Risco v2.2 — 3 novas tags aprovadas pelo Castrin (15/05/2026)

**Autor:** Mateus
**Status:** APROVADO pelo Head em 15/05/2026 via WhatsApp. Pronto para implementacao.

---

## TL;DR

3 tags novas, foco em deteccao do exploit de cancelamento (MINES + outros). Player 30311442 (caso real do exploit) cai em 1 das 3. Os 30 abusers sistematicos da base caem em 2-3 simultaneamente.

| # | Tag | Score | Criterio | Players estimados |
|---|---|---:|---|---:|
| 1 | **`MINES_PENDING_FRAUD`** | -10 / -30 | bet=cancel mesmo valor exato em <=15min + MINES (c_game_category=158) | ~150-330 |
| 2 | **`CANCEL_HEAVY_DAILY`** | -10 | >=10 cancelamentos (c_txn_type=72) em UM dia | 1.349 |
| 3 | **`CRASH_FARMER`** (soft) | -5 / -10 / -15 | 2+ jogos simultaneos onde >=1 e crash, gradiente por volume | ~1.040 |

---

## 1. Contexto

### 1.1 Demanda original (Castrin 14/05/2026)

> "matriz de risco da multi, vamos precisar fazer algum ajuste, 1 deles pra mim ta certo, que e a inclusao de cancelamento (imagino que e rollover) em regra pra negativar."

> "o 2o e sobre a fraude de ontem [...] id smartico: 30311442. esse player ai fez o abuse. ele ganhou x giros no jely de 0,8 centavos, acho que uns 140 giros algo assim. ele foi no mines e travou o saldo la, foi no jelly e zerou os giros deles. dps ele foi e atualizou a tela do mines, e o saldo dele voltou."

> "mesma sessao em 2 jogos diferentes CRASH. e pra ela funcionar pra mais jogos [...] -20 por exemplo."

### 1.2 Validacoes/decisoes Castrin (15/05/2026)

Apos investigacao empirica + calibracao na base 30d, Castrin confirmou via WhatsApp:

- ✅ Assinatura do exploit MINES validada: `bet (27) + cancel (72) MESMO valor exato em <=15min`
- ✅ Confirmacao que **cancelamento = `c_txn_type=72`** (nao rollover/wagering)
- ✅ Nova regra simples: **">=10 cancelamentos no dia ja da -10 pro maluco"**
- ✅ CRASH_FARMER aprovado **mas "mais soft pq tem nego maniaco que joga varios ao mesmo tempo"**
- ❌ Cortar: decomposicao do ROLLBACK_PLAYER, BONUS_DROPPED_HEAVY, CASHOUT_CANCELLER, WIN_CLAWBACK_FLAG (nao pediu)

---

## 2. Tag 1 — MINES_PENDING_FRAUD

### Mecanica do exploit

Player aposta no MINES (categoria 'crash', c_game_id=8372). O MINES so persiste o BUYIN no backend quando o player clica em pelo menos 1 tile. Enquanto a rodada esta aberta no front, o saldo aparenta debitado mas a transacao ainda nao foi escrita. Player vai pra outro jogo (slot/freespin), executa, depois F5 no MINES -> o estado pendente cai -> o saldo do MINES volta. Resultado: saldo restaurado + winnings do slot.

### Regra de deteccao

```
Par exploit = bet (c_txn_type=27, valor >= R$ 100, SUCCESS)
            + cancel (c_txn_type=72, MESMO valor exato, SUCCESS)
            + MESMO c_game_id
            + janela <= 15 minutos
            + c_game_category = 158 (MINES)
```

### Niveis

| Nivel | Criterio | Score | Players |
|---|---|---:|---:|
| **L1 — watchlist** | 1 par em 7d | 0 (alerta apenas) | ~260 |
| **L2 — suspeito** | 2+ pares em 30d **OU** 1 par + cashout reversal recorrente | -10 | ~150 |
| **L3 — abuser sistematico** | sinal composto (par + freespin co-ocorrente) >=10x em 30d | -30 | ~30 |

### Validacao empirica

- **30311442:** bet R$ 400 + cancel R$ 400, gap 35s, em 13/05 18:50:54 BRT, c_game_id=8372 (MINES) -> **cai em L2** (1 par + 4 cashout reversal)
- **Top 30 sistematicos:** 100-401 ocorrencias do sinal composto em 30d -> caem em L3

---

## 3. Tag 2 — CANCEL_HEAVY_DAILY

### Regra de deteccao

```
Player com >= 10 transacoes c_txn_type=72 (Cancel buyin)
status SUCCESS em qualquer dia (BRT) na janela de 30 dias.
```

### Score

| Criterio | Score | Players |
|---|---:|---:|
| max(cancels/dia) >= 10 em 30d | -10 | **1.349** (~2,3% base) |

### Calibracao na base 30d

| Faixa | Players |
|---|---:|
| 1/dia max | 1.762 |
| 2-4/dia | 1.720 |
| 5-9/dia | 931 |
| **>=10/dia (threshold Castrin)** | **1.349** |

### Por que >=10/dia funciona

- Cancel pontual e comum (mudou de ideia em jogo de mesa) — 4.413 players com 1-9/dia ficam de fora
- 10+ cancels num dia eh comportamento fora do normal — pode ser exploit, bot, ou padrao de farming
- 30311442 fez 2 cancels em 13/05 -> NAO cai (correto, exploit dele foi pontual). A regra captura quem repete

---

## 4. Tag 3 — CRASH_FARMER (soft)

### Regra de deteccao

```
Player com >= N "overlaps" em 30d, onde overlap = 
  bin de 15min com >= 2 c_game_id distintos E pelo menos 1 com c_game_category=158
```

### Niveis (3 camadas pra ser "soft")

| Nivel | Criterio | Score | Players |
|---|---|---:|---:|
| **L1 — multitasker leve** | 6-20 overlaps em 30d | -5 | 912 |
| **L2 — multitasker pesado** | 21-100 overlaps em 30d | -10 | 124 |
| **L3 — farmer real** | >100 overlaps em 30d | -15 | 4 |

Players com 1-5 overlaps em 30d (~8.000) ficam de fora — Castrin pediu "soft" e isso evita pegar o "nego maniaco" casual.

### Pendencia tecnica

`c_game_category=158` foi confirmado empiricamente como MINES (Hacksaw, c_game_id=8372). Pra cobertura completa de jogos crash precisaria mapear:
- Aviator
- Mines+ (Pragmatic)
- Spaceman
- Plinko
- etc

Lista completa em `multibet.game_image_mapping WHERE game_category='crash'` (21 jogos). Mas o mapeamento `c_game_category integer` -> `game_category string` ainda nao foi resolvido. **Sugestao:** subir como esta (c_game_category=158) e expandir num round 2 quando tivermos o mapping.

---

## 5. Implementacao

### Arquivos a criar/editar

| Arquivo | Acao |
|---|---|
| `ec2_deploy/sql/risk_matrix/MINES_PENDING_FRAUD.sql` | Criar |
| `ec2_deploy/sql/risk_matrix/CANCEL_HEAVY_DAILY.sql` | Criar |
| `ec2_deploy/sql/risk_matrix/CRASH_FARMER.sql` | Criar |
| `scripts/sql/risk_matrix/*.sql` (mirror) | Criar |
| `ec2_deploy/pipelines/risk_matrix_pipeline.py` | Adicionar 3 tags no `TAG_ORDER`, `TAG_TO_COLUMN`, `TAG_SCORES` + descricoes na legenda |
| `docs/notion_risk_matrix_*.md` | Atualizar com 3 tags novas (apos validacao) |

### Sequencia de execucao

```
1. Criar os 3 SQLs em ec2_deploy/sql/risk_matrix/
2. Atualizar risk_matrix_pipeline.py (TAG_ORDER + TAG_SCORES)
3. Smoke test --dry-run com --only "MINES_PENDING_FRAUD CANCEL_HEAVY_DAILY CRASH_FARMER"
4. Validar empiricamente:
   - 30311442 dispara MINES_PENDING_FRAUD L2 ✓
   - Top 30 sistematicos disparam L3 ✓
   - Total players flagados bate com calibracao (~3.700)
5. Commit + push + handoff Gusta
6. Atualizar docs/notion_risk_matrix_v2.md + PUBLICO + IMPORT
7. Smartico push (dia seguinte automatico)
```

---

## 6. Apendice — scripts de validacao

```bash
# Investigacao empirica do 30311442
python scripts/investiga_fraude_30311442.py
python scripts/diag_assinatura_30311442.py
python scripts/confirma_game_category_30311442.py

# Calibracao das tags na base (30d)
python scripts/calibra_tags_v2_2_round2.py          # MINES_PENDING_FRAUD
python scripts/calibra_cancel_heavy_e_crash_farmer.py  # CANCEL_HEAVY + CRASH_FARMER
```

Logs gerados:
- [logs/investiga_fraude_30311442.log](../logs/investiga_fraude_30311442.log)
- [logs/diag_assinatura_30311442.log](../logs/diag_assinatura_30311442.log)
- [logs/calibra_tags_v2_2_round2.log](../logs/calibra_tags_v2_2_round2.log)
- [logs/calibra_cancel_heavy_e_crash_farmer.log](../logs/calibra_cancel_heavy_e_crash_farmer.log)
- [logs/confirma_game_category_30311442.log](../logs/confirma_game_category_30311442.log)
- [logs/fecha_dados_v2_2.log](../logs/fecha_dados_v2_2.log)

---

**Proximo passo:** implementar SQLs + pipeline + smoke test. Castrin ja validou o desenho, nao precisa nova aprovacao.
