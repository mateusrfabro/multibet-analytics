# Validacao de Status de Conta: Redshift (PGS) vs Smartico

**Data da analise:** 11/03/2026
**Responsavel:** Mateus Fabro -- Squad Intelligence Engine
**Periodo analisado:** 01/01/2026 a 11/03/2026
**Volume:** 187.408 contas com mudanca de status no periodo
**Match entre bases:** 100% (todas as contas do Redshift encontradas na Smartico)

---

## 1. Objetivo

Validar se o status de conta no Redshift (base verdade / PGS) esta sincronizado com os campos da Smartico (CRM):
- **Frente 1:** `c_category` (Redshift) vs `core_external_account_status` (Smartico)
- **Frente 2:** `c_category` (Redshift) vs `core_account_status` (Smartico)

---

## 2. Metodologia

- **Base verdade:** Redshift `bireports.tbl_ecr.c_category` (status atual da conta no PGS)
- **Filtro:** Contas com `c_category_updated_time` entre 01/01/2026 e 11/03/2026
- **Join:** Redshift `c_external_id` = Smartico `user_ext_id`
- **Mapeamento esperado (Frente 2):** real_user/play_user -> ACTIVE | closed -> DEACTIVATED | suspended -> SUSPENDED | fraud -> BLOCKED | rg_closed/rg_cool_off -> ACTIVE
- **Validacao temporal:** `update_date` da Smartico comparado com `c_category_updated_time` do Redshift para verificar se a Smartico recebeu atualizacoes apos a mudanca de status
- **Script reproduzivel:** `validacoes/validar_status_conta.py`

---

## 3. Resultados

### Frente 1 -- c_category (Redshift) vs core_external_account_status (Smartico)

| Resultado | Quantidade | % |
|---|---|---|
| Consistentes | 131.353 | 70,1% |
| **Divergentes** | **56.055** | **29,9%** |

**Principais divergencias:**

| Status no Redshift (base verdade) | Status na Smartico (CRM) | Qtd | Observacao |
|---|---|---|---|
| play_user | ACTIVE | 28.510 | Smartico nao recebeu a granularidade play/real -- mostra generico ACTIVE |
| real_user | ACTIVE | 18.114 | Idem |
| closed | ACTIVE | 7.033 | **CRITICO** -- conta fechada no Redshift aparece como ACTIVE na Smartico |
| rg_closed | TRUE | 1.563 | Valor "TRUE" nao e um status valido -- mapeamento incorreto na integracao |
| rg_cool_off | ACTIVE | 575 | Conta em pausa (cool off) no Redshift aparece como ACTIVE na Smartico |

### Frente 2 -- c_category (Redshift) vs core_account_status (Smartico, com mapeamento)

| Resultado | Quantidade | % |
|---|---|---|
| Consistentes | 172.978 | 92,3% |
| **Divergentes** | **14.430** | **7,7%** |

**Principais divergencias:**

| Status no Redshift (base verdade) | Esperado na Smartico | Status real na Smartico | Qtd | Observacao |
|---|---|---|---|---|
| closed | DEACTIVATED | ACTIVE | 12.317 | **CRITICO** -- conta fechada no Redshift, mas Smartico mostra ACTIVE |
| rg_closed | ACTIVE | SELF_EXCLUDED | 1.563 | Redshift marca como rg_closed, Smartico como SELF_EXCLUDED -- conceitualmente aceitavel |
| rg_cool_off | ACTIVE | SELF_EXCLUDED | 542 | Redshift marca como rg_cool_off, Smartico como SELF_EXCLUDED -- conceitualmente aceitavel |
| fraud | BLOCKED | ACTIVE | 5 | Conta marcada como fraude no Redshift, mas aparece ACTIVE na Smartico |

---

## 4. Analise Temporal: Quebra de Sincronizacao

A analise por semana revelou que a integracao **funcionava corretamente ate meados de fevereiro** e **parou de atualizar** a partir da semana 7 (~10/02).

### Taxa de sincronizacao do core_account_status (Smartico) por semana

| Semana | Periodo aprox. | closed | play_user | real_user | rg_closed | rg_cool_off |
|---|---|---|---|---|---|---|
| 1 | 01/01 - 05/01 | 99,9% | 100% | 100% | 99,6% | 100% |
| 2 | 06/01 - 12/01 | 99,7% | 100% | 100% | 99,8% | 100% |
| 3 | 13/01 - 19/01 | 99,3% | 100% | 100% | 99,8% | 100% |
| 4 | 20/01 - 26/01 | 99,7% | 100% | 100% | 100% | 100% |
| 5 | 27/01 - 02/02 | 99,6% | 100% | 100% | 100% | 98,1% |
| 6 | 03/02 - 09/02 | 99,6% | 100% | 100% | 100% | 100% |
| **7** | **10/02 - 16/02** | **65,8%** | 100% | 100% | **78,4%** | **73,9%** |
| **8** | **17/02 - 23/02** | **12,7%** | 100% | 100% | **9,6%** | **13,4%** |
| 9 | 24/02 - 02/03 | 14,1% | 100% | 100% | 5,0% | 6,7% |
| 10 | 03/03 - 09/03 | 15,6% | 100% | 100% | 6,7% | 10,1% |
| 11 | 10/03 - 11/03 | 15,6% | 100% | 100% | 7,0% | 1,3% |

**Ponto-chave:** Categorias `play_user` e `real_user` continuam 100% sincronizadas no `core_account_status` (ambas mapeiam para ACTIVE). A quebra afeta especificamente `closed`, `rg_closed` e `rg_cool_off` -- categorias que exigem uma **mudanca real de status** na Smartico.

### Taxa de sincronizacao do core_external_account_status (Smartico) por semana

| Semana | Periodo aprox. | closed | play_user | real_user | rg_closed | rg_cool_off |
|---|---|---|---|---|---|---|
| 1-6 | 01/01 - 09/02 | >99% | >98% | >99% | >99% | >98% |
| **7** | **10/02 - 16/02** | **91,7%** | **49,6%** | **91,0%** | **77,8%** | **68,2%** |
| **8** | **17/02 - 23/02** | **63,3%** | **0,0%** | **18,5%** | **9,1%** | **5,2%** |
| 9-11 | 24/02 - 11/03 | 36-41% | 0,0% | ~20% | 5-7% | 1-3% |

A partir da semana 8, `play_user` parou 100% de sincronizar no `external_account_status`. Este campo e o mais afetado pela quebra.

---

## 5. Investigacao de Causa Raiz

### 5.1 A Smartico recebe atualizacoes, mas o status NAO muda

Analisamos o campo `update_date` da Smartico (ultima atualizacao de qualquer dado do jogador) e comparamos com a data de fechamento no Redshift:

| Grupo | Qtd | Smartico atualizou DEPOIS? | Diferenca media |
|---|---|---|---|
| Corretas (closed no Redshift, DEACTIVATED na Smartico) | 23.319 | **100%** | +43,5 dias |
| Divergentes (closed no Redshift, ACTIVE na Smartico) | 12.317 | **100%** | +13,0 dias |

**Conclusao:** A Smartico **continua recebendo dados** dessas contas (stats financeiras, login, etc.), pois o `update_date` e posterior ao fechamento. O que parou de funcionar e especificamente **a propagacao do campo de status**.

### 5.2 Dois problemas distintos identificados

A investigacao revelou que as 12.317 contas divergentes vem de **dois problemas independentes**:

#### Problema 1: Re-processamento SIGAP nao atualiza Smartico (9.183 contas)

Contas que ja estavam `closed` no Redshift e foram re-processadas pelo SIGAP (closed -> closed):

| Tipo de mudanca | Total | Divergentes | % |
|---|---|---|---|
| closed -> closed (re-processamento) | 13.104 | 9.183 | **70,1%** |
| play_user -> closed (mudanca real) | 20.209 | 2.909 | 14,4% |
| real_user -> closed (mudanca real) | 2.188 | 212 | 9,7% |

**Causa provavel:** Quando o SIGAP re-processa uma conta que ja tem status `closed`, o Redshift registra a atualizacao (closed -> closed), mas a integracao com a Smartico **nao dispara evento** porque nao houve mudanca real de categoria. Essas contas provavelmente ja estavam incorretas na Smartico antes do periodo analisado.

| Semana | Re-processamentos | Divergentes | % |
|---|---|---|---|
| 1-6 | 2.381 | 6 | 0,3% |
| **7** | **330** | **49** | **14,8%** |
| **8-11** | **10.393** | **9.128** | **87,8%** |

O volume de re-processamentos SIGAP aumentou significativamente a partir da semana 8. A combinacao de maior volume + quebra na integracao resultou nos 9.183 divergentes.

#### Problema 2: Quebra na integracao a partir da semana 7 (3.134 contas)

Mudancas reais de status (play_user/real_user -> closed) que **deveriam** ter sido propagadas para a Smartico:

| Semana | Mudancas reais | Divergentes | % |
|---|---|---|---|
| 1-6 | 17.625 | 62 | **0,3%** |
| **7** | **3.568** | **1.285** | **36,0%** |
| **8** | **1.984** | **1.727** | **87,0%** |
| 9-11 | 355 | 60 | 16,9% |

**Semanas 1-6: taxa de erro de 0,3% (normal/aceitavel).**
**Semana 7 em diante: a sincronizacao quebrou.** A semana 8 atinge 87% de divergencia.

---

## 6. Detalhamento: Contas Closed

Das 35.636 contas com status `closed` no Redshift no periodo:

| Origem (no Redshift) | Qtd | % que aparece ACTIVE na Smartico | Risco |
|---|---|---|---|
| Ja era closed, re-processada pelo SIGAP | 13.104 | 70,1% (9.183) | Alto -- Smartico nao refletiu o fechamento original |
| Mudou de play_user para closed | 20.209 | 14,4% (2.909) | Alto -- fechamento recente no Redshift nao refletiu na Smartico |
| Mudou de real_user para closed | 2.188 | 9,7% (212) | Alto -- idem |
| Mudou de rg_cool_off para closed | 43 | 30,2% (13) | Medio |
| Mudou de rg_closed para closed | 91 | 0,0% (0) | OK |

**Motivos do fechamento (100% via SIGAP_CHECK_ON_LOGIN):**
- `CLOSED_FOR_SOCIAL_WELFARE_PROGRAM` -- beneficiarios CadUnico/Bolsa Familia
- `CLOSED_FOR_NATIONAL_SE_REGISTER` -- cadastro nacional de autoexclusao

**Impacto:** Contas que estao fechadas no Redshift (base verdade) por determinacao regulatoria continuam aparecendo como ACTIVE na Smartico (CRM). Isso significa que essas contas podem estar recebendo comunicacoes de CRM (push, email, bonus), o que configura risco de compliance.

---

## 7. Conclusoes

1. **Semanas 1 a 6 (01/01 a 09/02): a integracao funcionava corretamente**, com taxa de sincronizacao >99% em todas as categorias. Isso confirma que o fluxo funciona por design.

2. **Semana 7 em diante (~10/02): houve uma quebra na propagacao de status** do Redshift para a Smartico. A Smartico continua recebendo outros dados (stats, login), mas o campo de status especificamente parou de ser atualizado.

3. **Sao dois problemas distintos:**
   - **Problema 1 (9.183 contas):** Re-processamentos SIGAP (closed -> closed) nao disparam atualizacao de status na Smartico. Essas contas provavelmente ja estavam incorretas antes.
   - **Problema 2 (3.134 contas):** Mudancas reais de status (play_user/real_user -> closed) pararam de sincronizar a partir da semana 7.

4. **12.317 contas fechadas no Redshift por motivo regulatorio (SIGAP) continuam como ACTIVE na Smartico (CRM)** -- risco de compliance.

5. **5 contas marcadas como `fraud` no Redshift aparecem como ACTIVE na Smartico** -- volume baixo mas gravidade alta.

6. **O campo `core_external_account_status` e o mais afetado** (29,9% de divergencia total). Alem da quebra de sync, apresenta mapeamentos incorretos (valor "TRUE" para rg_closed, "ACTIVE" generico onde deveria ter PLAY_USER/REAL_USER).

---

## 8. Recomendacoes

1. **Urgente -- Investigar quebra na semana 7:** O time de integracao/engenharia precisa identificar o que mudou por volta de 10-16/02 que impactou a propagacao de status Redshift -> Smartico.

2. **Urgente -- Corrigir as 12.317 contas divergentes:** Contas que estao `closed` no Redshift mas aparecem como ACTIVE na Smartico precisam ser corrigidas para evitar comunicacoes indevidas a contas regulatoriamente fechadas.

3. **Avaliar re-processamentos SIGAP:** Verificar se o fluxo de integracao deveria tratar re-processamentos (closed -> closed) como evento de atualizacao, garantindo que contas ja fechadas estejam corretamente refletidas na Smartico.

4. **Correcao de mapeamento:** Revisar o mapeamento de `rg_closed` que envia "TRUE" como `external_account_status` na Smartico em vez de "RG_CLOSED".

5. **Monitoramento:** Apos correcao, re-rodar esta validacao (`validacoes/validar_status_conta.py`) para confirmar que a sincronizacao voltou ao padrao >99%.

---

## 9. Arquivos de Suporte

Todos em `validacoes/`:

| Arquivo | Descricao |
|---|---|
| `validar_status_conta.py` | Script reproduzivel da validacao completa |
| `divergencias_status_*.csv` | Todas as contas divergentes com detalhes |
| `cross_status_completo_*.csv` | Tabela cruzada de todas as combinacoes de status |
| `sync_ext_status_por_semana_*.csv` | Taxa de sync do external_account_status por semana |
| `sync_acc_status_por_semana_*.csv` | Taxa de sync do account_status por semana |
| `detalhe_contas_closed_*.csv` | Breakdown das contas closed |
| `causa_raiz_por_tipo_*.csv` | Divergencia por tipo de mudanca (old_category) |
| `causa_raiz_mudancas_reais_semana_*.csv` | Divergencia por semana (mudancas reais) |
| `causa_raiz_reprocessamento_semana_*.csv` | Divergencia por semana (re-processamentos) |
| `dataset_completo_*.csv` | Dataset completo (187k registros) para analise ad-hoc |
