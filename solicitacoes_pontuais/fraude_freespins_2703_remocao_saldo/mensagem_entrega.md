# Mensagem de entrega — Remoção de Saldo / Fraude Freespins 27/03/2026

Boa tarde,

Segue a base de saldos extraída para os IDs sinalizados na demanda. Pontos relevantes e como tratamos cada um:

## Cobertura do cohort

Do arquivo recebido com 3.118 registros, identificamos 17 IDs duplicados e 2 vazios na limpeza padrão. Dos restantes:

- **3.092 IDs únicos** processados e 100% mapeados em `ps_bi.dim_user` — todos os tamanhos de ID presentes na planilha (15, 8 e 6 dígitos) são `external_id` Smartico válidos.
- **7 IDs em notação científica** que o Excel não preserva (formato corrompe os últimos dígitos de IDs longos quando salvos como número). Estão listados na aba `Auditoria`. **Sugestão:** se possível, encaminhar a base original em CSV ou TXT — em 5 minutos recuperamos esses 7 e refazemos o snapshot incremental.

## Bônus convertido em saldo real (27/03)

Extraído pelo campo `c_actual_issued_amount` em `bonus_ec2.tbl_bonus_summary_details`, filtrado por data de emissão BRT.

**Validação empírica realizada no banco para o dia 27/03 (universo total, não cohort):**

| Hipótese para "Total Bonus Cost" | Valor |
|---|---|
| Actual Issued Amount (campo usado) | R$ 514.611,21 |
| + Freespin Win | R$ 501.771,02 *(redundante: para freespins, é o mesmo dinheiro)* |
| Soma das duas | R$ 1.016.382,23 *(dobra o valor — não usar)* |

A coluna `c_actual_issued_amount` é a métrica defensável: captura issued tanto de freespins quanto de cash bonus, e para freespins ela já equivale ao `c_freespin_win` (overlap empírico de 99,94% em 21.813 registros do dia). Somar os dois duplica.

**Para o cohort dos 3.092 IDs mapeados**, o total de bônus convertido em saldo real em 27/03 foi de **R$ 441.525,61** em **2.213 jogadores** com valor > 0. Há ainda 4 registros adicionais de bônus emitidos no dia para esse cohort com `c_actual_issued_amount = 0` (bônus offered/cancelado no mesmo dia, sem conversão efetiva) — total de 2.217 registros, mas só 2.213 representam dinheiro real a remover. Os 879 IDs restantes do cohort não têm bônus emitido em 27/03 no banco (categoria `SEM_BONUS_2703` na aba `Remocao_Saldo` — recomenda-se verificação individual).

**Sugestão de validação no BKO Pragmatic:** abrir Resumo > Financial Summary do dia 27/03 e conferir o campo "Total Bonus Cost". Cohort vs universo do dia: nosso recorte cobre 85,8% (R$ 441,5k de R$ 514,6k) — coerente, dado que a triagem manual anterior já retirou parte. Caso prefiram validar por ID individual, posso comparar um caso pontual.

## Risco operacional na remoção (coluna `risco_remocao`)

Classificamos cada linha de acordo com o que será viável remover:

| Status | Qtd | Significado | Ação |
|---|---|---|---|
| `OK_REMOVER_TOTAL` | **855** | Saldo atual ≥ bônus convertido | Remoção integral viável |
| `REMOCAO_PARCIAL` | **587** | Saldo atual < bônus convertido | Remover apenas o que sobrou — jogador gastou parte |
| `DINHEIRO_JA_SACADO` | **771** | Saldo zerado + saque pós-27/03 ≥ bônus | **Dinheiro já saiu do caixa — decisão de cobrança/contencioso** |
| `SEM_BONUS_2703` | **879** | Está na planilha, sem bônus em 27/03 no banco | Verificar individualmente |

**Implicação importante:** 771 jogadores (R$ 3,66M sacados pós-27/03 no agregado do cohort) já moveram dinheiro pra fora. Para esses, a remoção do saldo não resolve mais — requer decisão jurídica/operacional do time de Risco sobre cobrança.

**Observação adicional sobre `REMOCAO_PARCIAL`:** 450 dos 587 jogadores nessa categoria têm saldo atual entre R$ 0,01 e R$ 0,99 — a remoção operacional vai retornar valores em centavos, mas categoria correta tecnicamente (saldo > 0 e < bônus convertido). Se quiserem ignorar saldos < R$ 1 para simplificar a operação, podemos reclassificar — fica a critério do time de Risco.

## Casos com saldo zerado (etapa 2 — desbloqueio)

Conforme solicitado, mantivemos na base os jogadores com saldo zerado. A coluna `saldo_zero` (Sim/Não) já marca essa condição e a coluna `ecr_status` traz o status atual da conta (bloqueada/ativa) para acelerar a triagem de desbloqueio.

## Rentabilidade lifetime (etapa 3 — reativação)

Incluídas as colunas `ggr_lifetime_brl`, `ngr_lifetime_brl` e flag `rentavel_lifetime` (NGR > 0). Base já pronta para a campanha de reativação pré-Copa sem precisar de nova extração.

## Rastreabilidade do snapshot

- Hash SHA256: `f0516e7516046e53`
- Timestamp da extração: `2026-05-13 19:17`
- Arquivo de metadata JSON acompanha o Excel
- Pipeline reprodutível versionado em `solicitacoes_pontuais/fraude_freespins_2703_remocao_saldo/`

Caso surja questionamento durante a remoção, reproduzimos exatamente os mesmos dados.

## Pontos que precisam de validação externa (resumo)

1. **"Total Bonus Cost" no BKO Pragmatic vs R$ 441.525,61 do cohort** — qualquer pessoa com acesso ao Pragmatic consegue fazer essa conferência em 1 minuto. Já temos o universo do dia 27/03 (R$ 514.611,21) caso queiram bater o número agregado.
2. **7 IDs em notação científica** — arquivo original em CSV/TXT resolve.
3. **879 IDs `SEM_BONUS_2703`** — verificação caso a caso no BKO Pragmatic (podem ter recebido por outro canal/data, ou estar no escopo errado da planilha).

Estamos à disposição.

---

# Anexos

- `remocao_saldo_fraude_freespins_2703_<timestamp>.xlsx`
  - `Remocao_Saldo`: 3.092 jogadores com saldo, bônus, saques, NGR/GGR, risco_remocao
  - `Auditoria`: resumo numérico + IDs corrompidos + distribuição por risco + IDs sem bônus
  - `Legenda`: dicionário de colunas + glossário + fontes técnicas
- `remocao_saldo_fraude_freespins_2703_<timestamp>_metadata.json`: hash + contagens + totais
