# Prep Reunião — Relatório de Bônus, Freespins e Cashback
## Participantes: Squad Dados + Financeiro
## Data: Março/2026

---

## 1. FONTE DOS DADOS

**P: De onde vêm esses números?**
- Banco: AWS Redshift (Pragmatic Solutions) — base transacional da plataforma
- Tabela principal: `fund.tbl_bonus_sub_fund_txn`
  - Registra toda movimentação de saldo de bônus do jogador
  - Cada linha = uma entrada ou saída de bônus na carteira
- Fuso horário: Redshift opera em UTC. Convertemos para BRT com `DATEADD(hour, -3, c_start_time)`
- Unidade monetária: valores armazenados em **centavos** (menor denominação BRL). Dividimos por 100 para obter reais.

**P: Por que essa tabela e não outra?**
- `tbl_bonus_sub_fund_txn` é a sub-ledger de bônus — registra especificamente o fluxo de dinheiro de bônus
- A tabela pai `tbl_real_fund_txn` registra o evento geral, mas o valor de bônus fica zerado em alguns tipos (ex: ISSUE_BONUS mostra R$ 0 na tabela pai, mas o valor real está na sub-tabela)
- É a fonte mais granular e precisa para incentivos

---

## 2. COLUNAS UTILIZADAS

| Coluna | Tabela | Descrição |
|---|---|---|
| `c_start_time` | tbl_bonus_sub_fund_txn | Timestamp da transação (UTC) |
| `c_txn_type` | tbl_bonus_sub_fund_txn | Código numérico do tipo de transação |
| `c_op_type` | tbl_bonus_sub_fund_txn | 'CR' = crédito (entrada), 'DB' = débito (saída) |
| `c_txn_amount` | tbl_bonus_sub_fund_txn | Valor em centavos BRL |
| `c_ecr_id` | tbl_bonus_sub_fund_txn | ID interno do jogador |

---

## 3. MAPEAMENTO DE TIPOS (c_txn_type)

### Bônus
| Código | Nome | Operação | Significado |
|---|---|---|---|
| 19 | OFFER_BONUS | CR | Bônus concedido ao jogador |
| 20 | ISSUE_BONUS | DB | Bônus convertido em real cash (wagering cumprido) |
| 30 | BONUS_EXPIRED | DB | Bônus expirou sem uso |
| 37 | BONUS_DROPPED | DB | Bônus cancelado (pelo jogador ou sistema) |
| 88 | ISSUE_DROP_AMOUNT_DEBIT | DB | Excesso debitado na conversão |

### Freespins
| Código | Nome | Operação | Significado |
|---|---|---|---|
| 80 | CASINO_FREESPIN_WIN | CR | Ganho monetário do free spin |

### Cashback
- **Não está nas tabelas de bônus.** É distribuído via BackOffice como ajuste manual (txn_type 3 = REAL_CASH_ADDITION_BY_CS), misturado com outros ajustes.

---

## 4. LÓGICA DO CÁLCULO

### Concedido (quanto a empresa deu)
```
SUM(c_txn_amount / 100.0) WHERE c_op_type = 'CR'
```
- Para Bônus: txn_type = 19 (OFFER_BONUS)
- Para Freespins: txn_type = 80 (CASINO_FREESPIN_WIN) — aqui "concedido" = valor dos wins

### Utilizado (quanto saiu da carteira de bônus)
```
SUM(c_txn_amount / 100.0) WHERE c_op_type = 'DB'
```
- txn_type 20 = convertido em real cash (jogador bateu wagering)
- txn_type 30 = expirou
- txn_type 37 = foi cancelado/dropped
- txn_type 88 = excesso debitado

---

## 5. PERGUNTAS PROVÁVEIS DO FINANCEIRO

### P: "Por que o utilizado é maior que o concedido em alguns meses?"

**R:** Porque bônus concedidos em meses anteriores ainda estavam ativos e foram utilizados naquele mês. Exemplo: jogador recebe bônus em novembro, cumpre wagering em janeiro — a concessão entra em novembro, mas a conversão entra em janeiro.

**Prova:** Em Janeiro/26, R$ 3,1M dos R$ 7,2M convertidos vieram de bônus concedidos em meses anteriores (carry-over validado via query cruzando c_ecr_id e timestamps de concessão vs conversão).

---

### P: "Como vocês validaram esses números?"

**R:** Cruzamos com os relatórios PDF dos meses anteriores:
- Dez/25: query R$ 8.117.199 vs ref R$ 8.100.624 (sem cashback) = **+0,2% de diferença**
- Jan/26: query R$ 1.344.103 vs ref R$ 1.338.725 = **+0,4% de diferença**

Diferença < 0,5% nos dois meses. A pequena variação pode ser por arredondamento ou transações na fronteira do fuso horário (UTC→BRT).

---

### P: "E o cashback? Cadê?"

**R:** O cashback (R$ 257.779 em Dez/25) é distribuído como **ajuste manual de real cash** no BackOffice da plataforma, não como bônus. Na base, cai no txn_type 3 (REAL_CASH_ADDITION_BY_CS) junto com outros ajustes manuais — não tem como separar automaticamente só o cashback.

**Ação necessária:** Solicitar à plataforma (Pragmatic) um flag ou txn_type específico para cashback, ou extrair o valor diretamente do BackOffice mensalmente.

---

### P: "O que é churn de bônus?"

**R:** É o percentual de bônus que "morreu" sem ser convertido em dinheiro real:
- **Expirado** (txn_type 30): jogador não jogou a tempo
- **Dropped** (txn_type 37): jogador cancelou ou sistema removeu

Nossos números:
- Dez/25: 1,4% de churn (excelente)
- Jan/26: 9,6% (aceitável)
- Fev/26: 8,9% (bom)

Isso significa que **90-98% dos bônus são efetivamente utilizados** pelos jogadores.

---

### P: "Qual a diferença entre o relatório antigo e esse?"

**R:** O relatório antigo (PDFs) misturava concedido e utilizado em um valor só. Agora separamos:
- **Concedido** = quanto a empresa distribuiu de incentivo (custo do lado da empresa)
- **Utilizado** = quanto o jogador efetivamente consumiu (converteu, expirou ou cancelou)

Isso permite entender a eficiência das campanhas: se o concedido é alto mas o convertido é baixo, as campanhas estão gerando custo sem retorno.

---

### P: "Esses R$ 1,67M de Fevereiro — é muito ou pouco?"

**R:** Contexto comparativo:

| Mês | Concedido Total |
|---|---|
| Dez/25 | R$ 8.117.199 (mês atípico — Natal/Ano Novo) |
| Jan/26 | R$ 1.344.103 |
| Fev/26 | R$ 1.671.216 |

Fevereiro está ~24% acima de Janeiro, o que pode indicar campanhas mais agressivas ou Carnaval. Comparar com o GGR do mês para avaliar o ROI promocional (ideal: Promo Cost < 15-20% do GGR).

---

### P: "O valor de freespin é concedido ou utilizado?"

**R:** O valor que reportamos para Freespins (R$ 272.115 em Fev) representa o **custo real dos freespins** = quanto os jogadores ganharam jogando os free spins (CASINO_FREESPIN_WIN). Não é o "número de rodadas grátis", é o valor monetário que saiu para o jogador.

Não existe "concedido" monetário de freespin — a concessão é em número de rodadas, o custo só se materializa quando o jogador joga e ganha.

---

### P: "Vocês conseguem abrir isso por campanha ou por segmento?"

**R:** Com a estrutura atual, conseguimos abrir por jogador (c_ecr_id). Para abrir por campanha/segmento, precisamos cruzar com:
- `bonus.tbl_ecr_bonus_details` → vincula o bônus ao `c_bonus_id` (campanha)
- `bonus.tbl_bonus_segment_details` → vincula campanha ao segmento CRM

É factível como próximo passo se necessário.

---

## 6. QUERY UTILIZADA (para referência)

```sql
SELECT
    DATE_TRUNC('month', DATEADD(hour, -3, c_start_time))::DATE AS mes,
    CASE
        WHEN c_txn_type IN (19, 20, 30, 37, 88) THEN 'BONUS'
        WHEN c_txn_type = 80                     THEN 'FREESPINS'
    END AS tipo_incentivo,
    SUM(CASE WHEN c_op_type = 'CR' THEN c_txn_amount ELSE 0 END) / 100.0
        AS total_concedido_brl,
    SUM(CASE WHEN c_op_type = 'DB' THEN c_txn_amount ELSE 0 END) / 100.0
        AS total_utilizado_brl
FROM fund.tbl_bonus_sub_fund_txn
WHERE DATEADD(hour, -3, c_start_time) >= '2025-12-01'
  AND DATEADD(hour, -3, c_start_time) <  '2026-03-01'
  AND c_txn_amount > 0
  AND c_txn_type IN (19, 20, 30, 37, 88, 80)
GROUP BY 1, 2
ORDER BY 1, 2
```

---

## 7. RESUMO EXECUTIVO (se pedirem em 30 segundos)

> "Validamos os relatórios de Dezembro e Janeiro com menos de 0,5% de diferença.
> Geramos o fechamento inédito de Fevereiro: R$ 1,67M concedidos em bônus e freespins,
> com R$ 1,51M efetivamente utilizados pelos jogadores. O churn de bônus está em 8,9%,
> indicando campanhas saudáveis. O único ponto aberto é o cashback, que é distribuído
> pelo BackOffice fora das tabelas de bônus e precisa ser confirmado com a plataforma."