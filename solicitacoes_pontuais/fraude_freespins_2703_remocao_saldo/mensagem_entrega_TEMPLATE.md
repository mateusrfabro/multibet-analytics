# Mensagem de entrega — Remoção de Saldo / Fraude Freespins 27/03/2026

> Template profissional, sem nomes. Padrão: fato → impacto → ação sugerida.
> Substituir [PLACEHOLDERS] com valores reais após rodar o pipeline.

---

Boa tarde,

Segue a base de saldos extraída para os IDs sinalizados na demanda de remoção. Pontos relevantes e como tratamos cada um:

## Cobertura do cohort

Do arquivo recebido com 3.118 registros, identificamos 18 IDs duplicados e 2 vazios na limpeza padrão. Dos restantes:

- **[X] IDs mapeados** no banco interno e processados (aba `Remocao_Saldo`)
- **[Y] IDs não localizados** em `ps_bi.dim_user` — listados na aba `Auditoria` para verificação individual
- **7 IDs vieram em notação científica** do Excel, formato que não preserva os dígitos finais de IDs longos. **Sugestão:** se possível, encaminhar a base original em CSV ou TXT para recuperarmos esses 7 antes da remoção definitiva. Estão também na aba `Auditoria`.

## Bônus convertido em saldo real (27/03)

Extraído pelo campo canônico `c_actual_issued_amount` em `bonus_ec2.tbl_bonus_summary_details`, filtrado por data de emissão em BRT.

**Validação empírica realizada no banco para 27/03:**

| Hipótese para "Total Bonus Cost" | Valor (universo total do dia) |
|---|---|
| Actual Issued Amount (campo usado) | R$ 514.611,21 |
| + Freespin Win | R$ 501.771,02 *(redundante: para freespins, é o mesmo dinheiro)* |
| Soma das duas | R$ 1.016.382,23 *(dobra o valor, não usar)* |

A coluna `c_actual_issued_amount` é a métrica correta porque captura issued tanto de freespins quanto de cash bonus, e para freespins ela já equivale ao `c_freespin_win` (overlap empírico de 99,94% em 21.813 registros). Somar os dois duplica o valor real.

**Para o cohort dos 3.117 IDs**, o total de bônus convertido em saldo real em 27/03 foi de **R$ [TOTAL_BONUS_COHORT]** em [QTD_COM_BONUS] jogadores. **Sugestão de validação:** abrir o BKO Pragmatic em Resumo > Financial Summary do dia 27/03 e conferir o campo "Total Bonus Cost". Caso prefiram validar por ID individual, podemos comparar um caso pontual (ex.: ID `[ID_EXEMPLO]`, valor no banco R$ [VALOR_EXEMPLO]).

## Risco operacional na remoção (coluna `risco_remocao`)

Identificamos casos onde a remoção integral não será viável e classificamos cada linha:

| Status | Qtd | Significado |
|---|---|---|
| `OK_REMOVER_TOTAL` | [N1] | Saldo atual ≥ bônus convertido — remoção integral viável |
| `REMOCAO_PARCIAL` | [N2] | Saldo atual < bônus convertido — jogador já gastou parte, só dá pra remover o que sobrou |
| `DINHEIRO_JA_SACADO` | [N3] | Saldo zerado + saque pós-27/03 ≥ bônus convertido — **dinheiro já saiu do caixa** |
| `SALDO_INSUFICIENTE` | [N4] | Saldo zerado, sem saque equivalente (perdeu apostando) |
| `SEM_BONUS_2703` | [N5] | ID na planilha mas sem bônus emitido em 27/03 no banco — investigar individualmente |

**Implicação:** para os casos `DINHEIRO_JA_SACADO`, a remoção do saldo não é mais possível operacionalmente — esses representam passivo que requer decisão de cobrança/contencioso. Valor total já sacado pós-27/03 neste cohort: **R$ [TOTAL_SACADO]** em [N_SACOU] jogadores.

## Casos com saldo zerado (atendendo etapa 2)

Conforme solicitado, mantivemos na base os jogadores com saldo zerado para análise de desbloqueio. São **[N_SALDO_ZERO] jogadores** com saldo total < R$ 1. Já incluímos a coluna `ecr_status` (status atual da conta) para acelerar a triagem.

## Rentabilidade lifetime (preparação para etapa 3)

Adicionamos GGR e NGR históricos por jogador, mais flag `rentavel_lifetime` (NGR > 0). Já fica pronto para a campanha de reativação pré-Copa sem demandar nova extração.

## Rastreabilidade do snapshot

A base está versionada com:

- Hash SHA256 dos dados: `[HASH]`
- Timestamp da extração: `[TIMESTAMP]`
- Arquivo JSON de metadata acompanhando o Excel
- Pipeline reprodutível versionado no repositório (caminho no commit)

Caso surja qualquer questionamento durante o processo de remoção, conseguimos refazer a extração reproduzindo exatamente esse mesmo conjunto de dados.

## Pontos que precisam de validação externa

1. **Confirmação do "Total Bonus Cost" no BKO Pragmatic** — pedido acima.
2. **7 IDs em notação científica** — arquivo original em CSV/TXT.
3. **[Y] IDs não mapeados em `dim_user`** — verificação individual no BKO (podem ser contas antigas migradas, IDs de outro sistema, ou erro de digitação).

Estamos à disposição.

---

# Anexos da entrega

- `remocao_saldo_fraude_freespins_2703_[TIMESTAMP].xlsx`
  - Aba `Remocao_Saldo`: dados por jogador (cohort processado)
  - Aba `Auditoria`: resumo + corrompidos + não-mapeados + distribuição por risco
  - Aba `Legenda`: dicionário de colunas + glossário + fontes técnicas
  - Aba `Validacao_Raw_vs_Gold`: comparação entre fontes raw e camada BI
- `remocao_saldo_fraude_freespins_2703_[TIMESTAMP]_metadata.json`: metadados do snapshot (hash, contagens, totais)
