# Feedback — Segmentacao de Jogadores (PVS)

**De:** Mateus Fabro  
**Para:** Castrin  
**Data:** 06/04/2026  
**Ref:** Apresentacao_Segmentacao_Diretoria.html

---

## 1. Perguntas tecnicas (clarificar antes da call de quarta)

### 1.1 Fonte e formula do GGR
O report nao menciona de onde vem o GGR nem a formula usada. Isso e critico porque:
- Se veio de `fund_ec2` (tipos 27-45): **rollbacks (tipo 72) foram descontados?** Sem rollbacks, GGR infla 5-7x. Tivemos incidente em 25/03 onde reportamos R$85.9M vs R$11.6M correto.
- Se veio de `ps_bi.fct_player_activity_daily` (campo `casino_ggr`): ja tem isolation, ok.
- O GGR de R$19.7M parece coerente com Q1 (R$17.8M), mas vale confirmar a fonte.

**Sugestao:** incluir nota de rodape com fonte + formula.

### 1.2 Test users foram excluidos?
167 contas de teste inflam ~3% do volume (R$26.6M). Se a base de 179.941 inclui test users, os numeros dos segmentos mudam. Filtro: `is_test = false` (ps_bi) ou `c_test_user = false` (bireports/ecr).

### 1.3 Definicao de "jogador ativo"
Base = 179.941, periodo Jan/2026 a presente (~95 dias). Qual o criterio?
- Pelo menos 1 deposito? 1 aposta? 1 login?
- Se "ativo" = fez 1 transacao em 95 dias, entao 50% da base (Casual) nao e ativo de verdade — sao jogadores que entraram, jogaram 1 dia e nao voltaram. Talvez a narrativa mude de "50% e casual" para "50% ja saiu".

### 1.4 O "lifetime" cobre qual periodo?
O report fala em "Deposito Lifetime" e "GGR Total", mas o periodo e Jan/2026 a presente. Jogadores registrados antes de janeiro tem historico anterior incluido? De qual fonte?

### 1.5 Normalizacao do PVS
Os pesos positivos somam 85 e negativos 15, mas o score vai de 0 a 100. Como e o scaling? Min-max? Percentil? Log? Sem isso, nao da pra reproduzir nem auditar — e a galera vai perguntar "por que Fulano tem 71 e nao 72".

### 1.6 W/D Ratio do Regular = 0,00
Regular tem 2 depositos, 2 dias ativos, mas saca zero. Duas hipoteses:
- Perderam tudo e nao tem saldo pra sacar (provavel, faz sentido)
- Bug no calculo (divisao por zero virou zero)

Se for a primeira, vale explicar. Se for a segunda, corrigir.

### 1.7 Hold Rate do Premium parece alto
Premium com 21.2% de hold rate esta acima da media da operacao (Jan 13.2%, Fev 19.2%, Mar meta 16.97%). Pode ser efeito de mix de produto (slots mais volateis), mas vale investigar se nao tem distorcao.

---

## 2. Lacunas analiticas

### 2.1 GGR negativo de 75% da base — falta o "por que"
Este e o achado mais importante do report, mas nao explica a causa. Regular e Casual tem GGR negativo — a casa perde dinheiro com eles. Mas por que?
- Estao ganhando nas apostas? (improvavel em escala)
- Custo de bonus esta comendo a margem? (provavel)
- BTR (bonus convertido em saque) alto?

**Sem separar GGR Real vs GGR Bonus, nao da pra saber se o problema e o jogador ou a politica de bonus.** Nos temos o pipeline de sub-fund isolation que faz essa separacao.

### 2.2 NGR ausente
Reconhecido nas limitacoes, mas e uma lacuna critica. GGR - BTR - RCA = NGR. Um Whale com R$5.166 de GGR e R$4.000 em bonus e muito diferente de um com R$5.166 e R$500 em bonus. Sem NGR, a rentabilidade real por segmento e desconhecida.

### 2.3 Sem fonte de aquisicao
Temos os IDs dos affiliates mapeados (Google: 297657, 445431, 468114 | Meta: 532570, 532571, 464673). Cruzar segmento com fonte de aquisicao responde: "Whales vem mais de Google ou Meta?" — isso muda investimento em midia.

### 2.4 Padrao temporal de sport pode ser sazonal
O pico de apostas sport na terca para Casual (19.1%) pode ser efeito de Champions League / Libertadores, nao comportamento intrinseco do segmento. Se o periodo amostral e curto (30 dias), um calendario esportivo especifico pode distorcer.

---

## 3. Sugestoes para fortalecer antes da call

| # | Sugestao | Impacto | Esforco |
|---|----------|---------|---------|
| 1 | Incluir fonte + formula do GGR em nota de rodape | Credibilidade | Baixo |
| 2 | Confirmar exclusao de test users | Acuracia | Baixo |
| 3 | Documentar normalizacao do PVS (formula completa) | Reprodutibilidade | Baixo |
| 4 | Explicar W/D ratio = 0 do Regular | Clareza | Baixo |
| 5 | Separar GGR Real vs Bonus nos segmentos negativos | Profundidade analitica | Medio (tenho pipeline) |
| 6 | Cruzar segmentos com affiliate/fonte de aquisicao | Estrategia de midia | Medio (tenho dados) |
| 7 | Definir caminho de ativacao no CRM (BigQuery suspenso) | Operacionalizacao | Medio (depende de acesso) |
| 8 | Adicionar lamina "como operacionalizar" (quem faz o que) | Acao pos-validacao | Baixo |

---

## 4. O que posso contribuir

Tenho pipelines prontos que podem enriquecer essa segmentacao:

- **Validacao cruzada do GGR** — scripts `fct_player_activity_daily` e `fct_casino_activity_daily` ja rodando
- **Sub-fund isolation** — separacao Real vs Bonus por jogador via `tbl_realcash_sub_fund_txn` + `tbl_bonus_sub_fund_txn`
- **Enriquecimento com affiliate** — cruzar `dim_user.affiliate_id` com a base segmentada
- **Dashboard CRM** — ja temos estrutura Flask + HTML que pode receber os segmentos como filtro

Se for util, posso rodar essas analises complementares antes da call de quarta.
