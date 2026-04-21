# PCR — Player Credit Rating

> Sistema de classificação de jogadores no estilo credit score bancário (D a AAA / E a S), baseado em valor gerado, risco operacional e outlook de engajamento.

---

## O que é o PCR?

O PCR (Player Credit Rating) é um sistema automatizado que classifica cada jogador ativo em um **rating no estilo credit score bancário**, de forma análoga ao que agências de risco fazem para empresas (Moody's, S&P). A diferença é que, em vez de avaliar risco de default de dívida, o PCR avalia o **valor do jogador para a operação** combinado com **padrão de comportamento**.

Ele responde à pergunta: **"Qual o valor deste jogador para a operação e como o CRM deve tratá-lo?"**

O modelo processa diariamente os últimos 90 dias de atividade, atribuindo um score numérico (PVS — Player Value Score, de 0 a 100) e traduzindo-o em um rating discreto (S, A, B, C, D, E, NEW).

---

## Como funciona

```
1. Coleta dados de atividade (apostas, depósitos, saques, bônus) dos últimos 90 dias
2. Calcula 9 componentes de score (valor, risco, outlook, frequência, mix de produto)
3. Pondera os componentes em um único score (PVS) de 0 a 100
4. Classifica em 7 ratings (S, A, B, C, D, E, NEW)
5. Persiste snapshot histórico diário no banco relacional
6. Publica o rating no CRM via API (atualização diária automática)
```

---

## Ratings (Classificações)

| Rating | Faixa do PVS | Faixa da base | Significado | Ação sugerida de CRM |
| --- | --- | --- | --- | --- |
| **S** | Top 1% | ~1% | Whales e jogadores maduros de elite | Atendimento VIP, host dedicado, experiência personalizada |
| **A** | 92 a 99 | ~7% | Alto valor sustentado | Ofertas premium, benefícios exclusivos, cashback elevado |
| **B** | 75 a 92 | ~17% | Valor consistente, engajamento saudável | Cashback, torneios, retenção ativa |
| **C** | 50 a 75 | ~25% | Mediano, padrão regular | Campanhas massivas, free spins, missões |
| **D** | 25 a 50 | ~25% | Baixo valor, porém ativo | Reengajamento, bônus de reativação |
| **E** | Bottom 25% | ~25% | Engajamento mínimo, alto risco de churn | Campanhas de último esforço, possível offboard |
| **NEW** | *sem PVS* | ~10 a 15% | Novatos (menos de 14 dias ativos OU menos de 3 depósitos) | Jornada de onboarding e boas-vindas |

### Por que o NEW é separado

A fórmula do PVS usa razões (margem GGR, razão bônus/depósito, taxa de atividade) que são **estatisticamente instáveis em amostras pequenas**. Um jogador recém-cadastrado com 1 depósito cai automaticamente no rating E por construção matemática, recebendo campanha de reativação quando deveria receber a jornada de boas-vindas. Separar em um bucket próprio garante cálculo justo para os maduros **e** jornada correta para os novatos.

---

## Os 9 Componentes do PVS

O PVS é uma soma ponderada de 9 scores normalizados, cada um em escala 0 a 100 via **percentil rank** dentro da base de jogadores maduros (novatos são excluídos do cálculo dos percentis para não distorcer a cauda).

### Bloco Valor (40% do peso)

| Componente | Peso | O que mede | Por que importa |
| --- | --- | --- | --- |
| **Score GGR** | 25% | GGR total nos 90 dias (apostas menos ganhos) | Métrica primária de valor — quanto o jogador deixa na casa |
| **Score Depósito** | 15% | Total depositado em BRL | Volume de investimento financeiro |

### Bloco Risco (20% do peso)

| Componente | Peso | O que mede | Por que importa |
| --- | --- | --- | --- |
| **Score Margem** | 10% (invertido) | GGR / turnover total | Margem menor = retorno mais previsível para a casa |
| **Score Bônus Penalidade** | -10% | Bônus emitido / total depositado | Penaliza bonus-chaser, premia jogador orgânico |

### Bloco Outlook (25% do peso)

| Componente | Peso | O que mede | Por que importa |
| --- | --- | --- | --- |
| **Score Recência** | 12% (invertido) | Dias desde última atividade | Jogador ativo recentemente tem maior chance de engajar |
| **Score Dias Ativos** | 8% | Quantos dias distintos foi ativo nos 90 dias | Frequência indica hábito, não evento isolado |
| **Score Taxa Atividade** | 5% | Proporção dos 90 dias em que jogou | Complementa dias ativos com escala fixa |

### Bloco Frequência Transacional (10% do peso)

| Componente | Peso | O que mede | Por que importa |
| --- | --- | --- | --- |
| **Score Número Depósitos** | 10% | Quantidade de depósitos no período | Complementa valor (depósito total) com frequência |

### Bloco Mix de Produto (5% do peso)

| Componente | Peso | O que mede | Por que importa |
| --- | --- | --- | --- |
| **Score Mix Produto** | 5% | Cassino + Sportsbook vs. mono-produto | Diversificação reduz risco de churn por vertical |

---

## Fórmula do PVS

```
PVS = 0,25 × score_ggr
    + 0,15 × score_depósito
    + 0,12 × score_recência
    + 0,10 × score_margem
    + 0,10 × score_num_dep
    + 0,08 × score_dias_ativos
    + 0,05 × score_mix_produto
    + 0,05 × score_taxa_atividade
    - 0,10 × score_bônus_penalidade
```

- Resultado final é limitado entre **0 e 100**.
- Cada `score_*` é o **percentil rank (0 a 100)** da métrica bruta dentro da base de maduros.
- Percentis recalculados a cada snapshot (jogadores NEW não entram no cálculo).

### Tradução PVS → Rating

| PVS (faixa) | Rating | Definição |
| --- | --- | --- |
| 0 a 24 | E | Bottom 25% dos maduros |
| 25 a 49 | D | Percentis 25 a 50 |
| 50 a 74 | C | Percentis 50 a 75 |
| 75 a 91 | B | Percentis 75 a 92 |
| 92 a 98 | A | Percentis 92 a 99 |
| 99 a 100 | S | Top 1% |
| (sem PVS) | NEW | Novatos (< 14 dias ativos OU < 3 depósitos) |

Os cortes são **percentis relativos à base de maduros** — cada snapshot recalcula os limiares. Isso garante que as faixas se ajustem ao comportamento observado da operação ao longo do tempo.

---

## Regras e Parâmetros

### Base de jogadores

- **Quem entra:** jogadores reais (não-teste) com pelo menos 1 depósito OU 1 aposta OU 1 rodada nos últimos 90 dias.
- **Quem não entra:** contas de teste, contas fechadas, contas em auto-exclusão (jogo responsável) e contas de demonstração.

### Filtros de qualidade

- Apenas transações confirmadas/efetivadas (exclui tentativas falhadas e em andamento).
- Apenas jogadores com status "real_user" (exclui fraude, encerradas, etc.) — evita contaminação dos percentis e push de CRM para contas bloqueadas.
- Timestamps em UTC, convertidos para horário local onde aplicável.

### Janela temporal

- **90 dias rolling** — cada dia a janela avança, sempre olhando os últimos 3 meses.
- Snapshot D-1 (dados do dia atual excluídos) para evitar dados parciais.

### Jogador NEW

Jogador é classificado como NEW quando satisfaz qualquer uma das condições:
- Menos de **14 dias ativos** na janela (tempo insuficiente para estabilizar padrão)
- OU menos de **3 depósitos** no período (amostra estatisticamente insuficiente para calcular razões como margem GGR ou razão bônus/depósito)

---

## Como cada área pode usar

### CRM e Retenção

| Uso | Como |
| --- | --- |
| Segmentação por tier | Criar segmentos automáticos (S/A = VIP, B/C = Saudável, D/E = Monitorar) |
| Onboarding dedicado | Jornada de boas-vindas específica para `PCR_RATING_NEW` |
| Personalização de bônus | Valor do bônus proporcional ao rating (S/A ganham mais, D/E recebem bônus de reativação) |
| Detecção de queda | Alerta quando jogador migra de A → B → C em poucos dias |
| Reativação | Campanhas específicas para `PCR_RATING_E` antes de churn |

### Financeiro e Head de Dados

| Uso | Como |
| --- | --- |
| Previsão de receita | Concentração de GGR nos tiers S/A permite projetar impacto de perda de whales |
| ROI de aquisição | Medir qual canal traz mais jogadores S/A vs. D/E ao longo do tempo |
| Análise de cohort | Comparar distribuição de ratings entre cohorts de aquisição (safra de abril vs. maio) |
| Monitoramento de saúde | Acompanhar % da base em cada tier como indicador de qualidade geral |

### Compliance e Jogo Responsável

| Uso | Como |
| --- | --- |
| Filtro de automações | Contas com status `rg_closed` (auto-exclusão) nunca entram no push — tratado no pipeline |
| Auditoria histórica | Snapshots diários preservados permitem reconstruir qualquer estado passado |

---

## Exemplos Práticos

### Jogador "S" (PVS 98)

> GGR elevado nos 90 dias (R$ 50k+), deposita 15 vezes no período (frequência alta), margem GGR baixa (não vence muito — previsível), joga cassino e sports (mix diversificado), ativo em 60 dos 90 dias, última atividade ontem, bônus representa menos de 10% dos depósitos.

**Rating:** S (Top 1%)
**Tag no CRM:** `PCR_RATING_S`
**Ação sugerida:** Account manager dedicado, cashback maior, experiência VIP, convite para eventos presenciais.

### Jogador "C" (PVS 62)

> GGR médio (R$ 3k), deposita 6 vezes no período, margem próxima da mediana da base, só joga cassino, ativo em 35 dos 90 dias, última atividade há 3 dias, razão bônus/depósito equilibrada.

**Rating:** C (Mediano)
**Tag no CRM:** `PCR_RATING_C`
**Ação sugerida:** Campanhas massivas, free spins semanais, testar missão de cross-product para migrar para misto.

### Jogador "E" (PVS 12)

> GGR baixo (R$ 200), apenas 3 depósitos, margem alta (venceu algumas vezes grande), razão bônus/depósito alta (depende de bônus), ativo em apenas 8 dos 90 dias, última atividade há 45 dias.

**Rating:** E (Bottom 25%)
**Tag no CRM:** `PCR_RATING_E`
**Ação sugerida:** Campanha de reativação de último esforço; se não converter em 30 dias, considerar offboard de comunicação.

### Jogador "NEW" (sem PVS)

> Cadastrado há 5 dias, 1 depósito, 2 dias ativos. Amostra insuficiente para calcular PVS de forma confiável.

**Rating:** NEW
**Tag no CRM:** `PCR_RATING_NEW`
**Ação sugerida:** Jornada dedicada de boas-vindas (mensagem de acolhimento, bônus de primeiro depósito reforçado, tutorial de navegação).

---

## Integração com CRM (Smartico)

O rating é publicado automaticamente no CRM via API (S2S Event API), permitindo segmentação em tempo quase real.

### Bucket do CRM

As tags `PCR_RATING_*` são publicadas no bucket **`core_external_segment`**, separado do bucket `core_external_markers` (onde ficam tags operacionais como risk matrix, flags de compliance, etc.). Os dois buckets são tecnicamente equivalentes (arrays de strings), mas a separação permite ao time de CRM configurar automations específicas de PCR no painel sem colisão com outras integrações.

### Fluxo completo

```
1. Pipeline roda diariamente (madrugada)
   → Coleta dados dos últimos 90 dias no data lake
   → Calcula PVS + rating para cada jogador

2. Resultado é salvo no banco com snapshot histórico
   → Cada dia gera um novo snapshot
   → Snapshots anteriores são preservados para auditoria e diff temporal

3. Push automático para o CRM (após o pipeline)
   → Compara snapshot atual vs. anterior (envia apenas quem mudou de rating)
   → Operação atômica: remove rating anterior + adiciona rating novo
   → Tags de outras integrações são preservadas (nunca tocadas)
   → Opção de não disparar automações/jornadas (apenas popular o perfil)

4. Tags ficam disponíveis no BackOffice do CRM
   → Campo: core_external_segment
   → 1 tag por jogador (ex.: PCR_RATING_A, PCR_RATING_NEW)
   → CRM pode criar segmentos e regras baseados nessa tag
```

### Exemplo de estado de um jogador no CRM

```
Bucket core_external_segment:
  PCR_RATING_A

Bucket core_external_markers (outras integrações):
  RISK_TIER_BOM
  RISK_REGULAR_DEPOSITOR
  WHATSAPP_OPTIN
```

Esse jogador tem rating A no PCR e é classificado como "Bom" na Matriz de Risco — duas dimensões complementares que o CRM pode cruzar para montar segmentos ricos.

### Dedup por diff

O push envia **apenas quem mudou de rating** entre snapshots:
- Snapshot D-1: jogador 12345 era `PCR_RATING_B`
- Snapshot D: jogador 12345 virou `PCR_RATING_A`
- Payload enviado ao CRM
- Se não mudou: `skip` (reduz de 80% a 95% do volume diário)

Essa otimização evita poluir a API do CRM e reduz custo operacional.

### Rollout em 3 fases (para deployments)

Qualquer alteração no push é validada em 3 fases antes de entrar em produção:

1. **Canary:** 1 usuário seguro (rating intermediário, sem extremos)
2. **Amostra:** 10 usuários via CSV
3. **Full:** produção completa

Cada fase exige validação do time de CRM no painel antes de avançar.

---

## Monitoramento e Saúde

Métricas acompanhadas diariamente após cada execução do pipeline:

| Métrica | Threshold esperado | Ação se estiver fora |
| --- | --- | --- |
| Jogadores NEW | 10% a 20% da base | Se > 25%: verificar afluxo de novos cadastros; se < 5%: verificar aquisição |
| Distribuição S/A/B/C/D/E | ~1/7/17/25/25/25% | Desvios maiores que 5 pp indicam mudança de comportamento ou bug de cálculo |
| Status da conta não nulo | > 99% | Se abaixo: dados de cadastro podem estar desatualizados |
| Tempo de execução | < 5 min | Se > 10 min: investigar lentidão de data lake ou aumento de escopo |
| Tamanho do diff do CRM | 5% a 15% da base/dia | Se > 30%: ruído nos cortes de rating (possível instabilidade nos percentis) |

---

## Lições Aprendidas

Decisões de design documentadas em auditoria interna:

1. **Filtrar status de conta antes do ranking** — se não filtrar, contas em auto-exclusão ou fraude contaminam os percentis e podem receber tag do CRM (risco de compliance).

2. **Dedup determinístico em ROW_NUMBER** — quando a base pode ter duplicatas, usar `ORDER BY id` ou timestamp explícito evita que o rating oscile entre rodadas.

3. **Amostra mínima para ranking** — jogadores com amostra insuficiente (menos de 14 dias ativos ou menos de 3 depósitos) devem ficar fora do ranking PVS para não distorcer os percentis dos maduros. Daí o rating NEW.

4. **Shadow mode obrigatório para novas tags** — antes de ativar push de uma tag nova no CRM, rodar em modo que grava na base mas não envia ao CRM. Garante que a tag esteja pré-registrada no painel do CRM e que a jornada correspondente esteja configurada.

5. **Separação de buckets no CRM** — ratings comportamentais (PCR) e tags operacionais (risk matrix, compliance) em buckets diferentes no CRM. Evita colisão entre equipes e facilita configuração de automations específicas.

---

## Evolução Planejada

| Fase | O que muda | Status |
| --- | --- | --- |
| **v1.0** | Scoring CSV-only, sem integração CRM | Legado |
| **v1.2** | Pipeline Athena + PostgreSQL + integração CRM via S2S | Entregue (abril/2026) |
| **v1.3** | Rating NEW separado + filtro de status de conta + tie-breaker determinístico | Entregue (abril/2026) |
| **v1.4** | Migração para bucket `core_external_segment` no CRM | Entregue (abril/2026) |
| **v2.0** | Cortes de PVS com média móvel de 30 dias (reduz volatilidade artificial) | Planejado |
| **v2.1** | Envio de `score_norm` como Custom Property numérica no CRM (segmentação por faixa dinâmica) | Planejado |
| **v3.0** | Scoring preditivo (probabilidade de migrar de tier em 7/30 dias) usando ML | Planejado |

---

## Especificações Técnicas (resumo)

| Item | Valor |
| --- | --- |
| **Atualização** | Diária (automatizada) |
| **Janela de análise** | 90 dias (rolling) |
| **Número de ratings** | 7 (S, A, B, C, D, E, NEW) |
| **Escala do score (PVS)** | 0 a 100 |
| **Número de componentes do PVS** | 9 |
| **Fonte de dados** | Dados transacionais da plataforma (data lake) |
| **Destino CRM** | Smartico — campo `core_external_segment` |
| **Persistência** | Banco relacional + snapshots históricos |
| **Histórico** | Snapshots diários preservados para auditoria e backtest |
| **Dedup** | Envia somente diff (quem mudou de rating) |

---

> **Desenvolvido por:** Squad Intelligence Engine
> **Última atualização:** Abril de 2026
