# Super Nova DB — Guia do Usuário

> Painel de analytics para operações de apostas reguladas.
> Este guia mostra **o que cada tela faz, como ler cada gráfico e como usar no dia a dia**.
> Versão: Abril/2026 · Público: times de operação, CRM, tráfego, produto, diretoria.

---

## Antes de começar — o essencial em 1 minuto

- **O que é:** plataforma multi-operador de analytics. Mostra o desempenho da operação quase em tempo real, com visões financeira, de aquisição e por vertical (Cassino e Sportsbook).
- **Atualização:** os números ficam atualizados no máximo a cada 30 minutos durante o dia. Por padrão, a tela mostra **até ontem (D-1)**, porque o dia de hoje ainda está em andamento e seria parcial. Se você quiser ver o parcial do dia, use o botão **Hoje** no filtro.
- **Filtro de data global:** fica no canto superior direito — **Hoje, Ontem, 3 dias, 7 dias, 30 dias (padrão), 90 dias** ou intervalo livre. Esse filtro vale para **toda a tela que você estiver vendo**.
- **Login seguro por operador:** você só enxerga os dados do seu operador. Não tem mistura.
- **Indicador ao vivo (bolinha verde animada no topo):** confirma que o painel está conectado e com os dados mais recentes carregados.

---

## Navegação — 4 áreas na lateral esquerda

| Ícone | Área | Para quê serve |
|---|---|---|
| ⬡ | **Dashboard** | Sua página inicial. Indicadores-chave e gráfico principal. |
| ◈ | **Builder** | Personaliza quais indicadores aparecem no seu Dashboard. |
| ≡ | **Relatórios** | Análises detalhadas (Financeiro, Aquisição, Performance por Vertical). |
| ◎ | **Alertas** | Em desenvolvimento — próxima versão. |

No canto inferior esquerdo fica seu **avatar**: clique nele para trocar senha ou sair. Se você for admin, também é onde você troca de operador.

---

# 1. Dashboard

Tela inicial. Serve para **enxergar o pulso do negócio em segundos**, sem precisar abrir relatório.

### O que tem na tela

**1.1. Cartões de indicadores (KPIs) no topo**
Cada cartão mostra um número grande (valor do período) e a variação em relação ao período anterior. Você escolhe quais cartões aparecem no **Builder** (próxima seção).

**1.2. Gráfico de evolução (linha)**
Mostra como o indicador selecionado se comportou ao longo do período. Em cima do gráfico tem abas: **GGR · NGR · Depósitos · Saques · Apostas · Jogadores · FTD**. Clique para trocar.
- **Como usar:** identifique tendência (subindo? caindo? estável?) e picos ou quedas bruscas que mereçam investigação.

**1.3. Pizza "GGR por Vertical"**
Quanto do resultado veio de **Cassino** vs **Sportsbook** no período.
- **Como usar:** se você tem uma meta de diversificação (ex: aumentar Sportsbook), é aqui que você acompanha se está indo bem.

**1.4. Quick Stats**
Linha de métricas rápidas complementares (depósito médio, ticket médio, etc.).
- **Como usar:** são números de apoio para bater o olho antes de abrir um relatório. Se um deles estiver fora do padrão, vale ir direto no relatório específico para investigar.

---

# 2. Builder — personalizar seu Dashboard

Clique em **◈ Personalizar** no topo para abrir o painel lateral.

**O que você pode fazer:**
- Ligar/desligar indicadores
- Filtrar por categoria: **Financeiro · Transacional · Jogo · Usuários · Performance · Retenção**
- Buscar por nome

**Indicadores disponíveis hoje:**

| Indicador | O que mede |
|---|---|
| **GGR** | Receita bruta de jogo (tudo que entrou − prêmios − bônus) |
| **NGR** | Receita líquida (GGR − custos operacionais) |
| **Depósitos** | Total depositado pelos jogadores no período |
| **Saques** | Total sacado no período |
| **Volume Apostas** | Quanto foi apostado (turnover) |
| **Jogadores Ativos** | Jogadores únicos que apostaram no período selecionado |
| **ARPU** | Receita média por jogador ativo (NGR ÷ Ativos) |
| **Margem GGR** | Quanto da aposta virou receita (%) |
| **Churn Rate** | % de jogadores que ficaram inativos |
| **LTV** | Valor vitalício estimado do jogador |

**Em breve:** fórmulas personalizadas (ex: "GGR Real = GGR − Bônus − Chargebacks").

> Nas fórmulas exibidas pelo painel aparece a palavra "player". Leia como sinônimo de "jogador" — é o mesmo conceito.

**Dica:** monte 1 painel enxuto com 4–6 KPIs que você olha toda manhã. Mais do que isso vira poluição visual.

---

# 3. Relatórios

Três relatórios prontos, cada um com foco diferente.

## 3.1. Relatório Financeiro

> **Para quem:** finanças, diretoria, qualquer pessoa que precisa responder "como foi o dia/semana/mês financeiramente?".

### O que tem na tela

**Cartões no topo — 4 indicadores de eficiência:**
- **GGR Cassino / Depósito** — quanto o cassino gera em receita para cada R$1 depositado.
- **GGR Sports / Depósito** — o mesmo, mas para o sportsbook.
- **Retenção / Receita** — quanto do que entra volta ao jogador em prêmios.
- **Receita / Depósito** — eficiência geral da operação.

### Tabela Diária (a principal)

Cada linha é um dia. Cada coluna é uma métrica (Depósito, Saque, Net Deposit, Turnover/Prêmio/GGR por vertical, Hold Rate, BTR, FTD, Conversão, Ativos, etc.).

- **Filtros:** intervalo de datas + dia da semana (ex: "só segundas-feiras").
- **Exportar CSV:** botão no rodapé.
- **Como usar:** abra a tabela com 30 dias, ordene por uma métrica problemática (ex: GGR caindo) e investigue o dia específico.

### Análise Diária — gráficos que acompanham a tabela

| Gráfico | O que mostra | Como usar |
|---|---|---|
| **Depósito · Saque · Net Deposit** (3 mini-gráficos) | Evolução diária das 3 métricas | Achar dias anômalos (pico de saque, queda de depósito) |
| **Distribuição GGR e NGR** | Barras de GGR + linha de NGR dia a dia | Enxergar a diferença entre receita bruta e líquida |
| **Hold Rate Diário — Cassino, Sports e Total** | Quanto da aposta ficou com a casa, por vertical | Identificar dias "ruins" (hold baixo = jogador ganhou muito) |
| **FTD e Conversão Diária** | Primeiros depósitos + taxa de conversão | Ver se campanhas de aquisição estão convertendo |

### Análise por Hora

Quatro gráficos do tipo "hora a hora" ao longo dos dias do período:
- **Depósitos por hora**
- **Saques por hora**
- **Receita (NGR) por hora**
- **FTD acumulado por hora**

**Como usar:** descobre **em que horário o dinheiro entra e sai**. Útil para CRM (mandar push na hora quente) e operação (times de atendimento no pico de saques).

### Heatmap — Hora x Dia da Semana

Matriz com 24 horas (linhas) × 7 dias da semana (colunas). A cor mostra intensidade.
Abas: **Receita · Depósito · Saque**.

**Como usar:** descobre o padrão semanal. Ex: "segunda à noite é o pico de receita, sábado de manhã é o de saque". Agenda campanhas e promoções em cima disso.

### Análise Semanal

| Gráfico | O que mostra |
|---|---|
| **Média Diária de Depósito por Semana** | Tendência de crescimento/queda semana a semana |
| **Depósito por Dia da Semana · Linhas por Semana** | Cada semana é uma linha; enxerga sazonalidade |
| **GGR · NGR · Hold · Bonus/GGR Unificado** | Gráfico consolidado para bater olho e ver tudo junto |
| **FTDs por Dia da Semana** | Qual dia da semana rende mais novos jogadores |
| **DAU por Dia da Semana** | Quantos jogadores únicos por dia |
| **Active Player Retention vs Repeat Depositors** | Proporção entre jogadores que voltam a jogar e jogadores que voltam a depositar |

**Insight chave:** o cruzamento "Retention vs Repeat Depositors" mostra se sua base está **engajada mas sem dinheiro** (joga bônus e vai embora) ou **realmente ativa** (deposita de novo).

---

## 3.2. Relatório de Aquisição

> **Para quem:** tráfego, CRM, marketing. Responde "quem são os leads que entraram hoje, de onde vêm, e o que fazem depois?".
> **Visão safrada:** cada lead é agrupado pela sua **data de cadastro**, e acompanhamos o que ele faz ao longo do tempo (primeiro depósito, segundo, terceiro, e assim por diante).

### Cartões no topo

- **Cadastros** — quantos leads entraram
- **FTD** — quantos desses fizeram o primeiro depósito
- **Conversão FTD** — % de cadastros que viraram FTD
- **Ticket Médio FTD** — quanto eles depositaram na média
- **FTD Amount** — total depositado pela safra

**Como usar esses cartões juntos:** Cadastros sem FTD correspondente indicam problema no onboarding (verificação, KYC, CPF); Conversão FTD caindo com Ticket Médio subindo pode significar que sua base está mais "premium" mas menor; Conversão estável com Ticket caindo é sinal de aquisição "ralo" (lead de pior qualidade).

### Tabela Diária

Cada linha é uma data de cadastro. Colunas mostram a "vida" daquela safra: **FTD · STD (segundo depósito) · TTD (terceiro) · QTD+ (quarto em diante)** + valores financeiros + **Custos · CAC · CAC FTD · Retorno**.

- **Filtros:** intervalo + **Fonte** (canal de aquisição)
- **Exportar CSV:** botão no rodapé

**Como ler uma linha:** "No dia 01/04 entraram 500 leads. Destes, 180 fizeram FTD (36% de conversão), 95 fizeram STD, 60 fizeram TTD e 35 já fizeram 4+ depósitos. Gastamos R$ 2.500 em mídia naquele dia — CAC ficou em R$ 5,00 e CAC FTD em R$ 13,89."

### Funil de Aquisição (pirâmide)

Pirâmide visual: **Cadastros → FTD → STD → TTD → QTD+**. Cada nível mostra quantidade e % sobre o anterior.
**Como usar:** identifica **onde o jogador desiste**. Se a queda de STD para TTD é muito grande, tem algo no momento do 3º depósito que precisa de atenção (bônus? UX? oferta?).

### Funil por Fonte (barras agrupadas)

Mesmo funil, quebrado por canal de tráfego. Permite comparar qual fonte traz leads mais qualificados.
**Como usar:** não olhe só para "quem traz mais cadastros". Olhe **qual fonte tem o melhor funil até TTD/QTD+** — essa é a fonte que traz jogador real, não só curioso.

### Volume Diário por Estágio

Linha do tempo comparando o ritmo de FTD, STD, TTD e QTD+ dia a dia.
**Como usar:** você enxerga se o 1º depósito está acontecendo no mesmo ritmo dos recorrentes, ou se a base está "velha" (só recorrente, sem FTD novo).

### Trio de Insights

- **Receita total por estágio (donut)** — quanto do dinheiro vem de FTD, STD, TTD e QTD+. Normalmente QTD+ domina, porque é onde moram os jogadores fiéis.
- **Ticket médio por estágio** — quanto o jogador deposita em média em cada depósito. Tendência é subir à medida que ele evolui no funil.
- **Taxa de conversão WoW por estágio** — variação semana a semana por estágio. Permite ver se a operação está "melhorando ou piorando" a conversão.

### Cohort de Retenção

Matriz clássica de cohort. Aba **Depositantes** ou **Jogadores**.
Cada linha = semana de cadastro. Cada coluna = semana depois do cadastro (W0, W1, W2…). A célula mostra % que continuou depositando/jogando.
**Como usar:** a primeira coluna é sempre 100%. O que importa é a **queda da W1 para W4**. Se em W4 você ainda tem 20%+, sua retenção está saudável. Abaixo de 10% — sinal de vermelho.

---

## 3.3. Performance por Vertical

> **Para quem:** produto, diretoria, time de jogos, risco.
> Dividido em 4 abas: **Overview · Casino · Sportsbook · Players (Jogadores)**.

### 3.3.1. Overview — comparativo Cassino vs Sportsbook

**Cartões:** GGR Total, GGR Casino, GGR Sports, Turnover Total, Hold Rate Geral, Jogadores Ativos (30d).
Cada cartão mostra o **delta vs período anterior** — seta verde/vermelha.

**Gráficos:**

| Gráfico | O que mostra | Como usar |
|---|---|---|
| **GGR Diário — Casino vs Sports** | Duas linhas lado a lado, dia a dia | Identifica dias em que uma vertical "carregou" a receita e a outra ficou fraca |
| **Composição de Receita** | Área empilhada: participação de cada vertical ao longo do tempo | Enxerga se a dependência de uma vertical está aumentando ou diminuindo |
| **GGR por Vertical** (pizza) | Fatia final do período | Número único para reportar para diretoria |
| **Jogadores por Vertical** (pizza) | Quantos jogadores em cada vertical — inclui os **Mistos** (jogam as duas) | Acompanha evolução da base que usa os dois produtos |
| **Top 5 Jogos (GGR)** | Ranking dos jogos de cassino que mais geraram receita | Confirma se os jogos "carro-chefe" continuam entregando |

**Insight chave:** compare "% de receita por vertical" com "% de jogadores por vertical". Se Casino gera 80% da receita com só 60% dos jogadores, significa **Casino é mais rentável por jogador**. Útil para priorização de produto.

### 3.3.2. Casino

**Filtros extras:** Provedor (ex: Pragmatic) e Jogo específico.

**Cartões:** Jogadores (30d), Rodadas, Turnover, GGR, Hold, RTP Médio.

| Visualização | O que mostra | Como usar |
|---|---|---|
| **GGR Diário + RTP (%)** | Barra (GGR) + linha (RTP) | RTP acima da média sinaliza dias em que os jogadores ganharam mais — se virar tendência, revisar mix de jogos |
| **GGR por Categoria** (pizza + tabela) | Slots · Live Casino · Crash · Table Games etc. | Vê onde o cassino concentra receita; útil para decidir onde alocar marketing e estoque de jogos |
| **Top 10 Providers** | Ranking com rodadas, turnover, GGR, Hold%, RTP%, Bônus, Bônus/GGR% | Identifica provedores mais rentáveis e os que estão "caros de bônus" |
| **Top 20 Jogos** | Ranking com provider, categoria, rodadas, GGR, Hold, Bônus | Prioriza quais jogos manter em destaque no lobby e quais reduzir exposição |

**Insight chave:** monitore **Bonus/GGR** por jogo. Se um jogo tem Bônus/GGR > 50%, significa que a maior parte da receita vem "comprada" de bônus — não é um jogo saudável.

### 3.3.3. Sportsbook

**Cartões:** GGR Sports, Turnover, Margem, Ticket Médio, Qty Apostas, Jogadores (30d).

| Visualização | O que mostra | Como usar |
|---|---|---|
| **GGR Diário + Hold (%)** | Barra (GGR) + linha (Margem/Hold). Valor negativo indica que os jogadores ganharam mais que a casa no dia | Dias negativos isolados são normais; vários dias seguidos negativos pedem revisão de gestão de odds |
| **GGR por Categoria (Top 10)** | Esportes com mais receita | Confirma se a concentração está onde o produto espera (ex: futebol líder) |
| **Detalhes por Categoria** | Mesma info em tabela, com Turnover e Margem | Comparar margem entre esportes — margens baixas podem indicar mercado desalinhado |
| **Pré-Live vs Live — Apostas por Dia** | Linha comparando tipos de aposta | Mede evolução do produto ao vivo; se Live não cresce, algo na experiência pode estar travando |
| **Jogos de Hoje ou Em Aberto — Mais Apostados** | Lista dos eventos **ainda em andamento ou futuros** com mais apostas. Tem filtro por esporte | Visão operacional: onde a casa tem maior exposição imediata |
| **Top 10 Esportes (GGR)** | Ranking de esportes | Útil para decisões de alocação de mídia e boost de odds |
| **Exposição Atual — Apostas Abertas** | Quanto a casa pode pagar se todas as apostas abertas ganharem. Por esporte | Ver tabela abaixo — é a visão mais importante para risco |
| **Projeção GGR por Data de Liquidação** | GGR esperado por dia à medida que as apostas se liquidam | Antecipa o impacto financeiro de cada rodada de campeonato |

**Insight chave — Exposição:** esta tabela é **operacional quase em tempo real**. Se o "Passivo Projetado" está muito alto para um esporte, a operação pode precisar limitar novas apostas ou ajustar odds.

### 3.3.4. Players (Jogadores)

> **Importante:** os KPIs e gráficos desta aba usam **janelas fixas** (30 dias, 8 semanas, cohorts). Isso é proposital: comparações históricas e cohorts só fazem sentido com janelas iguais entre si. Apenas as tabelas **Top Winners, Top Losers e Top Bonus Abusers** respondem ao filtro de data do topo — porque ali você quer listar quem performou naquele período específico.

**Cartões (sempre 30d):** Jogadores Ativos 30d, Novos Cadastros, FTD, Depósito Médio, LTV 30d, Churn Rate.

**Top 50 Winners** — jogadores com **maior lucro** (o que ganharam menos o que apostaram) no período.
**Top 50 Losers** — jogadores com **maior prejuízo** no período.
**Top 50 Bonus Abusers** — jogadores com maior razão **Bônus ÷ Depósito**. Inclui uma coluna "Status Matriz de Risco" indicando se já estão sinalizados pelo time antifraude.

**Outros gráficos:**

| Gráfico | O que mostra |
|---|---|
| **Distribuição por Faixa de Depósito (30d)** | Quantos jogadores em cada faixa: até R$50, R$50–200, R$200–1k, R$1k+ |
| **Retenção Semanal — Depositantes Retidos (%)** | Curva de retenção |
| **Overlap Casino ↔ Sports por Período** | Quantos jogam só um, ambos, em cada janela (7/14/30/60d) |
| **Composição (Últimos 30d)** | Só Casino vs Só Sports vs Misto (pizza) |
| **Matriz — Períodos × Produtos** | Tabela com Casino, Sportsbook, Misto, Só Casino, Só Sports, Total Único, % Misto por período |

**Insight chave — Overlap:** "% Misto" é um indicador de maturidade do jogador. Jogador que usa as duas verticais tipicamente tem LTV maior. Se você está investindo em cross-sell Casino↔Sports, acompanhe este número aqui.

---

# 4. Alertas

Em desenvolvimento. Próxima versão trará alertas automáticos (ex: hold abaixo do mínimo, pico de saque, bonus abuser novo). Por enquanto, esta aba está desativada.

---

# 5. Boas práticas — como usar no dia a dia

**Rotina diária (5 minutos):**
1. Abrir em **30 dias**, olhar o gráfico GGR no Dashboard.
2. Ir em **Relatórios → Financeiro → tabela diária** e bater o olho em Hold Rate, BTR e Conversão FTD do dia anterior.
3. Se algo estiver fora do padrão — ir no Heatmap hora×dia para descobrir o momento exato.

**Rotina semanal (30 minutos):**
1. **Aquisição**: conferir o funil por fonte. Qual canal está entregando jogador qualificado?
2. **Verticais → Casino → Top 20 Jogos**: algum jogo com Bônus/GGR acima de 50%? Algum com Hold muito abaixo da média do provider?
3. **Verticais → Sportsbook → Exposição**: a exposição está dentro do limite de tolerância definido?
4. **Verticais → Players → Bonus Abusers**: revisar os 10 primeiros com o time de risco.

**Rotina mensal:**
1. Comparar o cohort de retenção mês atual vs mês anterior.
2. Evolução de LTV 30d (no Players).
3. % Misto no Overlap — está crescendo? (meta de cross-sell)

**Dicas operacionais:**
- **Use D-1 como padrão.** Dados de hoje são parciais. Só olhe "Hoje" quando precisar reagir em tempo real (ex: conferir se a campanha do dia está rodando).
- **Exporte CSV para formar histórico próprio.** O painel guarda os dados, mas se você precisa cruzar com algo externo (planilha de mídia, relatório financeiro), o CSV é seu amigo.
- **Compare sempre com período anterior.** O delta verde/vermelho dos cartões já faz isso — preste atenção.
- **Um gráfico sem contexto mente.** Um pico pode ser campanha, pode ser bug, pode ser ação de risco. Sempre cruze com o que aconteceu na operação naquele dia.

---

# 6. Glossário rápido (para quem está começando)

| Termo | O que significa |
|---|---|
| **GGR** | Gross Gaming Revenue. Receita bruta de jogo: apostas − prêmios pagos − bônus. |
| **NGR** | Net Gaming Revenue. Receita líquida: GGR menos bônus, chargebacks e taxas operacionais. |
| **Turnover** | Volume total apostado no período. |
| **Hold Rate** | % do turnover que ficou com a casa (GGR ÷ Turnover). |
| **RTP** | Return to Player. Complementar do Hold. RTP alto = jogador ganhou mais. |
| **Margem** | Hold Rate do sportsbook (mesma lógica, nome diferente). |
| **Net Deposit** | Depósitos − Saques. |
| **FTD / STD / TTD / QTD+** | 1º / 2º / 3º / 4+ depósitos do jogador. |
| **CAC** | Custo de Aquisição por cadastro (custo ÷ cadastros). |
| **CAC FTD** | Custo de Aquisição por depositante (custo ÷ FTDs). |
| **ARPU** | Receita Média Por Usuário (NGR ÷ ativos). |
| **LTV** | Lifetime Value — valor estimado que o jogador gera na vida dele. |
| **MAU / DAU** | Monthly / Daily Active Users — jogadores únicos no mês/dia. |
| **Churn** | Taxa de jogadores que ficaram inativos. |
| **BTR** | Bonus Turned Real — quando o bônus vira dinheiro real na conta. |
| **Cohort** | Grupo de jogadores agrupado pela data de cadastro. |
| **Overlap** | Jogadores que atuam em mais de uma vertical (Cassino + Sports). |
| **Exposição** | Quanto a casa pagaria se todas as apostas abertas fossem ganhadoras. |

---

# 7. Sobre a atualização dos dados

- Dados agregados do dia anterior: **atualizados de madrugada**. Por isso a tela abre ancorada em D-1 por padrão.
- Durante o dia: **refresh a cada 30 minutos** (automático). Esse refresh alimenta os dados de **Hoje** e **Ontem** quando você seleciona manualmente no filtro — úteis para acompanhamento operacional, mas lembre-se que o dia de hoje é sempre parcial até o último refresh.
- O horário do último refresh aparece no topo da tela, ao lado do nome do operador.
- Todos os dados estão em **horário de Brasília (BRT)**.
- O painel também usa **cache de 5 minutos por tela** para ficar rápido ao trocar de relatório. Se você acabou de rodar um ajuste no banco e não está vendo a mudança, aguarde 5 minutos e recarregue.

---

# 8. Suporte

Dúvida, problema ou sugestão: fale com o time de Analytics/Dados.
Bugs ou erros na tela: mande print + horário aproximado.

---

*Fim do guia.*
