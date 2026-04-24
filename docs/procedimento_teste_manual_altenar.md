# Procedimento de Teste Manual — Mercados Abertos Pos-Evento

> **Objetivo:** Verificar se o bug de mercados abertos apos o fim de eventos esportivos esta ativo AGORA, tanto na MultiBet quanto em outras bets que usam Altenar.
> **Quando executar:** Hoje a noite (13/04/2026), entre 21:00 e 01:00 BRT — horario de NBA/NCAAB.
> **Quem executa:** Mateus (ou qualquer pessoa do time com acesso ao site da MultiBet).
> **Tempo estimado:** 30-60 minutos.

---

## Passo 1 — Preparar fontes de resultados em tempo real

Abrir em abas separadas no navegador:

| Fonte | URL | O que mostra |
|---|---|---|
| **Flashscore** | https://www.flashscore.com.br/ | Resultados ao vivo de todos os esportes |
| **Sofascore** | https://www.sofascore.com/pt/ | Resultados ao vivo + estatisticas |
| **NBA.com** | https://www.nba.com/games | Resultados oficiais NBA |
| **ESPN** | https://www.espn.com.br/nba/resultados | Resultados em portugues |

---

## Passo 2 — Identificar jogos que estao terminando

1. No Flashscore, filtrar por **Basquete** (ou clicar na aba Basquete)
2. Procurar jogos com status **"Encerrado"** ou **"FT"** (Full Time)
3. **Anotar exatamente:**
   - Nome dos times (ex: "Los Angeles Lakers vs Golden State Warriors")
   - Horario de fim (ex: "23:45 BRT")
   - Placar final (ex: "112-108")
   - Liga (ex: "NBA")

**Fazer o mesmo para Futebol** — especialmente ligas menores (Brasileirao Serie B/C, ligas europeias de segundo escalao, ligas sul-americanas).

---

## Passo 3 — Verificar na MultiBet (nosso site)

1. Abrir o sportsbook da MultiBet: `https://multibet.app.br` (ou URL do sportsbook)
2. **30 minutos apos o jogo ter terminado no Flashscore:**
   - Ir na secao do esporte (Basquete, Futebol)
   - Procurar o jogo que acabou
   - **Se aparecer odds abertas / botao de apostar:** o mercado esta aberto indevidamente
3. **Se encontrar mercado aberto:**
   - **SCREENSHOT imediato** (Print Screen / Win+Shift+S)
   - Anotar: nome do jogo, horario que terminou (do Flashscore), horario do screenshot
   - **NAO apostar** — apenas documentar
4. **Repetir 60 minutos apos o jogo ter terminado** — se ainda estiver aberto, e confirmacao forte
5. **Repetir 120 minutos apos** — se ainda estiver aberto, e absurdo

---

## Passo 4 — Verificar em outras bets Altenar

Bets brasileiras que usam Altenar como provedor de sportsbook (verificar se ainda usam — mercado muda rapido):

| Bet | URL | Nota |
|---|---|---|
| **Betano** | https://www.betano.com.br | Uma das maiores do Brasil |
| **Rivalo** | https://www.rivalo.com | Usa Altenar |
| **KTO** | https://www.kto.com | Historicamente Altenar |
| **Betsson** | https://www.betsson.com.br | Pode usar Altenar |

**IMPORTANTE:** Voce NAO precisa ter conta nessas bets. Basta acessar o site e verificar se o jogo que ja terminou aparece com odds abertas na pagina de sportsbook. A maioria dos sites mostra odds mesmo sem login.

**Para cada bet, fazer o mesmo:**
1. 30 min apos um jogo terminar → ir na secao de basquete/futebol
2. Procurar o jogo que acabou
3. **Se tiver odds abertas:** SCREENSHOT com data/hora visivel
4. Anotar: bet, jogo, horario real de fim, horario do screenshot

---

## Passo 5 — Organizar as evidencias

### Estrutura de pastas sugerida

```
evidence/
  altenar_bug_13-04-2026/
    multibet/
      screenshot_nba_lakers_warriors_2345brt.png
      screenshot_nba_lakers_warriors_0030brt.png  (30min depois)
    betano/
      screenshot_nba_lakers_warriors_2345brt.png
    rivalo/
      screenshot_nba_lakers_warriors_2345brt.png
    notas.txt  (anotacoes manuais)
```

### Template para notas.txt

```
TESTE: Mercados abertos pos-evento — 13/04/2026
Executor: [seu nome]

JOGO 1:
  Evento: Los Angeles Lakers vs Golden State Warriors
  Liga: NBA
  Fim real (Flashscore): 23:45 BRT
  Placar: 112-108

  MultiBet:
    23:45 - Jogo terminou no Flashscore
    00:15 (+30min) - [ABERTO/FECHADO] no sportsbook MultiBet. Screenshot: [arquivo]
    00:45 (+60min) - [ABERTO/FECHADO]. Screenshot: [arquivo]
    01:45 (+120min) - [ABERTO/FECHADO]. Screenshot: [arquivo]

  Betano:
    00:15 (+30min) - [ABERTO/FECHADO]. Screenshot: [arquivo]

  Rivalo:
    00:15 (+30min) - [ABERTO/FECHADO]. Screenshot: [arquivo]

JOGO 2:
  ...
```

---

## Passo 6 — Interpretar os resultados

### Se encontrar mercados abertos na MultiBet E em outras bets:
**Conclusao:** O bug e da Altenar (feed compartilhado). Argumento fortissimo para cobrar correcao.

### Se encontrar mercados abertos APENAS na MultiBet:
**Conclusao:** Pode ser problema de configuracao/integracao nossa. Escalar pro Gusta investigar.

### Se NAO encontrar mercados abertos em nenhuma bet:
**Conclusao:** O bug pode ter sido corrigido recentemente, ou ocorre apenas em ligas/horarios especificos. Nossos dados historicos continuam validos.

---

## Dicas para maximizar a chance de encontrar

1. **Foque em basquete americano (NBA/NCAAB)** — foram os mais afetados nos 90 dias
2. **Jogos que terminam de madrugada no Brasil** (21:00-02:00 BRT) — momento de menor monitoramento
3. **Ligas menores de futebol** (Brasileirao Serie C, ligas europeias de 2a divisao) — menos monitoramento da Altenar
4. **E-sports** — IEM Rio 2026, DreamLeague, VCT Americas — nosso scan de hoje mostrou varios
5. **Cheque o mercado de "Total de gols"** e **"Handicap"** — sao os mais usados pelos fraudadores porque tem resultado deterministico pos-jogo

---

## Horarios sugeridos para hoje (13/04, segunda)

| Horario BRT | O que verificar | Fonte |
|---|---|---|
| 21:00-22:00 | Jogos de NBA que comecaram as 18:30-19:00 | NBA.com / Flashscore |
| 22:30-23:30 | Jogos de NBA do horario das 20:00-20:30 | NBA.com |
| 00:00-01:00 | Jogos tardios de NBA + NCAAB | ESPN / Flashscore |
| 01:00-02:00 | Verificacao final — mercados que ficaram abertos >2h | MultiBet + outras bets |

---

**Autor:** Squad 3 Intelligence Engine
**Data:** 13/04/2026