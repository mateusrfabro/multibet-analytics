# Arquitetura MVP — Bot de Analytics via WhatsApp

**Autor:** Mateus Fabro (Squad 3 — Intelligence Engine)
**Data:** 20/04/2026
**Status:** Rascunho v1 — para discussão técnica com Castrin + Gusta
**Diretriz do Head (completa):**
> *"ideal é isso rodar via db (post), pra não onerar o Athena, pq pensa numa
> query imensa rodando e ferrando nois. pq usuário vai pedir bagui grotesco né"*

**Racional estratégico (complementado):**
1. **Proteger Athena** — usuário vai pedir agregações enormes; queries cruas no
   Iceberg Data Lake custam caro (AWS cobra por dados escaneados) e podem travar.
2. **Reusar views/matviews do Super Nova DB** — os produtos SN existentes
   (matriz_financeiro, matriz_aquisicao, heatmap_hour, risco, etc.) **já puxam de lá**.
   Dados pré-agregados pelos pipelines ETL, prontos pra consumo.
3. **Escalabilidade de produto** — Play4Tune também está ganhando views gold (GGR
   por jogador/jogo, sportsbook, casino). **Mesmo bot atende P4T + MultiBet** —
   e qualquer operação futura SN que replicar o mesmo **contrato de views**.

**Consequência arquitetural:** bot consulta **APENAS** views/matviews do
Super Nova DB (PostgreSQL). Zero acesso direto a Athena ou Super Nova Bet DB.

---

## Visão geral em 1 parágrafo

Stakeholder manda mensagem no WhatsApp → webhook recebe no Flask (EC2) →
LLM interpreta intent + parâmetros usando o catálogo (memória da squad como
knowledge base) → roteador chama query **APENAS sobre views/matviews do Super
Nova DB (PostgreSQL)** → resultado formatado em tabela/PNG/Excel → devolve no
WhatsApp. Tudo logado no próprio Super Nova DB (schema dedicado).

**Por que só views:** (1) pipelines ETL já pré-calculam o que importa e gravam em
views — zero custo computacional pra responder; (2) Athena fica blindado de
queries "grotescas" que o usuário pode pedir; (3) mesmo contrato de views nas
várias operações SN = mesmo bot plug-and-play.

---

## Diagrama ASCII

```
 ┌────────────────────────────────────────────────────────────────────────────┐
 │                         STAKEHOLDER (Head, CTO, CGO)                        │
 │                        ─── WhatsApp no celular ───                          │
 └────────────────────────────────────────────┬───────────────────────────────┘
                                              │  "GGR afundando P4T ontem?"
                                              ▼
 ┌────────────────────────────────────────────────────────────────────────────┐
 │  Z-API (provedor WhatsApp — ~R$ 150/mes)                                    │
 │  - recebe mensagem → webhook                                                │
 │  - envia resposta → texto / imagem / Excel                                  │
 └────────────────────────────────────────────┬───────────────────────────────┘
                                              │  HTTP POST webhook
                                              ▼
 ┌────────────────────────────────────────────────────────────────────────────┐
 │  BOT APP (Flask + Python, roda na EC2 existente)                            │
 │  ┌──────────────────┐   ┌──────────────────┐   ┌───────────────────────┐   │
 │  │ 1. Guardrails    │   │ 2. LLM Router    │   │ 3. Intent Executor    │   │
 │  │ - whitelist nº   │──▶│ - lê memory/ e   │──▶│ - chama script py     │   │
 │  │ - rate limit     │   │   catálogo       │   │ - aplica filtros      │   │
 │  │ - LGPD check     │   │ - identifica     │   │ - default D-1,        │   │
 │  └──────────────────┘   │   intent + args  │   │   test users excl.    │   │
 │           │             └──────────────────┘   └───────────┬───────────┘   │
 │           │                                                │                │
 │           ▼                                                ▼                │
 │  ┌──────────────────┐                             ┌───────────────────┐   │
 │  │ 4. Formatter     │◀────────────────────────────│ 5. Renderer       │   │
 │  │ - texto          │                             │ - tabela → PNG    │   │
 │  │ - PNG tabela     │                             │   (matplotlib)    │   │
 │  │ - Excel anexo    │                             │ - Excel w/ legenda│   │
 │  └──────────────────┘                             └───────────────────┘   │
 └─────────┬──────────────────────────────────────────────────────────────────┘
           │
           ├──────────────────────────────┬──────────────────────────────┐
           ▼                              ▼                              ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │  SUPER NOVA DB (PostgreSQL)  —  UNICA FONTE QUE O BOT CONSULTA              │
 │                                                                              │
 │  ┌────────────────────────────┐    ┌─────────────────────────────────────┐ │
 │  │ Schema: bot_ana (infra)    │    │ Schemas operacionais (dados)        │ │
 │  │ - intents                  │    │                                     │ │
 │  │ - logs                     │    │ MultiBet BR:                        │ │
 │  │ - whitelist                │    │   matriz_financeiro, matriz_aquis.  │ │
 │  │ - cache_result             │    │   heatmap_hour, risco, tab_cassino  │ │
 │  │ - feedback                 │    │   (matviews e views)                │ │
 │  └────────────────────────────┘    │                                     │ │
 │                                    │ play4 (Play4Tune via foreign +      │ │
 │                                    │   views gold novas):                │ │
 │                                    │   vw_ggr_player_game_daily,         │ │
 │                                    │   vw_top_jogos_ggr, vw_ativos,      │ │
 │                                    │   vw_casino_resumo, etc.            │ │
 │                                    └─────────────────────────────────────┘ │
 └─────────────────────────────────────────────────────────────────────────────┘
                              ▲
                              │ (pipelines ETL alimentam as views — rodam em
                              │  cronjobs; bot consome dado ja pronto, zero
                              │  custo computacional no momento da pergunta)
                              │
                ┌─────────────┴────────────┐
                │                          │
       ┌────────────────┐         ┌─────────────────┐
       │ ATHENA         │         │ SUPER NOVA BET  │
       │ (Iceberg DL)   │         │ (Play4Tune raw) │
       │ fonte bruta BR │         │ fonte bruta P4T │
       │ (NAO acessado  │         │ (NAO acessado   │
       │  pelo bot)     │         │  pelo bot)      │
       └────────────────┘         └─────────────────┘
```

---

## Componentes detalhados

### 1. Z-API (provedor WhatsApp)
- **Por quê:** brasileiro, documentação PT-BR, webhook simples, sem burocracia Meta.
- **Alternativas:** Twilio (gringo, mais caro), API oficial Meta (semanas de aprovação).
- **Custo:** plano Start R$ 150/mês (até 10K mensagens).
- **Limitações:** mensagens de iniciativa precisam ser enviadas em até 24h após o último contato do usuário (regra WhatsApp). Templates precisam ser pré-aprovados.

### 2. Bot App (Flask + Python) — roda na EC2 existente
- **Por quê:** Flask é stack que já uso, EC2 já tá lá, zero custo extra.
- **Endpoints:**
  - `POST /webhook/zapi` — recebe mensagens do WhatsApp
  - `GET /healthcheck` — liveness probe
  - `POST /admin/test-intent` — testar intent manualmente (uso interno squad)
- **Deploy:** via `ec2_deploy/deploy_bot_analytics.sh` (padrão do time — git push → scp → systemd).

### 3. Super Nova DB — schema `bot_ana` (infra central, diretriz do Head)
```sql
CREATE SCHEMA IF NOT EXISTS bot_ana;

-- Catálogo de intents (single source of truth pro roteador)
CREATE TABLE bot_ana.intents (
  id              VARCHAR(64) PRIMARY KEY,       -- ex: 'ggr_afundando_p4t'
  nome            VARCHAR(200) NOT NULL,
  descricao       TEXT,
  script_path     VARCHAR(500),                  -- ex: 'scripts/report_...py'
  parametros      JSONB,                         -- schema dos parametros
  filtros_default JSONB,                         -- D-1, test users, etc.
  operacao        VARCHAR(20),                   -- MULTIBET / PLAY4 / CROSS
  stakeholders    TEXT[],                        -- quem pode perguntar
  risco_lgpd      VARCHAR(10),                   -- BAIXO / MEDIO / ALTO
  ativo           BOOLEAN DEFAULT true,
  criado_em       TIMESTAMP DEFAULT NOW()
);

-- Whitelist de numeros autorizados
CREATE TABLE bot_ana.whitelist (
  telefone        VARCHAR(20) PRIMARY KEY,       -- ex: '5531987654321'
  nome            VARCHAR(100) NOT NULL,
  cargo           VARCHAR(50),                   -- Head / CTO / CGO / ...
  intents_permitidas TEXT[],                     -- '*' = todas
  nivel_lgpd      VARCHAR(10),                   -- BAIXO / MEDIO / ALTO
  ativo           BOOLEAN DEFAULT true,
  criado_em       TIMESTAMP DEFAULT NOW()
);

-- Log de auditoria (pergunta + resposta + tempo execucao)
CREATE TABLE bot_ana.logs (
  id              BIGSERIAL PRIMARY KEY,
  recebido_em     TIMESTAMPTZ DEFAULT NOW(),
  telefone        VARCHAR(20),
  mensagem_raw    TEXT,
  intent_id       VARCHAR(64) REFERENCES bot_ana.intents(id),
  parametros      JSONB,
  status          VARCHAR(20),   -- OK / ERRO / NAO_ENTENDI / NAO_AUTORIZADO
  tempo_ms        INTEGER,
  resposta        TEXT,
  llm_tokens      INTEGER,
  llm_custo_usd   NUMERIC(8,4)
);

-- Cache de respostas (evitar re-executar query igual em 5 min)
CREATE TABLE bot_ana.cache_resultado (
  hash_pergunta   VARCHAR(64) PRIMARY KEY,       -- hash intent+parametros
  resposta_json   JSONB,
  expira_em       TIMESTAMPTZ
);

-- Feedback do usuario (👍/👎 na resposta)
CREATE TABLE bot_ana.feedback (
  id              BIGSERIAL PRIMARY KEY,
  log_id          BIGINT REFERENCES bot_ana.logs(id),
  avaliacao       VARCHAR(10),    -- util / nao_util / erro
  comentario      TEXT,
  recebido_em     TIMESTAMPTZ DEFAULT NOW()
);
```

- **Por quê aqui:** Head pediu "rodar via db (post)". Super Nova DB já roda,
  Gusta é DBA, schema isolado não polui os outros.
- **Tamanho estimado:** <100MB no primeiro ano (logs são texto curto).

### 4. LLM (Claude Haiku ou GPT-4o-mini)
- **Função:** apenas **interpretar** a pergunta e mapear pra uma intent do catálogo.
  NÃO escreve SQL, NÃO inventa query.
- **Prompt system:** catálogo de intents (lido do DB) + regras de ouro + exemplos.
- **Saída estruturada:** JSON `{intent_id, parametros, confianca}`.
- **Se confianca < 0.7:** bot pede esclarecimento ou passa pro humano na squad.
- **Custo estimado:** Haiku ~R$ 0,003 por mensagem, ~1000 msgs/mês = R$ 3-10/mês.
- **Alternativa local:** rodar Llama 3.1 8B numa EC2 (custo fixo, zero API). Vale
  avaliar na fase 2 se volume crescer.

### 5. Intent Executor
- **Core:** cada intent é basicamente uma **query parametrizada contra uma view**
  do Super Nova DB. Sem Python pesado, sem chamar Athena, sem lógica extra.
- **Exemplo:** intent `ggr_afundando_p4t` com params `(data_ini, data_fim, top_n)`
  executa algo como:
  ```sql
  SELECT * FROM play4.vw_ggr_player_game_daily
  WHERE data BETWEEN %s AND %s AND casa_perdeu = true
  ORDER BY ggr ASC LIMIT %s
  ```
- **Os scripts do `scripts/` viram backfill/criação das views**, não execução por pergunta.
  Ou seja: o trabalho de `report_players_afundando_ggr_play4.py` vira uma **view
  materializada** que atualiza via cron. O bot só faz SELECT.
- **Vantagem:** latência baixíssima (SELECT em view materializada = ms), previsível,
  audita-se a query 1x na criação da view.

### 6. Renderer / Formatter
- **Tabela pequena (≤8 linhas):** texto monoespaçado (emoji/markdown WhatsApp).
- **Tabela maior:** renderiza PNG via matplotlib (título + tabela + rodapé
  com fonte e timestamp). Z-API envia imagem.
- **Detalhe completo:** Excel com aba Legenda (padrão do time), enviado como
  anexo via Z-API.

---

## Fluxo de LGPD/Segurança (camadas)

1. **Whitelist por número** — se o número não está em `bot_ana.whitelist` com
   `ativo=true`, bot responde "não autorizado" e loga tentativa.
2. **Nível LGPD por intent** — intent com risco ALTO (ex: deep dive jogador com phone/email)
   só responde se `nivel_lgpd` do whitelist permitir.
3. **Mascaramento automático** — phone/email sempre parcialmente ocultos em
   respostas ("maharshani44377***@gmail.com").
4. **Log completo** — toda pergunta + resposta gravada. Auditável.
5. **Retention** — logs mais antigos que 180 dias são agregados/anonimizados.
6. **Sem SQL livre** — LLM nunca escreve query; só escolhe intent + parâmetros
   validados contra o schema do catálogo.

---

## Custo mensal estimado (MVP em operação)

| Item                          | Custo/mês      |
|-------------------------------|----------------|
| Z-API (plano Start)           | R$ 150         |
| LLM (Claude Haiku / GPT mini) | R$ 10-50       |
| EC2 (aproveitando existente)  | R$ 0 incremental |
| Super Nova DB (aproveitando)  | R$ 0 incremental |
| **TOTAL**                     | **R$ 160-200** |

Com 1000 mensagens/mês, custo por mensagem fica **R$ 0,20** — muito menor que
o custo do analista rodar a query (20-40 min de trabalho = R$ 30-60 de salário).

---

## Prazo estimado

**MVP (5 intents Alta freq / Baixa complexidade):**
- Semana 1: infra (schema DB + deploy EC2 + webhook Z-API), roteador LLM,
  padronização dos 5 scripts pro contrato `run(params)`.
- Semana 2: guardrails (whitelist, LGPD, rate limit), formatter/renderer, logs.
- Semana 3: beta fechado com Castrin + você, ajustes, hardening.

**Total: 3 semanas para MVP em produção com 5 intents.**

Fase 2 (+5 intents): +2 semanas.
Catálogo completo (15 intents): **~6 semanas total.**

---

## Riscos e mitigações

| Risco | Probabilidade | Impacto | Mitigação |
|-------|:-:|:-:|---|
| LLM entende errado e executa intent errada | Média | Alto | Confiança <0.7 → pede esclarecimento; confirmar antes de ações sensíveis |
| Vazamento LGPD (número fora whitelist) | Baixa | Crítico | Whitelist dura no código + DB; log auditável; alertar squad em tentativa |
| Z-API cai no sábado à noite | Baixa | Médio | Monitoramento; fallback "tenta novamente em 5min" |
| Stakeholder pede coisa que não está no catálogo | Alta | Baixo | Bot responde "não sei fazer isso ainda, adicionando na lista" + alerta pro squad |
| Bot vira muleta — ninguém mais pensa | Baixa | Médio | Intent "preciso de análise" encaminha pro humano; periodicamente revisar uso |
| Super Nova DB sobrecarregado por logs | Muito Baixa | Baixo | Logs são texto curto; <100MB/ano estimado |

---

## Vira produto SN (não ferramenta interna)

Como o bot consulta **exclusivamente views/matviews** do Super Nova DB, qualquer
operação SN que implemente o **mesmo contrato de views** recebe o bot plug-and-play.

**Contrato mínimo de views (propostas — refinar com Gusta/CTO):**

| View canônica (nome comum) | Conteúdo | Granularidade | Observação |
|----------------------------|----------|:-:|---|
| `vw_ggr_diario`            | GGR da casa por dia (casino + sb) | dia | já existe em várias formas |
| `vw_top_jogos_ggr`         | Ranking jogos por GGR | jogo × período | Play4 já tem |
| `vw_ggr_player_game_daily` | GGR granular player × jogo × dia | triplo | Play4 já tem (criada 09/04) |
| `vw_cadastros_ftd`         | Cadastros e FTD por canal/dia | dia × canal | Play4 já tem |
| `vw_matriz_financeiro`     | KPIs financeiros consolidados | dia | MultiBet tem |
| `vw_matriz_aquisicao`      | Aquisição por fonte/tracker | dia × fonte | MultiBet tem |
| `vw_risco_flags`           | Flags de risco ativos | PID × flag × dia | MultiBet tem |
| `vw_rtp_jogos`             | RTP observado vs configurado | jogo × janela | **criar** |
| `vw_affiliates_resumo`     | Performance affiliates top | affiliate × dia | MultiBet tem parcial |

**Estratégia:**
- **MultiBet BR** tem ~70% das views; faltam algumas.
- **Play4Tune** já ganhou views gold em 08-09/04 (Gusta + Castrin), continua crescendo.
- Cada operação nova SN que for plugada só precisa implementar essas views ETL →
  bot atende sem desenvolvimento adicional.

**É por isso que vira produto, não ferramenta interna.**

---

## Pontos abertos pro Castrin/Gabriel

1. **"Rodar via db (post)"** — é isso que eu entendi (Super Nova DB como infra central),
   ou o Head quis dizer outra coisa? (ex: expor API POST? rodar tudo no Postgres e não chamar Athena?)
2. **Whitelist inicial** — quem entra no MVP? (sugestão: Castrin, Mauro, Gusta, Gabriel, você + Head de Tráfego)
3. **Orçamento aprovado:** ~R$ 200/mês para provedor + LLM?
4. **Z-API ou API oficial Meta?** (Z-API agiliza MVP; oficial Meta é mais robusta a longo prazo)
5. **Fast-track MVP em 3 semanas ou colocar no backlog normal?**
6. **Responsabilidade por erro** — se bot entregar número errado e Head tomar decisão ruim,
   quem responde? (Sugestão: squad audita cada intent antes de subir; depois é do bot, com possibilidade de correção rápida.)

---

## Próximos passos concretos

1. **Castrin aprova conceito** (1-pager executivo — próximo doc).
2. **Definir whitelist** e criar tabela no Super Nova DB (Gusta libera).
3. **Gusta avalia impacto** no Super Nova DB (tamanho, backup).
4. **Contratar Z-API** (plano Start).
5. **Padronizar primeiros 5 scripts** pro contrato `run(params)`.
6. **Deploy beta na EC2** — testar com os 2-3 números do MVP antes de abrir pro time.

---
