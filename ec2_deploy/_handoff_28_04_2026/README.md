# Handoff Segmentação A+S — 28/04/2026

**Foco:** atualizar PCR + Segmentação A+S no orquestrador. Mais nada.

## Os 6 arquivos desta pasta vão para a EC2 nestes destinos:

| De (aqui) | Para (EC2) | Ação |
|---|---|---|
| `pipelines/pcr_pipeline.py` | `~/multibet-analytics/pipelines/pcr_pipeline.py` | substitui |
| `pipelines/segmentacao_sa_diaria.py` | `~/multibet-analytics/pipelines/segmentacao_sa_diaria.py` | substitui |
| `pipelines/segmentacao_sa_enriquecimento.py` | `~/multibet-analytics/pipelines/segmentacao_sa_enriquecimento.py` | NOVO |
| `pipelines/segmentacao_sa_smartico.py` | `~/multibet-analytics/pipelines/segmentacao_sa_smartico.py` | NOVO |
| `db/slack_uploader.py` | `~/multibet-analytics/db/slack_uploader.py` | NOVO |
| `run_segmentacao_sa.sh` | `~/multibet-analytics/run_segmentacao_sa.sh` | substitui |

## Mais 3 coisas (não-arquivo)

1. **Instalar dependência:**
   ```bash
   pip install slack-sdk
   ```

2. **Adicionar 2 vars no `.env` da EC2:**
   ```
   SLACK_BOT_TOKEN=xoxb-8609546431681-11018748003444-tTxNdg9d5K3pQzhcJcLEfEy1
   SLACK_CHANNEL_ID=C0B0JNUR2F6
   ```

3. **Cron:**
   - **Tirar:** `push_pcr_to_smartico.py` (não roda mais — `segmentacao_sa_diaria.py` faz o push agora)
   - **Manter:** `pcr_pipeline.py` às **03:30 BRT**
   - **Manter:** `segmentacao_sa_diaria.py` às **04:00 BRT**

## Não precisa mexer em nada fora dessa pasta

Os outros pipelines do orquestrador (sync_meta_ads, grandes_ganhos, etc.) **não mudaram**. Deixa como está.
