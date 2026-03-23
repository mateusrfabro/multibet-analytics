-- =============================================================================
-- REG e FTD por HORA para affiliates Google Ads
-- Affiliates: 297657 (principal), 445431, 468114
-- Fonte: ps_bi.dim_user (camada dbt pre-agregada, valores em BRL)
-- Autor: Extractor (iGaming Data Squad)
-- Data: 2026-03-21
-- =============================================================================
-- RACIONAL:
-- ps_bi.dim_user tem signup_datetime (UTC) e ftd_datetime (UTC), ambos com
-- granularidade de segundo. Convertemos para BRT e agrupamos por hora.
--
-- CUIDADO COM O FUSO: registration_date e ftd_date sao datas UTC.
-- Para capturar tudo que e "dia 21 BRT", precisamos incluir o dia anterior
-- em UTC (21h-23h UTC do dia 20 = 00h-02h BRT do dia 21) e o dia corrente.
-- Depois filtramos pela data BRT no HAVING/WHERE interno.
--
-- REG e FTD sao eventos INDEPENDENTES: um player pode ter se registrado
-- em outro dia e feito FTD hoje. A taxa de conversao so faz sentido
-- para a mesma cohort (REGs do dia).
-- =============================================================================

WITH
-- CTE 1: Registros por hora BRT
-- Inclui dia anterior UTC para pegar registros que em BRT caem no dia alvo
reg_por_hora AS (
    SELECT
        CAST(date_trunc('hour',
            signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
        ) AS VARCHAR) AS hora_brt,
        HOUR(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora,
        CAST(affiliate_id AS VARCHAR) AS affiliate_id,
        COUNT(*) AS reg
    FROM ps_bi.dim_user
    WHERE
        -- Particao: incluir dia anterior e corrente para cobrir fuso BRT
        registration_date IN (DATE '2026-03-20', DATE '2026-03-21')
        -- Filtro real em BRT: somente registros do dia 21 em horario brasileiro
        AND CAST(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-21'
        -- Affiliates Google Ads
        AND CAST(affiliate_id AS VARCHAR) IN ('297657', '445431', '468114')
        -- Excluir test users
        AND is_test = false
    GROUP BY 1, 2, 3
),

-- CTE 2: FTDs por hora BRT
-- Mesmo tratamento de fuso para ftd_date
ftd_por_hora AS (
    SELECT
        CAST(date_trunc('hour',
            ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
        ) AS VARCHAR) AS hora_brt,
        HOUR(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora,
        CAST(affiliate_id AS VARCHAR) AS affiliate_id,
        COUNT(*) AS ftd,
        -- Valor total dos FTDs em BRL (ps_bi ja esta em BRL, NAO dividir)
        SUM(ftd_amount_inhouse) AS ftd_amount_brl
    FROM ps_bi.dim_user
    WHERE
        -- Particao: incluir dia anterior e corrente para cobrir fuso BRT
        ftd_date IN (DATE '2026-03-20', DATE '2026-03-21')
        -- Filtro real em BRT: somente FTDs do dia 21 em horario brasileiro
        AND CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-21'
        -- Affiliates Google Ads
        AND CAST(affiliate_id AS VARCHAR) IN ('297657', '445431', '468114')
        -- Excluir test users
        AND is_test = false
    GROUP BY 1, 2, 3
)

-- Query final: FULL OUTER JOIN para capturar horas com REG sem FTD e vice-versa
SELECT
    COALESCE(r.hora_brt, f.hora_brt)           AS hora_brt,
    COALESCE(r.hora, f.hora)                   AS hora,
    COALESCE(r.affiliate_id, f.affiliate_id)   AS affiliate_id,
    COALESCE(r.reg, 0)                         AS reg,
    COALESCE(f.ftd, 0)                         AS ftd,
    ROUND(COALESCE(f.ftd_amount_brl, 0), 2)   AS ftd_amount_brl,
    -- Taxa de conversao REG -> FTD (%)
    -- So faz sentido quando ha REGs (mesma cohort); caso contrario NULL
    CASE
        WHEN COALESCE(r.reg, 0) > 0
        THEN ROUND(CAST(COALESCE(f.ftd, 0) AS DOUBLE) / r.reg * 100, 1)
        ELSE NULL
    END AS conv_rate_pct
FROM reg_por_hora r
FULL OUTER JOIN ftd_por_hora f
    ON r.hora_brt = f.hora_brt AND r.affiliate_id = f.affiliate_id
ORDER BY affiliate_id, hora