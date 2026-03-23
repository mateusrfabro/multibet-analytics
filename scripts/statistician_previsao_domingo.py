"""
Previsao Estatistica — Domingo 23/03/2026
==========================================
Autor: PhD Estatistica Aplicada (iGaming Forecasting)
Analista: Mateus F. (Squad Intelligence Engine)
Data: 2026-03-22

Objetivo: Gerar previsoes com rigor estatistico para o domingo 23/03/2026
usando dados historicos de 7 domingos consecutivos (02/fev a 16/mar 2026).

Metodologia:
  - Media simples, media ponderada exponencial, regressao linear
  - Intervalos de confianca via t-Student (n=7, amostra pequena)
  - Teste de tendencia Mann-Kendall + regressao com p-value
  - Analise de volatilidade GGR (CV, ajuste de distribuicao)
  - Padrao horario (golden hours para CRM)
  - Cenarios (base, otimista, pessimista)
  - Comparativo domingo/sabado

Premissas e limitacoes:
  - Amostra pequena (n=7) — todos os intervalos usam t-Student, nao Normal
  - Assume estacionariedade local (sem quebras estruturais)
  - GGR casino tem variancia extrema — distribuicao assimetrica
  - Dados de sabado 22/03 sao parciais (podem subestimar)
  - Sazonalidade dia do mes nao pode ser isolada com n=7

Saida:
  - Console: resumo completo em linguagem acessivel
  - JSON: output/previsao_domingo_23mar2026.json
"""

import sys
import os
import json
import logging
import warnings
from datetime import datetime

# Forcar UTF-8 no stdout/stderr (evitar UnicodeEncodeError no Windows cp1252)
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
if sys.stderr.encoding != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8")

# Caminho do projeto
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)

import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import t as t_dist
from scipy.stats import shapiro, lognorm, gamma, kstest

warnings.filterwarnings("ignore", category=RuntimeWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# Diretorios
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# DADOS HISTORICOS (extraidos via Athena em 22/03/2026)
# Fonte: scripts/extract_domingos_historico.py
# =============================================================================

# Depositos por domingo (CORRIGIDO: datas reais de DOMINGO, nao segunda)
DEPOSITOS = pd.DataFrame({
    "data": pd.to_datetime([
        "2026-02-01", "2026-02-08", "2026-02-15", "2026-02-22",
        "2026-03-01", "2026-03-08", "2026-03-15"
    ]),
    "qtd": [8915, 9622, 8390, 8393, 9312, 11066, 11409],
    "total_brl": [1037980.43, 1189437.28, 1079345.06, 1095181.67,
                  1331640.68, 1432130.48, 1536200.59],
    "unicos": [5348, 5515, 4499, 4766, 5243, 6050, 5847],
    "ticket": [116.43, 123.62, 128.65, 130.49, 143.00, 129.42, 134.65],
})

# Saques por domingo (CORRIGIDO)
SAQUES = pd.DataFrame({
    "data": pd.to_datetime([
        "2026-02-01", "2026-02-08", "2026-02-15", "2026-02-22",
        "2026-03-01", "2026-03-08", "2026-03-15"
    ]),
    "total_saques_brl": [1094286.31, 1161913.87, 1109025.89, 1426418.76,
                         1170929.42, 1781574.61, 1890606.37],
})

# GGR Casino por domingo (CORRIGIDO)
GGR = pd.DataFrame({
    "data": pd.to_datetime([
        "2026-02-01", "2026-02-08", "2026-02-15", "2026-02-22",
        "2026-03-01", "2026-03-08", "2026-03-15"
    ]),
    "ggr_casino": [1151619.92, 2072286.28, 3519264.61, 485523.36,
                   206692.25, 157790.70, -51668.41],
})

# FTDs por domingo (CORRIGIDO)
FTD = pd.DataFrame({
    "data": pd.to_datetime([
        "2026-02-01", "2026-02-08", "2026-02-15", "2026-02-22",
        "2026-03-01", "2026-03-08", "2026-03-15"
    ]),
    "registros": [2024, 1895, 2287, 2555, 2406, 2485, 1604],
    "ftds": [862, 804, 714, 957, 932, 1028, 670],
    "conversao_pct": [42.59, 42.43, 31.22, 37.46, 38.74, 41.37, 41.77],
})

# Padrao horario (agregado de 7 domingos)
HORARIO = pd.DataFrame({
    "hora": list(range(24)),
    "qtd": [2612, 2020, 1220, 894, 609, 537, 876, 1420,
            2018, 2549, 3207, 3122, 3368, 3540, 3265, 3693,
            3799, 3364, 2978, 2894, 2921, 2718, 2441, 2127],
    "total_brl": [275796.42, 216591.37, 136999.09, 84660.66, 85878.60,
                  75687.00, 101497.58, 127078.08, 211004.12, 335114.94,
                  446679.13, 443744.50, 493640.93, 558439.81, 475064.12,
                  557900.60, 553748.98, 424161.63, 417378.04, 362766.20,
                  409665.61, 343916.87, 287832.81, 238688.67],
    "depositantes": [1916, 1473, 900, 629, 450, 418, 700, 1086,
                     1571, 1974, 2492, 2448, 2589, 2662, 2504, 2775,
                     2875, 2527, 2290, 2131, 2217, 2033, 1806, 1545],
    "ticket": [105.59, 107.22, 112.29, 94.70, 141.02, 140.94, 115.86,
               89.49, 104.56, 131.47, 139.28, 142.13, 146.57, 157.75,
               145.50, 151.07, 145.76, 126.09, 140.15, 125.35, 140.25,
               126.53, 117.92, 112.22],
})

# Media depositos por dia da semana (30 dias)
MEDIA_DIA_SEMANA = {
    "Segunda": 1124000, "Terca": 1280000, "Quarta": 1259000,
    "Quinta": 1292000, "Sexta": 1944000, "Sabado": 1561000,
    "Domingo": 1349000,
}

# Sabado 22/03 (dado parcial do bot)
SABADO_22MAR = {
    "depositos_brl": 1440481.73,
    "transacoes": 10806,
    "ftds": 1389,
}


# =============================================================================
# FUNCOES AUXILIARES ESTATISTICAS
# =============================================================================

def intervalo_confianca_t(dados, confianca=0.95):
    """
    Calcula intervalo de confianca usando distribuicao t-Student.
    Adequado para amostras pequenas (n < 30).

    Por que t-Student e nao Normal?
    Com n=7, a distribuicao Normal subestimaria a incerteza.
    A t-Student tem caudas mais pesadas, gerando intervalos mais largos
    (e mais honestos) para amostras pequenas.

    Retorna: (media, margem_erro, ic_lower, ic_upper)
    """
    n = len(dados)
    media = np.mean(dados)
    sem = stats.sem(dados)  # erro padrao da media = std / sqrt(n)
    # t_critico para (n-1) graus de liberdade
    t_crit = t_dist.ppf((1 + confianca) / 2, df=n - 1)
    margem = t_crit * sem
    return media, margem, media - margem, media + margem


def media_ponderada_exponencial(dados, alpha=0.3):
    """
    Media ponderada com decaimento exponencial.
    Pesos maiores para observacoes mais recentes.

    alpha controla a velocidade de decaimento:
      - alpha alto (0.5+): pesa MUITO os ultimos dados
      - alpha baixo (0.1): quase igual a media simples

    Usamos alpha=0.3 como equilibrio entre reatividade e estabilidade.

    Retorna: valor da media ponderada
    """
    n = len(dados)
    pesos = np.array([(1 - alpha) ** (n - 1 - i) for i in range(n)])
    pesos = pesos / pesos.sum()  # normalizar para somar 1
    return np.dot(dados, pesos)


def regressao_linear_previsao(dados, passos_a_frente=1):
    """
    Regressao linear simples para extrapolacao de tendencia.
    x = indice temporal (0, 1, 2, ..., n-1)
    y = valores observados

    Retorna: (previsao, slope, intercept, r_squared, p_value, std_error_forecast)
    """
    n = len(dados)
    x = np.arange(n)
    y = np.array(dados)

    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    r_squared = r_value ** 2

    # Previsao para o proximo ponto
    x_pred = n - 1 + passos_a_frente
    y_pred = intercept + slope * x_pred

    # Erro padrao da previsao (inclui incerteza do modelo + variancia residual)
    residuos = y - (intercept + slope * x)
    s_resid = np.sqrt(np.sum(residuos ** 2) / (n - 2))  # MSE
    x_mean = np.mean(x)
    sx2 = np.sum((x - x_mean) ** 2)
    # Variancia da previsao individual (nao da media)
    se_forecast = s_resid * np.sqrt(1 + 1 / n + (x_pred - x_mean) ** 2 / sx2)

    return y_pred, slope, intercept, r_squared, p_value, se_forecast


def mann_kendall_test(dados):
    """
    Teste de tendencia de Mann-Kendall (nao-parametrico).

    Por que usar Mann-Kendall?
    - Nao assume distribuicao dos dados (nao-parametrico)
    - Robusto a outliers
    - Testa se ha tendencia monotonica (crescente ou decrescente)

    Retorna: (tau, p_value, tendencia_str)
    tau positivo = tendencia crescente | tau negativo = decrescente
    p_value < 0.05 → tendencia estatisticamente significativa a 95%
    """
    n = len(dados)
    s = 0
    for i in range(n - 1):
        for j in range(i + 1, n):
            diff = dados[j] - dados[i]
            if diff > 0:
                s += 1
            elif diff < 0:
                s -= 1
            # diff == 0 contribui 0

    # Tau de Kendall
    tau = s / (n * (n - 1) / 2)

    # Variancia de S sob H0 (sem ties)
    var_s = n * (n - 1) * (2 * n + 5) / 18

    # Estatistica Z
    if s > 0:
        z = (s - 1) / np.sqrt(var_s)
    elif s < 0:
        z = (s + 1) / np.sqrt(var_s)
    else:
        z = 0

    p_value = 2 * (1 - stats.norm.cdf(abs(z)))

    if p_value < 0.05:
        tendencia = "CRESCENTE" if tau > 0 else "DECRESCENTE"
    else:
        tendencia = "SEM TENDENCIA SIGNIFICATIVA"

    return tau, p_value, tendencia


def coeficiente_variacao(dados):
    """
    Coeficiente de variacao (CV) = desvio padrao / media * 100

    Interpretacao para iGaming:
      CV < 15%: baixa variacao (depositos tendem a ser previsive
      CV 15-30%: variacao moderada
      CV > 30%: alta variacao (GGR casino tipicamente cai aqui)
      CV > 50%: variacao extrema — previsao pontual pouco confiavel
    """
    media = np.mean(dados)
    std = np.std(dados, ddof=1)
    return (std / media) * 100 if media != 0 else float("inf")


def ajustar_distribuicoes(dados):
    """
    Testa qual distribuicao se ajusta melhor aos dados.
    Candidatas: Normal, Lognormal, Gamma.

    Usa o teste Kolmogorov-Smirnov para medir aderencia.
    Menor p-value de KS = pior ajuste (queremos p alto).
    """
    resultados = {}

    # Normal
    mu, sigma = stats.norm.fit(dados)
    ks_stat, ks_p = kstest(dados, "norm", args=(mu, sigma))
    resultados["Normal"] = {
        "params": {"mu": mu, "sigma": sigma},
        "ks_stat": ks_stat,
        "ks_p": ks_p,
    }

    # Lognormal (so se todos positivos)
    if np.all(np.array(dados) > 0):
        shape, loc, scale = lognorm.fit(dados, floc=0)
        ks_stat, ks_p = kstest(dados, "lognorm", args=(shape, loc, scale))
        resultados["Lognormal"] = {
            "params": {"shape": shape, "loc": loc, "scale": scale},
            "ks_stat": ks_stat,
            "ks_p": ks_p,
        }

    # Gamma (so se todos positivos)
    if np.all(np.array(dados) > 0):
        a, loc, scale = gamma.fit(dados, floc=0)
        ks_stat, ks_p = kstest(dados, "gamma", args=(a, loc, scale))
        resultados["Gamma"] = {
            "params": {"a": a, "loc": loc, "scale": scale},
            "ks_stat": ks_stat,
            "ks_p": ks_p,
        }

    # Ordenar por melhor ajuste (maior p-value do KS)
    melhor = max(resultados.items(), key=lambda x: x[1]["ks_p"])
    return melhor[0], melhor[1], resultados


# =============================================================================
# 1. PREVISAO DE DEPOSITOS DOMINGO 23/03
# =============================================================================

def prever_depositos():
    """
    Combina 3 metodos + calcula intervalos de confianca.

    RACIONAL:
    - Media simples: baseline conservador, pesa todos os domingos igualmente
    - Media ponderada: reativo a tendencia recente (ultimos 3 crescentes)
    - Regressao linear: captura tendencia temporal explicita
    - Previsao final: media dos 3 metodos (ensemble simples)

    INTERVALO DE CONFIANCA:
    - t-Student com n-1=6 graus de liberdade
    - IC 80%: 1 em 5 vezes o valor real ficara fora (usado para planejamento)
    - IC 95%: 1 em 20 vezes (usado para comunicacao ao C-level)
    """
    dados = DEPOSITOS["total_brl"].values
    n = len(dados)

    # --- Metodo 1: Media simples ---
    media_simples = np.mean(dados)

    # --- Metodo 2: Media ponderada exponencial ---
    media_pond = media_ponderada_exponencial(dados, alpha=0.3)

    # --- Metodo 3: Regressao linear ---
    reg_pred, slope, intercept, r2, p_val, se_forecast = regressao_linear_previsao(dados)

    # --- Previsao combinada (ensemble) ---
    # Pesos: regressao e ponderada recebem mais peso se tendencia significativa
    if p_val < 0.10:  # tendencia marginalmente significativa
        previsao_final = 0.25 * media_simples + 0.35 * media_pond + 0.40 * reg_pred
    else:
        previsao_final = 0.34 * media_simples + 0.33 * media_pond + 0.33 * reg_pred

    # --- Intervalos de confianca ---
    # Usamos a variancia residual da regressao + t-Student
    _, _, ic80_lo, ic80_hi = intervalo_confianca_t(dados, 0.80)
    _, _, ic95_lo, ic95_hi = intervalo_confianca_t(dados, 0.95)

    # Tambem calculamos IC para a previsao da regressao
    t80 = t_dist.ppf(0.90, df=n - 2)  # 80% bilateral -> 90% unilateral
    t95 = t_dist.ppf(0.975, df=n - 2)
    reg_ic80 = (reg_pred - t80 * se_forecast, reg_pred + t80 * se_forecast)
    reg_ic95 = (reg_pred - t95 * se_forecast, reg_pred + t95 * se_forecast)

    # IC final usa o IC da regressao (mais informativo, incorpora tendencia)
    ic80_final = reg_ic80
    ic95_final = reg_ic95

    resultado = {
        "previsao_final_brl": round(previsao_final, 2),
        "metodo_media_simples": round(media_simples, 2),
        "metodo_media_ponderada": round(media_pond, 2),
        "metodo_regressao_linear": round(reg_pred, 2),
        "ic_80": {
            "lower": round(ic80_final[0], 2),
            "upper": round(ic80_final[1], 2),
        },
        "ic_95": {
            "lower": round(ic95_final[0], 2),
            "upper": round(ic95_final[1], 2),
        },
        "regressao": {
            "slope_por_semana": round(slope, 2),
            "r_squared": round(r2, 4),
            "p_value": round(p_val, 4),
            "interpretacao": (
                f"A cada domingo, os depositos crescem ~R$ {slope:,.0f}. "
                f"O modelo explica {r2*100:.1f}% da variacao. "
                f"p-value={p_val:.4f} {'< 0.05 (SIGNIFICATIVO)' if p_val < 0.05 else '(nao significativo a 5%)'}"
            ),
        },
        "premissas": [
            f"Amostra: {n} domingos (02/fev a 16/mar 2026)",
            "Distribuicao t-Student com 5 graus de liberdade (regressao) e 6 gl (media)",
            "Assume continuidade da tendencia observada (sem eventos extraordinarios)",
            "23/03 e fim de mes (potencial efeito salario — nao quantificavel com n=7)",
        ],
    }

    # Submetricas
    qtd_dados = DEPOSITOS["qtd"].values
    qtd_pred, _, _, _, _, _ = regressao_linear_previsao(qtd_dados)
    unicos_dados = DEPOSITOS["unicos"].values
    unicos_pred, _, _, _, _, _ = regressao_linear_previsao(unicos_dados)
    ticket_dados = DEPOSITOS["ticket"].values

    resultado["submetricas"] = {
        "qtd_depositos_prevista": int(round(qtd_pred)),
        "depositantes_unicos_previstos": int(round(unicos_pred)),
        "ticket_medio_previsto": round(np.mean(ticket_dados), 2),
        "ticket_medio_recente_3dom": round(np.mean(ticket_dados[-3:]), 2),
    }

    return resultado


# =============================================================================
# 2. PREVISAO NET DEPOSIT
# =============================================================================

def prever_net_deposit():
    """
    Net Deposit = Depositos - Saques

    RACIONAL:
    Em iGaming, Net Deposit e o fluxo de caixa real.
    - Positivo: jogadores estao trazendo mais dinheiro do que retirando
    - Negativo: jogadores estao sacando mais do que depositando

    Para a operacao, Net Deposit negativo em um dia nao e necessariamente ruim
    (pode ser pagamento de premios grandes), mas uma TENDENCIA negativa sim.
    """
    dep = DEPOSITOS["total_brl"].values
    saq = SAQUES["total_saques_brl"].values
    net = dep - saq

    n = len(net)
    media_net = np.mean(net)
    std_net = np.std(net, ddof=1)

    # IC t-Student
    _, _, ic80_lo, ic80_hi = intervalo_confianca_t(net, 0.80)
    _, _, ic95_lo, ic95_hi = intervalo_confianca_t(net, 0.95)

    # Probabilidade de net deposit positivo
    # Usando t-Student: P(X > 0) onde X ~ t(media, sem, df=n-1)
    sem = std_net / np.sqrt(n)
    t_stat = (0 - media_net) / sem
    prob_positivo = 1 - t_dist.cdf(t_stat, df=n - 1)

    # Regressao para tendencia
    reg_pred, slope, _, r2, p_val, se_forecast = regressao_linear_previsao(net)

    # Historico detalhado
    historico = []
    for i in range(n):
        historico.append({
            "data": DEPOSITOS["data"].iloc[i].strftime("%Y-%m-%d"),
            "depositos": round(dep[i], 2),
            "saques": round(saq[i], 2),
            "net_deposit": round(net[i], 2),
            "sinal": "POSITIVO" if net[i] > 0 else "NEGATIVO",
        })

    # Ratio saques/depositos
    ratio_saq_dep = saq / dep

    resultado = {
        "previsao_net_brl": round(media_net, 2),
        "previsao_regressao_brl": round(reg_pred, 2),
        "ic_80": {"lower": round(ic80_lo, 2), "upper": round(ic80_hi, 2)},
        "ic_95": {"lower": round(ic95_lo, 2), "upper": round(ic95_hi, 2)},
        "probabilidade_positivo_pct": round(prob_positivo * 100, 2),
        "media_historica": round(media_net, 2),
        "desvio_padrao": round(std_net, 2),
        "domingos_negativos": int(np.sum(net < 0)),
        "domingos_positivos": int(np.sum(net > 0)),
        "ratio_medio_saques_depositos": round(np.mean(ratio_saq_dep), 4),
        "tendencia_slope": round(slope, 2),
        "tendencia_p_value": round(p_val, 4),
        "historico": historico,
        "interpretacao": (
            f"Nos 7 domingos, {int(np.sum(net < 0))} foram negativos (saques > depositos). "
            f"A media de net deposit e R$ {media_net:,.2f}. "
            f"Probabilidade de net positivo domingo: {prob_positivo*100:.1f}%. "
            f"Ratio medio saques/depositos: {np.mean(ratio_saq_dep):.2%} "
            f"(para cada R$1 depositado, R$ {np.mean(ratio_saq_dep):.2f} sao sacados)."
        ),
    }

    return resultado


# =============================================================================
# 3. ANALISE DE TENDENCIA
# =============================================================================

def analisar_tendencia():
    """
    Testa se os depositos aos domingos estao REALMENTE crescendo.

    DOIS TESTES:
    1. Mann-Kendall (nao-parametrico): nao assume distribuicao
    2. Regressao linear: assume relacao linear + normalidade dos residuos

    Por que dois testes?
    Se ambos concordam → confianca alta no resultado.
    Se divergem → cautela na interpretacao.

    CONTEXTO BUSINESS:
    Crescimento de depositos pode ser por:
    - Aumento de base ativa (mais jogadores)
    - Aumento de ticket medio (jogadores depositando mais)
    - Efeito calendario (epoca do ano)
    - Acoes de CRM/marketing
    Sem modelo causal, nao sabemos QUAL fator domina.
    """
    dep = DEPOSITOS["total_brl"].values
    qtd = DEPOSITOS["qtd"].values
    unicos = DEPOSITOS["unicos"].values
    ticket = DEPOSITOS["ticket"].values

    # Teste Mann-Kendall para depositos
    mk_tau, mk_p, mk_tend = mann_kendall_test(dep)

    # Regressao linear
    _, slope, _, r2, p_val, _ = regressao_linear_previsao(dep)

    # Testes para submetricas
    mk_qtd_tau, mk_qtd_p, mk_qtd_tend = mann_kendall_test(qtd)
    mk_uni_tau, mk_uni_p, mk_uni_tend = mann_kendall_test(unicos)
    mk_tkt_tau, mk_tkt_p, mk_tkt_tend = mann_kendall_test(ticket)

    # Teste de normalidade dos residuos (Shapiro-Wilk)
    x = np.arange(len(dep))
    residuos = dep - (slope * x + (np.mean(dep) - slope * np.mean(x)))
    sw_stat, sw_p = shapiro(residuos)

    # Ultimos 3 domingos — mini-tendencia
    ultimos3 = dep[-3:]
    crescente_consecutivo = all(ultimos3[i] < ultimos3[i + 1] for i in range(len(ultimos3) - 1))

    resultado = {
        "mann_kendall": {
            "tau": round(mk_tau, 4),
            "p_value": round(mk_p, 4),
            "conclusao": mk_tend,
            "interpretacao": (
                f"Tau = {mk_tau:.3f} (positivo = tendencia de alta). "
                f"p-value = {mk_p:.4f}. "
                f"{'EVIDENCIA de tendencia crescente' if mk_p < 0.10 else 'Nao ha evidencia estatistica suficiente de tendencia'} "
                f"(threshold: p < 0.10 para marginal, p < 0.05 para significativo)."
            ),
        },
        "regressao_linear": {
            "slope_brl_por_semana": round(slope, 2),
            "r_squared": round(r2, 4),
            "p_value": round(p_val, 4),
            "conclusao": (
                "SIGNIFICATIVO" if p_val < 0.05
                else "MARGINALMENTE SIGNIFICATIVO" if p_val < 0.10
                else "NAO SIGNIFICATIVO"
            ),
            "interpretacao": (
                f"Slope: +R$ {slope:,.0f}/semana. R2={r2:.3f} ({r2*100:.1f}% da variacao explicada). "
                f"p-value={p_val:.4f}."
            ),
        },
        "normalidade_residuos": {
            "shapiro_wilk_stat": round(sw_stat, 4),
            "shapiro_wilk_p": round(sw_p, 4),
            "residuos_normais": sw_p > 0.05,
            "interpretacao": (
                f"p={sw_p:.4f}. "
                f"{'Residuos sao normais (ok para regressao)' if sw_p > 0.05 else 'Residuos NAO normais — regressao pode ser inadequada'}"
            ),
        },
        "submetricas": {
            "qtd_depositos": {"tau": round(mk_qtd_tau, 4), "p": round(mk_qtd_p, 4), "tendencia": mk_qtd_tend},
            "depositantes_unicos": {"tau": round(mk_uni_tau, 4), "p": round(mk_uni_p, 4), "tendencia": mk_uni_tend},
            "ticket_medio": {"tau": round(mk_tkt_tau, 4), "p": round(mk_tkt_p, 4), "tendencia": mk_tkt_tend},
        },
        "ultimos_3_domingos_crescentes": crescente_consecutivo,
        "conclusao_geral": "",
    }

    # Conclusao consolidada
    testes_concordam = (
        (mk_p < 0.10 and p_val < 0.10) or
        (mk_p >= 0.10 and p_val >= 0.10)
    )

    if mk_p < 0.05 and p_val < 0.05:
        conclusao = (
            "TENDENCIA CRESCENTE CONFIRMADA por ambos os testes (p < 0.05). "
            "Os depositos aos domingos estao genuinamente crescendo."
        )
    elif mk_p < 0.10 or p_val < 0.10:
        conclusao = (
            "TENDENCIA MARGINALMENTE SIGNIFICATIVA. Ha indicios de crescimento, "
            "mas com n=7 nao e possivel afirmar com 95% de confianca. "
            "Os ultimos 3 domingos consecutivamente crescentes reforçam a hipotese."
        )
    else:
        conclusao = (
            "SEM EVIDENCIA ESTATISTICA de tendencia. A variacao observada pode ser "
            "aleatoria. Os ultimos 3 domingos crescentes podem ser coincidencia."
        )

    resultado["conclusao_geral"] = conclusao
    resultado["testes_concordam"] = testes_concordam

    return resultado


# =============================================================================
# 4. ANALISE DE VOLATILIDADE GGR
# =============================================================================

def analisar_volatilidade_ggr():
    """
    O GGR Casino tem variancia ENORME.
    Isso e NORMAL em iGaming: um unico jogador com grande ganho pode
    inverter o GGR do dia todo.

    METRICAS:
    - CV: coeficiente de variacao (std/media*100)
    - Ajuste de distribuicao: qual modelo probabilistico melhor descreve o GGR
    - Range provavel: P10-P90 para planejamento

    POR QUE O GGR E TAO VOLATIL:
    - E a diferença entre bets e wins (margem)
    - Um jackpot pago de R$ 500K pode levar o GGR a negativo
    - A variancia do GGR > variancia dos depositos SEMPRE
    """
    dados = GGR["ggr_casino"].values

    cv = coeficiente_variacao(dados)
    media = np.mean(dados)
    mediana = np.median(dados)
    std = np.std(dados, ddof=1)
    minimo = np.min(dados)
    maximo = np.max(dados)

    # ICs
    _, _, ic80_lo, ic80_hi = intervalo_confianca_t(dados, 0.80)
    _, _, ic95_lo, ic95_hi = intervalo_confianca_t(dados, 0.95)

    # Ajuste de distribuicoes
    melhor_dist, melhor_params, todas = ajustar_distribuicoes(dados)

    # Percentis empiricos (com n=7, sao aproximados)
    p10 = np.percentile(dados, 10)
    p25 = np.percentile(dados, 25)
    p50 = np.percentile(dados, 50)
    p75 = np.percentile(dados, 75)
    p90 = np.percentile(dados, 90)

    # Range interquartil (IQR)
    iqr = p75 - p25

    # Skewness (assimetria) — GGR tende a ser assimetrico positivo
    skew = stats.skew(dados)
    kurtosis = stats.kurtosis(dados)

    # Separar fases (visual: parece haver 2 regimes)
    fase1 = dados[:4]  # fev: GGR mais alto
    fase2 = dados[4:]  # mar: GGR mais baixo
    media_fase1 = np.mean(fase1)
    media_fase2 = np.mean(fase2)

    resultado = {
        "cv_pct": round(cv, 2),
        "classificacao_volatilidade": (
            "EXTREMA" if cv > 80 else
            "MUITO ALTA" if cv > 50 else
            "ALTA" if cv > 30 else
            "MODERADA"
        ),
        "estatisticas_descritivas": {
            "media": round(media, 2),
            "mediana": round(mediana, 2),
            "desvio_padrao": round(std, 2),
            "minimo": round(minimo, 2),
            "maximo": round(maximo, 2),
            "range": round(maximo - minimo, 2),
            "skewness": round(skew, 4),
            "kurtosis": round(kurtosis, 4),
        },
        "percentis": {
            "p10": round(p10, 2),
            "p25": round(p25, 2),
            "p50_mediana": round(p50, 2),
            "p75": round(p75, 2),
            "p90": round(p90, 2),
            "iqr": round(iqr, 2),
        },
        "ic_80": {"lower": round(ic80_lo, 2), "upper": round(ic80_hi, 2)},
        "ic_95": {"lower": round(ic95_lo, 2), "upper": round(ic95_hi, 2)},
        "melhor_distribuicao": {
            "nome": melhor_dist,
            "ks_p_value": round(melhor_params["ks_p"], 4),
            "interpretacao": (
                f"A distribuicao {melhor_dist} e a que melhor se ajusta (KS p={melhor_params['ks_p']:.4f}). "
                f"{'Ajuste razoavel.' if melhor_params['ks_p'] > 0.10 else 'Nenhuma distribuicao se ajusta bem — dados muito irregulares.'}"
            ),
        },
        "regimes": {
            "fase_fev": {
                "media": round(media_fase1, 2),
                "domingos": "02/fev a 23/fev",
                "descricao": "GGR mais alto (media R$ {:.0f}K)".format(media_fase1 / 1000),
            },
            "fase_mar": {
                "media": round(media_fase2, 2),
                "domingos": "02/mar a 16/mar",
                "descricao": "GGR mais baixo (media R$ {:.0f}K)".format(media_fase2 / 1000),
            },
            "interpretacao": (
                "Parece haver uma MUDANCA DE REGIME entre fevereiro e março. "
                f"Media fev: R$ {media_fase1:,.0f} vs Media mar: R$ {media_fase2:,.0f}. "
                "Se o regime de março se mantiver, o GGR esperado para 23/03 e "
                f"~R$ {media_fase2:,.0f} (mas com alta incerteza)."
            ),
        },
        "range_provavel_domingo": {
            "conservador_p25_p75": {
                "lower": round(p25, 2),
                "upper": round(p75, 2),
            },
            "regime_marco": {
                "lower": round(np.min(dados[4:]), 2),
                "upper": round(np.max(dados[4:]), 2),
                "media": round(media_fase2, 2),
            },
        },
        "interpretacao_negocio": (
            f"O GGR Casino nos domingos tem CV de {cv:.0f}% — volatilidade "
            f"{'extrema' if cv > 80 else 'muito alta' if cv > 50 else 'alta'}. "
            "Isso significa que uma previsao pontual de GGR e POUCO CONFIAVEL. "
            "Para planejamento, use o range de cenarios, nao um numero unico. "
            f"Considerando apenas março, espere entre R$ {np.min(dados[4:]):,.0f} "
            f"e R$ {np.max(dados[4:]):,.0f}, com media de R$ {media_fase2:,.0f}."
        ),
    }

    return resultado


# =============================================================================
# 5. PADRAO HORARIO — GOLDEN HOURS
# =============================================================================

def analisar_padrao_horario():
    """
    Identifica as horas de maior concentracao de depositos nos domingos.

    POR QUE IMPORTA:
    - Campanhas CRM (push, SMS, email) tem custo fixo por disparo
    - Disparar no horario certo aumenta conversao e ROI
    - "Golden hours" = horas onde ha mais jogadores ativos E maior ticket

    METRICAS:
    - Volume: total de depositos por hora
    - Valor: total em BRL por hora
    - Ticket medio: valor medio por deposito
    - Concentracao: % do total diario naquela hora
    """
    df = HORARIO.copy()

    total_diario = df["total_brl"].sum()
    total_txn = df["qtd"].sum()

    df["pct_valor"] = (df["total_brl"] / total_diario * 100).round(2)
    df["pct_txn"] = (df["qtd"] / total_txn * 100).round(2)

    # Score composto: combina volume e ticket medio (ambos normalizados)
    df["score_volume_norm"] = (df["total_brl"] - df["total_brl"].min()) / (
        df["total_brl"].max() - df["total_brl"].min()
    )
    df["score_ticket_norm"] = (df["ticket"] - df["ticket"].min()) / (
        df["ticket"].max() - df["ticket"].min()
    )
    # Peso 60% volume, 40% ticket (volume importa mais para CRM de massa)
    df["score_roi"] = (0.6 * df["score_volume_norm"] + 0.4 * df["score_ticket_norm"]).round(4)

    # Golden hours: top 5 por score
    top5 = df.nlargest(5, "score_roi")
    golden_hours = top5["hora"].tolist()

    # Horas mortas: bottom 3
    bottom3 = df.nsmallest(3, "score_roi")
    dead_hours = bottom3["hora"].tolist()

    # Ratio pico/vale
    max_hora = df.loc[df["total_brl"].idxmax()]
    min_hora = df.loc[df["total_brl"].idxmin()]
    ratio_pico_vale = max_hora["total_brl"] / min_hora["total_brl"]

    # Janelas (periodos do dia) — dividir em 7 horas por media
    periodos = {
        "Madrugada (0-5h)": df[df["hora"].between(0, 5)],
        "Manha (6-11h)": df[df["hora"].between(6, 11)],
        "Tarde (12-17h)": df[df["hora"].between(12, 17)],
        "Noite (18-23h)": df[df["hora"].between(18, 23)],
    }

    janelas = {}
    for nome, sub in periodos.items():
        janelas[nome] = {
            "total_brl": round(sub["total_brl"].sum(), 2),
            "pct_dia": round(sub["total_brl"].sum() / total_diario * 100, 2),
            "ticket_medio": round(sub["ticket"].mean(), 2),
            "depositantes_media": int(sub["depositantes"].mean()),
        }

    # Concentracao 13-16h (parece ser o peak)
    peak_window = df[df["hora"].between(13, 16)]
    pct_peak_window = peak_window["total_brl"].sum() / total_diario * 100

    resultado = {
        "golden_hours": golden_hours,
        "golden_hours_detail": [],
        "dead_hours": dead_hours,
        "ratio_pico_vale": round(ratio_pico_vale, 2),
        "pico": {
            "hora": int(max_hora["hora"]),
            "valor_brl": round(max_hora["total_brl"], 2),
            "ticket": round(max_hora["ticket"], 2),
        },
        "vale": {
            "hora": int(min_hora["hora"]),
            "valor_brl": round(min_hora["total_brl"], 2),
            "ticket": round(min_hora["ticket"], 2),
        },
        "janelas": janelas,
        "concentracao_13_16h_pct": round(pct_peak_window, 2),
        "recomendacao_crm": [],
    }

    # Detalhes das golden hours
    for _, row in top5.iterrows():
        resultado["golden_hours_detail"].append({
            "hora": int(row["hora"]),
            "total_brl_7dom": round(row["total_brl"], 2),
            "media_por_domingo": round(row["total_brl"] / 7, 2),
            "ticket_medio": round(row["ticket"], 2),
            "score_roi": round(row["score_roi"], 4),
            "pct_dia": round(row["pct_valor"], 2),
        })

    # Recomendacoes CRM
    resultado["recomendacao_crm"] = [
        {
            "acao": "Push notifications / SMS principal",
            "horario": "12h-13h (antes do pico)",
            "racional": (
                "Disparar 30-60min ANTES do pico natural (13-16h) para "
                "capturar jogadores que ja estariam inclinados a depositar. "
                "Nao disparar no pico — o jogador ja esta ativo."
            ),
        },
        {
            "acao": "Email marketing (dose matinal)",
            "horario": "8h-9h",
            "racional": (
                "Segundo momento de subida natural. Email tem delay de leitura, "
                "entao disparar cedo para que seja lido quando o jogador ativar."
            ),
        },
        {
            "acao": "Campanha bonus / freebet",
            "horario": "17h-18h (anti-queda)",
            "racional": (
                "O volume começa a cair depois das 16h. Uma oferta nesse momento "
                "pode segurar jogadores que estariam saindo."
            ),
        },
        {
            "acao": "NAO disparar",
            "horario": "3h-6h (hora morta)",
            "racional": (
                "Volume minimo, ticket alto (poucos jogadores whale). "
                "CRM de massa aqui e desperdicio. Se houver budget limitado, "
                "alocar todo nas golden hours."
            ),
        },
    ]

    return resultado


# =============================================================================
# 6. CENARIOS
# =============================================================================

def gerar_cenarios():
    """
    Cenarios para planejamento de operacao e CRM.

    CENARIO BASE: media ponderada dos ultimos domingos + tendencia
    CENARIO OTIMISTA: base + efeito CRM + efeito fim de mes (salario)
    CENARIO PESSIMISTA: base com desconto de volatilidade

    PREMISSAS DE CADA CENARIO documentadas para auditoria.
    """
    dep = DEPOSITOS["total_brl"].values

    # Base: ensemble da previsao
    prev = prever_depositos()
    base = prev["previsao_final_brl"]

    # Uplift CRM estimado
    # Bonus domingos CRM: ~R$ 17K em bonus distribuidos (dado CRM extraido)
    # Estimamos multiplier de 3-5x do bonus em depositos adicionais
    bonus_crm = 17000  # R$ 17K medio por domingo
    multiplier_crm_conservador = 3.0
    multiplier_crm_otimista = 5.0
    uplift_crm_base = bonus_crm * multiplier_crm_conservador  # R$ 51K
    uplift_crm_otimista = bonus_crm * multiplier_crm_otimista  # R$ 85K

    # Efeito fim de mes (salario): estimado em +3% a +7%
    # 23/03 esta 8 dias antes do fim do mes — efeito PARCIAL
    efeito_salario_pct = 0.03  # conservador: +3%

    # Crescimento tendencial (slope da regressao)
    slope = prev["regressao"]["slope_por_semana"]

    # Sabado como proxy
    ratio_dom_sab_hist = np.mean(dep) / MEDIA_DIA_SEMANA["Sabado"]
    previsao_via_sabado = SABADO_22MAR["depositos_brl"] * ratio_dom_sab_hist

    # === CENARIO BASE ===
    cenario_base = base

    # === CENARIO OTIMISTA ===
    # Base + uplift CRM otimista + efeito salario
    cenario_otimista = base * (1 + efeito_salario_pct) + uplift_crm_otimista
    # Cap: nao mais que 1.5x o maximo historico
    max_hist = np.max(dep)
    cenario_otimista = min(cenario_otimista, max_hist * 1.5)

    # === CENARIO PESSIMISTA ===
    # P25 historico (abaixo da mediana)
    p25_hist = np.percentile(dep, 25)
    cenario_pessimista = p25_hist

    resultado = {
        "cenarios": {
            "base": {
                "depositos_brl": round(cenario_base, 2),
                "premissas": [
                    "Continuidade da tendencia recente",
                    "Sem eventos extraordinarios (queda de sistema, regulacao)",
                    f"Ensemble de 3 metodos (media, ponderada, regressao)",
                ],
            },
            "otimista": {
                "depositos_brl": round(cenario_otimista, 2),
                "premissas": [
                    f"Efeito salario fim de mes: +{efeito_salario_pct*100:.0f}%",
                    f"CRM bonus otimista: +R$ {uplift_crm_otimista:,.0f} (multiplier {multiplier_crm_otimista}x)",
                    "Campanhas CRM disparadas nas golden hours (13-16h)",
                    "Sem queda de plataforma ou concorrencia agressiva",
                ],
                "delta_vs_base_pct": round((cenario_otimista / cenario_base - 1) * 100, 2),
            },
            "pessimista": {
                "depositos_brl": round(cenario_pessimista, 2),
                "premissas": [
                    "Sem efeito CRM incremental",
                    "Volume cai para P25 historico",
                    "Cenario equivalente ao pior domingo recente (23/fev)",
                    "Possivel concorrencia ou evento externo",
                ],
                "delta_vs_base_pct": round((cenario_pessimista / cenario_base - 1) * 100, 2),
            },
        },
        "previsao_via_sabado": {
            "ratio_dom_sab_historico": round(ratio_dom_sab_hist, 4),
            "sabado_22mar_brl": SABADO_22MAR["depositos_brl"],
            "previsao_domingo_via_sabado": round(previsao_via_sabado, 2),
            "interpretacao": (
                f"Ratio historico dom/sab: {ratio_dom_sab_hist:.3f}. "
                f"Aplicando ao sabado 22/03 (R$ {SABADO_22MAR['depositos_brl']:,.2f}): "
                f"previsao = R$ {previsao_via_sabado:,.2f}."
            ),
        },
        "resumo_range": {
            "pessimista": round(cenario_pessimista, 2),
            "base": round(cenario_base, 2),
            "otimista": round(cenario_otimista, 2),
            "via_sabado": round(previsao_via_sabado, 2),
        },
    }

    return resultado


# =============================================================================
# 7. COMPARATIVO DOMINGO vs SABADO
# =============================================================================

def comparar_domingo_sabado():
    """
    Usa o sabado como indicador antecedente.

    RACIONAL:
    Em iGaming, sabado e domingo tem correlacao alta.
    Um sabado forte geralmente antecede um domingo forte.
    O ratio historico dom/sab permite "escalar" o dado do sabado
    para estimar o domingo.

    LIMITACAO:
    - Temos apenas 1 sabado como dado (22/03) — nao da para calcular IC
    - O ratio e baseado em medias de 30 dias, nao pareados por semana
    """
    dep = DEPOSITOS["total_brl"].values
    media_dom = np.mean(dep)
    media_sab = MEDIA_DIA_SEMANA["Sabado"]

    ratio = media_dom / media_sab
    previsao = SABADO_22MAR["depositos_brl"] * ratio

    # Ratios por outras metricas
    media_dom_qtd = np.mean(DEPOSITOS["qtd"].values)
    sab_qtd = SABADO_22MAR["transacoes"]
    # Estimativa de qtd domingo via sabado
    # Media historica: dom tem ~8K txns, se sab teve 10.8K, ratio e diferente
    # Mas nao temos historico pareado sab por sab — usar media geral

    resultado = {
        "ratio_dom_sab_valor": round(ratio, 4),
        "sabado_22mar": {
            "depositos_brl": SABADO_22MAR["depositos_brl"],
            "transacoes": SABADO_22MAR["transacoes"],
            "ftds": SABADO_22MAR["ftds"],
        },
        "previsao_domingo_via_ratio": round(previsao, 2),
        "media_historica_domingo": round(media_dom, 2),
        "media_historica_sabado_30d": round(media_sab, 2),
        "observacao": (
            f"O sabado 22/03 foi {'ACIMA' if SABADO_22MAR['depositos_brl'] > media_sab else 'ABAIXO'} "
            f"da media de sabados ({SABADO_22MAR['depositos_brl']:,.2f} vs {media_sab:,.2f}). "
            f"Isso sugere um domingo {'mais forte' if SABADO_22MAR['depositos_brl'] > media_sab else 'mais fraco'} "
            f"que a media."
        ),
        "ftd_sabado_22mar": {
            "valor": SABADO_22MAR["ftds"],
            "media_ftd_domingo": round(np.mean(FTD["ftds"].values), 0),
            "interpretacao": (
                f"O sabado 22/03 teve {SABADO_22MAR['ftds']} FTDs — "
                f"{'muito acima' if SABADO_22MAR['ftds'] > np.mean(FTD['ftds'].values) * 1.5 else 'acima' if SABADO_22MAR['ftds'] > np.mean(FTD['ftds'].values) else 'abaixo'} "
                f"da media de FTDs em domingos ({np.mean(FTD['ftds'].values):.0f}). "
                "Novos jogadores do sabado podem depositar novamente no domingo."
            ),
        },
    }

    return resultado


# =============================================================================
# PREVISAO FTD DOMINGO
# =============================================================================

def prever_ftd():
    """
    Previsao de FTDs (primeiro deposito) para o domingo 23/03.
    FTD e indicador de aquisicao — novos jogadores entrando na plataforma.
    """
    ftds = FTD["ftds"].values
    regs = FTD["registros"].values
    conv = FTD["conversao_pct"].values

    # Media e IC
    media_ftd = np.mean(ftds)
    _, _, ic80_lo, ic80_hi = intervalo_confianca_t(ftds, 0.80)
    _, _, ic95_lo, ic95_hi = intervalo_confianca_t(ftds, 0.95)

    # Regressao
    reg_pred, slope, _, r2, p_val, se = regressao_linear_previsao(ftds)

    # Media ponderada
    mp = media_ponderada_exponencial(ftds, alpha=0.3)

    # Ensemble
    previsao = round(0.34 * media_ftd + 0.33 * mp + 0.33 * reg_pred)

    resultado = {
        "previsao_ftd": int(previsao),
        "media_simples": round(media_ftd, 0),
        "media_ponderada": round(mp, 0),
        "regressao": round(reg_pred, 0),
        "ic_80": {"lower": int(round(ic80_lo)), "upper": int(round(ic80_hi))},
        "ic_95": {"lower": int(round(ic95_lo)), "upper": int(round(ic95_hi))},
        "conversao_media_pct": round(np.mean(conv), 2),
        "registros_previstos": int(round(np.mean(regs))),
        "tendencia_slope": round(slope, 2),
        "tendencia_p_value": round(p_val, 4),
    }

    return resultado


# =============================================================================
# CONSOLIDACAO E OUTPUT
# =============================================================================

def consolidar_e_salvar():
    """Executa todas as analises, consolida e salva resultados."""

    log.info("=" * 70)
    log.info("  PREVISAO ESTATISTICA — DOMINGO 23/03/2026")
    log.info("  Professor Doutor em Estatistica Aplicada (iGaming)")
    log.info("=" * 70)

    resultados = {}

    # 1. Previsao depositos
    log.info("\n[1/7] Calculando previsao de depositos...")
    resultados["depositos"] = prever_depositos()

    # 2. Net deposit
    log.info("[2/7] Calculando previsao de net deposit...")
    resultados["net_deposit"] = prever_net_deposit()

    # 3. Analise de tendencia
    log.info("[3/7] Executando testes de tendencia...")
    resultados["tendencia"] = analisar_tendencia()

    # 4. Volatilidade GGR
    log.info("[4/7] Analisando volatilidade do GGR Casino...")
    resultados["ggr_volatilidade"] = analisar_volatilidade_ggr()

    # 5. Padrao horario
    log.info("[5/7] Identificando golden hours...")
    resultados["padrao_horario"] = analisar_padrao_horario()

    # 6. Cenarios
    log.info("[6/7] Gerando cenarios (base/otimista/pessimista)...")
    resultados["cenarios"] = gerar_cenarios()

    # 7. Comparativo domingo/sabado
    log.info("[7/7] Comparando domingo vs sabado...")
    resultados["domingo_vs_sabado"] = comparar_domingo_sabado()

    # Extra: FTD
    resultados["ftd"] = prever_ftd()

    # Metadata
    resultados["metadata"] = {
        "data_previsao": "2026-03-23",
        "data_geracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "modelo": "Ensemble (media simples + media ponderada exponencial + regressao linear)",
        "distribuicao_ic": "t-Student",
        "amostra_n": 7,
        "fonte_dados": "Athena (fund_ec2, bireports_ec2, ps_bi) extraidos em 22/03/2026",
        "autor": "PhD Estatistica Aplicada / Mateus F. (Squad Intelligence Engine)",
    }

    # ==========================================================================
    # SALVAR JSON
    # ==========================================================================
    json_path = os.path.join(OUTPUT_DIR, "previsao_domingo_23mar2026.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"\nJSON salvo: {json_path}")

    # ==========================================================================
    # PRINT RESUMO COMPLETO (para console e stakeholders)
    # ==========================================================================
    dep = resultados["depositos"]
    nd = resultados["net_deposit"]
    tend = resultados["tendencia"]
    ggr = resultados["ggr_volatilidade"]
    hor = resultados["padrao_horario"]
    cen = resultados["cenarios"]
    dsab = resultados["domingo_vs_sabado"]
    ftd_prev = resultados["ftd"]

    print("\n")
    print("=" * 70)
    print("  RELATORIO DE PREVISAO — DOMINGO 23/03/2026")
    print("  Gerado em:", resultados["metadata"]["data_geracao"])
    print("=" * 70)

    # --- DEPOSITOS ---
    print("\n" + "-" * 70)
    print("  1. PREVISAO DE DEPOSITOS")
    print("-" * 70)
    print(f"""
  PREVISAO FINAL: R$ {dep['previsao_final_brl']:,.2f}

  Intervalo de confianca 80%: R$ {dep['ic_80']['lower']:,.2f}  a  R$ {dep['ic_80']['upper']:,.2f}
  Intervalo de confianca 95%: R$ {dep['ic_95']['lower']:,.2f}  a  R$ {dep['ic_95']['upper']:,.2f}

  Detalhamento por metodo:
    Media simples (7 dom):      R$ {dep['metodo_media_simples']:,.2f}
    Media ponderada (recente):  R$ {dep['metodo_media_ponderada']:,.2f}
    Regressao linear:           R$ {dep['metodo_regressao_linear']:,.2f}

  Submetricas previstas:
    Quantidade de depositos:    ~{dep['submetricas']['qtd_depositos_prevista']:,}
    Depositantes unicos:        ~{dep['submetricas']['depositantes_unicos_previstos']:,}
    Ticket medio:               R$ {dep['submetricas']['ticket_medio_previsto']:.2f}

  Regressao: {dep['regressao']['interpretacao']}""")

    # --- NET DEPOSIT ---
    print("\n" + "-" * 70)
    print("  2. PREVISAO NET DEPOSIT (Depositos - Saques)")
    print("-" * 70)
    print(f"""
  Previsao Net Deposit:       R$ {nd['previsao_net_brl']:,.2f}
  IC 80%:                     R$ {nd['ic_80']['lower']:,.2f}  a  R$ {nd['ic_80']['upper']:,.2f}
  IC 95%:                     R$ {nd['ic_95']['lower']:,.2f}  a  R$ {nd['ic_95']['upper']:,.2f}

  Probabilidade de net POSITIVO: {nd['probabilidade_positivo_pct']:.1f}%

  Historico (7 domingos): {nd['domingos_positivos']} positivos, {nd['domingos_negativos']} negativos
  Ratio medio saques/depositos: {nd['ratio_medio_saques_depositos']:.2%}

  {nd['interpretacao']}

  Historico detalhado:""")
    for h in nd["historico"]:
        sinal = "+" if h["net_deposit"] > 0 else ""
        print(f"    {h['data']}: Dep R$ {h['depositos']:>12,.2f} | Saq R$ {h['saques']:>12,.2f} | Net {sinal}R$ {h['net_deposit']:>12,.2f} [{h['sinal']}]")

    # --- TENDENCIA ---
    print("\n" + "-" * 70)
    print("  3. ANALISE DE TENDENCIA")
    print("-" * 70)
    print(f"""
  Mann-Kendall:  tau={tend['mann_kendall']['tau']:.3f}, p={tend['mann_kendall']['p_value']:.4f} → {tend['mann_kendall']['conclusao']}
  Regressao:     slope=+R$ {tend['regressao_linear']['slope_brl_por_semana']:,.0f}/semana, p={tend['regressao_linear']['p_value']:.4f} → {tend['regressao_linear']['conclusao']}

  Ultimos 3 domingos consecutivamente crescentes: {'SIM' if tend['ultimos_3_domingos_crescentes'] else 'NAO'}
  Testes concordam: {'SIM' if tend['testes_concordam'] else 'NAO'}

  Submetricas:
    Qtd depositos:     tau={tend['submetricas']['qtd_depositos']['tau']:.3f}, p={tend['submetricas']['qtd_depositos']['p']:.4f} → {tend['submetricas']['qtd_depositos']['tendencia']}
    Unicos:            tau={tend['submetricas']['depositantes_unicos']['tau']:.3f}, p={tend['submetricas']['depositantes_unicos']['p']:.4f} → {tend['submetricas']['depositantes_unicos']['tendencia']}
    Ticket medio:      tau={tend['submetricas']['ticket_medio']['tau']:.3f}, p={tend['submetricas']['ticket_medio']['p']:.4f} → {tend['submetricas']['ticket_medio']['tendencia']}

  CONCLUSAO: {tend['conclusao_geral']}""")

    # --- GGR VOLATILIDADE ---
    print("\n" + "-" * 70)
    print("  4. VOLATILIDADE GGR CASINO")
    print("-" * 70)
    gs = ggr["estatisticas_descritivas"]
    print(f"""
  Coeficiente de variacao (CV): {ggr['cv_pct']:.1f}% — {ggr['classificacao_volatilidade']}

  Estatisticas descritivas:
    Media:    R$ {gs['media']:>12,.2f}
    Mediana:  R$ {gs['mediana']:>12,.2f}
    Std Dev:  R$ {gs['desvio_padrao']:>12,.2f}
    Minimo:   R$ {gs['minimo']:>12,.2f}
    Maximo:   R$ {gs['maximo']:>12,.2f}
    Range:    R$ {gs['range']:>12,.2f}

  Percentis:
    P10: R$ {ggr['percentis']['p10']:>12,.2f}  |  P90: R$ {ggr['percentis']['p90']:>12,.2f}
    P25: R$ {ggr['percentis']['p25']:>12,.2f}  |  P75: R$ {ggr['percentis']['p75']:>12,.2f}

  Skewness: {gs['skewness']:.3f} ({'assimetria positiva — cauda direita longa' if gs['skewness'] > 0 else 'assimetria negativa'})

  Melhor distribuicao: {ggr['melhor_distribuicao']['nome']} (KS p={ggr['melhor_distribuicao']['ks_p_value']:.4f})

  REGIMES IDENTIFICADOS:
    Fevereiro (4 dom): media R$ {ggr['regimes']['fase_fev']['media']:,.2f}
    Marco (3 dom):     media R$ {ggr['regimes']['fase_mar']['media']:,.2f}

  Range provavel para 23/03 (regime Marco):
    R$ {ggr['range_provavel_domingo']['regime_marco']['lower']:,.2f} a R$ {ggr['range_provavel_domingo']['regime_marco']['upper']:,.2f}
    (media: R$ {ggr['range_provavel_domingo']['regime_marco']['media']:,.2f})

  {ggr['interpretacao_negocio']}""")

    # --- PADRAO HORARIO ---
    print("\n" + "-" * 70)
    print("  5. PADRAO HORARIO — GOLDEN HOURS")
    print("-" * 70)
    print(f"""
  GOLDEN HOURS (melhor ROI para CRM):
    {', '.join([str(h) + 'h' for h in hor['golden_hours']])}

  HORAS MORTAS (evitar disparos CRM):
    {', '.join([str(h) + 'h' for h in hor['dead_hours']])}

  Pico: {hor['pico']['hora']}h (R$ {hor['pico']['valor_brl']:,.2f} em 7 dom, ticket R$ {hor['pico']['ticket']:.2f})
  Vale: {hor['vale']['hora']}h (R$ {hor['vale']['valor_brl']:,.2f} em 7 dom, ticket R$ {hor['vale']['ticket']:.2f})
  Ratio pico/vale: {hor['ratio_pico_vale']:.1f}x
  Concentracao 13-16h: {hor['concentracao_13_16h_pct']:.1f}% do total diario

  Top 5 horas por score ROI:""")
    for gh in hor["golden_hours_detail"]:
        print(f"    {gh['hora']:2d}h: R$ {gh['media_por_domingo']:>10,.2f}/dom | ticket R$ {gh['ticket_medio']:.2f} | {gh['pct_dia']:.1f}% do dia | score {gh['score_roi']:.3f}")

    print(f"""
  Janelas do dia:""")
    for nome, j in hor["janelas"].items():
        print(f"    {nome:25s}: R$ {j['total_brl']:>12,.2f} ({j['pct_dia']:5.1f}% do dia) | ticket R$ {j['ticket_medio']:.2f}")

    print(f"""
  RECOMENDACOES CRM:""")
    for rec in hor["recomendacao_crm"]:
        print(f"    [{rec['horario']}] {rec['acao']}")
        print(f"      → {rec['racional']}")

    # --- CENARIOS ---
    print("\n" + "-" * 70)
    print("  6. CENARIOS")
    print("-" * 70)
    sc = cen["cenarios"]
    print(f"""
  PESSIMISTA:  R$ {sc['pessimista']['depositos_brl']:>12,.2f}  ({sc['pessimista']['delta_vs_base_pct']:+.1f}% vs base)
  BASE:        R$ {sc['base']['depositos_brl']:>12,.2f}
  OTIMISTA:    R$ {sc['otimista']['depositos_brl']:>12,.2f}  ({sc['otimista']['delta_vs_base_pct']:+.1f}% vs base)
  VIA SABADO:  R$ {cen['previsao_via_sabado']['previsao_domingo_via_sabado']:>12,.2f}

  Premissas BASE: {'; '.join(sc['base']['premissas'])}
  Premissas OTIMISTA: {'; '.join(sc['otimista']['premissas'])}
  Premissas PESSIMISTA: {'; '.join(sc['pessimista']['premissas'])}""")

    # --- DOMINGO vs SABADO ---
    print("\n" + "-" * 70)
    print("  7. COMPARATIVO DOMINGO vs SABADO")
    print("-" * 70)
    print(f"""
  Sabado 22/03:
    Depositos: R$ {dsab['sabado_22mar']['depositos_brl']:,.2f}
    Transacoes: {dsab['sabado_22mar']['transacoes']:,}
    FTDs: {dsab['sabado_22mar']['ftds']:,}

  Ratio historico dom/sab: {dsab['ratio_dom_sab_valor']:.4f}
  Previsao domingo via ratio: R$ {dsab['previsao_domingo_via_ratio']:,.2f}

  {dsab['observacao']}
  {dsab['ftd_sabado_22mar']['interpretacao']}""")

    # --- FTD ---
    print("\n" + "-" * 70)
    print("  8. PREVISAO FTD (Primeiro Deposito)")
    print("-" * 70)
    print(f"""
  Previsao FTD: {ftd_prev['previsao_ftd']} novos depositantes
  IC 80%: {ftd_prev['ic_80']['lower']} a {ftd_prev['ic_80']['upper']}
  IC 95%: {ftd_prev['ic_95']['lower']} a {ftd_prev['ic_95']['upper']}

  Conversao media registro→FTD: {ftd_prev['conversao_media_pct']:.1f}%
  Registros previstos: ~{ftd_prev['registros_previstos']:,}""")

    # --- RESUMO EXECUTIVO ---
    print("\n" + "=" * 70)
    print("  RESUMO EXECUTIVO PARA O CTO")
    print("=" * 70)
    print(f"""
  DEPOSITOS DOMINGO 23/03:
    Previsao: R$ {dep['previsao_final_brl']:,.2f}
    (IC 80%: R$ {dep['ic_80']['lower']:,.2f} — R$ {dep['ic_80']['upper']:,.2f})
    (IC 95%: R$ {dep['ic_95']['lower']:,.2f} — R$ {dep['ic_95']['upper']:,.2f})

  NET DEPOSIT: R$ {nd['previsao_net_brl']:,.2f} (prob positivo: {nd['probabilidade_positivo_pct']:.0f}%)

  GGR CASINO ESPERADO (regime marco): R$ {ggr['range_provavel_domingo']['regime_marco']['lower']:,.0f} — R$ {ggr['range_provavel_domingo']['regime_marco']['upper']:,.0f}

  FTD: ~{ftd_prev['previsao_ftd']} novos depositantes

  TENDENCIA: {tend['conclusao_geral']}

  GOLDEN HOURS CRM: {', '.join([str(h) + 'h' for h in hor['golden_hours']])} (concentrar disparos aqui)

  CENARIOS:
    Pessimista: R$ {sc['pessimista']['depositos_brl']:>10,.0f}
    Base:       R$ {sc['base']['depositos_brl']:>10,.0f}
    Otimista:   R$ {sc['otimista']['depositos_brl']:>10,.0f}
    Via Sabado: R$ {cen['previsao_via_sabado']['previsao_domingo_via_sabado']:>10,.0f}

  ATENCAO:
    - GGR Casino e ALTAMENTE VOLATIL (CV={ggr['cv_pct']:.0f}%) — nao confiar em previsao pontual
    - Amostra pequena (n=7) — intervalos de confianca sao largos por precaucao
    - 23/03 esta proximo ao fim do mes — possivel efeito salario (nao quantificado)
    - Sabado 22/03 foi forte (R$ {dsab['sabado_22mar']['depositos_brl']:,.0f}) — sinal positivo para domingo

  ACAO RECOMENDADA:
    1. Disparar CRM principal as 12h (antes do pico natural 13-16h)
    2. Bonus anti-queda as 17h para segurar jogadores
    3. NAO disparar entre 3h-6h (custo alto, conversao baixa)
    4. Monitorar depositos em tempo real — se abaixo de R$ 500K ate 14h, considerar push extra
""")

    print("=" * 70)
    print(f"  Resultados completos salvos em: {json_path}")
    print("=" * 70)

    return resultados


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    consolidar_e_salvar()
