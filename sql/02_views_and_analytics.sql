-- ==============================================================================
-- 02. VIEWS ANALÍTICAS E MODELAGEM DE CONSUMO PARA BI / DASHBOARDS
-- ==============================================================================

USE CATALOG workspace;

-- View de Negócio 1: Dashboard de Variações Econômicas e Divergência de Mercado
CREATE OR REPLACE VIEW workspace.gold_economia.vw_gold_dashboard AS
SELECT
    data,
    ipca,
    boi_gordo,
    variacao_ipca,
    variacao_boi,
    ROUND((variacao_ipca + variacao_boi) / 2, 2) AS media_variacoes,
    CASE
        WHEN variacao_boi > variacao_ipca THEN 'Preço do boi cresce mais'
        WHEN variacao_ipca > variacao_boi THEN 'Inflação cresce mais'
        ELSE 'Empate'
    END AS destaque,
    CASE
        WHEN ABS(variacao_boi - variacao_ipca) > 5 THEN 'Alta divergência'
        WHEN ABS(variacao_boi - variacao_ipca) BETWEEN 2 AND 5 THEN 'Média divergência'
        ELSE 'Baixa divergência'
    END AS classe_impacto
FROM workspace.gold_economia.insights;

-- View de Negócio 2: Resumo Executivo de Cotações da B3 (Alpha Vantage)
CREATE OR REPLACE VIEW workspace.analytics_api.vw_cotacoes_resumo AS
SELECT
    ticker,
    MAX(data)           AS ultima_data,
    MAX(alta)           AS maior_alta_no_periodo,
    MIN(baixa)          AS menor_baixa_no_periodo,
    ROUND(AVG(fechamento), 2) AS preco_medio_fechamento,
    SUM(volume)         AS volume_total_negociado,
    MAX(data_ingestao)  AS ultima_ingestao
FROM workspace.bronze_api.cotacoes_alpha
GROUP BY ticker;
