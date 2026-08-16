-- ==============================================================================
-- 01. CRIAÇÃO DE SCHEMAS E ESTRUTURAS DDL - UNITY CATALOG (DATABRICKS)
-- ==============================================================================

USE CATALOG workspace;

-- Criação dos Schemas da Arquitetura Medalhão
CREATE SCHEMA IF NOT EXISTS workspace.bronze_economia
COMMENT 'Armazena dados brutos de economia e commodities com carimbo de coleta';

CREATE SCHEMA IF NOT EXISTS workspace.silver_economia
COMMENT 'Armazena dados tratados, limpos e integrados por competência temporal';

CREATE SCHEMA IF NOT EXISTS workspace.gold_economia
COMMENT 'Armazena tabelas e métricas analíticas prontas para BI e consumo';

CREATE SCHEMA IF NOT EXISTS workspace.bronze_api
COMMENT 'Armazena dados brutos de séries temporais financeiras (Alpha Vantage)';

CREATE SCHEMA IF NOT EXISTS workspace.analytics_api
COMMENT 'Views e tabelas de consumo de mercado financeiro';

-- ==============================================================================
-- DDL DAS TABELAS EM FORMATO DELTA
-- ==============================================================================

-- Tabela Bronze: IPCA
CREATE TABLE IF NOT EXISTS workspace.bronze_economia.ipca (
    data STRING,
    ipca DOUBLE,
    data_coleta TIMESTAMP
) USING DELTA;

-- Tabela Bronze: Boi Gordo
CREATE TABLE IF NOT EXISTS workspace.bronze_economia.boi_gordo (
    data STRING,
    boi_gordo DOUBLE,
    data_coleta TIMESTAMP
) USING DELTA;

-- Tabela Silver: Economia Unificada
CREATE TABLE IF NOT EXISTS workspace.silver_economia.economia (
    data DATE,
    ipca DOUBLE,
    boi_gordo DOUBLE,
    data_coleta TIMESTAMP
) USING DELTA;

-- Tabela Gold: Insights Macroeconômicos
CREATE TABLE IF NOT EXISTS workspace.gold_economia.insights (
    data DATE,
    ipca DOUBLE,
    boi_gordo DOUBLE,
    data_coleta TIMESTAMP,
    variacao_ipca DOUBLE,
    variacao_boi DOUBLE
) USING DELTA;
