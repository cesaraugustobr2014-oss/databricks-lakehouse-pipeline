-- ==============================================================================
-- 03. OTIMIZAÇÕES, TUNING DE PERFORMANCE E GOVERNANÇA NO DELTA LAKE / SPARK
-- ==============================================================================

USE CATALOG workspace;

-- ==============================================================================
-- 1. COMPACTAÇÃO DE ARQUIVOS PEQUENOS (Bin-Packing)
-- Reduz o "Small File Problem" consolidando múltiplos arquivos pequenos em arquivos de ~1GB
-- ==============================================================================
OPTIMIZE workspace.silver_economia.economia;

-- ==============================================================================
-- 2. CO-LOCALIZAÇÃO MULTIDIMENSIONAL (Z-ORDER)
-- Melhora a poda de dados (data skipping) em consultas filtradas por múltiplas colunas
-- ==============================================================================
OPTIMIZE workspace.bronze_api.cotacoes_alpha
ZORDER BY (ticker, data);

-- ==============================================================================
-- 3. OTIMIZAÇÕES AUTOMÁTICAS (Delta Auto Optimize)
-- Ativa compactação automática durante a escrita e otimização de commits
-- ==============================================================================
ALTER TABLE workspace.bronze_api.cotacoes_alpha SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- ==============================================================================
-- 4. OTIMIZAÇÃO DE JOINS DISTRIBUÍDOS: BROADCAST HASH JOIN
-- Envia a tabela menor (dimensão) para todos os nós executores, eliminando o Shuffle
-- ==============================================================================
-- Exemplo de consulta otimizada com Hint de Broadcast:
-- SELECT /*+ BROADCAST(p) */
--     v.venda_id,
--     v.data_venda,
--     p.nome_produto,
--     p.categoria,
--     v.valor_total
-- FROM vendas v
-- JOIN produtos p ON v.produto_id = p.produto_id;

-- ==============================================================================
-- 5. COLETA DE ESTATÍSTICAS PARA O CBO (Cost-Based Optimizer)
-- ==============================================================================
ANALYZE TABLE workspace.silver_economia.economia COMPUTE STATISTICS;
ANALYZE TABLE workspace.bronze_api.cotacoes_alpha COMPUTE STATISTICS FOR COLUMNS ticker, data;

-- Visualização de metadados estendidos e histórico da tabela (Time Travel)
DESCRIBE EXTENDED workspace.silver_economia.economia;
DESCRIBE HISTORY workspace.silver_economia.economia;
