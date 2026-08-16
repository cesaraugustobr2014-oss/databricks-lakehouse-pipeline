# 📋 Architecture Decision Records (ADR) & Technical Decisions

Este documento registra as decisões técnicas, princípios de engenharia e justificativas arquiteturais adotadas no desenvolvimento da plataforma Lakehouse.

---

## 📑 Índice de Decisões
- [ADR-001: Adoção do Paradigma Data Lakehouse e Formato Delta Lake](#adr-001-adoção-do-paradigma-data-lakehouse-e-formato-delta-lake)
- [ADR-002: Estruturação em Arquitetura Medalhão (Bronze, Silver, Gold)](#adr-002-estruturação-em-arquitetura-medalhão-bronze-silver-gold)
- [ADR-003: Mitigação do Small File Problem via Delta OPTIMIZE](#adr-003-mitigação-do-small-file-problem-via-delta-optimize)
- [ADR-004: Co-localização Multidimensional com Z-ORDER Clustering](#adr-004-co-localização-multidimensional-com-z-order-clustering)
- [ADR-005: Utilização de Window Functions em Substituição a Self-Joins](#adr-005-utilização-de-window-functions-em-substituição-a-self-joins)
- [ADR-006: Otimização de Junções Distribuídas via Broadcast Hash Join](#adr-006-otimização-de-junções-distribuídas-via-broadcast-hash-join)
- [ADR-007: Desacoplamento da Camada de Consumo Analítico via Views](#adr-007-desacoplamento-da-camada-de-consumo-analítico-via-views)

---

## ADR-001: Adoção do Paradigma Data Lakehouse e Formato Delta Lake

### Status: `ACEITO / IMPLEMENTADO`

### Contexto & Problema
A ingestão contínua de dados originados de múltiplas APIs externas (Banco Central do Brasil e Alpha Vantage) exige uma camada de armazenamento que ofereça baixo custo para dados históricos em lote, suporte a schemas dinâmicos e garantias transacionais rigorosas contra escritas concorrentes e leituras parciais.

### Decisão Técnica
Optou-se pela adoção do **Delta Lake** como camada de armazenamento primária sobre o Apache Spark em vez de formatos Parquet convencionais ou bancos relacionais monolíticos.

### Racional & Consequências
- **Garantias ACID:** O *Transaction Log* (`_delta_log/`) garante atomicidade e consistência durante inserções concorrentes.
- **Time Travel & Auditoria:** Viabiliza auditoria e reprocessamento histórico (*Point-in-Time Queries*) sem custos de infraestrutura adicionais.
- **Schema Enforcement & Evolution:** Impede a poluição de tabelas por payloads corrompidos ou tipos incompatíveis.

---

## ADR-002: Estruturação em Arquitetura Medalhão (Bronze, Silver, Gold)

### Status: `ACEITO / IMPLEMENTADO`

### Contexto & Problema
A mistura de regras de limpeza, enriquecimento e métricas de negócio em um único pipeline monolítico prejudica a rastreabilidade, aumenta o custo de reprocessamento e compromete a governança de dados.

### Decisão Técnica
Segregar o ciclo de vida dos dados em três camadas lógicas bem delimitadas:
1. **Bronze:** Raw data imutável com carimbo de coleta (`data_coleta`, `data_ingestao`).
2. **Silver:** Dados conformados, tipados estritamente (`DoubleType`, datas em `yyyy-MM-01`) e unificados por junção temporal.
3. **Gold:** Tabelas analíticas agregadas com métricas calculadas e regras de negócio prontas para BI.

### Racional & Consequências
- **Idempotência:** A camada Bronze preserva o estado puro de origem, permitindo reconstruir as camadas Silver e Gold a qualquer momento em caso de alteração de regras de negócio.
- **Clareza de Responsabilidade:** Engenheiros de dados operam nas camadas Bronze/Silver, enquanto analistas de BI e cientistas de dados consomem da camada Gold.

---

## ADR-003: Mitigação do Small File Problem via Delta OPTIMIZE

### Status: `ACEITO / IMPLEMENTADO`

### Contexto & Problema
Ingestões frequentes de APIs em lote tendem a gerar milhares de arquivos de poucos kilobytes, causando degradação de performance nas leituras devido ao overhead de conexões I/O e metadados no Spark.

### Decisão Técnica
Implementar rotinas de compactação via comando `OPTIMIZE` e habilitar propriedades de escrita auto-otimizada:
```sql
OPTIMIZE workspace.silver_economia.economia;
ALTER TABLE workspace.bronze_api.cotacoes_alpha SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
```

### Racional & Consequências
- Consolidação de múltiplos fragmentos em arquivos de ~1GB via *bin-packing*.
- Redução de latência em varreduras de tabelas para consultas analíticas.

---

## ADR-004: Co-localização Multidimensional com Z-ORDER Clustering

### Status: `ACEITO / IMPLEMENTADO`

### Contexto & Problema
Consultas analíticas sobre séries temporais financeiras frequentemente aplicam filtros combinados por ativo (`ticker`) e intervalo de datas (`data`). O particionamento em diretórios por ambas as colunas causaria particionamento excessivo (*over-partitioning*).

### Decisão Técnica
Aplicar a técnica de **Z-ORDER Clustering** nas colunas de alta cardinalidade de consulta:
```sql
OPTIMIZE workspace.bronze_api.cotacoes_alpha ZORDER BY (ticker, data);
```

### Racional & Consequências
- Co-localiza dados em curvas de Hilbert multidimensionais dentro dos arquivos Delta.
- Habilita o recurso de *Data Skipping*, permitindo que o Spark pule blocos inteiros de arquivos irrelevantes durante o scan.

---

## ADR-005: Utilização de Window Functions em Substituição a Self-Joins

### Status: `ACEITO / IMPLEMENTADO`

### Contexto & Problema
O cálculo de taxas de variação relativa mês a mês ($\Delta\%$) tradicionalmente envolvia realizar um *Self-Join* da tabela com ela mesma deslocada no tempo, gerando operações custosas de *Shuffle* em larga escala.

### Decisão Técnica
Utilizar **Window Functions** nativas do PySpark (`Window.orderBy("data")` com `F.lag()`) para recuperar o valor da competência anterior em um único estágio de execução:
```python
window_spec = Window.orderBy("data")
df.withColumn("ipca_ant", F.lag("ipca").over(window_spec)) \
  .withColumn("variacao_ipca", (F.col("ipca") - F.col("ipca_ant")) / F.col("ipca_ant") * 100)
```

### Racional & Consequências
- Eliminação de operações de Shuffle adicionais na rede.
- Redução de tempo de processamento e menor consumo de memória por executor.

---

## ADR-006: Otimização de Junções Distribuídas via Broadcast Hash Join

### Status: `ACEITO / IMPLEMENTADO`

### Contexto & Problema
A junção padrão no Spark (*Sort-Merge Join*) exige a redistribuição e re-ordenação de todos os registros entre os nós da rede (*Shuffle*), o que se torna ineficiente ao cruzar tabelas de fatos com pequenas tabelas dimensionais.

### Decisão Técnica
Forçar o uso de **Broadcast Hash Join** através de SQL Hints (`/*+ BROADCAST(tabela_menor) */`) em junções onde uma das tabelas cabe confortavelmente na memória dos executores (< 10MB default).

### Racional & Consequências
- Elimina o *Shuffle Exchange*, reduzindo significativamente o tráfego de rede e tempo de resposta.

---

## ADR-007: Desacoplamento da Camada de Consumo Analítico via Views

### Status: `ACEITO / IMPLEMENTADO`

### Contexto & Problema
Ferramentas de visualização (Power BI, Metabase, Databricks SQL Dashboards) não devem depender diretamente das estruturas físicas de armazenamento para evitar retrabalho em caso de refatorações internas.

### Decisão Técnica
Disponibilizar o acesso aos dados para usuários finais e analistas exclusivamente através de **Views Analíticas** (`vw_gold_dashboard`, `vw_cotacoes_resumo`).

### Racional & Consequências
- Desacoplamento total entre a modelagem física do storage e a interface de consumo de negócio.
- Aplicação de regras de apresentação e formatação sem redundância de dados físicos.
