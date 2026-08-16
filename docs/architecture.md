# 🏛️ Arquitetura do Data Lakehouse & Decisões Técnicas

Este documento detalha os princípios de design, decisões de engenharia e mecanismos de computação distribuída empregados no projeto.

---

## 1. Visão Geral da Arquitetura

O projeto implementa o paradigma **Modern Data Lakehouse** sobre o ecossistema **Databricks**, **Apache Spark** e **Delta Lake**, gerenciado pelo **Unity Catalog**.

```
                   ┌────────────────────────────────────────────────────────┐
                   │                     DATA SOURCES                       │
                   │   • API Banco Central (IPCA)                           │
                   │   • API Alpha Vantage (B3 Stock Tickers)               │
                   │   • Cotações de Commodities (Boi Gordo)                │
                   └───────────────────────────┬────────────────────────────┘
                                               │ Ingestão REST / HTTP (Batch)
                                               ▼
  ┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                    DATABRICKS LAKEHOUSE                                         │
  │                                                                                                 │
  │  ┌──────────────────────┐      ┌──────────────────────┐      ┌───────────────────────────────┐  │
  │  │    CAMADA BRONZE     │      │    CAMADA SILVER     │      │          CAMADA GOLD          │  │
  │  │     (Raw / Ingest)   │ ───► │   (Clean & Joined)   │ ───► │      (Business & Insights)    │  │
  │  │                      │      │                      │      │                               │  │
  │  │ • bronze_economia    │      │ • silver_economia    │      │ • gold_economia.insights      │  │
  │  │   - ipca             │      │   - economia         │      │ • gold_economia.gold_analitico│  │
  │  │   - boi_gordo        │      │   (Join temporal por │      │ • vw_gold_dashboard           │  │
  │  │ • bronze_api         │      │    competência YYYY-MM)     │ • vw_cotacoes_resumo          │  │
  │  │   - cotacoes_alpha   │      │                      │      │                               │  │
  │  └──────────────────────┘      └──────────────────────┘      └───────────────┬───────────────┘  │
  │                                                                              │                  │
  └──────────────────────────────────────────────────────────────────────────────┼──────────────────┘
                                                                                 │
                                                                                 ▼
                                                                 ┌───────────────────────────────┐
                                                                 │     CONSUMO & ANALYTICS       │
                                                                 │   • Databricks SQL Dashboard  │
                                                                 │   • Power BI / Tableau / Metabase │
                                                                 │   • Modelos de Machine Learning│
                                                                 └───────────────────────────────┘
```

---

## 2. Por que Arquitetura Medalhão?

A segregação em camadas atende a objetivos fundamentais de governança, escalabilidade e qualidade de dados:

### 🥉 Camada Bronze (Raw Ingestion)
- **Objetivo:** Armazenar os dados brutos exatamente como vieram da fonte de dados de origem.
- **Princípio:** Nenhuma regra de negócio deve ser aplicada nesta camada.
- **Rastreabilidade:** Adição de metadados de auditoria como `data_coleta` ou `data_ingestao` para garantir auditoria completa e permitir reprocessamentos históricos (*Replay/Time Travel*).

### 🥈 Camada Silver (Enriched & Conformed)
- **Objetivo:** Garantir a qualidade, integridade referencial e conformidade estrutural dos dados.
- **Transformações:**
  - Padronização de datas díspares (`MM/yyyy` e `dd/MM/yyyy`) para o padrão canônico ISO `yyyy-MM-01`.
  - Tratamento de casas decimais e casting seguro para `DoubleType`.
  - Junção temporal (*Inner Join*) entre séries macroeconômicas e commodities para criar uma visão única consolidada por competência.

### 🥇 Camada Gold (Business & Aggregated)
- **Objetivo:** Entregar tabelas e visualizações otimizadas para consumo de negócio, com agregações e regras analíticas prontas.
- **Implementações:**
  - Aplicação de **Window Functions** (`lag()`) no Apache Spark para calcular variações percentuais mensais do IPCA e do Boi Gordo.
  - Categorização dinâmica de cenários (*"Preço do boi cresce mais"*, *"Inflação cresce mais"*, níveis de divergência).
  - Criação de **Views** para desacoplar a camada de armazenamento físico da camada de consumo analítico.

---

## 3. Mecanismos de Otimização no Apache Spark & Delta Lake

1. **Delta Transaction Log (`_delta_log/`):** Garante garantias ACID em operações distribuídas, prevenindo leitura de dados inconsistentes durante gravações e viabilizando o *Time Travel*.
2. **Compactação de Arquivos (`OPTIMIZE`):** Mitiga o *Small File Problem* comum em ingestões frequentes de APIs, agregando arquivos pequenos em blocos otimizados de ~1GB para leitura rápida.
3. **Z-ORDER Clustering (`OPTIMIZE ... ZORDER BY (ticker, data)`):** Organiza os dados em curvas de Hilbert multidimensionais, permitindo que o Spark pule blocos inteiros de arquivos (*Data Skipping*) durante filtros por data e ticker.
4. **Broadcast Hash Join (`/*+ BROADCAST(tabela_menor) */`):** Evita operações de *Shuffle* (troca massiva de dados pela rede entre executores) ao enviar uma cópia integral da tabela dimensão para a memória de cada executor.
5. **Cost-Based Optimizer (`ANALYZE TABLE ... COMPUTE STATISTICS`):** Coleta estatísticas de histograma e cardinalidade para que o Catalyst Optimizer do Spark selecione os melhores planos de execução de queries automaticamente.
