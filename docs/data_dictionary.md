# 📖 Dicionário de Dados do Lakehouse

Este documento descreve todas as entidades, schemas, tabelas e campos presentes nas camadas **Bronze**, **Silver** e **Gold**.

---

## 1. Camada Bronze (Raw Ingestion)

### Tabela: `bronze_economia.ipca`
- **Descrição:** Série histórica bruta da taxa de inflação oficial do Brasil (IPCA) obtida via API do Banco Central.
- **Formato de Armazenamento:** Delta Lake

| Coluna | Tipo de Dado | Nullable | Descrição / Exemplo |
| :--- | :--- | :--- | :--- |
| `data` | `STRING` | Não | Data da competência no formato original da API (`dd/MM/yyyy`). Ex: `'01/01/2024'` |
| `ipca` | `DOUBLE` | Não | Variação percentual mensal do IPCA. Ex: `0.42` |
| `data_coleta` | `TIMESTAMP` | Não | Carimbo de data/hora do momento em que a ingestão ocorreu |

---

### Tabela: `bronze_economia.boi_gordo`
- **Descrição:** Cotações históricas da arroba do Boi Gordo (indicador agropecuário).
- **Formato de Armazenamento:** Delta Lake

| Coluna | Tipo de Dado | Nullable | Descrição / Exemplo |
| :--- | :--- | :--- | :--- |
| `data` / `Data` | `STRING` | Não | Mês/Ano de referência da cotação (`MM/yyyy`). Ex: `'01/2024'` |
| `boi_gordo` / `Valor`| `DOUBLE` | Não | Valor nominal médio da arroba em Reais (R$). Ex: `248.50` |
| `data_coleta` | `TIMESTAMP` | Não | Carimbo de data/hora do momento em que a ingestão ocorreu |

---

### Tabela: `bronze_api.cotacoes_alpha`
- **Descrição:** Séries temporais de cotações diárias de ativos negociados na B3 obtidas via Alpha Vantage.
- **Formato de Armazenamento:** Delta Lake

| Coluna | Tipo de Dado | Nullable | Descrição / Exemplo |
| :--- | :--- | :--- | :--- |
| `data` | `DATE` | Não | Data do pregão negociado (`yyyy-MM-dd`). Ex: `'2024-05-10'` |
| `abertura` | `DOUBLE` | Não | Preço de abertura da ação no pregão (R$) |
| `alta` | `DOUBLE` | Não | Preço máximo atingido no pregão (R$) |
| `baixa` | `DOUBLE` | Não | Preço mínimo atingido no pregão (R$) |
| `fechamento` | `DOUBLE` | Não | Preço de fechamento da ação (R$) |
| `volume` | `DOUBLE` | Não | Volume total financeiro/quantidade de papéis negociados |
| `ticker` | `STRING` | Não | Código de negociação do ativo na B3. Ex: `'PETR4.SA'` |
| `data_ingestao` | `TIMESTAMP` | Não | Timestamp UTC da extração da API |

---

## 2. Camada Silver (Enriched & Cleaned)

### Tabela: `silver_economia.economia`
- **Descrição:** Tabela normalizada e integrada contendo os indicadores de inflação e cotação do boi gordo alinhados pela mesma competência mensal.
- **Formato de Armazenamento:** Delta Lake

| Coluna | Tipo de Dado | Nullable | Descrição / Exemplo |
| :--- | :--- | :--- | :--- |
| `data` | `DATE` | Não | Primeiro dia do mês da competência (`yyyy-MM-01`). Ex: `'2024-01-01'` |
| `ipca` | `DOUBLE` | Não | Taxa de inflação mensal (%) |
| `boi_gordo` | `DOUBLE` | Não | Preço da arroba do boi gordo (R$) |
| `data_coleta` | `TIMESTAMP` | Não | Data/hora de linhagem herdada da extração |

---

## 3. Camada Gold (Business & Analytics)

### Tabela: `gold_economia.insights`
- **Descrição:** Tabela analítica com cálculo de variações relativas mês a mês via Window Functions.
- **Formato de Armazenamento:** Delta Lake

| Coluna | Tipo de Dado | Nullable | Descrição / Exemplo |
| :--- | :--- | :--- | :--- |
| `data` | `DATE` | Não | Data da competência (`yyyy-MM-01`) |
| `ipca` | `DOUBLE` | Não | Valor nominal do IPCA no mês |
| `boi_gordo` | `DOUBLE` | Não | Valor nominal da arroba do boi gordo no mês |
| `variacao_ipca` | `DOUBLE` | Sim | Variação percentual do IPCA em relação ao mês anterior (%) |
| `variacao_boi` | `DOUBLE` | Sim | Variação percentual da arroba em relação ao mês anterior (%) |

---

### Tabela / View: `gold_economia.gold_analitico` / `vw_gold_dashboard`
- **Descrição:** Entidade de consumo analítico enriquecida com regras de negócio e categorizações de impacto.

| Coluna | Tipo de Dado | Descrição / Regra de Negócio |
| :--- | :--- | :--- |
| `data` | `DATE` | Data da competência |
| `ipca` | `DOUBLE` | Valor do IPCA |
| `boi_gordo` | `DOUBLE` | Valor da arroba do Boi Gordo |
| `variacao_ipca` | `DOUBLE` | Variação percentual mês a mês do IPCA |
| `variacao_boi` | `DOUBLE` | Variação percentual mês a mês do Boi Gordo |
| `media_variacoes` | `DOUBLE` | Média aritmética entre a variação do IPCA e a do Boi |
| `destaque` | `STRING` | Classificação condicional: `'Preço do boi cresce mais'`, `'Inflação cresce mais'` ou `'Empate'` |
| `classe_impacto` | `STRING` | Nível de divergência absoluta: `'Alta divergência'` (> 5%), `'Média divergência'` (2% a 5%), `'Baixa divergência'` (< 2%) |
