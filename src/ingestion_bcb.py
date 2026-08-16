"""
Módulo de Ingestão de Dados Macroeconômicos e Agro (Camada Bronze)
- Consumo da API do Banco Central do Brasil (SGS 433 - IPCA)
- Ingestão de Cotações Históricas de Commodities (Boi Gordo)
- Persistência em Delta Lake (Bronze) com Metadados de Auditoria
"""
import requests
import pandas as pd
from datetime import datetime

from src.config import (
    BCB_IPCA_URL,
    EXECUTION_ENV,
    TABLE_IPCA_BRONZE,
    TABLE_BOI_BRONZE,
    get_table_path_or_name,
    get_spark_session,
)


def fetch_ipca_from_bcb(
    data_inicial: str = "01/01/2024", data_final: str = "31/12/2024"
) -> pd.DataFrame:
    """
    Consome a API REST do Banco Central do Brasil para a série histórica do IPCA.
    """
    params = {
        "formato": "json",
        "dataInicial": data_inicial,
        "dataFinal": data_final,
    }
    print(f"📡 [BCB API] Solicitando dados de {data_inicial} até {data_final}...")
    response = requests.get(BCB_IPCA_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    if not data:
        raise ValueError("A API do Banco Central retornou uma lista vazia.")

    df_pd = pd.DataFrame(data)
    df_pd.columns = ["data", "ipca"]
    # Limpeza básica em Pandas antes de converter para Spark
    df_pd["ipca"] = df_pd["ipca"].str.replace(",", ".").astype(float)
    print(f"✅ [BCB API] {len(df_pd)} registros de IPCA obtidos com sucesso.")
    return df_pd


def generate_sample_boi_gordo_data() -> pd.DataFrame:
    """
    Gera dados históricos de cotação da arroba do Boi Gordo para 2024
    (utilizado no ambiente local para simular a tabela boi_gordo_aula).
    """
    data = [
        {"Data": "01/2024", "Valor": 248.50},
        {"Data": "02/2024", "Valor": 242.10},
        {"Data": "03/2024", "Valor": 235.80},
        {"Data": "04/2024", "Valor": 230.20},
        {"Data": "05/2024", "Valor": 225.40},
        {"Data": "06/2024", "Valor": 228.90},
        {"Data": "07/2024", "Valor": 234.10},
        {"Data": "08/2024", "Valor": 241.60},
        {"Data": "09/2024", "Valor": 258.30},
        {"Data": "10/2024", "Valor": 304.50},
        {"Data": "11/2024", "Valor": 335.20},
        {"Data": "12/2024", "Valor": 322.80},
    ]
    return pd.DataFrame(data)


def ingest_ipca_bronze(spark) -> str:
    """
    Executa a extração do IPCA e grava na camada Bronze em formato Delta Lake.
    """
    from pyspark.sql.functions import current_timestamp

    df_pd = fetch_ipca_from_bcb()
    df_spark = spark.createDataFrame(df_pd).withColumn(
        "data_coleta", current_timestamp()
    )

    target = get_table_path_or_name("bronze", TABLE_IPCA_BRONZE, spark)

    if EXECUTION_ENV == "DATABRICKS":
        df_spark.write.format("delta").mode("overwrite").saveAsTable(target)
    else:
        df_spark.write.format("delta").mode("overwrite").save(target)

    print(f"📦 [BRONZE] Tabela IPCA salva em formato Delta: {target}")
    return target


def ingest_boi_gordo_bronze(spark) -> str:
    """
    Ingere dados de Boi Gordo e salva na camada Bronze em formato Delta Lake.
    """
    from pyspark.sql.functions import current_timestamp
    if EXECUTION_ENV == "DATABRICKS":
        try:
            df_spark = spark.table("workspace.bronze_economia.boi_gordo_aula")
        except Exception:
            df_pd = generate_sample_boi_gordo_data()
            df_spark = spark.createDataFrame(df_pd)
    else:
        df_pd = generate_sample_boi_gordo_data()
        df_spark = spark.createDataFrame(df_pd)

    df_spark = df_spark.withColumn("data_coleta", current_timestamp())
    target = get_table_path_or_name("bronze", TABLE_BOI_BRONZE, spark)

    if EXECUTION_ENV == "DATABRICKS":
        df_spark.write.format("delta").mode("overwrite").saveAsTable(target)
    else:
        df_spark.write.format("delta").mode("overwrite").save(target)

    print(f"📦 [BRONZE] Tabela Boi Gordo salva em formato Delta: {target}")
    return target


if __name__ == "__main__":
    spark_session = get_spark_session("IngestionBronzeTest")
    ingest_ipca_bronze(spark_session)
    ingest_boi_gordo_bronze(spark_session)
