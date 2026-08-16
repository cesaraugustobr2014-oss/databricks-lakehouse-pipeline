"""
Módulo de Transformação e Qualidade de Dados (Camada Silver)
- Limpeza e padronização temporal para competência mensal (yyyy-MM-01)
- Cast estrito de tipos numéricos (DoubleType)
- Join temporal entre indicadores econômicos (IPCA x Boi Gordo)
- Persistência em Delta Lake Silver com schema evolution controlado
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, date_format, regexp_replace

from src.config import (
    EXECUTION_ENV,
    TABLE_IPCA_BRONZE,
    TABLE_BOI_BRONZE,
    TABLE_ECONOMIA_SILVER,
    get_table_path_or_name,
    get_spark_session,
)


def load_bronze_table(spark: SparkSession, table_name: str):
    """
    Carrega tabela da camada Bronze respeitando o ambiente (Databricks vs Local).
    """
    path_or_name = get_table_path_or_name("bronze", table_name, spark)
    if EXECUTION_ENV == "DATABRICKS":
        return spark.table(path_or_name)
    else:
        return spark.read.format("delta").load(path_or_name)


def transform_silver_economia(spark: SparkSession) -> str:
    """
    Executa a transformação e enriquecimento dos dados das tabelas Bronze (IPCA e Boi Gordo),
    gerando a tabela unificada na camada Silver.
    """
    print("🔄 [SILVER] Carregando dados da camada Bronze...")
    df_ipca = load_bronze_table(spark, TABLE_IPCA_BRONZE)
    df_boi = load_bronze_table(spark, TABLE_BOI_BRONZE)

    # 1. Renomeação e padronização da tabela Boi Gordo
    # Se colunas estiverem em maiúsculas (Data, Valor), padroniza para minúsculas
    if "Data" in df_boi.columns:
        df_boi = df_boi.withColumnRenamed("Data", "data")
    if "Valor" in df_boi.columns:
        df_boi = df_boi.withColumnRenamed("Valor", "boi_gordo")

    # 2. Padronização de datas para competência mensal (yyyy-MM)
    # Boi Gordo vem como 'MM/yyyy' -> converte para string 'yyyy-MM'
    df_boi_clean = df_boi.withColumn("data", to_date(col("data"), "MM/yyyy"))
    df_boi_clean = df_boi_clean.withColumn("data", date_format(col("data"), "yyyy-MM"))

    # IPCA vem como 'dd/MM/yyyy' -> converte para string 'yyyy-MM'
    df_ipca_clean = df_ipca.withColumn("data", to_date(col("data"), "dd/MM/yyyy"))
    df_ipca_clean = df_ipca_clean.withColumn("data", date_format(col("data"), "yyyy-MM"))

    # 3. Inner Join por competência mensal
    ip = df_ipca_clean.alias("ip")
    bo = df_boi_clean.alias("bo")

    df_join = ip.join(bo, col("ip.data") == col("bo.data"), "inner").select(
        col("ip.data").alias("data"),
        col("ip.ipca").alias("ipca"),
        col("bo.boi_gordo").alias("boi_gordo"),
        col("ip.data_coleta").alias("data_coleta"),
    )

    # 4. Tipagem estrita: 'yyyy-MM' vira DateType (yyyy-MM-01) e números viram DoubleType
    df_silver = (
        df_join.withColumn("data", to_date(col("data"), "yyyy-MM"))
        .withColumn("ipca", regexp_replace(col("ipca").cast("string"), ",", ".").cast("double"))
        .withColumn(
            "boi_gordo", regexp_replace(col("boi_gordo").cast("string"), ",", ".").cast("double")
        )
    )

    target = get_table_path_or_name("silver", TABLE_ECONOMIA_SILVER, spark)

    # 5. Escrita na camada Silver (Delta Lake)
    if EXECUTION_ENV == "DATABRICKS":
        df_silver.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target)
    else:
        df_silver.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(target)

    print(f"📦 [SILVER] Tabela Silver unificada e gravada em Delta Lake: {target}")
    return target


if __name__ == "__main__":
    spark_session = get_spark_session("SilverTransformationsTest")
    transform_silver_economia(spark_session)
