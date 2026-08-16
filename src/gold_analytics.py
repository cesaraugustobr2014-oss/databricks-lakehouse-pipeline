"""
Módulo de Inteligência Analítica e Métricas de Negócio (Camada Gold)
- Cálculo de variações percentuais relativas usando Window Functions (lag)
- Classificação de cenários econômicos (Boi x Inflação) e divergência de mercado
- Criação de tabelas analíticas para dashboards e ferramentas de BI
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.config import (
    EXECUTION_ENV,
    TABLE_ECONOMIA_SILVER,
    TABLE_INSIGHTS_GOLD,
    TABLE_ANALITICO_GOLD,
    get_table_path_or_name,
    get_spark_session,
)


def load_silver_table(spark: SparkSession, table_name: str):
    """
    Carrega tabela da camada Silver respeitando o ambiente (Databricks vs Local).
    """
    path_or_name = get_table_path_or_name("silver", table_name, spark)
    if EXECUTION_ENV == "DATABRICKS":
        return spark.table(path_or_name)
    else:
        return spark.read.format("delta").load(path_or_name)


def generate_gold_insights(spark: SparkSession) -> str:
    """
    Calcula métricas de variação percentual mês a mês via Window Functions
    e grava a tabela Delta Gold 'insights'.
    """
    print("🔄 [GOLD] Processando métricas analíticas e Window Functions...")
    df_silver = load_silver_table(spark, TABLE_ECONOMIA_SILVER)

    # Janela temporal ordenada cronologicamente
    window_spec = Window.orderBy("data")

    df_gold = (
        df_silver.withColumn("ipca_ant", F.lag("ipca").over(window_spec))
        .withColumn("boi_ant", F.lag("boi_gordo").over(window_spec))
        .withColumn(
            "variacao_ipca",
            F.round((F.col("ipca") - F.col("ipca_ant")) / F.col("ipca_ant") * 100, 2),
        )
        .withColumn(
            "variacao_boi",
            F.round((F.col("boi_gordo") - F.col("boi_ant")) / F.col("boi_ant") * 100, 2),
        )
        .drop("ipca_ant", "boi_ant")
    )

    target_insights = get_table_path_or_name("gold", TABLE_INSIGHTS_GOLD, spark)

    if EXECUTION_ENV == "DATABRICKS":
        df_gold.write.format("delta").mode("overwrite").saveAsTable(target_insights)
    else:
        df_gold.write.format("delta").mode("overwrite").save(target_insights)

    print(f"📦 [GOLD] Tabela de Insights gravada com sucesso em: {target_insights}")
    return target_insights


def generate_gold_analitico(spark: SparkSession) -> str:
    """
    Enriquece os insights com regras de negócio qualitativas e classificação de divergência.
    """
    insights_path = get_table_path_or_name("gold", TABLE_INSIGHTS_GOLD, spark)
    if EXECUTION_ENV == "DATABRICKS":
        df_insights = spark.table(insights_path)
    else:
        df_insights = spark.read.format("delta").load(insights_path)

    df_analitico = df_insights.select(
        "data",
        "ipca",
        "boi_gordo",
        "variacao_ipca",
        "variacao_boi",
        F.round((F.col("variacao_ipca") + F.col("variacao_boi")) / 2, 2).alias("media_variacoes"),
        F.when(F.col("variacao_boi") > F.col("variacao_ipca"), "Preço do boi cresce mais")
        .when(F.col("variacao_ipca") > F.col("variacao_boi"), "Inflação cresce mais")
        .otherwise("Empate")
        .alias("destaque"),
        F.when(F.abs(F.col("variacao_boi") - F.col("variacao_ipca")) > 5, "Alta divergência")
        .when(
            F.abs(F.col("variacao_boi") - F.col("variacao_ipca")).between(2, 5),
            "Média divergência",
        )
        .otherwise("Baixa divergência")
        .alias("classe_impacto"),
    )

    target_analitico = get_table_path_or_name("gold", TABLE_ANALITICO_GOLD, spark)

    if EXECUTION_ENV == "DATABRICKS":
        df_analitico.write.format("delta").mode("overwrite").saveAsTable(target_analitico)
    else:
        df_analitico.write.format("delta").mode("overwrite").save(target_analitico)

    print(f"📦 [GOLD] Tabela Analítica de Negócio gravada com sucesso em: {target_analitico}")
    return target_analitico


if __name__ == "__main__":
    spark_session = get_spark_session("GoldAnalyticsTest")
    generate_gold_insights(spark_session)
    generate_gold_analitico(spark_session)
