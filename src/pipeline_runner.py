"""
Orquestrador Principal do Pipeline Lakehouse
Executa o fluxo completo de ponta a ponta:
Fontes de Dados ➔ Bronze ➔ Silver ➔ Gold ➔ Visualização de Resultados
"""
import sys
import time
from src.config import get_spark_session, EXECUTION_ENV, CATALOG
from src.ingestion_bcb import ingest_ipca_bronze, ingest_boi_gordo_bronze
from src.ingestion_alpha import ingest_b3_stocks_bronze
from src.silver_transformations import transform_silver_economia
from src.gold_analytics import generate_gold_insights, generate_gold_analitico


def run_pipeline():
    print("=" * 70)
    print("🚀 INICIANDO EXECUÇÃO DO PIPELINE DATABRICKS / DELTA LAKEHOUSE")
    print(f"📌 Ambiente Detectado: {EXECUTION_ENV}")
    if EXECUTION_ENV == "DATABRICKS":
        print(f"🏛️ Catálogo Unity Catalog: {CATALOG}")
    print("=" * 70)

    start_time = time.time()

    # 1. Inicializa a Sessão do Spark
    print("\n⚡ [1/5] Inicializando motor Apache Spark com extensões Delta Lake...")
    spark = get_spark_session("LakehousePipelineRunner")

    # 2. Ingestão Camada Bronze (BCB & Boi Gordo + Alpha Vantage)
    print("\n📥 [2/5] Ingerindo dados para a Camada Bronze...")
    ingest_ipca_bronze(spark)
    ingest_boi_gordo_bronze(spark)
    ingest_b3_stocks_bronze(spark)

    # 3. Transformação e Qualidade na Camada Silver
    print("\n🧪 [3/5] Processando e normalizando dados para a Camada Silver...")
    transform_silver_economia(spark)

    # 4. Inteligência e Modelagem na Camada Gold
    print("\n🏆 [4/5] Gerando métricas analíticas e Window Functions para a Camada Gold...")
    generate_gold_insights(spark)
    generate_gold_analitico(spark)

    # 5. Exibição de Resultados
    print("\n📊 [5/5] Amostra Final dos Dados Consolidados na Camada Gold:")
    print("-" * 70)
    try:
        from src.gold_analytics import load_silver_table
        from src.config import get_table_path_or_name, TABLE_ANALITICO_GOLD

        gold_path = get_table_path_or_name("gold", TABLE_ANALITICO_GOLD, spark)
        if EXECUTION_ENV == "DATABRICKS":
            df_final = spark.table(gold_path)
        else:
            df_final = spark.read.format("delta").load(gold_path)

        df_final.show(15, truncate=False)
    except Exception as e:
        print(f"⚠️ Erro ao exibir amostra final: {e}")

    elapsed = round(time.time() - start_time, 2)
    print("=" * 70)
    print(f"✅ PIPELINE EXECUTADO COM SUCESSO EM {elapsed} SEGUNDOS!")
    print("=" * 70)


if __name__ == "__main__":
    run_pipeline()
