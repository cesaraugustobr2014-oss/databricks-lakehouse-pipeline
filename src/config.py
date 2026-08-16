"""
Módulo de Configuração Central do Lakehouse
Compatível com Databricks (Unity Catalog) e Ambiente Local (PySpark + Delta Lake).
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env se existir
load_dotenv()

# ==============================================================================
# 1. PARÂMETROS GERAIS DE EXECUÇÃO
# ==============================================================================
EXECUTION_ENV = os.getenv("EXECUTION_ENV", "LOCAL").upper()
CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")

# Diretórios para armazenamento local em formato Delta
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_DATA_DIR = PROJECT_ROOT / "data"
LOCAL_BRONZE_DIR = LOCAL_DATA_DIR / "bronze"
LOCAL_SILVER_DIR = LOCAL_DATA_DIR / "silver"
LOCAL_GOLD_DIR = LOCAL_DATA_DIR / "gold"

# Nomes de Schemas e Tabelas (Unity Catalog)
SCHEMA_BRONZE = "bronze_economia"
SCHEMA_SILVER = "silver_economia"
SCHEMA_GOLD = "gold_economia"

SCHEMA_BRONZE_API = "bronze_api"
SCHEMA_ANALYTICS_API = "analytics_api"

TABLE_IPCA_BRONZE = "ipca"
TABLE_BOI_BRONZE = "boi_gordo"
TABLE_ECONOMIA_SILVER = "economia"
TABLE_INSIGHTS_GOLD = "insights"
TABLE_ANALITICO_GOLD = "gold_analitico"
TABLE_COTACOES_BRONZE = "cotacoes_alpha"

# ==============================================================================
# 2. PARÂMETROS DE APIS EXTERNAS
# ==============================================================================
# API do Banco Central do Brasil (SGS - Sistema Gerenciador de Séries Temporais)
BCB_IPCA_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"

# API Alpha Vantage (Mercado Financeiro / Cotações B3)
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "SUA_API_KEY_AQUI")
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"

# Tickers da B3 para monitoramento
B3_TICKERS = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "ABEV3.SA"]

# ==============================================================================
# 3. GESTÃO DA SESSÃO SPARK
# ==============================================================================
def get_spark_session(app_name: str = "DatabricksLakehousePipeline"):
    """
    Retorna a sessão do Spark ativa (se rodando no Databricks)
    ou cria uma nova SparkSession com extensões Delta Lake ativadas (se local).
    """
    try:
        # Tenta obter sessão ativa (comum no Databricks)
        from pyspark.sql import SparkSession
        active_spark = SparkSession.getActiveSession()
        if active_spark is not None:
            return active_spark
    except Exception:
        pass

    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(LOCAL_DATA_DIR / "warehouse"))
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def get_table_path_or_name(layer: str, table_name: str, spark=None) -> str:
    """
    Retorna o nome completo de 3 níveis no Unity Catalog (ex: workspace.bronze_economia.ipca)
    ou o caminho do diretório local (ex: ./data/bronze/ipca).
    """
    if EXECUTION_ENV == "DATABRICKS":
        schema_map = {
            "bronze": SCHEMA_BRONZE,
            "silver": SCHEMA_SILVER,
            "gold": SCHEMA_GOLD,
            "bronze_api": SCHEMA_BRONZE_API,
            "analytics_api": SCHEMA_ANALYTICS_API,
        }
        schema = schema_map.get(layer, SCHEMA_BRONZE)
        return f"{CATALOG}.{schema}.{table_name}"
    else:
        layer_dir_map = {
            "bronze": LOCAL_BRONZE_DIR,
            "silver": LOCAL_SILVER_DIR,
            "gold": LOCAL_GOLD_DIR,
            "bronze_api": LOCAL_BRONZE_DIR / "api",
            "analytics_api": LOCAL_GOLD_DIR / "api",
        }
        target_dir = layer_dir_map.get(layer, LOCAL_BRONZE_DIR) / table_name
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        return str(target_dir)
