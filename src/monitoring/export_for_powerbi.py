"""
Exporte un instantané des 3 tables Gold vers des fichiers Parquet plats
(sans la couche Delta) -- Power BI ne comprend pas nativement le journal
de transactions Delta (_delta_log), et pointer un connecteur "dossier"
directement sur une table Delta renvoie la liste des fichiers bruts,
potentiellement obsolètes, plutôt que les données réelles.

Alternative écartée : connexion Power BI <-> Databricks en direct --
nécessiterait de garder le cluster Databricks actif en permanence pour
chaque rafraîchissement, contraire à la discipline de coût du projet
(apply -> vérifier -> destroy).

Usage :
    python -m src.monitoring.export_for_powerbi
"""
import logging
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.ingestion import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

EXPORT_DIR = str(Path(__file__).resolve().parent.parent.parent / "data" / "powerbi_export")

TABLES_TO_EXPORT = {
    "gold_realtime_alerts": config.DELTA_GOLD_REALTIME_ALERTS_PATH,
    "gold_disruption_context": config.DELTA_GOLD_DISRUPTION_CONTEXT_PATH,
    "gold_punctuality_trends": config.DELTA_GOLD_PUNCTUALITY_TRENDS_PATH,
}


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-export-powerbi")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_export() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    for table_name, delta_path in TABLES_TO_EXPORT.items():
        df = spark.read.format("delta").load(delta_path)
        output_path = f"{EXPORT_DIR}/{table_name}.parquet"
        # coalesce(1) -- un seul fichier Parquet, plus simple à partager
        # avec l'environnement Windows que 200 petits fichiers fragmentés.
        df.coalesce(1).write.mode("overwrite").parquet(output_path)
        logger.info("%s exporté (%d lignes) -> %s", table_name, df.count(), output_path)


if __name__ == "__main__":
    run_export()
