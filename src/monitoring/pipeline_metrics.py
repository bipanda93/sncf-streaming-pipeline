"""
Collecte des métriques quantitatives après chaque run du pipeline temps
réel -- row_count et taux de nullité sur une colonne clé, par table Gold.
Alimente la détection d'anomalies (voir anomaly_detector.py).

Conçu comme une étape séparée, ajoutée en fin de DAG, plutôt que
d'instrumenter chaque script existant (silver_transform.py,
gold_realtime_alerts.py...) -- évite de modifier du code déjà testé et
stable juste pour ajouter cette fonctionnalité.

Les deux métriques choisies (row_count, taux de nullité) ne sont pas
arbitraires : elles auraient détecté plusieurs incidents réels de ce
projet -- la pagination SNCF (25 lignes reçues au lieu de ~600) et le NULL
silencieux sur les alertes NO_SERVICE (voir notes/incidents_2026-08-12.md)
se seraient tous les deux traduits par une valeur anormale sur l'une de
ces deux métriques.

Usage :
    python -m src.monitoring.pipeline_metrics
"""
import logging
from datetime import datetime, timezone

from delta import configure_spark_with_delta_pip
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col

from src.ingestion import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

TABLES_TO_MONITOR = {
    "gold_realtime_alerts": {
        "path": config.DELTA_GOLD_REALTIME_ALERTS_PATH,
        "key_column": "nb_affected_stations",
    },
    "gold_disruption_context": {
        "path": config.DELTA_GOLD_DISRUPTION_CONTEXT_PATH,
        "key_column": "region_taux_historique_moyen",
    },
}


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-pipeline-metrics")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _table_exists(spark, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).count()
        return True
    except Exception:
        return False


def collect_metrics_for_table(spark, table_name: str, table_path: str, key_column: str) -> Row:
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()

    if row_count == 0:
        null_rate = None
    else:
        null_count = df.filter(col(key_column).isNull()).count()
        null_rate = null_count / row_count

    return Row(
        table_name=table_name,
        executed_at=datetime.now(timezone.utc),
        row_count=row_count,
        key_column_name=key_column,
        null_rate=null_rate,
    )


def run_pipeline_metrics() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    rows = []
    for table_name, cfg in TABLES_TO_MONITOR.items():
        if not _table_exists(spark, cfg["path"]):
            logger.warning("Table %s introuvable à %s -- ignorée pour ce run.", table_name, cfg["path"])
            continue
        rows.append(collect_metrics_for_table(spark, table_name, cfg["path"], cfg["key_column"]))

    if not rows:
        logger.info("Aucune table disponible -- rien à mesurer.")
        return

    metrics_df = spark.createDataFrame(rows)

    mode = "append" if _table_exists(spark, config.DELTA_GOLD_PIPELINE_METRICS_PATH) else "overwrite"
    metrics_df.write.format("delta").mode(mode).save(config.DELTA_GOLD_PIPELINE_METRICS_PATH)

    for r in rows:
        logger.info(
            "Métriques -- %s : %d lignes, taux de nullité sur '%s' = %s",
            r["table_name"], r["row_count"], r["key_column_name"],
            f"{r['null_rate']:.3f}" if r["null_rate"] is not None else "N/A",
        )


if __name__ == "__main__":
    run_pipeline_metrics()
