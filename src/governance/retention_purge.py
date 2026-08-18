"""
Purge Bronze temps réel selon une politique de rétention glissante.

Politique complète : voir notes/politique_retention.md. En résumé :
Bronze temps réel = 12 mois glissants (données opérationnelles brutes,
déjà distillées dans Silver/Gold) ; toutes les autres tables = rétention
indéfinie (l'analyse de tendance long terme est l'objectif même du
projet).

Chaque exécution trace son propre passage dans gold_audit_log via
audit_logger.py.

Usage :
    python -m src.governance.retention_purge --dry-run
    python -m src.governance.retention_purge
"""
import argparse
import logging
from datetime import datetime, timedelta, timezone

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.governance.audit_logger import log_job_execution
from src.ingestion import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

RETENTION_MONTHS_BRONZE_REALTIME = 12


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-retention-purge")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_retention_purge(dry_run: bool = False) -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_MONTHS_BRONZE_REALTIME * 30)

    bronze_df = spark.read.format("delta").load(config.DELTA_BRONZE_PATH)
    total_before = bronze_df.count()
    purge_count = bronze_df.filter(col("bronze_ingested_at") < cutoff).count()

    if dry_run:
        logger.info(
            "DRY RUN -- %d ligne(s) sur %d seraient purgées (antérieures à %s)",
            purge_count, total_before, cutoff.isoformat(),
        )
        log_job_execution(
            spark, job_name="retention_purge", source_table=config.DELTA_BRONZE_PATH,
            target_table=config.DELTA_BRONZE_PATH, rows_processed=purge_count,
            status="dry_run", details=f"cutoff={cutoff.isoformat()}",
        )
        return

    if purge_count > 0:
        delta_table = DeltaTable.forPath(spark, config.DELTA_BRONZE_PATH)
        delta_table.delete(col("bronze_ingested_at") < cutoff)

    remaining = spark.read.format("delta").load(config.DELTA_BRONZE_PATH).count()
    logger.info(
        "Purge rétention -- %d ligne(s) supprimée(s) (cutoff %s) -- %d -> %d",
        purge_count, cutoff.isoformat(), total_before, remaining,
    )
    log_job_execution(
        spark, job_name="retention_purge", source_table=config.DELTA_BRONZE_PATH,
        target_table=config.DELTA_BRONZE_PATH, rows_processed=purge_count,
        status="success", details=f"cutoff={cutoff.isoformat()}, {total_before} -> {remaining}",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Purge Bronze temps reel selon la politique de retention")
    parser.add_argument("--dry-run", action="store_true", help="Simule sans supprimer")
    args = parser.parse_args()
    run_retention_purge(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
