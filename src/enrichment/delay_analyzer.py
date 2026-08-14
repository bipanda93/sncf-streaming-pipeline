"""
Job batch mensuel -- Delta Silver (temps réel) -> Delta Gold
monthly_disruption_summaries.

Rôle : pour chaque perturbation distincte du mois ciblé (status "active" ou
"past", "future" exclu), génère un résumé en une phrase via Claude API
(Haiku), stocké dans une table Gold dédiée.

Ce job tourne selon sa propre fréquence (mensuelle), indépendamment du
calendrier Airflow des DAGs d'ingestion. Voir notes/journal_projet.md,
section "DÉCISION — Fréquence et rôle de l'enrichissement LLM".

Idempotence (corrigé après test réel) : écriture via replaceWhere sur
(target_year, target_month) plutôt que append -- un re-run du même mois
(retry Airflow, backfill, correction manuelle) remplace ses propres lignes
au lieu de les dupliquer, sans toucher aux autres mois déjà traités.
Vérifié : un run identique lancé deux fois de suite sur le même mois
donnait 1090 lignes (545 x 2) avec append, contre 545 attendu.

Source : Silver, PAS gold_realtime_alerts (instantané écrasé à chaque run,
pas un historique du mois). affected_stations réutilise la fonction déjà
débogée _add_affected_stations() de gold_realtime_alerts.py.

Usage :
    python -m src.enrichment.delay_analyzer --month 2026-08 --mock
    python -m src.enrichment.delay_analyzer --month 2026-08
    python -m src.enrichment.delay_analyzer  # mois précédent par défaut
"""
import argparse
import logging
from datetime import datetime, timezone

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, month, year

from src.enrichment.claude_client import ClaudeClient
from src.ingestion import config
from src.streaming.gold_realtime_alerts import _add_affected_stations

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-monthly-llm-enrichment")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _parse_target_month(month_str) -> tuple[int, int]:
    """Retourne (année, mois). Sans argument, le mois précédent (UTC)."""
    if month_str:
        year_num, month_num = month_str.split("-")
        return int(year_num), int(month_num)
    now = datetime.now(timezone.utc)
    if now.month == 1:
        return now.year - 1, 12
    return now.year, now.month - 1


def _table_exists(spark, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).count()
        return True
    except Exception:
        return False


def run_monthly_enrichment(target_year: int, target_month: int, mock_mode: bool) -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.format("delta").load(config.DELTA_SILVER_PATH)

    month_df = (
        silver_df.withColumn("period_year", year(col("period_begin")))
        .withColumn("period_month_num", month(col("period_begin")))
        .filter(col("period_year") == target_year)
        .filter(col("period_month_num") == target_month)
        .filter(col("status") != "future")
    )
    with_stations = _add_affected_stations(month_df)

    rows = with_stations.select(
        "disruption_id", "cause", "severity_name", "severity_effect",
        "message_text", "affected_stations", "status",
    ).collect()

    if not rows:
        logger.info(
            "Aucune perturbation trouvée pour %04d-%02d -- rien à enrichir.",
            target_year, target_month,
        )
        return

    client = ClaudeClient(mock_mode=mock_mode)
    summaries = [
        (row["disruption_id"], client.summarize_disruption(row.asDict()), row["status"])
        for row in rows
    ]

    summary_df = (
        spark.createDataFrame(summaries, schema=["disruption_id", "resume_llm", "status"])
        .withColumn("target_year", lit(target_year))
        .withColumn("target_month", lit(target_month))
        .withColumn("llm_processed_at", current_timestamp())
    )

    writer = summary_df.write.format("delta").partitionBy("target_year", "target_month")

    if _table_exists(spark, config.DELTA_GOLD_MONTHLY_SUMMARIES_PATH):
        # replaceWhere : ne remplace QUE les partitions du mois ciblé --
        # les autres mois déjà écrits restent intacts.
        (
            writer.mode("overwrite")
            .option("replaceWhere", f"target_year = {target_year} AND target_month = {target_month}")
            .save(config.DELTA_GOLD_MONTHLY_SUMMARIES_PATH)
        )
    else:
        # Premier run : la table n'existe pas encore, replaceWhere échouerait.
        writer.mode("overwrite").save(config.DELTA_GOLD_MONTHLY_SUMMARIES_PATH)

    logger.info(
        "Enrichissement mensuel -- %d perturbation(s) résumée(s) pour %04d-%02d (mode %s) -> '%s'",
        len(summaries), target_year, target_month,
        "MOCK" if mock_mode or client.mock_mode else "réel",
        config.DELTA_GOLD_MONTHLY_SUMMARIES_PATH,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrichissement LLM mensuel des perturbations SNCF")
    parser.add_argument("--month", help="Mois cible au format YYYY-MM (défaut : mois précédent)")
    parser.add_argument("--mock", action="store_true", help="Mode mock (pas d'appel API réel)")
    args = parser.parse_args()

    target_year, target_month = _parse_target_month(args.month)
    run_monthly_enrichment(target_year, target_month, mock_mode=args.mock)


if __name__ == "__main__":
    main()
