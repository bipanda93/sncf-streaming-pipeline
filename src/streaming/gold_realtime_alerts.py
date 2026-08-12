"""
Gold -- Delta Silver (temps réel) -> Delta Gold realtime_alerts.

Rôle : répond à UNE question métier précise -- "quelles perturbations
actives dépassent le seuil d'alerte, là, maintenant ?" -- conformément au
principe retenu pour Gold : une table, une définition, un cas d'usage.

Ce que fait cette table, que Silver ne fait pas :
- Filtre sur status == "active".
- Dérive un niveau d'alerte (alert_level) à partir de severity_effect.
- Aplatit impacted_objects en une liste lisible de gares affectées.

Point de vigilance corrigé après investigation réelle (pas supposée) :
pour les perturbations NO_SERVICE, impacted_objects est un tableau NON
NULL contenant le train concerné, mais le champ impacted_stops à
l'intérieur de cet élément vaut None -- logique métier réelle (un train
totalement annulé n'a pas d'arrêts avec horaires "retardés" à lister,
puisqu'il ne circule pas). Une première tentative de coalesce() imbriqué
à l'intérieur des transform() n'a pas fonctionné de façon fiable
(probable souci de Spark à inférer le type d'un array() vide sans schéma
dans ce contexte imbriqué) -- corrigé en appliquant coalesce() une seule
fois, à la toute fin, sur le résultat final (nb_affected_stations,
affected_stations) plutôt qu'au milieu du calcul. Plus simple, et
garanti correct quelle que soit l'origine exacte du null dans la
structure imbriquée.

Usage :
    python -m src.streaming.gold_realtime_alerts
"""
import logging

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array_distinct, coalesce, col, concat_ws, current_timestamp,
    flatten, lit, size, transform, when,
)

from src.ingestion import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

ALERT_LEVEL_RULES = {
    "NO_SERVICE": "critical",
    "REDUCED_SERVICE": "high",
    "SIGNIFICANT_DELAYS": "high",
    "DETOUR": "medium",
    "MODIFIED_SERVICE": "medium",
    "STOP_MOVED": "medium",
    "ACCESSIBILITY_ISSUE": "medium",
    "ADDITIONAL_SERVICE": "info",
    "NO_EFFECT": "info",
}
ALERT_PRIORITY_RANK = {"critical": 1, "high": 2, "medium": 3, "low": 4, "info": 5}


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-gold-realtime-alerts")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _add_alert_level(df):
    expr = None
    for effect, level in ALERT_LEVEL_RULES.items():
        cond = when(col("severity_effect") == effect, level)
        expr = cond if expr is None else expr.when(col("severity_effect") == effect, level)
    return df.withColumn("alert_level", expr.otherwise("low"))


def _add_alert_priority(df):
    expr = None
    for level, rank in ALERT_PRIORITY_RANK.items():
        cond = when(col("alert_level") == level, rank)
        expr = cond if expr is None else expr.when(col("alert_level") == level, rank)
    return df.withColumn("alert_priority_rank", expr.otherwise(99))


def _add_affected_stations(df):
    # Pas de coalesce() imbriqué ici -- transform() sur impacted_stops=null
    # propage null jusqu'au bout du calcul, et on le corrige une seule
    # fois, à la fin, plutôt que d'essayer de l'intercepter au milieu.
    station_names = flatten(
        transform(
            col("impacted_objects"),
            lambda obj: transform(obj["impacted_stops"], lambda s: s["stop_point"]["name"]),
        )
    )
    distinct_stations = array_distinct(station_names)
    return df.withColumn(
        "affected_stations", coalesce(concat_ws(", ", distinct_stations), lit(""))
    ).withColumn("nb_affected_stations", coalesce(size(distinct_stations), lit(0)))


def run_gold_realtime_alerts() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.format("delta").load(config.DELTA_SILVER_PATH)
    active_df = silver_df.filter(col("status") == "active")

    enriched_df = _add_affected_stations(_add_alert_priority(_add_alert_level(active_df)))

    gold_df = enriched_df.select(
        "disruption_id", "alert_level", "alert_priority_rank",
        "cause", "severity_name", "severity_effect", "message_text",
        "affected_stations", "nb_affected_stations",
        "period_begin", "period_end", "updated_at",
    ).withColumn("gold_processed_at", current_timestamp())

    silver_active_count = active_df.count()
    gold_count = gold_df.count()

    gold_df.write.format("delta").mode("overwrite").save(config.DELTA_GOLD_REALTIME_ALERTS_PATH)

    logger.info(
        "Gold realtime_alerts -- %d perturbation(s) active(s) -> %d ligne(s) -> '%s'",
        silver_active_count, gold_count, config.DELTA_GOLD_REALTIME_ALERTS_PATH,
    )


if __name__ == "__main__":
    run_gold_realtime_alerts()
