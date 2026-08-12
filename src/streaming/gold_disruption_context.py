"""
Gold -- Delta Gold realtime_alerts + Delta Gold punctuality_trends ->
Delta Gold disruption_context.

Rôle : répond à la question "sommes-nous dans une période historiquement
difficile ?" en enrichissant chaque perturbation active du contexte
historique national du mois en cours (toutes années confondues), par
type de transport.

Phase 1 (celle-ci) : jointure TEMPORELLE uniquement -- comparaison au
même mois, niveau national. Une jointure GÉOGRAPHIQUE (gare -> région)
est prévue en Phase 2, mais nécessite un référentiel gare/région qu'on
n'a pas encore construit (voir notes/journal_projet.md).

Usage :
    python -m src.streaming.gold_disruption_context
"""
import logging
from datetime import datetime, timezone

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, lit, month, year

from src.ingestion import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-gold-disruption-context")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _build_monthly_context(spark, current_month_num: int):
    """
    Moyenne historique du taux de ponctualité pour le mois en cours,
    toutes années confondues, par type de transport -- puis mise à plat
    en une seule ligne (ter_..., tgv_..., intercites_...) prête à être
    croisée avec chaque perturbation active.
    """
    hist_df = spark.read.format("delta").load(config.DELTA_GOLD_PUNCTUALITY_TRENDS_PATH)

    monthly = (
        hist_df.withColumn("month_num", month(col("period_date")))
        .withColumn("year_num", year(col("period_date")))
        .filter(col("month_num") == current_month_num)
        .filter(col("taux_ponctualite").isNotNull())
        .groupBy("transport_type")
        .agg(
            avg("taux_ponctualite").alias("avg_taux"),
            count("*").alias("nb_points"),
            countDistinct("year_num").alias("nb_annees"),
        )
    )

    rows = {r["transport_type"]: r for r in monthly.collect()}
    context = {"context_month": current_month_num}
    for transport in ["TER", "TGV", "Intercités"]:
        key = transport.lower().replace("é", "e")
        r = rows.get(transport)
        context[f"{key}_taux_historique_moyen"] = float(r["avg_taux"]) if r else None
        context[f"{key}_nb_annees_reference"] = int(r["nb_annees"]) if r else 0

    return spark.createDataFrame([context])


def run_gold_disruption_context() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    alerts_df = spark.read.format("delta").load(config.DELTA_GOLD_REALTIME_ALERTS_PATH)

    current_month_num = datetime.now(timezone.utc).month
    monthly_context_df = _build_monthly_context(spark, current_month_num)

    # Cross join volontaire : le contexte mensuel est identique pour
    # toutes les perturbations actives au même instant (une seule ligne
    # de contexte à droite), pas une vraie jointure relationnelle.
    gold_df = alerts_df.crossJoin(monthly_context_df)

    alerts_count = alerts_df.count()
    gold_count = gold_df.count()

    gold_df.write.format("delta").mode("overwrite").save(config.DELTA_GOLD_DISRUPTION_CONTEXT_PATH)

    logger.info(
        "Gold disruption_context -- %d alerte(s) enrichie(s) du contexte du mois %02d -> %d ligne(s) -> '%s'",
        alerts_count, current_month_num, gold_count, config.DELTA_GOLD_DISRUPTION_CONTEXT_PATH,
    )


if __name__ == "__main__":
    run_gold_disruption_context()
