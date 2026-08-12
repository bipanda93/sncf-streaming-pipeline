"""
Gold -- Delta Silver historique (5 jeux régularité) -> Delta Gold
punctuality_trends.

Rôle : répond à UNE question métier précise -- "quelle est la tendance de
ponctualité par ligne/région/axe dans le temps ?" -- en unifiant 5 sources
au schéma hétérogène (confirmé lors de la construction Bronze/Silver) en
une seule table à grain flexible.

Décision de conception : plutôt que de forcer les 5 sources dans des
colonnes identiques (impossible -- tgv_globale n'a pas de dimension
géographique, tgv_liaisons n'a pas de taux direct...), le grain de chaque
ligne est explicité via deux colonnes (scope_type, scope_value) -- un
motif standard en modélisation dimensionnelle pour unifier des faits à
granularité hétérogène. C'est ce genre de logique "nécessitant une
connaissance du domaine" qui appartient à Gold, pas à Silver.

Le taux de ponctualité est calculé différemment selon la source :
- TER / Intercités : taux_de_regularite, déjà fourni par SNCF
- TGV globale / axes : regularite_composite, déjà fourni par SNCF
- TGV liaisons : recalculé depuis les compteurs bruts (nb_train_prevu,
  nb_train_retard_arrivee) -- cette source n'a aucun taux direct

Usage :
    python -m src.historical.gold_punctuality_trends
"""
import logging

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, when

from src.ingestion import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

GOLD_COLUMNS = [
    "period_date", "period_label", "dataset_key", "transport_type",
    "scope_type", "scope_value", "taux_ponctualite",
    "nb_trains_prevus", "nb_annulations",
]


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-gold-punctuality-trends")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _read_silver(spark, dataset_key: str):
    return spark.read.format("delta").load(f"{config.DELTA_SILVER_HISTORICAL_PATH}/{dataset_key}")


def _harmonize_ter(df):
    return df.select(
        col("period_date"), col("period_label"),
        lit("ter").alias("dataset_key"), lit("TER").alias("transport_type"),
        lit("region").alias("scope_type"), col("region").alias("scope_value"),
        col("taux_de_regularite").alias("taux_ponctualite"),
        col("nombre_de_trains_programmes").alias("nb_trains_prevus"),
        col("nombre_de_trains_annules").alias("nb_annulations"),
    )


def _harmonize_intercites(df):
    return df.select(
        col("period_date"), col("period_label"),
        lit("intercites").alias("dataset_key"), lit("Intercités").alias("transport_type"),
        lit("liaison").alias("scope_type"),
        concat_ws(" -> ", col("depart"), col("arrivee")).alias("scope_value"),
        col("taux_de_regularite").alias("taux_ponctualite"),
        col("nombre_de_trains_programmes").alias("nb_trains_prevus"),
        col("nombre_de_trains_annules").alias("nb_annulations"),
    )


def _harmonize_tgv_globale(df):
    return df.select(
        col("period_date"), col("period_label"),
        lit("tgv_globale").alias("dataset_key"), lit("TGV").alias("transport_type"),
        lit("national").alias("scope_type"), lit("France").alias("scope_value"),
        col("regularite_composite").alias("taux_ponctualite"),
        lit(None).cast("double").alias("nb_trains_prevus"),
        lit(None).cast("double").alias("nb_annulations"),
    )


def _harmonize_tgv_axes(df):
    return df.select(
        col("period_date"), col("period_label"),
        lit("tgv_axes").alias("dataset_key"), lit("TGV").alias("transport_type"),
        lit("axe").alias("scope_type"), col("axe").alias("scope_value"),
        col("regularite_composite").alias("taux_ponctualite"),
        lit(None).cast("double").alias("nb_trains_prevus"),
        lit(None).cast("double").alias("nb_annulations"),
    )


def _harmonize_tgv_liaisons(df):
    taux = when(
        col("nb_train_prevu") > 0,
        (col("nb_train_prevu") - col("nb_train_retard_arrivee")) / col("nb_train_prevu") * 100,
    ).otherwise(None)
    return df.select(
        col("period_date"), col("period_label"),
        lit("tgv_liaisons").alias("dataset_key"), lit("TGV").alias("transport_type"),
        lit("liaison").alias("scope_type"),
        concat_ws(" -> ", col("gare_depart"), col("gare_arrivee")).alias("scope_value"),
        taux.alias("taux_ponctualite"),
        col("nb_train_prevu").alias("nb_trains_prevus"),
        col("nb_annulation").alias("nb_annulations"),
    )


HARMONIZERS = {
    "ter": _harmonize_ter,
    "intercites": _harmonize_intercites,
    "tgv_globale": _harmonize_tgv_globale,
    "tgv_axes": _harmonize_tgv_axes,
    "tgv_liaisons": _harmonize_tgv_liaisons,
}


def run_gold_punctuality_trends() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    harmonized_dfs = []
    counts_by_source = {}
    for dataset_key, harmonize_fn in HARMONIZERS.items():
        silver_df = _read_silver(spark, dataset_key)
        counts_by_source[dataset_key] = silver_df.count()
        harmonized_dfs.append(harmonize_fn(silver_df).select(*GOLD_COLUMNS))

    union_df = harmonized_dfs[0]
    for df in harmonized_dfs[1:]:
        union_df = union_df.unionByName(df)

    gold_df = union_df.withColumn("gold_processed_at", current_timestamp())

    total_silver = sum(counts_by_source.values())
    gold_count = gold_df.count()

    gold_df.write.format("delta").mode("overwrite").save(config.DELTA_GOLD_PUNCTUALITY_TRENDS_PATH)

    logger.info(
        "Gold punctuality_trends -- detail par source: %s -- total: %d -> Gold: %d -> '%s'",
        counts_by_source, total_silver, gold_count, config.DELTA_GOLD_PUNCTUALITY_TRENDS_PATH,
    )


if __name__ == "__main__":
    run_gold_punctuality_trends()
