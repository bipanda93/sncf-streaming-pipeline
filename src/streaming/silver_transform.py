"""
Transformation Silver -- Delta Bronze (temps réel) -> Delta Silver.

Rôle : dédoublonner et typer les perturbations SNCF. Bronze contient des
doublons (une même perturbation vue à plusieurs cycles d'ingestion) et ne
stocke que du texte brut (raw_payload = JSON en chaîne). Silver produit
un enregistrement propre par perturbation, avec des colonnes typées.

Décision retenue : garder les perturbations "active" ET "past" dans la
même table (colonne status), plutôt que de filtrer dès Silver -- la
profondeur historique est déjà couverte par la source régularité, donc
Silver temps réel n'a pas besoin de trancher ce choix à la place de la
couche Gold / Power BI, qui pourra filtrer sur status si besoin.

Batch, pas streaming : comme historical_loader.py, ce job relit
l'intégralité de la table Bronze à chaque exécution -- le dédoublonnage
nécessite de voir toutes les lignes pour déterminer laquelle est la plus
récente par perturbation, ce qui n'est pas naturellement exprimable en
streaming pur.

Usage :
    python -m src.streaming.silver_transform
"""
import logging

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, desc, from_json, row_number, to_timestamp
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

from src.ingestion import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Format des dates SNCF/Navitia observé en direct : "20260811T083332"
SNCF_TIMESTAMP_FORMAT = "yyyyMMdd'T'HHmmss"

# Schéma explicite du JSON raw_payload -- une seule passe de parsing
# (from_json), reflète la structure réelle observée dans les données SNCF.
_SEVERITY_SCHEMA = StructType([
    StructField("name", StringType()),
    StructField("effect", StringType()),
    StructField("priority", IntegerType()),
])
_MESSAGE_SCHEMA = StructType([StructField("text", StringType())])
_APPLICATION_PERIOD_SCHEMA = StructType([
    StructField("begin", StringType()),
    StructField("end", StringType()),
])
_STOP_POINT_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
])
_IMPACTED_STOP_SCHEMA = StructType([
    StructField("stop_point", _STOP_POINT_SCHEMA),
    StructField("base_arrival_time", StringType()),
    StructField("base_departure_time", StringType()),
    StructField("amended_arrival_time", StringType()),
    StructField("amended_departure_time", StringType()),
    StructField("cause", StringType()),
    StructField("stop_time_effect", StringType()),
])
_PT_OBJECT_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
])
_IMPACTED_OBJECT_SCHEMA = StructType([
    StructField("pt_object", _PT_OBJECT_SCHEMA),
    StructField("impacted_stops", ArrayType(_IMPACTED_STOP_SCHEMA)),
])

DISRUPTION_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("status", StringType()),
    StructField("updated_at", StringType()),
    StructField("cause", StringType()),
    StructField("severity", _SEVERITY_SCHEMA),
    StructField("messages", ArrayType(_MESSAGE_SCHEMA)),
    StructField("application_periods", ArrayType(_APPLICATION_PERIOD_SCHEMA)),
    StructField("impacted_objects", ArrayType(_IMPACTED_OBJECT_SCHEMA)),
    StructField("contributor", StringType()),
])


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-silver-transform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_silver_transform() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = spark.read.format("delta").load(config.DELTA_BRONZE_PATH)

    parsed_df = bronze_df.withColumn(
        "disruption", from_json(col("raw_payload"), DISRUPTION_SCHEMA)
    )

    flat_df = parsed_df.select(
        col("disruption.id").alias("disruption_id"),
        col("disruption.status").alias("status"),
        to_timestamp(col("disruption.updated_at"), SNCF_TIMESTAMP_FORMAT).alias("updated_at"),
        col("disruption.cause").alias("cause"),
        col("disruption.severity.name").alias("severity_name"),
        col("disruption.severity.effect").alias("severity_effect"),
        col("disruption.messages")[0]["text"].alias("message_text"),
        to_timestamp(col("disruption.application_periods")[0]["begin"], SNCF_TIMESTAMP_FORMAT).alias("period_begin"),
        to_timestamp(col("disruption.application_periods")[0]["end"], SNCF_TIMESTAMP_FORMAT).alias("period_end"),
        col("disruption.impacted_objects").alias("impacted_objects"),
        col("disruption.contributor").alias("contributor"),
        col("source"),
        col("kafka_key"),
        col("offset"),
        col("bronze_ingested_at"),
    )

    # Reject zone : JSON non parsable ou id absent -- jamais ignoré en silence.
    reject_df = flat_df.filter(col("disruption_id").isNull())
    valid_df = flat_df.filter(col("disruption_id").isNotNull())

    # Dédoublonnage : une ligne par perturbation, la plus récente
    # (updated_at, puis offset Kafka en cas d'égalité).
    window = Window.partitionBy("disruption_id").orderBy(desc("updated_at"), desc("offset"))
    deduped_df = (
        valid_df.withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("silver_processed_at", current_timestamp())
    )

    bronze_count = bronze_df.count()
    deduped_count = deduped_df.count()
    reject_count = reject_df.count()

    deduped_df.write.format("delta").mode("overwrite").save(config.DELTA_SILVER_PATH)
    if reject_count > 0:
        reject_df.write.format("delta").mode("overwrite").save(config.DELTA_SILVER_REJECT_PATH)

    logger.info(
        "Silver -- %d lignes Bronze -> %d perturbations distinctes (%d rejetées) -> '%s'",
        bronze_count, deduped_count, reject_count, config.DELTA_SILVER_PATH,
    )


if __name__ == "__main__":
    run_silver_transform()
