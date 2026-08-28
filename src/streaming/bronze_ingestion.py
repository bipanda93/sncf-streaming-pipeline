"""
Job Databricks Structured Streaming -- Kafka/Event Hubs -> Delta Bronze.

Rôle : lire en continu le topic `sncf-raw`, écrire les événements TELS
QUELS (le message brut est toujours conservé dans `raw_value`) dans une
table Delta Bronze -- aucune transformation métier ici, conformément au
principe Bronze : données brutes, horodatées, traçables. Le dédoublonnage
et le typage fin sont le rôle de la couche Silver.

Portable local <-> Azure : les mêmes variables que src/ingestion/config.py
pilotent la connexion Kafka/Event Hubs -- une seule source de vérité pour
la config, jamais dupliquée entre le producer et ce consumer.

Usage :
    python -m src.streaming.bronze_ingestion            # tourne en continu
    python -m src.streaming.bronze_ingestion --once      # traite ce qui est
                                                            disponible puis s'arrête
                                                            (utile pour tester)
"""
import argparse
import logging

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, get_json_object, to_date

from src.ingestion import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Connecteur Kafka pour Structured Streaming -- package Maven distinct de
# Delta, doit être ajouté explicitement. Version alignée sur PySpark/Delta
# 4.1.0 et Scala 2.13 (cohérence obligatoire, sinon échec au chargement).
KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"


def build_spark_session() -> SparkSession:
    """
    Construit une SparkSession avec Delta Lake ET le connecteur Kafka
    configurés.

    configure_spark_with_delta_pip attache automatiquement les JARs Delta
    en local -- sur un vrai cluster Databricks, Delta est déjà natif et
    cette étape est transparente. Le connecteur Kafka, lui, doit être
    ajouté explicitement via extra_packages : ce n'est pas un JAR Delta,
    Spark ne le télécharge jamais tout seul.
    """
    builder = (
        SparkSession.builder.appName("sncf-bronze-ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=[KAFKA_CONNECTOR_PACKAGE],
    ).getOrCreate()


def build_kafka_read_options() -> dict:
    """
    Options de lecture Kafka pour Spark, dérivées de la même config que
    le producer -- jamais dupliquées.

    INCIDENT DU 28/08 (voir notes/incidents_2026-08-28.md) : un
    redémarrage complet de Kafka (stop + rm + up, pour purger un fichier
    de checkpoint interne corrompu) a réinitialisé le topic, alors que le
    checkpoint Spark (stocké séparément, dans data/delta/checkpoints/) se
    souvenait encore de l'ancien offset. Spark refusait de repartir,
    l'offset attendu n'existant plus dans le nouveau Kafka -- erreur
    KafkaIllegalStateException.

    failOnDataLoss=false : cohérent avec le rôle de Kafka dans cette
    architecture -- un simple tampon de transit, jamais la source de
    vérité (l'API SNCF, réinterrogeable à volonté, joue ce rôle). Une
    perte de position dans ce tampon ne justifie pas d'arrêter tout le
    pipeline.
    """
    options = {
        "kafka.bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "subscribe": config.TOPIC_RAW,
        "startingOffsets": "earliest",  # récupère aussi les messages déjà publiés
        "failOnDataLoss": "false",
    }
    if config.KAFKA_SECURITY_PROTOCOL.startswith("SASL"):
        options.update(
            {
                "kafka.security.protocol": config.KAFKA_SECURITY_PROTOCOL,
                "kafka.sasl.mechanism": config.KAFKA_SASL_MECHANISM or "PLAIN",
                "kafka.sasl.jaas.config": (
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    f'username="{config.KAFKA_SASL_USERNAME or "$ConnectionString"}" '
                    f'password="{config.KAFKA_SASL_PASSWORD}";'
                ),
            }
        )
    return options


def run_bronze_ingestion(once: bool = False) -> None:
    """Lit en continu (ou une fois) le topic Kafka sncf-raw et écrit vers Delta Bronze."""
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # les logs Spark par défaut sont très verbeux

    raw_stream = spark.readStream.format("kafka").options(**build_kafka_read_options()).load()

    value_str = col("value").cast("string")
    bronze_df = raw_stream.select(
        col("key").cast("string").alias("kafka_key"),
        value_str.alias("raw_value"),  # message complet, jamais perdu -- filet de sécurité
        get_json_object(value_str, "$.ingested_at").alias("ingested_at"),
        get_json_object(value_str, "$.source").alias("source"),
        get_json_object(value_str, "$.event_type").alias("event_type"),
        get_json_object(value_str, "$.raw").alias("raw_payload"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("bronze_ingested_at"),
        to_date(current_timestamp()).alias("ingestion_date"),
    )

    writer = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.CHECKPOINT_BRONZE_PATH)
        .partitionBy("ingestion_date")
    )

    logger.info(
        "Ingestion Bronze -- lecture '%s' -> écriture '%s' (checkpoint: %s)",
        config.TOPIC_RAW,
        config.DELTA_BRONZE_PATH,
        config.CHECKPOINT_BRONZE_PATH,
    )

    if once:
        query = writer.trigger(availableNow=True).start(config.DELTA_BRONZE_PATH)
    else:
        query = writer.start(config.DELTA_BRONZE_PATH)

    query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion Bronze SNCF -- Kafka -> Delta Lake")
    parser.add_argument(
        "--once", action="store_true",
        help="Traite les messages disponibles puis s'arrête (au lieu de tourner en continu)",
    )
    args = parser.parse_args()
    run_bronze_ingestion(once=args.once)


if __name__ == "__main__":
    main()
