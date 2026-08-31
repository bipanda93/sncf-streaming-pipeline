"""
Exporte un instantané des tables Gold + Silver vers des fichiers Parquet
plats, consommables directement par Power BI (Import mode, Desktop).

Deux exports supplémentaires (by_region, by_station) éclatent les colonnes
multi-valeurs (regions_affectees, affected_stations) en une ligne par
valeur -- fait en Python plutôt qu'en Power Query.

INCIDENT DU 27/08 : Fabric refusait les fichiers ("Illegal Parquet type:
INT64 (TIMESTAMP(NANOS,false))") -- corrigé en forçant coerce_timestamps='us'
à l'écriture, Delta Lake n'acceptant que la précision microseconde.

INCIDENT DU 31/08 : l'export de sncf_disruptions (Silver) échouait
("cannot mix list and non-list, non-null values", colonne
impacted_objects) -- ce champ est une structure imbriquée complexe
(liste d'objets gares/horaires), jamais aplatie à ce stade contrairement
aux tables Gold. PyArrow ne peut pas l'écrire en Parquet telle quelle.
Corrigé en sélectionnant explicitement les colonnes utiles à Power BI
avant l'export, plutôt que d'exporter le schéma Silver complet.

AJOUT DU 31/08 : export de sncf_disruptions (Silver) -- les tables Gold ne
contiennent que les perturbations ACTIVES à l'instant du calcul. Impossible
d'y calculer un total ou une moyenne journalière, puisque les perturbations
déjà closes en disparaissent. Silver garde active ET past (décision
documentée dès la conception) -- seule table permettant ce type de mesure
DAX temporelle.

Nettoyage automatique avant écriture -- une version antérieure du script
utilisait df.write.parquet() (Spark), qui crée un DOSSIER portant le nom
de la table plutôt qu'un fichier unique.

Usage :
    python -m src.monitoring.export_for_powerbi
"""
import logging
import shutil
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.ingestion import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

EXPORT_DIR = str(Path(__file__).resolve().parent.parent.parent / "data" / "powerbi_export")

# Delta Lake n'accepte que les timestamps en précision microseconde --
# voir incident du 27/08 ci-dessus.
PARQUET_TIMESTAMP_KWARGS = {"coerce_timestamps": "us", "allow_truncated_timestamps": True}

TABLES_TO_EXPORT = {
    "gold_realtime_alerts": config.DELTA_GOLD_REALTIME_ALERTS_PATH,
    "gold_disruption_context": config.DELTA_GOLD_DISRUPTION_CONTEXT_PATH,
    "gold_punctuality_trends": config.DELTA_GOLD_PUNCTUALITY_TRENDS_PATH,
    "sncf_disruptions": config.DELTA_SILVER_PATH,
}

# Colonnes Silver utiles pour Power BI -- exclut volontairement
# impacted_objects (structure imbriquée, fait planter l'export Parquet,
# voir incident du 31/08) et les colonnes techniques Kafka
# (kafka_key, offset, source) sans intérêt pour la restitution.
SILVER_COLUMNS_FOR_POWERBI = [
    "disruption_id", "status", "updated_at", "cause",
    "severity_name", "severity_effect", "message_text",
    "period_begin", "period_end", "silver_processed_at",
]


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-export-powerbi")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _clear_existing(output_path: str) -> None:
    p = Path(output_path)
    if p.is_dir():
        shutil.rmtree(p)
    elif p.exists():
        p.unlink()


def export_exploded(pandas_df, column_to_split: str, output_name: str) -> None:
    exploded = pandas_df.copy()
    exploded[column_to_split] = exploded[column_to_split].str.split(", ")
    exploded = exploded.explode(column_to_split)
    exploded = exploded[exploded[column_to_split].notna() & (exploded[column_to_split] != "")]

    output_path = f"{EXPORT_DIR}/{output_name}.parquet"
    _clear_existing(output_path)
    exploded.to_parquet(output_path, index=False, **PARQUET_TIMESTAMP_KWARGS)
    logger.info("%s exporté (%d lignes, éclaté sur '%s') -> %s", output_name, len(exploded), column_to_split, output_path)


def run_export() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    Path(EXPORT_DIR).mkdir(parents=True, exist_ok=True)

    for table_name, delta_path in TABLES_TO_EXPORT.items():
        df = spark.read.format("delta").load(delta_path)

        # Silver contient une colonne imbriquée (impacted_objects) que
        # PyArrow ne sait pas écrire en Parquet -- on ne garde que les
        # colonnes utiles à Power BI pour cette table précise.
        if table_name == "sncf_disruptions":
            df = df.select(*SILVER_COLUMNS_FOR_POWERBI)

        pandas_df = df.toPandas()

        output_path = f"{EXPORT_DIR}/{table_name}.parquet"
        _clear_existing(output_path)
        pandas_df.to_parquet(output_path, index=False, **PARQUET_TIMESTAMP_KWARGS)
        logger.info("%s exporté (%d lignes) -> %s", table_name, len(pandas_df), output_path)

        if table_name == "gold_disruption_context":
            cols_communes = [
                "disruption_id", "alert_level", "severity_name",
                "period_begin", "period_end",
            ]
            export_exploded(
                pandas_df[cols_communes + ["regions_affectees"]],
                "regions_affectees", "gold_disruption_by_region",
            )
            export_exploded(
                pandas_df[cols_communes + ["affected_stations"]],
                "affected_stations", "gold_disruption_by_station",
            )


if __name__ == "__main__":
    run_export()
