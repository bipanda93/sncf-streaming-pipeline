"""
Exporte un instantané des tables Gold vers des fichiers Parquet plats,
consommables directement par Power BI (mode Direct Lake, Fabric).

Deux exports supplémentaires (by_region, by_station) éclatent les colonnes
multi-valeurs (regions_affectees, affected_stations) en une ligne par
valeur -- fait en Python plutôt qu'en Power Query, puisque Direct Lake ne
passe pas par une couche de transformation intermédiaire. Les deux
éclatements sont VOLONTAIREMENT séparés (pas dans le même export) pour
éviter un produit croisé région x gare qui fausserait les comptages.

Nettoyage automatique avant écriture -- une version antérieure du script
utilisait df.write.parquet() (Spark), qui crée un DOSSIER portant le nom
de la table plutôt qu'un fichier unique. Sans ce nettoyage, pandas refuse
d'écrire un fichier là où un dossier du même nom existe déjà.

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


def _clear_existing(output_path: str) -> None:
    """Supprime tout fichier OU dossier résiduel au chemin cible, avant
    écriture -- garantit un export idempotent quel que soit ce qui
    traînait avant (résidu Spark, ancien run, etc.)."""
    p = Path(output_path)
    if p.is_dir():
        shutil.rmtree(p)
    elif p.exists():
        p.unlink()


def export_exploded(pandas_df, column_to_split: str, output_name: str) -> None:
    """Une ligne par valeur individuelle (région ou gare) -- jamais les
    deux colonnes multi-valeurs éclatées ensemble (évite le produit
    croisé)."""
    exploded = pandas_df.copy()
    exploded[column_to_split] = exploded[column_to_split].str.split(", ")
    exploded = exploded.explode(column_to_split)
    exploded = exploded[exploded[column_to_split].notna() & (exploded[column_to_split] != "")]

    output_path = f"{EXPORT_DIR}/{output_name}.parquet"
    _clear_existing(output_path)
    exploded.to_parquet(output_path, index=False)
    logger.info("%s exporté (%d lignes, éclaté sur '%s') -> %s", output_name, len(exploded), column_to_split, output_path)


def run_export() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    Path(EXPORT_DIR).mkdir(parents=True, exist_ok=True)

    for table_name, delta_path in TABLES_TO_EXPORT.items():
        df = spark.read.format("delta").load(delta_path)
        pandas_df = df.toPandas()

        output_path = f"{EXPORT_DIR}/{table_name}.parquet"
        _clear_existing(output_path)
        pandas_df.to_parquet(output_path, index=False)
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
