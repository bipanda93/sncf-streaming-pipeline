"""
Job batch -- Fichiers CSV régularité SNCF -> Delta Bronze historique.

Rôle : télécharger un jeu de régularité (via RegularityClient), écrire les
données TELLES QUELLES (toutes les colonnes en texte brut, aucun typage)
dans une table Delta Bronze historique -- même principe Bronze que pour le
flux temps réel. Le typage est le rôle de la couche Silver.

Différence avec bronze_ingestion.py : ici pas de streaming, un CSV
téléchargé une fois est traité en BATCH.

Note technique importante : le parsing CSV est fait par Python (module
csv standard), PAS par le lecteur CSV de Spark. Constat expérimental : le
fichier régularité SNCF contient des champs multi-lignes (colonne
"commentaires") que le lecteur CSV de Spark fragmente en lignes cassées,
même avec l'option multiLine=True correctement activée (comportement
documenté comme fragile sur ce genre de fichier). Le module csv de Python,
lui, parse ce fichier de façon stable et vérifiée (2366 lignes, 9 colonnes
constantes, aucune anomalie, résultat reproductible sur plusieurs tests).
On construit donc le DataFrame Spark directement depuis les lignes déjà
parsées par Python, sans repasser par un fichier texte que Spark
réinterpréterait avec ses propres ambiguïtés.

Usage :
    python -m src.historical.historical_loader --dataset ter
    python -m src.historical.historical_loader --dataset ter --mock
"""
import argparse
import csv
import io
import logging

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date

from src.historical.regularity_client import DATASETS, RegularityClient
from src.ingestion import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-historical-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _parse_csv_rows(csv_text: str) -> tuple[list[str], list[list[str]]]:
    """
    Parse le CSV avec le module csv standard de Python (gère nativement
    les champs multi-lignes entre guillemets, contrairement au lecteur
    CSV par défaut de Spark). Retourne (en-têtes, lignes de données).
    """
    reader = csv.reader(io.StringIO(csv_text), delimiter=";")
    rows = list(reader)
    header, data_rows = rows[0], rows[1:]
    # Ceinture et bretelles : élimine les éventuelles lignes totalement
    # vides (ex. ligne finale vide du fichier).
    data_rows = [r for r in data_rows if any(field.strip() for field in r)]
    return header, data_rows


def load_dataset_to_bronze(dataset_key: str, mock_mode: bool = False) -> int:
    """
    Télécharge un jeu de régularité et l'écrit en Delta Bronze historique.
    Retourne le nombre de lignes écrites.
    """
    client = RegularityClient(mock_mode=mock_mode)
    csv_text = client.download_csv(dataset_key)
    header, data_rows = _parse_csv_rows(csv_text)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Toutes les colonnes en string -- aucun typage ici, conforme au
    # principe Bronze déjà appliqué dans bronze_ingestion.py. Construit
    # directement depuis les lignes Python déjà parsées, pas depuis un
    # fichier texte réinterprété par Spark.
    raw_df = spark.createDataFrame(data_rows, schema=header)

    bronze_df = (
        raw_df.withColumn("dataset_key", lit(dataset_key))
        .withColumn("source", lit("mock" if mock_mode else "data_sncf_com"))
        .withColumn("bronze_ingested_at", current_timestamp())
        .withColumn("ingestion_date", to_date(current_timestamp()))
    )

    row_count = bronze_df.count()
    output_path = f"{config.DELTA_BRONZE_HISTORICAL_PATH}/{dataset_key}"

    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .save(output_path)
    )

    logger.info(
        "Historique Bronze -- %d ligne(s) écrite(s) pour '%s' -> '%s'",
        row_count, dataset_key, output_path,
    )
    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Chargement Bronze historique SNCF (batch)")
    parser.add_argument(
        "--dataset", required=True, choices=list(DATASETS),
        help="Jeu de régularité à charger",
    )
    parser.add_argument("--mock", action="store_true", help="Mode mock (pas de téléchargement réel)")
    args = parser.parse_args()

    load_dataset_to_bronze(args.dataset, mock_mode=args.mock)


if __name__ == "__main__":
    main()
