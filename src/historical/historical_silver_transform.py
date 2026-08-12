"""
Transformation Silver -- Delta Bronze historique (CSV régularité) -> Delta
Silver historique.

Rôle : typer les colonnes numériques (toutes en string depuis Bronze) et
convertir la colonne "date" (format "YYYY-MM") en un vrai type date.

Point de vigilance découvert en vérifiant les schémas réels avant d'écrire
ce fichier : les 5 jeux de régularité N'ONT PAS le même schéma -- colonnes
différentes selon le type de train. La liste des colonnes numériques est
donc définie explicitement, par jeu de données, après lecture réelle de
chaque schéma (voir notes/journal_projet.md).

Pas de dédoublonnage ici, contrairement au Silver temps réel :
historical_loader.py écrase Bronze avec le fichier complet à chaque
exécution (mode overwrite), donc pas d'accumulation de doublons.

Usage :
    python -m src.historical.historical_silver_transform --dataset ter
    python -m src.historical.historical_silver_transform --dataset tgv_liaisons
"""
import argparse
import logging
from functools import reduce

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, current_timestamp, expr, lit, to_date, trim
from pyspark.sql.types import DoubleType

from src.historical.regularity_client import DATASETS
from src.ingestion import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

NUMERIC_COLUMNS = {
    "ter": [
        "nombre_de_trains_programmes", "nombre_de_trains_ayant_circule",
        "nombre_de_trains_annules", "nombre_de_trains_en_retard_a_l_arrivee",
        "taux_de_regularite", "nombre_de_trains_a_l_heure_pour_un_train_en_retard_a_l_arrivee",
    ],
    "intercites": [
        "nombre_de_trains_programmes", "nombre_de_trains_ayant_circule",
        "nombre_de_trains_annules", "nombre_de_trains_en_retard_a_l_arrivee",
        "taux_de_regularite", "nombre_de_trains_a_l_heure_pour_un_train_en_retard_a_l_arrivee",
    ],
    "tgv_globale": ["regularite_composite", "ponctualite_origine"],
    "tgv_axes": ["regularite_composite", "ponctualite_origine"],
    "tgv_liaisons": [
        "duree_moyenne", "nb_train_prevu", "nb_annulation", "nb_train_depart_retard",
        "retard_moyen_depart", "retard_moyen_tous_trains_depart", "nb_train_retard_arrivee",
        "retard_moyen_arrivee", "retard_moyen_tous_trains_arrivee", "nb_train_retard_sup_15",
        "retard_moyen_trains_retard_sup15", "nb_train_retard_sup_30", "nb_train_retard_sup_60",
        "prct_cause_externe", "prct_cause_infra", "prct_cause_gestion_traffic",
        "prct_cause_materiel_roulant", "prct_cause_gestion_gare", "prct_cause_prise_en_charge_voyageurs",
    ],
}


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-historical-silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _invalid_flag(df, numeric_columns):
    """
    True pour une ligne où au moins une colonne numérique contient une
    valeur non vide qui échoue au cast en double (donnée corrompue,
    différente d'un champ simplement vide -- une valeur vide reste
    considérée comme une donnée manquante normale, pas une erreur).
    """
    conditions = [
        (trim(col(c)).isNotNull()) & (trim(col(c)) != "") & (col(c).cast(DoubleType()).isNull())
        for c in numeric_columns
    ]
    if not conditions:
        return lit(False)
    return reduce(lambda a, b: a | b, conditions)


def run_historical_silver_transform(dataset_key: str) -> None:
    if dataset_key not in NUMERIC_COLUMNS:
        raise ValueError(f"dataset_key inconnu : {dataset_key!r}. Valeurs valides : {list(NUMERIC_COLUMNS)}")

    numeric_columns = NUMERIC_COLUMNS[dataset_key]

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_path = f"{config.DELTA_BRONZE_HISTORICAL_PATH}/{dataset_key}"
    bronze_df = spark.read.format("delta").load(bronze_path)

    flagged_df = bronze_df.withColumn("_is_invalid", _invalid_flag(bronze_df, numeric_columns))
    reject_df = flagged_df.filter(col("_is_invalid")).drop("_is_invalid")
    valid_df = flagged_df.filter(~col("_is_invalid")).drop("_is_invalid")

    select_exprs = []
    for c in valid_df.columns:
        if c == "date":
            select_exprs.append(to_date(concat(col("date"), lit("-01")), "yyyy-MM-dd").alias("period_date"))
            select_exprs.append(col("date").alias("period_label"))
        elif c in numeric_columns:
            # try_cast (pas cast) : renvoie NULL pour une valeur vide au
            # lieu de lever une erreur -- Spark 4.x est en mode ANSI par
            # défaut, où un cast() classique sur '' plante au lieu de
            # renvoyer null comme dans les anciennes versions. Les vraies
            # valeurs corrompues (non vides mais non numériques) ont déjà
            # été isolées dans reject_df juste au-dessus -- try_cast ici
            # ne traite donc plus que le cas légitime des champs vides.
            select_exprs.append(expr(f"try_cast(`{c}` AS DOUBLE)").alias(c))
        else:
            select_exprs.append(col(c))

    silver_df = valid_df.select(*select_exprs).withColumn("silver_processed_at", current_timestamp())

    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    reject_count = reject_df.count()

    output_path = f"{config.DELTA_SILVER_HISTORICAL_PATH}/{dataset_key}"
    silver_df.write.format("delta").mode("overwrite").save(output_path)
    if reject_count > 0:
        reject_path = f"{config.DELTA_SILVER_HISTORICAL_REJECT_PATH}/{dataset_key}"
        reject_df.write.format("delta").mode("overwrite").save(reject_path)

    logger.info(
        "Silver historique [%s] -- %d lignes Bronze -> %d valides (%d rejetées) -> '%s'",
        dataset_key, bronze_count, silver_count, reject_count, output_path,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Transformation Silver historique SNCF")
    parser.add_argument("--dataset", required=True, choices=list(DATASETS), help="Jeu de données à transformer")
    args = parser.parse_args()
    run_historical_silver_transform(args.dataset)


if __name__ == "__main__":
    main()
