"""
Détection d'anomalies sur les métriques de pipeline (voir
pipeline_metrics.py) -- deux niveaux, choisis selon le volume d'historique
disponible par table (décision du 18/08, voir
notes/decisions_architecture.md, section "IA & enrichissement").

Niveau 1 -- seuils statistiques (z-score) : utilisable dès qu'il existe au
moins MIN_SAMPLES_STATS points d'historique pour une table. Compare le
dernier point à la moyenne/écart-type des points précédents.

Niveau 2 -- Isolation Forest : ne s'active qu'à partir de
MIN_SAMPLES_ISOLATION_FOREST points -- avant ce seuil, retombe
automatiquement sur le niveau 1 seul. Le modèle est ajusté à chaque
exécution sur l'historique des métriques (il n'est pas conservé entre deux
runs).

Chaque exécution enregistre un verdict par table dans la table Gold
anomaly_results (voir resultats_anomalies.py), lue par le tableau de bord
Streamlit. Si cet enregistrement échoue, le verdict reste dans les logs et
la tâche n'échoue pas.

Usage :
    python -m src.monitoring.anomaly_detector
"""
import logging

import numpy as np
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.ingestion import config
from src.monitoring.resultats_anomalies import (
    MIN_SAMPLES_ISOLATION_FOREST,
    MIN_SAMPLES_STATS,
    construire_resultat,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

Z_SCORE_THRESHOLD = 2.5

ANOMALY_RESULTS_SCHEMA = StructType([
    StructField("table_name", StringType(), False),
    StructField("metrics_executed_at", TimestampType(), False),
    StructField("detected_at", TimestampType(), False),
    StructField("n_history", IntegerType(), False),
    StructField("level", StringType(), False),
    StructField("is_anomaly_stat", BooleanType(), False),
    StructField("is_anomaly_if", BooleanType(), True),
    StructField("if_score", DoubleType(), True),
])


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-anomaly-detector")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def check_statistical_thresholds(history: np.ndarray, latest: np.ndarray, feature_names: list) -> list:
    """Z-score sur chaque feature -- retourne la liste des features anormales."""
    anomalies = []
    means = history.mean(axis=0)
    stds = history.std(axis=0)

    for i, name in enumerate(feature_names):
        if stds[i] == 0:
            continue
        z = abs((latest[i] - means[i]) / stds[i])
        if z > Z_SCORE_THRESHOLD:
            anomalies.append(f"{name} (z-score={z:.2f}, valeur={latest[i]:.3f}, moyenne historique={means[i]:.3f})")

    return anomalies


def check_isolation_forest(history: np.ndarray, latest: np.ndarray) -> tuple[bool, float]:
    """Retourne (est_anormal, score) : un score négatif signifie « anormal »."""
    from sklearn.ensemble import IsolationForest

    model = IsolationForest(contamination=0.1, random_state=42)
    model.fit(history)
    score = float(model.decision_function(latest.reshape(1, -1))[0])
    return score < 0, score


def enregistrer_resultats(spark, resultats: list) -> None:
    """Ajoute les verdicts à la table Gold anomaly_results, sans jamais faire échouer la tâche."""
    if not resultats:
        return
    try:
        lignes = [tuple(r[nom] for nom in ANOMALY_RESULTS_SCHEMA.fieldNames()) for r in resultats]
        df = spark.createDataFrame(lignes, schema=ANOMALY_RESULTS_SCHEMA)
        df.write.format("delta").mode("append").save(config.DELTA_GOLD_ANOMALY_RESULTS_PATH)
    except Exception:  # le verdict reste dans les logs, on ne casse pas le DAG
        logger.exception("Enregistrement des verdicts impossible -- ignoré pour ce run.")


def run_anomaly_detection() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(config.DELTA_GOLD_PIPELINE_METRICS_PATH)
    pdf = df.toPandas()

    resultats = []
    for table_name in pdf["table_name"].unique():
        table_df = pdf[pdf["table_name"] == table_name].sort_values("executed_at")
        n_samples = len(table_df)

        if n_samples < 2:
            logger.info("%s -- 1 seul point d'historique, rien à comparer.", table_name)
            continue

        features = table_df[["row_count", "null_rate"]].fillna(0).values
        history = features[:-1]
        latest = features[-1]

        if n_samples - 1 < MIN_SAMPLES_STATS:
            logger.info(
                "%s -- %d point(s) d'historique (< %d requis) -- pas assez de données pour une détection fiable.",
                table_name, n_samples - 1, MIN_SAMPLES_STATS,
            )
            continue

        stat_anomalies = check_statistical_thresholds(history, latest, ["row_count", "null_rate"])

        resultat_if = None
        if n_samples - 1 >= MIN_SAMPLES_ISOLATION_FOREST:
            resultat_if = check_isolation_forest(history, latest)
            logger.info(
                "%s -- Isolation Forest actif (%d points d'historique) -- anomalie=%s (score=%.3f)",
                table_name, n_samples - 1, resultat_if[0], resultat_if[1],
            )
        else:
            logger.info(
                "%s -- Isolation Forest inactif (%d/%d points requis) -- seuils statistiques uniquement.",
                table_name, n_samples - 1, MIN_SAMPLES_ISOLATION_FOREST,
            )

        resultats.append(construire_resultat(
            table_name,
            table_df["executed_at"].iloc[-1].to_pydatetime(),
            n_samples - 1,
            stat_anomalies,
            resultat_if,
        ))

        if stat_anomalies:
            logger.warning("%s -- ANOMALIE(S) DÉTECTÉE(S) : %s", table_name, "; ".join(stat_anomalies))
        else:
            logger.info("%s -- aucune anomalie détectée (niveau statistique).", table_name)

    enregistrer_resultats(spark, resultats)


if __name__ == "__main__":
    run_anomaly_detection()