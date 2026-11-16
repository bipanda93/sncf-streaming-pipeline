"""Construction des lignes de verdict de la détection d'anomalies.

Logique pure, sans Spark ni numpy, pour rester testable dans la CI
(voir anomaly_detector.py, qui l'utilise et enregistre les verdicts).
"""
from datetime import UTC, datetime

MIN_SAMPLES_STATS = 5
MIN_SAMPLES_ISOLATION_FOREST = 50

NIVEAU_STATISTIQUE = "statistique"
NIVEAU_ISOLATION_FOREST = "isolation_forest"


def construire_resultat(
    table_name,
    metrics_executed_at,
    n_history,
    stat_anomalies,
    resultat_if=None,
    detected_at=None,
):
    """Une ligne de verdict pour une table et une exécution des métriques.

    resultat_if : (est_anormal, score), ou None si l'Isolation Forest est inactif.
    """
    est_anormal_if, score = resultat_if if resultat_if is not None else (None, None)
    return {
        "table_name": table_name,
        "metrics_executed_at": metrics_executed_at,
        "detected_at": detected_at or datetime.now(UTC),
        "n_history": n_history,
        "level": NIVEAU_ISOLATION_FOREST if resultat_if is not None else NIVEAU_STATISTIQUE,
        "is_anomaly_stat": bool(stat_anomalies),
        "is_anomaly_if": est_anormal_if,
        "if_score": score,
    }
