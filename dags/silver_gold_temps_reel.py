"""
DAG Airflow -- silver_gold_temps_reel.

Silver -> Gold (alertes + contexte) -> métriques -> détection d'anomalies,
toutes les 4h.

Les deux dernières tâches (pipeline_metrics, anomaly_detector) ont été
ajoutées le 21/08 -- accumulent l'historique nécessaire à la détection
d'anomalies (voir notes/decisions_architecture.md, section "IA &
enrichissement"). Isolation Forest ne s'activera réellement qu'après
~50 exécutions accumulées (à 6 runs/jour, environ 8-9 jours) -- avant ce
seuil, seuls les seuils statistiques s'appliquent, comportement attendu et
documenté, pas un dysfonctionnement.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "franck",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="silver_gold_temps_reel",
    description="Silver -> Gold -> metriques -> detection d'anomalies",
    default_args=default_args,
    schedule="0 */4 * * *",
    start_date=datetime(2026, 8, 17),
    catchup=False,
    tags=["sncf", "silver", "gold", "monitoring"],
) as dag:

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=f"cd {PROJECT_DIR} && python3 -m src.streaming.silver_transform",
    )
    gold_realtime_alerts = BashOperator(
        task_id="gold_realtime_alerts",
        bash_command=f"cd {PROJECT_DIR} && python3 -m src.streaming.gold_realtime_alerts",
    )
    gold_disruption_context = BashOperator(
        task_id="gold_disruption_context",
        bash_command=f"cd {PROJECT_DIR} && python3 -m src.streaming.gold_disruption_context",
    )
    pipeline_metrics = BashOperator(
        task_id="pipeline_metrics",
        bash_command=f"cd {PROJECT_DIR} && python3 -m src.monitoring.pipeline_metrics",
    )
    anomaly_detector = BashOperator(
        task_id="anomaly_detector",
        bash_command=f"cd {PROJECT_DIR} && python3 -m src.monitoring.anomaly_detector",
    )

    silver_transform >> gold_realtime_alerts >> gold_disruption_context >> pipeline_metrics >> anomaly_detector
