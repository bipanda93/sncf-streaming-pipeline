"""
DAG Airflow -- silver_gold_temps_reel.

Silver -> Gold (alertes + contexte) -> métriques -> détection d'anomalies
-> export Power BI, toutes les 4h.

Les deux tâches pipeline_metrics/anomaly_detector ont été ajoutées le
21/08 -- accumulent l'historique nécessaire à la détection d'anomalies
(voir notes/decisions_architecture.md, section "IA & enrichissement").
Isolation Forest ne s'activera réellement qu'après ~50 exécutions
accumulées.

export_powerbi ajoutée le 31/08 -- jusque-là lancée manuellement à chaque
fois (voir notes/incidents_2026-08-*.md). Placée en tout dernier : elle
lit les tables Gold, doit donc s'exécuter après leur recalcul, pas avant.
Ne couvre que la génération des fichiers Parquet locaux -- le rechargement
dans Fabric (Files -> Load to Tables) reste manuel, bloqué par la
permission Service Principal (voir board Trello, "Problèmes et
requêtes").
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
    description="Silver -> Gold -> metriques -> detection d'anomalies -> export PowerBI",
    default_args=default_args,
    schedule="0 */4 * * *",
    start_date=datetime(2026, 8, 17),
    catchup=False,
    tags=["sncf", "silver", "gold", "monitoring", "powerbi"],
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
    export_powerbi = BashOperator(
        task_id="export_powerbi",
        bash_command=f"cd {PROJECT_DIR} && python3 -m src.monitoring.export_for_powerbi",
    )

    silver_transform >> gold_realtime_alerts >> gold_disruption_context >> pipeline_metrics >> anomaly_detector >> export_powerbi
