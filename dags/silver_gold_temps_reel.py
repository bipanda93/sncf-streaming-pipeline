"""
DAG Airflow -- silver_gold_temps_reel.

Orchestre la chaîne batch en aval du flux Kafka continu (main.py,
bronze_ingestion.py tournent déjà en boucle propre, hors Airflow) :
Silver -> Gold (2 tables temps réel).

Fréquence : toutes les 4h -- décision documentée dans
notes/journal_projet.md ("DÉCISION -- Ajout d'une profondeur IA"), calibrée
pour accumuler assez de runs (~672 d'ici décembre) avant l'entraînement
d'un modèle Isolation Forest de détection d'anomalies (tâche à ajouter une
fois ce module construit).

Chaque tâche invoque la même commande CLI déjà testée manuellement tout
au long du projet, via BashOperator plutôt qu'un import direct des
fonctions Python -- évite les conflits de cycle de vie SparkSession entre
tâches séquentielles dans le même processus Airflow.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "franck",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_gold_temps_reel",
    description="Silver -> Gold (alertes temps reel + contexte) sur le flux SNCF",
    default_args=default_args,
    schedule="0 */4 * * *",
    start_date=datetime(2026, 8, 17),
    catchup=False,
    tags=["sncf", "temps-reel", "gold"],
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

    silver_transform >> gold_realtime_alerts >> gold_disruption_context
