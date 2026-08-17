"""
DAG Airflow -- ingestion_bronze_temps_reel.

Remplace la boucle Python interne de main.py (while True + sleep) par le
planificateur Airflow -- Airflow devient LE mécanisme de répétition, pas
un supplément à côté d'une boucle qui tournerait déjà toute seule. Les
deux scripts appelés existent déjà en mode --once, construits dès le
début du projet pour les tests.

Fréquence : toutes les heures -- choix assumé, moins fréquent que
l'objectif initial du projet ("opérateur alerté en moins de 2 minutes").
Une perturbation nouvelle peut attendre jusqu'à ~59 min avant d'atteindre
Bronze à cette cadence. Compromis délibéré entre fraîcheur et charge
d'exécution (chaque run redémarre une JVM Spark, non négligeable).

Volontairement indépendant du DAG silver_gold_temps_reel (pas de
TriggerDagRunOperator) : Bronze garde son propre rythme, Silver/Gold
garde le sien (4h, décidé pour l'entraînement du futur modèle Isolation
Forest -- voir notes/journal_projet.md).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "franck",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ingestion_bronze_temps_reel",
    description="API SNCF -> Kafka -> Delta Bronze, toutes les heures via Airflow",
    default_args=default_args,
    schedule="0 * * * *",
    start_date=datetime(2026, 8, 17),
    catchup=False,
    max_active_runs=1,
    tags=["sncf", "temps-reel", "bronze"],
) as dag:

    sncf_to_kafka = BashOperator(
        task_id="sncf_to_kafka",
        bash_command=f"cd {PROJECT_DIR} && python3 -m src.ingestion.main --once",
    )

    kafka_to_bronze = BashOperator(
        task_id="kafka_to_bronze",
        bash_command=f"cd {PROJECT_DIR} && python3 -m src.streaming.bronze_ingestion --once",
    )

    sncf_to_kafka >> kafka_to_bronze
