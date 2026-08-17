"""
DAG Airflow -- historique_mensuel.

Pendant historique du DAG ingestion_bronze_temps_reel : orchestre les 5
jeux de régularité SNCF (chargement + typage Silver), puis leur unification
en gold_punctuality_trends une fois les 5 prêts.

Fréquence : le 1er de chaque mois, 3h du matin -- SNCF publie ses données
de régularité mensuellement ; horaire creux pour éviter toute collision
avec les DAGs temps réel qui tournent en continu.

Chaque jeu suit sa propre chaîne load -> silver, en parallèle des 4
autres -- convergence uniquement au niveau Gold, qui a besoin des 5 pour
produire une vision unifiée (voir gold_punctuality_trends.py, motif
scope_type/scope_value).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
DATASETS = ["ter", "intercites", "tgv_globale", "tgv_axes", "tgv_liaisons"]

default_args = {
    "owner": "franck",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="historique_mensuel",
    description="Chargement + Silver + Gold pour les 5 jeux de régularité SNCF",
    default_args=default_args,
    schedule="0 3 1 * *",
    start_date=datetime(2026, 8, 17),
    catchup=False,
    tags=["sncf", "historique", "mensuel"],
) as dag:

    gold_task = BashOperator(
        task_id="gold_punctuality_trends",
        bash_command=f"cd {PROJECT_DIR} && python3 -m src.historical.gold_punctuality_trends",
    )

    for dataset in DATASETS:
        load_task = BashOperator(
            task_id=f"load_{dataset}",
            bash_command=f"cd {PROJECT_DIR} && python3 -m src.historical.historical_loader --dataset {dataset}",
        )
        silver_task = BashOperator(
            task_id=f"silver_{dataset}",
            bash_command=f"cd {PROJECT_DIR} && python3 -m src.historical.historical_silver_transform --dataset {dataset}",
        )
        load_task >> silver_task >> gold_task
