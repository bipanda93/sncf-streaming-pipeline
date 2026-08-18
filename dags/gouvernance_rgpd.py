"""
DAG Airflow -- gouvernance_rgpd.

Purge Bronze temps réel selon la politique de rétention (voir
notes/politique_retention.md), trace sa propre exécution dans
gold_audit_log.

Fréquence : mensuelle, 1er du mois, 5h -- après historique_mensuel (3h)
et enrichissement_llm_mensuel (4h), pour éviter toute collision de
ressources sur le conteneur local (une seule JVM Spark à la fois).

Périmètre assumé (voir notes/decisions_architecture.md et
notes/politique_retention.md) : aucune donnée de ce projet n'est une
donnée à caractère personnel au sens RGPD -- ce DAG applique les
principes de minimisation et de limitation de conservation par
discipline d'ingénierie, pas par obligation légale stricte sur ces
données précises.
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
    dag_id="gouvernance_rgpd",
    description="Purge Bronze selon politique de retention, trace l'execution",
    default_args=default_args,
    schedule="0 5 1 * *",
    start_date=datetime(2026, 8, 18),
    catchup=False,
    tags=["sncf", "gouvernance", "mensuel"],
) as dag:

    retention_purge = BashOperator(
        task_id="retention_purge",
        bash_command=f"cd {PROJECT_DIR} && python3 -m src.governance.retention_purge",
    )
