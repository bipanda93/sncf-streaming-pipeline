"""
DAG Airflow -- pipeline_complet_manuel.

Déclenchement MANUEL uniquement (schedule=None -- ne tourne JAMAIS tout
seul). Les 4 DAGs qu'il orchestre gardent chacun leur propre planning
automatique (horaire, 4h, mensuel...), choisi pour de vraies raisons
documentées dans le journal -- ce DAG ne les remplace pas, il offre un
point d'entrée unique pour les lancer tous à la demande (démonstration
soutenance, test de bout en bout, rattrapage manuel).

Point de vigilance : deferrable=False forcé explicitement sur chaque
TriggerDagRunOperator -- un bug documenté d'Airflow 3.x fait que
wait_for_completion=True combiné à deferrable=True peut laisser une tâche
bloquée indéfiniment en état "deferred", sans jamais échouer ni réussir.

Exécution strictement séquentielle (pas en parallèle) : l'environnement
local ne dispose que d'une seule JVM Spark à la fois -- lancer plusieurs
jobs Spark en parallèle épuiserait les ressources du conteneur.

Usage :
    docker-compose exec airflow airflow dags trigger pipeline_complet_manuel
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="pipeline_complet_manuel",
    description="Declenche les 4 DAGs en sequence, uniquement a la demande",
    schedule=None,
    start_date=datetime(2026, 8, 17),
    catchup=False,
    tags=["sncf", "manuel", "demo"],
) as dag:

    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion_bronze",
        trigger_dag_id="ingestion_bronze_temps_reel",
        wait_for_completion=True,
        deferrable=False,
        poke_interval=15,
    )
    trigger_silver_gold = TriggerDagRunOperator(
        task_id="trigger_silver_gold",
        trigger_dag_id="silver_gold_temps_reel",
        wait_for_completion=True,
        deferrable=False,
        poke_interval=15,
    )
    trigger_historique = TriggerDagRunOperator(
        task_id="trigger_historique",
        trigger_dag_id="historique_mensuel",
        wait_for_completion=True,
        deferrable=False,
        poke_interval=15,
    )
    trigger_llm = TriggerDagRunOperator(
        task_id="trigger_llm",
        trigger_dag_id="enrichissement_llm_mensuel",
        wait_for_completion=True,
        deferrable=False,
        poke_interval=15,
    )

    trigger_ingestion >> trigger_silver_gold >> trigger_historique >> trigger_llm
