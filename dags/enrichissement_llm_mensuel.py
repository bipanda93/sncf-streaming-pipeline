"""
DAG Airflow -- enrichissement_llm_mensuel.

Wrapper autour de delay_analyzer.py (déjà écrit et testé manuellement) --
génère un résumé en une phrase par perturbation du mois précédent, via
Claude API (Haiku).

Fréquence : le 1er de chaque mois, 4h du matin -- décalé d'une heure après
historique_mensuel (3h) pour éviter deux jobs Spark lourds simultanés sur
le même conteneur (une seule JVM à la fois, ressources limitées en local).

Pas de dépendance avec historique_mensuel malgré l'horaire proche : ce
DAG lit Silver TEMPS RÉEL (perturbations), pas les données de régularité
-- deux sources et deux tables Silver complètement différentes.

Aucun argument --month : delay_analyzer.py calcule par défaut le mois
précédent (UTC) -- comportement correct pour un run mensuel en
production. Contrairement aux tests manuels précédents (--month 2026-08
explicite), ce DAG utilisera automatiquement le bon mois à chaque
exécution réelle, sans jamais avoir besoin d'être modifié.

Mode mock automatique tant qu'aucune clé ANTHROPIC_API_KEY n'est
configurée dans .env -- ClaudeClient bascule seul en mock si la clé est
absente (voir src/enrichment/claude_client.py), donc ce DAG ne consomme
aucun coût réel tant que la clé n'est pas ajoutée délibérément.
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
    dag_id="enrichissement_llm_mensuel",
    description="Resume LLM (Claude Haiku) des perturbations du mois precedent",
    default_args=default_args,
    schedule="0 4 1 * *",
    start_date=datetime(2026, 8, 17),
    catchup=False,
    tags=["sncf", "llm", "mensuel"],
) as dag:

    delay_analyzer = BashOperator(
        task_id="delay_analyzer",
        bash_command=f"cd {PROJECT_DIR} && python3 -m src.enrichment.delay_analyzer",
    )
