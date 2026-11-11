"""Alerte Slack quand une tâche Airflow échoue (après ses nouvelles tentatives).

Branchée via on_failure_callback dans default_args. Elle ne doit jamais faire
échouer le pipeline : si Slack ou le webhook manque, on journalise et on continue.
"""
import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)


def construire_message(context):
    ti = context.get("task_instance")
    dag_id = getattr(ti, "dag_id", "inconnu")
    task_id = getattr(ti, "task_id", "inconnue")
    run_id = getattr(ti, "run_id", "inconnu")
    essai = getattr(ti, "try_number", "?")
    base = os.environ.get("AIRFLOW_UI_URL", "http://localhost:8081")
    lien = f"{base}/dags/{dag_id}/runs/{run_id}/tasks/{task_id}"
    return f"Échec Airflow : DAG {dag_id}, tâche {task_id} (essai {essai}). Logs : {lien}"


def alerter_slack(context):
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        logger.warning("SLACK_WEBHOOK_URL absent : alerte non envoyée")
        return
    try:
        requete = urllib.request.Request(
            webhook,
            data=json.dumps({"text": construire_message(context)}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(requete, timeout=10):
            pass
    except Exception:
        logger.exception("Envoi de l'alerte Slack impossible")