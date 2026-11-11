"""Tests unitaires de l'alerte Slack : aucun appel réseau."""
import json

from src.monitoring import alerte_slack


class FauxTI:
    dag_id = "ingestion_bronze_temps_reel"
    task_id = "sncf_to_kafka"
    run_id = "scheduled__2026-11-11"
    try_number = 3


def test_le_message_contient_dag_tache_et_lien():
    message = alerte_slack.construire_message({"task_instance": FauxTI()})
    assert "ingestion_bronze_temps_reel" in message
    assert "sncf_to_kafka" in message
    assert "/dags/ingestion_bronze_temps_reel/runs/" in message


def test_sans_webhook_rien_n_est_envoye(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    appels = []
    monkeypatch.setattr(alerte_slack.urllib.request, "urlopen", lambda *a, **k: appels.append(a))
    alerte_slack.alerter_slack({"task_instance": FauxTI()})
    assert appels == []


def test_le_webhook_recoit_le_message(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/x")
    recus = []

    class Reponse:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def faux_urlopen(requete, timeout):
        recus.append(json.loads(requete.data))
        return Reponse()

    monkeypatch.setattr(alerte_slack.urllib.request, "urlopen", faux_urlopen)
    alerte_slack.alerter_slack({"task_instance": FauxTI()})
    assert "sncf_to_kafka" in recus[0]["text"]


def test_une_panne_de_slack_ne_fait_pas_echouer(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/x")

    def panne(*a, **k):
        raise OSError("réseau coupé")

    monkeypatch.setattr(alerte_slack.urllib.request, "urlopen", panne)
    alerte_slack.alerter_slack({"task_instance": FauxTI()})