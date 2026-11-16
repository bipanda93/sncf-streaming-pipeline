"""Tests de src/monitoring/resultats_anomalies.py -- logique pure, sans Spark."""
from datetime import UTC, datetime

from src.monitoring.resultats_anomalies import construire_resultat

T = datetime(2026, 11, 16, 8, 0, tzinfo=UTC)


def test_niveau_statistique_quand_isolation_forest_inactif():
    r = construire_resultat("gold_realtime_alerts", T, 12, [], None)
    assert r["level"] == "statistique"
    assert r["is_anomaly_if"] is None
    assert r["if_score"] is None
    assert r["is_anomaly_stat"] is False


def test_isolation_forest_actif_porte_verdict_et_score():
    r = construire_resultat("gold_realtime_alerts", T, 131, [], (True, -0.12))
    assert r["level"] == "isolation_forest"
    assert r["is_anomaly_if"] is True
    assert r["if_score"] == -0.12


def test_anomalie_statistique_signalee():
    r = construire_resultat("gold_disruption_context", T, 20, ["row_count (z-score=3.10)"], None)
    assert r["is_anomaly_stat"] is True
