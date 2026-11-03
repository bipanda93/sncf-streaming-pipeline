"""Tests unitaires du producteur Kafka : aucun broker, la classe Producer est remplacée."""
import json

import pytest

from src.ingestion import kafka_producer
from src.ingestion.kafka_producer import SNCFKafkaProducer


class FauxProducer:
    def __init__(self, config):
        self.config = config
        self.messages = []
        self.timeout_flush = None

    def produce(self, topic, key, value, callback):
        self.messages.append({"topic": topic, "key": key, "value": value})

    def poll(self, timeout):
        pass

    def flush(self, timeout):
        self.timeout_flush = timeout
        return 0


@pytest.fixture
def producteur(monkeypatch):
    monkeypatch.setattr(kafka_producer, "Producer", FauxProducer)
    return SNCFKafkaProducer()


def test_enveloppe_contient_les_metadonnees_et_le_payload(producteur):
    producteur.send("sncf-raw", "disruption", "mock", {"id": "abc"})

    message = producteur._producer.messages[0]
    enveloppe = json.loads(message["value"].decode("utf-8"))

    assert message["topic"] == "sncf-raw"
    assert enveloppe["source"] == "mock"
    assert enveloppe["event_type"] == "disruption"
    assert enveloppe["raw"] == {"id": "abc"}
    assert "ingested_at" in enveloppe


def test_la_cle_est_encodee_en_octets_ou_absente(producteur):
    producteur.send("sncf-raw", "disruption", "mock", {}, key="ligne-A")
    producteur.send("sncf-raw", "disruption", "mock", {})

    avec_cle, sans_cle = producteur._producer.messages
    assert avec_cle["key"] == b"ligne-A"
    assert sans_cle["key"] is None


def test_flush_transmet_le_timeout_et_renvoie_le_resultat(producteur):
    assert producteur.flush(timeout=3.0) == 0
    assert producteur._producer.timeout_flush == 3.0