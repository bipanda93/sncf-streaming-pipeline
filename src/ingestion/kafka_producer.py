"""
Producteur Kafka pour le topic sncf-raw.

Conçu pour être portable entre :
- Kafka local (Docker Compose, PLAINTEXT) pendant le développement
- Azure Event Hubs (SASL_SSL, protocole Kafka natif) en production

Seule la config change (config.py), jamais ce fichier.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer

from . import config

logger = logging.getLogger(__name__)


def _delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Échec de livraison Kafka : %s", err)
    else:
        logger.debug(
            "Message livré -> topic=%s partition=%s offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


class SNCFKafkaProducer:
    def __init__(self):
        self._producer = Producer(config.build_kafka_config())

    def send(
        self,
        topic: str,
        event_type: str,
        source: str,
        payload: dict,
        key: Optional[str] = None,
    ) -> None:
        """
        Publie un événement enveloppé (metadata + payload brut) sur Kafka.

        L'enveloppe correspond au principe de la couche Bronze : données
        brutes horodatées, aucune transformation à ce stade -- elle aura
        lieu en Silver (dédoublonnage, nettoyage, typage).
        """
        envelope = {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source": source,  # "sncf_api" ou "mock"
            "event_type": event_type,  # "disruption", "departure", ...
            "raw": payload,
        }
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key else None,
            value=json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
            callback=_delivery_report,
        )
        self._producer.poll(0)  # déclenche les callbacks en attente, non bloquant

    def flush(self, timeout: float = 10.0) -> int:
        """À appeler avant de quitter le programme pour vider le buffer d'envoi."""
        return self._producer.flush(timeout)
