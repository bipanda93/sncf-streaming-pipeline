"""
Point d'entrée de l'ingestion SNCF -> Kafka.

Usage :
    python -m src.ingestion.main            # boucle continue (toutes les POLL_INTERVAL_SECONDS)
    python -m src.ingestion.main --once      # une seule itération (pour tests / CI)
"""
import argparse
import logging
import time

from . import config
from .kafka_producer import SNCFKafkaProducer
from .sncf_client import SNCFClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run_once(client: SNCFClient, producer: SNCFKafkaProducer) -> int:
    """Exécute un cycle complet de collecte + publication.

    Retourne le nombre d'événements envoyés (utile pour les tests).
    """
    disruptions = client.get_disruptions()
    source = "mock" if client.mock_mode else "sncf_api"

    for disruption in disruptions:
        producer.send(
            topic=config.TOPIC_RAW,
            event_type="disruption",
            source=source,
            payload=disruption,
            key=disruption.get("id"),
        )

    producer.flush()
    logger.info(
        "%d perturbation(s) publiée(s) sur le topic '%s' (source=%s)",
        len(disruptions),
        config.TOPIC_RAW,
        source,
    )
    return len(disruptions)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion SNCF Open Data -> Kafka")
    parser.add_argument("--once", action="store_true", help="Une seule itération puis quitte")
    args = parser.parse_args()

    client = SNCFClient(token=config.SNCF_API_TOKEN)
    producer = SNCFKafkaProducer()

    if args.once:
        run_once(client, producer)
        return

    logger.info(
        "Démarrage de la boucle d'ingestion (intervalle = %ss)",
        config.POLL_INTERVAL_SECONDS,
    )
    while True:
        try:
            run_once(client, producer)
        except Exception:
            logger.exception(
                "Erreur pendant le cycle d'ingestion -- on continue à la prochaine itération."
            )
        time.sleep(config.POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
