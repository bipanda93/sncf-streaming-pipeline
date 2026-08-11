"""
Configuration centralisée, chargée depuis les variables d'environnement (.env en local).

Le même code fonctionne dans deux contextes sans modification :
- Développement local : Kafka en PLAINTEXT via Docker Compose
- Production Azure    : Event Hubs en SASL_SSL (protocole Kafka natif d'Event Hubs)
Seule la configuration change (variables d'environnement), jamais le code applicatif.
"""
import os

from dotenv import load_dotenv

load_dotenv()

# --- SNCF API -----------------------------------------------------------
SNCF_API_TOKEN = os.getenv("SNCF_API_TOKEN") or None

# --- Kafka / Event Hubs --------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "sncf-raw")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))

# --- Delta Lake / Streaming (temps réel) ----------------------------------
DELTA_BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", "/tmp/delta/bronze/sncf_raw")
CHECKPOINT_BRONZE_PATH = os.getenv("CHECKPOINT_BRONZE_PATH", "/tmp/delta/checkpoints/bronze_sncf_raw")

# --- Delta Lake / Historique (batch) --------------------------------------
DELTA_BRONZE_HISTORICAL_PATH = os.getenv("DELTA_BRONZE_HISTORICAL_PATH", "/tmp/delta/bronze/historical")


def build_kafka_config() -> dict:
    """
    Construit la config confluent-kafka.

    - Local (Docker Compose)  : security.protocol=PLAINTEXT, pas d'auth.
    - Azure Event Hubs        : security.protocol=SASL_SSL, username fixe
      "$ConnectionString" et password = la connection string Event Hubs.
    """
    cfg = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
        "client.id": "sncf-ingestion-producer",
    }
    if KAFKA_SECURITY_PROTOCOL.startswith("SASL"):
        cfg.update(
            {
                "sasl.mechanism": KAFKA_SASL_MECHANISM or "PLAIN",
                "sasl.username": KAFKA_SASL_USERNAME or "$ConnectionString",
                "sasl.password": KAFKA_SASL_PASSWORD,
            }
        )
    return cfg
