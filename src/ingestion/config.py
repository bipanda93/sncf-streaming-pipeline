"""
Configuration centralisée, chargée depuis les variables d'environnement (.env en local).
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SNCF_API_TOKEN = os.getenv("SNCF_API_TOKEN") or None

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
def _get_secret_from_keyvault(secret_name: str) -> str:
    """
    Récupère un secret depuis Azure Key Vault (authentification via
    DefaultAzureCredential -- réutilise la session `az login` locale,
    ou l'identité managée une fois déployé sur AKS).
    """
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    vault_name = os.getenv("AZURE_KEY_VAULT_NAME", "kv-sncf-dev")
    vault_uri = f"https://{vault_name}.vault.azure.net"
    credential = DefaultAzureCredential(process_timeout=30)
    client = SecretClient(vault_url=vault_uri, credential=credential)
    return client.get_secret(secret_name).value

if KAFKA_SECURITY_PROTOCOL.startswith("SASL"):
    KAFKA_SASL_PASSWORD = _get_secret_from_keyvault("eventhub-listen-connection-string")
    KAFKA_SASL_PASSWORD_SEND = _get_secret_from_keyvault("eventhub-connection-string")
else:
    KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
    KAFKA_SASL_PASSWORD_SEND = os.getenv("KAFKA_SASL_PASSWORD", "")

TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "sncf-raw")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))

# --- Stockage Delta Lake -----------------------------------------------------
# INCIDENT DU 21/08 (voir notes/incidents_2026-08-21.md) : tout était stocké
# sous /tmp/delta -- macOS vide intégralement /tmp à chaque redémarrage du
# Mac, ce qui a effacé silencieusement plusieurs jours de données accumulées
# (Bronze, Silver, Gold, métriques). C'est la cause racine probable du faible
# volume d'historique observé pour Isolation Forest toute la semaine.
#
# Correction : chemin de base calculé depuis l'emplacement de ce fichier
# (racine du projet + data/delta), jamais sous /tmp -- fonctionne
# identiquement sur l'hôte (Mac) et dans le conteneur Airflow, puisque tout
# le dossier du projet est déjà monté dans le conteneur
# (voir docker-compose.yml, ./:/opt/airflow/project).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DELTA_BASE_PATH = os.getenv("DELTA_BASE_PATH", str(_PROJECT_ROOT / "data" / "delta"))

DELTA_BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", f"{DELTA_BASE_PATH}/bronze/sncf_raw")
CHECKPOINT_BRONZE_PATH = os.getenv("CHECKPOINT_BRONZE_PATH", f"{DELTA_BASE_PATH}/checkpoints/bronze_sncf_raw")

DELTA_BRONZE_HISTORICAL_PATH = os.getenv("DELTA_BRONZE_HISTORICAL_PATH", f"{DELTA_BASE_PATH}/bronze/historical")

DELTA_SILVER_PATH = os.getenv("DELTA_SILVER_PATH", f"{DELTA_BASE_PATH}/silver/sncf_disruptions")
DELTA_SILVER_REJECT_PATH = os.getenv("DELTA_SILVER_REJECT_PATH", f"{DELTA_BASE_PATH}/silver/sncf_disruptions_reject")

DELTA_SILVER_HISTORICAL_PATH = os.getenv("DELTA_SILVER_HISTORICAL_PATH", f"{DELTA_BASE_PATH}/silver/historical")
DELTA_SILVER_HISTORICAL_REJECT_PATH = os.getenv(
    "DELTA_SILVER_HISTORICAL_REJECT_PATH", f"{DELTA_BASE_PATH}/silver/historical_reject"
)

DELTA_GOLD_REALTIME_ALERTS_PATH = os.getenv("DELTA_GOLD_REALTIME_ALERTS_PATH", f"{DELTA_BASE_PATH}/gold/realtime_alerts")
DELTA_GOLD_PUNCTUALITY_TRENDS_PATH = os.getenv(
    "DELTA_GOLD_PUNCTUALITY_TRENDS_PATH", f"{DELTA_BASE_PATH}/gold/punctuality_trends"
)
DELTA_GOLD_DISRUPTION_CONTEXT_PATH = os.getenv(
    "DELTA_GOLD_DISRUPTION_CONTEXT_PATH", f"{DELTA_BASE_PATH}/gold/disruption_context"
)

# --- Enrichissement LLM (mensuel) ------------------------------------------
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY") or None
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
DELTA_GOLD_MONTHLY_SUMMARIES_PATH = os.getenv(
    "DELTA_GOLD_MONTHLY_SUMMARIES_PATH", f"{DELTA_BASE_PATH}/gold/monthly_disruption_summaries"
)

# --- Gouvernance ------------------------------------------------------------
DELTA_GOLD_AUDIT_LOG_PATH = os.getenv("DELTA_GOLD_AUDIT_LOG_PATH", f"{DELTA_BASE_PATH}/gold/audit_log")
DELTA_GOLD_PIPELINE_METRICS_PATH = os.getenv(
    "DELTA_GOLD_PIPELINE_METRICS_PATH", f"{DELTA_BASE_PATH}/gold/pipeline_metrics"
)


def build_kafka_config() -> dict:
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
                "sasl.password": KAFKA_SASL_PASSWORD_SEND,
            }
        )
    return cfg
