"""
Configuration centralisée, chargée depuis les variables d'environnement (.env en local).
"""
import os

from dotenv import load_dotenv

load_dotenv()

SNCF_API_TOKEN = os.getenv("SNCF_API_TOKEN") or None

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "sncf-raw")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))

DELTA_BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", "/tmp/delta/bronze/sncf_raw")
CHECKPOINT_BRONZE_PATH = os.getenv("CHECKPOINT_BRONZE_PATH", "/tmp/delta/checkpoints/bronze_sncf_raw")

DELTA_BRONZE_HISTORICAL_PATH = os.getenv("DELTA_BRONZE_HISTORICAL_PATH", "/tmp/delta/bronze/historical")

DELTA_SILVER_PATH = os.getenv("DELTA_SILVER_PATH", "/tmp/delta/silver/sncf_disruptions")
DELTA_SILVER_REJECT_PATH = os.getenv("DELTA_SILVER_REJECT_PATH", "/tmp/delta/silver/sncf_disruptions_reject")

DELTA_SILVER_HISTORICAL_PATH = os.getenv("DELTA_SILVER_HISTORICAL_PATH", "/tmp/delta/silver/historical")
DELTA_SILVER_HISTORICAL_REJECT_PATH = os.getenv("DELTA_SILVER_HISTORICAL_REJECT_PATH", "/tmp/delta/silver/historical_reject")

DELTA_GOLD_REALTIME_ALERTS_PATH = os.getenv("DELTA_GOLD_REALTIME_ALERTS_PATH", "/tmp/delta/gold/realtime_alerts")
DELTA_GOLD_PUNCTUALITY_TRENDS_PATH = os.getenv("DELTA_GOLD_PUNCTUALITY_TRENDS_PATH", "/tmp/delta/gold/punctuality_trends")
DELTA_GOLD_DISRUPTION_CONTEXT_PATH = os.getenv("DELTA_GOLD_DISRUPTION_CONTEXT_PATH", "/tmp/delta/gold/disruption_context")

# --- Enrichissement LLM (mensuel) ------------------------------------------
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY") or None
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
DELTA_GOLD_MONTHLY_SUMMARIES_PATH = os.getenv(
    "DELTA_GOLD_MONTHLY_SUMMARIES_PATH", "/tmp/delta/gold/monthly_disruption_summaries"
)

# --- Gouvernance ------------------------------------------------------------
DELTA_GOLD_AUDIT_LOG_PATH = os.getenv("DELTA_GOLD_AUDIT_LOG_PATH", "/tmp/delta/gold/audit_log")
DELTA_GOLD_PIPELINE_METRICS_PATH = os.getenv("DELTA_GOLD_PIPELINE_METRICS_PATH", "/tmp/delta/gold/pipeline_metrics")


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
                "sasl.password": KAFKA_SASL_PASSWORD,
            }
        )
    return cfg
