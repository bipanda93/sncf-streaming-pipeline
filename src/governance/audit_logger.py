"""
Utilitaire de traçabilité -- enregistre l'exécution des jobs de
gouvernance dans une table Delta dédiée (gold_audit_log).

Rôle : rendre la conformité démontrable plutôt qu'affirmée -- le principe
de responsabilité (RGPD Art. 5, "accountability") demande de pouvoir
prouver ce qui a été fait, pas seulement l'affirmer dans un document.

Portée actuelle : utilisé par les jobs de gouvernance (retention_purge).
Extension possible non implémentée : les 4 DAGs de pipeline existants
pourraient adopter le même mécanisme -- noté comme piste, pas fait à ce
stade pour ne pas modifier des scripts déjà testés et stables.
"""
import logging
from datetime import datetime, timezone

from pyspark.sql import Row

from src.ingestion import config

logger = logging.getLogger(__name__)


def log_job_execution(spark, job_name, source_table, target_table, rows_processed, status="success", details=None):
    """Ajoute une ligne à gold_audit_log. Crée la table au premier appel."""
    row = Row(
        job_name=job_name,
        executed_at=datetime.now(timezone.utc),
        source_table=source_table,
        target_table=target_table,
        rows_processed=int(rows_processed),
        status=status,
        details=details or "",
    )
    audit_df = spark.createDataFrame([row])

    try:
        spark.read.format("delta").load(config.DELTA_GOLD_AUDIT_LOG_PATH).limit(1).count()
        table_exists = True
    except Exception:
        table_exists = False

    mode = "append" if table_exists else "overwrite"
    audit_df.write.format("delta").mode(mode).save(config.DELTA_GOLD_AUDIT_LOG_PATH)

    logger.info("Audit log -- %s : %d ligne(s), statut=%s", job_name, rows_processed, status)
