"""Silver : lecture du JSON Bronze, zone de rejet et déduplication par perturbation."""
import json

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql.functions import col, date_format
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.streaming.silver_transform import (
    deduplicate_latest,
    flatten_bronze,
    split_valid_reject,
)

BRONZE_SCHEMA = StructType([
    StructField("raw_payload", StringType()),
    StructField("source", StringType()),
    StructField("kafka_key", StringType()),
    StructField("offset", LongType()),
    StructField("bronze_ingested_at", StringType()),
])


def _payload(disruption_id="d1", status="active", updated_at="20261120T083758", **extra):
    base = {
        "id": disruption_id,
        "status": status,
        "updated_at": updated_at,
        "cause": "travaux",
        "severity": {"name": "Perturbation", "effect": "SIGNIFICANT_DELAYS", "priority": 1},
        "messages": [{"text": "Trains retardés"}, {"text": "second message"}],
        "application_periods": [{"begin": "20261120T080000", "end": "20261120T180000"}],
        "impacted_objects": [],
        "contributor": "shortterm.tn_sncf",
    }
    base.update(extra)
    return json.dumps(base)


def _bronze(spark, lignes):
    """lignes : liste de (raw_payload, offset)."""
    rows = [(raw, "sncf", "k", offset, "2026-11-20T08:40:00") for raw, offset in lignes]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def _silver(spark, lignes):
    valid, _ = split_valid_reject(flatten_bronze(_bronze(spark, lignes)))
    return deduplicate_latest(valid)


# --- lecture / aplatissement -------------------------------------------------

def test_flatten_extrait_les_champs_utiles(spark):
    flat = flatten_bronze(_bronze(spark, [(_payload(), 1)])).first()
    assert flat["disruption_id"] == "d1"
    assert flat["status"] == "active"
    assert flat["cause"] == "travaux"
    assert flat["severity_effect"] == "SIGNIFICANT_DELAYS"
    assert flat["message_text"] == "Trains retardés"  # premier message seulement


def test_flatten_convertit_les_heures_locales_en_utc(spark):
    flat = flatten_bronze(_bronze(spark, [(_payload(updated_at="20261120T083758"), 1)]))
    ligne = flat.select(
        date_format(col("updated_at"), "yyyy-MM-dd HH:mm:ss").alias("maj"),
        date_format(col("period_begin"), "yyyy-MM-dd HH:mm:ss").alias("debut"),
    ).first()
    assert ligne["maj"] == "2026-11-20 07:37:58"   # Paris hiver = UTC+1
    assert ligne["debut"] == "2026-11-20 07:00:00"


# --- zone de rejet ------------------------------------------------------------

def test_json_illisible_va_en_rejet(spark):
    flat = flatten_bronze(_bronze(spark, [("ceci n'est pas du json", 1)]))
    valid, reject = split_valid_reject(flat)
    assert valid.count() == 0
    assert reject.count() == 1


def test_payload_sans_identifiant_va_en_rejet(spark):
    sans_id = json.dumps({"status": "active", "updated_at": "20261120T083758"})
    valid, reject = split_valid_reject(flatten_bronze(_bronze(spark, [(sans_id, 1)])))
    assert valid.count() == 0
    assert reject.count() == 1


def test_aucune_ligne_n_est_perdue_entre_valide_et_rejet(spark):
    lignes = [(_payload("d1"), 1), ("{}", 2), ("pas du json", 3), (_payload("d2"), 4)]
    valid, reject = split_valid_reject(flatten_bronze(_bronze(spark, lignes)))
    assert valid.count() + reject.count() == len(lignes)
    assert valid.count() == 2
    assert reject.count() == 2


# --- déduplication ------------------------------------------------------------

def test_dedup_garde_une_seule_ligne_par_perturbation(spark):
    lignes = [(_payload("d1", updated_at=f"20261120T08{m:02d}00"), m) for m in range(5)]
    assert _silver(spark, lignes).count() == 1


def test_dedup_garde_la_version_la_plus_recente(spark):
    lignes = [
        (_payload("d1", status="active", updated_at="20261120T080000"), 1),
        (_payload("d1", status="past", updated_at="20261120T120000"), 2),
    ]
    assert _silver(spark, lignes).first()["status"] == "past"


def test_dedup_se_fonde_sur_updated_at_et_non_sur_l_ordre_d_arrivee(spark):
    # La version la plus ancienne arrive en dernier (offset plus grand) :
    # elle ne doit pas gagner.
    lignes = [
        (_payload("d1", status="past", updated_at="20261120T120000"), 1),
        (_payload("d1", status="active", updated_at="20261120T080000"), 2),
    ]
    assert _silver(spark, lignes).first()["status"] == "past"


def test_dedup_departage_l_egalite_par_l_offset_kafka(spark):
    lignes = [
        (_payload("d1", status="active", updated_at="20261120T080000"), 1),
        (_payload("d1", status="past", updated_at="20261120T080000"), 2),
    ]
    assert _silver(spark, lignes).first()["status"] == "past"


def test_dedup_ne_melange_pas_des_perturbations_differentes(spark):
    lignes = [(_payload("d1"), 1), (_payload("d1"), 2), (_payload("d2"), 3)]
    ids = sorted(r["disruption_id"] for r in _silver(spark, lignes).collect())
    assert ids == ["d1", "d2"]


def test_dedup_ajoute_la_date_de_traitement(spark):
    ligne = _silver(spark, [(_payload(), 1)]).first()
    assert ligne["silver_processed_at"] is not None
    assert "rn" not in _silver(spark, [(_payload(), 1)]).columns


# --- robustesse ---------------------------------------------------------------

def test_perturbation_sans_message_ne_fait_pas_planter_silver(spark):
    """Un tableau messages vide ou absent doit donner message_text = null."""
    vide = _payload("d1", messages=[])
    absent = json.loads(_payload("d2"))
    del absent["messages"]
    lignes = [(vide, 1), (json.dumps(absent), 2)]
    resultat = {r["disruption_id"]: r["message_text"]
                for r in flatten_bronze(_bronze(spark, lignes)).collect()}
    assert resultat == {"d1": None, "d2": None}


def test_perturbation_sans_periode_d_application_ne_fait_pas_planter_silver(spark):
    vide = _payload("d1", application_periods=[])
    absent = json.loads(_payload("d2"))
    del absent["application_periods"]
    lignes = [(vide, 1), (json.dumps(absent), 2)]
    for ligne in flatten_bronze(_bronze(spark, lignes)).collect():
        assert ligne["period_begin"] is None
        assert ligne["period_end"] is None
