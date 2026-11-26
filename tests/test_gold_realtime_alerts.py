"""Gold realtime_alerts : niveau d'alerte, rang de priorité et gares affectées."""
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from src.streaming.gold_realtime_alerts import (
    _add_affected_stations,
    _add_alert_level,
    _add_alert_priority,
)

# Règle métier attendue, écrite en dur : si quelqu'un modifie ALERT_LEVEL_RULES,
# ce test doit le signaler (la règle n'est pas relue depuis le code).
NIVEAU_ATTENDU = {
    "NO_SERVICE": "critical",
    "REDUCED_SERVICE": "high",
    "SIGNIFICANT_DELAYS": "high",
    "DETOUR": "medium",
    "MODIFIED_SERVICE": "medium",
    "STOP_MOVED": "medium",
    "ACCESSIBILITY_ISSUE": "medium",
    "ADDITIONAL_SERVICE": "info",
    "NO_EFFECT": "info",
}
RANG_ATTENDU = {"critical": 1, "high": 2, "medium": 3, "low": 4, "info": 5}


def _niveaux(spark, effets):
    df = spark.createDataFrame([(e,) for e in effets], "severity_effect string")
    return [r["alert_level"] for r in _add_alert_level(df).collect()]


# --- niveau d'alerte ----------------------------------------------------------

@pytest.mark.parametrize("effet,niveau", sorted(NIVEAU_ATTENDU.items()))
def test_niveau_d_alerte_par_effet(spark, effet, niveau):
    assert _niveaux(spark, [effet]) == [niveau]


def test_effet_inconnu_donne_le_niveau_low(spark):
    assert _niveaux(spark, ["UNKNOWN_EFFECT", "OTHER_EFFECT"]) == ["low", "low"]


def test_effet_absent_donne_le_niveau_low(spark):
    assert _niveaux(spark, [None]) == ["low"]


def test_une_ligne_en_entree_donne_une_ligne_en_sortie(spark):
    effets = list(NIVEAU_ATTENDU) + ["INCONNU", None]
    assert len(_niveaux(spark, effets)) == len(effets)


# --- rang de priorité ---------------------------------------------------------

@pytest.mark.parametrize("niveau,rang", sorted(RANG_ATTENDU.items()))
def test_rang_de_priorite_par_niveau(spark, niveau, rang):
    df = spark.createDataFrame([(niveau,)], "alert_level string")
    assert _add_alert_priority(df).first()["alert_priority_rank"] == rang


def test_niveau_inconnu_est_classe_en_dernier(spark):
    df = spark.createDataFrame([("inconnu",)], "alert_level string")
    assert _add_alert_priority(df).first()["alert_priority_rank"] == 99


def test_une_alerte_critique_passe_avant_une_alerte_haute(spark):
    df = spark.createDataFrame(
        [("SIGNIFICANT_DELAYS",), ("NO_SERVICE",), ("NO_EFFECT",)], "severity_effect string"
    )
    classees = (
        _add_alert_priority(_add_alert_level(df))
        .orderBy("alert_priority_rank")
        .select("severity_effect")
        .collect()
    )
    assert [r["severity_effect"] for r in classees] == [
        "NO_SERVICE", "SIGNIFICANT_DELAYS", "NO_EFFECT",
    ]


# --- gares affectées ----------------------------------------------------------

IMPACTED_DDL = (
    "id string, impacted_objects array<struct<"
    "pt_object:struct<id:string,name:string>,"
    "impacted_stops:array<struct<stop_point:struct<id:string,name:string>>>>>"
)


def _arret(nom):
    return {"stop_point": {"id": nom, "name": nom}}


def _objet(*noms):
    return {"pt_object": {"id": "t", "name": "train"}, "impacted_stops": [_arret(n) for n in noms]}


def _gares(spark, impacted_objects):
    df = spark.createDataFrame([("d1", impacted_objects)], IMPACTED_DDL)
    ligne = _add_affected_stations(df).first()
    return ligne["affected_stations"], ligne["nb_affected_stations"]


def test_gares_listees_et_comptees(spark):
    gares, nb = _gares(spark, [_objet("Lille", "Arras", "Paris Nord")])
    assert gares == "Lille, Arras, Paris Nord"
    assert nb == 3


def test_gares_dedoublonnees_entre_plusieurs_trains(spark):
    gares, nb = _gares(spark, [_objet("Lille", "Arras"), _objet("Arras", "Paris Nord")])
    assert nb == 3
    assert sorted(gares.split(", ")) == ["Arras", "Lille", "Paris Nord"]


def test_train_supprime_sans_arrets_donne_chaine_vide_et_zero(spark):
    """Cas NO_SERVICE : impacted_stops vaut null (incident documenté dans le module)."""
    gares, nb = _gares(spark, [{"pt_object": {"id": "t", "name": "train"}, "impacted_stops": None}])
    assert gares == ""
    assert nb == 0


def test_aucun_objet_impacte_donne_chaine_vide_et_zero(spark):
    assert _gares(spark, []) == ("", 0)


def test_objets_impactes_absents_donnent_chaine_vide_et_zero(spark):
    assert _gares(spark, None) == ("", 0)


def test_nb_gares_est_toujours_un_entier_non_nul(spark):
    df = spark.createDataFrame(
        [("d1", None), ("d2", []), ("d3", [_objet("Lille")])], IMPACTED_DDL
    )
    comptes = [r["nb_affected_stations"] for r in _add_affected_stations(df).collect()]
    assert None not in comptes
    assert sorted(comptes) == [0, 0, 1]
