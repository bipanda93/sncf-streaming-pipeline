"""Tests de src/carte/geo.py -- logique pure, sans Spark ni Streamlit."""
import pytest

from src.carte.geo import (
    CHEMIN_GARES,
    agreger_par_gare,
    charger_gares,
    extraire_uic,
    localiser_arrets,
)

GARES = {
    "87751008": {"nom": "Marseille Saint-Charles", "latitude": 43.302, "longitude": 5.380},
    "87686006": {"nom": "Paris Gare de Lyon", "latitude": 48.844, "longitude": 2.373},
}


def arret(disruption_id, stop_id):
    return {"disruption_id": disruption_id, "stop_id": stop_id, "nom_source": "x"}


def test_extraire_uic_cas_normaux_et_invalides():
    assert extraire_uic("stop_point:SNCF:87751008:LongDistanceTrain") == "87751008"
    assert extraire_uic("stop_point:SNCF:87271023:RapidTransit") == "87271023"
    assert extraire_uic(None) is None
    assert extraire_uic("") is None
    assert extraire_uic("stop_area:SNCF:87751008") is None
    assert extraire_uic("stop_point:SNCF:123:Coach") is None


def test_localiser_arrets_garde_les_gares_connues_et_compte_le_reste():
    arrets = [
        arret("d1", "stop_point:SNCF:87751008:LongDistanceTrain"),
        arret("d1", "stop_point:SNCF:88140010:LongDistanceTrain"),  # Bruxelles, absente
        arret("d2", "stop_point:SNCF:87177360:Coach"),  # car, absent
        arret("d2", None),
    ]
    localises, stats = localiser_arrets(arrets, GARES)
    assert len(localises) == 1
    assert localises[0]["gare"] == "Marseille Saint-Charles"
    assert localises[0]["latitude"] == 43.302
    assert localises[0]["nom_source"] == "x"  # les champs d'origine sont conservés
    assert stats == {"arrets_total": 4, "arrets_localises": 1, "part_localisee": 0.25}


def test_localiser_arrets_liste_vide():
    assert localiser_arrets([], GARES)[1]["part_localisee"] == 0.0


def test_agreger_par_gare_compte_les_perturbations_distinctes_et_le_pire_niveau():
    arrets = [
        arret("d1", "stop_point:SNCF:87751008:LongDistanceTrain"),
        arret("d1", "stop_point:SNCF:87751008:RapidTransit"),  # même perturbation, même gare
        arret("d2", "stop_point:SNCF:87751008:LongDistanceTrain"),
        arret("d3", "stop_point:SNCF:87686006:LongDistanceTrain"),
    ]
    localises, _ = localiser_arrets(arrets, GARES)
    gares = agreger_par_gare(localises, {"d1": "medium", "d2": "critical", "d3": "info"})
    marseille = next(g for g in gares if g["uic"] == "87751008")
    assert marseille["nb_perturbations"] == 2  # d1 et d2, pas 3
    assert marseille["niveau"] == "critical"
    assert gares[0]["uic"] == "87751008"  # la plus grave en premier


def test_agreger_par_gare_niveau_absent_vaut_low():
    localises, _ = localiser_arrets([arret("dX", "stop_point:SNCF:87686006:LongDistanceTrain")], GARES)
    assert agreger_par_gare(localises, {})[0]["niveau"] == "low"


def test_agreger_par_gare_niveau_inconnu_est_signale():
    localises, _ = localiser_arrets([arret("d1", "stop_point:SNCF:87686006:LongDistanceTrain")], GARES)
    with pytest.raises(ValueError, match="inconnu"):
        agreger_par_gare(localises, {"d1": "bizarre"})


def test_referentiel_des_gares_du_depot_est_utilisable():
    gares = charger_gares(CHEMIN_GARES)
    assert len(gares) > 2000
    assert "87686006" in gares  # Paris Gare de Lyon
    g = gares["87686006"]
    assert 48 < g["latitude"] < 49 and 2 < g["longitude"] < 3


def test_preparer_stations_produit_ce_que_streamlit_affiche():
    pd = pytest.importorskip("pandas")  # absent du job CI unit-tests : test exécuté en local
    from src.carte.affichage import preparer_stations

    arrets_df = pd.DataFrame({
        "disruption_id": ["d1", "d2", "d2"],
        "stop_id": [
            "stop_point:SNCF:87751008:LongDistanceTrain",
            "stop_point:SNCF:87751008:LongDistanceTrain",
            "stop_point:SNCF:88140010:LongDistanceTrain",
        ],
    })
    alertes_df = pd.DataFrame({"disruption_id": ["d1", "d2"], "alert_level": ["high", "critical"]})
    stations, stats = preparer_stations(arrets_df, alertes_df, GARES)
    assert len(stations) == 1
    assert stations.iloc[0]["niveau"] == "critical"
    assert stations.iloc[0]["couleur"] == "#d62728"
    assert stations.iloc[0]["nb_perturbations"] == 2
    assert stats["arrets_total"] == 3 and stats["arrets_localises"] == 2

    vide, _ = preparer_stations(arrets_df.iloc[0:0], alertes_df, GARES)
    assert len(vide) == 0
