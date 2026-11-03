"""
Carte des perturbations -- logique pure (sans Spark ni Streamlit).

Les données Silver identifient les arrêts par un identifiant Navitia du
type `stop_point:SNCF:87751008:LongDistanceTrain` mais ne contiennent
aucune coordonnée. Le code UIC (8 chiffres, ici 87751008) permet de
retrouver la position de la gare dans le référentiel ouvert « Liste des
gares » de SNCF, réduit aux gares voyageurs dans
reference/gares_voyageurs.csv (code_uic, nom, latitude, longitude).

Ce module ne dépend que de la bibliothèque standard : il est testé en CI
sans installer Spark ni Streamlit.
"""
import csv
import re
from pathlib import Path

CHEMIN_GARES = Path(__file__).resolve().parent.parent.parent / "reference" / "gares_voyageurs.csv"

# Du plus grave au moins grave -- même ordre que ALERT_PRIORITY_RANK dans
# src/streaming/gold_realtime_alerts.py.
NIVEAUX_ORDRE = ["critical", "high", "medium", "low", "info"]

_MOTIF_STOP_POINT = re.compile(r"^stop_point:SNCF:(\d{8}):")


def extraire_uic(stop_point_id):
    """Code UIC d'un identifiant d'arrêt, ou None s'il n'a pas le format attendu."""
    if not stop_point_id:
        return None
    m = _MOTIF_STOP_POINT.match(stop_point_id)
    return m.group(1) if m else None


def charger_gares(chemin=CHEMIN_GARES):
    """Référentiel des gares : {code_uic: {"nom", "latitude", "longitude"}}."""
    gares = {}
    with open(chemin, encoding="utf-8", newline="") as f:
        for ligne in csv.DictReader(f):
            gares[ligne["code_uic"]] = {
                "nom": ligne["nom"],
                "latitude": float(ligne["latitude"]),
                "longitude": float(ligne["longitude"]),
            }
    return gares


def localiser_arrets(arrets, gares):
    """
    Ajoute latitude/longitude aux arrêts dont la gare est dans le référentiel.

    `arrets` : liste de dicts avec au moins `disruption_id` et `stop_id`.
    Retourne (arrets localisés, statistiques). Les arrêts non localisés
    (cars, gares étrangères, identifiants inattendus) sont comptés, pas
    perdus en silence.
    """
    localises = []
    for arret in arrets:
        uic = extraire_uic(arret.get("stop_id"))
        gare = gares.get(uic) if uic else None
        if gare is None:
            continue
        localises.append({
            **arret,
            "uic": uic,
            "gare": gare["nom"],
            "latitude": gare["latitude"],
            "longitude": gare["longitude"],
        })
    stats = {
        "arrets_total": len(arrets),
        "arrets_localises": len(localises),
        "part_localisee": len(localises) / len(arrets) if arrets else 0.0,
    }
    return localises, stats


def agreger_par_gare(arrets_localises, niveaux):
    """
    Une ligne par gare : nombre de perturbations distinctes et niveau le
    plus grave. `niveaux` : {disruption_id: alert_level} (table Gold
    realtime_alerts). Une perturbation absente de `niveaux` compte pour "low".
    """
    par_gare = {}
    for arret in arrets_localises:
        entree = par_gare.setdefault(arret["uic"], {
            "uic": arret["uic"],
            "gare": arret["gare"],
            "latitude": arret["latitude"],
            "longitude": arret["longitude"],
            "perturbations": set(),
        })
        entree["perturbations"].add(arret["disruption_id"])

    resultat = []
    for entree in par_gare.values():
        perturbations = entree.pop("perturbations")
        niveau = _niveau_le_plus_grave(
            [niveaux.get(p, "low") for p in perturbations]
        )
        resultat.append({**entree, "nb_perturbations": len(perturbations), "niveau": niveau})
    return sorted(resultat, key=lambda g: (NIVEAUX_ORDRE.index(g["niveau"]), -g["nb_perturbations"]))


def _niveau_le_plus_grave(niveaux):
    inconnus = [n for n in niveaux if n not in NIVEAUX_ORDRE]
    if inconnus:
        raise ValueError(f"Niveau d'alerte inconnu : {inconnus[0]!r}")
    return min(niveaux, key=NIVEAUX_ORDRE.index)
