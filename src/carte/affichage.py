"""
Carte des perturbations -- préparation des données pour Streamlit.

Prend les DataFrames pandas lus depuis Silver et Gold, et retourne ce que
st.map() sait afficher. pandas est importé ici et pas dans geo.py, pour
que la logique pure reste testable sans dépendance.
"""
import pandas as pd

from src.carte.geo import agreger_par_gare, localiser_arrets

COULEURS_NIVEAUX = {
    "critical": "#d62728",
    "high": "#ff7f0e",
    "medium": "#f2c12e",
    "low": "#4c9be8",
    "info": "#8a97ad",
}

_COLONNES = ["uic", "gare", "latitude", "longitude", "nb_perturbations", "niveau", "couleur", "taille"]


def preparer_stations(arrets_df, alertes_df, gares):
    """
    Retourne (stations_df, stats).

    arrets_df  : colonnes disruption_id, stop_id (un arrêt impacté par ligne).
    alertes_df : colonnes disruption_id, alert_level (Gold realtime_alerts).
    stations_df : une ligne par gare, avec couleur et taille de point.
    """
    arrets = arrets_df[["disruption_id", "stop_id"]].to_dict("records")
    localises, stats = localiser_arrets(arrets, gares)
    niveaux = dict(zip(alertes_df["disruption_id"], alertes_df["alert_level"]))
    stations = pd.DataFrame(agreger_par_gare(localises, niveaux), columns=[
        "uic", "gare", "latitude", "longitude", "nb_perturbations", "niveau",
    ])
    stations["couleur"] = stations["niveau"].map(COULEURS_NIVEAUX)
    # Rayon en mètres : un point visible à l'échelle de la France, qui grossit avec le nombre de perturbations.
    stations["taille"] = 3000 + 1500 * stations["nb_perturbations"].clip(upper=20)
    return stations[_COLONNES], stats
