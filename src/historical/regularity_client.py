"""
Client pour télécharger les jeux de données de régularité mensuelle SNCF
(data.sncf.com, plateforme Opendatasoft).

Accès public complet -- aucune authentification requise (vérifié : HTTP 200
sans token sur l'export CSV, le 11/08/2026).

Mode mock : comme pour SNCFClient, permet de développer/tester sans
dépendre de la disponibilité du service externe.
"""
import logging

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets"

# Identifiants exacts des jeux de données (vérifiés sur ressources.data.sncf.com).
# Note : "reglarite-mensuelle-tgv-nationale" contient une coquille officielle
# côté SNCF (pas de "u" après "reg") -- ne pas "corriger", ça casserait l'URL.
DATASETS = {
    "ter": "regularite-mensuelle-ter",
    "intercites": "regularite-mensuelle-intercites",
    "tgv_globale": "reglarite-mensuelle-tgv-nationale",
    "tgv_axes": "regularite-mensuelle-tgv-axes",
    "tgv_liaisons": "regularite-mensuelle-tgv-aqst",
}


class RegularityAPIError(Exception):
    """Levée quand le téléchargement échoue en mode réel."""


class RegularityClient:
    def __init__(self, mock_mode: bool = False, timeout: int = 30):
        self.mock_mode = mock_mode
        self.timeout = timeout
        if self.mock_mode:
            logger.warning(
                "RegularityClient en mode MOCK -- CSV synthétique généré, "
                "aucun téléchargement réel."
            )

    def download_csv(self, dataset_key: str) -> str:
        """
        Télécharge le CSV d'un jeu de données de régularité.
        dataset_key : une des clés de DATASETS ("ter", "intercites", ...).
        Retourne le contenu CSV brut (texte, délimiteur ';'), à parser
        ensuite avec Spark dans historical_loader.py.
        """
        if dataset_key not in DATASETS:
            raise ValueError(
                f"dataset_key inconnu : {dataset_key!r}. Valeurs valides : {list(DATASETS)}"
            )

        if self.mock_mode:
            return self._mock_csv()

        dataset_id = DATASETS[dataset_key]
        url = f"{BASE_URL}/{dataset_id}/exports/csv"
        response = requests.get(url, timeout=self.timeout)
        if response.status_code != 200:
            raise RegularityAPIError(
                f"Échec du téléchargement de {dataset_key} ({dataset_id}) : "
                f"HTTP {response.status_code}"
            )
        # Le CSV SNCF est encodé avec un BOM UTF-8 en tête ("\ufeffdate" au
        # lieu de "date") -- utf-8-sig le retire proprement (qu'il soit
        # présent ou non), évitant un nom de colonne invisible mais invalide.
        return response.content.decode("utf-8-sig")

    @staticmethod
    def _mock_csv() -> str:
        """
        CSV synthétique respectant le schéma réel du jeu TER, vérifié en
        direct le 11/08/2026 : date;region;nombre_de_trains_programmes;
        nombre_de_trains_ayant_circule;nombre_de_trains_annules;
        nombre_de_trains_en_retard_a_l_arrivee;taux_de_regularite;
        nombre_de_trains_a_l_heure_pour_un_train_en_retard_a_l_arrivee;
        commentaires -- délimiteur ';'.
        """
        header = (
            "date;region;nombre_de_trains_programmes;nombre_de_trains_ayant_circule;"
            "nombre_de_trains_annules;nombre_de_trains_en_retard_a_l_arrivee;"
            "taux_de_regularite;nombre_de_trains_a_l_heure_pour_un_train_en_retard_a_l_arrivee;"
            "commentaires\n"
        )
        rows = (
            "2026-01;Bretagne (mock);1000;950;50;95;90.0;10.0;\n"
            "2026-01;Occitanie (mock);1200;1100;100;110;85.0;8.5;\n"
        )
        return header + rows
