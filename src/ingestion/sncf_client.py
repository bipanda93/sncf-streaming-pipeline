"""
Client HTTP pour l'API SNCF (moteur Navitia).

Authentification : HTTP Basic Auth avec le token comme username et un mot
de passe vide — spécificité de l'API Navitia/SNCF, différente d'un Bearer
token classique. Doc officielle : https://doc.navitia.io/

Mode mock : si aucun token n'est fourni au constructeur, le client génère
des données synthétiques respectant le même schéma que l'API réelle. Ça
permet de développer et tester tout le pipeline (Kafka, Databricks, etc.)
avant même d'avoir reçu l'accès à l'API SNCF.
"""
import logging
import random
import uuid
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.sncf.com/v1/coverage/sncf"


class SNCFAPIError(Exception):
    """Levée quand l'API SNCF répond avec une erreur, ou quand on tente un
    appel réseau réel alors que le client est en mode mock."""


class SNCFClient:
    def __init__(self, token: Optional[str] = None, timeout: int = 10):
        self.token = token
        self.timeout = timeout
        self.mock_mode = token is None
        if self.mock_mode:
            logger.warning(
                "Aucun SNCF_API_TOKEN fourni -> le client fonctionne en mode "
                "MOCK (données synthétiques, aucun appel réseau réel)."
            )

    # ------------------------------------------------------------------ #
    # Appels API réels                                                    #
    # ------------------------------------------------------------------ #

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        if self.mock_mode:
            raise SNCFAPIError(
                "Appel réseau demandé alors que le client est en mode mock "
                "(aucun token configuré)."
            )

        response = requests.get(
            f"{BASE_URL}{path}",
            params=params,
            auth=(self.token, ""),  # Navitia : token en username, password vide
            timeout=self.timeout,
        )
        if response.status_code != 200:
            raise SNCFAPIError(
                f"SNCF API a répondu {response.status_code} sur {path} : "
                f"{response.text[:200]}"
            )
        return response.json()

    def get_disruptions(self) -> list[dict]:
        """
        Récupère les perturbations actives sur tout le réseau SNCF.
        Ne nécessite pas d'identifiant de gare (contrairement aux départs)
        -> c'est le point d'entrée le plus simple pour démarrer le pipeline.
        """
        if self.mock_mode:
            return self._mock_disruptions()

        data = self._get("/disruptions")
        return data.get("disruptions", [])

    def search_places(self, query: str) -> list[dict]:
        """
        Recherche des lieux (gares, arrêts) par nom.
        À utiliser pour trouver l'ID exact d'une gare AVANT d'appeler
        get_departures() -- ne jamais coder un stop_area_id en dur, les
        identifiants Navitia ne sont pas devinables.
        Exemple : search_places("Gare de Lyon")
        """
        if self.mock_mode:
            return self._mock_places(query)

        data = self._get("/places", params={"q": query})
        return data.get("places", [])

    def get_departures(self, stop_area_id: str) -> list[dict]:
        """
        Récupère les prochains départs pour une gare donnée.
        stop_area_id : identifiant Navitia, ex. "stop_area:SNCF:87686006"
        (à obtenir via search_places(), jamais codé en dur).
        """
        if self.mock_mode:
            return self._mock_departures(stop_area_id)

        data = self._get(f"/stop_areas/{stop_area_id}/departures")
        return data.get("departures", [])

    # ------------------------------------------------------------------ #
    # Génération de données synthétiques (mode mock)                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _mock_disruptions() -> list[dict]:
        causes = ["Incident technique", "Grève", "Conditions météorologiques", "Colis suspect"]
        severities = [
            {"name": "information", "effect": "NO_SERVICE"},
            {"name": "perturbation", "effect": "SIGNIFICANT_DELAYS"},
            {"name": "critique", "effect": "REDUCED_SERVICE"},
        ]
        now = datetime.now(timezone.utc).isoformat()
        n = random.randint(1, 5)
        return [
            {
                "id": f"mock-disruption-{uuid.uuid4().hex[:8]}",
                "status": "active",
                "cause": random.choice(causes),
                "severity": random.choice(severities),
                "messages": [
                    {"text": "Donnée simulée -- mode mock (aucun token SNCF configuré)."}
                ],
                "application_periods": [{"begin": now, "end": None}],
                "updated_at": now,
            }
            for _ in range(n)
        ]

    @staticmethod
    def _mock_places(query: str) -> list[dict]:
        return [
            {
                "id": f"stop_area:SNCF:mock-{query[:3].upper()}",
                "name": f"{query} (mock)",
                "embedded_type": "stop_area",
            }
        ]

    @staticmethod
    def _mock_departures(stop_area_id: str) -> list[dict]:
        now = datetime.now(timezone.utc).isoformat()
        n = random.randint(2, 8)
        return [
            {
                "stop_date_time": {
                    "departure_date_time": now,
                    "base_departure_date_time": now,
                },
                "display_informations": {
                    "direction": random.choice(
                        ["Lyon Part-Dieu", "Marseille St-Charles", "Lille Flandres"]
                    ),
                    "network": "SNCF",
                    "commercial_mode": random.choice(["TGV", "TER", "Intercités"]),
                },
                "stop_area_id": stop_area_id,
            }
            for _ in range(n)
        ]
