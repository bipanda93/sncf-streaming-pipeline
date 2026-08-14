"""
Client pour l'enrichissement LLM des perturbations via l'API Claude.

Mode mock : comme les autres clients du projet (SNCFClient, RegularityClient),
si aucune clé API n'est configurée, génère un résumé synthétique déterministe
plutôt que d'appeler l'API réelle -- développement et tests sans coût ni
dépendance réseau.

Modèle retenu : Haiku -- tâche de synthèse simple (une phrase par
perturbation), pas besoin d'un modèle plus coûteux. Coût et fréquence
justifiés dans notes/journal_projet.md ("DÉCISION — Fréquence et rôle de
l'enrichissement LLM") : job mensuel rétrospectif, indépendant du calendrier
des mises à jour Airflow du pipeline lui-même.
"""
import logging

from src.ingestion import config

logger = logging.getLogger(__name__)

SUMMARY_PROMPT_TEMPLATE = """Voici les informations sur une perturbation du réseau ferroviaire français :

Cause : {cause}
Sévérité : {severity_name} ({severity_effect})
Message officiel : {message_text}
Gares affectées : {affected_stations}

Rédige UNE SEULE phrase claire et concise (25 mots maximum), en français, expliquant ce qui s'est passé pour un lecteur non technique (voyageur ou opérateur). Ne répète pas les données brutes, synthétise-les. Réponds uniquement avec la phrase, sans préambule."""


class ClaudeAPIError(Exception):
    """Levée quand l'appel à l'API Claude échoue en mode réel."""


class ClaudeClient:
    def __init__(self, api_key=None, mock_mode=None, model=None):
        self.api_key = api_key if api_key is not None else config.ANTHROPIC_API_KEY
        self.model = model or config.CLAUDE_MODEL
        self.mock_mode = mock_mode if mock_mode is not None else (self.api_key is None)

        if self.mock_mode:
            logger.warning(
                "ClaudeClient en mode MOCK -- résumés synthétiques générés, "
                "aucun appel réel à l'API Claude."
            )
        else:
            import anthropic
            self._client = anthropic.Anthropic(api_key=self.api_key)

    def summarize_disruption(self, disruption: dict) -> str:
        """
        Génère un résumé en une phrase d'une perturbation.
        disruption : dict avec cause, severity_name, severity_effect,
        message_text, affected_stations.
        """
        if self.mock_mode:
            return self._mock_summary(disruption)

        prompt = SUMMARY_PROMPT_TEMPLATE.format(
            cause=disruption.get("cause") or "non précisée",
            severity_name=disruption.get("severity_name") or "inconnue",
            severity_effect=disruption.get("severity_effect") or "inconnu",
            message_text=disruption.get("message_text") or "aucun message",
            affected_stations=disruption.get("affected_stations") or "non précisées",
        )
        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text.strip()
        except Exception as e:
            raise ClaudeAPIError(f"Échec de l'appel à l'API Claude : {e}") from e

    @staticmethod
    def _mock_summary(disruption: dict) -> str:
        cause = disruption.get("cause") or disruption.get("severity_name") or "incident"
        stations = disruption.get("affected_stations") or ""
        first_station = stations.split(",")[0].strip() if stations else "une gare non précisée"
        return (
            f"[MOCK] Perturbation liée à {cause} affectant {first_station} "
            f"(résumé simulé, aucun appel API réel)."
        )
