"""Tests unitaires du client SNCF : aucun réseau, aucun token réel."""
import pytest

from src.ingestion import sncf_client
from src.ingestion.sncf_client import SNCFAPIError, SNCFClient


class FausseReponse:
    def __init__(self, donnees, status_code=200):
        self._donnees = donnees
        self.status_code = status_code
        self.text = str(donnees)

    def json(self):
        return self._donnees


def test_mode_mock_sans_token():
    client = SNCFClient()
    assert client.mock_mode is True
    assert len(client.get_disruptions()) > 0


def test_appel_reseau_interdit_en_mode_mock():
    with pytest.raises(SNCFAPIError):
        SNCFClient()._get("/disruptions")


def test_erreur_http_leve_une_exception(monkeypatch):
    monkeypatch.setattr(
        sncf_client.requests, "get", lambda *a, **k: FausseReponse({}, status_code=500)
    )
    with pytest.raises(SNCFAPIError):
        SNCFClient(token="bidon")._get("/disruptions")


def test_get_disruptions_suit_la_pagination(monkeypatch):
    page0 = {
        "disruptions": [{"id": "a"}, {"id": "b"}],
        "pagination": {"total_result": 3},
    }
    page1 = {
        "disruptions": [{"id": "c"}],
        "pagination": {"total_result": 3},
    }
    reponses = [FausseReponse(page0), FausseReponse(page1)]
    appels = []

    def faux_get(url, params=None, **kwargs):
        appels.append(params)
        return reponses[len(appels) - 1]

    monkeypatch.setattr(sncf_client.requests, "get", faux_get)

    resultat = SNCFClient(token="bidon").get_disruptions()

    assert [d["id"] for d in resultat] == ["a", "b", "c"]
    assert len(appels) == 2
    assert appels[1]["start_page"] == 1