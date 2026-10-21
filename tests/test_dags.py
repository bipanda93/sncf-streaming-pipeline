"""Vérifie que les 6 DAGs SNCF se chargent sans erreur."""
import runpy
from pathlib import Path

import pytest
from airflow import DAG

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"
DAG_FILES = sorted(DAGS_DIR.glob("*.py"))


def dags_du_fichier(fichier):
    variables = runpy.run_path(str(fichier))
    return [v for v in variables.values() if isinstance(v, DAG)]


def test_six_fichiers_de_dags():
    assert len(DAG_FILES) == 6


@pytest.mark.parametrize("fichier", DAG_FILES, ids=lambda f: f.name)
def test_le_fichier_contient_un_dag(fichier):
    assert len(dags_du_fichier(fichier)) == 1


def test_les_dag_id_sont_uniques():
    ids = [d.dag_id for f in DAG_FILES for d in dags_du_fichier(f)]
    assert len(ids) == len(set(ids)) == 6
