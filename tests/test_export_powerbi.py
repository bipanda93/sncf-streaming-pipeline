"""Export Power BI : éclatement des colonnes multi-valeurs et format Parquet."""
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")
pd = pytest.importorskip("pandas")
pq = pytest.importorskip("pyarrow.parquet")

from src.monitoring import export_for_powerbi as export


@pytest.fixture
def dossier_export(tmp_path, monkeypatch):
    monkeypatch.setattr(export, "EXPORT_DIR", str(tmp_path))
    return tmp_path


def _lire(dossier, nom):
    return pd.read_parquet(dossier / f"{nom}.parquet")


# --- éclatement ---------------------------------------------------------------

def test_une_ligne_par_valeur(dossier_export):
    df = pd.DataFrame({"disruption_id": ["d1"], "regions": ["Bretagne, Normandie, Occitanie"]})
    export.export_exploded(df, "regions", "par_region")
    resultat = _lire(dossier_export, "par_region")
    assert list(resultat["regions"]) == ["Bretagne", "Normandie", "Occitanie"]
    assert list(resultat["disruption_id"]) == ["d1", "d1", "d1"]


def test_les_autres_colonnes_sont_repetees_a_l_identique(dossier_export):
    df = pd.DataFrame({"disruption_id": ["d1", "d2"], "alert_level": ["high", "critical"],
                       "regions": ["A, B", "C"]})
    export.export_exploded(df, "regions", "par_region")
    resultat = _lire(dossier_export, "par_region")
    assert len(resultat) == 3
    assert list(resultat[resultat.disruption_id == "d1"]["alert_level"]) == ["high", "high"]
    assert list(resultat[resultat.disruption_id == "d2"]["alert_level"]) == ["critical"]


def test_valeurs_vides_ou_nulles_sont_supprimees(dossier_export):
    """Une perturbation sans région n'apparaît pas dans l'export par région."""
    df = pd.DataFrame({"disruption_id": ["d1", "d2", "d3"], "regions": ["Bretagne", "", None]})
    export.export_exploded(df, "regions", "par_region")
    resultat = _lire(dossier_export, "par_region")
    assert list(resultat["disruption_id"]) == ["d1"]


def test_le_dataframe_d_entree_n_est_pas_modifie(dossier_export):
    df = pd.DataFrame({"disruption_id": ["d1"], "regions": ["A, B"]})
    avant = df.copy()
    export.export_exploded(df, "regions", "par_region")
    pd.testing.assert_frame_equal(df, avant)


def test_un_fichier_existant_est_remplace(dossier_export):
    df = pd.DataFrame({"disruption_id": ["d1"], "regions": ["A"]})
    export.export_exploded(df, "regions", "par_region")
    export.export_exploded(df.assign(regions="B"), "regions", "par_region")
    assert list(_lire(dossier_export, "par_region")["regions"]) == ["B"]


def test_un_dossier_du_meme_nom_est_remplace_par_un_fichier(dossier_export):
    """Ancien comportement de df.write.parquet() de Spark : il créait un dossier."""
    ancien = dossier_export / "par_region.parquet"
    ancien.mkdir()
    (ancien / "part-0000.parquet").write_bytes(b"x")
    df = pd.DataFrame({"disruption_id": ["d1"], "regions": ["A"]})
    export.export_exploded(df, "regions", "par_region")
    assert ancien.is_file()


# --- format Parquet (incident du 27/08) ---------------------------------------

def test_les_horodatages_sont_ecrits_en_microsecondes(dossier_export):
    """Fabric refusait TIMESTAMP(NANOS) : l'export doit forcer la précision µs."""
    df = pd.DataFrame({
        "disruption_id": ["d1"],
        "regions": ["A"],
        "period_begin": pd.to_datetime(["2026-11-20 07:00:00"]).astype("datetime64[ns]"),
    })
    export.export_exploded(df, "regions", "par_region")
    schema = pq.read_schema(dossier_export / "par_region.parquet")
    assert schema.field("period_begin").type.unit == "us"


# --- colonnes Silver exportées (incident du 31/08) ----------------------------

def test_l_export_silver_exclut_la_structure_imbriquee():
    assert "impacted_objects" not in export.SILVER_COLUMNS_FOR_POWERBI


def test_l_export_silver_exclut_les_colonnes_techniques_kafka():
    for technique in ("kafka_key", "offset", "source"):
        assert technique not in export.SILVER_COLUMNS_FOR_POWERBI
