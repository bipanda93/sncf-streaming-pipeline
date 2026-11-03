"""
Dashboard Streamlit -- santé des données du pipeline SNCF, complémentaire
à Grafana (infra Kubernetes) et Power BI (analytics business). Se
concentre sur ce que ni l'un ni l'autre ne couvre : fraîcheur, volume et
qualité des tables Delta elles-mêmes.

Lancer depuis la racine du projet :
    streamlit run src/dashboard_streamlit.py
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pydeck as pdk
import streamlit as st
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, posexplode
from pyspark.sql.types import TimestampType

from src.carte.affichage import COULEURS_NIVEAUX, preparer_stations
from src.carte.geo import NIVEAUX_ORDRE, charger_gares
from src.dashboard_theme import appliquer_theme
from src.ingestion import config


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-streamlit-dashboard")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    packages = []
    if config.AZURE_STORAGE_ACCOUNT_NAME:
        packages.append("org.apache.hadoop:hadoop-azure:3.4.2")
        storage_key = config._get_secret_from_keyvault("storage-account-key")
        builder = builder.config(
            f"spark.hadoop.fs.azure.account.key.{config.AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            storage_key,
        )
    return configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()


@st.cache_resource
def get_spark():
    return build_spark_session()


def load_delta_table(path: str):
    spark = get_spark()
    try:
        return spark.read.format("delta").load(path), None
    except Exception as e:  # noqa: BLE001 -- message affiché dans le tableau de bord
        return None, str(e)


TABLES = [
    ("Bronze", "sncf_raw", config.DELTA_BRONZE_PATH, 2, False),
    ("Silver", "sncf_disruptions", config.DELTA_SILVER_PATH, 2, False),
    ("Silver", "sncf_disruptions_reject", config.DELTA_SILVER_REJECT_PATH, None, True),
    ("Gold", "realtime_alerts", config.DELTA_GOLD_REALTIME_ALERTS_PATH, 4, False),
    ("Gold", "disruption_context", config.DELTA_GOLD_DISRUPTION_CONTEXT_PATH, 4, False),
    ("Gold", "punctuality_trends", config.DELTA_GOLD_PUNCTUALITY_TRENDS_PATH, 24 * 35, False),
]

# Couleurs medallion -- reprend la convention visuelle standard de
# l'architecture (bronze/argent/or), cohérent avec le vocabulaire déjà
# utilisé partout ailleurs dans le projet (docs, Power BI).
LAYER_ICONS = {"Bronze": "🟤", "Silver": "⚪", "Gold": "🟡"}


def compute_fraicheur(last_write, expected_hours):
    if last_write is None or expected_hours is None:
        return "—"
    try:
        lw = pd.Timestamp(last_write)
        if lw.tzinfo is None:
            lw = lw.tz_localize("UTC")
        age_h = (pd.Timestamp.now(tz="UTC") - lw).total_seconds() / 3600
        return "Fraîche" if age_h <= expected_hours else "À surveiller"
    except (TypeError, ValueError):
        return "—"


def build_health_table() -> pd.DataFrame:
    rows = []
    ts_candidates = ["ingested_at", "processed_at", "created_at"]

    for layer, name, path, expected_hours, is_reject_zone in TABLES:
        df, error = load_delta_table(path)

        if df is None:
            if not is_reject_zone:
                st.warning(f"{name} : lecture impossible ({error})")
            statut = "Aucun rejet (sain)" if is_reject_zone else "Jamais alimentée"
            rows.append({
                "": LAYER_ICONS[layer], "Couche": layer, "Table": name, "Statut": statut,
                "Lignes": None, "Dernière écriture": None, "Fraîcheur": "—",
            })
            continue

        count = df.count()
        ts_col = next((c for c in df.columns if any(t in c for t in ts_candidates)), None)
        last_write = df.agg({ts_col: "max"}).collect()[0][0] if ts_col else None

        rows.append({
            "": LAYER_ICONS[layer], "Couche": layer, "Table": name,
            "Statut": f"{count:,} rejet(s)" if is_reject_zone else "OK",
            "Lignes": count,
            "Dernière écriture": str(last_write) if last_write is not None else None,
            "Fraîcheur": compute_fraicheur(last_write, expected_hours),
        })

    return pd.DataFrame(rows)


def style_health_table(df: pd.DataFrame):
    """Colore les cellules Statut/Fraîcheur selon leur valeur -- garde
    st.dataframe natif (tri, redimensionnement) plutôt qu'un tableau HTML
    brut qui perdrait ces fonctionnalités."""

    def color_fraicheur(val):
        return {
            "Fraîche": "background-color: rgba(46, 160, 67, 0.25); color: #3fb950; font-weight: 600;",
            "À surveiller": "background-color: rgba(219, 109, 40, 0.25); color: #f0883e; font-weight: 600;",
        }.get(val, "color: #8b949e;")

    def color_statut(val):
        s = str(val)
        if s == "OK" or "sain" in s:
            return "color: #3fb950; font-weight: 600;"
        if "Jamais" in s:
            return "color: #f85149; font-weight: 600;"
        return "color: #f0883e;"

    return df.style.map(color_fraicheur, subset=["Fraîcheur"]).map(color_statut, subset=["Statut"])


st.set_page_config(page_title="SNCF -- Santé du pipeline", page_icon="🚆", layout="wide")

appliquer_theme()

header_col1, header_col2 = st.columns([5, 1])
with header_col1:
    st.title("🚆 Pipeline SNCF — Tableau de bord opérationnel")
    st.caption(
        "Complémentaire à Grafana (infra Kubernetes) et Power BI (analytics) -- "
        "se concentre sur la fraîcheur et le volume des données elles-mêmes."
    )
with header_col2:
    st.caption("Actualisé")
    st.caption(datetime.now().strftime("%d/%m %H:%M"))

def vers_pandas(df):
    """toPandas() sûr : les timestamps passent en texte (bug pandas 3 + pyspark 4)."""
    colonnes = [
        col(f.name).cast("string").alias(f.name)
        if isinstance(f.dataType, TimestampType)
        else col(f.name)
        for f in df.schema.fields
    ]
    return df.select(*colonnes).toPandas()


@st.cache_data(ttl=300, show_spinner="Lecture des perturbations actives…")
def charger_donnees_carte():
    """Arrêts impactés (Silver) et niveaux d'alerte (Gold), en pandas."""
    spark = get_spark()
    silver_actif = spark.read.format("delta").load(config.DELTA_SILVER_PATH).filter("status = 'active'")
    arrets = (
        silver_actif.select("disruption_id", posexplode("impacted_objects").alias("objet", "o"))
        .select("disruption_id", posexplode("o.impacted_stops").alias("position", "s"))
        .select("disruption_id", col("s.stop_point.id").alias("stop_id"))
        .toPandas()
    )
    alertes = (
        spark.read.format("delta").load(config.DELTA_GOLD_REALTIME_ALERTS_PATH)
        .select("disruption_id", "alert_level")
        .toPandas()
    )
    return arrets, alertes, silver_actif.count()


@st.cache_resource
def charger_referentiel_gares():
    return charger_gares()


tab_data, tab_carte, tab_ml = st.tabs(["📊 Santé des données", "🗺️ Carte", "🤖 ML & LLM"])

with tab_data:
    health_df = build_health_table()

    total_lignes = int(health_df["Lignes"].dropna().sum())
    n_a_surveiller = int((health_df["Fraîcheur"] == "À surveiller").sum())
    n_fraiche = int((health_df["Fraîcheur"] == "Fraîche").sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tables suivies", len(health_df))
    c2.metric("Lignes totales", f"{total_lignes:,}")
    c3.metric("Fraîches", n_fraiche)
    c4.metric("À surveiller", n_a_surveiller, delta=f"-{n_a_surveiller}" if n_a_surveiller else None, delta_color="inverse")

    st.dataframe(
        style_health_table(health_df),
        use_container_width=True,
        hide_index=True,
        column_config={
            "": st.column_config.TextColumn(width="small"),
            "Lignes": st.column_config.NumberColumn(format="%d"),
        },
    )

with tab_carte:
    st.subheader("Gares touchées par les perturbations actives")
    try:
        arrets_df, alertes_df, nb_actives = charger_donnees_carte()
    except Exception as e:  # noqa: BLE001 -- message affiché dans le tableau de bord
        st.info(f"Données indisponibles (Silver ou Gold absents ou illisibles) : {e}")
    else:
        stations_df, stats = preparer_stations(arrets_df, alertes_df, charger_referentiel_gares())

        niveaux_choisis = st.multiselect(
            "Niveaux d'alerte affichés", NIVEAUX_ORDRE, default=["critical", "high", "medium"]
        )
        affichees = stations_df[stations_df["niveau"].isin(niveaux_choisis)]

        k1, k2, k3 = st.columns(3)
        k1.metric("Perturbations actives", f"{nb_actives:,}")
        k2.metric("Gares affichées", f"{len(affichees):,}")
        k3.metric(
            "Arrêts localisés",
            f"{stats['part_localisee']:.0%}",
            help="Part des arrêts impactés retrouvés dans le référentiel des gares voyageurs. "
                 "Les cars et les gares étrangères n'y figurent pas.",
        )

        if affichees.empty:
            st.info("Aucune gare à afficher pour ces niveaux d'alerte.")
        else:
            pts = affichees.copy()
            # Les plus graves sont dessinées en dernier (au-dessus des autres).
            pts["ordre"] = pts["niveau"].map({n: i for i, n in enumerate(NIVEAUX_ORDRE)})
            pts = pts.sort_values("ordre", ascending=False)
            pts["rgb"] = pts["couleur"].map(
                lambda h: [int(h[i:i + 2], 16) for i in (1, 3, 5)] + [210]
            )
            pts["rayon"] = 1500 + 500 * pts["nb_perturbations"].clip(upper=10)
            st.pydeck_chart(
                pdk.Deck(
                    map_style="dark",
                    initial_view_state=pdk.ViewState(latitude=46.6, longitude=2.5, zoom=5.3, pitch=0),
                    layers=[
                        pdk.Layer(
                            "ScatterplotLayer",
                            data=pts[["gare", "niveau", "nb_perturbations", "latitude", "longitude", "rgb", "rayon"]],
                            get_position="[longitude, latitude]",
                            get_fill_color="rgb",
                            get_radius="rayon",
                            radius_min_pixels=3,
                            radius_max_pixels=9,
                            pickable=True,
                            stroked=True,
                            get_line_color=[7, 13, 28, 255],
                            line_width_min_pixels=1,
                        )
                    ],
                    tooltip={
                        "html": "<b>{gare}</b><br/>{nb_perturbations} perturbation(s) · {niveau}",
                        "style": {"backgroundColor": "#0e1628", "color": "#e8ecf5"},
                    },
                ),
                height=620,
            )
            st.caption(
                " · ".join(f"<span style='color:{COULEURS_NIVEAUX[n]}'>●</span> {n}" for n in NIVEAUX_ORDRE),
                unsafe_allow_html=True,
            )
            st.dataframe(
                affichees[["gare", "niveau", "nb_perturbations"]].head(20),
                use_container_width=True,
                hide_index=True,
            )

with tab_ml:
    st.subheader("Résumés mensuels (Claude API)")
    df, error = load_delta_table(config.DELTA_GOLD_MONTHLY_SUMMARIES_PATH)
    if df is None:
        st.info(
            "Pas encore de données -- le DAG enrichissement_llm_mensuel "
            "n'a jamais tourné. Cet onglet affichera les résumés dès "
            "qu'il aura produit des résultats, sans changement de code."
        )
    else:
        st.dataframe(vers_pandas(df))

    st.subheader("Détection d'anomalies (Isolation Forest)")
    st.info(
        "Modèle pas encore entraîné (historique encore insuffisant). "
        "Cet onglet affichera les scores d'anomalie une fois disponible."
    )
