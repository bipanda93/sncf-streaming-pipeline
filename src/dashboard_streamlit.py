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
import streamlit as st
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

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
    except Exception as e:
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
    except Exception:
        return "—"


def build_health_table() -> pd.DataFrame:
    rows = []
    ts_candidates = ["ingested_at", "processed_at", "created_at"]

    for layer, name, path, expected_hours, is_reject_zone in TABLES:
        df, error = load_delta_table(path)

        if df is None:
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

st.markdown("""
<style>
    div[data-testid="stMetric"] {
        background-color: rgba(150, 150, 150, 0.08);
        border: 1px solid rgba(150, 150, 150, 0.2);
        border-radius: 10px;
        padding: 16px 20px;
    }
    div[data-testid="stMetricValue"] { font-size: 2rem; }
    h1 { padding-bottom: 0px; }
</style>
""", unsafe_allow_html=True)

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

tab_data, tab_ml = st.tabs(["📊 Santé des données", "🤖 ML & LLM"])

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
        st.dataframe(df.toPandas())

    st.subheader("Détection d'anomalies (Isolation Forest)")
    st.info(
        "Modèle pas encore entraîné (historique encore insuffisant). "
        "Cet onglet affichera les scores d'anomalie une fois disponible."
    )
