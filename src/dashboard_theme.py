"""
Thème du tableau de bord Streamlit -- même identité visuelle que le site de
documentation (gare de nuit en fond, panneaux en verre dépoli, accent jaune).

L image est servie depuis src/static/gare.jpg (enableStaticServing dans
.streamlit/config.toml). Les couleurs de base sont dans le même fichier.
"""
import streamlit as st

CSS = """
<style>
/* Fond : la gare de nuit sur toute la page, fixe pendant le défilement */
.stApp {
    background:
        linear-gradient(90deg, rgba(5,9,22,.86) 0%, rgba(5,9,22,.58) 45%, rgba(5,9,22,.18) 100%),
        url("app/static/gare.jpg") center / cover no-repeat fixed;
}
header[data-testid="stHeader"] { background: transparent; }

/* Contenu en verre dépoli */
.block-container {
    background: rgba(14, 22, 40, .55);
    border: 1px solid rgba(255, 255, 255, .10);
    border-radius: 16px;
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    max-width: 1280px !important;
    padding: 2.2rem 2.6rem 3rem;
    margin: 3.2rem auto 2rem !important;
}

/* Titre et onglets */
h1 { font-weight: 700; letter-spacing: -.01em; padding-bottom: 0; }
div[data-baseweb="tab-list"] { gap: .6rem; }
button[data-baseweb="tab"][aria-selected="true"] { color: #f6d860; }
div[data-baseweb="tab-highlight"] { background-color: #f6d860; }

/* Indicateurs */
div[data-testid="stMetric"] {
    background: rgba(255, 255, 255, .04);
    border: 1px solid rgba(255, 255, 255, .10);
    border-radius: 14px;
    padding: 16px 20px;
}
div[data-testid="stMetricValue"] { font-size: 2rem; color: #f6d860; }
div[data-testid="stMetricLabel"] { opacity: .75; }

/* Tableaux et cartes */
div[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }
div[data-testid="stDeckGlChart"], div[data-testid="stArrowVegaLiteChart"] { border-radius: 14px; overflow: hidden; }
</style>
"""


def appliquer_theme():
    st.markdown(CSS, unsafe_allow_html=True)
