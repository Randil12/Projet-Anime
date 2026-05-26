import streamlit as st
import plotly.express as px
from utils import run_query

st.set_page_config(
    page_title="Anime Analytics",
    page_icon="🎬",
    layout="wide",
)

st.title("🎬 Anime Analytics — Vue d'ensemble")
st.caption("Données MyAnimeList — modèle en étoile (Gold layer)")

# ====================
# KPIs globaux
# ====================
col1, col2, col3, col4 = st.columns(4)

kpi_animes = run_query("SELECT COUNT(*) AS n FROM dim_anime").iloc[0]["n"]
kpi_users = run_query("SELECT COUNT(*) AS n FROM dim_user").iloc[0]["n"]
kpi_ratings = run_query(
    "SELECT COUNT(*) AS n FROM fact_ratings WHERE rating IS NOT NULL"
).iloc[0]["n"]
kpi_genres = run_query("SELECT COUNT(*) AS n FROM dim_genre").iloc[0]["n"]

col1.metric("Animes", f"{int(kpi_animes):,}")
col2.metric("Utilisateurs", f"{int(kpi_users):,}")
col3.metric("Notes données", f"{int(kpi_ratings):,}")
col4.metric("Genres uniques", f"{int(kpi_genres):,}")

st.divider()

# ====================
# Distribution des notes MAL
# ====================
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("📈 Distribution des notes MAL")
    mal_dist = run_query("""
        SELECT mal_rating
        FROM dim_anime
        WHERE mal_rating IS NOT NULL
    """)
    fig = px.histogram(
        mal_dist, x="mal_rating", nbins=40,
        labels={"mal_rating": "Note MAL"},
    )
    fig.update_layout(yaxis_title="Nombre d'animes")
    st.plotly_chart(fig, use_container_width=True)

with col_b:
    st.subheader("🎞️ Répartition par type")
    type_dist = run_query("""
        SELECT type, COUNT(*) AS nb
        FROM dim_anime
        WHERE type IS NOT NULL
        GROUP BY type
        ORDER BY nb DESC
    """)
    fig = px.pie(type_dist, names="type", values="nb", hole=0.4)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ====================
# Top animes
# ====================
st.subheader("🏆 Top 10 animes par note MAL (min 10k membres)")
top_animes = run_query("""
    SELECT name, type, mal_rating, members
    FROM dim_anime
    WHERE mal_rating IS NOT NULL AND members > 10000
    ORDER BY mal_rating DESC
    LIMIT 10
""")
st.dataframe(top_animes, use_container_width=True, hide_index=True)

st.divider()

st.info(
    "📍 **Navigation** : utilise la sidebar à gauche pour explorer le catalogue des animes "
    "ou analyser le comportement des utilisateurs."
)
