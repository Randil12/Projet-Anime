import streamlit as st
import plotly.express as px
from utils import run_query

st.set_page_config(page_title="Animes", page_icon="📊", layout="wide")

st.title("📊 Catalogue des Animes")
st.caption("Explore les animes avec des filtres dynamiques.")

# ====================
# Sidebar : filtres
# ====================
st.sidebar.header("Filtres")

types_all = run_query("SELECT DISTINCT type FROM dim_anime WHERE type IS NOT NULL ORDER BY type")["type"].tolist()
genres_all = run_query("SELECT genre_name FROM dim_genre ORDER BY genre_name")["genre_name"].tolist()

selected_types = st.sidebar.multiselect("Type", types_all, default=types_all)
selected_genres = st.sidebar.multiselect("Genre (ET logique)", genres_all)
min_members = st.sidebar.slider("Membres minimum", 0, 500_000, 10_000, step=5_000)
min_rating = st.sidebar.slider("Note MAL minimum", 0.0, 10.0, 0.0, step=0.5)

# Construction du WHERE dynamique
where_clauses = ["a.mal_rating IS NOT NULL", f"a.members >= {min_members}", f"a.mal_rating >= {min_rating}"]

if selected_types:
    types_in = ", ".join(f"'{t}'" for t in selected_types)
    where_clauses.append(f"a.type IN ({types_in})")

# Filtre genre via jointure si sélectionné
join_clause = ""
group_clause = ""
having_clause = ""
if selected_genres:
    genres_in = ", ".join(f"'{g}'" for g in selected_genres)
    join_clause = """
        JOIN bridge_anime_genre b ON b.anime_id = a.anime_id
        JOIN dim_genre g ON g.genre_id = b.genre_id
    """
    where_clauses.append(f"g.genre_name IN ({genres_in})")
    group_clause = "GROUP BY a.anime_id, a.name, a.type, a.episodes, a.mal_rating, a.members"
    having_clause = f"HAVING COUNT(DISTINCT g.genre_name) = {len(selected_genres)}"

where_sql = " AND ".join(where_clauses)

base_query = f"""
    SELECT a.anime_id, a.name, a.type, a.episodes, a.mal_rating, a.members
    FROM dim_anime a
    {join_clause}
    WHERE {where_sql}
    {group_clause}
    {having_clause}
"""

filtered = run_query(base_query)

# ====================
# Résultats
# ====================
st.metric("Animes correspondants", f"{len(filtered):,}")

if filtered.empty:
    st.warning("Aucun anime ne correspond à ces filtres.")
    st.stop()

st.divider()

# Top 20 par note
st.subheader("🏆 Top 20 par note MAL")
top20 = filtered.sort_values("mal_rating", ascending=False).head(20)
st.dataframe(
    top20[["name", "type", "episodes", "mal_rating", "members"]],
    use_container_width=True,
    hide_index=True,
)

st.divider()

# ====================
# Graphiques
# ====================
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("📈 Distribution des notes MAL")
    fig = px.histogram(filtered, x="mal_rating", nbins=30, labels={"mal_rating": "Note MAL"})
    fig.update_layout(yaxis_title="Nombre d'animes")
    st.plotly_chart(fig, use_container_width=True)

with col_b:
    st.subheader("🔥 Popularité vs Note")
    st.caption("Y a-t-il des animes très populaires mais mal notés ? Ou des pépites peu connues ?")
    fig = px.scatter(
        filtered,
        x="members",
        y="mal_rating",
        hover_name="name",
        color="type",
        log_x=True,
        labels={"members": "Membres (log)", "mal_rating": "Note MAL"},
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# Note moyenne par genre (basée sur le filtre global, pas les genres sélectionnés)
st.subheader("🏷️ Note MAL moyenne par genre (sur la sélection courante)")
genre_avg = run_query(f"""
    SELECT g.genre_name,
           ROUND(AVG(a.mal_rating)::numeric, 2) AS avg_rating,
           COUNT(*) AS nb_animes
    FROM dim_anime a
    JOIN bridge_anime_genre b ON b.anime_id = a.anime_id
    JOIN dim_genre g ON g.genre_id = b.genre_id
    WHERE a.mal_rating IS NOT NULL
      AND a.members >= {min_members}
      AND a.mal_rating >= {min_rating}
    GROUP BY g.genre_name
    HAVING COUNT(*) >= 5
    ORDER BY avg_rating DESC
""")
fig = px.bar(
    genre_avg.head(20),
    x="genre_name",
    y="avg_rating",
    hover_data=["nb_animes"],
    labels={"genre_name": "Genre", "avg_rating": "Note MAL moyenne"},
)
st.plotly_chart(fig, use_container_width=True)
