import plotly.express as px
import streamlit as st

from utils import run_query


st.set_page_config(page_title="Animes", page_icon="Chart", layout="wide")

st.title("Catalogue des animes")
st.caption("Explore les animes avec des filtres dynamiques.")

st.sidebar.header("Filtres")

types_all = run_query("""
    SELECT DISTINCT type
    FROM dim_anime
    WHERE type IS NOT NULL
    ORDER BY type
""")["type"].tolist()
genres_all = run_query("""
    SELECT genre_name
    FROM dim_genre
    ORDER BY genre_name
""")["genre_name"].tolist()

selected_types = st.sidebar.multiselect("Type", types_all, default=types_all)
selected_genres = st.sidebar.multiselect("Genre (ET logique)", genres_all)
min_members = st.sidebar.slider("Membres minimum", 0, 500_000, 10_000, step=5_000)
min_rating = st.sidebar.slider("Note MAL minimum", 0.0, 10.0, 0.0, step=0.5)

where_clauses = [
    "a.mal_rating IS NOT NULL",
    f"a.members >= {min_members}",
    f"a.mal_rating >= {min_rating}",
]

if selected_types:
    types_in = ", ".join(f"'{t}'" for t in selected_types)
    where_clauses.append(f"a.type IN ({types_in})")

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

filtered = run_query(f"""
    SELECT a.anime_id, a.name, a.type, a.episodes, a.mal_rating, a.members
    FROM dim_anime a
    {join_clause}
    WHERE {where_sql}
    {group_clause}
    {having_clause}
""")

st.metric("Animes correspondants", f"{len(filtered):,}")

if filtered.empty:
    st.warning("Aucun anime ne correspond a ces filtres.")
    st.stop()

st.divider()

st.subheader("Top 20 par note MAL")
top20 = filtered.sort_values(["mal_rating", "members"], ascending=[False, False]).head(20)
st.dataframe(
    top20[["name", "type", "episodes", "mal_rating", "members"]],
    use_container_width=True,
    hide_index=True,
)

st.divider()

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Distribution des notes MAL")
    fig = px.histogram(
        filtered,
        x="mal_rating",
        nbins=30,
        labels={"mal_rating": "Note MAL"},
    )
    fig.update_layout(yaxis_title="Nombre d'animes", bargap=0.05)
    st.plotly_chart(fig, use_container_width=True)

with col_b:
    st.subheader("Membres vs note MAL")
    fig = px.scatter(
        filtered,
        x="members",
        y="mal_rating",
        hover_name="name",
        color="type",
        log_x=True,
        labels={"members": "Membres (log)", "mal_rating": "Note MAL"},
    )
    fig.update_layout(yaxis_range=[0, 10])
    st.plotly_chart(fig, use_container_width=True)

st.divider()

st.subheader("Genres les mieux notes dans la selection")
genre_map = run_query("""
    SELECT b.anime_id, g.genre_name
    FROM bridge_anime_genre b
    JOIN dim_genre g ON g.genre_id = b.genre_id
""")
genre_avg = (
    filtered[["anime_id", "mal_rating"]]
    .merge(genre_map, on="anime_id", how="inner")
    .groupby("genre_name", as_index=False)
    .agg(avg_rating=("mal_rating", "mean"), nb_animes=("anime_id", "nunique"))
    .query("nb_animes >= 5")
    .sort_values(["avg_rating", "nb_animes"], ascending=[False, False])
)

if genre_avg.empty:
    st.info("Pas assez d'animes par genre pour calculer une moyenne fiable.")
else:
    genre_avg["avg_rating"] = genre_avg["avg_rating"].round(2)
    fig = px.bar(
        genre_avg.head(20).sort_values("avg_rating"),
        x="avg_rating",
        y="genre_name",
        orientation="h",
        hover_data=["nb_animes"],
        labels={"genre_name": "Genre", "avg_rating": "Note MAL moyenne"},
    )
    fig.update_layout(xaxis_range=[0, 10], yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)
