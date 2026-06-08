import streamlit as st
import plotly.express as px
from utils import run_query

st.set_page_config(page_title="Recommandations", page_icon="🎯", layout="wide")

st.title("🎯 Recommandations personnalisées")
st.caption(
    "Modèle ALS (Spark MLlib) entraîné sur 7M de notes — recommandations matérialisées "
    "dans Postgres par le DAG `dag_ml_recommendation`."
)

# ============================================================================
# Sidebar : sélection d'un utilisateur
# ============================================================================
st.sidebar.header("Choisir un utilisateur")

# Exemples d'utilisateurs actifs (qui ont des recos générées)
sample_users = run_query("""
    SELECT DISTINCT u.user_id,
           u.user_total_reviews,
           ROUND(u.user_average_given_rating::numeric, 2) AS avg_rating
    FROM dim_user u
    JOIN recommendations r ON r.user_id = u.user_id
    ORDER BY u.user_total_reviews DESC
    LIMIT 10
""")

if sample_users.empty:
    st.error(
        "Aucune recommandation trouvée. Lance d'abord le DAG `dag_ml_recommendation` "
        "dans Airflow."
    )
    st.stop()

default_user = int(sample_users.iloc[0]["user_id"])
selected_user = st.sidebar.number_input(
    "User ID",
    min_value=1,
    value=default_user,
    step=1,
)

st.sidebar.markdown("**Top 10 users les plus actifs :**")
st.sidebar.dataframe(sample_users, use_container_width=True, hide_index=True)

# ============================================================================
# Profil utilisateur
# ============================================================================
profile = run_query(f"""
    SELECT user_total_reviews, user_average_given_rating
    FROM dim_user
    WHERE user_id = {int(selected_user)}
""")

if profile.empty:
    st.error(f"User {selected_user} introuvable dans `dim_user`.")
    st.stop()

st.subheader(f"👤 Profil — User {selected_user}")

col1, col2, col3 = st.columns(3)
col1.metric("Animes vus", f"{int(profile.iloc[0]['user_total_reviews']):,}")
avg_rating = profile.iloc[0]["user_average_given_rating"]
col2.metric(
    "Note moyenne donnée",
    f"{avg_rating:.2f}" if avg_rating is not None else "—",
)

# Compte les recos disponibles
nb_recos = run_query(
    f"SELECT COUNT(*) AS n FROM recommendations WHERE user_id = {int(selected_user)}"
).iloc[0]["n"]
col3.metric("Recommandations générées", f"{int(nb_recos)}")

st.divider()

# ============================================================================
# Historique : animes que l'utilisateur a notés
# ============================================================================
st.subheader("📜 Historique — ses animes les mieux notés (top 10)")
st.caption("Pour comprendre les goûts du user et juger si les recos sont cohérentes.")

history = run_query(f"""
    SELECT
        a.name,
        a.type,
        ROUND(a.mal_rating::numeric, 2) AS mal_rating,
        f.rating AS note_user
    FROM fact_ratings f
    JOIN dim_anime a ON a.anime_id = f.anime_id
    WHERE f.user_id = {int(selected_user)} AND f.rating IS NOT NULL
    ORDER BY f.rating DESC, a.members DESC
    LIMIT 10
""")

if history.empty:
    st.info("Cet utilisateur n'a noté aucun anime.")
else:
    st.dataframe(history, use_container_width=True, hide_index=True)

st.divider()

# ============================================================================
# Recommandations ALS
# ============================================================================
st.subheader("⭐ Top 10 recommandations ALS")
st.caption(
    "Le score ALS représente l'affinité prédite par le modèle. Les animes déjà notés "
    "sont automatiquement exclus."
)

recos = run_query(f"""
    SELECT
        r.rank,
        a.name,
        a.type,
        a.episodes,
        ROUND(a.mal_rating::numeric, 2) AS mal_rating,
        a.members,
        ROUND(r.predicted_rating::numeric, 2) AS score_als
    FROM recommendations r
    JOIN dim_anime a ON a.anime_id = r.anime_id
    WHERE r.user_id = {int(selected_user)}
    ORDER BY r.rank
""")

if recos.empty:
    st.warning(
        f"Aucune recommandation pour user {selected_user}. "
        "Possible cause : trop peu de notes pour qu'ALS apprenne."
    )
else:
    st.dataframe(recos, use_container_width=True, hide_index=True)

    st.divider()

    # ========================================================================
    # Comparaison genres : historique vs recommandés
    # ========================================================================
    st.subheader("🏷️ Genres : historique vs recommandations")
    st.caption(
        "Les genres que le user aime (gauche) vs ceux que ALS lui recommande (droite). "
        "Plus la distribution est similaire, plus les recos sont 'cohérentes'."
    )

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("**Genres dans son historique**")
        hist_genres = run_query(f"""
            SELECT g.genre_name, COUNT(*) AS nb
            FROM fact_ratings f
            JOIN bridge_anime_genre b ON b.anime_id = f.anime_id
            JOIN dim_genre g ON g.genre_id = b.genre_id
            WHERE f.user_id = {int(selected_user)}
            GROUP BY g.genre_name
            ORDER BY nb DESC
            LIMIT 15
        """)
        if not hist_genres.empty:
            fig = px.bar(
                hist_genres, x="nb", y="genre_name", orientation="h",
                labels={"nb": "Nombre d'animes vus", "genre_name": ""},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.markdown("**Genres dans les recommandations**")
        reco_genres = run_query(f"""
            SELECT g.genre_name, COUNT(*) AS nb
            FROM recommendations r
            JOIN bridge_anime_genre b ON b.anime_id = r.anime_id
            JOIN dim_genre g ON g.genre_id = b.genre_id
            WHERE r.user_id = {int(selected_user)}
            GROUP BY g.genre_name
            ORDER BY nb DESC
        """)
        if not reco_genres.empty:
            fig = px.bar(
                reco_genres, x="nb", y="genre_name", orientation="h",
                labels={"nb": "Nombre de recos", "genre_name": ""},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=400)
            st.plotly_chart(fig, use_container_width=True)
