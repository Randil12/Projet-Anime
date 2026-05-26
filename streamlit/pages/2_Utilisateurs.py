import streamlit as st
import plotly.express as px
from utils import run_query

st.set_page_config(page_title="Utilisateurs", page_icon="👤", layout="wide")

st.title("👤 Analyse des Utilisateurs")
st.caption("Comportement et profils des utilisateurs MyAnimeList.")

# ====================
# Sidebar : filtres
# ====================
st.sidebar.header("Filtres")

stats = run_query("""
    SELECT MIN(user_total_reviews) AS min_r,
           MAX(user_total_reviews) AS max_r
    FROM dim_user
""").iloc[0]

min_reviews = st.sidebar.slider(
    "Nombre minimum d'animes dans la liste",
    int(stats["min_r"]), int(stats["max_r"]),
    value=10,
    step=10,
)

severity = st.sidebar.select_slider(
    "Profil de notation",
    options=["Tous", "Sévères (< 6)", "Modérés (6-8)", "Indulgents (> 8)"],
    value="Tous",
)

# Filtres
where = [f"user_total_reviews >= {min_reviews}", "user_average_given_rating IS NOT NULL"]
if severity == "Sévères (< 6)":
    where.append("user_average_given_rating < 6")
elif severity == "Modérés (6-8)":
    where.append("user_average_given_rating BETWEEN 6 AND 8")
elif severity == "Indulgents (> 8)":
    where.append("user_average_given_rating > 8")

where_sql = " AND ".join(where)

users_df = run_query(f"""
    SELECT user_id, user_total_reviews, user_average_given_rating
    FROM dim_user
    WHERE {where_sql}
""")

# ====================
# KPIs
# ====================
col1, col2, col3 = st.columns(3)
col1.metric("Utilisateurs correspondants", f"{len(users_df):,}")
if not users_df.empty:
    col2.metric("Note moyenne (du groupe)", f"{users_df['user_average_given_rating'].mean():.2f}")
    col3.metric("Animes vus en moyenne", f"{users_df['user_total_reviews'].mean():.0f}")

if users_df.empty:
    st.warning("Aucun utilisateur ne correspond à ces filtres.")
    st.stop()

st.divider()

# ====================
# Histogrammes
# ====================
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("📚 Distribution de l'activité")
    st.caption("Combien d'animes les utilisateurs ont-ils dans leur liste ?")
    fig = px.histogram(
        users_df, x="user_total_reviews", nbins=50,
        labels={"user_total_reviews": "Nombre d'animes"},
    )
    fig.update_layout(yaxis_title="Nombre d'utilisateurs")
    st.plotly_chart(fig, use_container_width=True)

with col_b:
    st.subheader("⭐ Distribution de la note moyenne donnée")
    st.caption("Les utilisateurs sont-ils plutôt généreux ou sévères ?")
    fig = px.histogram(
        users_df, x="user_average_given_rating", nbins=40,
        labels={"user_average_given_rating": "Note moyenne donnée"},
    )
    fig.update_layout(yaxis_title="Nombre d'utilisateurs")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ====================
# Scatter activité × sévérité
# ====================
st.subheader("🎯 Activité vs Sévérité")
st.caption("Y a-t-il un lien entre la quantité d'animes regardés et la sévérité de notation ?")

sample = users_df.sample(min(5000, len(users_df)), random_state=42)
fig = px.scatter(
    sample,
    x="user_total_reviews",
    y="user_average_given_rating",
    log_x=True,
    opacity=0.4,
    labels={
        "user_total_reviews": "Animes dans la liste (log)",
        "user_average_given_rating": "Note moyenne donnée",
    },
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ====================
# Top utilisateurs
# ====================
st.subheader("🏆 Top 20 utilisateurs les plus actifs (du groupe filtré)")
top_users = users_df.nlargest(20, "user_total_reviews").assign(
    user_average_given_rating=lambda d: d["user_average_given_rating"].round(2)
)
st.dataframe(top_users, use_container_width=True, hide_index=True)
