import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils import run_query


st.set_page_config(page_title="Utilisateurs", page_icon="User", layout="wide")

st.title("Analyse des utilisateurs")
st.caption("Comportement et profils des utilisateurs MyAnimeList.")

st.sidebar.header("Filtres")

stats = run_query("""
    SELECT MIN(user_total_reviews) AS min_r,
           MAX(user_total_reviews) AS max_r
    FROM dim_user
""").iloc[0]

min_reviews = st.sidebar.slider(
    "Nombre minimum d'animes dans la liste",
    int(stats["min_r"]),
    int(stats["max_r"]),
    value=10,
    step=10,
)

severity = st.sidebar.select_slider(
    "Profil de notation",
    options=["Tous", "Severes (< 6)", "Moderes (6-8)", "Indulgents (> 8)"],
    value="Tous",
)

where = [f"user_total_reviews >= {min_reviews}", "user_average_given_rating IS NOT NULL"]
if severity == "Severes (< 6)":
    where.append("user_average_given_rating < 6")
elif severity == "Moderes (6-8)":
    where.append("user_average_given_rating BETWEEN 6 AND 8")
elif severity == "Indulgents (> 8)":
    where.append("user_average_given_rating > 8")

where_sql = " AND ".join(where)

users_df = run_query(f"""
    SELECT user_id, user_total_reviews, user_average_given_rating
    FROM dim_user
    WHERE {where_sql}
""")

col1, col2, col3 = st.columns(3)
col1.metric("Utilisateurs correspondants", f"{len(users_df):,}")
if not users_df.empty:
    col2.metric("Note moyenne du groupe", f"{users_df['user_average_given_rating'].mean():.2f}")
    col3.metric("Animes dans la liste en moyenne", f"{users_df['user_total_reviews'].mean():.0f}")

if users_df.empty:
    st.warning("Aucun utilisateur ne correspond a ces filtres.")
    st.stop()

st.divider()

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Animes dans les listes utilisateurs")
    fig = px.histogram(
        users_df,
        x="user_total_reviews",
        nbins=50,
        labels={"user_total_reviews": "Nombre d'animes"},
    )
    fig.update_layout(yaxis_title="Nombre d'utilisateurs", bargap=0.05)
    st.plotly_chart(fig, use_container_width=True)

with col_b:
    st.subheader("Notes moyennes donnees")
    fig = px.histogram(
        users_df,
        x="user_average_given_rating",
        nbins=40,
        labels={"user_average_given_rating": "Note moyenne donnee"},
    )
    fig.update_layout(yaxis_title="Nombre d'utilisateurs", bargap=0.05)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

st.subheader("Animes dans la liste vs note moyenne donnee")

sample = users_df.sample(min(5000, len(users_df)), random_state=42)
fig = px.scatter(
    sample,
    x="user_total_reviews",
    y="user_average_given_rating",
    log_x=True,
    opacity=0.35,
    labels={
        "user_total_reviews": "Animes dans la liste (log)",
        "user_average_given_rating": "Note moyenne donnee",
    },
)

bin_count = min(10, len(users_df))
if bin_count >= 2:
    trend = users_df[["user_total_reviews", "user_average_given_rating"]].copy()
    trend["activity_bin"] = pd.qcut(
        trend["user_total_reviews"].rank(method="first"),
        q=bin_count,
        duplicates="drop",
    )
    trend = (
        trend.groupby("activity_bin", observed=True)
        .agg(
            user_total_reviews=("user_total_reviews", "median"),
            user_average_given_rating=("user_average_given_rating", "median"),
        )
        .sort_values("user_total_reviews")
    )
    fig.add_trace(
        go.Scatter(
            x=trend["user_total_reviews"],
            y=trend["user_average_given_rating"],
            mode="lines+markers",
            name="Tendance mediane",
            line={"width": 3},
        )
    )

fig.update_layout(yaxis_range=[0, 10])
st.plotly_chart(fig, use_container_width=True)

st.divider()

st.subheader("Top 20 utilisateurs les plus actifs")
top_users = users_df.nlargest(20, "user_total_reviews").assign(
    user_average_given_rating=lambda d: d["user_average_given_rating"].round(2)
)
st.dataframe(top_users, use_container_width=True, hide_index=True)
