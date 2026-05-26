import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

DB_USER = os.environ.get("DB_BUSINESS_USER")
DB_PASSWORD = os.environ.get("DB_BUSINESS_PASSWORD")
DB_NAME = os.environ.get("DB_BUSINESS_NAME")
DB_HOST = os.environ.get("DB_BUSINESS_HOST")
DB_PORT = os.environ.get("DB_BUSINESS_PORT")


@st.cache_resource
def get_engine():
    """Engine SQLAlchemy cachée pour ne pas recréer la connexion à chaque rerun."""
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    """Exécute une requête SQL et retourne un DataFrame. Résultat caché 5 min."""
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)
