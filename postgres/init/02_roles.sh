#!/bin/bash
# ============================================================================
# RBAC : création des 3 rôles métier
#   - data_engineer : full RW + DDL
#   - data_scientist : R sur Gold + RW sur recommendations
#   - data_analyst : R seule sur tout
# Les mots de passe viennent des variables d'environnement injectées par
# docker-compose (PG_DE_PASSWORD, PG_DS_PASSWORD, PG_DA_PASSWORD).
# ============================================================================
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- ============================================
    -- Data Engineer : full access
    -- ============================================
    CREATE ROLE data_engineer LOGIN PASSWORD '${PG_DE_PASSWORD}';
    GRANT ALL PRIVILEGES ON SCHEMA public TO data_engineer;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO data_engineer;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO data_engineer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT ALL ON TABLES TO data_engineer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT ALL ON SEQUENCES TO data_engineer;

    -- ============================================
    -- Data Scientist : R sur Gold, RW sur recommendations
    -- ============================================
    CREATE ROLE data_scientist LOGIN PASSWORD '${PG_DS_PASSWORD}';
    GRANT USAGE ON SCHEMA public TO data_scientist;
    GRANT SELECT ON dim_anime, dim_genre, dim_user, fact_ratings, bridge_anime_genre
        TO data_scientist;
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON recommendations
        TO data_scientist;
    GRANT SELECT, INSERT, DELETE, TRUNCATE ON stg_recommendations, reject_records
        TO data_scientist;
    GRANT USAGE, SELECT ON SEQUENCE reject_records_reject_id_seq
        TO data_scientist;

    -- ============================================
    -- Data Analyst : R seule sur tout
    -- ============================================
    CREATE ROLE data_analyst LOGIN PASSWORD '${PG_DA_PASSWORD}';
    GRANT USAGE ON SCHEMA public TO data_analyst;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO data_analyst;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO data_analyst;

EOSQL

echo "✅ Rôles RBAC créés : data_engineer, data_scientist, data_analyst"
