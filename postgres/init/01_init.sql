-- ============================================================================
-- Schema Gold : Star/Snowflake pour analyse anime/rating
-- ============================================================================

-- ==== DIMENSIONS ====

CREATE TABLE IF NOT EXISTS dim_anime (
    anime_id    INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    type        TEXT,
    episodes    INTEGER,
    is_airing   BOOLEAN,
    mal_rating  REAL,
    members     INTEGER
);

CREATE TABLE IF NOT EXISTS dim_genre (
    genre_id    INTEGER PRIMARY KEY,
    genre_name  TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_user (
    user_id                   INTEGER PRIMARY KEY,
    user_total_reviews        INTEGER,
    user_average_given_rating REAL
);

-- ==== BRIDGE (many-to-many anime <-> genre) ====

CREATE TABLE IF NOT EXISTS bridge_anime_genre (
    anime_id  INTEGER NOT NULL,
    genre_id  INTEGER NOT NULL,
    PRIMARY KEY (anime_id, genre_id),
    FOREIGN KEY (anime_id) REFERENCES dim_anime(anime_id),
    FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id)
);

-- ==== FACT ====

-- Pas de FK fact_ratings → dim_user : dim_user est dérivé de fact_ratings
-- (la cohérence est garantie par le pipeline)
CREATE TABLE IF NOT EXISTS fact_ratings (
    user_id   INTEGER NOT NULL,
    anime_id  INTEGER NOT NULL,
    rating    INTEGER,
    PRIMARY KEY (user_id, anime_id),
    FOREIGN KEY (anime_id) REFERENCES dim_anime(anime_id)
);

-- ==== ML : recommandations générées par XGBoost (ml_05_inference) ====

CREATE TABLE IF NOT EXISTS recommendations (
    user_id          INTEGER NOT NULL,
    anime_id         INTEGER NOT NULL,
    predicted_rating REAL,
    rank             INTEGER NOT NULL,
    generated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, rank),
    FOREIGN KEY (anime_id) REFERENCES dim_anime(anime_id)
);

-- ==== STAGING : chargement Spark avant remplacement transactionnel ====

CREATE TABLE IF NOT EXISTS stg_dim_anime (
    anime_id    INTEGER,
    name        TEXT,
    type        TEXT,
    episodes    INTEGER,
    is_airing   BOOLEAN,
    mal_rating  REAL,
    members     INTEGER
);

CREATE TABLE IF NOT EXISTS stg_dim_genre (
    genre_id    INTEGER,
    genre_name  TEXT
);

CREATE TABLE IF NOT EXISTS stg_dim_user (
    user_id                   INTEGER,
    user_total_reviews        INTEGER,
    user_average_given_rating REAL
);

CREATE TABLE IF NOT EXISTS stg_bridge_anime_genre (
    anime_id  INTEGER,
    genre_id  INTEGER
);

CREATE TABLE IF NOT EXISTS stg_fact_ratings (
    user_id   INTEGER,
    anime_id  INTEGER,
    rating    INTEGER
);

CREATE TABLE IF NOT EXISTS stg_recommendations (
    user_id          INTEGER,
    anime_id         INTEGER,
    predicted_rating REAL,
    rank             INTEGER
);

-- ==== REJETS : lignes ignorees par les jobs Spark ====

CREATE TABLE IF NOT EXISTS reject_records (
    reject_id    BIGSERIAL PRIMARY KEY,
    job_name     TEXT NOT NULL,
    target_table  TEXT NOT NULL,
    error_reason TEXT NOT NULL,
    payload      TEXT,
    rejected_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==== INDEXES pour requêtes analytiques ====

CREATE INDEX IF NOT EXISTS idx_fact_ratings_anime ON fact_ratings(anime_id);
CREATE INDEX IF NOT EXISTS idx_fact_ratings_user  ON fact_ratings(user_id);
CREATE INDEX IF NOT EXISTS idx_bridge_genre       ON bridge_anime_genre(genre_id);
CREATE INDEX IF NOT EXISTS idx_bridge_anime       ON bridge_anime_genre(anime_id);
CREATE INDEX IF NOT EXISTS idx_reco_user          ON recommendations(user_id);
CREATE INDEX IF NOT EXISTS idx_reject_target      ON reject_records(target_table);
CREATE INDEX IF NOT EXISTS idx_reject_job         ON reject_records(job_name);
