CREATE TABLE IF NOT EXISTS posts (
    id CHARACTER VARYING(7),
    title TEXT,
    post_text TEXT,
    author TEXT,
    num_comments INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    vader_score_title TEXT,
    roberta_score_title TEXT,
    vader_score_post_text TEXT,
    roberta_score_post_text TEXT,
    PRIMARY KEY(id)
);    