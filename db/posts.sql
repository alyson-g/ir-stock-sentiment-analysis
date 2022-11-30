CREATE TABLE IF NOT EXISTS posts (
    id CHARACTER VARYING(7),
    title TEXT,
    post_text TEXT,
    author TEXT,
    num_comments INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY(id)
);    