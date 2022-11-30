CREATE TABLE IF NOT EXISTS comments (
    id CHARACTER VARYING(7),
    parent_id CHARACTER VARYING(7),
    author TEXT,
    body TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY(id),
    CONSTRAINT fk_posts
        FOREIGN KEY(parent_id)
        REFERENCES posts(id)
);
