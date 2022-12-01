CREATE TABLE IF NOT EXISTS posts_stocks_xref (
    id CHARACTER VARYING(7),
    symbol CHARACTER VARYING(5),
    CONSTRAINT fk_posts
        FOREIGN KEY(id)
        REFERENCES posts(id),
    CONSTRAINT fk_stocks
        FOREIGN KEY(symbol)
        REFERENCES stocks(symbol)
);