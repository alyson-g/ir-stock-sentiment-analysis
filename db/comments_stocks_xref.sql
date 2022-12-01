CREATE TABLE IF NOT EXISTS comments_stocks_xref (
    id CHARACTER VARYING(7),
    symbol CHARACTER VARYING(5),
    CONSTRAINT fk_comments
        FOREIGN KEY(id)
        REFERENCES comments(id),
    CONSTRAINT fk_stocks
        FOREIGN KEY(symbol)
        REFERENCES stocks(symbol)
);