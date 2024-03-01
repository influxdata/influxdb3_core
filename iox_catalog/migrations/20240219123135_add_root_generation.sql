CREATE TABLE IF NOT EXISTS root (
    generation BIGINT NOT NULL DEFAULT 0
);

INSERT INTO root (generation) VALUES (0);
