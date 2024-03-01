CREATE TABLE IF NOT EXISTS root (
    generation INTEGER NOT NULL DEFAULT 0
);

INSERT INTO root (generation) VALUES (0);
