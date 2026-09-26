CREATE TABLE IF NOT EXISTS books (
    id SERIAL,
    title TEXT NOT NULL,
    author_name TEXT,
    first_publish_year TEXT
)