CREATE TABLE IF NOT EXISTS events (
    id          uuid        PRIMARY KEY,
    type        text        NOT NULL,
    user_id     text        NOT NULL,
    occurred_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    payload     jsonb       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS events_user_occurred_idx ON events (user_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS events_type_occurred_idx ON events (type, occurred_at DESC);
