-- +goose Up

CREATE TABLE pending_events (
    trade_id   TEXT      NOT NULL,
    source     TEXT      NOT NULL,
    payload    JSONB     NOT NULL DEFAULT '{}',
    first_seen TIMESTAMPTZ NOT NULL,
    deadline   TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_id, source)
);

CREATE INDEX idx_pending_events_deadline ON pending_events (deadline);

CREATE TABLE audit_log (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_id    TEXT        NOT NULL,
    outcome     TEXT        NOT NULL CHECK (outcome IN ('reconciled', 'break')),
    sources     JSONB       NOT NULL DEFAULT '[]',
    detail      JSONB       NOT NULL DEFAULT '{}',
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_log_trade_id ON audit_log (trade_id);
CREATE INDEX idx_audit_log_outcome ON audit_log (outcome, recorded_at);
CREATE INDEX idx_audit_log_recorded_at ON audit_log (recorded_at);

-- +goose Down

DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS pending_events;
