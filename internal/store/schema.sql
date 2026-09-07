-- Applied on worker startup. Idempotent by construction, so every worker can run
-- it and the Nth start is a no-op.
--
-- A production system would use versioned migrations (goose, golang-migrate)
-- rather than CREATE TABLE IF NOT EXISTS, because this cannot express a change
-- to an existing column. For a three-table lab, the extra tool is not worth it.

CREATE TABLE IF NOT EXISTS orders (
    order_id     UUID        NOT NULL,
    -- The delivery identity of the event that created this row. UNIQUE is what
    -- makes redelivery a no-op instead of a duplicate order.
    event_id     UUID        NOT NULL,
    customer_id  TEXT        NOT NULL,
    items        JSONB       NOT NULL,
    total_cents  BIGINT      NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT orders_pkey PRIMARY KEY (order_id),
    CONSTRAINT orders_event_id_key UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS orders_customer_id_idx ON orders (customer_id);
