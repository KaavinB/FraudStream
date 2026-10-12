-- Kafka offsets are not a stable identity: recreating a topic (or rebuilding the broker while
-- PostgreSQL keeps its volume) reuses offsets. With (topic, partition, offset) as the key, a new
-- quarantined event at a reused offset hit ON CONFLICT DO NOTHING, its offset was committed and
-- the event was lost without a decision or quarantine row. Redelivery of the same record still
-- deduplicates because its payload hash is identical. Writers supply the hex SHA-256 of payload.
ALTER TABLE quarantined_events ADD COLUMN IF NOT EXISTS payload_sha256 text;
UPDATE quarantined_events
    SET payload_sha256 = encode(sha256(convert_to(payload, 'UTF8')), 'hex')
    WHERE payload_sha256 IS NULL;
ALTER TABLE quarantined_events ALTER COLUMN payload_sha256 SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'quarantined_events'::regclass AND i.indisprimary AND a.attname = 'payload_sha256'
    ) THEN
        ALTER TABLE quarantined_events DROP CONSTRAINT quarantined_events_pkey;
        ALTER TABLE quarantined_events
            ADD CONSTRAINT quarantined_events_pkey PRIMARY KEY (topic, partition, event_offset, payload_sha256);
    END IF;
END $$;
