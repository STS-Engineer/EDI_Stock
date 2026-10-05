-- REVIEW AND APPLY IN STAGING FIRST. The application never runs migrations.
-- Existing EDIGlobal / DeliveryDetails tables and columns remain unchanged.
BEGIN;
CREATE TABLE IF NOT EXISTS edi_imports (
    idempotency_key varchar(200) PRIMARY KEY,
    payload_hash char(64) NOT NULL,
    import_id uuid NOT NULL UNIQUE,
    row_count integer NOT NULL CHECK (row_count > 0),
    file_type varchar(16) NOT NULL CHECK (file_type IN ('EDI', 'LIVRAISON')),
    imported_at timestamptz NOT NULL DEFAULT now()
);
COMMIT;
-- Do not delete ledger entries while callers might retry their keys.
