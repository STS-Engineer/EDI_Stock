# Executable integration core: parse, commit, reconcile, then move

Updated 6 October 2026. **Implemented and tested locally; not deployed or connected to live Make.**

## What changed since the first review kit

This package now contains working code for the previously missing boundary:

1. `parser_adapter.parse_attachment` converts bytes into the exact normalized import envelope.
2. `IntakeBridge.capture` freezes a complete non-inline attachment manifest before parsing any file. It records malformed/unsupported files as quarantine entries and verifies that parsed row sites match the explicitly configured site.
3. `MessageGate` persists the manifest, exact request payload, parser version, attempt budget, retry time, source-archive proof and import receipt in a private SQLite file.
4. `NormalizedHTTPTransport` sends only to an explicitly supplied HTTPS `/api/v1/imports` endpoint. Redirects are rejected; credentials are requested from an approved provider only at call time and never stored in the gate.
5. The mail-move gate opens only after **every expected attachment** has an accepted committed-import receipt and a verified raw-file archive proof.
6. A mail move is claimed once. A timeout leaves it `archiving`; a restart must read back the provider destination through `reconcile_move`. It never blindly repeats an uncertain move.
7. Explicit operator recovery through `reviewed_resume` records an operator/review reference and preserves the same frozen key/payload. It cannot clear accepted receipts or bypass a payload/idempotency conflict.

A synthetic Germany Valeo CSV now passes through the real local Flask import API and this durable gate. A commit followed by a simulated timeout retries the same request and produces only one business write in the injected test repository.

## Why the existing raw API cannot be chained into the new API

The actual legacy source was found in [STS-Engineer/DATABASE_API/App.py at pinned commit 51976838666a332f155029d8b89e2d425df03c5b](https://github.com/STS-Engineer/DATABASE_API/blob/51976838666a332f155029d8b89e2d425df03c5b/App.py).

- `/process-TunisiaSite`, starting at line 2367, extracts records and then calls the legacy business-table writers.
- `/process-GermanySite`, starting at line 3284, similarly calls `save_to_postgres_with_conflict_reporting`.
- Their response contains `records_processed`, `records_inserted`, `records_failed`, `errors` and file/company metadata. It does **not** contain the normalized rows.
- These routes return HTTP 200 when at least one row was inserted, even if other rows failed. HTTP 200 alone is not a whole-file acknowledgement.
- `/detect-client-info`, starting at line 2669, returns classification/filename metadata rather than an importable row envelope.

**Do not invoke an old `/process-*` route and then call `/api/v1/imports`. That risks duplicate writes and still lacks extracted rows.** No legacy route or live attachment was executed in this work.

The earlier `STS-Engineer/API` repository is a different current implementation exposing `/insert`; its source did not establish the raw processor contract. The pinned DATABASE_API source does establish the route definitions, but its identity with the deployed Azure revision still needs confirmation.

## Supported inputs

- `normalized-csv-v1`: explicit normalized EDI or LIVRAISON CSV headers.
- `legacy-valeo-germany-v1`: explicit Germany Valeo CSV profile, grounded in the pinned legacy function at lines 2875–2959. It does not guess the profile from sender, filename or document text.

The parser deliberately rejects PDF, Excel, unknown customer layouts, unreviewed plant/material mappings, malformed rows and ambiguous dates. One bad row rejects the whole attachment. The valid source is not partially imported.

The separate parser README documents exact headers, encodings, mappings and synthetic examples. Its pure validator is a checksum-locked copy of the reviewed application contract; a server-contract change requires coordinated revalidation. The server still validates independently.

### Smallest remaining evidence for the first real pilot

1. One representative Germany Valeo CSV attachment with the same headers/encoding/quoting as production. Redact business values consistently while preserving structure if necessary.
2. The expected normalized rows reviewed by the process owner, including plant/client mapping, customer/AVO material mapping, quantities, dates/weeks and status.
3. Confirmation that the pinned five plant mappings and nine material mappings remain current and that the intended site value is `Germany`.

For another customer/PDF format, provide one representative source document plus reviewed expected normalized rows, and identify its site/customer. A successful count-only response from the old endpoint is insufficient.

## How to exercise locally

Use the EDI_Stock application's development environment with requirements-dev.txt installed:

    PYTHONPATH=integration/edi_intake:. python -m unittest discover -s integration/edi_intake -v

Run from the EDI_Stock repository root. The application source supplies the real Flask test client; the parser/gate use standard-library-only runtime dependencies. No database URL, real API token or live mail access is needed. Test credentials, addresses and files are synthetic.

This public code-only subset now passes **73 tests**, including the production-column contract repair:

- 16 receipt-policy tests (the five private operational blueprint tests are excluded)
- 30 parser tests, including required DateUntil, varchar boundaries, published fixtures, server/validator parity and fresh-process network/database/application/file-I/O tripwires
- 17 persistent message-gate tests
- 10 real local Flask integration/transport tests

The historical private review kit passed 73 tests on 5 October; its five private
operational blueprint tests are not included or rerun in this public suite.

Scoped Ruff checks and Python compilation also pass for the new integration code. This verifies application behavior with an injected test repository, not PostgreSQL concurrency, Microsoft Graph, SharePoint or Make runtime behavior.

## Adapter usage

The intended call sequence is implemented in Python, with external actions supplied by authenticated adapters:

    gate = MessageGate('/approved-persistent-path/intake.sqlite')
    bridge = IntakeBridge(gate, parser_adapter.parse_attachment)
    message_key, attachment_keys = bridge.capture(
        site='Germany', mailbox=verified_mailbox,
        internet_message_id=stable_message_id,
        attachments=complete_downloaded_attachment_list,
        listing_complete=True,
    )

For each attachment, the trusted source adapter uploads or verifies the raw archive, compares its content digest, then calls `record_source_archive`. The import worker calls `gate.process(message_key, attachment_key, approved_import_transport)` only for due prepared/retry entries. The archive/move worker calls `gate.move(...)` only when `ready_to_move` succeeds. The move callback must return the provider's actual moved-message ID.

This is code usage guidance, not permission to run live actions. Raw attachment objects need an explicit canonical `site`, `file_type`, `parser_profile`, bytes and an actual boolean `inline`. Finish all provider attachment-list pagination before claiming a complete manifest. Never derive `inline` from a truthy string or omit unsupported non-inline files.

### Restart and recovery

- An expired import lease can be retried only with the exact stored key/payload. The import API ledger protects an ambiguous commit.
- Retry times/budgets survive restarts. A failed four-attempt budget quarantines the attachment.
- Unsupported or invalid input remains in the frozen manifest and blocks a mail move.
- An uncertain mail move remains blocked until an authenticated read-back confirms the same source message at the same target. `pending_move_claim` and `reconcile_move` allow recovery without raw database access. A negative/inconclusive read-back does not automatically issue another move.
- An approved operator may recover a fixed configuration/authentication or reconciled backend issue through `reviewed_resume`. The caller must authenticate/authorize that operator before invoking it. The persisted review event is an audit record, not an authentication mechanism.
- Corrections to invalid source data or an idempotency conflict require a separate reviewed correction workflow; this implementation will not silently replace a frozen payload or generate a new key.

## Still required before deployment

This closes the **local parser/controller implementation**, not the production rollout. Remaining requirements:

- Authenticate the Make/source/import/archive/operator adapters; no such external credentials were provisioned or tested here.
- Choose a durable approved state backend and backup/recovery policy. SQLite here is a single-host persistent implementation. Do not place it in an ephemeral Azure filesystem or use it as an unreviewed multi-instance queue. A shared production database adapter requires its own schema/concurrency tests.
- The source adapter must retain enough stable provider identity to download/reconcile the message and its attachments after a restart. Hashed keys alone are not Graph lookup IDs.
- Implement/validate actual Microsoft Graph pagination/download/move/read-back and SharePoint archive/hash-verification calls through approved connections. The Python callbacks are explicit integration seams, not deployed provider adapters.
- Reconnect any invalid Microsoft account grants through the owner's authorized setup process.
- Validate the new API's real PostgreSQL migration/transaction/concurrency behavior and restorable backup.
- Reconcile the representative real CSV fixture, then stage the Make mappings/output paths and full two-attachment failure/retry/move cycle.
- Ensure the legacy writer does not also process the pilot source during cutover. No writer, schedule or account was disabled here.
- Review live scenario revisions again before applying any approved configuration change. The original snapshots/patch revisions are historical evidence, not fresh deployment authority.

Operational scenario snapshots and connection/folder identifiers are intentionally excluded from this code tree. A public PR should contain only generic integration code, parser provenance, synthetic fixtures and tests, never operational configuration exports or credentials.
