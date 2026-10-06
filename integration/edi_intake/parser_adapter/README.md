# Offline attachment parser adapter

Local, side-effect-free implementation of the missing attachment-to-row step.
It returns the **actual new API contract**, exactly `{"file_type": "EDI" | "LIVRAISON", "rows": [...]}`.
It does not expose an HTTP service, invoke a legacy route, import `DATABASE_API/App.py`,
read credentials, run OCR, connect to a database, write a file, or submit an import.

## API and integration

Put the directory containing `parser_adapter/` on the Python path:

```python
from parser_adapter import AdapterError, parse_attachment

payload = parse_attachment(
    attachment_bytes,       # bytes already obtained by the caller
    attachment_filename,    # only checked for .csv; never opened
    "EDI",                  # explicit normalized API kind, not "csv"
    profile="legacy-valeo-germany-v1",
)
```

Only `file_type` and `rows` appear in a successful result. The profile, filename,
Base64 content, customer detection, counts, errors and metadata are not added to
the import envelope. Values are strings, integers or null; no float, Decimal or
date object is returned. Failure raises `AdapterError` with a stable `code` and
bounded `errors`; it does not return partial rows or a success-shaped object.

`parse_base64_attachment(encoded_text, filename, file_type, **options)` is an
optional strict decoder feeding the same function and returning the same shape.
It rejects whitespace, MIME/data-URI prefixes, invalid alphabets and noncanonical
padding. The in-memory bytes function is preferred when attachment bytes are
already available.

Defaults: maximum 10 MiB, maximum 10,000 input data rows, UTF-8 with optional BOM,
automatic delimiter selection only when the parsed header uniquely matches the
explicit profile. `max_bytes` and `max_rows` may reduce these limits. Set
`encoding="latin-1"` only for a known inspected encoding; no encoding is guessed.
`delimiter` can explicitly select one of the delimiters allowed below. Blank
lines are ignored. Header names and cell edges are stripped; duplicate/empty
headers, inconsistent column counts, malformed CSV, NULs and fields longer than
1,000 characters are rejected. Standard quoted fields and embedded newlines are
preserved. Filenames never select a site or customer profile.

After this succeeds, the orchestrator should serialize and freeze the exact
payload, persist its stable idempotency key, and use that same payload/key on
retries. The transaction/import ledger remains exclusively at `/api/v1/imports`.
This adapter does not turn existing Make Base64 requests into working API calls
by an URL replacement. Hosting/wrapping this pure function is a separate reviewed
step. Existing `/process-GermanySite` and `/process-TunisiaSite` routes must not
be used as parse-only steps: they already write business tables and return counts.

## Supported profile 1: normalized-csv-v1 (default)

The first row uses exactly the new application's column names. Comma, semicolon,
tab and vertical-bar delimiters are supported. Unknown columns fail closed.

- EDI required: Site, ClientCode, ClientMaterialNo, AVOMaterialNo, DateFrom, DateUntil,
  Quantity, ForecastDate, EDIStatus.
- EDI optional: LastDeliveryDate, LastDeliveredQuantity,
  CumulatedQuantity, ProductName, LastDeliveryNo.
- LIVRAISON required: Site, AVOMaterialNo, DeliveryNo, Quantity, Date, Status.

Normalization is identical to the supplied application's current pure row
validator, including optional nulls, date/week checks, material suffix joining,
delivery status normalization, delivery duplicate aggregation and integer bounds.
It preserves textual identifiers with leading zeros. Sites are supplied by the
CSV and validated as required text; no site allowlist is invented. EDI DateUntil
is required and must be a real ISO date or ISO week, like DateFrom. Production
column limits were verified read-only on 6 October 2026 and are enforced after
trimming and existing PL/SP material joining, without truncation. Other
normalization follows the length check:

- EDI text: 50 characters per field, except ProductName (100).
- LIVRAISON: Site 20, AVOMaterialNo 30, Date 20, Status 30; DeliveryNo remains
  limited to 28 to reserve `_T` within its 30-character database column.
- Existing date/status rules and business-required fields remain stricter than
  raw database types/nullability. ClientMaterialNo and EDIStatus stay required.

The exact pure validator is vendored as `_contract_validation.py`, with this
SHA-256: `fc6ca3b0125eaa0b8f3ccd7d10e4cdc4e7cfa63986c5cabc47404c2e49bdf266`.
This avoids importing the application's package initializer, Flask, SQLAlchemy
or DB repository just to validate rows. Its source was the supplied
`edi_stock/validation.py` workspace version. It is a snapshot, not a live import:
reconcile it explicitly if application validation changes. The server still
revalidates every imported row. Tests check the snapshot's exact digest and its
byte-for-byte equality with the server validator when the checkout is present.

## Supported profile 2: legacy-valeo-germany-v1

This is a narrowly supported real legacy CSV header path, grounded in a pinned
source function and covered by synthetic fixtures. **No representative customer
attachment has been tested; production compatibility is not established.**
Selecting this profile explicitly authorizes its Germany-specific interpretation;
headers alone cannot distinguish Tunisia and Germany Valeo routes.

The CSV must use comma or semicolon delimiters and have all these headers:

```text
Org_Name_Customer
Plant_No
Material_No_Customer
Delivery_Date
Date
Despatch_Qty
Last_Delivery_Note_Date
Last_Delivery_Quantity
Cum_Quantity
Commitment_Level
Description
Last_Delivery_Note
```

`Customer_No` is optional; if present, every value must contain integer digits.
Additional nonduplicate legacy headers are accepted but ignored, matching the
source's named-field selection. Every row must have `Org_Name_Customer=Valeo`.
All rows must map to a reviewed plant and material:

| Plant_No | ClientCode |
| --- | --- |
| CZ22 | 100442 |
| FUEN | 100541 |
| KJ01 | 100506 |
| CA02 | 100573 |
| ET01 | 100523 |

| Material_No_Customer after lookup-only zero trim | AVOMaterialNo |
| --- | --- |
| 190313 | 1023093 |
| 191663 | 1023645 |
| 187144 | 1026188 |
| 194470 | 1026258 |
| 202066 | 1026540 |
| 214188 | 1026629 |
| 471550 | 1026325 |
| 478537 | 1026365 |
| 470737 | 1026384 |

The output Site is Germany. ClientMaterialNo retains its original leading zeros.
These exact mappings come from the pinned source; they are not asserted to be a
current customer/product master and unknown mappings block the entire file.

Date interpretation:

- `Date` must be `YYYY-MM-DD` or `DD.MM.YYYY` and becomes an ISO week in ForecastDate.
- `Delivery_Date` uses those formats or `CW nn/YYYY`; DateFrom becomes an ISO
  week. DateUntil preserves the calendar date as `YYYY-MM-DD`, or the supplied
  week as `YYYY-Wnn`. The raw dotted/CW text is no longer emitted as DateUntil.
- `Last_Delivery_Note_Date` uses those delivery formats or is blank (null output).
- ISO week-years are used correctly at year boundaries. The source's emitted
  ForecastDate calls `to_forecast_week(Date)` and does **not** use its earlier
  unused, shifted `forecast_date` variable. Wednesday does not add a week here.
- Ambiguous slash dates, BACKORDER, invalid weeks, date placeholders and other
  legacy date dialects are blocked, even if the old permissive parser accepted
  them. They require reviewed examples before adding another supported dialect.

Quantities accept unsigned integer digits; blank quantities become zero as in
the source. Fractions, grouping separators, units, signs and out-of-range values
are blocked. Commitment_Level P/p maps to Forecast and F/f maps to Firm; remaining
values must meet the shared EDIStatus validator. Description and Last_Delivery_Note
are preserved as optional ProductName and LastDeliveryNo.

Intentional safety differences from the legacy service: an unknown plant,
unknown material, invalid row, mixed vendor, malformed date or malformed quantity
rejects the whole attachment. Nothing is silently skipped or truncated. Quotes
and commas are handled by CSV syntax, never stripped/replaced globally.

## Unsupported formats and sample gate

PDF, scanned PDF, OCR, Excel, EDI message syntax, Tunisia customer CSVs, Germany
Nidec/Inteva CSVs and all other vendor formats fail closed. The existing new UI's
facture parser is not reused automatically: it assumes Tunisia and Dispatched,
and its own documentation says partial extraction is possible. That is not a
safe unattended import guarantee.

Before enabling a production legacy customer branch, obtain:

1. One anonymized, structurally intact actual Germany Valeo CSV attachment from
   the intended branch, with original encoding, delimiter, complete header and
   representative valid lines. Preserve quoting, empty cells, leading zeros,
   mixed material/plant cases and date/quantity syntax.
2. A reviewed expected normalized row set for that file, including row count,
   quantities, Site, ClientCode, AVOMaterialNo and week interpretation.
3. Explicit current confirmation of the source-derived plant/material mappings.
4. For each missing vendor/site or PDF layout: its own original representative
   attachment plus expected rows. A redacted multi-page text PDF must retain
   page/table boundaries, invoice header, reference suffixes, totals and all
   material lines; include scanned/unknown PDFs as rejection fixtures.

Only synthetic fixtures are included. No customer attachments, production
credentials, raw legacy application or source configuration files are bundled.

## Provenance (static read only)

Repository: STS-Engineer/DATABASE_API

Commit: `51976838666a332f155029d8b89e2d425df03c5b`

App.py blob: `2e08f5b0b7583336b239b9a5f32782490e6b5d3c`

- [Germany Valeo named columns and mappings, L2875-L2959](https://github.com/STS-Engineer/DATABASE_API/blob/51976838666a332f155029d8b89e2d425df03c5b/App.py#L2875-L2959)
- [Valeo detection, L406-L440](https://github.com/STS-Engineer/DATABASE_API/blob/51976838666a332f155029d8b89e2d425df03c5b/App.py#L406-L440)
- [Date-to-ISO-week behavior, L497-L519](https://github.com/STS-Engineer/DATABASE_API/blob/51976838666a332f155029d8b89e2d425df03c5b/App.py#L497-L519)
- [Legacy date formats, L632-L654](https://github.com/STS-Engineer/DATABASE_API/blob/51976838666a332f155029d8b89e2d425df03c5b/App.py#L632-L654)
- [Germany route and DB write, L3284-L3405](https://github.com/STS-Engineer/DATABASE_API/blob/51976838666a332f155029d8b89e2d425df03c5b/App.py#L3284-L3405)
- [Tunisia route including OCR and DB writes, L2367-L2580](https://github.com/STS-Engineer/DATABASE_API/blob/51976838666a332f155029d8b89e2d425df03c5b/App.py#L2367-L2580)

Source was inspected through the read-only GitHub connector. The original module
was never imported or executed. No legacy service endpoint was called.

## Verification

From the directory containing `parser_adapter/`:

```sh
python -B -m unittest discover -s parser_adapter/tests -v
ruff check --config parser_adapter/pyproject.toml parser_adapter
```

The tests cover both exact envelopes, JSON-native output, all copied mapping
entries, leading zeros, ISO year boundaries, delivery grouping, quoted content,
encodings, base64, rejection paths, input bounds, and atomic rejection of invalid
legacy rows. A fresh-interpreter test blocks imports of legacy/application/DB/
HTTP libraries and trips on network/DB/process audit events; parsing runs with
file opening disabled. A static import-graph check enforces stdlib plus the pure
vendored validator. These checks establish the implemented synthetic contract;
they do not establish live attachment fidelity or a deployed parse endpoint.
