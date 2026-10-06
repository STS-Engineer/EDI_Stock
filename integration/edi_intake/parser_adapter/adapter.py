"""Bounded, in-memory CSV -> edi_stock import envelope.

This module never imports the legacy App.py, creates an application, opens a
file, starts a scheduler, or calls a network/database library. The parser profile
is chosen by the caller, not guessed from email text, filename, or attachment
contents. See README.md for the deliberately narrow supported input contracts.
"""

import base64
import binascii
import csv
import io
import re
from datetime import date

from ._contract_validation import REQUIRED, SCHEMAS, ValidationError, validate_rows

DEFAULT_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_MAX_ROWS = 10000
NORMALIZED = "normalized-csv-v1"
VALEO_GERMANY = "legacy-valeo-germany-v1"
PROFILES = frozenset({NORMALIZED, VALEO_GERMANY})

# Pinned DATABASE_API/App.py L2878-2896. These mappings are a reviewed snapshot,
# not a live customer master. Unknown mappings require a new reviewed profile.
PLANT_TO_CLIENT = {
    "CZ22": "100442", "FUEN": "100541", "KJ01": "100506",
    "CA02": "100573", "ET01": "100523",
}
MATERIAL_TO_AVO = {
    "190313": "1023093", "191663": "1023645", "187144": "1026188",
    "194470": "1026258", "202066": "1026540", "214188": "1026629",
    "471550": "1026325", "478537": "1026365", "470737": "1026384",
}
VALEO_REQUIRED = frozenset({
    "Org_Name_Customer", "Plant_No", "Material_No_Customer", "Delivery_Date",
    "Date", "Despatch_Qty", "Last_Delivery_Note_Date", "Last_Delivery_Quantity",
    "Cum_Quantity", "Commitment_Level", "Description", "Last_Delivery_Note",
})


class AdapterError(ValueError):
    """No payload exists on failure. Errors omit raw attachment values."""

    def __init__(self, code, message, errors=None):
        super().__init__(message)
        self.code = code
        self.errors = errors or []


def _options(file_type, profile, max_rows, max_bytes, encoding, delimiter):
    if not isinstance(file_type, str) or file_type not in SCHEMAS:
        raise AdapterError("unsupported_file_type", "file_type must be EDI or LIVRAISON.")
    if not isinstance(profile, str) or profile not in PROFILES:
        raise AdapterError("unsupported_profile", "An explicitly supported parser profile is required.")
    if profile == VALEO_GERMANY and file_type != "EDI":
        raise AdapterError("profile_type_mismatch", "The Germany Valeo profile only produces EDI rows.")
    if type(max_rows) is not int or not 1 <= max_rows <= DEFAULT_MAX_ROWS:
        raise AdapterError("invalid_limit", "max_rows must be between 1 and 10000.")
    if type(max_bytes) is not int or not 1 <= max_bytes <= DEFAULT_MAX_BYTES:
        raise AdapterError("invalid_limit", "max_bytes must be between 1 and 10485760.")
    if encoding not in ("utf-8-sig", "latin-1"):
        raise AdapterError("unsupported_encoding", "Use UTF-8, or explicitly select latin-1 after inspection.")
    allowed = (",", ";", "\t", "|") if profile == NORMALIZED else (",", ";")
    if delimiter is not None and delimiter not in allowed:
        raise AdapterError("unsupported_delimiter", "Delimiter is not supported for the selected profile.")
    return allowed


def _header_is_supported(header, file_type, profile):
    if not header or len(header) > 64:
        return False
    if any(not name or len(name) > 128 for name in header) or len(set(header)) != len(header):
        return False
    names = set(header)
    if profile == NORMALIZED:
        return REQUIRED[file_type] <= names <= set(SCHEMAS[file_type])
    return VALEO_REQUIRED <= names


def _read_csv(text, file_type, profile, delimiters, max_rows):
    # Only accept a delimiter when its parsed header satisfies the exact selected
    # contract. Sniffer heuristics and destructive quote/comma replacement are
    # deliberately not used.
    candidates = []
    for delimiter in delimiters:
        reader = csv.reader(io.StringIO(text, newline=""), delimiter=delimiter, strict=True)
        try:
            header = [cell.strip() for cell in next(reader)]
        except (StopIteration, csv.Error):
            continue
        if _header_is_supported(header, file_type, profile):
            candidates.append((header, reader))
    if len(candidates) != 1:
        raise AdapterError("unsupported_headers", "Headers do not uniquely match the selected CSV profile.")
    header, reader = candidates[0]
    result = []
    try:
        for cells in reader:
            if not cells or not any(cell.strip() for cell in cells):
                continue
            if len(cells) != len(header):
                raise AdapterError("invalid_csv_shape", "Every nonempty CSV row must have the header's column count.")
            if any(len(cell) > 1000 for cell in cells):
                raise AdapterError("field_too_long", "CSV fields must not exceed 1000 characters.")
            result.append(dict(zip(header, (cell.strip() for cell in cells))))
            if len(result) > max_rows:
                raise AdapterError("too_many_rows", "Attachment exceeds the configured row limit.")
    except csv.Error as exc:
        raise AdapterError("malformed_csv", "CSV syntax or field size is invalid.") from exc
    if not result:
        raise AdapterError("empty_rows", "The CSV contains no data rows.")
    return result


def _legacy_date(value, *, allow_week=False, optional=False, as_week=True):
    if not value and optional:
        return None
    if value in {"0001-01-01", "01.01.0001"}:
        raise ValueError("Placeholder dates need review.")
    if allow_week and re.fullmatch(r"CW\s*\d{1,2}/\d{4}", value, re.IGNORECASE):
        match = re.fullmatch(r"CW\s*(\d{1,2})/(\d{4})", value, re.IGNORECASE)
        year, week = int(match[2]), int(match[1])
        date.fromisocalendar(year, week, 1)
        return f"{year:04d}-W{week:02d}"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        parsed = date.fromisoformat(value)
    elif re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", value):
        day, month, year = (int(part) for part in value.split("."))
        parsed = date(year, month, day)
    else:
        raise ValueError("Use YYYY-MM-DD or DD.MM.YYYY; supported delivery fields also accept CW nn/YYYY.")
    if not as_week:
        return parsed.isoformat()
    year, week, _ = parsed.isocalendar()
    return f"{year:04d}-W{week:02d}"


def _legacy_integer(value):
    # Source uses int(value or 0). A narrower unsigned integer grammar prevents
    # truncation, thousands/decimal ambiguity, negatives, and booleans.
    if not value:
        return 0
    if not re.fullmatch(r"[0-9]+", value):
        raise ValueError("Legacy Valeo quantities must contain only integer digits.")
    result = int(value)
    if result > 2147483647:
        raise ValueError("Quantity exceeds the target integer limit.")
    return result


def _germany_valeo(rows):
    result = []
    errors = []
    for number, row in enumerate(rows, 2):
        # Unlike the source, never skip unknown rows and never return a partial
        # attachment. One rejected row blocks all rows before import.
        try:
            if row["Org_Name_Customer"] != "Valeo":
                raise ValueError("Every row must identify Valeo as Org_Name_Customer.")
            if "Customer_No" in row and not re.fullmatch(r"[0-9]+", row["Customer_No"]):
                raise ValueError("Customer_No must be numeric when the column is present.")
            client = PLANT_TO_CLIENT.get(row["Plant_No"])
            if client is None:
                raise ValueError("Plant_No has no reviewed Germany client mapping.")
            material = row["Material_No_Customer"]
            avo = MATERIAL_TO_AVO.get(material.lstrip("0"))
            if avo is None:
                raise ValueError("Material_No_Customer has no reviewed Germany AVO mapping.")
            status = {"p": "Forecast", "f": "Firm"}.get(row["Commitment_Level"].lower(), row["Commitment_Level"])
            result.append({
                "Site": "Germany", "ClientCode": client,
                "ClientMaterialNo": material, "AVOMaterialNo": avo,
                "DateFrom": _legacy_date(row["Delivery_Date"], allow_week=True),
                "DateUntil": _legacy_date(row["Delivery_Date"], allow_week=True, as_week=False),
                "Quantity": _legacy_integer(row["Despatch_Qty"]),
                # Source L2940 emits to_forecast_week(date_str), not the earlier
                # unused forecast_date variable. There is no Wednesday shift.
                "ForecastDate": _legacy_date(row["Date"]),
                "LastDeliveryDate": _legacy_date(row["Last_Delivery_Note_Date"], allow_week=True, optional=True),
                "LastDeliveredQuantity": _legacy_integer(row["Last_Delivery_Quantity"]),
                "CumulatedQuantity": _legacy_integer(row["Cum_Quantity"]),
                "EDIStatus": status, "ProductName": row["Description"],
                "LastDeliveryNo": row["Last_Delivery_Note"],
            })
        except ValueError as exc:
            errors.append({"row": number, "column": "", "message": str(exc)})
    if errors:
        raise AdapterError("legacy_validation_failed", "Legacy CSV requires correction; no rows were returned.", errors[:100])
    return result


def parse_attachment(data, filename, file_type, *, profile=NORMALIZED,
                     max_rows=DEFAULT_MAX_ROWS, max_bytes=DEFAULT_MAX_BYTES,
                     encoding="utf-8-sig", delimiter=None):
    """Return exactly {'file_type': str, 'rows': list[dict]} or raise AdapterError.

    data is bytes already obtained by the caller. Filename is only used to
    reject unsupported extensions; it is never opened. Defaults handle native
    normalized CSV, not customer-specific legacy layouts. All successful values
    are JSON-native (str/int/None); API/database calls belong to the caller.
    """
    allowed = _options(file_type, profile, max_rows, max_bytes, encoding, delimiter)
    if not isinstance(filename, str) or not filename.lower().endswith(".csv"):
        raise AdapterError("unsupported_format", "Only the documented CSV profiles are supported; PDF and Excel require review.")
    if not isinstance(data, bytes):
        raise AdapterError("invalid_bytes", "Attachment content must be bytes.")
    if not data:
        raise AdapterError("empty_attachment", "The attachment is empty.")
    if len(data) > max_bytes:
        raise AdapterError("attachment_too_large", "Attachment exceeds the configured byte limit.")
    if data.startswith((b"%PDF-", b"PK\x03\x04", b"\xd0\xcf\x11\xe0")):
        raise AdapterError("unsupported_format", "Attachment content is not a supported CSV.")
    try:
        text = data.decode(encoding)
    except UnicodeDecodeError as exc:
        raise AdapterError("invalid_encoding", "Decode failed; UTF-8 is required unless latin-1 was explicitly selected.") from exc
    if "\x00" in text:
        raise AdapterError("invalid_text", "NUL bytes and UTF-16/binary inputs are unsupported.")
    rows = _read_csv(text, file_type, profile, (delimiter,) if delimiter is not None else allowed, max_rows)
    if profile == VALEO_GERMANY:
        rows = _germany_valeo(rows)
    try:
        normalized = validate_rows(rows, file_type, max_rows=max_rows)
    except ValidationError as exc:
        raise AdapterError("row_validation_failed", "The attachment does not satisfy the normalized row contract.", exc.errors) from exc
    return {"file_type": file_type, "rows": normalized}


def parse_base64_attachment(file_content_base64, filename, file_type, **options):
    """Strict compatibility input helper; response is the same exact envelope.

    No endpoint is invoked. MIME prefixes, whitespace, invalid alphabet and
    oversized Base64 strings are rejected rather than repaired heuristically.
    """
    max_bytes = options.get("max_bytes", DEFAULT_MAX_BYTES)
    if type(max_bytes) is not int or not 1 <= max_bytes <= DEFAULT_MAX_BYTES:
        raise AdapterError("invalid_limit", "max_bytes must be between 1 and 10485760.")
    if not isinstance(file_content_base64, str):
        raise AdapterError("invalid_base64", "Base64 content must be a string.")
    if len(file_content_base64) > 4 * ((max_bytes + 2) // 3):
        raise AdapterError("attachment_too_large", "Encoded attachment exceeds the configured byte limit.")
    try:
        data = base64.b64decode(file_content_base64, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise AdapterError("invalid_base64", "Base64 content is malformed.") from exc
    if base64.b64encode(data).decode("ascii") != file_content_base64:
        raise AdapterError("invalid_base64", "Base64 content must use canonical padding and encoding.")
    return parse_attachment(data, filename, file_type, **options)
