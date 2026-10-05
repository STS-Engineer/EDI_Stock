"""Offline-tested intake/receipt policy for the caller of normalized-import.

This module has no HTTP, Make, mail, SharePoint or database integration. Its one
write helper persists a local, redacted quarantine manifest. Deployment adapters
must preserve source attachments and durably store accepted receipts themselves.
"""

from __future__ import annotations
import base64
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import uuid

MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024
MAX_ROWS = 10000
RETRY_DELAYS = (60, 300, 900)  # after first, second, third failed attempts
SUPPORTED_EXTENSIONS = {"pdf", "csv", "xls", "xlsx", "txt", "edi", "xml"}
IDEMPOTENCY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,195}\Z")


@dataclass(frozen=True)
class Decision:
    disposition: str
    safe_to_archive: bool = (
        False  # per attachment only; all-attachment gate still mandatory
    )
    retry_after_seconds: int | None = None
    import_id: str | None = None
    rows_imported: int = 0
    rows_received: int = 0
    error_code: str | None = None


def safe_filename(name: str, content_bytes_base64: str) -> str:
    """Equivalent of the pilot Make expression; digest is over canonical base64 text."""
    if not isinstance(name, str) or not isinstance(content_bytes_base64, str):
        raise ValueError("name and contentBytes must be text")
    digest = hashlib.sha256(content_bytes_base64.encode()).hexdigest()
    stem = re.sub("[^A-Za-z0-9_-]", "_", name)[:72]
    extension = re.sub("[^a-z0-9]", "", name.split(".")[-1].lower())[:8] or "bin"
    return f"{digest}_{stem}.{extension}"


def validate_attachment(name: str, encoded: str) -> bytes:
    if (
        not isinstance(name, str)
        or not name
        or not isinstance(encoded, str)
        or not encoded
    ):
        raise ValueError("missing_attachment")
    if "." not in name or name.rsplit(".", 1)[1].lower() not in SUPPORTED_EXTENSIONS:
        raise ValueError("unsupported_extension")
    if len(encoded) > 4 * ((MAX_ATTACHMENT_BYTES + 2) // 3):
        raise ValueError("attachment_too_large")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError) as e:
        raise ValueError("invalid_base64") from e
    if not raw or len(raw) > MAX_ATTACHMENT_BYTES:
        raise ValueError("invalid_attachment_size")
    # Canonicalize upstream before Make hashing if a source can supply alternate encodings.
    if base64.b64encode(raw).decode() != encoded:
        raise ValueError("noncanonical_base64")
    return raw


def source_key(
    site: str, mailbox: str, internet_message_id: str, name: str, raw: bytes
) -> str:
    """A stable source identity; never use now, executionId or a moved Graph message ID."""
    if (
        not all(
            isinstance(x, str) and x for x in [site, mailbox, internet_message_id, name]
        )
        or not raw
    ):
        raise ValueError("stable_source_identity_required")
    identity = [
        site,
        mailbox.casefold(),
        internet_message_id,
        name,
        hashlib.sha256(raw).hexdigest(),
    ]
    digest = hashlib.sha256(
        json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    return "edi:v1:" + digest


def validate_envelope(payload, key):
    """Structural guard; the EDI_Stock server remains the authoritative row validator."""
    if not isinstance(key, str) or not IDEMPOTENCY_RE.fullmatch(key):
        raise ValueError("invalid_idempotency_key")
    if not isinstance(payload, dict) or set(payload) != {"file_type", "rows"}:
        raise ValueError("invalid_payload_keys")
    if payload["file_type"] not in ("EDI", "LIVRAISON"):
        raise ValueError("invalid_file_type")
    rows = payload["rows"]
    if (
        not isinstance(rows, list)
        or not 1 <= len(rows) <= MAX_ROWS
        or not all(isinstance(r, dict) for r in rows)
    ):
        raise ValueError("invalid_rows")
    json.dumps(
        payload, allow_nan=False
    )  # reject NaN and non-JSON values before sending


def classify(
    status_code, body, *, expected_type, expected_rows, attempt=1, retry_after=None
) -> Decision:
    if (
        expected_type not in ("EDI", "LIVRAISON")
        or type(expected_rows) is not int
        or not 1 <= expected_rows <= MAX_ROWS
    ):
        raise ValueError("invalid_expected_import")
    if type(attempt) is not int or attempt < 1:
        raise ValueError("invalid_attempt")
    error = body.get("error", {}) if isinstance(body, dict) else {}
    if not isinstance(error, dict):
        error = {}
    code = error.get("code")
    code = (
        code
        if isinstance(code, str) and re.fullmatch("[a-z0-9_]{1,80}", code)
        else "unknown_error"
    )
    if status_code in (200, 201):
        expected = "imported" if status_code == 201 else "already_imported"
        valid = (
            isinstance(body, dict)
            and body.get("status") == expected
            and body.get("file_type") == expected_type
        )
        valid = (
            valid
            and type(body.get("rows_received")) is int
            and body["rows_received"] == expected_rows
        )
        valid = (
            valid
            and type(body.get("rows_imported")) is int
            and 0 < body["rows_imported"] <= expected_rows
        )
        if expected_type == "EDI":
            valid = valid and body.get("rows_imported") == expected_rows
        try:
            uuid.UUID(body.get("import_id", "") if isinstance(body, dict) else "")
        except (ValueError, AttributeError, TypeError):
            valid = False
        if valid:
            return Decision(
                "accepted",
                True,
                import_id=body["import_id"],
                rows_imported=body["rows_imported"],
                rows_received=body["rows_received"],
            )
        return Decision("blocked", error_code="invalid_acknowledgement")
    if status_code in (400, 409, 413, 415, 422):
        return Decision("quarantine", error_code=code)
    retryable = (
        status_code in (0, 408, 429)
        or (
            status_code in (500, 502, 504)
            and ("retryable" not in error or error.get("retryable") is True)
        )
        or (status_code == 503 and error.get("retryable") is True)
    )
    if retryable:
        if attempt > len(RETRY_DELAYS):
            return Decision("quarantine", error_code="retry_exhausted")
        delay = RETRY_DELAYS[attempt - 1]
        if isinstance(retry_after, str) and retry_after.isdecimal():
            if len(retry_after) > 5 or int(retry_after) > 86400:
                return Decision("blocked", error_code="retry_after_requires_review")
            delay = max(delay, int(retry_after))
        return Decision("retry", retry_after_seconds=delay, error_code=code)
    return Decision("blocked", error_code=code)


def message_ready(expected_keys, receipts) -> bool:
    """Only archive after *all* original, non-inline processable attachments committed.

    Caller must freeze the complete expected set BEFORE filtering/parsing/uploading.
    Unsupported non-inline attachments require explicit disposition; never omit them.
    """
    expected = set(expected_keys)
    if not expected or len(expected) != len(expected_keys):
        return False
    if set(receipts) != expected:
        return False
    return all(
        isinstance(r, Decision)
        and r.disposition == "accepted"
        and r.safe_to_archive
        and r.import_id
        for r in receipts.values()
    )


def persist_quarantine(
    directory, key, decision, *, content_sha256, source_reference_hash
) -> Path:
    """Atomic local metadata-only manifest; source email and original attachment remain in place."""
    if not IDEMPOTENCY_RE.fullmatch(key):
        raise ValueError("invalid_key")
    if decision.disposition != "quarantine":
        raise ValueError("not_quarantine")
    if not all(
        re.fullmatch("[0-9a-f]{64}", v or "")
        for v in [content_sha256, source_reference_hash]
    ):
        raise ValueError("invalid_digest")
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    name = hashlib.sha256(key.encode()).hexdigest() + ".json"
    # Store no payload, auth data, original filename, recipient, message text or server error detail.
    value = {
        "schema_version": 1,
        "idempotency_key": key,
        "disposition": "quarantine",
        "error_code": decision.error_code,
        "content_sha256": content_sha256,
        "source_reference_hash": source_reference_hash,
    }
    fd, tmp = tempfile.mkstemp(prefix=".pending-", dir=path)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(value, f, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path / name)
        dfd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    return path / name
