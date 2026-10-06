"""Executable parse-only intake bridge. Production adapters are explicit injections.

No legacy API imports or calls. The normalized transport fixes /api/v1/imports,
requires HTTPS and blocks redirects. Credentials are obtained only at call time
from a caller-supplied approved provider; never serialized in the state database.
"""

from dataclasses import dataclass
import hashlib
import json
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from message_gate import GateError, batch_key
from receipt_policy import source_key


@dataclass(frozen=True)
class Attachment:
    name: str
    data: bytes
    file_type: str
    parser_profile: str = "normalized-csv-v1"
    inline: bool = False


class IntakeBridge:
    def __init__(self, gate, parser):
        self.gate = gate
        self.parser = parser

    def capture(
        self, *, site, mailbox, internet_message_id, attachments, listing_complete
    ):
        if listing_complete is not True or not isinstance(attachments, list):
            raise GateError("complete_attachment_listing_required")
        if any(
            not isinstance(a, Attachment) or type(a.inline) is not bool
            for a in attachments
        ):
            raise GateError("typed_attachment_manifest_required")
        selected = [a for a in attachments if not a.inline]
        if not selected:
            raise GateError("no_processable_manifest")
        entries = []
        keys = []
        for a in selected:
            if (
                not isinstance(a, Attachment)
                or not a.name
                or not isinstance(a.data, bytes)
                or not a.data
            ):
                raise GateError("attachment_identity_or_content_missing")
            key = source_key(site, mailbox, internet_message_id, a.name, a.data)
            keys.append(key)
            entries.append(
                {"key": key, "content_sha256": hashlib.sha256(a.data).hexdigest()}
            )
        message = batch_key(mailbox, internet_message_id)
        self.gate.register(message, entries, manifest_complete=True)
        # Freeze the complete manifest before the first parser call. A parse failure
        # keeps its expected entry and prevents every subsequent whole-message move.
        states = {
            r["key"]: r["state"] for r in self.gate.snapshot(message)["attachments"]
        }
        for a, key in zip(selected, keys):
            if states[key] != "pending":
                continue  # existing frozen request must be retried without reparsing
            try:
                payload = self.parser(
                    a.data, a.name, a.file_type, profile=a.parser_profile
                )
                if (
                    not isinstance(payload, dict)
                    or not isinstance(payload.get("rows"), list)
                    or any(
                        not isinstance(row, dict) or row.get("Site") != site
                        for row in payload["rows"]
                    )
                ):
                    raise ValueError("parser_site_mismatch")
                self.gate.prepare(
                    message, key, payload, parser_version=a.parser_profile
                )
            except ValueError:
                self.gate.reject(message, key, error_code="parse_or_validation_failed")
        return message, keys


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class NormalizedHTTPTransport:
    """Production-capable transport; no connection is made until __call__.

    Operator supplies an approved exact staging/production endpoint and a secure
    authorization_provider returning 'Bearer ...'. Do not put a token in source,
    config exports, Make scenario inputs, logs or the SQLite state file.
    """

    def __init__(self, endpoint, authorization_provider, *, opener=None):
        url = urlsplit(endpoint)
        if (
            url.scheme != "https"
            or not url.hostname
            or url.path != "/api/v1/imports"
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise GateError("approved_https_normalized_endpoint_required")
        self.endpoint = endpoint
        self.authorization_provider = authorization_provider
        self.opener = opener if opener is not None else build_opener(_NoRedirect())

    def __call__(self, key, payload):
        from receipt_policy import validate_envelope

        validate_envelope(payload, key)
        encoded = json.dumps(
            payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
        if len(encoded) > 16 * 1024 * 1024:
            raise GateError("payload_too_large")
        authorization = self.authorization_provider()
        if (
            not isinstance(authorization, str)
            or not authorization.startswith("Bearer ")
            or not authorization[7:].strip()
        ):
            raise GateError("approved_bearer_credential_required")
        if any(c in authorization for c in ("\r", "\n", "\x00")):
            raise GateError("invalid_credential_header")
        request = Request(
            self.endpoint,
            data=encoded,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Idempotency-Key": key,
                "Authorization": authorization,
            },
        )
        try:
            response = self.opener.open(request, timeout=90)
        except HTTPError as error:
            response = error
        with response:
            body = response.read(65537)
            status = response.status if hasattr(response, "status") else response.code
            headers = dict(response.headers)
        if len(body) > 65536:
            return status, {"status": "invalid_response"}, headers
        try:
            parsed = json.loads(body)
        except (ValueError, UnicodeError):
            parsed = {"status": "invalid_response"}
        return status, parsed, headers
