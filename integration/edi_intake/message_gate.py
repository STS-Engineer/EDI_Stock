"""Durable local intake coordinator with explicit, crash-safe archive authorization.

SQLite is a single-host persistent-store implementation, not an Azure deployment
choice. A production adapter must use durable approved storage and authenticate
all callers. This module never contacts Make, Graph, SharePoint or legacy routes.
Import and mail operations are injected; tests inject local/synthetic functions.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid

from receipt_policy import Decision, IDEMPOTENCY_RE, classify, validate_envelope


class GateError(ValueError):
    pass


class StateConflict(GateError):
    pass


class NotReady(GateError):
    pass


def _digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def _key(value):
    if not isinstance(value, str) or not IDEMPOTENCY_RE.fullmatch(value):
        raise GateError("invalid_identity")
    return value


def _canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def batch_key(mailbox, internet_message_id):
    if not all(isinstance(v, str) and v for v in (mailbox, internet_message_id)):
        raise GateError("stable_message_identity_required")
    return "mail:v1:" + _digest(_canonical([mailbox.casefold(), internet_message_id]))


class MessageGate:
    """Persist complete manifests, frozen payloads, retries, receipts and archive claims."""

    def __init__(self, path, *, clock=time.time):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.path.is_symlink():
            raise GateError("state_path_must_not_be_symlink")
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        os.chmod(self.path, 0o600)
        self.clock = clock
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS messages (
                    key TEXT PRIMARY KEY, manifest TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'open',
                    move_token TEXT, target_hash TEXT, moved_id TEXT, updated REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS recovery_events (
                    id TEXT PRIMARY KEY, message_key TEXT NOT NULL, attachment_key TEXT NOT NULL,
                    previous_state TEXT NOT NULL, previous_error TEXT, payload_sha256 TEXT NOT NULL,
                    operator_id TEXT NOT NULL, approval_reference TEXT NOT NULL, reason TEXT NOT NULL,
                    reviewed_outcome TEXT NOT NULL, created REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS attachments (
                    message_key TEXT NOT NULL REFERENCES messages(key), key TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                    payload TEXT, payload_sha256 TEXT, parser_version TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0, claim_token TEXT, lease_until REAL,
                    not_before REAL NOT NULL DEFAULT 0, receipt TEXT, error_code TEXT,
                    archive_proof TEXT, updated REAL NOT NULL,
                    PRIMARY KEY(message_key, key), UNIQUE(key)
                );
            """)

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, isolation_level=None, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        # DELETE journal avoids persisting business data in a loosely permissioned WAL.
        db.execute("PRAGMA journal_mode=DELETE")
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def _tx(self):
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def register(self, message_key, attachments, *, manifest_complete):
        """Freeze every non-inline attachment before filtering, parsing or side effects.

        attachments=[{'key': stable_attachment_key, 'content_sha256': raw_bytes_hash}].
        Source adapter must finish all provider pagination before manifest_complete=True.
        """
        _key(message_key)
        if (
            manifest_complete is not True
            or not isinstance(attachments, list)
            or not attachments
        ):
            raise GateError("complete_nonempty_manifest_required")
        normalized = []
        for item in attachments:
            if not isinstance(item, dict) or set(item) != {"key", "content_sha256"}:
                raise GateError("invalid_manifest_item")
            key = _key(item["key"])
            digest = item["content_sha256"]
            if not isinstance(digest, str) or not re.fullmatch("[0-9a-f]{64}", digest):
                raise GateError("invalid_content_digest")
            normalized.append({"key": key, "content_sha256": digest})
        if len({i["key"] for i in normalized}) != len(normalized):
            raise GateError("duplicate_attachment_identity")
        manifest = _canonical(sorted(normalized, key=lambda x: x["key"]))
        with self._tx() as db:
            existing = db.execute(
                "SELECT manifest FROM messages WHERE key=?", (message_key,)
            ).fetchone()
            if existing:
                if existing["manifest"] != manifest:
                    raise StateConflict("message_manifest_changed")
                return False
            db.execute(
                "INSERT INTO messages(key,manifest,updated) VALUES(?,?,?)",
                (message_key, manifest, self.clock()),
            )
            for item in normalized:
                try:
                    db.execute(
                        "INSERT INTO attachments(message_key,key,content_sha256,updated) VALUES(?,?,?,?)",
                        (
                            message_key,
                            item["key"],
                            item["content_sha256"],
                            self.clock(),
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise StateConflict(
                        "attachment_identity_already_in_another_message"
                    ) from exc
        return True

    def _row(self, db, message_key, attachment_key):
        row = db.execute(
            "SELECT a.*,m.state AS message_state FROM attachments a JOIN messages m ON m.key=a.message_key "
            "WHERE a.message_key=? AND a.key=?",
            (message_key, attachment_key),
        ).fetchone()
        if not row:
            raise GateError("attachment_not_registered")
        return row

    def prepare(self, message_key, attachment_key, payload, *, parser_version):
        """Freeze exactly the request to retry, including parser version and canonical hash."""
        validate_envelope(payload, attachment_key)
        if not isinstance(parser_version, str) or not re.fullmatch(
            "[A-Za-z0-9_.:/-]{1,160}", parser_version
        ):
            raise GateError("invalid_parser_version")
        encoded = _canonical(payload)
        if len(encoded.encode()) > 16 * 1024 * 1024:
            raise GateError("payload_too_large")
        with self._tx() as db:
            row = self._row(db, message_key, attachment_key)
            if row["payload"] is not None:
                if row["payload"] != encoded or row["parser_version"] != parser_version:
                    raise StateConflict("frozen_payload_or_parser_changed")
                return False
            if row["message_state"] != "open" or row["state"] != "pending":
                raise StateConflict("cannot_prepare_in_current_state")
            db.execute(
                "UPDATE attachments SET payload=?,payload_sha256=?,parser_version=?,state='prepared',updated=? "
                "WHERE message_key=? AND key=?",
                (
                    encoded,
                    _digest(encoded),
                    parser_version,
                    self.clock(),
                    message_key,
                    attachment_key,
                ),
            )
        return True

    def reject(self, message_key, attachment_key, *, error_code):
        """Persist unsupported/invalid attachment quarantine without dropping its manifest entry."""
        if not isinstance(error_code, str) or not re.fullmatch(
            "[a-z0-9_]{1,80}", error_code
        ):
            raise GateError("invalid_error_code")
        with self._tx() as db:
            row = self._row(db, message_key, attachment_key)
            if row["message_state"] != "open" or row["state"] not in (
                "pending",
                "prepared",
            ):
                raise StateConflict("cannot_quarantine_in_current_state")
            db.execute(
                "UPDATE attachments SET state='quarantine',error_code=?,updated=? WHERE message_key=? AND key=?",
                (error_code, self.clock(), message_key, attachment_key),
            )

    def record_source_archive(
        self, message_key, attachment_key, *, provider, item_id, content_sha256
    ):
        """Trusted archive adapter supplies a verified item identity and content hash, not a guessed URL."""
        if (
            provider not in ("sharepoint", "approved-object-store")
            or not isinstance(item_id, str)
            or not 1 <= len(item_id) <= 2048
        ):
            raise GateError("invalid_archive_proof")
        with self._tx() as db:
            row = self._row(db, message_key, attachment_key)
            if (
                row["message_state"] != "open"
                or row["content_sha256"] != content_sha256
            ):
                raise StateConflict("archive_content_or_message_mismatch")
            proof = _canonical(
                {
                    "provider": provider,
                    "item_id": item_id,
                    "content_sha256": content_sha256,
                }
            )
            if row["archive_proof"] and row["archive_proof"] != proof:
                raise StateConflict("archive_proof_changed")
            db.execute(
                "UPDATE attachments SET archive_proof=?,updated=? WHERE message_key=? AND key=?",
                (proof, self.clock(), message_key, attachment_key),
            )

    def claim_import(self, message_key, attachment_key, *, lease_seconds=180):
        if (
            not isinstance(lease_seconds, int)
            or isinstance(lease_seconds, bool)
            or lease_seconds < 120
        ):
            raise GateError("import_lease_must_exceed_transport_timeout")
        with self._tx() as db:
            row = self._row(db, message_key, attachment_key)
            if row["message_state"] != "open" or row["state"] not in (
                "prepared",
                "retry",
                "inflight",
            ):
                raise NotReady("attachment_not_importable")
            now = self.clock()
            if row["not_before"] > now or (
                row["state"] == "inflight" and row["lease_until"] > now
            ):
                raise NotReady("retry_or_lease_not_due")
            if row["attempts"] >= 4:
                db.execute(
                    "UPDATE attachments SET state='quarantine',error_code='retry_exhausted',updated=? WHERE message_key=? AND key=?",
                    (now, message_key, attachment_key),
                )
                return None
            token = str(uuid.uuid4())
            db.execute(
                "UPDATE attachments SET state='inflight',claim_token=?,lease_until=?,attempts=attempts+1,updated=? "
                "WHERE message_key=? AND key=?",
                (token, now + lease_seconds, now, message_key, attachment_key),
            )
            return {
                "token": token,
                "payload": json.loads(row["payload"]),
                "idempotency_key": attachment_key,
                "attempt": row["attempts"] + 1,
                "payload_sha256": row["payload_sha256"],
            }

    def finish_import(
        self, message_key, attachment_key, token, status, body, *, retry_after=None
    ):
        with self._tx() as db:
            row = self._row(db, message_key, attachment_key)
            if row["state"] != "inflight" or row["claim_token"] != token:
                raise StateConflict("stale_import_claim")
            payload = json.loads(row["payload"])
            decision = classify(
                status,
                body,
                expected_type=payload["file_type"],
                expected_rows=len(payload["rows"]),
                attempt=row["attempts"],
                retry_after=retry_after,
            )
            db.execute(
                "UPDATE attachments SET state=?,receipt=?,error_code=?,not_before=?,claim_token=NULL,lease_until=NULL,updated=? "
                "WHERE message_key=? AND key=?",
                (
                    decision.disposition,
                    _canonical(asdict(decision)),
                    decision.error_code,
                    self.clock() + (decision.retry_after_seconds or 0),
                    self.clock(),
                    message_key,
                    attachment_key,
                ),
            )
            return decision

    def process(self, message_key, attachment_key, importer):
        """importer(key, frozen_payload) -> (HTTP status, JSON body, headers).

        The importer must target ONLY /api/v1/imports. Never inject an existing
        /process-* endpoint, because those routes already mutate the database.
        """
        claim = self.claim_import(message_key, attachment_key)
        if claim is None:
            return Decision("quarantine", error_code="retry_exhausted")
        try:
            status, body, headers = importer(claim["idempotency_key"], claim["payload"])
        except GateError:
            status, body, headers = (
                503,
                {"error": {"code": "transport_not_configured", "retryable": False}},
                {},
            )
        except (TimeoutError, ConnectionError, OSError):
            status, body, headers = 0, None, {}
        retry_after = next(
            (v for k, v in (headers or {}).items() if k.lower() == "retry-after"), None
        )
        return self.finish_import(
            message_key,
            attachment_key,
            claim["token"],
            status,
            body,
            retry_after=retry_after,
        )

    def reviewed_resume(
        self,
        message_key,
        attachment_key,
        *,
        operator_id,
        approval_reference,
        reason,
        reviewed_outcome,
        expected_state,
    ):
        """Explicit trusted-operator recovery only; never called automatically.

        Authorization must be checked by the authenticated adapter before entry.
        Persist a real review/ticket reference, preserve the exact key/payload and
        source archive proof, and start a fresh bounded attempt budget. No method
        changes a frozen payload or clears an accepted receipt. Invalid/unparsed
        content needs a separately reviewed corrected-source workflow instead.
        """
        allowed = {
            "connection_reauthenticated",
            "api_configuration_repaired",
            "backend_constraint_reconciled",
            "transient_failure_reconciled",
        }
        for value in (operator_id, approval_reference):
            if not isinstance(value, str) or not re.fullmatch(
                "[A-Za-z0-9_.:/@-]{1,196}", value
            ):
                raise GateError("verified_operator_and_review_reference_required")
        if (
            reason not in allowed
            or reviewed_outcome != "same_key_payload_safe"
            or expected_state not in ("blocked", "quarantine")
        ):
            raise GateError("explicit_safe_replay_review_required")
        with self._tx() as db:
            row = self._row(db, message_key, attachment_key)
            if (
                row["message_state"] != "open"
                or row["state"] != expected_state
                or not row["payload"]
            ):
                raise StateConflict("not_a_reviewable_frozen_import")
            # A changed idempotency key/payload cannot be repaired by replaying.
            if row["error_code"] in (
                "idempotency_conflict",
                "validation_failed",
                "database_data_error",
                "parse_or_validation_failed",
            ):
                raise StateConflict("corrected_source_requires_separate_review")
            event = str(uuid.uuid4())
            db.execute(
                "INSERT INTO recovery_events VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event,
                    message_key,
                    attachment_key,
                    row["state"],
                    row["error_code"],
                    row["payload_sha256"],
                    operator_id,
                    approval_reference,
                    reason,
                    reviewed_outcome,
                    self.clock(),
                ),
            )
            db.execute(
                "UPDATE attachments SET state='prepared',attempts=0,claim_token=NULL,lease_until=NULL,not_before=0,receipt=NULL,error_code=NULL,updated=? "
                "WHERE message_key=? AND key=?",
                (self.clock(), message_key, attachment_key),
            )
            return event

    def _ready(self, db, message_key):
        message = db.execute(
            "SELECT * FROM messages WHERE key=?", (message_key,)
        ).fetchone()
        if not message:
            raise GateError("message_not_registered")
        rows = db.execute(
            "SELECT * FROM attachments WHERE message_key=?", (message_key,)
        ).fetchall()
        expected = json.loads(message["manifest"])
        if not rows or len(rows) != len(expected):
            return message, False
        return message, all(
            row["state"] == "accepted" and row["receipt"] and row["archive_proof"]
            for row in rows
        )

    def ready_to_move(self, message_key):
        with self._db() as db:
            message, ready = self._ready(db, message_key)
            return message["state"] == "open" and ready

    def claim_move(self, message_key, *, target_identity):
        """Claim once after all receipts AND raw archive proofs; no automatic move replay."""
        if not isinstance(target_identity, str) or not target_identity:
            raise GateError("verified_mail_target_required")
        with self._tx() as db:
            message, ready = self._ready(db, message_key)
            if message["state"] != "open" or not ready:
                raise NotReady("whole_message_not_ready_or_move_ambiguous")
            token = str(uuid.uuid4())
            db.execute(
                "UPDATE messages SET state='archiving',move_token=?,target_hash=?,updated=? WHERE key=?",
                (token, _digest(target_identity), self.clock(), message_key),
            )
            return token

    def confirm_move(self, message_key, token, *, target_identity, moved_message_id):
        """Call only after provider acknowledgement or a read-back that verifies the destination."""
        if not isinstance(moved_message_id, str) or not moved_message_id:
            raise GateError("provider_move_receipt_required")
        with self._tx() as db:
            row = db.execute(
                "SELECT * FROM messages WHERE key=?", (message_key,)
            ).fetchone()
            if (
                not row
                or row["state"] != "archiving"
                or row["move_token"] != token
                or row["target_hash"] != _digest(target_identity)
            ):
                raise StateConflict("move_receipt_mismatch")
            db.execute(
                "UPDATE messages SET state='archived',moved_id=?,updated=? WHERE key=?",
                (_digest(moved_message_id), self.clock(), message_key),
            )

    def pending_move_claim(self, message_key, *, target_identity):
        """Trusted adapter recovery handle. Never expose this token in public status views."""
        if not isinstance(target_identity, str) or not target_identity:
            raise GateError("verified_mail_target_required")
        with self._db() as db:
            row = db.execute(
                "SELECT state,move_token,target_hash FROM messages WHERE key=?",
                (message_key,),
            ).fetchone()
            if (
                not row
                or row["state"] != "archiving"
                or row["target_hash"] != _digest(target_identity)
            ):
                raise StateConflict("no_matching_pending_move")
            return row["move_token"]

    def reconcile_move(self, message_key, *, target_identity, verifier):
        """Read back provider state after a crash/timeout; never repeat the mutation.

        verifier(target_identity) must return the exact moved-message ID only when
        it proves this source message is in that destination. None stays blocked.
        """
        token = self.pending_move_claim(message_key, target_identity=target_identity)
        moved_id = verifier(target_identity)
        if not moved_id:
            return False
        self.confirm_move(
            message_key,
            token,
            target_identity=target_identity,
            moved_message_id=moved_id,
        )
        return True

    def snapshot(self, message_key):
        """Status only. Do not expose frozen business payloads or external item identities."""
        with self._db() as db:
            message = db.execute(
                "SELECT state,updated FROM messages WHERE key=?", (message_key,)
            ).fetchone()
            if not message:
                raise GateError("message_not_registered")
            rows = db.execute(
                "SELECT key,state,attempts,error_code,not_before,updated FROM attachments WHERE message_key=? ORDER BY key",
                (message_key,),
            ).fetchall()
            return {
                "message": dict(message),
                "attachments": [dict(row) for row in rows],
            }

    def move(self, message_key, *, target_identity, mover):
        """One provider call after the complete gate. An uncertain call remains archiving.

        mover(target_identity) must return the provider's moved-message ID. If the
        call times out, do not call it again; verify the destination and use
        confirm_move with the existing token (available only to the trusted adapter).
        """
        token = self.claim_move(message_key, target_identity=target_identity)
        moved_id = mover(target_identity)
        self.confirm_move(
            message_key,
            token,
            target_identity=target_identity,
            moved_message_id=moved_id,
        )
        return moved_id
