import hashlib
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch
import uuid

from message_gate import MessageGate, GateError, NotReady, StateConflict, batch_key


class GateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "state.sqlite"
        self.now = [1000]
        self.gate = MessageGate(self.path, clock=lambda: self.now[0])
        self.message = batch_key("edi@example.invalid", "<fixture@example.invalid>")
        self.keys = ["edi:v1:a", "edi:v1:b"]
        self.digest = hashlib.sha256(b"synthetic").hexdigest()
        self.manifest = [{"key": k, "content_sha256": self.digest} for k in self.keys]
        self.payload = {
            "file_type": "LIVRAISON",
            "rows": [
                {
                    "Site": "Tunisia",
                    "AVOMaterialNo": "001",
                    "DeliveryNo": "D001",
                    "Quantity": 1,
                    "Date": "2026-10-05",
                    "Status": "Dispatched",
                }
            ],
        }
        self.gate.register(self.message, self.manifest, manifest_complete=True)

    def tearDown(self):
        self.tmp.cleanup()

    def prepare(self, key):
        self.gate.prepare(
            self.message, key, self.payload, parser_version="normalized-csv-v1"
        )

    def response(self, key, payload):
        return (
            201,
            {
                "status": "imported",
                "import_id": str(uuid.uuid4()),
                "file_type": payload["file_type"],
                "rows_imported": len(payload["rows"]),
                "rows_received": len(payload["rows"]),
            },
            {},
        )

    def accept(self, key, archive=True):
        self.prepare(key)
        decision = self.gate.process(self.message, key, self.response)
        if archive:
            self.gate.record_source_archive(
                self.message,
                key,
                provider="sharepoint",
                item_id="synthetic-" + key,
                content_sha256=self.digest,
            )
        return decision

    def test_register_requires_complete_and_preserves_manifest(self):
        self.assertFalse(
            self.gate.register(
                self.message, list(reversed(self.manifest)), manifest_complete=True
            )
        )
        for manifest, complete in [
            (self.manifest, False),
            ([], True),
            ([self.manifest[0]] * 2, True),
        ]:
            with self.assertRaises(GateError):
                self.gate.register("new", manifest, manifest_complete=complete)
        with self.assertRaises(StateConflict):
            self.gate.register(self.message, self.manifest[:1], manifest_complete=True)

    def test_no_early_move_and_all_archives_required(self):
        self.accept(self.keys[0])
        self.assertFalse(self.gate.ready_to_move(self.message))
        self.accept(self.keys[1], archive=False)
        with self.assertRaises(NotReady):
            self.gate.claim_move(self.message, target_identity="verified-destination")
        self.gate.record_source_archive(
            self.message,
            self.keys[1],
            provider="sharepoint",
            item_id="b",
            content_sha256=self.digest,
        )
        self.assertTrue(self.gate.ready_to_move(self.message))

    def test_wrong_archive_hash_cannot_satisfy_gate(self):
        with self.assertRaises(StateConflict):
            self.gate.record_source_archive(
                self.message,
                self.keys[0],
                provider="sharepoint",
                item_id="a",
                content_sha256="0" * 64,
            )

    def test_payload_is_immutable_even_after_reopen(self):
        self.prepare(self.keys[0])
        reopened = MessageGate(self.path, clock=lambda: self.now[0])
        changed = json.loads(json.dumps(self.payload))
        changed["rows"][0]["Quantity"] = 2
        with self.assertRaises(StateConflict):
            reopened.prepare(
                self.message, self.keys[0], changed, parser_version="normalized-csv-v1"
            )
        with self.assertRaises(StateConflict):
            reopened.prepare(
                self.message, self.keys[0], self.payload, parser_version="other-v2"
            )
        self.assertFalse(
            reopened.prepare(
                self.message,
                self.keys[0],
                self.payload,
                parser_version="normalized-csv-v1",
            )
        )

    def test_lease_excludes_concurrent_worker(self):
        self.prepare(self.keys[0])
        first = self.gate.claim_import(self.message, self.keys[0])
        other = MessageGate(self.path, clock=lambda: self.now[0])
        with self.assertRaises(NotReady):
            other.claim_import(self.message, self.keys[0])
        self.now[0] += 181
        second = other.claim_import(self.message, self.keys[0])
        self.assertEqual(first["payload"], second["payload"])
        self.assertEqual(first["idempotency_key"], second["idempotency_key"])
        with self.assertRaises(StateConflict):
            self.gate.finish_import(
                self.message,
                self.keys[0],
                first["token"],
                *self.response("", self.payload)[:2],
            )

    def test_timeout_then_identical_retry_and_persisted_budget(self):
        self.prepare(self.keys[0])
        calls = []

        def timeout(key, payload):
            calls.append((key, payload))
            raise TimeoutError

        self.assertEqual(
            self.gate.process(self.message, self.keys[0], timeout).disposition, "retry"
        )
        with self.assertRaises(NotReady):
            self.gate.process(self.message, self.keys[0], self.response)
        self.now[0] += 60
        reopened = MessageGate(self.path, clock=lambda: self.now[0])

        def success(key, payload):
            calls.append((key, payload))
            return self.response(key, payload)

        self.assertTrue(
            reopened.process(self.message, self.keys[0], success).safe_to_archive
        )
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(
            reopened.snapshot(self.message)["attachments"][0]["attempts"], 2
        )

    def test_quarantine_survives_restart_and_blocks_message(self):
        self.gate.reject(self.message, self.keys[0], error_code="unsupported_profile")
        reopened = MessageGate(self.path)
        self.assertEqual(
            reopened.snapshot(self.message)["attachments"][0]["state"], "quarantine"
        )
        with self.assertRaises(NotReady):
            reopened.claim_move(self.message, target_identity="x")

    def test_permanent_error_stays_blocked(self):
        self.prepare(self.keys[0])

        def error(*args):
            return 503, {"error": {"code": "not_configured", "retryable": False}}, {}

        self.assertEqual(
            self.gate.process(self.message, self.keys[0], error).disposition, "blocked"
        )
        self.now[0] += 10000
        with self.assertRaises(NotReady):
            self.gate.claim_import(self.message, self.keys[0])

    def test_reviewed_config_resume_preserves_payload_and_records_event(self):
        self.prepare(self.keys[0])
        self.gate.process(
            self.message,
            self.keys[0],
            lambda *a: (
                503,
                {"error": {"code": "not_configured", "retryable": False}},
                {},
            ),
        )
        with self.assertRaises(GateError):
            self.gate.reviewed_resume(
                self.message,
                self.keys[0],
                operator_id="",
                approval_reference="ticket-1",
                reason="api_configuration_repaired",
                reviewed_outcome="same_key_payload_safe",
                expected_state="blocked",
            )
        event = self.gate.reviewed_resume(
            self.message,
            self.keys[0],
            operator_id="synthetic-admin",
            approval_reference="ticket-1",
            reason="api_configuration_repaired",
            reviewed_outcome="same_key_payload_safe",
            expected_state="blocked",
        )
        self.assertTrue(uuid.UUID(event))
        self.assertTrue(
            self.gate.process(self.message, self.keys[0], self.response).safe_to_archive
        )
        with self.assertRaises(StateConflict):
            self.gate.reviewed_resume(
                self.message,
                self.keys[0],
                operator_id="synthetic-admin",
                approval_reference="ticket-2",
                reason="api_configuration_repaired",
                reviewed_outcome="same_key_payload_safe",
                expected_state="blocked",
            )

    def test_reviewed_resume_cannot_bypass_payload_conflict_or_unparsed_source(self):
        self.prepare(self.keys[0])
        self.gate.process(
            self.message,
            self.keys[0],
            lambda *a: (
                409,
                {"error": {"code": "idempotency_conflict", "retryable": False}},
                {},
            ),
        )
        self.gate.reject(
            self.message, self.keys[1], error_code="parse_or_validation_failed"
        )
        for key in self.keys:
            with self.assertRaises(StateConflict):
                self.gate.reviewed_resume(
                    self.message,
                    key,
                    operator_id="synthetic-admin",
                    approval_reference="ticket-1",
                    reason="backend_constraint_reconciled",
                    reviewed_outcome="same_key_payload_safe",
                    expected_state="quarantine",
                )

    def test_retry_exhaustion_persists(self):
        self.prepare(self.keys[0])
        for _ in range(4):
            self.gate.process(self.message, self.keys[0], lambda *a: (429, {}, {}))
            self.now[0] += 1000
        self.assertEqual(
            self.gate.snapshot(self.message)["attachments"][0]["state"], "quarantine"
        )
        with self.assertRaises(NotReady):
            self.gate.claim_import(self.message, self.keys[0])

    def test_committed_move_once(self):
        for k in self.keys:
            self.accept(k)
        calls = []
        self.gate.move(
            self.message,
            target_identity="folder-A",
            mover=lambda target: calls.append(target) or "moved-id",
        )
        self.assertEqual(calls, ["folder-A"])
        self.assertEqual(
            self.gate.snapshot(self.message)["message"]["state"], "archived"
        )
        with self.assertRaises(NotReady):
            self.gate.move(
                self.message,
                target_identity="folder-A",
                mover=lambda _: self.fail("second move"),
            )

    def test_ambiguous_move_never_replayed(self):
        for k in self.keys:
            self.accept(k)

        def timeout(target):
            raise TimeoutError

        with self.assertRaises(TimeoutError):
            self.gate.move(self.message, target_identity="folder-A", mover=timeout)
        reopened = MessageGate(self.path)
        self.assertEqual(
            reopened.snapshot(self.message)["message"]["state"], "archiving"
        )
        with self.assertRaises(NotReady):
            reopened.move(
                self.message,
                target_identity="folder-A",
                mover=lambda _: self.fail("unsafe replay"),
            )

    def test_restart_can_reconcile_ambiguous_move_without_database_access(self):
        for k in self.keys:
            self.accept(k)

        def timeout(target):
            raise TimeoutError

        with self.assertRaises(TimeoutError):
            self.gate.move(self.message, target_identity="folder-A", mover=timeout)
        reopened = MessageGate(self.path)
        with self.assertRaises(StateConflict):
            reopened.pending_move_claim(self.message, target_identity="wrong-folder")
        self.assertFalse(
            reopened.reconcile_move(
                self.message, target_identity="folder-A", verifier=lambda _: None
            )
        )
        self.assertTrue(
            reopened.reconcile_move(
                self.message,
                target_identity="folder-A",
                verifier=lambda _: "verified-moved-id",
            )
        )
        self.assertEqual(
            reopened.snapshot(self.message)["message"]["state"], "archived"
        )

    def test_readback_confirmation_requires_same_target(self):
        for k in self.keys:
            self.accept(k)
        token = self.gate.claim_move(self.message, target_identity="folder-A")
        with self.assertRaises(StateConflict):
            self.gate.confirm_move(
                self.message, token, target_identity="folder-B", moved_message_id="x"
            )
        self.gate.confirm_move(
            self.message,
            token,
            target_identity="folder-A",
            moved_message_id="verified-read-back-id",
        )
        self.assertEqual(
            self.gate.snapshot(self.message)["message"]["state"], "archived"
        )

    def test_store_permissions_and_status_redaction(self):
        self.prepare(self.keys[0])
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertNotIn("Quantity", json.dumps(self.gate.snapshot(self.message)))

    def test_no_network_in_gate(self):
        with patch.object(
            socket.socket, "connect", side_effect=AssertionError("network forbidden")
        ):
            self.accept(self.keys[0])
            self.accept(self.keys[1])
            self.assertTrue(self.gate.ready_to_move(self.message))


if __name__ == "__main__":
    unittest.main(verbosity=2)
