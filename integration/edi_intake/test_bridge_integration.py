"""Real local Flask API + durable gate. Every source/credential here is synthetic."""

import hashlib
import io
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch
import uuid

from edi_stock import create_app
from edi_stock.repository import ImportConflict, payload_hash
from parser_adapter import parse_attachment
from parser_adapter.tests.test_adapter import LEGACY_ROW, csv_bytes
from intake_bridge import Attachment, IntakeBridge, NormalizedHTTPTransport
from message_gate import MessageGate, GateError, NotReady


class MemoryRepository:
    def __init__(self):
        self.saved = {}
        self.writes = 0

    def import_rows(self, file_type, rows, key):
        digest = payload_hash(file_type, rows)
        if key in self.saved:
            previous, receipt = self.saved[key]
            if previous != digest:
                raise ImportConflict("synthetic conflict")
            return {**receipt, "status": "already_imported"}
        receipt = {
            "status": "imported",
            "import_id": str(uuid.uuid4()),
            "rows_imported": len(rows),
            "file_type": file_type,
        }
        self.saved[key] = digest, receipt
        self.writes += 1
        return receipt


def native_parser(data, name, file_type, profile):
    return parse_attachment(data, name, file_type, profile=profile)


CSV = b"Site,AVOMaterialNo,DeliveryNo,Quantity,Date,Status\nTunisia,001,D001,12,2026-10-05,Dispatched\n"


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.no_network = patch.object(
            socket.socket, "connect", side_effect=AssertionError("network forbidden")
        )
        self.no_network.start()
        self.tmp = tempfile.TemporaryDirectory()
        self.now = [1000]
        self.gate = MessageGate(
            Path(self.tmp.name) / "state.sqlite", clock=lambda: self.now[0]
        )
        self.repo = MemoryRepository()
        self.app = create_app(
            {
                "TESTING": True,
                "SECRET_KEY": "synthetic-session-key",
                "IMPORT_API_TOKEN": "synthetic-test-token",
                "DATABASE_URL": None,
                "PREVIEW_DIR": self.tmp.name,
            },
            repository=self.repo,
        )
        self.client = self.app.test_client()
        self.bridge = IntakeBridge(self.gate, native_parser)

    def tearDown(self):
        self.no_network.stop()
        self.tmp.cleanup()

    def capture(self, attachments, site="Tunisia"):
        return self.bridge.capture(
            site=site,
            mailbox="test@example.invalid",
            internet_message_id="<synthetic@example.invalid>",
            attachments=attachments,
            listing_complete=True,
        )

    def importer(self, key, payload):
        r = self.client.post(
            "/api/v1/imports",
            json=payload,
            headers={
                "Authorization": "Bearer synthetic-test-token",
                "Idempotency-Key": key,
            },
        )
        return r.status_code, r.get_json(), dict(r.headers)

    def archive(self, message, key, data):
        self.gate.record_source_archive(
            message,
            key,
            provider="sharepoint",
            item_id="synthetic-" + key,
            content_sha256=hashlib.sha256(data).hexdigest(),
        )

    def test_csv_to_real_flask_to_durable_receipt_and_gate(self):
        message, keys = self.capture([Attachment("fixture.csv", CSV, "LIVRAISON")])
        decision = self.gate.process(message, keys[0], self.importer)
        self.assertTrue(decision.safe_to_archive)
        self.assertEqual(self.repo.writes, 1)
        self.assertFalse(self.gate.ready_to_move(message))
        self.archive(message, keys[0], CSV)
        self.assertTrue(self.gate.ready_to_move(message))
        self.gate.move(
            message,
            target_identity="synthetic-folder",
            mover=lambda _: "synthetic-moved-id",
        )
        self.assertEqual(self.gate.snapshot(message)["message"]["state"], "archived")

    def test_grounded_legacy_germany_valeo_csv_to_real_api(self):
        raw = csv_bytes([LEGACY_ROW])
        message, keys = self.capture(
            [
                Attachment(
                    "valeo.csv", raw, "EDI", parser_profile="legacy-valeo-germany-v1"
                )
            ],
            site="Germany",
        )
        decision = self.gate.process(message, keys[0], self.importer)
        self.assertTrue(decision.safe_to_archive)
        self.assertEqual(decision.rows_imported, 1)
        self.archive(message, keys[0], raw)
        self.assertTrue(self.gate.ready_to_move(message))

    def test_site_mismatch_quarantines_before_import(self):
        raw = csv_bytes([LEGACY_ROW])
        message, keys = self.capture(
            [
                Attachment(
                    "valeo.csv", raw, "EDI", parser_profile="legacy-valeo-germany-v1"
                )
            ],
            site="Tunisia",
        )
        self.assertEqual(
            self.gate.snapshot(message)["attachments"][0]["state"], "quarantine"
        )
        with self.assertRaises(NotReady):
            self.gate.process(message, keys[0], self.importer)
        self.assertEqual(self.repo.writes, 0)

    def test_commit_then_timeout_replays_same_payload_once(self):
        message, keys = self.capture([Attachment("fixture.csv", CSV, "LIVRAISON")])
        calls = []

        def ambiguous(key, payload):
            calls.append((key, payload))
            self.importer(key, payload)
            raise TimeoutError

        self.assertEqual(
            self.gate.process(message, keys[0], ambiguous).disposition, "retry"
        )
        self.now[0] += 60

        def replay(key, payload):
            calls.append((key, payload))
            return self.importer(key, payload)

        self.assertTrue(self.gate.process(message, keys[0], replay).safe_to_archive)
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(self.repo.writes, 1)

    def test_one_bad_attachment_blocks_whole_message(self):
        message, keys = self.capture(
            [
                Attachment("fixture.csv", CSV, "LIVRAISON"),
                Attachment("unsupported.pdf", b"%PDF-synthetic", "EDI"),
            ]
        )
        self.gate.process(message, keys[0], self.importer)
        self.archive(message, keys[0], CSV)
        self.assertEqual(
            self.gate.snapshot(message)["attachments"][0]["state"]
            in ("accepted", "quarantine"),
            True,
        )
        with self.assertRaises(NotReady):
            self.gate.move(
                message,
                target_identity="folder",
                mover=lambda _: self.fail("must not move"),
            )
        self.assertEqual(self.repo.writes, 1)

    def test_repeated_capture_does_not_reparse_frozen_payload(self):
        attachment = Attachment("fixture.csv", CSV, "LIVRAISON")
        self.capture([attachment])
        self.bridge.parser = lambda *a, **k: self.fail("reparse forbidden")
        self.capture([attachment])

    def test_nonboolean_inline_cannot_hide_attachment(self):
        with self.assertRaises(GateError):
            self.capture([Attachment("fixture.csv", CSV, "LIVRAISON", inline="yes")])

    def test_missing_secure_transport_configuration_is_persisted_blocked(self):
        message, keys = self.capture([Attachment("fixture.csv", CSV, "LIVRAISON")])
        transport = NormalizedHTTPTransport(
            "https://staging.invalid/api/v1/imports", lambda: None
        )
        self.assertEqual(
            self.gate.process(message, keys[0], transport).disposition, "blocked"
        )
        self.assertEqual(
            self.gate.snapshot(message)["attachments"][0]["state"], "blocked"
        )

    def test_transport_rejects_legacy_endpoints_and_insecure_urls(self):
        for endpoint in [
            "https://sts-api.azurewebsites.net/process-GermanySite",
            "http://x/api/v1/imports",
            "https://user:pass@x/api/v1/imports",
            "https://x/api/v1/imports?token=x",
        ]:
            with self.assertRaises(GateError):
                NormalizedHTTPTransport(endpoint, lambda: "Bearer synthetic")

    def test_transport_request_is_normalized_and_bounded(self):
        class Response(io.BytesIO):
            status = 201
            headers = {}

        class Opener:
            request = None
            timeout = None

            def open(inner, request, timeout):
                inner.request = request
                inner.timeout = timeout
                return Response(b'{"status":"imported"}')

        opener = Opener()
        transport = NormalizedHTTPTransport(
            "https://staging.invalid/api/v1/imports",
            lambda: "Bearer synthetic",
            opener=opener,
        )
        payload = native_parser(CSV, "fixture.csv", "LIVRAISON", "normalized-csv-v1")
        status, body, _ = transport("edi:v1:test", payload)
        self.assertEqual(status, 201)
        self.assertEqual(body["status"], "imported")
        self.assertEqual(json.loads(opener.request.data), payload)
        self.assertEqual(opener.timeout, 90)
        self.assertNotIn("file_content_base64", json.loads(opener.request.data))


if __name__ == "__main__":
    unittest.main(verbosity=2)
