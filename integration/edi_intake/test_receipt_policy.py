import base64
import json
import tempfile
import unittest
import uuid
from receipt_policy import (
    classify,
    safe_filename,
    validate_attachment,
    source_key,
    validate_envelope,
    message_ready,
    persist_quarantine,
)


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.encoded = base64.b64encode(b"Synthetic EDI file").decode()
        self.ok = {
            "status": "imported",
            "import_id": str(uuid.uuid4()),
            "file_type": "EDI",
            "rows_imported": 2,
            "rows_received": 2,
        }

    def classify(self, status, body, **kw):
        return classify(status, body, expected_type="EDI", expected_rows=2, **kw)

    def test_success_and_replay(self):
        self.assertTrue(self.classify(201, self.ok).safe_to_archive)
        replay = {**self.ok, "status": "already_imported"}
        self.assertTrue(self.classify(200, replay).safe_to_archive)

    def test_aggregated_delivery_ack(self):
        receipt = {
            **self.ok,
            "file_type": "LIVRAISON",
            "rows_imported": 1,
            "rows_received": 2,
        }
        self.assertTrue(
            classify(
                201, receipt, expected_type="LIVRAISON", expected_rows=2
            ).safe_to_archive
        )

    def test_bad_ack_never_archives(self):
        for status, body in [
            (202, self.ok),
            (200, self.ok),
            (201, {}),
            (201, "html"),
            (201, {**self.ok, "rows_received": 1}),
            (201, {**self.ok, "rows_imported": 1}),
            (201, {**self.ok, "rows_imported": True}),
            (201, {**self.ok, "import_id": "x"}),
            (201, {**self.ok, "file_type": "LIVRAISON"}),
        ]:
            self.assertFalse(self.classify(status, body).safe_to_archive)

    def test_conflicts_quarantine(self):
        for status in (400, 409, 413, 415, 422):
            self.assertEqual(self.classify(status, {}).disposition, "quarantine")

    def test_bounded_retry(self):
        for attempt, delay in [(1, 60), (2, 300), (3, 900)]:
            self.assertEqual(
                self.classify(429, {}, attempt=attempt).retry_after_seconds, delay
            )
        self.assertEqual(self.classify(429, {}, attempt=4).disposition, "quarantine")
        self.assertEqual(
            self.classify(429, {}, retry_after="3600").retry_after_seconds, 3600
        )

    def test_long_retry_after_blocks_for_review(self):
        self.assertEqual(
            self.classify(429, {}, retry_after="100000").disposition, "blocked"
        )

    def test_explicit_permanent_server_error_not_retried(self):
        for status in (500, 502, 504):
            self.assertEqual(
                self.classify(
                    status, {"error": {"code": "internal_error", "retryable": False}}
                ).disposition,
                "blocked",
            )
            self.assertEqual(self.classify(status, {}).disposition, "retry")

    def test_malformed_retry_flag_blocks(self):
        for value in (None, "false", "true", 0, 1):
            self.assertEqual(
                self.classify(500, {"error": {"retryable": value}}).disposition,
                "blocked",
            )

    def test_configuration_not_retried(self):
        self.assertEqual(
            self.classify(
                503, {"error": {"code": "not_configured", "retryable": False}}
            ).disposition,
            "blocked",
        )
        self.assertEqual(
            self.classify(503, {"error": {"retryable": True}}).disposition, "retry"
        )
        for status in (401, 403, 404, 301):
            self.assertEqual(self.classify(status, {}).disposition, "blocked")

    def test_network_error_retry(self):
        self.assertEqual(self.classify(0, None).disposition, "retry")

    def test_filename_security(self):
        for name in [
            "../CON.pdf",
            'a\\b:<c>|"?*.PDF',
            "a" * 600 + ".xlsx",
            "nul\0name.csv",
            "測試.csv",
            ".",
            "",
        ]:
            n = safe_filename(name, self.encoded)
            self.assertLessEqual(len(n), 146)
            self.assertRegex(n, r"^[0-9a-f]{64}_[A-Za-z0-9_-]{0,72}\.[a-z0-9]{1,8}$")
        self.assertEqual(
            safe_filename("x.pdf", self.encoded), safe_filename("x.pdf", self.encoded)
        )
        self.assertNotEqual(
            safe_filename("x.pdf", self.encoded),
            safe_filename("x.pdf", base64.b64encode(b"changed").decode()),
        )

    def test_attachment_validation(self):
        self.assertEqual(
            validate_attachment("x.PDF", self.encoded), b"Synthetic EDI file"
        )
        for name, content in [
            ("x.exe", self.encoded),
            ("x.pdf", "%%%"),
            ("x.pdf", ""),
            ("nodot", self.encoded),
        ]:
            with self.assertRaises(ValueError):
                validate_attachment(name, content)

    def test_stable_source_key(self):
        args = (
            "Frankfurt",
            "edi@example.invalid",
            "<stable-message@example.invalid>",
            "x.pdf",
            b"x",
        )
        self.assertEqual(source_key(*args), source_key(*args))
        self.assertRegex(source_key(*args), r"^edi:v1:[0-9a-f]{64}$")
        with self.assertRaises(ValueError):
            source_key("x", "", "m", "f", b"x")

    def test_payload_validation(self):
        validate_envelope({"file_type": "EDI", "rows": [{}]}, "edi:v1:abc")
        for p, k in [
            ({"file_type": "EDI", "rows": []}, "x"),
            ({"file_type": "EDI", "rows": [{}], "raw": "x"}, "x"),
            ({"file_type": "PDF", "rows": [{}]}, "x"),
            ({"file_type": "EDI", "rows": [{}]}, "bad key"),
            ({"file_type": "EDI", "rows": [{"Quantity": float("nan")}]}, "x"),
        ]:
            with self.assertRaises(ValueError):
                validate_envelope(p, k)

    def test_all_attachment_gate(self):
        good = self.classify(201, self.ok)
        self.assertTrue(message_ready(["a", "b"], {"a": good, "b": good}))
        self.assertFalse(message_ready(["a", "b"], {"a": good}))
        self.assertFalse(message_ready(["a"], {"a": self.classify(422, {})}))
        self.assertFalse(message_ready([], {}))
        self.assertFalse(message_ready(["a", "a"], {"a": good}))

    def test_quarantine_is_durable_and_redacted(self):
        with tempfile.TemporaryDirectory() as d:
            p = persist_quarantine(
                d,
                "edi:v1:abc",
                self.classify(422, {"error": {"code": "validation_failed"}}),
                content_sha256="a" * 64,
                source_reference_hash="b" * 64,
            )
            saved = json.loads(p.read_text())
            self.assertEqual(saved["error_code"], "validation_failed")
            self.assertEqual(p.stat().st_mode & 0o777, 0o600)
            self.assertNotIn("file_content", p.read_text())


if __name__ == "__main__":
    unittest.main(verbosity=2)
