import ast
import base64
import csv
import hashlib
import io
import json
import subprocess
import sys
import unittest
from pathlib import Path

from parser_adapter import AdapterError, parse_attachment, parse_base64_attachment
from parser_adapter.adapter import MATERIAL_TO_AVO, PLANT_TO_CLIENT, VALEO_GERMANY

ROOT = Path(__file__).resolve().parents[1]
VALIDATION_SHA256 = "fc6ca3b0125eaa0b8f3ccd7d10e4cdc4e7cfa63986c5cabc47404c2e49bdf266"
LEGACY_ROW = {
    "Org_Name_Customer": "Valeo", "Customer_No": "001234", "Plant_No": "CZ22",
    "Material_No_Customer": "00190313", "Delivery_Date": "2026-10-12",
    "Date": "2026-10-07", "Despatch_Qty": "120",
    "Last_Delivery_Note_Date": "2026-10-01", "Last_Delivery_Quantity": "30",
    "Cum_Quantity": "1000", "Commitment_Level": "P", "Description": "Synthetic brush",
    "Last_Delivery_Note": "000007",
}
DELIVERY_ROW = {
    "Site": "Tunisia", "AVOMaterialNo": "000123 PL", "DeliveryNo": "000007",
    "Quantity": "12", "Date": "2026-10-05", "Status": "Sent",
}
EDI_ROW = {
    "Site": "Germany", "ClientCode": "00100", "ClientMaterialNo": "000321",
    "AVOMaterialNo": "000123", "DateFrom": "2026-W41", "DateUntil": "2026-W42", "Quantity": "0",
    "ForecastDate": "2026-10-05", "EDIStatus": "Forecast",
}


def csv_bytes(rows, *, delimiter=";", encoding="utf-8", fields=None):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields or list(rows[0]), delimiter=delimiter)
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode(encoding)


def legacy(rows=None, **options):
    return parse_attachment(csv_bytes(rows or [LEGACY_ROW]), "synthetic.csv", "EDI",
                            profile=VALEO_GERMANY, **options)


class AdapterTests(unittest.TestCase):
    def test_normalized_delivery_contract_and_aggregation(self):
        result = parse_attachment(csv_bytes([DELIVERY_ROW, DELIVERY_ROW]), "delivery.CSV", "LIVRAISON")
        self.assertEqual(result, {"file_type": "LIVRAISON", "rows": [{
            "Site": "Tunisia", "AVOMaterialNo": "000123PL", "DeliveryNo": "000007",
            "Quantity": 24, "Date": "2026-10-05", "Status": "Dispatched",
        }]})
        self.assertEqual(json.loads(json.dumps(result, allow_nan=False)), result)

    def test_normalized_edi_keeps_leading_zeroes_and_optional_nulls(self):
        result = parse_attachment(csv_bytes([EDI_ROW]), "edi.csv", "EDI")
        self.assertEqual(set(result), {"file_type", "rows"})
        row = result["rows"][0]
        self.assertEqual(row["ClientCode"], "00100")
        self.assertEqual(row["ClientMaterialNo"], "000321")
        self.assertEqual(row["Quantity"], 0)
        self.assertEqual(row["DateUntil"], "2026-W42")
        self.assertIsNone(row["LastDeliveryDate"])
        self.assertEqual(len(row), 14)

    def test_normalized_delimiters_and_utf8_bom(self):
        for delimiter in (",", ";", "\t", "|"):
            with self.subTest(delimiter=delimiter):
                content = b"\xef\xbb\xbf" + csv_bytes([DELIVERY_ROW], delimiter=delimiter)
                self.assertEqual(parse_attachment(content, "delivery.csv", "LIVRAISON")["rows"][0]["Quantity"], 12)

    def test_quotes_and_embedded_newline_are_not_destructively_replaced(self):
        row = {**LEGACY_ROW, "Description": 'Synthetic, "quoted"; part\nline two'}
        self.assertEqual(legacy([row])["rows"][0]["ProductName"], row["Description"])

    def test_normalized_all_rows_fail_together(self):
        for bad in ({"Quantity": "0"}, {"Quantity": "1.2"}, {"Quantity": "12 kg"},
                    {"Date": "2026-02-30"}, {"Site": ""}, {"Status": "unknown"}):
            with self.subTest(bad=bad), self.assertRaises(AdapterError):
                parse_attachment(csv_bytes([DELIVERY_ROW, {**DELIVERY_ROW, **bad}]), "data.csv", "LIVRAISON")

    def test_csv_duplicate_unknown_missing_headers_fail(self):
        samples = [
            b"Site,Site,Quantity\nGermany,Germany,1\n",
            csv_bytes([{**DELIVERY_ROW, "Unknown": "x"}]),
            csv_bytes([{k: v for k, v in DELIVERY_ROW.items() if k != "Status"}]),
        ]
        for content in samples:
            with self.subTest(content=content), self.assertRaises(AdapterError):
                parse_attachment(content, "data.csv", "LIVRAISON")

    def test_csv_shape_and_syntax_fail(self):
        valid = csv_bytes([DELIVERY_ROW])
        for content in (valid + b"a;b\n", valid + b'"unterminated\n', valid.rstrip() + b";unexpected\n"):
            with self.subTest(content=content), self.assertRaises(AdapterError):
                parse_attachment(content, "data.csv", "LIVRAISON")

    def test_empty_attachment_and_header_only_fail(self):
        for content in (b"", b"  \n", csv_bytes([DELIVERY_ROW]).splitlines()[0] + b"\n"):
            with self.subTest(content=content), self.assertRaises(AdapterError):
                parse_attachment(content, "data.csv", "LIVRAISON")

    def test_limits_apply_before_delivery_grouping(self):
        with self.assertRaises(AdapterError) as error:
            parse_attachment(csv_bytes([DELIVERY_ROW, DELIVERY_ROW]), "data.csv", "LIVRAISON", max_rows=1)
        self.assertEqual(error.exception.code, "too_many_rows")
        with self.assertRaises(AdapterError):
            parse_attachment(csv_bytes([DELIVERY_ROW]), "data.csv", "LIVRAISON", max_bytes=10)
        with self.assertRaises(AdapterError):
            parse_attachment(csv_bytes([{**DELIVERY_ROW, "Site": "x" * 1001}]), "data.csv", "LIVRAISON")

    def test_configuration_is_bounded(self):
        for options in ({"max_rows": True}, {"max_rows": 10001}, {"max_rows": 0},
                        {"max_bytes": 10485761}, {"max_bytes": 0},
                        {"profile": "auto"}, {"encoding": "utf-16"}, {"delimiter": ":"}):
            with self.subTest(options=options), self.assertRaises(AdapterError):
                parse_attachment(csv_bytes([DELIVERY_ROW]), "data.csv", "LIVRAISON", **options)

    def test_explicit_encoding_only(self):
        content = csv_bytes([{**LEGACY_ROW, "Description": "Pièce synthétique"}], encoding="latin-1")
        with self.assertRaises(AdapterError):
            parse_attachment(content, "data.csv", "EDI", profile=VALEO_GERMANY)
        result = parse_attachment(content, "data.csv", "EDI", profile=VALEO_GERMANY, encoding="latin-1")
        self.assertEqual(result["rows"][0]["ProductName"], "Pièce synthétique")

    def test_binaries_and_unsupported_extensions_fail_closed(self):
        for filename, content in (("test.pdf", b"%PDF-1.7"), ("test.xlsx", b"PK\x03\x04"),
                                  ("masked.csv", b"%PDF-1.7"), ("nul.csv", b"a\x00b"),
                                  ("noextension", csv_bytes([EDI_ROW]))):
            with self.subTest(filename=filename), self.assertRaises(AdapterError):
                parse_attachment(content, filename, "EDI")

    def test_profile_is_explicit_and_type_is_not_inferred(self):
        with self.assertRaises(AdapterError):
            parse_attachment(csv_bytes([LEGACY_ROW]), "Germany.csv", "EDI")
        with self.assertRaises(AdapterError):
            parse_attachment(csv_bytes([LEGACY_ROW]), "data.csv", "LIVRAISON", profile=VALEO_GERMANY)
        with self.assertRaises(AdapterError):
            parse_attachment(csv_bytes([LEGACY_ROW]), "data.csv", "csv", profile=VALEO_GERMANY)

    def test_legacy_golden_exact_envelope(self):
        self.assertEqual(legacy(), {"file_type": "EDI", "rows": [{
            "Site": "Germany", "ClientCode": "100442", "ClientMaterialNo": "00190313",
            "AVOMaterialNo": "1023093", "DateFrom": "2026-W42", "DateUntil": "2026-10-12",
            "Quantity": 120, "ForecastDate": "2026-W41", "LastDeliveryDate": "2026-W40",
            "LastDeliveredQuantity": 30, "CumulatedQuantity": 1000, "EDIStatus": "Forecast",
            "ProductName": "Synthetic brush", "LastDeliveryNo": "000007",
        }]})
        self.assertEqual(json.loads(json.dumps(legacy(), allow_nan=False)), legacy())

    def test_all_source_plant_and_product_mappings(self):
        for plant, client in PLANT_TO_CLIENT.items():
            with self.subTest(plant=plant):
                self.assertEqual(legacy([{**LEGACY_ROW, "Plant_No": plant}])["rows"][0]["ClientCode"], client)
        for material, avo in MATERIAL_TO_AVO.items():
            with self.subTest(material=material):
                self.assertEqual(legacy([{**LEGACY_ROW, "Material_No_Customer": material}])["rows"][0]["AVOMaterialNo"], avo)

    def test_iso_week_year_boundary_and_no_unused_shift(self):
        for original, expected in (("2021-01-01", "2020-W53"), ("2025-12-31", "2026-W01"),
                                   ("2026-10-07", "2026-W41"), ("07.10.2026", "2026-W41")):
            with self.subTest(original=original):
                self.assertEqual(legacy([{**LEGACY_ROW, "Date": original}])["rows"][0]["ForecastDate"], expected)

    def test_legacy_delivery_week_dot_dates_and_empty_optionals(self):
        row = {**LEGACY_ROW, "Delivery_Date": "CW 42/2026", "Last_Delivery_Note_Date": "",
               "Last_Delivery_Quantity": "", "Cum_Quantity": "", "Description": "", "Last_Delivery_Note": ""}
        result = legacy([row])["rows"][0]
        self.assertEqual(result["DateFrom"], "2026-W42")
        self.assertEqual(result["DateUntil"], "2026-W42")
        self.assertIsNone(result["LastDeliveryDate"])
        self.assertEqual(result["LastDeliveredQuantity"], 0)
        self.assertIsNone(result["ProductName"])
        self.assertEqual(legacy([{**row, "Delivery_Date": "12.10.2026"}])["rows"][0]["DateFrom"], "2026-W42")
        self.assertEqual(legacy([{**row, "Delivery_Date": "12.10.2026"}])["rows"][0]["DateUntil"], "2026-10-12")

    def test_legacy_optional_customer_number_and_unused_extra_columns(self):
        row = {k: v for k, v in LEGACY_ROW.items() if k != "Customer_No"}
        row["IgnoredSourceColumn"] = "synthetic"
        self.assertEqual(legacy([row]), legacy())

    def test_legacy_invalid_row_is_not_silently_dropped(self):
        for bad in ({"Org_Name_Customer": "Other"}, {"Plant_No": "UNKNOWN"},
                    {"Material_No_Customer": "unknown"}, {"Customer_No": "notnumeric"},
                    {"Date": "2026-02-30"}, {"Date": "07/10/2026"},
                    {"Delivery_Date": "BACKORDER"}, {"Delivery_Date": "CW 53/2025"},
                    {"Delivery_Date": "CW 42/2026 extra"}, {"Last_Delivery_Note_Date": "0001-01-01"},
                    {"Despatch_Qty": "1,000"}, {"Despatch_Qty": "1.2"},
                    {"Despatch_Qty": "-1"}, {"Despatch_Qty": "2147483648"}, {"Despatch_Qty": "12 kg"},
                    {"Commitment_Level": "unknown"}):
            with self.subTest(bad=bad), self.assertRaises(AdapterError):
                legacy([LEGACY_ROW, {**LEGACY_ROW, **bad}])

    def test_normalized_integer_overflow_blocks_aggregate(self):
        row = {**DELIVERY_ROW, "Quantity": "2147483647"}
        with self.assertRaises(AdapterError):
            parse_attachment(csv_bytes([row, DELIVERY_ROW]), "data.csv", "LIVRAISON")

    def test_base64_helper_returns_same_envelope(self):
        content = csv_bytes([DELIVERY_ROW])
        result = parse_base64_attachment(base64.b64encode(content).decode("ascii"), "data.csv", "LIVRAISON")
        self.assertEqual(result, parse_attachment(content, "data.csv", "LIVRAISON"))

    def test_noncanonical_base64_is_rejected(self):
        for value in ("!!", "YWJj\n", "YWJj====", "YQ", "YR==", "é", "data:text/csv;base64,YQ=="):
            with self.subTest(value=value), self.assertRaises(AdapterError):
                parse_base64_attachment(value, "data.csv", "LIVRAISON")

    def test_validation_snapshot_integrity(self):
        self.assertEqual(hashlib.sha256((ROOT / "_contract_validation.py").read_bytes()).hexdigest(), VALIDATION_SHA256)

    def test_vendored_contract_matches_server_when_available(self):
        server = ROOT.parents[2] / "edi_stock" / "validation.py"
        if not server.is_file():
            self.skipTest("Standalone parser installation has no application checkout.")
        self.assertEqual((ROOT / "_contract_validation.py").read_bytes(), server.read_bytes())

    def test_normalized_edi_requires_date_until_header_and_calendar_value(self):
        missing = {key: value for key, value in EDI_ROW.items() if key != "DateUntil"}
        with self.assertRaises(AdapterError) as error:
            parse_attachment(csv_bytes([missing]), "edi.csv", "EDI")
        self.assertEqual(error.exception.code, "unsupported_headers")
        for value in ("", "2026-02-30", "2025-W53", "BACKORDER"):
            with self.subTest(value=value), self.assertRaises(AdapterError) as error:
                parse_attachment(csv_bytes([EDI_ROW, {**EDI_ROW, "DateUntil": value}]), "edi.csv", "EDI")
            self.assertEqual(error.exception.code, "row_validation_failed")
            self.assertEqual(error.exception.errors[0]["column"], "DateUntil")
            self.assertEqual(error.exception.errors[0]["row"], 3)

    def test_database_text_boundaries_are_checked_before_envelope_is_returned(self):
        for file_type, source, column, limit in (
            ("EDI", EDI_ROW, "Site", 50), ("EDI", EDI_ROW, "ClientCode", 50),
            ("EDI", EDI_ROW, "ClientMaterialNo", 50), ("EDI", EDI_ROW, "AVOMaterialNo", 50),
            ("EDI", EDI_ROW, "ProductName", 100), ("EDI", EDI_ROW, "LastDeliveryNo", 50),
            ("LIVRAISON", DELIVERY_ROW, "Site", 20), ("LIVRAISON", DELIVERY_ROW, "AVOMaterialNo", 30),
            ("LIVRAISON", DELIVERY_ROW, "DeliveryNo", 28),
        ):
            with self.subTest(file_type=file_type, column=column):
                row = {**source, column: "é" * limit}
                accepted = parse_attachment(csv_bytes([row]), "fixture.csv", file_type)
                self.assertEqual(accepted["rows"][0][column], row[column])
                with self.assertRaises(AdapterError) as error:
                    parse_attachment(csv_bytes([{**row, column: row[column] + "é"}]), "fixture.csv", file_type)
                self.assertEqual(error.exception.code, "row_validation_failed")
                self.assertEqual(error.exception.errors[0]["column"], column)
        for file_type, source, limit in (("EDI", EDI_ROW, 50), ("LIVRAISON", DELIVERY_ROW, 30)):
            with self.subTest(file_type=file_type, boundary="joined material suffix"):
                row = {**source, "AVOMaterialNo": "A" * (limit - 2) + " PL"}
                accepted = parse_attachment(csv_bytes([row]), "fixture.csv", file_type)
                self.assertEqual(accepted["rows"][0]["AVOMaterialNo"], "A" * (limit - 2) + "PL")
                with self.assertRaises(AdapterError):
                    parse_attachment(csv_bytes([{**row, "AVOMaterialNo": "A" + row["AVOMaterialNo"]}]),
                                     "fixture.csv", file_type)

    def test_legacy_product_name_limit_is_not_truncated(self):
        self.assertEqual(legacy([{**LEGACY_ROW, "Description": "P" * 100}])["rows"][0]["ProductName"], "P" * 100)
        with self.assertRaises(AdapterError) as error:
            legacy([{**LEGACY_ROW, "Description": "P" * 101}])
        self.assertEqual(error.exception.errors[0]["column"], "ProductName")

    def test_published_synthetic_examples_match_expected_payloads(self):
        for name, kind, profile in (("synthetic-normalized-delivery", "LIVRAISON", "normalized-csv-v1"),
                                    ("synthetic-normalized-edi", "EDI", "normalized-csv-v1"),
                                    ("synthetic-valeo-germany", "EDI", VALEO_GERMANY)):
            with self.subTest(example=name):
                example = ROOT / "examples" / name
                self.assertEqual(parse_attachment(example.with_suffix(".csv").read_bytes(), name + ".csv", kind,
                                                  profile=profile),
                                 json.loads(example.with_suffix(".expected.json").read_text()))

    def test_parser_import_graph_is_stdlib_or_vendored_only(self):
        allowed = {"base64", "binascii", "csv", "io", "re", "datetime", "math", "decimal"}
        for path in (ROOT / "adapter.py", ROOT / "_contract_validation.py"):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self.assertIn(alias.name, allowed)
                elif isinstance(node, ast.ImportFrom) and not node.level:
                    self.assertIn(node.module, allowed)
                elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    self.assertNotIn(node.func.id, {"open", "exec", "eval", "compile", "__import__"})

    def test_fresh_import_and_parse_under_io_tripwires(self):
        # A clean interpreter proves imports do not transitively load the Flask
        # app/repository. Runtime tripwires reject socket, DB, processes and file
        # I/O during actual parse. Only this harness starts the child process.
        script = '''
import builtins, sys
sys.dont_write_bytecode = True
original_import = builtins.__import__
blocked_roots = {"App", "edi_stock", "sqlalchemy", "psycopg", "psycopg2", "sqlite3", "requests", "urllib", "http", "flask", "apscheduler"}
def guarded_import(name, *args, **kwargs):
    if name.split(".")[0] in blocked_roots:
        raise AssertionError("Forbidden dependency: " + name)
    return original_import(name, *args, **kwargs)
builtins.__import__ = guarded_import
def audit(event, args):
    if event.startswith(("socket.", "sqlite3.", "subprocess.")):
        raise AssertionError("Forbidden I/O: " + event)
sys.addaudithook(audit)
from parser_adapter import parse_attachment
content = REPLACE_LEGACY_BYTES
delivery = REPLACE_DELIVERY_BYTES
def no_io(*args, **kwargs):
    raise AssertionError("File I/O during parse")
builtins.open = no_io
result = parse_attachment(content, "synthetic.csv", "EDI", profile="legacy-valeo-germany-v1")
assert result["rows"][0]["AVOMaterialNo"] == "1023093"
assert parse_attachment(delivery, "synthetic.csv", "LIVRAISON")["rows"][0]["Quantity"] == 12
assert not blocked_roots.intersection(sys.modules)
'''
        script = script.replace("REPLACE_LEGACY_BYTES", repr(csv_bytes([LEGACY_ROW])))
        script = script.replace("REPLACE_DELIVERY_BYTES", repr(csv_bytes([DELIVERY_ROW])))
        completed = subprocess.run([sys.executable, "-B", "-c", script], cwd=ROOT.parent,
                                   capture_output=True, text=True, timeout=20, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
