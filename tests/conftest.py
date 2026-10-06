import socket
import pytest

from edi_stock import create_app
from edi_stock.repository import ImportConflict, payload_hash


class MemoryRepository:
    def __init__(self):
        self.calls = []
        self.saved = {}
        self.fail = None

    def import_rows(self, file_type, rows, key):
        self.calls.append((file_type, rows, key))
        if self.fail:
            raise self.fail
        digest = payload_hash(file_type, rows)
        if key in self.saved:
            old, receipt = self.saved[key]
            if old != digest:
                raise ImportConflict('Key conflict')
            return {**receipt, 'status': 'already_imported'}
        receipt = {'status': 'imported', 'import_id': '00000000-0000-0000-0000-000000000001',
                   'rows_imported': len(rows), 'file_type': file_type}
        self.saved[key] = digest, receipt
        return receipt


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError('Tests must not access any network or real database')
    monkeypatch.setattr(socket.socket, 'connect', blocked)
    monkeypatch.delenv('DATABASE_URL', raising=False)
    monkeypatch.delenv('SECRET_KEY', raising=False)
    monkeypatch.delenv('IMPORT_API_TOKEN', raising=False)


@pytest.fixture
def repository():
    return MemoryRepository()


@pytest.fixture
def app(tmp_path, repository):
    return create_app({'TESTING': True, 'SECRET_KEY': 'test-only-session-key-do-not-use-in-production',
                       'IMPORT_API_TOKEN': 'test-only-api-token', 'SESSION_COOKIE_SECURE': False,
                       'PREVIEW_DIR': str(tmp_path), 'UI_IMPORTS_ENABLED': True}, repository=repository)


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def row():
    return {'Site': 'Tunisia', 'AVOMaterialNo': '00123', 'DeliveryNo': '00007', 'Quantity': 12,
            'Date': '2026-10-05', 'Status': 'Dispatched'}


@pytest.fixture
def edi_row():
    return {'Site': 'Germany', 'ClientCode': '0001', 'ClientMaterialNo': '002',
            'AVOMaterialNo': '003', 'DateFrom': '2026-W41', 'DateUntil': '2026-W42',
            'Quantity': 0, 'ForecastDate': '2026-W40', 'EDIStatus': 'Firm'}


@pytest.fixture
def headers():
    return {'Authorization': 'Bearer test-only-api-token', 'Idempotency-Key': 'message-001:attachment-002'}
