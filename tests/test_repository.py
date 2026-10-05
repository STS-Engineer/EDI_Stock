from contextlib import contextmanager
from unittest.mock import MagicMock
import pytest
from sqlalchemy.exc import OperationalError
from edi_stock.repository import ImportConflict, InventoryConflict, PostgresRepository, payload_hash


class Connection:
    def __init__(self, previous=None, transit=None, fail_insert=False):
        self.previous, self.transit, self.fail_insert = previous, transit or [], fail_insert
        self.queries = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.queries.append((sql, params))
        result = MagicMock()
        result.rowcount = 1
        result.mappings.return_value.first.return_value = self.previous
        result.mappings.return_value.all.return_value = self.transit
        if self.fail_insert and 'INSERT INTO "DeliveryDetails"' in sql:
            raise OperationalError('test', {}, Exception('mock failure'))
        return result


class Engine:
    def __init__(self, conn, fail_commit=False):
        self.conn, self.fail_commit = conn, fail_commit
        self.committed = self.rolled_back = False

    @contextmanager
    def begin(self):
        try:
            yield self.conn
            if self.fail_commit:
                raise OperationalError('COMMIT', {}, Exception('mock commit failure'))
            self.committed = True
        except Exception:
            self.rolled_back = True
            raise


def repository(conn, **kwargs):
    repo = PostgresRepository('unused-mock-url')
    repo._engine = Engine(conn, **kwargs)
    return repo


def test_dispatch_update_scoped_to_site_and_material(row):
    conn = Connection(transit=[{'DeliveryNo': 'old', 'Date': '2026-10-01', 'Quantity': 2}])
    repo = repository(conn)
    receipt = repo.import_rows('LIVRAISON', [row], 'ui:test')
    sql, params = next((s, p) for s, p in conn.queries if 'UPDATE "DeliveryDetails"' in s)
    assert '"Site"=:site' in sql
    assert 'COALESCE("AVOMaterialNo",\'\')=:material' in sql
    assert params['site'] == row['Site'] and params['material'] == row['AVOMaterialNo']
    assert params['qty'] == 14
    assert receipt['status'] == 'imported' and repo._engine.committed


def test_missing_transit_creates_transit_and_event(row):
    conn = Connection()
    repository(conn).import_rows('LIVRAISON', [row], 'test')
    inserts = [p for s, p in conn.queries if 'INSERT INTO "DeliveryDetails"' in s]
    assert len(inserts) == 2
    assert inserts[0][0]['Status'] == 'InTransit' and inserts[0][0]['DeliveryNo'].endswith('_T')


def test_idempotent_replay_has_no_writes(row):
    previous = {'payload_hash': payload_hash('LIVRAISON', [row]), 'import_id': 'id', 'row_count': 1, 'file_type': 'LIVRAISON'}
    conn = Connection(previous=previous)
    assert repository(conn).import_rows('LIVRAISON', [row], 'test')['status'] == 'already_imported'
    assert not any('INSERT' in s or 'UPDATE' in s for s, _ in conn.queries)


def test_mismatched_key_rolls_back(row):
    repo = repository(Connection(previous={'payload_hash': 'different'}))
    with pytest.raises(ImportConflict):
        repo.import_rows('LIVRAISON', [row], 'test')
    assert repo._engine.rolled_back


def test_insert_failure_rolls_back_and_no_ledger(row):
    conn = Connection(fail_insert=True)
    repo = repository(conn)
    with pytest.raises(OperationalError):
        repo.import_rows('LIVRAISON', [row], 'test')
    assert repo._engine.rolled_back
    assert not any('INSERT INTO edi_imports' in s for s, _ in conn.queries)


def test_commit_failure_does_not_acknowledge(row):
    repo = repository(Connection(), fail_commit=True)
    with pytest.raises(OperationalError):
        repo.import_rows('LIVRAISON', [row], 'test')
    assert not repo._engine.committed


def test_over_delivery_fails_without_clamping(row):
    row['Status'] = 'Delivered'
    repo = repository(Connection(transit=[{'DeliveryNo': 'old', 'Date': '2026-10-01', 'Quantity': 1}]))
    with pytest.raises(InventoryConflict):
        repo.import_rows('LIVRAISON', [row], 'test')
    assert repo._engine.rolled_back


def test_duplicate_transit_fails_safely(row):
    with pytest.raises(InventoryConflict):
        repository(Connection(transit=[{}, {}])).import_rows('LIVRAISON', [row], 'test')


def test_edi_bulk_insert():
    conn = Connection()
    rows = [{'Quantity': 1}, {'Quantity': 2}]
    repository(conn).import_rows('EDI', rows, 'test')
    assert [p for s, p in conn.queries if 'INSERT INTO "EDIGlobal"' in s] == [rows]


def test_creation_does_not_connect(monkeypatch):
    called = []
    monkeypatch.setattr('edi_stock.repository.create_engine', lambda *a, **k: called.append(a))
    PostgresRepository('unused')
    assert not called


def test_concurrent_same_key_serializes_with_mock_advisory_locks():
    """Exercises repository logic against simulated transaction/advisory semantics, not live PostgreSQL."""
    import threading
    from concurrent.futures import ThreadPoolExecutor
    locks, ledger, inserts = {}, {}, []
    guard = threading.Lock()
    barrier = threading.Barrier(2)

    class ConcurrentConnection:
        def __init__(self):
            self.held = []
            self.pending = None

        def execute(self, statement, params=None):
            sql = str(statement)
            result = MagicMock()
            if 'pg_advisory_xact_lock' in sql:
                with guard:
                    lock = locks.setdefault(params['key'], threading.Lock())
                lock.acquire()
                self.held.append(lock)
            elif 'SELECT payload_hash' in sql:
                result.mappings.return_value.first.return_value = ledger.get(params['key'])
            elif 'INSERT INTO "EDIGlobal"' in sql:
                inserts.append(params)
            elif 'INSERT INTO edi_imports' in sql:
                self.pending = params
            return result

    class ConcurrentEngine:
        @contextmanager
        def begin(self):
            conn = ConcurrentConnection()
            try:
                yield conn
                if conn.pending:
                    p = conn.pending
                    ledger[p['key']] = {'payload_hash': p['digest'], 'import_id': p['id'],
                                        'row_count': p['count'], 'file_type': p['type']}
            finally:
                for lock in reversed(conn.held):
                    lock.release()

    repo = PostgresRepository('unused')
    repo._engine = ConcurrentEngine()
    def submit():
        barrier.wait(timeout=3)
        return repo.import_rows('EDI', [{'Quantity': 10}], 'same-key')['status']
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: submit(), range(2)))
    assert sorted(results) == ['already_imported', 'imported']
    assert len(inserts) == 1
