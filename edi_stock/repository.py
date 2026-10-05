"""PostgreSQL persistence; never connects at import time or creates schemas automatically."""
import hashlib
import json
import uuid
from threading import Lock

from sqlalchemy import create_engine, text

from .validation import SCHEMAS


class ImportConflict(Exception):
    pass


class InventoryConflict(Exception):
    pass


def payload_hash(file_type, rows):
    serialized = json.dumps({'file_type': file_type, 'rows': rows}, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(serialized.encode()).hexdigest()


def lock_key(value):
    return int.from_bytes(hashlib.sha256(value.encode()).digest()[:8], 'big', signed=True)


class PostgresRepository:
    def __init__(self, url):
        self.url = url
        self._engine = None
        self._engine_lock = Lock()

    @property
    def engine(self):
        with self._engine_lock:
            if self._engine is None:
                self._engine = create_engine(self.url, pool_pre_ping=True, pool_size=5, max_overflow=5,
                                             connect_args={'connect_timeout': 5}, hide_parameters=True)
        return self._engine

    def import_rows(self, file_type, rows, idempotency_key):
        digest = payload_hash(file_type, rows)
        with self.engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text("SET LOCAL statement_timeout = '30s'"))
            conn.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': lock_key('import:' + idempotency_key)})
            previous = conn.execute(text('SELECT payload_hash, import_id, row_count, file_type FROM edi_imports WHERE idempotency_key=:key'), {'key': idempotency_key}).mappings().first()
            if previous:
                if previous['payload_hash'] != digest:
                    raise ImportConflict('Cette clé correspond déjà à un autre contenu.')
                return {'status': 'already_imported', 'import_id': str(previous['import_id']),
                        'rows_imported': previous['row_count'], 'file_type': previous['file_type']}
            if file_type == 'EDI':
                self._insert(conn, 'EDIGlobal', SCHEMAS['EDI'], rows)
            else:
                # Stable ordering avoids deadlocks when two batches overlap several materials.
                for site, material in sorted({(r['Site'], r['AVOMaterialNo']) for r in rows}):
                    scope = json.dumps([site, material], ensure_ascii=False)
                    conn.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': lock_key('transit:' + scope)})
                for row in rows:
                    self._delivery(conn, row)
            import_id = str(uuid.uuid4())
            conn.execute(text('INSERT INTO edi_imports (idempotency_key,payload_hash,import_id,row_count,file_type) VALUES (:key,:digest,:id,:count,:type)'),
                         {'key': idempotency_key, 'digest': digest, 'id': import_id, 'count': len(rows), 'type': file_type})
        # Return acknowledgement only after the context manager has committed.
        return {'status': 'imported', 'import_id': import_id, 'rows_imported': len(rows), 'file_type': file_type}

    @staticmethod
    def _insert(conn, table, columns, rows):
        # Identifiers come only from constants, never from incoming content.
        names = ','.join('"' + c + '"' for c in columns)
        binds = ','.join(':' + c for c in columns)
        conn.execute(text(f'INSERT INTO "{table}" ({names}) VALUES ({binds})'), rows)

    def _delivery(self, conn, row):
        scope = {'site': row['Site'], 'material': row['AVOMaterialNo']}
        transit_rows = conn.execute(text('''SELECT "DeliveryNo","Quantity","Date"
            FROM "DeliveryDetails" WHERE "Site"=:site AND COALESCE("AVOMaterialNo",'')=:material
            AND "Status"='InTransit' ORDER BY "Date" DESC LIMIT 2 FOR UPDATE'''), scope).mappings().all()
        if len(transit_rows) > 1:
            raise InventoryConflict('Plusieurs soldes de transit existent pour cet article. Réconciliation nécessaire.')
        transit = transit_rows[0] if transit_rows else None
        status = row['Status']
        if status == 'InTransit' and transit:
            raise InventoryConflict('Un solde de transit existe déjà pour cet article.')
        if status in {'Dispatched', 'Delivered'} and transit:
            previous = int(transit['Quantity'])
            next_quantity = previous + row['Quantity'] if status == 'Dispatched' else previous - row['Quantity']
            if next_quantity < 0 or next_quantity > 2147483647:
                raise InventoryConflict('La quantité ne correspond pas au solde de transit. Réconciliation nécessaire.')
            params = {**scope, 'qty': next_quantity, 'date': row['Date'], 'number': row['DeliveryNo'],
                      'old_number': transit['DeliveryNo'], 'old_date': transit['Date']}
            updated = conn.execute(text('''UPDATE "DeliveryDetails" SET "Quantity"=:qty,"Date"=:date,"DeliveryNo"=:number
                WHERE "Site"=:site AND COALESCE("AVOMaterialNo",'')=:material
                AND "DeliveryNo"=:old_number AND "Date"=:old_date AND "Status"='InTransit' '''), params)
            if updated.rowcount != 1:
                raise InventoryConflict('Le solde de transit a changé. Réessayez après vérification.')
        elif status == 'Dispatched':
            self._insert(conn, 'DeliveryDetails', SCHEMAS['LIVRAISON'],
                         [{**row, 'DeliveryNo': row['DeliveryNo'] + '_T', 'Status': 'InTransit'}])
        # Preserve historic Delivered-without-transit behavior. Review this business rule before deployment.
        self._insert(conn, 'DeliveryDetails', SCHEMAS['LIVRAISON'], [row])
