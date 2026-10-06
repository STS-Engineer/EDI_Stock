import os
import time
import pytest
from edi_stock.staging import PreviewStore
from edi_stock.validation import ValidationError


def test_unique_previews_and_tampering(tmp_path):
    store = PreviewStore(tmp_path, 'test-secret')
    first = store.save('owner', 'EDI', [{'x': '1'}])
    second = store.save('owner', 'EDI', [{'x': '1'}])
    assert first != second and len(list(tmp_path.glob('*.json'))) == 2
    with pytest.raises(ValidationError):
        store.load(first + 'tamper', 'owner')
    with pytest.raises(ValidationError):
        store.load(first, 'other-owner')


def test_cleanup_only_expired_json(tmp_path):
    store = PreviewStore(tmp_path, 'test-secret', ttl=10)
    old = tmp_path / 'expired.json'
    old.write_text('{}')
    os.utime(old, (time.time() - 20, time.time() - 20))
    other = tmp_path / 'keep.pdf'
    other.write_text('keep')
    store.cleanup()
    assert not old.exists() and other.exists()


def test_signed_expiration(tmp_path, monkeypatch):
    store = PreviewStore(tmp_path, 'test-secret', ttl=10)
    token = store.save('owner', 'EDI', [])
    now = time.time()
    monkeypatch.setattr('itsdangerous.timed.time.time', lambda: now + 11)
    with pytest.raises(ValidationError):
        store.load(token, 'owner')
