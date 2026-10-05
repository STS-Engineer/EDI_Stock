"""Temporary previews scoped to signed browser sessions. No predictable filenames."""
import json
import os
import time
import uuid
from pathlib import Path

from itsdangerous import BadSignature, URLSafeTimedSerializer

from .validation import ValidationError


class PreviewStore:
    def __init__(self, directory, secret, ttl=3600):
        self.directory = Path(directory)
        self.signer = URLSafeTimedSerializer(secret, salt='edi-preview-v1')
        self.ttl = ttl

    def save(self, owner, file_type, rows):
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.cleanup()
        preview_id = uuid.uuid4().hex
        path = self.directory / (preview_id + '.json')
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump({'file_type': file_type, 'rows': rows}, stream, ensure_ascii=False)
        return self.signer.dumps({'id': preview_id, 'owner': owner, 'file_type': file_type})

    def load(self, token, owner, file_type=None):
        try:
            identity = self.signer.loads(token, max_age=self.ttl)
            if identity['owner'] != owner or file_type is not None and identity['file_type'] != file_type:
                raise ValueError
            if not isinstance(identity['id'], str) or len(identity['id']) != 32 or any(c not in '0123456789abcdef' for c in identity['id']):
                raise ValueError
            path = self.directory / (identity['id'] + '.json')
            data = json.loads(path.read_text(encoding='utf-8'))
            return identity['id'], data
        except (BadSignature, ValueError, KeyError, OSError, TypeError) as exc:
            raise ValidationError('Aperçu expiré ou inaccessible. Importez le fichier à nouveau.') from exc

    def delete(self, token, owner):
        preview_id, _ = self.load(token, owner)
        (self.directory / (preview_id + '.json')).unlink(missing_ok=True)

    def cleanup(self):
        # Only our own JSON staging files, never arbitrary uploads or repository outputs.
        cutoff = time.time() - self.ttl
        for path in self.directory.glob('*.json'):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink(missing_ok=True)
            except FileNotFoundError:
                continue
