import io
import re
import pytest
from sqlalchemy.exc import OperationalError
from edi_stock import create_app


def csrf(client):
    client.get('/')
    with client.session_transaction() as state:
        return state['csrf']


def preview(client):
    data = b'Site,AVOMaterialNo,DeliveryNo,Quantity,Date,Status\nTunisia,0012,0007,15,2026-10-05,Sent\n'
    result = client.post('/preview', data={'csrf_token': csrf(client), 'file_type': 'LIVRAISON',
                                         'file': (io.BytesIO(data), 'delivery.csv')})
    assert result.status_code == 200
    token = re.search(r'name="temp_file"\s+value="([^"]+)"', result.text).group(1)
    return token


def test_health_without_database():
    app = create_app({'TESTING': True})
    assert app.test_client().get('/healthz').json == {'status': 'ok'}
    assert app.extensions['import_repository'] is None


def test_home_and_headers(client):
    result = client.get('/')
    assert result.status_code == 200
    assert "object-src 'none'" in result.headers['Content-Security-Policy']
    assert result.headers['Cache-Control'] == 'no-store'


def test_official_logo_on_both_flows(client):
    for flow in ['EDI', 'LIVRAISON']:
        result = client.get(f'/?tab={flow}')
        assert result.status_code == 200
        assert 'src="/static/avocarbon-logo.png"' in result.text
        assert 'alt="AVOCarbon Group"' in result.text
        assert 'width="718" height="132"' in result.text
        assert 'brand-symbol' not in result.text


def test_official_logo_static_asset(client):
    import hashlib

    result = client.get('/static/avocarbon-logo.png')
    assert result.status_code == 200
    assert result.mimetype == 'image/png'
    assert result.data.startswith(b'\x89PNG\r\n\x1a\n')
    assert hashlib.sha256(result.data).hexdigest() == 'a11f05215397c2ba59980c1f11a92a402b5c5d772504e3f3a9740c0c1426b347'


def test_csrf_rejected(client, repository):
    assert client.post('/insert', data={}).status_code == 403
    assert repository.calls == []


def test_preview_confirm_and_replay(client, repository):
    token = preview(client)
    assert repository.calls == []
    payload = {'csrf_token': csrf(client), 'file_type': 'LIVRAISON', 'temp_file': token}
    assert client.post('/insert', data=payload).status_code == 200
    assert client.post('/insert', data=payload).status_code == 200
    assert len(repository.saved) == 1
    assert repository.calls[0][1][0]['AVOMaterialNo'] == '0012'


def test_cross_session_preview_denied(client, app, repository):
    token = preview(client)
    other = app.test_client()
    result = other.post('/insert', data={'csrf_token': csrf(other), 'file_type': 'LIVRAISON', 'temp_file': token})
    assert result.status_code == 422
    assert repository.calls == []


def test_preview_type_cannot_change(client, repository):
    token = preview(client)
    result = client.post('/insert', data={'csrf_token': csrf(client), 'file_type': 'EDI', 'temp_file': token})
    assert result.status_code == 422
    assert repository.calls == []


def test_cancel_removes_preview(client, repository):
    token = preview(client)
    assert client.post('/cancel', data={'csrf_token': csrf(client), 'temp_file': token}).status_code == 302
    result = client.post('/insert', data={'csrf_token': csrf(client), 'file_type': 'LIVRAISON', 'temp_file': token})
    assert result.status_code == 422
    assert repository.calls == []


def test_api_import_and_retry(client, row, headers, repository):
    payload = {'file_type': 'LIVRAISON', 'rows': [row]}
    result = client.post('/api/v1/imports', json=payload, headers=headers)
    assert result.status_code == 201
    assert result.json['rows_imported'] == 1
    replay = client.post('/api/v1/imports', json=payload, headers=headers)
    assert replay.status_code == 200
    assert replay.json['status'] == 'already_imported'
    row['Quantity'] += 1
    assert client.post('/api/v1/imports', json=payload, headers=headers).status_code == 409
    assert len(repository.saved) == 1


def test_api_auth_missing(client, row, repository):
    assert client.post('/api/v1/imports', json={'file_type': 'LIVRAISON', 'rows': [row]}).status_code == 401
    assert repository.calls == []


def test_api_key_required(client, row, headers):
    del headers['Idempotency-Key']
    assert client.post('/api/v1/imports', json={'file_type': 'LIVRAISON', 'rows': [row]}, headers=headers).status_code == 400


def test_api_validation_atomic(client, row, headers, repository):
    result = client.post('/api/v1/imports', json={'file_type': 'LIVRAISON', 'rows': [row, {**row, 'Quantity': 'oops'}]}, headers=headers)
    assert result.status_code == 422
    assert result.json['error']['retryable'] is False
    assert repository.calls == []


def test_api_database_error_sanitized(client, row, headers, repository, caplog):
    repository.fail = OperationalError('SQL', {}, Exception('private-diagnostic-data'))
    result = client.post('/api/v1/imports', json={'file_type': 'LIVRAISON', 'rows': [row]}, headers=headers)
    assert result.status_code == 503
    assert result.json['error']['retryable'] is True
    assert result.headers['Retry-After'] == '30'
    assert 'private-diagnostic-data' not in result.text + caplog.text


def test_oversized_upload(client, app):
    app.config['MAX_CONTENT_LENGTH'] = 100
    result = client.post('/api/v1/imports', data='x' * 101, headers={'Authorization': 'Bearer test-only-api-token', 'Idempotency-Key': 'x', 'Content-Type': 'application/json'})
    assert result.status_code == 413


def test_missing_configuration_fails_closed():
    client = create_app({'TESTING': True}).test_client()
    assert client.post('/api/v1/imports', json={}).status_code == 503
    assert client.post('/preview', data={}).status_code == 503


def test_templates(client):
    assert client.get('/download/template/edi_template.xlsx').status_code == 200
    assert client.get('/download/template/delivery_template.csv').status_code == 200
    assert client.get('/download/template/bad.xlsx').status_code == 404
    assert client.get('/view/temp/anything').status_code == 404


def test_ui_oversized_request_no_recursive_error(client, app):
    app.config['MAX_CONTENT_LENGTH'] = 100
    assert client.post('/preview', data={'file': (io.BytesIO(b'x' * 101), 'file.csv')}).status_code == 413


def test_ui_write_disabled_by_default(tmp_path, repository):
    app = create_app({'TESTING': True, 'SECRET_KEY': 'test-secret', 'SESSION_COOKIE_SECURE': False,
                      'PREVIEW_DIR': str(tmp_path)}, repository=repository)
    client = app.test_client()
    assert client.post('/insert', data={'csrf_token': csrf(client)}).status_code == 503
    assert repository.calls == []


def test_invalid_json_file_type(client, headers):
    for kind in [[], {}, 5, None]:
        assert client.post('/api/v1/imports', json={'file_type': kind, 'rows': []}, headers=headers).status_code == 422


def test_non_ascii_auth_is_rejected_not_error(client, headers):
    headers['Authorization'] = 'Bearer échec'
    assert client.post('/api/v1/imports', json={}, headers=headers).status_code == 401


def test_recoverable_insert_failure_preserves_same_retry_token(client, repository):
    token = preview(client)
    payload = {'csrf_token': csrf(client), 'file_type': 'LIVRAISON', 'temp_file': token}
    repository.fail = OperationalError('COMMIT', {}, Exception('mock ambiguous failure'))
    result = client.post('/insert', data=payload)
    assert result.status_code == 503
    retry_token = re.search(r'name="temp_file"\s+value="([^"]+)"', result.text).group(1)
    assert retry_token == token
    original_key = repository.calls[-1][2]
    repository.fail = None
    assert client.post('/insert', data={**payload, 'temp_file': retry_token}).status_code == 200
    assert repository.calls[-1][2] == original_key


def test_database_permanent_errors_not_retried(client, row, headers, repository):
    from sqlalchemy.exc import DataError, IntegrityError, ProgrammingError
    for error_class, expected_status in [(DataError, 422), (IntegrityError, 409), (ProgrammingError, 503)]:
        repository.fail = error_class('private-statement', {}, Exception('private-detail'))
        result = client.post('/api/v1/imports', json={'file_type': 'LIVRAISON', 'rows': [row]}, headers=headers)
        assert result.status_code == expected_status
        assert result.json['error']['retryable'] is False
        assert 'private' not in result.text


def test_api_receipt_distinguishes_received_and_aggregated_rows(client, row, headers):
    result = client.post('/api/v1/imports', json={'file_type': 'LIVRAISON', 'rows': [row, row]}, headers=headers)
    assert result.status_code == 201
    assert result.json['rows_received'] == 2
    assert result.json['rows_imported'] == 1


@pytest.mark.parametrize('change', [{'DateUntil': None}, {'DateUntil': '2026-02-30'},
                                    {'ClientCode': 'C' * 51}, {'ProductName': 'P' * 101}])
def test_edi_schema_errors_return_422_before_any_write(client, edi_row, headers, repository, change):
    result = client.post('/api/v1/imports', json={'file_type': 'EDI', 'rows': [edi_row, {**edi_row, **change}]},
                         headers=headers)
    assert result.status_code == 422
    assert result.json['error']['code'] == 'validation_failed'
    assert result.json['error']['retryable'] is False
    assert result.json['error']['details'][0]['row'] == 3
    assert repository.calls == []


def test_edi_date_until_reaches_repository_normalized(client, edi_row, headers, repository):
    result = client.post('/api/v1/imports', json={'file_type': 'EDI', 'rows': [edi_row]}, headers=headers)
    assert result.status_code == 201
    assert repository.calls[0][1][0]['DateUntil'] == '2026-W42'


def test_edi_preview_rejects_missing_date_until(client, repository):
    data = (b'Site,ClientCode,ClientMaterialNo,AVOMaterialNo,DateFrom,Quantity,ForecastDate,EDIStatus\n'
            b'Germany,0001,002,003,2026-W41,0,2026-W40,Firm\n')
    result = client.post('/preview', data={'csrf_token': csrf(client), 'file_type': 'EDI',
                                         'file': (io.BytesIO(data), 'edi.csv')})
    assert result.status_code == 422
    assert 'DateUntil' in result.text
    assert repository.calls == []
