import pytest
from datetime import date, datetime
from edi_stock.validation import TEXT_LIMITS, ValidationError, calendar_value, normalize_material, quantity, validate_rows


@pytest.mark.parametrize('value,expected', [('1 000', 1000), ('1\u00a0000', 1000), ('1,000', 1000), ('12.0', 12), ('12,0', 12), (12, 12), (0, 0)])
def test_quantities(value, expected):
    assert quantity(value) == expected


@pytest.mark.parametrize('value', ['12kg', '1.2', '-1', 'nan', 'inf', '', None, True, '1,23,4', '1e3', '2147483648'])
def test_invalid_quantities(value):
    with pytest.raises(ValueError):
        quantity(value)


def test_leading_zeros_and_suffix(row):
    row['AVOMaterialNo'] = '00012 PL'
    result = validate_rows([row], 'LIVRAISON')[0]
    assert result['AVOMaterialNo'] == '00012PL'
    assert result['DeliveryNo'] == '00007'
    assert normalize_material('ABC Long Reference') == 'ABC Long Reference'


def test_duplicates_aggregate(row):
    assert validate_rows([row, row], 'LIVRAISON')[0]['Quantity'] == 24


@pytest.mark.parametrize('change', [{'Quantity': 'wrong'}, {'Date': '2026-02-30'}, {'Status': 'unknown'}, {'Site': ''}, {'Quantity': 0}])
def test_invalid_row_fails_entire_import(row, change):
    with pytest.raises(ValidationError) as exc:
        validate_rows([row, {**row, **change}], 'LIVRAISON')
    assert exc.value.errors[0]['row'] == 3


def test_unknown_column(row):
    with pytest.raises(ValidationError):
        validate_rows([{**row, 'surprise': 'value'}], 'LIVRAISON')


def test_real_iso_week():
    assert calendar_value('2026-W53', weeks=True) == '2026-W53'
    with pytest.raises(ValueError):
        calendar_value('2025-W53', weeks=True)


def test_edi_optional_values(edi_row):
    result = validate_rows([edi_row], 'EDI')[0]
    assert result['DateUntil'] == '2026-W42'
    for column in ['LastDeliveryDate', 'LastDeliveredQuantity', 'CumulatedQuantity', 'ProductName', 'LastDeliveryNo']:
        assert result[column] is None
    assert result['Quantity'] == 0
    assert result['ClientCode'] == '0001'


@pytest.mark.parametrize('bad', ['x\x00y', '\ud800'])
def test_invalid_database_text_rejected(row, bad):
    row['Site'] = bad
    with pytest.raises(ValidationError):
        validate_rows([row], 'LIVRAISON')


def test_whitespace_material_hint_is_ignored():
    assert normalize_material('ABC', '   ') == 'ABC'


def test_text_limits_match_verified_database_schema():
    assert TEXT_LIMITS == {
        'EDI': {'Site': 50, 'ClientCode': 50, 'ClientMaterialNo': 50, 'AVOMaterialNo': 50,
                'DateFrom': 50, 'DateUntil': 50, 'ForecastDate': 50, 'LastDeliveryDate': 50,
                'EDIStatus': 50, 'ProductName': 100, 'LastDeliveryNo': 50},
        'LIVRAISON': {'Site': 20, 'AVOMaterialNo': 30, 'DeliveryNo': 30, 'Date': 20, 'Status': 30},
    }


@pytest.mark.parametrize('file_type,column,limit', [
    (file_type, column, limit) for file_type, fields in TEXT_LIMITS.items() for column, limit in fields.items()
])
def test_every_text_field_rejects_database_limit_plus_one(row, edi_row, file_type, column, limit):
    source = edi_row if file_type == 'EDI' else row
    with pytest.raises(ValidationError) as exc:
        validate_rows([source, {**source, column: 'é' * (limit + 1)}], file_type)
    assert exc.value.errors == [{'row': 3, 'column': column,
                                 'message': f'Champ trop long (maximum {limit} caractères).'}]


@pytest.mark.parametrize('file_type,column,limit', [
    ('EDI', 'Site', 50), ('EDI', 'ClientCode', 50), ('EDI', 'ClientMaterialNo', 50),
    ('EDI', 'AVOMaterialNo', 50), ('EDI', 'ProductName', 100), ('EDI', 'LastDeliveryNo', 50),
    ('LIVRAISON', 'Site', 20), ('LIVRAISON', 'AVOMaterialNo', 30), ('LIVRAISON', 'DeliveryNo', 28),
])
def test_free_text_accepts_exact_limit_after_trimming(row, edi_row, file_type, column, limit):
    source = edi_row if file_type == 'EDI' else row
    value = 'é' * limit
    assert validate_rows([{**source, column: ' ' + value + ' '}], file_type)[0][column] == value


@pytest.mark.parametrize('file_type,limit', [('EDI', 50), ('LIVRAISON', 30)])
def test_material_limit_applies_after_existing_suffix_join(row, edi_row, file_type, limit):
    source = edi_row if file_type == 'EDI' else row
    material = 'A' * (limit - 2) + ' PL'
    accepted = validate_rows([{**source, 'AVOMaterialNo': material}], file_type)[0]
    assert accepted['AVOMaterialNo'] == 'A' * (limit - 2) + 'PL'
    with pytest.raises(ValidationError) as exc:
        validate_rows([{**source, 'AVOMaterialNo': 'A' + material}], file_type)
    assert exc.value.errors[0]['column'] == 'AVOMaterialNo'


@pytest.mark.parametrize('length', [29, 30])
def test_delivery_number_reserves_transit_suffix(row, length):
    with pytest.raises(ValidationError) as exc:
        validate_rows([{**row, 'DeliveryNo': 'D' * length}], 'LIVRAISON')
    assert exc.value.errors[0]['column'] == 'DeliveryNo'
    assert '28' in exc.value.errors[0]['message']


@pytest.mark.parametrize('value', [None, '', '   ', '2026-02-30', '2025-W53', '2026-W00',
                                  '2026-W54', '2026-13-01', '06/10/2026', 'BACKORDER',
                                  datetime(2026, 10, 6, 12)])
def test_date_until_is_required_and_validated(edi_row, value):
    with pytest.raises(ValidationError) as exc:
        validate_rows([{**edi_row, 'DateUntil': value}], 'EDI')
    assert exc.value.errors[0]['column'] == 'DateUntil'


def test_date_until_missing_column_is_rejected(edi_row):
    del edi_row['DateUntil']
    with pytest.raises(ValidationError) as exc:
        validate_rows([edi_row], 'EDI')
    assert any(error['column'] == 'DateUntil' for error in exc.value.errors)


@pytest.mark.parametrize('value,expected', [('2026-10-06', '2026-10-06'), ('2026-W53', '2026-W53'),
                                         (date(2026, 10, 6), '2026-10-06'),
                                         (datetime(2026, 10, 6), '2026-10-06')])
def test_date_until_accepts_calendar_dates_and_real_iso_weeks(edi_row, value, expected):
    assert validate_rows([{**edi_row, 'DateUntil': value}], 'EDI')[0]['DateUntil'] == expected


@pytest.mark.parametrize('column', ['ClientMaterialNo', 'EDIStatus'])
def test_business_required_fields_remain_required_despite_database_nullability(edi_row, column):
    with pytest.raises(ValidationError) as exc:
        validate_rows([{**edi_row, column: None}], 'EDI')
    assert exc.value.errors[0]['column'] == column
