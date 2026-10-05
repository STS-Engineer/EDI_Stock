import pytest
from edi_stock.validation import ValidationError, calendar_value, normalize_material, quantity, validate_rows


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


def test_edi_optional_values():
    result = validate_rows([{'Site': 'Tunisia', 'ClientCode': '0001', 'ClientMaterialNo': '002',
                            'AVOMaterialNo': '003', 'DateFrom': '2026-W41', 'Quantity': '0',
                            'ForecastDate': '2026-W40', 'EDIStatus': 'Firm'}], 'EDI')[0]
    assert result['DateUntil'] is None
    assert result['Quantity'] == 0
    assert result['ClientCode'] == '0001'


@pytest.mark.parametrize('bad', ['x\x00y', '\ud800'])
def test_invalid_database_text_rejected(row, bad):
    row['Site'] = bad
    with pytest.raises(ValidationError):
        validate_rows([row], 'LIVRAISON')


def test_whitespace_material_hint_is_ignored():
    assert normalize_material('ABC', '   ') == 'ABC'
