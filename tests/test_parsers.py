import io
from unittest.mock import MagicMock
import pytest
from openpyxl import Workbook
from edi_stock.parsers import parse_delivery_pdf, parse_upload
from edi_stock.validation import ValidationError


def test_semicolon_bom_and_zeros():
    rows = parse_upload('\ufeffSite;AVOMaterialNo;DeliveryNo;Quantity;Date;Status\nTunisia;0012;0007;15;2026-10-05;Sent'.encode(), 'import.csv', 'LIVRAISON')
    assert rows[0]['AVOMaterialNo'] == '0012'


@pytest.mark.parametrize('data', [b'a,a\n1,2', b'a,b\n1,2,3', b'a,b\n1'])
def test_bad_csv_headers_or_width(data):
    with pytest.raises(ValidationError):
        parse_upload(data, 'file.csv', 'EDI')


def test_reject_edi_pdf():
    with pytest.raises(ValidationError):
        parse_upload(b'%PDF', 'file.pdf', 'EDI')


def test_excel_preserves_text():
    wb = Workbook()
    wb.active.append(['Site', 'AVOMaterialNo'])
    wb.active.append(['Tunisia', '0012'])
    buffer = io.BytesIO()
    wb.save(buffer)
    assert parse_upload(buffer.getvalue(), 'file.xlsx', 'LIVRAISON')[0]['AVOMaterialNo'] == '0012'


def test_pdf_extracted_once_preserves_legitimate_identical_lines(monkeypatch):
    page = MagicMock()
    page.extract_text.return_value = 'FACTURE n° F001\nDate 05/10/2026'
    page.extract_tables.return_value = [[['REFERENCE', 'QUANTITÉ'], ['V001', '10'], ['V001', '10']]]
    pdf = MagicMock()
    pdf.pages = [page]
    context = MagicMock()
    context.__enter__.return_value = pdf
    monkeypatch.setattr('edi_stock.parsers.pdfplumber.open', lambda _: context)
    rows = parse_delivery_pdf(b'fake')
    assert len(rows) == 2
    page.extract_tables.assert_called_once()
    page.extract_table.assert_not_called()


def test_pdf_missing_header_not_invented(monkeypatch):
    page = MagicMock()
    page.extract_text.return_value = 'missing metadata'
    context = MagicMock()
    context.__enter__.return_value.pages = [page]
    monkeypatch.setattr('edi_stock.parsers.pdfplumber.open', lambda _: context)
    with pytest.raises(ValidationError):
        parse_delivery_pdf(b'fake')


def test_native_excel_date_cells():
    from datetime import datetime
    from edi_stock.validation import validate_rows
    wb = Workbook()
    wb.active.append(['Site', 'AVOMaterialNo', 'DeliveryNo', 'Quantity', 'Date', 'Status'])
    wb.active.append(['Tunisia', '0012', '0007', 15, datetime(2026, 10, 5), 'Dispatched'])
    buffer = io.BytesIO()
    wb.save(buffer)
    rows = parse_upload(buffer.getvalue(), 'file.xlsx', 'LIVRAISON')
    assert rows[0]['Date'] == '2026-10-05'
    assert validate_rows(rows, 'LIVRAISON')[0]['AVOMaterialNo'] == '0012'
