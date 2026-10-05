"""Bounded parsers. All output is validated before preview or insertion."""
import csv
import io
import re
import unicodedata
import zipfile
from datetime import datetime

import chardet
import pandas as pd
import pdfplumber

from .validation import SCHEMAS, ValidationError, normalize_material, quantity


def parse_upload(data, filename, file_type, *, max_rows=10000):
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    if ext not in {'csv', 'xls', 'xlsx', 'pdf'}:
        raise ValidationError('Formats acceptés : CSV, XLS, XLSX et PDF de livraison.')
    if not data:
        raise ValidationError('Le fichier est vide.')
    try:
        if ext == 'pdf':
            if file_type != 'LIVRAISON':
                raise ValidationError('Le PDF est pris en charge uniquement pour les livraisons.')
            return parse_delivery_pdf(data)
        if ext == 'xlsx':
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                infos = archive.infolist()
                if len(infos) > 5000 or sum(i.file_size for i in infos) > 50 * 1024 * 1024:
                    raise ValidationError('Classeur décompressé trop volumineux.')
        if ext in {'xls', 'xlsx'}:
            date_columns = {'Date', 'DateFrom', 'ForecastDate', 'LastDeliveryDate'}
            def excel_date(value):
                if isinstance(value, datetime) and value.time() == datetime.min.time():
                    return value.date().isoformat()
                return str(value) if value is not None else ''
            df = pd.read_excel(
                io.BytesIO(data), dtype={c: str for c in SCHEMAS[file_type] if c not in date_columns},
                converters={c: excel_date for c in date_columns}, keep_default_na=False, nrows=max_rows + 1,
            )
            return df.to_dict(orient='records')
        detected = chardet.detect(data[:200000])
        encoding = detected.get('encoding') or 'utf-8-sig'
        content = data.decode(encoding).lstrip('\ufeff')
        try:
            dialect = csv.Sniffer().sniff(content[:8192], delimiters=',;\t|')
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(io.StringIO(content), dialect=dialect, strict=True)
        names = reader.fieldnames or []
        trimmed = [s.strip() for s in names]
        if not names or len(set(trimmed)) != len(trimmed) or any(not s for s in trimmed):
            raise ValidationError('En-têtes absents, vides ou dupliqués.')
        reader.fieldnames = trimmed
        rows = []
        for row in reader:
            if None in row or any(v is None for v in row.values()):
                raise ValidationError('Nombre de colonnes incohérent dans le CSV.')
            if not any(v.strip() for v in row.values()):
                continue
            rows.append(row)
            if len(rows) > max_rows:
                raise ValidationError(f'Limite dépassée : {max_rows} lignes par import.')
        return rows
    except ValidationError:
        raise
    except Exception as exc:
        raise ValidationError('Fichier illisible ou format non reconnu. Vérifiez son contenu.') from exc


def _plain(value):
    return ''.join(c for c in unicodedata.normalize('NFD', value or '') if not unicodedata.combining(c)).upper()


def parse_delivery_pdf(data, *, default_site='Tunisia'):
    rows = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        if not pdf.pages or len(pdf.pages) > 50:
            raise ValidationError('Le PDF doit contenir entre 1 et 50 pages.')
        texts = [p.extract_text() or '' for p in pdf.pages]
        header = '\n'.join(texts[:3])
        number = re.search(r'FACTURE\s*n[°o]\s*([A-Za-z0-9\-_/]+)', header, re.I)
        dated = re.search(r'\bDate\s+(\d{1,2}/\d{1,2}/\d{4})\b', header, re.I)
        if not number or not dated:
            raise ValidationError('Numéro ou date de facture introuvable. Utilisez le modèle Excel après vérification.')
        invoice_date = datetime.strptime(dated[1], '%d/%m/%Y').date().isoformat()
        def add(ref, qty):
            rows.append({'Site': default_site, 'AVOMaterialNo': ref, 'DeliveryNo': number[1],
                         'Quantity': quantity(qty), 'Date': invoice_date, 'Status': 'Dispatched'})
        line_pattern = re.compile(r'^\s*\d{8}\s+(?:OUI|NON)\s+([A-Z0-9][A-Z0-9.\-]+)(?:\s+(PL|SP))?\s+.+?\s+(\d{1,9})\s+(?:\d+[.,]\d+\s+){2,4}\S+', re.I)
        for page, page_text in zip(pdf.pages, texts):
            found = 0
            # Extract once: combining extract_table and extract_tables doubles rows.
            for table in page.extract_tables() or []:
                indices = None
                for header_index, cells in enumerate(table[:3]):
                    normalized = [_plain(c) for c in cells]
                    refs = [i for i, c in enumerate(normalized) if re.search(r'\bREFERENCE\b|\bREF\b', c)]
                    qtys = [i for i, c in enumerate(normalized) if re.search(r'\bQUANTITE\b|\bQTE\b|\bQTY\b', c)]
                    if refs and qtys:
                        indices = header_index, refs[0], qtys[0]
                        break
                if indices is None:
                    continue
                start, ref_index, qty_index = indices
                for cells in table[start + 1:]:
                    if not cells or _plain(' '.join(c or '' for c in cells)).strip().startswith('TOTAL'):
                        continue
                    ref = (cells[ref_index] or '').strip() if ref_index < len(cells) else ''
                    qty = (cells[qty_index] or '').strip() if qty_index < len(cells) else ''
                    if not ref and not qty:
                        continue
                    if not ref or not qty:
                        raise ValidationError('Ligne PDF incomplète : vérifiez le document original.')
                    hint = cells[ref_index + 1] if ref_index + 1 < len(cells) and ref_index + 1 != qty_index else None
                    add(normalize_material(ref, hint), qty)
                    found += 1
            # Fallback is page-scoped, preserving later text-only pages.
            if not found:
                for line in page_text.splitlines():
                    matched = line_pattern.search(line)
                    if matched:
                        add(normalize_material(matched[1], matched[2]), matched[3])
    if not rows:
        raise ValidationError('Aucune ligne détectée. Les PDF scannés nécessitent une saisie vérifiée dans le modèle.')
    return rows
