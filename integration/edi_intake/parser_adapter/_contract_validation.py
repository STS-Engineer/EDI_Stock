"""Pure validation shared by interactive and machine imports; no network or database I/O."""
import math
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

SCHEMAS = {
    'EDI': ['Site', 'ClientCode', 'ClientMaterialNo', 'AVOMaterialNo', 'DateFrom', 'DateUntil',
            'Quantity', 'ForecastDate', 'LastDeliveryDate', 'LastDeliveredQuantity',
            'CumulatedQuantity', 'EDIStatus', 'ProductName', 'LastDeliveryNo'],
    'LIVRAISON': ['Site', 'AVOMaterialNo', 'DeliveryNo', 'Quantity', 'Date', 'Status'],
}
REQUIRED = {
    'EDI': {'Site', 'ClientCode', 'ClientMaterialNo', 'AVOMaterialNo', 'DateFrom', 'DateUntil',
            'Quantity', 'ForecastDate', 'EDIStatus'},
    'LIVRAISON': set(SCHEMAS['LIVRAISON']),
}
# PostgreSQL varchar limits verified read-only on 2026-10-06. Required business
# fields remain stricter than database nullability; never truncate input to fit.
TEXT_LIMITS = {
    'EDI': {'Site': 50, 'ClientCode': 50, 'ClientMaterialNo': 50, 'AVOMaterialNo': 50,
            'DateFrom': 50, 'DateUntil': 50, 'ForecastDate': 50, 'LastDeliveryDate': 50,
            'EDIStatus': 50, 'ProductName': 100, 'LastDeliveryNo': 50},
    'LIVRAISON': {'Site': 20, 'AVOMaterialNo': 30, 'DeliveryNo': 30, 'Date': 20, 'Status': 30},
}

class ValidationError(ValueError):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors or []


def clean_string(value):
    if value is None or isinstance(value, float) and math.isnan(value):
        return ''
    if isinstance(value, (dict, list, bool)):
        raise ValueError('Valeur simple attendue.')
    result = str(value).strip()
    if '\x00' in result:
        raise ValueError('Caractère nul interdit dans les données.')
    try:
        result.encode('utf-8')
    except UnicodeEncodeError as exc:
        raise ValueError('Caractère Unicode invalide.') from exc
    return result


def quantity(value):
    if isinstance(value, bool):
        raise ValueError('Quantité entière attendue.')
    s = clean_string(value).replace('\u00a0', '').replace('\u202f', '').replace(' ', '')
    if re.fullmatch(r'\d{1,3}(,\d{3})+', s):
        s = s.replace(',', '')
    if not re.fullmatch(r'\+?\d+(?:[.,]\d+)?', s):
        raise ValueError('Quantité entière positive ou nulle attendue, sans unité.')
    try:
        result = Decimal(s.replace(',', '.'))
        if result != result.to_integral_value() or result > 2147483647:
            raise ValueError('Quantité entière hors limites ou fractionnaire.')
        return int(result)
    except InvalidOperation as exc:
        raise ValueError('Quantité invalide.') from exc


def normalize_material(value, following_hint=None):
    value = clean_string(value)
    match = re.fullmatch(r'(\S+)\s+(PL|SP)', value, re.IGNORECASE)
    if match:
        return match[1] + match[2].upper()
    if following_hint is not None and clean_string(following_hint):
        suffix = clean_string(following_hint).split()[0].upper()
        if suffix in {'PL', 'SP'} and not value.upper().endswith(suffix):
            return value + suffix
    return value


def normalize_status(value):
    statuses = {'sent': 'Dispatched', 'dispatched': 'Dispatched',
                'intransit': 'InTransit', 'delivered': 'Delivered'}
    key = clean_string(value).lower().replace(' ', '').replace('-', '')
    if key not in statuses:
        raise ValueError('Statut attendu : Dispatched, Delivered ou InTransit.')
    return statuses[key]


def calendar_value(value, *, weeks=False):
    if isinstance(value, datetime):
        if value.time() != datetime.min.time():
            raise ValueError('Une date sans heure est attendue.')
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    value = clean_string(value)
    if weeks and re.fullmatch(r'\d{4}-W\d{2}', value):
        year, week = value.split('-W')
        date.fromisocalendar(int(year), int(week), 1)
        return value
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('Date attendue : AAAA-MM-JJ' + (' ou AAAA-WSS.' if weeks else '.'))
    return date.fromisoformat(value).isoformat()


def validate_rows(rows, file_type, *, max_rows=10000):
    if not isinstance(file_type, str) or file_type not in SCHEMAS:
        raise ValidationError('Type de fichier attendu : EDI ou LIVRAISON.')
    if not isinstance(rows, list) or not rows:
        raise ValidationError('Aucune ligne exploitable dans le fichier.')
    if len(rows) > max_rows:
        raise ValidationError(f'Limite dépassée : {max_rows} lignes par import.')
    clean, errors = [], []
    for number, source in enumerate(rows, start=2):
        if not isinstance(source, dict):
            errors.append({'row': number, 'column': '', 'message': 'Objet de données attendu.'})
            continue
        if any(not isinstance(key, str) for key in source):
            errors.append({'row': number, 'column': '', 'message': 'Les noms de colonnes doivent être du texte.'})
            continue
        extra = set(source) - set(SCHEMAS[file_type])
        missing = REQUIRED[file_type] - set(source)
        if extra or missing:
            errors.append({'row': number, 'column': '', 'message': 'Colonnes inconnues ou obligatoires absentes : ' + ', '.join(sorted(extra | missing))})
        record = {}
        for column in SCHEMAS[file_type]:
            try:
                value = clean_string(source.get(column))
                if len(value) > 1000:
                    raise ValueError('Champ trop long (maximum 1000 caractères).')
                if column == 'AVOMaterialNo':
                    value = normalize_material(value)
                limit = TEXT_LIMITS[file_type].get(column)
                if limit is not None and len(value) > limit:
                    raise ValueError(f'Champ trop long (maximum {limit} caractères).')
                if not value:
                    if column in REQUIRED[file_type]:
                        raise ValueError('Champ obligatoire.')
                    record[column] = None
                    continue
                if column in {'Quantity', 'LastDeliveredQuantity', 'CumulatedQuantity'}:
                    value = quantity(value)
                    if file_type == 'LIVRAISON' and value == 0:
                        raise ValueError('La quantité de livraison doit être supérieure à zéro.')
                elif column == 'Status':
                    value = normalize_status(value)
                elif column == 'Date':
                    value = calendar_value(source.get(column))
                elif column in {'DateFrom', 'DateUntil', 'ForecastDate', 'LastDeliveryDate'}:
                    value = calendar_value(source.get(column), weeks=True)
                elif column == 'EDIStatus' and value not in {'Forcast', 'Forecast', 'Firm', 'PO'}:
                    raise ValueError('Statut attendu : Forecast, Forcast, Firm ou PO.')
                if column == 'DeliveryNo' and len(str(value)) > 28:
                    raise ValueError('Maximum 28 caractères (suffixe de transit réservé).')
                record[column] = value
            except ValueError as exc:
                errors.append({'row': number, 'column': column, 'message': str(exc)})
        clean.append(record)
    if errors:
        raise ValidationError('Import bloqué : corrigez les lignes signalées.', errors[:100])
    if file_type == 'LIVRAISON':
        grouped = {}
        keys = ['Site', 'AVOMaterialNo', 'DeliveryNo', 'Date', 'Status']
        for row in clean:
            key = tuple(row[k] for k in keys)
            if key not in grouped:
                grouped[key] = dict(row)
            else:
                grouped[key]['Quantity'] += row['Quantity']
                if grouped[key]['Quantity'] > 2147483647:
                    raise ValidationError('Quantité cumulée hors limites.')
        clean = list(grouped.values())
    return clean
