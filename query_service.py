"""The cloud API: Databento-style historical queries over an owner's private archive (docs/cloud-api.md).

A Lambda function URL. Every request names its owner with the archive identity, the API key: as the
HTTP basic-auth username with an empty password (Databento's convention) or `Authorization: Bearer`.
Endpoints are Databento's (`/v0/timeseries.get_range`, `/v0/metadata.*`); parameters come as the query
string or a form body. Results are Parquet, CSV or JSON lines; up to INLINE bytes in the response, larger
ones in results/OWNER/ behind a 303 to a short-lived URL. Free, with quotas per identity and, since identities
cost nothing to make, per source address (counted before the key is read) and for the whole service.
"""
import base64
import datetime as dt
import json
import os
from urllib.parse import parse_qsl
import uuid

import archive_service as archive
import query_engine as engine
from briskapi.schema import owner, require

BUCKET = os.environ.get('ARCHIVE_BUCKET', '')
INLINE = 4 * 1024 * 1024           # base64 in a 6 MB function URL response
URL_SECONDS = 900
REQUESTS = (60, 60)                 # per identity: requests per minute
RETURNED = (2 * 1024**3, 86400)     # per identity: bytes returned per day
# Identities cost nothing to make, so an address and the whole service have limits too.
ADDRESS = (120, 60)                 # per IP address (keyed hash; IPv6 by /64): requests per minute
ADDRESS_BYTES = (4 * 1024**3, 86400)  # per IP address: bytes returned per day
SERVED = (20 * 1024**3, 86400)      # everyone: bytes returned per day
MAX_BODY = 65536


class Quota(Exception):
    pass


def handler(event, context):
    s3, db = archive.clients()
    return api(event, s3, db)


def api(event, s3, db):
    try:
        return _api(event, s3, db)
    except engine.NotFound as error:
        return _json(404, {'detail': str(error) or 'Not found'})
    except Quota as error:
        return _json(429, {'detail': str(error)})
    except archive.INVALID as error:
        return _json(400, {'detail': str(error)[:300]})


def _api(event, s3, db):
    http = event['requestContext']['http']
    # The address first: requests without a valid key count too.
    address = archive.address(event)
    limit(db, f'query-ip-{address}', 1, *ADDRESS, f'{ADDRESS[0]} requests a minute from this address')
    path = event.get('rawPath') or http.get('path', '')
    require(path.startswith('/v0/'), 'Unknown endpoint: use /v0/timeseries.get_range or /v0/metadata.*')
    endpoint = path[len('/v0/'):]
    require(endpoint in ENDPOINTS, f'Unknown endpoint {endpoint!r}')
    params = dict(parse_qsl(event.get('rawQueryString') or '', keep_blank_values=True))
    if http['method'] == 'POST':
        body = event.get('body') or ''
        if event.get('isBase64Encoded'):
            body = base64.b64decode(body, validate=True).decode()
        require(len(body) <= MAX_BODY, 'Request too large')
        params |= dict(parse_qsl(body, keep_blank_values=True))
    else:
        require(http['method'] == 'GET', 'Use GET or POST')
    who = authenticate(event.get('headers') or {})
    limit(db, f'query-{who}', 1, *REQUESTS, f'{REQUESTS[0]} requests a minute for this identity')
    return ENDPOINTS[endpoint](params, who, engine.Bucket(s3, BUCKET), s3, db, address)


def authenticate(headers):
    """The owner ID of the identity a request names (basic-auth username, or Bearer)."""
    value = headers.get('authorization', '')
    if value.startswith('Basic '):
        try:
            key = base64.b64decode(value[len('Basic '):], validate=True).decode().split(':', 1)[0]
        except (ValueError, UnicodeDecodeError):
            key = ''
    elif value.startswith('Bearer '):
        key = value[len('Bearer '):]
    else:
        key = ''
    require(key, 'An API key is required: your archive identity, as the basic-auth username')
    return owner(key)


def limit(db, scope, amount, cap, window, what):
    try:
        archive.quota(db, scope, amount, cap, window)
    except archive.Quota:
        raise Quota(f"Query quota reached: {what}; retry {'in a minute' if window == 60 else 'tomorrow (UTC)'}") from None


def _gigabytes(cap):
    return f'{cap[0] / 1024**3:g} GB returned a day'


def _json(status, value):
    return {'statusCode': status, 'headers': {'content-type': 'application/json', 'cache-control': 'no-store'},
            'body': json.dumps(value)}


def _flag(params, name, default):
    value = params.get(name)
    if value in (None, ''):
        return default
    require(value.lower() in {'true', 'false', '1', '0'}, f'{name} is true or false')
    return value.lower() in {'true', '1'}


def _request(params):
    names = ('dataset', 'schema', 'symbols', 'start', 'end', 'limit', 'stype_in')
    return engine.request(**{name: params[name] for name in names if name in params and params[name] != ''},
                          snapshot=_flag(params, 'snapshot', False))


def get_range(params, who, bucket, s3, db, address):
    table = engine.get_range(bucket, who, _request(params))
    body, content, extension = engine.encode(table, params.get('encoding') or 'parquet', params.get('compression') or None,
                                             _flag(params, 'pretty_ts', True), _flag(params, 'pretty_px', True),
                                             _flag(params, 'map_symbols', True))
    size = max(len(body), 1)
    limit(db, f'query-bytes-{who}', size, *RETURNED, f'{_gigabytes(RETURNED)} for this identity')
    # Before the service-wide cap, so that one address alone cannot use up everyone's day.
    limit(db, f'query-bytes-ip-{address}', size, *ADDRESS_BYTES, f'{_gigabytes(ADDRESS_BYTES)} for this address')
    limit(db, 'query-bytes-all', size, *SERVED, f'{_gigabytes(SERVED)} for the whole service')
    if len(body) > INLINE:
        key = f'results/{who}/{uuid.uuid4()}.{extension}'
        s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType=content)
        url = s3.generate_presigned_url('get_object', Params={'Bucket': BUCKET, 'Key': key}, ExpiresIn=URL_SECONDS)
        return {'statusCode': 303, 'headers': {'location': url, 'cache-control': 'no-store'}, 'body': ''}
    headers = {'content-type': content, 'cache-control': 'no-store', 'x-record-count': str(table.num_rows)}
    if content in {'text/csv', 'application/x-ndjson'}:
        return {'statusCode': 200, 'headers': headers, 'body': body.decode()}
    return {'statusCode': 200, 'headers': headers, 'body': base64.b64encode(body).decode(), 'isBase64Encoded': True}


def list_datasets(params, who, bucket, s3, db, address):
    return _json(200, list(engine.DATASETS))


def list_schemas(params, who, bucket, s3, db, address):
    require(params.get('dataset') in (None, '', *engine.DATASETS), 'Unknown dataset')
    return _json(200, list(engine.SCHEMAS))


def list_fields(params, who, bucket, s3, db, address):
    return _json(200, engine.fields(params.get('schema', '')))


def list_publishers(params, who, bucket, s3, db, address):
    return _json(200, [{'publisher_id': pid, 'dataset': name, 'venue': 'XTKS', 'description': text}
                       for name, (pid, text) in engine.DATASETS.items()])


def _dates(params, who, bucket):
    dataset = params.get('dataset', '')
    require(dataset in engine.DATASETS, 'Unknown dataset')
    dates = engine.dataset_dates(bucket, who, dataset)
    if not dates:
        raise engine.NotFound(f'No {dataset} data in your archive yet')
    return dates


def get_dataset_range(params, who, bucket, s3, db, address):
    dates = sorted(_dates(params, who, bucket))
    day = lambda d: f'{d[:4]}-{d[4:6]}-{d[6:]}'  # noqa: E731
    return _json(200, {'start': day(dates[0]), 'end': day(dates[-1])})


def get_dataset_condition(params, who, bucket, s3, db, address):
    dates = _dates(params, who, bucket)
    low = (params.get('start_date') or '').replace('-', '') or min(dates)
    high = (params.get('end_date') or '').replace('-', '') or max(dates)
    require(all(len(d) == 8 and d.isdigit() for d in (low, high)), 'Dates are YYYY-MM-DD')
    day, last, out = dt.datetime.strptime(low, '%Y%m%d').date(), dt.datetime.strptime(high, '%Y%m%d').date(), []
    require((last - day).days < 366, 'At most a year per request')
    while day <= last:
        key = day.strftime('%Y%m%d')
        out.append({'date': day.isoformat(), 'condition': 'available' if key in dates else 'missing',
                    'last_modified_date': None, 'windows': dates.get(key, [])})
        day += dt.timedelta(days=1)
    return _json(200, out)


def get_record_count(params, who, bucket, s3, db, address):
    return _json(200, engine.get_range(bucket, who, _request(params)).num_rows)


def get_cost(params, who, bucket, s3, db, address):
    _request(params)
    return _json(200, 0.0)


ENDPOINTS = {
    'timeseries.get_range': get_range,
    'metadata.list_datasets': list_datasets,
    'metadata.list_schemas': list_schemas,
    'metadata.list_fields': list_fields,
    'metadata.list_publishers': list_publishers,
    'metadata.get_dataset_range': get_dataset_range,
    'metadata.get_dataset_condition': get_dataset_condition,
    'metadata.get_record_count': get_record_count,
    'metadata.get_cost': get_cost,
}
