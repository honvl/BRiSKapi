import copy
import gzip
import hashlib
import hmac
import io
import json
from pathlib import Path
import uuid

import pytest
from botocore.exceptions import ClientError

import briskapi.schema as schema
import archive_service as service
import briskapi.cli as cli


def batches(source='synthetic_test'):
    b = schema.synthetic_recording()
    b[0]['source'] = source
    return b

def stream(bs):
    return io.BytesIO(b''.join(map(schema.encode, bs)))

def raw(bs):
    # Decoder-style output: unsorted keys and issues, full-precision timings.
    out = []
    for b in copy.deepcopy(bs):
        if b['type'] == 'quotes':
            b['replay_lateness_ms'] = 0.1234567
        out.append(json.dumps(dict(reversed(list(b.items())))).encode() + b'\n')
    return b''.join(out)

def canon(bs):
    return b''.join(schema.canonical_lines(io.BytesIO(raw(bs))))

def reference(bs):
    return {bs[0]['source']: schema.build_reference(stream(bs))}

@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.delenv('BRISK_CONTRIBUTE', raising=False)
    monkeypatch.setattr(service, 'QUOTA_KEY', b'test-key')
    monkeypatch.setattr(cli, 'interactive', lambda: False)

@pytest.fixture
def packaged(tmp_path):
    events = tmp_path / 'source.jsonl'
    events.write_bytes(raw(batches()))
    directory = tmp_path / 'package'
    manifest = cli.package(events, directory, 'test', 'CC0-1.0')
    return directory, manifest

class S3:
    def __init__(self):
        self.objects = {}
        self.versions = {}
        self.deleted = []
    def put_object(self, Bucket, Key, Body, **kw):
        if kw.get('IfNoneMatch') and Key in self.objects:
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
        self.objects[Key] = Body.read() if hasattr(Body, 'read') else Body
        return {}
    def get_object(self, Bucket, Key, VersionId=None):
        try:
            data = self.versions[(Key, VersionId)] if VersionId else self.objects[Key]
        except KeyError:
            raise ClientError({'Error': {'Code': 'NoSuchVersion' if VersionId else 'NoSuchKey'}}, 'GetObject')
        return {'Body': io.BytesIO(data), 'ContentLength': len(data)}
    def delete_object(self, Bucket, Key, VersionId):
        self.deleted.append((Key, VersionId))
        if self.versions.pop((Key, VersionId), None) is None:
            raise ClientError({'Error': {'Code': 'NoSuchVersion'}}, 'DeleteObject')
    def generate_presigned_post(self, **kw):
        self.post_args = kw
        return {'url': 'https://example.test', 'fields': {'key': kw['Key']}}
    def get_paginator(self, name):
        return self
    def paginate(self, Bucket, Prefix):
        keys = [{'Key': k} for k in self.objects if k.startswith(Prefix)]
        return [{'Contents': keys[:1]}, {'Contents': keys[1:]}]

class DB:
    def __init__(self):
        self.calls = []; self.versions = {}; self.reject = False
    def update_item(self, **kw):
        self.calls.append(kw)
        key = kw['Key']['id']['S']; values = kw['ExpressionAttributeValues']
        if ':version' in values:
            version = values[':version']['S']
            if key in self.versions and self.versions[key] != version:
                raise ClientError({'Error': {'Code': 'ConditionalCheckFailedException'}}, 'UpdateItem')
            self.versions[key] = version
        if self.reject:
            raise ClientError({'Error': {'Code': 'ConditionalCheckFailedException'}}, 'UpdateItem')
        return {}

def event(method='POST', body=None, query=''):
    return {'requestContext': {'http': {'method': method, 'sourceIp': '127.0.0.1'}},
            'body': json.dumps(body), 'rawQueryString': query}

def upload(s3, manifest, directory, ticket, version='v1', data=None):
    key = f'incoming/{ticket}/events.jsonl.gz'
    s3.versions[(key, version)] = data if data is not None else (directory / 'events.jsonl.gz').read_bytes()
    return {'s3': {'bucket': {'name': service.BUCKET}, 'object': {'key': key, 'versionId': version}}}

def ticket_for(s3, db, m):
    return json.loads(service.api(event(body=m), s3, db)['body'])['ticket']

def test_automatic_publish_public_pull(packaged, tmp_path):
    directory, m = packaged; s3 = S3(); db = DB()
    ticket = ticket_for(s3, db, m)
    assert s3.post_args['Conditions'][-1] == ['content-length-range', m['bytes'], m['bytes']]
    rec = upload(s3, m, directory, ticket)
    service.ingest(rec, s3, db)
    status = json.loads(service.api(event('GET', query='ticket=' + ticket), s3, db)['body'])
    assert status['status'] == 'published'
    # The validated staging upload is gone; only the service's own bytes are public.
    assert not s3.versions and s3.deleted == [(f'incoming/{ticket}/events.jsonl.gz', 'v1')]
    published = s3.objects[status['prefix'] + '/events.jsonl.gz']
    assert hashlib.sha256(published).hexdigest() == status['sha256']
    entries = list(cli.manifests(s3, 'bucket'))
    assert len(entries) == 1 and entries[0][1]['summary'] == m['summary']
    result = cli.pull(s3, 'bucket', status['prefix'], tmp_path / 'download')
    assert result['sha256'] == status['sha256']
    assert (tmp_path / 'download/events.jsonl').read_bytes() == canon(batches())
    # At-least-once delivery, and extra POSTs with the same upload form, cannot
    # change a publication or leave data in staging.
    service.ingest(rec, s3, db)
    service.ingest(upload(s3, m, directory, ticket, 'v2', b'bad'), s3, db)
    assert not s3.versions
    assert json.loads(service.api(event('GET', query='ticket=' + ticket), s3, db)['body'])['status'] == 'published'
    with pytest.raises(ValueError, match='already exists'):
        cli.pull(s3, 'bucket', status['prefix'], tmp_path / 'download')

def test_gzip_container_never_published(packaged):
    directory, m = packaged; s3 = S3(); db = DB()
    content = gzip.decompress((directory / 'events.jsonl.gz').read_bytes())
    payload = b'ARBITRARY-PAYLOAD'
    stuffed = io.BytesIO()
    with gzip.GzipFile(filename=payload.decode(), mode='wb', fileobj=stuffed, mtime=123) as gz:
        gz.write(content[:10])
    stuffed.write(gzip.compress(content[10:]) + b'\0' * 64)
    data = stuffed.getvalue()
    m = {**m, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}
    ticket = ticket_for(s3, db, m)
    service.ingest(upload(s3, m, directory, ticket, data=data), s3, db)
    status = service.json_get(s3, f'incoming/{ticket}/status.json')
    assert status['status'] == 'published' and status['sha256'] != m['sha256']
    published = s3.objects[status['prefix'] + '/events.jsonl.gz']
    assert payload not in published and gzip.decompress(published) == content

@pytest.mark.parametrize('mutation', [
    lambda b: b[1].update(seq=3),
    lambda b: b[0].update(source='live'),
    lambda b: b[0].update(trading_date='20250230'),
    lambda b: b[0].update(trading_date='20210928'),
    lambda b: b[0].update(quotes=[]),
    lambda b: b[0].update(exchange_delay_ms=0),
    lambda b: b[0].update(source_timestamp_origin='exchange'),
    lambda b: b[0].update(input_transport={'token': 'secret'}),
    lambda b: b[0].update(input_transport={'kind': 'https_recorded_assets', 'origin': 'https://bad.test', 'asset_fetch_ms': 1}),
    lambda b: b[0].update(market_issue_count=2),
    lambda b: b[0].pop('input_transport'),
    lambda b: b[0]['master'].append(b[0]['master'][0]),
    lambda b: b[0]['master'][0].update(name='payload'),
    lambda b: b[0]['master'][0].update(name='x' * 300),
    lambda b: b[0]['master'][0].update(market=1),
    lambda b: b[0]['master'][0].pop('lot_size'),
    lambda b: b[1]['quotes'][0].update(code='1111'),
    lambda b: b[1]['quotes'][0].update(frame=0),
    lambda b: b[1]['quotes'][0].update(source_time_us=111),
    lambda b: b[1]['quotes'][0].update(indicative_price10=9999),
    lambda b: b[1]['quotes'][0].update(token='secret'),
    lambda b: b[1]['quotes'][0].pop('volume'),
    lambda b: b[1]['quotes'].append(b[1]['quotes'][0]),
    lambda b: b[1].update(source_time_us=99),
    lambda b: b[1].update(source_time_us=111),
    lambda b: b[1].update(type='bootstrap'),
    lambda b: b[1].update(master=[]),
    lambda b: b[1].update(decode_ns=-1),
    lambda b: b[1].update(decode_ns=10**12),
    lambda b: b[1].update(received_unix_ms=1),
    lambda b: b[1].update(replay_lateness_ms=-1),
    lambda b: b[1].update(replay_lateness_ms=0.1234567),
    lambda b: b[1].update(replay_lateness_ms=-0.0),
    lambda b: b[1].update(replay_lateness_ms=float('nan')),
    lambda b: b[2].update(quotes=b[0]['quotes']),
    lambda b: b[2].update(frames=9),
    lambda b: b[2].update(quote_updates=9),
    lambda b: b[2].update(replay_wall_ms=10**9),
    lambda b: b[2].update(type='bogus'),
    lambda b: b.pop(),
    lambda b: b.append(b[-1]),
])
def test_reject_invalid_stream(mutation):
    b = batches(); mutation(b)
    with pytest.raises((ValueError, KeyError)):
        schema.validate_stream(stream(b))

def paced(n=4):
    # A longer synthetic shape for clock checks, with its own reference.
    b = batches('historical_mock')
    quote = b[1]['quotes'][0]
    middle = [dict(b[1], seq=i, source_time_us=110 + i, received_unix_ms=1632700800000 + i,
                   quotes=[dict(quote, frame=1 + i, max_frame=1 + i, source_time_us=110 + i)]) for i in range(1, n)]
    end = dict(b[2], seq=n, source_time_us=110 + n - 1, frames=n, quote_updates=n - 1)
    return [b[0], *middle, end]

@pytest.mark.parametrize('change,valid', [
    (lambda b: None, True),
    (lambda b: [x.update(replay_lateness_ms=None) for x in b[1:-1]], True),
    (lambda b: b[2].update(replay_lateness_ms=None), False),
    (lambda b: b[2].update(received_unix_ms=b[1]['received_unix_ms'] - 1), False),
    (lambda b: b[3].update(received_unix_ms=b[1]['received_unix_ms'] + schema.MAX_SESSION_MS + 1), False),
])
def test_local_clock_rules(change, valid):
    b = paced(); ref = reference(paced()); change(b)
    if valid:
        assert schema.validate_stream(stream(b), ref)['batches'] == 5
    else:
        with pytest.raises(ValueError):
            schema.validate_stream(stream(b), ref)

@pytest.mark.parametrize('encoding', [
    lambda line: line.replace(b',', b', ', 1),
    lambda line: line.replace(b'{', b'{"seq":9,', 1),
    lambda line: line.replace(b'"Synthetic fixture"', b'"Synthetic \\u0066ixture"'),
    lambda line: b'{"type":"bootstrap",' + line[1:].replace(b',"type":"bootstrap"', b''),
    lambda line: line.rstrip(b'\n'),
    lambda line: line.replace(b'"lot_size":100', b'"lot_size":100.0'),
])
def test_reject_noncanonical_bytes(encoding):
    lines = stream(batches()).read().splitlines(True)
    lines[0] = encoding(lines[0])
    with pytest.raises(ValueError):
        schema.validate_stream(io.BytesIO(b''.join(lines)))

def test_decode_limits(monkeypatch):
    monkeypatch.setattr(schema, 'MAX_LINE', 3)
    with pytest.raises(ValueError, match='limit'):
        schema.validate_stream(stream(batches()))
    with pytest.raises(ValueError, match='limit'):
        list(schema.canonical_lines(stream(batches())))
    monkeypatch.setattr(schema, 'MAX_LINE', 1024 * 1024)
    monkeypatch.setattr(schema, 'MAX_EXPANDED', 3)
    with pytest.raises(ValueError, match='limit'):
        schema.validate_stream(stream(batches()))
    monkeypatch.setattr(schema, 'MAX_EXPANDED', 1024 * 1024)
    with pytest.raises(ValueError, match='Nonfinite'):
        schema.validate_stream(io.BytesIO(b'{"seq":NaN}\n'))

def test_reference_rules(monkeypatch):
    b = batches('historical_mock')
    with pytest.raises(ValueError, match='No reference'):
        schema.validate_stream(stream(b), {})
    assert schema.validate_stream(stream(b), reference(b))['source'] == 'historical_mock'
    other = reference(b); other['historical_mock']['issues'] = []
    with pytest.raises(ValueError, match='differs'):
        schema.validate_stream(stream(b), other)
    two = copy.deepcopy(b)
    two[0]['market_issue_count'] = 2
    with pytest.raises(ValueError, match='whole market'):
        schema.build_reference(stream(two))
    committed = schema.REFERENCES['historical_mock']
    assert committed['market_issue_count'] == len(committed['issues']) == 4131 and committed['batches'] == 18002

def test_canonical_lines_and_transport():
    b = batches('historical_mock')
    b[0]['input_transport'] = {'kind': 'https_recorded_assets', 'origin': schema.DEMO_ORIGIN, 'asset_fetch_ms': 123.4567891}
    b[0]['master'].insert(0, b[0]['master'][0] | {'issue_id': 5, 'code': '0005'})
    lines = list(schema.canonical_lines(io.BytesIO(raw(b))))
    bootstrap = json.loads(lines[0])
    assert bootstrap['input_transport']['asset_fetch_ms'] == 123.457
    assert [m['issue_id'] for m in bootstrap['master']] == [0, 5]
    assert json.loads(lines[1])['replay_lateness_ms'] == 0.123
    assert all(line == schema.encode(json.loads(line)) for line in lines)
    b[0]['master'].pop(0); b[0]['market_issue_count'] = 1
    canonical = b''.join(schema.canonical_lines(io.BytesIO(raw(b))))
    ref = {'historical_mock': schema.build_reference(io.BytesIO(canonical))}
    assert schema.validate_stream(io.BytesIO(canonical), ref)
    with pytest.raises(ValueError, match='timing'):
        schema.validate_stream(io.BytesIO(canonical.replace(b'"asset_fetch_ms":123.457', b'"asset_fetch_ms":1000000.0')), ref)
    with pytest.raises(ValueError):
        list(schema.canonical_lines(io.BytesIO(b'[1]\n')))
    with pytest.raises(ValueError):
        list(schema.canonical_lines(io.BytesIO(b'{"quotes":[1]}\n')))

@pytest.mark.parametrize('change', [{'sha256': 'bad'}, {'sha256': 1}, {'bytes': 0}, {'contributor': 'email@test'}, {'license': 'MIT'},
    {'redistribution_permitted': False}, {'summary': {'source': 'live'}}, {'schema': 'raw'}, {'extra': 1}])
def test_manifest_permissions(packaged, change):
    directory, m = packaged; m.update(change)
    with pytest.raises(ValueError):
        schema.validate_manifest(m)

@pytest.mark.parametrize('change', [{'codes': ['bad code']}, {'codes': []}, {'codes': 'x'}, {'batches': -1},
    {'trading_date': 'yesterday'}, {'extra': 1}])
def test_manifest_summary_strict(packaged, change):
    directory, m = packaged; m['summary'].update(change)
    with pytest.raises(ValueError):
        schema.validate_manifest(m)

def test_hash_summary_and_corrupt_gzip(packaged, tmp_path):
    directory, m = packaged; p = directory / 'events.jsonl.gz'
    bad = copy.deepcopy(m); bad['summary']['batches'] = 9
    with pytest.raises(ValueError, match='Summary'):
        schema.inspect_package(p, bad)
    with pytest.raises(ValueError, match='Summary'):
        schema.repack(p, bad, tmp_path / 'out.gz')
    with pytest.raises(ValueError, match='mismatch'):
        schema.repack(p, {**m, 'bytes': 1}, tmp_path / 'out.gz')
    p.write_bytes(b'not gzip'); m['bytes'] = p.stat().st_size; m['sha256'] = schema.digest(p)
    with pytest.raises(OSError):
        schema.inspect_package(p, m)

def corrupt_deflate(data):
    return data[:20] + bytes(b ^ 0xFF for b in data[20:40]) + data[40:]

@pytest.mark.parametrize('corrupt', [b'bad', None, 'tampered', 'deflate', 'nested'])
def test_rejection_stays_private(packaged, corrupt):
    directory, m = packaged; s3 = S3(); db = DB()
    ticket = ticket_for(s3, db, m)
    if corrupt is None:
        m = copy.deepcopy(m); m['summary']['batches'] = 123
        service.json_put(s3, f'incoming/{ticket}/manifest.json', m)
    elif corrupt == 'tampered':
        # Structurally valid but not the reference replay, with a matching manifest.
        b = batches(); b[1]['quotes'][0]['indicative_price10'] = 1250
        corrupt = gzip.compress(stream(b).read())
        m = {**m, 'sha256': hashlib.sha256(corrupt).hexdigest(), 'bytes': len(corrupt)}
        service.json_put(s3, f'incoming/{ticket}/manifest.json', m)
    elif corrupt in {'deflate', 'nested'}:
        corrupt = (corrupt_deflate((directory / 'events.jsonl.gz').read_bytes()) if corrupt == 'deflate'
                   else gzip.compress(b'[' * 200000 + b'\n'))
        m = {**m, 'sha256': hashlib.sha256(corrupt).hexdigest(), 'bytes': len(corrupt)}
        service.json_put(s3, f'incoming/{ticket}/manifest.json', m)
    service.ingest(upload(s3, m, directory, ticket, data=corrupt), s3, db)
    status = service.json_get(s3, f'incoming/{ticket}/status.json')
    assert status['status'] == 'rejected'
    assert not list(cli.manifests(s3, 'bucket')) and not s3.versions

def test_api_errors_and_handler(packaged, monkeypatch):
    directory, m = packaged; s3 = S3(); db = DB()
    monkeypatch.setattr(service, 'clients', lambda: (s3, db))
    assert service.handler(event('DELETE'), None)['statusCode'] == 400
    assert service.handler(event('GET', query='ticket=' + str(uuid.uuid4())), None)['statusCode'] == 404
    assert service.handler(event('GET', query='ticket=wrong'), None)['statusCode'] == 400
    assert service.handler(event(body={'secret': 'bad'}), None)['statusCode'] == 400
    nested = event(); nested['body'] = '[' * 200000
    assert service.handler(nested, None)['statusCode'] == 400
    db.reject = True
    assert service.handler(event(body=m), None)['statusCode'] == 400
    db.reject = False
    monkeypatch.setattr(service, 'QUOTA_KEY', b'')
    assert service.handler(event(body=m), None)['statusCode'] == 400
    monkeypatch.setattr(service, 'QUOTA_KEY', b'test-key')
    request = event(body=m)
    import base64
    request['isBase64Encoded'] = True; request['body'] = base64.b64encode(request['body'].encode()).decode()
    ticket = json.loads(service.handler(request, None)['body'])['ticket']
    assert service.handler({'Records': [upload(s3, m, directory, ticket)]}, None) == {'ok': True}

def test_quota_key_is_keyed_hash(packaged):
    directory, m = packaged; s3 = S3(); db = DB()
    ticket_for(s3, db, m)
    key = db.calls[0]['Key']['id']['S']
    assert key.split(':')[0] == 'ip-' + hmac.new(b'test-key', b'127.0.0.1', hashlib.sha256).hexdigest()
    assert '127.0.0.1' not in json.dumps(db.calls) and hashlib.sha256(b'127.0.0.1').hexdigest() not in key

@pytest.mark.parametrize('error', ['AccessDenied', 'InternalError'])
def test_cloud_errors_retry(error):
    class Fail:
        def update_item(self, **kw):
            raise ClientError({'Error': {'Code': error}}, 'UpdateItem')
        def put_object(self, **kw):
            raise ClientError({'Error': {'Code': error}}, 'PutObject')
        def delete_object(self, **kw):
            raise ClientError({'Error': {'Code': error}}, 'DeleteObject')
    with pytest.raises(ClientError):
        service.quota(Fail(), 'x', 1, 4, 3600)
    with pytest.raises(ClientError):
        service.json_put(Fail(), 'x', {})
    with pytest.raises(ClientError):
        service.discard(Fail(), 'x', 'v1')

def test_pull_tampered_atomic(packaged, tmp_path):
    directory, m = packaged; s3 = S3()
    prefix = f"archive/20210927/{m['sha256']}"
    service.json_put(s3, prefix + '/manifest.json', m)
    s3.objects[prefix + '/events.jsonl.gz'] = b'bad'
    with pytest.raises(ValueError, match='mismatch'):
        cli.pull(s3, 'bucket', prefix, tmp_path / 'bad')
    assert not (tmp_path / 'bad').exists()
    with pytest.raises(ValueError):
        list(cli.manifests(s3, 'bucket', '../'))
    with pytest.raises(ValueError):
        cli.pull(s3, 'bucket', 'incoming/secret', tmp_path / 'bad')

class HTTP:
    def __init__(self, value=b''): self.value = value
    def __enter__(self): return self
    def __exit__(self, *a): pass
    def read(self, *a): return self.value

@pytest.mark.parametrize('status', ['published', 'rejected', 'timeout'])
def test_contribute_auto_poll(packaged, monkeypatch, status):
    directory, m = packaged
    responses = [{'ticket': str(uuid.uuid4()), 'upload': {'url': 'https://example.test', 'fields': {'key': 'k'}}},
                 {'status': 'awaiting_upload'}, {'status': status}]
    monkeypatch.setattr(cli, 'request_json', lambda *a: responses.pop(0))
    monkeypatch.setattr(cli.urllib.request, 'urlopen', lambda *a, **k: HTTP())
    monkeypatch.setattr(cli.time, 'sleep', lambda *a: None)
    if status == 'timeout':
        monkeypatch.setattr(cli.time, 'monotonic', iter([0, 999]).__next__)
        with pytest.raises(TimeoutError): cli.contribute(directory, 'https://api.test', 1)
    elif status == 'rejected':
        with pytest.raises(ValueError): cli.contribute(directory, 'https://api.test')
    else:
        assert cli.contribute(directory, 'https://api.test', verify=False)['status'] == 'published'
    (directory / 'events.jsonl.gz').write_bytes(b'changed')
    with pytest.raises(ValueError, match='mismatch'):
        cli.contribute(directory, 'https://api.test', verify=False)

def test_request_json(monkeypatch):
    monkeypatch.setattr(cli.urllib.request, 'urlopen', lambda *a, **kw: HTTP(b'{"ok":true}'))
    assert cli.request_json('https://example.test', {'a': 1}) == {'ok': True}

def test_package_failure_leaves_nothing(tmp_path):
    events = tmp_path / 'partial.jsonl'
    events.write_bytes(raw(batches()[:-1]))
    with pytest.raises(ValueError, match='clean end'):
        cli.package(events, tmp_path / 'out', 'test', 'CC0-1.0')
    assert not (tmp_path / 'out/events.jsonl.gz').exists()

@pytest.fixture
def harness(tmp_path, monkeypatch):
    config = tmp_path / 'config.json'
    config.write_text(json.dumps(dict(bucket='b', region='ap-northeast-1', api_url='https://api.test')))
    uploads = []
    monkeypatch.setattr(cli, 'contribute', lambda *a, **k: uploads.append(a) or {'status': 'published'})
    monkeypatch.setattr(cli, 'client', lambda *a: None)
    def run(cmd, check, stdout=None):
        # Node decoder writes to stdout; the optional Rust collector writes --events.
        if stdout:
            stdout.write(raw(batches()))
        else:
            Path(cmd[cmd.index('--events') + 1]).write_bytes(raw(batches()))
    monkeypatch.setattr(cli.subprocess, 'run', run)
    monkeypatch.setattr(cli, 'check_node', lambda node='node': None)
    (tmp_path / 'source.jsonl').write_bytes(raw(batches()))
    def main(*args):
        cli.main(['--config', str(config), *args])
        return uploads
    return main

@pytest.mark.parametrize('command', ['package', 'record', 'upload', 'list', 'pull'])
def test_cli(command, packaged, tmp_path, monkeypatch, capsys, harness):
    directory, m = packaged
    monkeypatch.setattr(cli, 'manifests', lambda *a: [('archive/p', m)])
    monkeypatch.setattr(cli, 'pull', lambda *a: m)
    args = [command]
    if command in {'package', 'record'}:
        args += ['--output', str(tmp_path / 'out'), '--contributor', 'test', '--license', 'CC0-1.0', '--redistribution-permitted', '--upload']
        if command == 'package': args += ['--events', str(tmp_path / 'source.jsonl')]
        else: args += ['--web', '--codes', '0000', '--speed', '0']
    elif command == 'upload': args += [str(directory)]
    elif command == 'list': args += ['--source', 'synthetic_test']
    else: args += ['archive/p', '--output', str(tmp_path / 'out')]
    harness(*args)
    assert capsys.readouterr().out

def test_record_without_consent_stays_local(tmp_path, harness, capsys):
    assert harness('record', '--cache', str(tmp_path), '--output', str(tmp_path / 'local')) == []
    assert (tmp_path / 'local/events.jsonl').exists() and not (tmp_path / 'local/manifest.json').exists()
    assert 'not contributed' in capsys.readouterr().err
    with pytest.raises(ValueError, match='already exists'):
        harness('record', '--cache', str(tmp_path), '--output', str(tmp_path / 'local'))
    with pytest.raises(ValueError, match='consent'):
        harness('package', '--events', str(tmp_path / 'source.jsonl'), '--output', str(tmp_path / 'p'))
    with pytest.raises(ValueError, match='consent'):
        harness('record', '--cache', str(tmp_path), '--output', str(tmp_path / 'x'), '--upload')
    with pytest.raises(ValueError, match='together'):
        harness('package', '--events', str(tmp_path / 'source.jsonl'), '--output', str(tmp_path / 'p'), '--contributor', 'a')

@pytest.mark.parametrize('answer,uploaded', [('\n', True), ('y\n', True), ('n\n', False)])
def test_first_run_prompt(tmp_path, monkeypatch, harness, capsys, answer, uploaded):
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO(answer))
    uploads = harness('record', '--web', '--output', str(tmp_path / 'r'))
    out = capsys.readouterr()
    assert 'public and permanent' in out.err and 'Contribute automatically' not in out.out
    assert bool(uploads) == uploaded and (tmp_path / 'r/manifest.json').exists() == uploaded
    choice = cli.load_consent()
    assert choice['enabled'] == uploaded and choice['policy_version'] == cli.POLICY_VERSION
    if uploaded:
        assert choice['contributor'].startswith('anon-') and choice['license'] == 'CC0-1.0'
        assert json.loads((tmp_path / 'r/manifest.json').read_text())['contributor'] == choice['contributor']
    # The question is asked once; later runs follow the saved choice silently.
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO(''))
    assert bool(harness('record', '--web', '--output', str(tmp_path / 'again'))) == uploaded

def test_consent_controls(tmp_path, monkeypatch, harness, capsys):
    harness('consent')
    assert json.loads(capsys.readouterr().out)['enabled'] is None
    harness('consent', '--accept', '--contributor', 'alice', '--license', 'CC-BY-4.0')
    assert json.loads(capsys.readouterr().out)['contributor'] == 'alice'
    assert harness('record', '--web', '--output', str(tmp_path / 'a')) and \
        json.loads((tmp_path / 'a/manifest.json').read_text())['license'] == 'CC-BY-4.0'
    assert len(harness('record', '--web', '--no-upload', '--output', str(tmp_path / 'b'))) == 1
    assert (tmp_path / 'b/manifest.json').exists()
    monkeypatch.setenv('BRISK_CONTRIBUTE', '0')
    assert len(harness('package', '--events', str(tmp_path / 'source.jsonl'), '--output', str(tmp_path / 'c'))) == 1
    monkeypatch.delenv('BRISK_CONTRIBUTE')
    assert len(harness('record', '--web', '--limit-frames', '2', '--output', str(tmp_path / 'd'))) == 1
    assert (tmp_path / 'd/events.jsonl').exists()
    with pytest.raises(ValueError, match='complete'):
        harness('record', '--web', '--limit-frames', '2', '--upload', '--output', str(tmp_path / 'e'))
    harness('consent', '--revoke')
    assert json.loads(capsys.readouterr().out.splitlines()[-1])['enabled'] is False
    assert len(harness('record', '--web', '--output', str(tmp_path / 'f'))) == 1
    with pytest.raises(ValueError, match='Alias'):
        cli.save_consent(True, 'not an alias!')
    cli.consent_path().write_text('{"policy_version": 0, "enabled": true}')
    assert cli.load_consent() is None
    cli.consent_path().write_text('garbage')
    assert cli.load_consent() is None

def test_record_events_command(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(cli.subprocess, 'run', lambda cmd, check, stdout=None: calls.append((cmd, stdout)))
    monkeypatch.setattr(cli, 'check_node', lambda node='node': None)
    cli.record_events(tmp_path / 'e.jsonl', cache=tmp_path, codes=['7203', '6758'], limit_frames=5, speed=0)
    cmd, stdout = calls[0]
    assert cmd[:2] == ['node', str(cli.DECODER)] and stdout is not None and cli.DECODER.exists()
    assert cmd[cmd.index('--codes') + 1] == '7203,6758' and '--limit-frames' in cmd and '--cache' in cmd
    cli.record_events(tmp_path / 'r.jsonl', web=True, binary=tmp_path / 'collector')
    cmd, stdout = calls[1]
    assert cmd[0] == str(tmp_path / 'collector') and cmd[cmd.index('--decoder') + 1] == str(cli.DECODER)
    assert '--web' in cmd and stdout is None


def test_lambda_package_is_self_contained(tmp_path):
    """The deployed zip imports the service and loads references without the client API."""
    import importlib.util
    import os
    import subprocess
    import sys
    import zipfile
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('deploy', root / 'infra/deploy.py')
    deploy = importlib.util.module_from_spec(spec); spec.loader.exec_module(deploy)
    with zipfile.ZipFile(io.BytesIO(deploy.package())) as z:
        assert sorted(z.namelist()) == ['archive_service.py', 'briskapi/__init__.py',
                                        'briskapi/references/historical_mock.json', 'briskapi/schema.py']
        z.extractall(tmp_path)
    check = ("import archive_service, briskapi, briskapi.schema as s, sys; "
             f"assert briskapi.__file__.startswith({str(tmp_path)!r}); "
             "assert sorted(s.REFERENCES) == ['historical_mock', 'synthetic_test']; "
             "assert 'briskapi.cli' not in sys.modules")
    subprocess.run([sys.executable, '-c', check], cwd=tmp_path, check=True,
                   env={'PYTHONPATH': str(tmp_path), 'PATH': os.environ.get('PATH', '')})
