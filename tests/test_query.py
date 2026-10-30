"""The cloud API: query_engine over stored recordings, owners' bars, query_service and briskapi.Historical."""
import base64
import copy
import datetime as dt
import io
import json
from pathlib import Path
import urllib.error
import urllib.parse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

import archive_service as service
import briskapi
import briskapi.cli as cli
import briskapi.schema as schema
import query_engine as engine
import query_service
from briskapi import _historical, identity
from test_archive import (DB, OTHER, OWNER, S3, SECRET, isolated, packaged, stream, ticket_for, upload,  # noqa: F401
                          stored)
from test_spool import with_book_changes

JST = dt.timezone(dt.timedelta(hours=9))
DATE = '20261013'
CODES = ('7203', '167A')
OPEN_US = 9 * 3600 * 1_000_000
THIRD = 'brisk-id-v1-' + 'c' * 26
OTHER_OWNER = schema.owner(OTHER)


def trades_at(second):
    """The day's trades at this second after 09:00: Toyota every 10 s, 167A every 30 s."""
    out = []
    if second and second % 10 == 0:
        out.append((0, 30000 + 10 * (second // 10 % 7), 100 * (1 + second // 10 % 3)))
    if second and second % 30 == 0:
        out.append((1, 5000 + 10 * (second // 30 % 4), 200))
    return out


def cumulative(issue, second):
    volume = turnover = 0
    for s in range(second + 1):
        for i, price10, quantity in trades_at(s):
            if i == issue:
                volume, turnover = volume + quantity, turnover + price10 * quantity
    return volume, turnover


def session(first, last, date=DATE, source='brisk_live', lag_ms=20, changes=False):
    """A broker session from `first` to `last` (seconds after 09:00), one quote batch per second, each received `lag_ms`
    after BRiSK's stamp; with `changes`, its quotes have book changes (as a broker's from 1.9.0 on)."""
    midnight_ms = int(dt.datetime.strptime(date, '%Y%m%d').replace(tzinfo=JST).timestamp() * 1000)
    master = [{**dict.fromkeys(schema.MASTER_KEYS, 0), 'issue_id': i, 'code': code, 'name': f'Issue {code}', 'lot_size': 100,
               'base_price10': 30000 if i == 0 else 5000, 'limit_up10': 39000 if i == 0 else 6500,
               'limit_down10': 21000 if i == 0 else 3500, 'tick_type': 1, 'issue_type': 1} for i, code in enumerate(CODES)]

    def quote(i, second):
        volume, turnover = cumulative(i, second)
        price = 30000 if i == 0 else 5000
        opening = second < 5
        return {**dict.fromkeys(schema.QUOTE_KEYS, 0), **{key: [] for key in schema.BOOK_KEYS},
                'issue_id': i, 'code': CODES[i], 'frame': second + 1, 'max_frame': second + 1,
                'source_time_us': OPEN_US + second * 1_000_000, 'last_price10': price, 'volume': volume,
                'turnover10': turnover, 'bid_levels': [[price - 10, 300, 3], [price - 20, 200, 2]],
                'ask_levels': [[price + 10, 100, 1]], 'indicative_price10': price if opening else 0,
                'indicative_volume': 1000 if opening else 0, 'market_buy_quantity': 500 if opening else 0,
                'market_sell_quantity': 200 if opening else 0, 'reference_price10': price}

    batches = [dict(type='bootstrap', seq=0, source=source, trading_date=date, source_time_us=OPEN_US + first * 1_000_000,
                    input_transport={**schema.BROKER_TRANSPORTS['sbi'], 'decoder': '/private/decoder.js'},
                    source_timestamp_origin='brisk_decoder_unverified', exchange_delay_ms=None, market_issue_count=2,
                    master=master, quotes=[quote(0, first), quote(1, first)], trades=[])]
    for seq, second in enumerate(range(first + 1, last + 1), 1):
        now = OPEN_US + second * 1_000_000
        trades = [{'issue_id': i, 'code': CODES[i], 'frame': second + 1, 'time_us': now, 'price10': price10,
                   'quantity': quantity, 'side': 1 if i == 0 else 2} for i, price10, quantity in trades_at(second)]
        batches.append(dict(type='quotes', seq=seq, source_time_us=now, received_unix_ms=midnight_ms + now // 1000 + lag_ms,
                            decode_ns=250_000, replay_lateness_ms=None, quotes=[quote(0, second), quote(1, second)],
                            trades=trades))
    n = len(batches)
    batches.append(dict(type='end', seq=n, source_time_us=OPEN_US + last * 1_000_000, frames=n,
                        quote_updates=2 * (n - 1), replay_wall_ms=1.0))
    return with_book_changes(batches) if changes else batches


@pytest.fixture
def archive(tmp_path):
    s3, db = S3(), DB()

    def put(batches, secret=SECRET, name=None):
        events = tmp_path / f'{name or len(list(tmp_path.iterdir()))}.jsonl'
        events.write_bytes(stream(batches).read())
        directory = tmp_path / f'package-{events.stem}'
        m = cli.package(events, directory)
        ticket = ticket_for(s3, db, m, secret)
        service.ingest(upload(s3, m, directory, ticket), s3, db)
        status = service.json_get(s3, f'incoming/{ticket}/status.json')
        assert status['status'] == 'stored', status
        return status['prefix']
    return s3, db, put


def query(s3, owner, **params):
    return engine.get_range(engine.Bucket(s3, service.BUCKET), owner, engine.request(**params))


def at(second):
    return (dt.datetime(2026, 10, 13, 9, tzinfo=JST) + dt.timedelta(seconds=second)).isoformat()


def test_your_own_records_merged_deduplicated_and_time_ordered(archive):
    s3, db, put = archive
    put(session(0, 150))
    put(session(120, 300))                     # overlaps the first from 09:02:00 to 09:02:30
    put(session(0, 300, date='20261014'), OTHER)
    trades = query(s3, OWNER, dataset='XTKS.BRISK', schema='trades', symbols='7203', start='2026-10-13')
    assert trades.column('sequence').to_pylist() == [s + 1 for s in range(10, 301, 10)]   # each trade once
    assert trades.column('price').to_pylist()[0] == 3001.0 and set(trades.column('side').to_pylist()) == {'B'}
    assert trades.column('instrument_id').to_pylist()[0] == 7203 and trades.column('symbol').to_pylist()[0] == '7203'
    recv = pc.cast(trades.column('ts_recv'), pa.int64()).to_pylist()
    event = pc.cast(trades.column('ts_event'), pa.int64()).to_pylist()
    assert all(r - e == 20_000_000 for r, e in zip(recv, event))   # local receipt, 20 ms after BRiSK's stamp
    # 167A's sellers, its stable ID, and nothing of the other owner's day.
    other = query(s3, OWNER, dataset='XTKS.BRISK', schema='trades', symbols=['167A'], start=at(0), end=at(200))
    assert other.column('instrument_id').to_pylist()[0] == 154694 and set(other.column('side').to_pylist()) == {'A'}
    assert other.num_rows == 6
    assert query(s3, OWNER, dataset='XTKS.BRISK', schema='trades', start='2026-10-14').num_rows == 0
    assert query(s3, OTHER_OWNER, dataset='XTKS.BRISK', schema='trades', start='2026-10-14').num_rows == 40
    assert query(s3, OWNER, dataset='XTKS.BRISK', schema='trades', start='2026-10-13', limit=3).num_rows == 3


def test_books_definitions_imbalance_and_everything_else(archive):
    s3, db, put = archive
    put(session(0, 60))
    books = query(s3, OWNER, dataset='XTKS.BRISK', schema='mbp-10', symbols='7203', start=at(0), end=at(10))
    row = books.slice(0, 1).to_pylist()[0]
    assert row['flags'] == 32 and row['action'] == 'N' and row['sequence'] == 1          # the session's snapshot
    assert (row['bid_px_00'], row['bid_sz_00'], row['bid_ct_00'], row['bid_px_01']) == (2999.0, 300, 3, 2998.0)
    assert row['bid_px_02'] is None and (row['ask_px_00'], row['ask_px_01']) == (3001.0, None)
    assert books.num_rows == 10 and books.slice(1, 1).to_pylist()[0]['flags'] == 0
    definition = query(s3, OWNER, dataset='XTKS.BRISK', schema='definition', start='2026-10-13').to_pylist()
    assert [(d['raw_symbol'], d['min_lot_size_round_lot'], d['high_limit_price'], d['low_limit_price']) for d in definition] == \
        [('7203', 100, 3900.0, 2100.0), ('167A', 100, 650.0, 350.0)]
    imbalance = query(s3, OWNER, dataset='XTKS.BRISK', schema='imbalance', symbols='7203', start='2026-10-13').to_pylist()
    assert len(imbalance) == 5 and {r['auction_type'] for r in imbalance} == {'O'}
    assert (imbalance[0]['ind_match_price'], imbalance[0]['paired_qty'], imbalance[0]['market_imbalance_qty'],
            imbalance[0]['market_imbalance_side'], imbalance[0]['upper_collar']) == (3000.0, 1000, 300, 'B', 3900.0)
    quotes = query(s3, OWNER, dataset='XTKS.BRISK', schema='brisk-quotes', symbols='167A', start=at(0), end=at(2))
    assert {'turnover', 'closing_bids', 'bid_total_quantity', 'special_quote_time'} <= set(quotes.column_names)
    assert quotes.num_rows == 2


def bars(s3, owner, schema_, **params):
    return query(s3, owner, dataset='XTKS.BRISK', schema=schema_, start='2026-10-13', **params)


def test_each_owner_gets_bars_built_from_its_own_recordings(archive):
    s3, db, put = archive
    mine = put(session(0, 150))               # 09:00:00 to 09:02:30
    put(session(0, 300), OTHER)               # 09:00:00 to 09:05:00
    root, theirs = engine.bars_root(OWNER, DATE), engine.bars_root(OTHER_OWNER, DATE)
    assert sorted(k for k in s3.objects if k.startswith(engine.BARS_ROOT + '/')) == \
        sorted(f'{r}/{name}.parquet' for r in (root, theirs) for name in engine.BARS)
    minute = bars(s3, OWNER, 'ohlcv-1m', symbols='7203').to_pylist()
    # Whole minutes inside 09:00:00 to 09:02:30 only.
    assert [m['ts_event'].astimezone(JST).strftime('%H:%M') for m in minute] == ['09:00', '09:01']
    assert minute[0]['volume'] == sum(q for s in range(10, 60, 10) for i, _, q in trades_at(s) if i == 0)
    assert (minute[0]['open'], minute[0]['close']) == (3001.0, 3005.0)
    assert [m['ts_event'].astimezone(JST).strftime('%H:%M') for m in bars(s3, OTHER_OWNER, 'ohlcv-1m', symbols='7203').to_pylist()] == \
        ['09:00', '09:01', '09:02', '09:03', '09:04']
    seconds = bars(s3, OWNER, 'ohlcv-1s', symbols='7203')
    # The 09:02:30 second runs past the recording's last clock (09:02:30.000000), so 09:02:20 is the last whole one.
    assert pc.max(seconds.column('ts_event')).as_py().astimezone(JST).strftime('%H:%M:%S') == '09:02:20'
    # An hour or a day needs the whole session part of it covered.
    assert bars(s3, OTHER_OWNER, 'ohlcv-1h').num_rows == 0 and bars(s3, OTHER_OWNER, 'ohlcv-1d').num_rows == 0
    assert bars(s3, THIRD_OWNER(), 'ohlcv-1m').num_rows == 0
    # A second recording of the owner's own, overlapping the first: each trade counted once, and the other owner's
    # bars untouched.
    built = {k: v for k, v in s3.objects.items() if k.startswith(theirs)}
    later = put(session(120, 300))
    assert bars(s3, OWNER, 'ohlcv-1m', symbols='7203') == bars(s3, OTHER_OWNER, 'ohlcv-1m', symbols='7203')
    assert {k: v for k, v in s3.objects.items() if k.startswith(theirs)} == built
    # Deleting a recording rebuilds its owner's bars at once; restoring brings it back.
    assert service.api(_event('delete', later, SECRET), s3, db)['statusCode'] == 200
    assert bars(s3, OWNER, 'ohlcv-1m', symbols='7203').to_pylist() == minute
    service.api(_event('delete', mine, SECRET), s3, db)
    assert not [k for k in s3.objects if k.startswith(root)]
    service.api(_event('restore', mine, SECRET), s3, db)
    assert f'{root}/ohlcv-1m.parquet' in s3.objects and len(bars(s3, OWNER, 'ohlcv-1m', symbols='7203')) == 2
    # Without built bars (a rebuild failed), a query builds the same from the recordings.
    for key in [k for k in s3.objects if k.startswith(root)]:
        del s3.objects[key]
    assert bars(s3, OWNER, 'ohlcv-1m', symbols='7203').to_pylist() == minute


def test_a_days_bars_are_the_same_built_a_range_of_issues_at_a_time(archive, monkeypatch):
    from briskapi import _parquet
    s3, db, put = archive
    monkeypatch.setattr(_parquet, 'ROW_GROUP', 4)    # many row groups, each holding some issue's trades
    put(session(0, 150))
    put(session(120, 300))                          # overlapping: each trade is counted once
    bucket = engine.Bucket(s3, service.BUCKET)
    recs = engine.recordings(bucket, OWNER, [DATE])
    assert len(recs) == 2
    whole = engine.day_bars(bucket, recs)
    monkeypatch.setattr(engine, 'BAR_ROWS', 1)      # a range a row group
    built = []
    real = engine.build_bars
    monkeypatch.setattr(engine, 'build_bars', lambda *a, **k: built.append(1) or real(*a, **k))
    by_range = engine.day_bars(bucket, recs)
    monkeypatch.setattr(engine, 'build_bars', real)
    assert len(built) > 1, 'more than one range'
    everything = [engine._bar_trades(pq.read_table(io.BytesIO(bucket.read(r.files[engine.TRADES])), columns=engine._BAR_COLUMNS))
                  for r in recs]
    at_once = engine.build_bars(everything)
    assert set(whole) == set(by_range) == set(at_once) == set(engine.BARS)
    for name in whole:
        assert by_range[name].equals(whole[name]) and at_once[name].equals(whole[name]), name
    assert whole['ohlcv-1m'].num_rows > 3 and set(whole['ohlcv-1m'].column('symbol').to_pylist()) == set(CODES)


def made_up(price10=None):
    """A made-up broker session that passes every check: a bootstrap at 09:00:00 and batches at 09:00:50 (with one
    7203 print at `price10`, if given, in the frame of the real 09:00:50 print) and at 15:30:00, so that its clock
    range spans the trading day."""
    bootstrap, batch, end = session(0, 1)
    out = [bootstrap]
    for seq, second in enumerate((50, 23400), 1):
        b, us = copy.deepcopy(batch), OPEN_US + second * 1_000_000
        b.update(seq=seq, source_time_us=us, received_unix_ms=batch['received_unix_ms'] + (second - 1) * 1000, trades=[])
        for q in b['quotes']:
            q.update(source_time_us=us, frame=second + 1, max_frame=second + 1)
        if price10:
            b['quotes'][0].update(volume=100, turnover10=price10 * 100)
            if second == 50:
                b['trades'] = [{'issue_id': 0, 'code': '7203', 'frame': 51, 'time_us': us, 'price10': price10,
                                'quantity': 100, 'side': 1}]
        out.append(b)
    return out + [{**end, 'seq': 3, 'source_time_us': OPEN_US + 23400 * 1_000_000, 'frames': 3, 'quote_updates': 4}]


def test_no_upload_reveals_or_changes_another_owners_bars(archive):
    """Both worked on 1.0.0's shared bars: covering the day with a made-up session to receive everyone's bars, and
    a made-up price for a real print, from an owner ID that sorts first, replacing the real one in everyone's."""
    s3, db, put = archive
    put(session(0, 300))
    honest = bars(s3, OWNER, 'ohlcv-1m', symbols='7203').to_pylist()
    put(made_up(), OTHER)
    assert bars(s3, OTHER_OWNER, 'ohlcv-1m', symbols='7203').num_rows == 0
    attacker = next(s for s in iter(identity.new, None) if schema.owner(s) < OWNER)
    put(made_up(price10=99990), attacker)
    assert bars(s3, OWNER, 'ohlcv-1m', symbols='7203').to_pylist() == honest
    assert [b['high'] for b in bars(s3, schema.owner(attacker), 'ohlcv-1m', symbols='7203').to_pylist()] == [9999.0]


def THIRD_OWNER():
    return schema.owner(THIRD)


def _event(action, prefix, secret, method='POST'):
    return {'requestContext': {'http': {'method': method, 'sourceIp': '127.0.0.1'}},
            'headers': {'authorization': f'Bearer {secret}'}, 'body': json.dumps({'action': action, 'prefix': prefix})}


def test_a_failed_rebuild_removes_the_days_bars(archive, monkeypatch):
    s3, db, put = archive
    mine = put(session(0, 120))
    assert f'{engine.bars_root(OWNER, DATE)}/ohlcv-1m.parquet' in s3.objects
    monkeypatch.setattr(engine, 'rebuild_bars', lambda *a: 1 / 0)
    service.api(_event('delete', mine, SECRET), s3, db)
    assert not [k for k in s3.objects if k.startswith(engine.bars_root(OWNER, DATE))]


def test_the_demo_is_open_to_every_identity(packaged, archive, monkeypatch):
    s3, db, put = archive
    monkeypatch.setattr(engine, 'DEMO_SOURCE', 'synthetic_test')
    bucket = engine.Bucket(s3, service.BUCKET)
    with pytest.raises(engine.NotFound):
        engine.demo(bucket)
    stored(s3, db, *packaged)                 # the first verified replay becomes the archive's copy
    request = engine.request('XTKS.BRISKDEMO', 'trades', start='2021-09-27')
    trades = engine.get_range(bucket, THIRD_OWNER(), request).to_pylist()
    assert [(t['symbol'], t['price'], t['size']) for t in trades] == [('0000', 124.0, 100)]
    assert trades[0]['ts_recv'] == trades[0]['ts_event']     # a replay has no receipt time of its own
    second = engine.get_range(bucket, THIRD_OWNER(), engine.request('XTKS.BRISKDEMO', 'ohlcv-1s', start='2021-09-27'))
    assert second.to_pylist()[0]['volume'] == 100
    assert engine.dataset_dates(bucket, THIRD_OWNER(), 'XTKS.BRISKDEMO') == {'20210927': [['00:00:00', '00:00:00']]}
    assert engine.get_range(bucket, THIRD_OWNER(), engine.request('XTKS.BRISKDEMO', 'trades', start='2021-09-28')).num_rows == 0


def test_requests_and_encodings():
    midnight = int(dt.datetime(2026, 10, 13, tzinfo=JST).timestamp()) * 10**9
    assert engine.timestamp('2026-10-13') == midnight
    assert engine.timestamp('2026-10-13T09:00') == midnight + 9 * 3600 * 10**9      # JST unless it says otherwise
    assert engine.timestamp('2026-10-13T00:00Z') == midnight + 9 * 3600 * 10**9
    assert engine.timestamp(str(midnight)) == midnight == engine.timestamp(midnight)
    req = engine.request('XTKS.BRISK', start='2026-10-13')
    assert (req.end - req.start, req.symbols, req.limit, req.schema) == (86400 * 10**9, None, None, 'trades')
    assert engine.request('XTKS.BRISK', symbols='7203, 167A', start='2026-10-13', limit='5').symbols == {'7203', '167A'}
    for bad in (dict(dataset='XNAS.ITCH'), dict(schema='mbo'), dict(symbols='toyota'), dict(start=None),
                dict(end='2026-10-12'), dict(end='2026-12-31'), dict(limit='-1'), dict(stype_in='instrument_id'),
                dict(start='yesterday')):
        with pytest.raises(ValueError):
            engine.request(**{'dataset': 'XTKS.BRISK', 'start': '2026-10-13', **bad})
    assert (engine.instrument_id('7203'), engine.instrument_id('167A'), engine.instrument_id('25935')) == (7203, 154694, 25935)
    table = pa.table({'ts_event': pa.array([midnight], engine.TIME), 'price': [3001.5], 'symbol': ['7203'],
                      'book': pa.array([[{'price': 1.0}]])})
    body, content, extension = engine.encode(table, 'csv')
    assert content == 'text/csv' and b'2026-10-12T15:00:00.000000000Z' in body and b'3001.5' in body and b'"[{' in body
    body, _, _ = engine.encode(table, 'json', pretty_ts=False, pretty_px=False, map_symbols=False)
    row = json.loads(body)
    assert row['ts_event'] == midnight and row['price'] == 3001500000000 and 'symbol' not in row
    body, content, extension = engine.encode(table, 'csv', compression='zstd')
    assert content == 'application/zstd' and extension == 'csv.zst'
    assert b'3001.5' in pa.input_stream(pa.BufferReader(body), compression='zstd').read()
    with pytest.raises(ValueError):
        engine.encode(table, 'dbn')
    assert [f['name'] for f in engine.fields('ohlcv-1m')][:2] == ['ts_event', 'publisher_id']
    assert 'bid_px_09' in {f['name'] for f in engine.fields('mbp-10')}
    with pytest.raises(ValueError):
        engine.fields('mbo')


def api_event(path, params=None, secret=SECRET, method='POST', basic=True):
    headers = {}
    if secret:
        headers['authorization'] = ('Basic ' + base64.b64encode(f'{secret}:'.encode()).decode()) if basic else f'Bearer {secret}'
    body = urllib.parse.urlencode(params or {})
    return {'requestContext': {'http': {'method': method, 'sourceIp': '127.0.0.1'}}, 'rawPath': path, 'headers': headers,
            'rawQueryString': body if method == 'GET' else '', 'body': body if method == 'POST' else ''}


def test_the_service(archive, monkeypatch):
    s3, db, put = archive
    put(session(0, 150))
    call = lambda *a, **k: query_service.api(api_event(*a, **k), s3, db)  # noqa: E731
    params = {'dataset': 'XTKS.BRISK', 'schema': 'trades', 'symbols': '7203', 'start': '2026-10-13'}
    got = call('/v0/timeseries.get_range', params)
    assert got['statusCode'] == 200 and got['isBase64Encoded'] and got['headers']['x-record-count'] == '15'
    assert pq.read_table(io.BytesIO(base64.b64decode(got['body']))).num_rows == 15
    got = call('/v0/timeseries.get_range', {**params, 'encoding': 'csv'}, basic=False)
    assert got['headers']['content-type'] == 'text/csv' and got['body'].count('\n') == 16
    # Large results wait in results/OWNER/ behind a 303.
    monkeypatch.setattr(query_service, 'INLINE', 10)
    got = call('/v0/timeseries.get_range', params)
    assert got['statusCode'] == 303 and got['headers']['location'].startswith(f'https://s3.test/results/{OWNER}/')
    monkeypatch.setattr(query_service, 'INLINE', 4 * 1024 * 1024)
    assert json.loads(call('/v0/metadata.list_datasets', method='GET')['body']) == ['XTKS.BRISK', 'XTKS.BRISKDEMO']
    assert json.loads(call('/v0/metadata.list_publishers', method='GET')['body'])[0] == \
        {'publisher_id': 1, 'dataset': 'XTKS.BRISK', 'venue': 'XTKS', 'description': 'Tokyo Stock Exchange via BRiSK'}
    assert 'ohlcv-1m' in json.loads(call('/v0/metadata.list_schemas', {'dataset': 'XTKS.BRISK'}, method='GET')['body'])
    first = json.loads(call('/v0/metadata.list_fields', {'schema': 'trades'}, method='GET')['body'])[0]
    assert (first['name'], first['type']) == ('ts_recv', 'timestamp[ns, tz=UTC]') and 'received' in first['description']
    assert json.loads(call('/v0/metadata.get_dataset_range', {'dataset': 'XTKS.BRISK'}, method='GET')['body']) == \
        {'start': '2026-10-13', 'end': '2026-10-13'}
    condition = json.loads(call('/v0/metadata.get_dataset_condition', {'dataset': 'XTKS.BRISK', 'start_date': '2026-10-12'},
                                method='GET')['body'])
    assert condition == [{'date': '2026-10-12', 'condition': 'missing', 'last_modified_date': None, 'windows': []},
                         {'date': '2026-10-13', 'condition': 'available', 'last_modified_date': None,
                          'windows': [['09:00:00', '09:02:30']]}]
    assert json.loads(call('/v0/metadata.get_record_count', params)['body']) == 15
    assert json.loads(call('/v0/metadata.get_cost', params)['body']) == 0.0
    assert call('/v0/metadata.get_dataset_range', {'dataset': 'XTKS.BRISK'}, secret=OTHER, method='GET')['statusCode'] == 404
    assert call('/v0/timeseries.get_range', params, secret=None)['statusCode'] == 400
    assert call('/v0/timeseries.get_range', params, secret='brisk-id-v1-nope')['statusCode'] == 400
    assert call('/v0/nope', params)['statusCode'] == 400 and call('/v1/x', params)['statusCode'] == 400
    assert call('/v0/timeseries.get_range', {**params, 'pretty_ts': 'maybe'})['statusCode'] == 400
    db.reject = True
    assert call('/v0/metadata.list_datasets', method='GET')['statusCode'] == 429


def test_addresses_are_limited_before_keys_are_read(archive, monkeypatch):
    """Requests without a valid key count against the address, and a fresh identity does not reset it."""
    s3, _, put = archive
    put(session(0, 150))
    db = DB(enforce=True)
    monkeypatch.setattr(query_service, 'ADDRESS', (3, 60))
    call = lambda *a, ip='127.0.0.1', **k: query_service.api({**api_event(*a, **k), 'requestContext': {  # noqa: E731
        'http': {'method': k.get('method', 'POST'), 'sourceIp': ip}}}, s3, db)
    assert call('/v0/metadata.list_datasets', method='GET', secret=None)['statusCode'] == 400
    assert call('/v0/metadata.list_datasets', method='GET', secret='brisk-id-v1-nope')['statusCode'] == 400
    assert call('/v0/metadata.list_datasets', method='GET', secret=OTHER)['statusCode'] == 200
    refused = call('/v0/metadata.list_datasets', method='GET', secret=THIRD)
    assert refused['statusCode'] == 429 and 'from this address' in json.loads(refused['body'])['detail']
    assert call('/v0/metadata.list_datasets', method='GET', ip='2001:db8::1')['statusCode'] == 200
    # One address cannot use up the whole service's day of returned bytes, whatever identities it names.
    monkeypatch.setattr(query_service, 'ADDRESS', (120, 60))
    params = {'dataset': 'XTKS.BRISK', 'schema': 'trades', 'symbols': '7203', 'start': '2026-10-13'}
    size = len(base64.b64decode(call('/v0/timeseries.get_range', params, ip='2001:db8::2')['body']))
    monkeypatch.setattr(query_service, 'ADDRESS_BYTES', (size + 1, 86400))
    refused = call('/v0/timeseries.get_range', params, ip='2001:db8::3', secret=OTHER)  # the same /64
    assert refused['statusCode'] == 429 and 'for this address' in json.loads(refused['body'])['detail']
    assert db.used[next(k for k in db.used if k.startswith('query-bytes-all:'))] == size
    assert call('/v0/timeseries.get_range', params, ip='2001:db8:0:1::1')['statusCode'] == 200


def test_times_out_of_range_are_refused(archive):
    s3, db, _ = archive
    for start in ('1' * 31, '0001-01-01', '9999-12-31T23:59:59-14:00'):
        got = query_service.api(api_event('/v0/timeseries.get_range', {'dataset': 'XTKS.BRISK', 'start': start}), s3, db)
        assert got['statusCode'] == 400, start
    got = query_service.api(api_event('/v0/metadata.get_dataset_condition', {
        'dataset': 'XTKS.BRISKDEMO', 'start_date': '9999-12-01', 'end_date': '9999-12-31'}, method='GET'), s3, db)
    assert got['statusCode'] in (400, 404)


class _Response(io.BytesIO):
    def __init__(self, body, headers):
        super().__init__(body)
        self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


def test_the_python_client(archive, monkeypatch, tmp_path):
    s3, db, put = archive
    put(session(0, 150))

    class Opener:
        def open(self, request, timeout):
            parts = urllib.parse.urlsplit(request.full_url)
            event = {'requestContext': {'http': {'method': request.get_method(), 'sourceIp': '127.0.0.1'}},
                     'rawPath': parts.path, 'rawQueryString': parts.query,
                     'headers': {k.lower(): v for k, v in request.header_items()},
                     'body': (request.data or b'').decode()}
            out = query_service.api(event, s3, db)
            body = out['body'].encode() if not out.get('isBase64Encoded') else base64.b64decode(out['body'])
            if out['statusCode'] >= 300:
                raise urllib.error.HTTPError(request.full_url, out['statusCode'], 'error', out['headers'], io.BytesIO(body))
            return _Response(body, out['headers'])

    monkeypatch.setattr(_historical.urllib.request, 'build_opener', lambda *handlers: Opener())
    monkeypatch.setattr(_historical.urllib.request, 'urlopen',
                        lambda url, timeout, **_: _Response(s3.objects[url.removeprefix('https://s3.test/')], {}))
    with pytest.raises(ValueError, match='Refusing to send your identity'):
        briskapi.Historical(gateway='http://query.test/')
    client = briskapi.Historical(gateway='https://query.test/')
    assert client.key == SECRET and client.metadata.list_datasets() == ['XTKS.BRISK', 'XTKS.BRISKDEMO']
    data = client.timeseries.get_range('XTKS.BRISK', start='2026-10-13', symbols=['7203'], path=tmp_path / 't.parquet')
    assert len(data) == 15 and (tmp_path / 't.parquet').exists() and repr(data) == "DataStore(schema='trades', records=15)"
    frame = data.to_df()
    assert frame.index.name == 'ts_recv' and list(frame['symbol'].unique()) == ['7203']
    # databento.DBNStore's conversions.
    assert data.to_df(price_type='fixed')['price'].iloc[0] == 3001_000_000_000
    import decimal
    assert data.to_df(price_type='decimal')['price'].iloc[0] == decimal.Decimal('3001.0')
    assert data.to_df(pretty_ts=False, map_symbols=False)['ts_event'].dtype.kind == 'i' and 'symbol' not in data.to_df(map_symbols=False)
    assert str(data.to_df(tz='Asia/Tokyo').index.tz) == 'Asia/Tokyo'
    assert [len(chunk) for chunk in data.to_df(count=6)] == [6, 6, 3]
    for bad in (dict(price_type='cents'), dict(pretty_ts=False, tz='Asia/Tokyo')):
        with pytest.raises(ValueError):
            data.to_df(**bad)
    assert len(data.to_ndarray()) == 15 and [len(a) for a in data.to_ndarray(count=10)] == [10, 5]
    assert data.to_ndarray()['price'][0] == 3001.0
    assert (tmp_path / 'c.csv').exists() is False and data.to_csv(tmp_path / 'c.csv', pretty_px=False).read_text().count('\n') == 16
    assert '3001000000000' in (tmp_path / 'c.csv').read_text()
    lines = data.to_json(tmp_path / 'j.json').read_text().splitlines()
    assert len(lines) == 15 and json.loads(lines[0])['symbol'] == '7203'
    assert pq.read_table(data.to_parquet(tmp_path / 'f.parquet', price_type='fixed')).column('price')[0].as_py() == 3001_000_000_000
    assert data.to_file(tmp_path / 'raw.parquet').read_bytes() == (tmp_path / 't.parquet').read_bytes()
    records = []
    data.replay(records.append)
    assert len(records) == 15 and records == list(data) and records[0]['symbol'] == '7203'
    import builtins
    real_import = builtins.__import__
    monkeypatch.setattr(builtins, '__import__', lambda name, *a, **k: (_ for _ in ()).throw(ImportError(name))
                        if name == 'pandas' else real_import(name, *a, **k))
    with pytest.raises(ImportError, match=r"briskapi\[pandas\]"):
        data.to_df()
    assert len(list(data)) == 15                       # iteration needs no pandas
    monkeypatch.setattr(builtins, '__import__', real_import)
    with pytest.raises(ValueError, match='stype_out'):
        client.timeseries.get_range('XTKS.BRISK', start='2026-10-13', stype_out='raw_symbol')
    assert client.timeseries.get_range('XTKS.BRISK', start='2026-10-13', schema='ohlcv-1m').to_df().index.name == 'ts_event'
    monkeypatch.setattr(query_service, 'INLINE', 10)   # followed by hand, without the key
    assert len(client.timeseries.get_range('XTKS.BRISK', start='2026-10-13', symbols='7203')) == 15
    signed = s3.generate_presigned_url
    monkeypatch.setattr(s3, 'generate_presigned_url', lambda *a, **k: signed(*a, **k).replace('https://', 'http://'))
    with pytest.raises(briskapi.BriskError, match='HTTP 303'):    # only to https
        client.timeseries.get_range('XTKS.BRISK', start='2026-10-13', symbols='7203')
    monkeypatch.setattr(s3, 'generate_presigned_url', signed)
    assert client.metadata.get_record_count('XTKS.BRISK', start='2026-10-13', symbols='7203') == 15
    assert client.metadata.get_cost('XTKS.BRISK', start='2026-10-13') == 0.0
    assert client.metadata.get_dataset_range('XTKS.BRISK')['start'] == '2026-10-13'
    assert client.metadata.get_dataset_condition('XTKS.BRISK')[0]['windows'] == [['09:00:00', '09:02:30']]
    assert client.metadata.list_schemas('XTKS.BRISK')[-1] == 'ohlcv-1d' and client.metadata.list_publishers()[1]['publisher_id'] == 2
    assert client.metadata.list_fields('trades')[0]['name'] == 'ts_recv'
    with pytest.raises(briskapi.BriskError, match='HTTP 400: Unknown schema'):
        client.timeseries.get_range('XTKS.BRISK', start='2026-10-13', schema='mbo')
    monkeypatch.setattr(cli, 'settings', lambda path=None: {'api_url': 'x'})
    with pytest.raises(briskapi.BriskError, match='no query API'):
        briskapi.Historical()
    # Without an identity, the client makes one (it is the key) and says how to keep it.
    monkeypatch.delenv('BRISK_IDENTITY')
    with pytest.warns(UserWarning, match='recovery'):
        made = briskapi.Historical(gateway='https://query.test')
    assert schema.IDENTITY.fullmatch(made.key) and briskapi.Historical(gateway='https://query.test').key == made.key


def test_brisk_query(tmp_path, monkeypatch, capsys):
    config = tmp_path / 'archive.json'
    config.write_text(json.dumps({'bucket': 'b', 'region': 'ap-northeast-1', 'api_url': 'https://api.test',
                                  'query_url': 'https://query.test'}))
    seen = []

    def call(self, endpoint, params, post=False):
        seen.append((self.gateway, endpoint, params, post))
        return b'ts_event,price\n1,2\n', 'text/csv'
    monkeypatch.setattr(_historical.Historical, '_call', call)
    cli.main(['--config', str(config), 'query', 'XTKS.BRISK', '--start', '2026-10-13', '--symbols', '7203', '--schema', 'ohlcv-1m'])
    assert capsys.readouterr().out == 'ts_event,price\n1,2\n'
    assert seen[0][:2] == ('https://query.test', 'timeseries.get_range') and seen[0][3]
    assert seen[0][2] | {} == {'dataset': 'XTKS.BRISK', 'schema': 'ohlcv-1m', 'symbols': '7203', 'start': '2026-10-13',
                               'end': None, 'limit': None, 'encoding': 'csv'}
    cli.main(['--config', str(config), 'query', 'XTKS.BRISKDEMO', '--start', '2021-09-27', '--encoding', 'parquet',
              '--output', str(tmp_path / 'out.parquet')])
    assert (tmp_path / 'out.parquet').read_bytes() == b'ts_event,price\n1,2\n'
    with pytest.raises(ValueError, match='Parquet needs --output'):
        cli.main(['--config', str(config), 'query', 'XTKS.BRISK', '--start', '2026-10-13', '--encoding', 'parquet'])


def test_a_query_reads_only_the_row_groups_it_needs(archive, monkeypatch):
    s3, db, put = archive
    from briskapi import _parquet
    monkeypatch.setattr(_parquet, 'ROW_GROUP', 50)          # several row groups per table
    put(session(0, 150))
    read = []
    original = pq.ParquetFile.read_row_groups
    monkeypatch.setattr(pq.ParquetFile, 'read_row_groups', lambda self, groups, **kw: read.append((self.metadata.num_row_groups, list(groups)))
                        or original(self, groups, **kw))
    books = query(s3, OWNER, dataset='XTKS.BRISK', schema='mbp-10', symbols='167A', start=at(120), end=at(125))
    assert books.num_rows == 5 and set(books.column('symbol').to_pylist()) == {'167A'}
    total, groups = read[-1]
    # Sorted by issue, then batch: the group holding them, and one at the issue boundary whose statistics overlap.
    assert total == 7 and groups == [3, 5]


def test_every_schema_returns_exactly_its_documented_fields(archive):
    """get_range's columns and types are FIELDS (metadata.list_fields), with data and without; docs/data-format.md
    describes each one."""
    s3, db, put = archive
    put(session(0, 150, changes=True))
    root = Path(__file__).resolve().parents[1] / 'docs'
    docs, translated = ((root / name).read_text(encoding='utf-8')
                        for name in ('data-format.md', 'data-format.ja.md'))   # both name every field
    for name in engine.SCHEMAS:
        expected = pa.schema([(f, kind) for f, kind, _ in engine.FIELDS[name]])
        for start in ('2026-10-13', '2026-10-20'):          # a day with data, and one without
            got = query(s3, OWNER, dataset='XTKS.BRISK', schema=name, start=start)
            assert got.schema.equals(expected), (name, start)
        assert query(s3, OWNER, dataset='XTKS.BRISK', schema=name, start='2026-10-13').num_rows or name in {'ohlcv-1h', 'ohlcv-1d'}
        for field, _, text in engine.FIELDS[name]:
            for doc in (docs, translated):
                assert text and f'`{field}`' in doc or field.split('_')[0] in {'bid', 'ask'} and '_NN' in doc, (name, field)
    book = query(s3, OWNER, dataset='XTKS.BRISK', schema='brisk-quotes', symbols='7203', start=at(0), end=at(1)).to_pylist()[0]
    assert book['bids'] == [{'price': 2999.0, 'size': 300, 'count': 3}, {'price': 2998.0, 'size': 200, 'count': 2}]


def test_a_record_two_captures_hold_keeps_its_earliest_receipt(archive):
    s3, db, put = archive
    put(session(0, 150, lag_ms=500))
    put(session(120, 300, lag_ms=20))          # overlaps 09:02:00 to 09:02:30, received sooner
    quotes = query(s3, OWNER, dataset='XTKS.BRISK', schema='brisk-quotes', symbols='7203', start='2026-10-13')
    recv = pc.cast(quotes.column('ts_recv'), pa.int64()).to_numpy()
    event = pc.cast(quotes.column('ts_event'), pa.int64()).to_numpy()
    lag_ms = {int(seq): int((r - e) // 1_000_000) for seq, r, e in zip(quotes.column('sequence').to_pylist(), recv, event)}
    assert lag_ms[100] == 500 and lag_ms[200] == 20            # held by one capture each
    assert all(lag_ms[frame] == 20 for frame in range(122, 152))   # held by both: the sooner one, not the first listed


BASE = int(dt.datetime(2026, 10, 13, 9, tzinfo=JST).timestamp()) * 10**9


def quote_rows(rows):
    """A brisk-quotes-like body from (instrument_id, event s, receipt s, sequence) rows, seconds after 09:00 JST."""
    when = lambda values: pa.array([BASE + round(v * 1e9) for v in values], pa.int64()).cast(pa.timestamp('ns', tz='UTC'))  # noqa: E731
    table = pa.table({'ts_recv': when([r[2] for r in rows]), 'ts_event': when([r[1] for r in rows]),
                      'instrument_id': pa.array([r[0] for r in rows], pa.uint32()),
                      'sequence': pa.array([r[3] for r in rows], pa.uint64()),
                      'symbol': pa.array([str(r[0]) for r in rows])})
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


def served(monkeypatch, rows):
    """A Historical client whose cloud API answers every request with these rows, and the requests it saw."""
    seen = []

    def call(self, endpoint, params, post=False):
        seen.append((endpoint, dict(params)))
        return quote_rows(rows), 'application/octet-stream'
    monkeypatch.setattr(_historical.Historical, '_call', call)
    return briskapi.Historical(gateway='https://query.test/'), seen


ROWS = [(7203, 1.0, 1.02, 1), (7203, 9.99, 10.02, 2),      # the second was stamped inside the window, received after it
        (7203, 10.005, 9.995, 3),                         # stamped after the end, received before it (its stamp ran ahead)
        (6758, 0.5, 0.52, 4), (6758, 30.0, 30.02, 5)]     # the last is far outside


def test_get_range_can_cut_on_the_receipt_time(monkeypatch):
    client, seen = served(monkeypatch, ROWS)
    got = client.timeseries.get_range('XTKS.BRISK', start='2026-10-13T09:00:00', end='2026-10-13T09:00:10', schema='brisk-quotes',
                                      ts_filter='ts_recv')
    assert got.schema == 'brisk-quotes' and [r['sequence'] for r in got] == [4, 1, 3]     # by receipt; 2 and 5 are not yet/any more
    params = seen[0][1]                                   # the cloud API is asked for the window padded by skew
    assert (params['start'], params['end']) == (BASE - 10 * 10**9, BASE + 20 * 10**9) and 'limit' not in params
    assert [r['sequence'] for r in client.timeseries.get_range('XTKS.BRISK', start='2026-10-13T09:00:00', end='2026-10-13T09:00:10',
                                                               schema='brisk-quotes')] == [1, 2, 3, 4, 5]   # the default is untouched
    assert 'limit' in seen[1][1] and seen[1][1]['start'] == '2026-10-13T09:00:00'
    # limit is applied after the cut; path writes the cut records; the default window is the trading day.
    one = client.timeseries.get_range('XTKS.BRISK', start='2026-10-13T09:00:00', end='2026-10-13T09:00:10', schema='brisk-quotes',
                                      ts_filter='ts_recv', limit=1)
    assert [r['sequence'] for r in one] == [4]
    day = client.timeseries.get_range('XTKS.BRISK', start='2026-10-13', schema='brisk-quotes', ts_filter='ts_recv', skew=0)
    assert len(day) == 5 and seen[-1][1]['end'] - seen[-1][1]['start'] == 86400 * 10**9
    # BRiSK's stamp running ahead of the receipt clock by more than half of skew is said; a larger skew silences it.
    client, seen = served(monkeypatch, ROWS + [(7203, 15.0, 9.0, 6)])
    with pytest.warns(UserWarning, match=r'6\.0 s ahead'):
        client.timeseries.get_range('XTKS.BRISK', start='2026-10-13T09:00:00', end='2026-10-13T09:00:10', ts_filter='ts_recv')
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        client.timeseries.get_range('XTKS.BRISK', start='2026-10-13T09:00:00', end='2026-10-13T09:00:10', ts_filter='ts_recv', skew=20)
    for bad in (dict(ts_filter='ts_arrival'), dict(ts_filter='ts_recv', schema='ohlcv-1m'), dict(ts_filter='ts_recv', skew=-1)):
        with pytest.raises(ValueError):
            client.timeseries.get_range('XTKS.BRISK', start='2026-10-13', **bad)


def test_a_snapshot_is_the_latest_in_market_order_even_where_the_receipts_run_the_other_way(monkeypatch):
    # Two captures (or a repaired recording) hold 7203's frame 10 as received after its frame 11: the book as it stood is frame 11's.
    client, _ = served(monkeypatch, [(7203, 5.0, 8.0, 10), (7203, 5.5, 6.0, 11), (6758, 2.0, 2.02, 4)])
    held = lambda **options: dict((r['instrument_id'], r['sequence'])   # noqa: E731
                                  for r in client.timeseries.get_snapshot('XTKS.BRISK', **options))
    assert held(at='2026-10-13T09:00:07') == {6758: 4, 7203: 11}       # frame 10 has not reached us yet
    assert held(at='2026-10-13T09:00:09') == {6758: 4, 7203: 11}       # both have: the later frame is the book, not the later receipt
    assert held(at='2026-10-13T09:00:09', ts_filter='ts_event') == {6758: 4, 7203: 11}


def test_get_snapshot_is_the_latest_record_of_each_security_as_of_a_moment(monkeypatch, tmp_path):
    rows = [(7203, 1.0, 1.02, 1), (7203, 5.0, 5.02, 2), (7203, 5.5, 8.5, 3),      # 7203's third reached us at 08.5 s
            (6758, 2.0, 2.02, 4), (6758, 7.0, 7.02, 5), (167, 0.0, 0.02, 6)]
    client, seen = served(monkeypatch, rows)
    snap = client.timeseries.get_snapshot('XTKS.BRISK', at='2026-10-13T09:00:06', schema='brisk-quotes')
    assert [(r['instrument_id'], r['sequence']) for r in snap] == [(167, 6), (6758, 4), (7203, 2)] and len(snap) == 3
    assert snap.schema == 'brisk-quotes'
    midnight = BASE - 9 * 3600 * 10**9
    assert (seen[0][1]['start'], seen[0][1]['end']) == (midnight - 10**10, BASE + 6 * 10**9 + 1 + 10**10)   # padded by skew
    # as of 09:00:09 the third reached us; by BRiSK's stamp it was already there at 06 s.
    assert dict((r['instrument_id'], r['sequence']) for r in client.timeseries.get_snapshot(
        'XTKS.BRISK', at='2026-10-13T09:00:09', path=tmp_path / 's.parquet'))[7203] == 3 and (tmp_path / 's.parquet').exists()
    by_stamp = client.timeseries.get_snapshot('XTKS.BRISK', at='2026-10-13T09:00:06', ts_filter='ts_event')
    assert dict((r['instrument_id'], r['sequence']) for r in by_stamp)[7203] == 3
    assert len(client.timeseries.get_snapshot('XTKS.BRISK', at='2026-10-13T08:00:00')) == 0        # nothing yet
    assert seen[-1][1]['start'] == midnight - 10**10
    client.timeseries.get_snapshot('XTKS.BRISK', at='2026-10-13T09:00:06', since='2026-10-13T08:30', ts_filter='ts_event')
    assert seen[-1][1]['start'] == BASE - 30 * 60 * 10**9
    for bad in (dict(schema='ohlcv-1m'), dict(since='2026-10-13T10:00')):
        with pytest.raises(ValueError):
            client.timeseries.get_snapshot('XTKS.BRISK', at='2026-10-13T09:00:06', **bad)
