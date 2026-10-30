import datetime as dt
import gzip
import io
import json
from pathlib import Path
import sys

import pytest

import reference_package
import briskapi as brisk
import briskapi.cli as cli
import briskapi.schema as schema
import warnings
from briskapi import _archive, _live, _parquet, _recording

TOYOTA = {'issue_id': 1, 'code': '7203', 'tick_type': 3, 'base_price10': 101000, 'limit_up10': 131000,
          'limit_down10': 71000, 'lot_size': 100, 'issue_type': 111, 'name': 'Toyota'}


def quote(issue, code, frame, at, **fields):
    return {**dict.fromkeys(schema.QUOTE_KEYS, 0), **{key: [] for key in schema.BOOK_KEYS}, 'issue_id': issue, 'code': code,
            'frame': frame, 'max_frame': frame, 'source_time_us': at, **fields}


def market():
    """Two securities around the 09:00 open; source clock is microseconds since JST midnight."""
    open_us = 9 * 3600 * 1_000_000
    sony = {**TOYOTA, 'issue_id': 0, 'code': '6758', 'name': 'Sony', 'base_price10': 120000}
    timing = dict(received_unix_ms=1632700800000, decode_ns=1000, replay_lateness_ms=None)
    trade = lambda frame, at, price10, quantity, side: dict(issue_id=1, code='7203', frame=frame, time_us=at, price10=price10,  # noqa: E731
                                                          quantity=quantity, side=side)
    return [
        dict(type='bootstrap', seq=0, source='historical_mock', trading_date='20210927', source_time_us=open_us - 45,
             input_transport={'kind': 'local_cache'}, source_timestamp_origin='brisk_decoder_unverified', exchange_delay_ms=None,
             master=[sony, TOYOTA], market_issue_count=2, quotes=[
                 quote(0, '6758', 1, open_us - 50, indicative_price10=120500, market_buy_quantity=500, market_sell_quantity=900),
                 quote(1, '7203', 1, open_us - 45, indicative_price10=101500, indicative_volume=304700,
                       market_buy_quantity=201300, market_sell_quantity=105700, special_quote_time_us=open_us - 1,
                       bid_levels=[[101500, 3000, 4], [101400, 100, 1]], ask_levels=[[101600, 200, 2]],
                       bid_closing_levels=[[101500, 500, 1]], bid_closing_total_quantity=500, bid_closing_total_orders=1,
                       bid_total_quantity=3100, ask_total_quantity=200, quote_flag=2, quote_side=1,
                       reference_price10=101500)], trades=[]),
        dict(type='quotes', seq=1, source_time_us=open_us + 10, **timing, quotes=[
            quote(1, '7203', 2, open_us + 10, last_price10=101500, volume=304700, turnover10=30927050000)],
             trades=[trade(2, open_us + 5, 101500, 304700, 0)]),
        dict(type='quotes', seq=2, source_time_us=open_us + 20, **timing, quotes=[
            quote(0, '6758', 2, open_us + 20, last_price10=120500)], trades=[]),
        dict(type='quotes', seq=3, source_time_us=open_us + 30, **timing, quotes=[
            quote(1, '7203', 3, open_us + 30, last_price10=101600, volume=310000, turnover10=31465530000)],
             trades=[trade(3, open_us + 25, 101600, 2000, 1), trade(3, open_us + 25, 101600, 3300, 1)]),
        dict(type='end', seq=4, source_time_us=open_us + 30, frames=4, quote_updates=3, replay_wall_ms=1.5),
    ]


def save(batches, directory, form):
    """A recording of these batches: an events.jsonl or a Parquet recording directory."""
    directory.mkdir(parents=True, exist_ok=True)
    if form == 'jsonl':
        (directory / 'events.jsonl').write_text(''.join(json.dumps(b) + '\n' for b in batches))
    else:
        _parquet.write(batches, directory)
    return brisk.Recording(directory)


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path / 'cache'))
    monkeypatch.delenv('BRISK_UPLOAD', raising=False)
    monkeypatch.setattr(brisk, '_current', None)


@pytest.fixture(params=['parquet', 'jsonl'])
def recording(tmp_path, request):
    return brisk.load(save(market(), tmp_path / 'rec', request.param))


def test_recording_basics(recording, tmp_path):
    assert recording.codes == ['6758', '7203'] and recording.trading_date == '20210927'
    assert recording.source == 'historical_mock' and recording.manifest is None
    assert len(list(recording.batches())) == 5 and 'rec' in repr(recording)
    gz = tmp_path / 'gz' / 'events.jsonl.gz'
    gz.parent.mkdir()
    gz.write_bytes(gzip.compress(b''.join(recording.lines())))
    assert brisk.Recording(gz.parent).codes == recording.codes
    (tmp_path / 'empty').mkdir()
    with pytest.raises(brisk.NotFoundError, match='No recording in'):
        brisk.Recording(tmp_path / 'empty')
    with pytest.raises(brisk.NotFoundError, match='No recording'):
        brisk.Recording(tmp_path / 'missing.jsonl')
    with pytest.raises(brisk.NotFoundError) as error:
        recording.issue('9999')
    assert str(error.value) == '9999 is not in this recording'


def test_ticker(recording):
    toyota = brisk.Ticker('7203')
    assert repr(toyota) == "Ticker('7203')"
    info = toyota.info()
    assert info['name'] == 'Toyota' and info['base_price'] == 10100.0 and info['limit_up'] == 13100.0
    pre_open = toyota.quote(at='08:59:59.99996')
    assert pre_open['indicative_price'] == 10150.0 and pre_open['last_price'] is None
    assert pre_open['time'] == dt.datetime(2021, 9, 27, 8, 59, 59, 999955, tzinfo=brisk.JST)
    assert pre_open['special_quote_time'].second == 59 and 'source_time_us' not in pre_open
    assert toyota.quote()['last_price'] == 10160.0 and toyota.quote(raw=True)['last_price10'] == 101600
    auction = toyota.auction(at=dt.time(8, 59, 59, 999999))
    assert auction['market_order_imbalance'] == 95600 and auction['indicative_volume'] == 304700
    assert [q['frame'] for q in toyota.history()] == [1, 2, 3]
    assert [q['frame'] for q in toyota.history(start='09:00', end='09:00:00.000020')] == [2]
    assert toyota.history(raw=True)[0]['indicative_price10'] == 101500
    with pytest.raises(brisk.NotFoundError):
        toyota.quote(at='08:00')


def test_market(recording, tmp_path):
    m = brisk.Market()
    assert [s['code'] for s in m.stocks()] == ['6758', '7203'] and m.stocks()[0]['base_price'] == 12000.0
    snapshot = m.snapshot(at=32_399_999_960)
    assert [q['frame'] for q in snapshot] == [1, 1]
    assert m.snapshot(raw=True)[1]['frame'] == 3 and m.snapshot()[1]['frame'] == 3  # cached final state
    ranked = m.imbalances(at='08:59:59.99999', top=1)
    assert len(ranked) == 1 and ranked[0]['code'] == '7203' and ranked[0]['market_order_imbalance'] == 95600
    assert len(m.imbalances(at='08:59:59.99999')) == 2
    summary = m.summary()
    assert summary['securities'] == 2 and summary['end'].microsecond == 30
    (recording.directory / 'manifest.json').write_text(json.dumps(
        {'summary': {'last_source_time_us': 32_400_000_030, 'batches': 5}}))
    summary = brisk.Market(brisk.Recording(recording.directory)).summary()
    assert summary['batches'] == 5 and summary['end'].microsecond == 30


@pytest.mark.parametrize('form', ['parquet', 'jsonl'])
def test_state_uses_quote_clocks_even_in_later_batches(tmp_path, form):
    batches = market()
    open_us = 32_400_000_000
    batches[0]['quotes'][0]['source_time_us'] = open_us - 7655
    batches[0]['quotes'][1]['source_time_us'] = open_us - 6449
    batches[1]['quotes'][0]['source_time_us'] = open_us + 5
    recording = save(batches, tmp_path / 'rec', form)
    toyota = brisk.Ticker('7203', recording)
    initial = toyota.history(raw=True)[0]
    assert toyota.quote(at='08:59:59.9999', raw=True) == initial
    assert toyota.quote(at=initial['source_time_us'], raw=True) == initial
    with pytest.raises(brisk.NotFoundError):
        toyota.quote(at=initial['source_time_us'] - 1)
    with pytest.raises(brisk.NotFoundError):
        toyota.quote(at='08:59:59.99')
    # The 09:00:00.000010 batch contains a quote stamped .000005.
    assert toyota.quote(at=open_us + 5, raw=True) == toyota.history(end=open_us + 5, raw=True)[-1]
    snapshot = brisk.Market(recording).snapshot(at=open_us + 5, raw=True)
    assert [q['frame'] for q in snapshot] == [1, 2]
    assert all(q['source_time_us'] <= open_us + 5 for q in snapshot)
    assert toyota.quote()['frame'] == 3
    assert toyota.quote(at='08:59:59.9999')['frame'] == 1  # final cache does not affect past queries


@pytest.mark.parametrize('form', ['jsonl', 'gzip', 'parquet'])
def test_summary_covers_empty_batches_and_caches_the_range(tmp_path, monkeypatch, form):
    batches = market()
    batches[-1]['source_time_us'] += 100
    data = ''.join(json.dumps(b) + '\n' for b in batches).encode()
    if form == 'parquet':
        rec = save(batches, tmp_path / 'rec', form)
    else:
        path = tmp_path / ('events.jsonl.gz' if form == 'gzip' else 'events.jsonl')
        path.write_bytes(gzip.compress(data) if form == 'gzip' else data)
        rec = brisk.Recording(path)
    summary = brisk.Market(rec).summary()
    assert summary['start'].microsecond == 999955 and summary['end'].microsecond == 130
    monkeypatch.setattr(rec, 'batches', lambda: pytest.fail('clock range should be cached'))
    assert brisk.Market(rec).summary() == summary


def test_trades_auctions_and_special_quotes(recording):
    toyota = brisk.Ticker('7203')
    trades = toyota.trades()
    assert [(t['price'], t['quantity'], t['side']) for t in trades] == [(10150.0, 304700, None), (10160.0, 2000, 'buy'),
                                                                       (10160.0, 3300, 'buy')]
    assert trades[0]['time'] == dt.datetime(2021, 9, 27, 9, 0, 0, 5, tzinfo=brisk.JST) and trades[1]['frame'] == 3
    assert len(toyota.trades(start='09:00:00.000010')) == 2 and toyota.trades(raw=True)[0]['price10'] == 101500
    assert brisk.Ticker('6758').trades() == [] and len(brisk.Market().trades(end='09:00:00.000006')) == 1
    pre_open = toyota.quote(at='08:59:59.99996')
    assert pre_open['closing_bids'] == [{'price': 10150.0, 'quantity': 500, 'orders': 1}] and pre_open['closing_asks'] == []
    assert pre_open['special_quote'] == {'side': 'sell', 'price': 10150.0, 'until': pre_open['special_quote_time']}
    assert toyota.quote()['special_quote'] is None
    closing = toyota.auction(at='08:59:59.99996', closing=True)
    assert closing['bid_closing_total_quantity'] == 500 and 'closing_indicative_price' in closing and 'indicative_open_price' not in closing
    assert toyota.auction(at='08:59:59.99996')['bid_total_quantity'] == 3100
    assert brisk.Market().imbalances(at='08:59:59.99996', closing=True)[0]['market_order_imbalance'] == 0
    special = brisk.Market().special_quotes(at='08:59:59.99996')
    assert [(q['code'], q['side'], q['price']) for q in special] == [('7203', 'sell', 10150.0)]
    assert brisk.Market().special_quotes() == []
    frames = brisk.Market().frames()
    assert [(f['type'], f['quote_count'], f['trade_count']) for f in frames] == [
        ('bootstrap', 2, 0), ('quotes', 1, 1), ('quotes', 1, 0), ('quotes', 1, 2), ('end', 0, 0)]
    assert frames[1]['received_unix_ms'] == 1632700800000 and frames[0]['time'].hour == 8


@pytest.mark.parametrize('at,expected', [
    (None, None), (5, 5), ('9:00', 32_400_000_000), ('09:00:01.5', 32_401_500_000),
    (dt.time(9, 0, 0, 7), 32_400_000_007),
    (dt.datetime(2021, 9, 27, 9, 0), 32_400_000_000),
    (dt.datetime(2021, 9, 27, 0, 0, tzinfo=dt.timezone.utc), 32_400_000_000),
])
def test_time_arguments(at, expected):
    assert _recording.micros('20210927', at) == expected


@pytest.mark.parametrize('at,error', [('nine', ValueError), (dt.datetime(2021, 9, 28, 9), ValueError), (1.5, TypeError), (-1, TypeError)])
def test_bad_time_arguments(at, error):
    with pytest.raises(error):
        _recording.micros('20210927', at)


def test_default_recording_and_consent(recording):
    assert brisk.current() is recording
    brisk._current = None
    with pytest.raises(brisk.BriskError, match='Nothing loaded'):
        brisk.Ticker('7203').info()
    assert brisk.consent() == {'policy_version': cli.POLICY_VERSION, 'enabled': True, 'default': True}
    assert brisk.consent(accept=True)['enabled'] is True
    assert brisk.consent(revoke=True)['enabled'] is False
    with pytest.raises(ValueError):
        brisk.consent(accept=True, revoke=True)


def test_validate_synthetic(tmp_path):
    path = tmp_path / 'events.jsonl'
    path.write_bytes(b''.join(map(schema.encode, schema.synthetic_recording())))
    assert brisk.Recording(path).validate()['source'] == 'synthetic_test'
    assert save(schema.synthetic_recording(), tmp_path / 'pq', 'parquet').validate()['source'] == 'synthetic_test'
    assert brisk.Table([{'a': 1}]) == [{'a': 1}]


def test_parquet_recording_reads_back_the_decoder_batches(tmp_path):
    rec = save(market(), tmp_path / 'rec', 'parquet')
    assert list(rec.batches()) == [{**b, 'quotes': sorted(b['quotes'], key=lambda q: q['issue_id'])} if 'quotes' in b else b
                                   for b in market()]
    assert rec.lines().__next__() == schema.encode(next(rec.batches()))
    with pytest.raises(brisk.BriskError, match='Parquet recording'):
        rec.open()
    toyota = brisk.Ticker('7203', rec).quote(at='08:59:59.99996')
    assert toyota['bids'] == [{'price': 10150.0, 'quantity': 3000, 'orders': 4}, {'price': 10140.0, 'quantity': 100, 'orders': 1}]
    assert brisk.Ticker('7203', rec).quote()['turnover'] == 3146553000.0 and brisk.Ticker('7203', rec).history()[0]['frame'] == 1
    assert brisk.Ticker('7203', brisk.Recording(rec.directory)).history(raw=True)[1]['turnover10'] == 30927050000


def test_table_to_pandas():
    pd = pytest.importorskip('pandas')
    assert isinstance(brisk.Table([{'a': 1}]).to_pandas(), pd.DataFrame)


def test_archive(tmp_path, monkeypatch):
    owner = brisk.Archive().owner
    m = {'summary': {'source': 'synthetic_test', 'trading_date': '20210927', 'codes': ['0000'], 'batches': 3,
                     'quote_updates': 2}, 'bytes': 10, 'sha256': 'f' * 64}
    monkeypatch.setattr(cli, 'manifests', lambda url, date: [(f'archive/{owner}/20210927/085959-090300-' + 'f' * 64, m),
                                                             ('archive/x', {**m, 'summary': {**m['summary'], 'source': 'historical_mock'}})])
    pulls = []

    def pull(url, prefix, output):
        pulls.append(output)
        _parquet.write(schema.synthetic_recording(), output)

    monkeypatch.setattr(cli, 'pull', pull)
    archive = brisk.Archive()
    assert archive.config['region'] == 'ap-northeast-1' and len(archive.owner) == 32
    rows = brisk.recordings(source='synthetic_test')
    assert len(rows) == 1 and rows[0]['securities'] == 1 and len(archive.recordings()) == 2
    prefix = rows[0]['prefix']
    rec = brisk.pull(prefix)
    assert brisk.current() is rec and rec.codes == ['0000']
    assert pulls == [_archive.cache_dir() / prefix]
    archive.pull(prefix)
    assert len(pulls) == 1  # verified cache entry reused
    archive.pull(prefix, tmp_path / 'explicit')
    assert pulls[-1] == tmp_path / 'explicit'
    with pytest.raises(ValueError, match='prefix'):
        archive.pull('../../etc')
    monkeypatch.setattr(cli, 'upload', lambda directory, url, timeout: {'status': 'stored', 'url': url})
    assert archive.upload(tmp_path)['url'] == archive.config['api_url']
    monkeypatch.setattr(cli, 'delete', lambda url, prefix: {'status': 'deleted', 'prefix': prefix})
    assert archive.delete(prefix)['prefix'] == prefix
    monkeypatch.setattr(cli, 'timing_reports', lambda url, date: [('timing/x', {'frames': 1})])
    assert archive.timing() == [{'key': 'timing/x', 'frames': 1}]


@pytest.fixture
def fake_decoder(tmp_path, monkeypatch):
    script = tmp_path / 'fake.cjs'
    lines = ''.join(f'console.log({json.dumps(json.dumps(b))});\n' for b in market())
    script.write_text(f'console.error(JSON.stringify(process.argv.slice(2)));\n{lines}'
                      'process.exitCode = process.argv.includes("--limit-frames") ? 3 : 0;\n')
    monkeypatch.setattr(_live, 'DECODER', script)


def test_stream(fake_decoder, tmp_path, capfd):
    batches = list(brisk.stream(cache=tmp_path, codes=['7203', 6758], speed=0))
    assert [b['type'] for b in batches] == ['bootstrap', 'quotes', 'quotes', 'quotes', 'end']
    assert '"--codes","7203,6758"' in capfd.readouterr().err
    first = next(brisk.stream(codes='7203'))  # closing early stops the decoder
    assert first['type'] == 'bootstrap'
    with pytest.raises(brisk.BriskError, match='exited with 3'):
        list(brisk.stream(limit_frames=2))


@pytest.fixture
def recorder(monkeypatch):
    calls = []

    def record_events(events, cache, codes, limit_frames, speed, binary):
        calls.append(codes)
        Path(events).write_bytes(b''.join(map(schema.encode, schema.synthetic_recording())))

    uploads = []
    monkeypatch.setattr(cli, 'record_events', record_events)
    monkeypatch.setattr(cli, 'upload', lambda directory, url, verify: uploads.append(verify) or {'status': 'stored'})
    return calls, uploads


def test_record_uploads_by_default(recorder, tmp_path, monkeypatch):
    calls, uploads = recorder
    # Nothing decided: the recording uploads, and the process is told once.
    with pytest.warns(UserWarning, match='by default') as told:
        rec = brisk.record(tmp_path / 'first', codes=['7203'])
    assert 'recovery code' in str(told[0].message) and 'upload=False' in str(told[0].message)
    assert calls == ['7203'] and rec.format == 'parquet' and brisk.current() is rec
    assert rec.uploaded == {'status': 'stored'} and uploads == [False]
    assert set(rec.manifest) == {'schema', 'sha256', 'bytes', 'summary'}
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        assert brisk.record(tmp_path / 'second').uploaded == {'status': 'stored'} and len(uploads) == 2
    # The opt-outs package as usual but keep it local.
    rec = brisk.record(tmp_path / 'kept', upload=False)
    assert rec.uploaded is None and rec.manifest and len(uploads) == 2
    monkeypatch.setenv('BRISK_UPLOAD', '0')
    assert brisk.record(tmp_path / 'env').uploaded is None
    monkeypatch.delenv('BRISK_UPLOAD')
    assert brisk.record(tmp_path / 'partial', limit_frames=5).manifest is None
    with pytest.raises(brisk.BriskError, match='complete'):
        brisk.record(tmp_path / 'p2', limit_frames=5, upload=True)
    # Revoked: local only, and upload=True is refused.
    brisk.consent(revoke=True)
    rec = brisk.record(tmp_path / 'local')
    assert rec.uploaded is None and rec.manifest is None and len(uploads) == 2
    with pytest.raises(brisk.BriskError, match='already exists'):
        brisk.record(tmp_path / 'local', upload=False)
    with pytest.raises(brisk.BriskError, match='uploads on'):
        brisk.record(tmp_path / 'x', upload=True)


def test_module_entry_point(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['brisk', 'consent'])
    sys.modules.pop('briskapi.__main__', None)
    import briskapi.__main__  # noqa: F401
    assert json.loads(capsys.readouterr().out) == {'policy_version': cli.POLICY_VERSION, 'enabled': True, 'default': True}


@pytest.fixture
def gated_decoder(tmp_path, monkeypatch):
    """Fake decoder: prints the bootstrap, then the rest once the test opens the gate."""
    def make(batches=None, exit_code=0, hold=False, cut=''):
        lines = [json.dumps(b) for b in (market() if batches is None else batches)]
        script = tmp_path / f'gated{len(list(tmp_path.glob("gated*")))}.cjs'
        gate = tmp_path / 'go'
        script.write_text(
            f"const fs = require('fs'); const lines = {json.dumps(lines)};\n"
            f"if (!{json.dumps(hold)}) console.log(lines[0]);\n"
            f"const go = () => fs.existsSync({json.dumps(str(gate))}) ? (lines.slice(1).forEach(l => console.log(l)), "
            f"process.stdout.write({json.dumps(cut)}), process.exitCode = {exit_code}) : setTimeout(go, 5);\n"
            f"if (!{json.dumps(hold)}) go(); else setTimeout(() => {{}}, 60000);\n")
        monkeypatch.setattr(_live, 'DECODER', script)
        return gate
    return make


def test_live_feed(gated_decoder, tmp_path):
    gate = gated_decoder()
    with brisk.connect(site='demo', cache=tmp_path, codes=['7203', '6758'], upload=False, history=True) as feed:
        assert feed.status == 'running' and brisk.current() is feed and 'running' in repr(feed)
        assert feed.codes == ['6758', '7203'] and feed.source == 'historical_mock' and feed.trading_date == '20210927'
        assert brisk.Market().summary()['start'] == brisk.Market().summary()['end']
        assert brisk.Ticker('7203').quote()['indicative_price'] == 10150.0
        assert brisk.Market().imbalances(top=1)[0]['code'] == '7203'
        sony = []
        feed.on_quote(lambda q: sony.append(q['frame']), codes='6758')
        raw_all = []
        stop = feed.on_quote(raw_all.append, raw=True)
        toyota = feed.quotes('7203')
        assert next(toyota)['frame'] == 1  # current state first
        stop()
        gate.touch()
        assert [q['frame'] for q in toyota] == [2, 3]
        feed.wait()
        assert feed.status == 'completed' and feed.seq == 4 and feed.uploaded is None
        assert brisk.Market().summary()['end'].microsecond == 30
        assert sony == [1, 2] and len(raw_all) == 2 and 'last_price10' in raw_all[0]
        assert [q['frame'] for q in feed.updates('7203')] == [1, 2, 3]
        assert [q['frame'] for q in brisk.Ticker('7203').history(start='09:00:00.000011')] == [3]
        assert [q['last_price'] for q in feed.snapshot()] == [12050.0, 10160.0]
        assert feed.snapshot(raw=True)[1]['last_price10'] == 101600
        assert [q['frame'] for q in feed.quotes('6758')] == [2]  # finished feed: state, then end
        assert [t['quantity'] for t in feed.trades('7203')] == [304700, 2000, 3300] and len(feed.trades()) == 3
        assert brisk.Ticker('7203').trades(start='09:00:00.00002')[0]['side'] == 'buy'
        with pytest.raises(brisk.BriskError, match='no frames'):
            brisk.Market().frames()
        late = []
        feed.on_quote(late.append)
        assert len(late) == 2
        with pytest.raises(ValueError, match='current state'):
            feed.state(at='09:00')
        with pytest.raises(brisk.NotFoundError):
            feed.issue('9999')


def test_feed_failures(gated_decoder, tmp_path, monkeypatch):
    gate = gated_decoder(exit_code=3)
    feed = brisk.connect(site='demo', cache=tmp_path, upload=False)
    updates = feed.quotes()
    with pytest.raises(TimeoutError):
        list(feed.quotes(timeout=0.05))[2:]
    with pytest.raises(brisk.BriskError, match='history'):
        list(feed.updates('7203'))
    with pytest.raises(brisk.BriskError, match='history'):
        feed.trades()
    gate.touch()
    with pytest.raises(brisk.BriskError, match='exited with 3'):
        list(updates)
    with pytest.raises(brisk.BriskError, match='exited with 3'):
        feed.wait()
    gate.unlink()

    gated_decoder(market()[:-1])
    feed = brisk.connect(site='demo', cache=tmp_path, upload=False)
    gate.touch()
    with pytest.raises(brisk.BriskError, match='before replay completion'):
        feed.wait()
    gate.unlink()

    gil_enabled = _live.sys._is_gil_enabled
    for gil in (False, True):   # a line without its end: the decoder exited before writing all of it
        monkeypatch.setattr(_live.sys, '_is_gil_enabled', lambda: gil)
        gated_decoder(market()[:-1], cut=json.dumps(market()[-1])[:40])
        feed = brisk.connect(site='demo', cache=tmp_path, upload=False)
        gate.touch()
        with pytest.raises(brisk.BriskError, match='stopped in the middle of a line'):
            feed.wait()
        gate.unlink()
    monkeypatch.setattr(_live.sys, '_is_gil_enabled', gil_enabled)

    broken = market(); broken[2]['seq'] = 7
    gated_decoder(broken)
    feed = brisk.connect(site='demo', cache=tmp_path, upload=False)
    gate.touch()
    with pytest.raises(brisk.BriskError, match='sequence'):
        feed.wait()
    gate.unlink()

    gated_decoder()
    feed = brisk.connect(site='demo', cache=tmp_path, upload=False)
    feed.on_quote(lambda q: 1 / 0 if q['frame'] > 1 else None)
    gate.touch()
    with pytest.raises(brisk.BriskError, match='division'):
        feed.wait()
    gate.unlink()

    gated_decoder()
    feed = brisk.connect(site='demo', cache=tmp_path, upload=False)
    feed.close()
    assert feed.status == 'closed' and feed.codes  # state read before closing stays readable

    gated_decoder(hold=True)
    with pytest.raises(TimeoutError):
        brisk.connect(site='demo', cache=tmp_path, upload=False, timeout=0.2)
    feed = brisk.Feed(cache=tmp_path, upload=False)
    with pytest.raises(TimeoutError, match='still running'):
        feed.wait(timeout=0.05)
    feed.close()
    with pytest.raises(brisk.BriskError, match='closed before bootstrap'):
        feed.ready()


def test_feed_uploads_complete_sessions(gated_decoder, tmp_path, monkeypatch):
    gate = gated_decoder()
    gate.touch()
    packaged = []

    def package_parts(equity, parts, output, repair=False, piece=None, progress=None):
        packaged.append([b['seq'] for b in reference_package.load_parts(Path(equity), parts).iter_batches()])

    monkeypatch.setattr(cli, 'package_parts', package_parts)
    monkeypatch.setattr(cli, 'upload', lambda directory, url, verify, out, progress: {'status': 'stored', 'verify': verify})
    with pytest.warns(UserWarning, match='by default'):
        feed = brisk.connect(site='demo', cache=tmp_path).wait()
    assert feed.uploaded == {'status': 'stored', 'verify': False}
    assert packaged == [[b['seq'] for b in market()]]
    assert brisk.connect(site='demo', cache=tmp_path, limit_frames=2).wait().uploaded is None
    assert brisk.connect(site='demo', cache=tmp_path, upload=False).wait().uploaded is None and len(packaged) == 1
    brisk.consent(revoke=True)
    with pytest.raises(brisk.BriskError, match='uploads on'):
        brisk.Feed(cache=tmp_path, upload=True)
    assert brisk.connect(site='demo', cache=tmp_path).wait().uploaded is None and len(packaged) == 1
    brisk.consent(accept=True)
    monkeypatch.setattr(cli, 'package_parts', lambda *a, **k: 1 / 0)
    with pytest.warns(UserWarning, match='Packaging failed'):
        feed = brisk.connect(site='demo', cache=tmp_path).wait()
    assert feed.uploaded['status'] == 'failed' and feed.recordings[-1]['error'].startswith('Packaging failed')


def test_cli_live(gated_decoder, tmp_path, monkeypatch, capsys):
    gated_decoder().touch()
    monkeypatch.setattr(cli, 'package_parts', lambda *a, **k: None)
    monkeypatch.setattr(cli, 'upload', lambda *a, **k: {'status': 'stored'})
    cli.main(['live', '--site', 'demo', '--cache', str(tmp_path), '--codes', '7203', '--quotes'])
    out = capsys.readouterr()
    lines = [json.loads(line) for line in out.out.splitlines()]
    # The ungated fake races ahead of the snapshot; per-code order and final state still hold.
    frames = {}
    for q in lines:
        assert q['frame'] >= frames.get(q['code'], 0) and q['time'].startswith('2021-09-27T')
        frames[q['code']] = q['frame']
    assert frames == {'7203': 3, '6758': 2}
    # Without a terminal to ask on, the session uploads by default and says so.
    assert 'on by default' in out.err and '"uploaded": {"status": "stored"}' in out.err
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO('\n'))
    cli.main(['live', '--site', 'demo', '--cache', str(tmp_path), '--quotes', '--raw'])
    out = capsys.readouterr()
    assert 'private archive' in out.err and '"uploaded": {"status": "stored"}' in out.err
    assert all('last_price10' in json.loads(line) for line in out.out.splitlines())


def deep_session(frames=30, start=0, offset_us=0):
    """A broker session whose quotes list their ten best levels while their book changes hold the whole books: fifteen
    bids, and fifteen or twelve asks; the books of each update ({(issue, frame): four books}) with it."""
    from test_spool import session
    batches, before, books = session(frames=frames), {}, {}
    for b in batches:
        b['source_time_us'] += offset_us
        for q in b.get('quotes', ()):
            q['source_time_us'] += offset_us
            k = q['frame'] + start
            bids = [[9990 - 10 * n - k % 3, 100 * (n + 1) + k, 1 + n % 2] for n in range(15)]
            asks = [[10010 + 10 * n + k % 2, 100 + n + k, 1] for n in range(15 if k % 4 else 12)]
            whole = [bids, asks, [], []]
            q['bid_levels'], q['ask_levels'] = bids[:10], asks[:10]
            q[schema.BOOK_CHANGES] = schema.book_changes(before.get(q['issue_id']), whole)
            before[q['issue_id']], books[(q['issue_id'], q['frame'])] = whole, whole
    return batches, books


def test_a_tickers_book_is_every_level_its_book_changes_replay_to(tmp_path):
    batches, books = deep_session()
    (tmp_path / 'deep.jsonl').write_bytes(b''.join(schema.encode(b) for b in batches))
    cli.package(tmp_path / 'deep.jsonl', tmp_path / 'deep')
    recording = brisk.Recording(tmp_path / 'deep')
    toyota = brisk.Ticker('7203', recording)
    quotes = [q for b in batches for q in b.get('quotes', ()) if q['code'] == '7203']
    for q in quotes[::7] + quotes[-1:]:
        at = _recording.timestamp(recording.trading_date, q['source_time_us']).time()
        whole = books[(0, q['frame'])]
        raw = toyota.book(at=at, raw=True)
        assert raw['frame'] == q['frame'] and raw['bid_levels'] == whole[0] and raw['ask_levels'] == whole[1]
        assert len(raw['bid_levels']) == 15 > len(toyota.quote(at=at)['bids']) == 10
    view = toyota.book()
    assert view['asks'][0] == {'price': whole[1][0][0] / 10, 'quantity': whole[1][0][1], 'orders': 1}
    assert view['closing_bids'] == [] and view['time'] == toyota.quote()['time']
    assert schema.BOOK_CHANGES not in toyota.quote() and schema.BOOK_CHANGES in toyota.quote(raw=True)
    plain = tmp_path / 'plain.jsonl'
    plain.write_bytes(b''.join(schema.encode(b) for b in session_without_changes()))
    cli.package(plain, tmp_path / 'plain')
    with pytest.raises(brisk.BriskError, match='no book changes'):
        brisk.Ticker('7203', brisk.Recording(tmp_path / 'plain')).book()


def session_without_changes():
    from test_spool import session
    return session(frames=12)
