import datetime as dt
import gzip
import io
import json
from pathlib import Path
import sys

import pytest

import briskapi as brisk
import briskapi.cli as cli
import briskapi.schema as schema
from briskapi import _archive, _live, _recording

TOYOTA = {'issue_id': 1, 'code': '7203', 'tick_type': 3, 'base_price10': 101000, 'limit_up10': 131000,
          'limit_down10': 71000, 'lot_size': 100, 'issue_type': 111, 'name': 'Toyota'}


def quote(issue, code, frame, at, **fields):
    return {**dict.fromkeys(schema.QUOTE_KEYS, 0), 'issue_id': issue, 'code': code, 'frame': frame,
            'max_frame': frame, 'source_time_us': at, **fields}


def market():
    """Two securities around the 09:00 open; source clock is microseconds since JST midnight."""
    open_us = 9 * 3600 * 1_000_000
    sony = {**TOYOTA, 'issue_id': 0, 'code': '6758', 'name': 'Sony', 'base_price10': 120000}
    return [
        dict(type='bootstrap', seq=0, source='historical_mock', trading_date='20210927', source_time_us=open_us - 45,
             master=[sony, TOYOTA], market_issue_count=2, quotes=[
                 quote(0, '6758', 1, open_us - 50, indicative_price10=120500, market_buy_quantity=500, market_sell_quantity=900),
                 quote(1, '7203', 1, open_us - 45, indicative_price10=101500, indicative_volume=304700,
                       market_buy_quantity=201300, market_sell_quantity=105700, special_quote_time_us=open_us - 1)]),
        dict(type='quotes', seq=1, source_time_us=open_us + 10, quotes=[
            quote(1, '7203', 2, open_us + 10, last_price10=101500, volume=304700)]),
        dict(type='quotes', seq=2, source_time_us=open_us + 20, quotes=[
            quote(0, '6758', 2, open_us + 20, last_price10=120500)]),
        dict(type='quotes', seq=3, source_time_us=open_us + 30, quotes=[
            quote(1, '7203', 3, open_us + 30, last_price10=101600, volume=310000)]),
        dict(type='end', seq=4, source_time_us=open_us + 30, frames=4, quote_updates=3),
    ]


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path / 'cache'))
    monkeypatch.delenv('BRISK_CONTRIBUTE', raising=False)
    monkeypatch.setattr(brisk, '_current', None)


@pytest.fixture
def recording(tmp_path):
    path = tmp_path / 'rec' / 'events.jsonl'
    path.parent.mkdir()
    path.write_text(''.join(json.dumps(b) + '\n' for b in market()))
    return brisk.load(path.parent)


def test_recording_basics(recording, tmp_path):
    assert recording.codes == ['6758', '7203'] and recording.trading_date == '20210927'
    assert recording.source == 'historical_mock' and recording.manifest is None
    assert len(list(recording.batches())) == 5 and 'events.jsonl' in repr(recording)
    gz = tmp_path / 'gz' / 'events.jsonl.gz'
    gz.parent.mkdir()
    gz.write_bytes(gzip.compress(recording.path.read_bytes()))
    assert brisk.Recording(gz.parent).codes == recording.codes
    (tmp_path / 'empty').mkdir()
    with pytest.raises(brisk.NotFoundError, match='No events'):
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
        {'summary': {'last_source_time_us': 32_400_000_030, 'batches': 5}, 'contributor': 'alice', 'license': 'CC0-1.0'}))
    summary = brisk.Market(brisk.Recording(recording.directory)).summary()
    assert summary['contributor'] == 'alice' and summary['end'].microsecond == 30


def test_state_uses_quote_clocks_even_in_later_batches(recording):
    batches = market()
    open_us = 32_400_000_000
    batches[0]['quotes'][0]['source_time_us'] = open_us - 7655
    batches[0]['quotes'][1]['source_time_us'] = open_us - 6449
    batches[1]['quotes'][0]['source_time_us'] = open_us + 5
    recording.path.write_text(''.join(json.dumps(b) + '\n' for b in batches))
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


@pytest.mark.parametrize('compressed', [False, True])
def test_summary_covers_empty_batches_and_caches_the_range(tmp_path, monkeypatch, compressed):
    batches = market()
    batches[-1]['source_time_us'] += 100
    data = ''.join(json.dumps(b) + '\n' for b in batches).encode()
    path = tmp_path / ('events.jsonl.gz' if compressed else 'events.jsonl')
    path.write_bytes(gzip.compress(data) if compressed else data)
    rec = brisk.Recording(path)
    summary = brisk.Market(rec).summary()
    assert summary['start'].microsecond == 999955 and summary['end'].microsecond == 130
    monkeypatch.setattr(rec, 'batches', lambda: pytest.fail('clock range should be cached'))
    assert brisk.Market(rec).summary() == summary


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
    assert brisk.consent() is None
    assert brisk.consent(accept=True, contributor='bob')['contributor'] == 'bob'
    assert brisk.consent(revoke=True)['enabled'] is False
    with pytest.raises(ValueError):
        brisk.consent(accept=True, revoke=True)


def test_validate_synthetic(tmp_path):
    path = tmp_path / 'events.jsonl'
    path.write_bytes(b''.join(map(schema.encode, schema.synthetic_recording())))
    assert brisk.Recording(path).validate()['source'] == 'synthetic_test'
    assert brisk.Table([{'a': 1}]) == [{'a': 1}]


def test_table_to_pandas():
    pd = pytest.importorskip('pandas')
    assert isinstance(brisk.Table([{'a': 1}]).to_pandas(), pd.DataFrame)


class S3:
    pass


def test_archive(tmp_path, monkeypatch):
    m = {'summary': {'source': 'synthetic_test', 'trading_date': '20210927', 'codes': ['0000'], 'batches': 3,
                     'quote_updates': 2}, 'bytes': 10, 'contributor': 'c', 'license': 'CC0-1.0', 'sha256': 'f' * 64}
    monkeypatch.setattr(cli, 'manifests', lambda s3, bucket, date: [('archive/20210927/' + 'f' * 64, m),
                                                                    ('archive/x', {**m, 'summary': {**m['summary'], 'source': 'historical_mock'}})])
    pulls = []

    def pull(s3, bucket, prefix, output):
        pulls.append(output)
        output.mkdir(parents=True)
        (output / 'events.jsonl').write_bytes(b''.join(map(schema.encode, schema.synthetic_recording())))

    monkeypatch.setattr(cli, 'pull', pull)
    monkeypatch.setattr(cli, 'client', lambda config: S3())
    archive = brisk.Archive()
    assert isinstance(archive.s3, S3) and archive.config['region'] == 'ap-northeast-1'
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
    monkeypatch.setattr(cli, 'contribute', lambda directory, url, timeout: {'status': 'published', 'url': url})
    assert archive.contribute(tmp_path)['url'] == archive.config['api_url']


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
    first = next(brisk.stream(web=True, codes='7203'))  # closing early stops the decoder
    assert first['type'] == 'bootstrap'
    with pytest.raises(brisk.BriskError, match='exited with 3'):
        list(brisk.stream(web=True, limit_frames=2))
    with pytest.raises(ValueError, match='exactly one'):
        next(brisk.stream())


@pytest.fixture
def recorder(monkeypatch):
    calls = []

    def record_events(events, web, cache, codes, limit_frames, speed, binary):
        calls.append(codes)
        Path(events).write_bytes(b''.join(map(schema.encode, schema.synthetic_recording())))

    uploads = []
    monkeypatch.setattr(cli, 'record_events', record_events)
    monkeypatch.setattr(cli, 'contribute', lambda directory, url, verify: uploads.append(verify) or {'status': 'published'})
    return calls, uploads


def test_record_follows_consent(recorder, tmp_path, monkeypatch):
    calls, uploads = recorder
    with pytest.warns(UserWarning, match='undecided'):
        rec = brisk.record(tmp_path / 'local', web=True, codes=['7203'])
    assert calls == ['7203'] and rec.path.name == 'events.jsonl' and brisk.current() is rec and not uploads
    with pytest.raises(brisk.BriskError, match='already exists'):
        brisk.record(tmp_path / 'local', web=True, contribute=False)
    with pytest.raises(brisk.BriskError, match='consent'):
        brisk.record(tmp_path / 'x', web=True, contribute=True)
    brisk.consent(accept=True, contributor='carol')
    rec = brisk.record(tmp_path / 'shared', web=True)
    assert rec.contribution == {'status': 'published'} and uploads == [False]
    assert rec.manifest['contributor'] == 'carol' and rec.path.name == 'events.jsonl.gz'
    rec = brisk.record(tmp_path / 'kept', web=True, contribute=False)
    assert rec.contribution is None and rec.manifest and len(uploads) == 1
    monkeypatch.setenv('BRISK_CONTRIBUTE', '0')
    assert brisk.record(tmp_path / 'env', web=True).contribution is None
    monkeypatch.delenv('BRISK_CONTRIBUTE')
    assert brisk.record(tmp_path / 'partial', web=True, limit_frames=5).manifest is None
    with pytest.raises(brisk.BriskError, match='complete'):
        brisk.record(tmp_path / 'p2', web=True, limit_frames=5, contribute=True)


def test_module_entry_point(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['brisk', 'consent'])
    sys.modules.pop('briskapi.__main__', None)
    import briskapi.__main__  # noqa: F401
    assert json.loads(capsys.readouterr().out)['enabled'] is None


@pytest.fixture
def gated_decoder(tmp_path, monkeypatch):
    """Fake decoder: prints the bootstrap, then the rest once the test opens the gate."""
    def make(batches=None, exit_code=0, hold=False):
        lines = [json.dumps(b) for b in (market() if batches is None else batches)]
        script = tmp_path / f'gated{len(list(tmp_path.glob("gated*")))}.cjs'
        gate = tmp_path / 'go'
        script.write_text(
            f"const fs = require('fs'); const lines = {json.dumps(lines)};\n"
            f"if (!{json.dumps(hold)}) console.log(lines[0]);\n"
            f"const go = () => fs.existsSync({json.dumps(str(gate))}) ? (lines.slice(1).forEach(l => console.log(l)), "
            f"process.exitCode = {exit_code}) : setTimeout(go, 5);\n"
            f"if (!{json.dumps(hold)}) go(); else setTimeout(() => {{}}, 60000);\n")
        monkeypatch.setattr(_live, 'DECODER', script)
        return gate
    return make


def test_live_feed(gated_decoder, tmp_path):
    gate = gated_decoder()
    with brisk.connect(cache=tmp_path, codes=['7203', '6758'], contribute=False, history=True) as feed:
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
        assert feed.status == 'completed' and feed.seq == 4 and feed.contribution is None
        assert brisk.Market().summary()['end'].microsecond == 30
        assert sony == [1, 2] and len(raw_all) == 2 and 'last_price10' in raw_all[0]
        assert [q['frame'] for q in feed.updates('7203')] == [1, 2, 3]
        assert [q['frame'] for q in brisk.Ticker('7203').history(start='09:00:00.000011')] == [3]
        assert [q['last_price'] for q in feed.snapshot()] == [12050.0, 10160.0]
        assert feed.snapshot(raw=True)[1]['last_price10'] == 101600
        assert [q['frame'] for q in feed.quotes('6758')] == [2]  # finished feed: state, then end
        late = []
        feed.on_quote(late.append)
        assert len(late) == 2
        with pytest.raises(ValueError, match='current state'):
            feed.state(at='09:00')
        with pytest.raises(brisk.NotFoundError):
            feed.issue('9999')


def test_feed_failures(gated_decoder, tmp_path):
    gate = gated_decoder(exit_code=3)
    feed = brisk.connect(cache=tmp_path, contribute=False)
    updates = feed.quotes()
    with pytest.raises(TimeoutError):
        list(feed.quotes(timeout=0.05))[2:]
    with pytest.raises(brisk.BriskError, match='history'):
        list(feed.updates('7203'))
    gate.touch()
    with pytest.raises(brisk.BriskError, match='exited with 3'):
        list(updates)
    with pytest.raises(brisk.BriskError, match='exited with 3'):
        feed.wait()
    gate.unlink()

    gated_decoder(market()[:-1])
    feed = brisk.connect(cache=tmp_path, contribute=False)
    gate.touch()
    with pytest.raises(brisk.BriskError, match='before replay completion'):
        feed.wait()
    gate.unlink()

    broken = market(); broken[2]['seq'] = 7
    gated_decoder(broken)
    feed = brisk.connect(cache=tmp_path, contribute=False)
    gate.touch()
    with pytest.raises(brisk.BriskError, match='sequence'):
        feed.wait()
    gate.unlink()

    gated_decoder()
    feed = brisk.connect(cache=tmp_path, contribute=False)
    feed.on_quote(lambda q: 1 / 0 if q['frame'] > 1 else None)
    gate.touch()
    with pytest.raises(brisk.BriskError, match='division'):
        feed.wait()
    gate.unlink()

    gated_decoder()
    feed = brisk.connect(cache=tmp_path, contribute=False)
    feed.close()
    assert feed.status == 'closed' and feed.codes  # state read before closing stays readable

    gated_decoder(hold=True)
    with pytest.raises(TimeoutError):
        brisk.connect(cache=tmp_path, contribute=False, timeout=0.2)
    feed = brisk.Feed(cache=tmp_path, contribute=False)
    with pytest.raises(TimeoutError, match='still running'):
        feed.wait(timeout=0.05)
    feed.close()
    with pytest.raises(brisk.BriskError, match='closed before bootstrap'):
        feed.ready()


def test_feed_contributes_complete_sessions(gated_decoder, tmp_path, monkeypatch):
    gate = gated_decoder()
    gate.touch()
    packaged = []

    def package(events, output, contributor, license):
        packaged.append((events.read_text().splitlines(), contributor))

    monkeypatch.setattr(cli, 'package', package)
    monkeypatch.setattr(cli, 'contribute', lambda directory, url, verify, out: {'status': 'published', 'verify': verify})
    with pytest.raises(brisk.BriskError, match='consent'):
        brisk.Feed(cache=tmp_path, contribute=True)
    with pytest.warns(UserWarning, match='undecided'):
        assert brisk.connect(cache=tmp_path).wait().contribution is None
    brisk.consent(accept=True, contributor='dana')
    feed = brisk.connect(cache=tmp_path).wait()
    assert feed.contribution == {'status': 'published', 'verify': False}
    assert packaged == [([json.dumps(b) for b in market()], 'dana')]
    assert brisk.connect(cache=tmp_path, limit_frames=2).wait().contribution is None
    monkeypatch.setattr(cli, 'package', lambda *a: 1 / 0)
    with pytest.warns(UserWarning, match='contribution failed'):
        assert brisk.connect(cache=tmp_path).wait().contribution['status'] == 'failed'


def test_cli_live(gated_decoder, tmp_path, monkeypatch, capsys):
    gated_decoder().touch()
    cli.main(['live', '--cache', str(tmp_path), '--codes', '7203'])
    out = capsys.readouterr()
    lines = [json.loads(line) for line in out.out.splitlines()]
    # The ungated fake races ahead of the snapshot; per-code order and final state still hold.
    frames = {}
    for q in lines:
        assert q['frame'] >= frames.get(q['code'], 0) and q['time'].startswith('2021-09-27T')
        frames[q['code']] = q['frame']
    assert frames == {'7203': 3, '6758': 2}
    assert 'undecided' in out.err
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO('\n'))
    monkeypatch.setattr(cli, 'package', lambda *a: None)
    monkeypatch.setattr(cli, 'contribute', lambda *a, **k: {'status': 'published'})
    cli.main(['live', '--cache', str(tmp_path), '--raw'])
    out = capsys.readouterr()
    assert 'public and permanent' in out.err and '"contribution": {"status": "published"}' in out.err
    assert all('last_price10' in json.loads(line) for line in out.out.splitlines())
