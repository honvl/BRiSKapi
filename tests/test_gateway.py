"""briskapi.Live and the feed's gateway: Databento-style records from the feed on this computer, the same as
the cloud API's, served on 127.0.0.1 to holders of the gateway's key only."""
import collections
import datetime as dt
import io
import json
import os
from pathlib import Path
import socket
import sys
import threading
import time

import pyarrow as pa
import pytest

import briskapi as brisk
import briskapi.cli as cli
from briskapi import _gateway, _live, records
from briskapi import schema as brisk_schema
from test_api import market
from test_query import OPEN_US, OWNER, archive, at, isolated, query, session, trades_at  # noqa: F401


@pytest.fixture(autouse=True)
def nothing_loaded(monkeypatch):
    monkeypatch.setattr(brisk, '_current', None)


@pytest.fixture
def decoder(tmp_path):
    """A fake decoder printing these batches, waiting before each line index in `holds` until release(index)."""
    made = []

    def make(batches, holds=()):
        script = tmp_path / f'decoder{len(made)}.cjs'
        made.append(script)
        script.write_text(
            f"const fs = require('fs'); const lines = {json.dumps([json.dumps(b) for b in batches])};\n"
            f"const holds = {json.dumps(sorted(holds))}; const gate = {json.dumps(str(tmp_path / 'gate-'))}; let at = 0;\n"
            "const go = () => { while (at < lines.length) {\n"
            "  if (holds.includes(at) && !fs.existsSync(gate + at)) return setTimeout(go, 5);\n"
            "  console.log(lines[at++]); } };\n"
            "go();\n")
        return script, lambda index: (tmp_path / f'gate-{index}').touch()
    return make


def wait_for(condition, timeout=20):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, 'timed out'
        time.sleep(0.01)


def broker_feed(script, **options):
    """A broker-style feed (dataset XTKS.BRISK) from a fake decoder, serving briskapi.Live."""
    return _live.Feed(command=['node', str(script)], upload=False, gateway=True, **options).ready()


def test_live_records_are_the_cloud_apis(archive, decoder):
    """Replayed from the day's start, then live: every schema's records, bars included, equal the cloud API's for
    that session."""
    s3, _, put = archive
    batches = session(0, 150)
    put(batches)
    script, release = decoder(batches, holds=[70])
    feed = broker_feed(script)
    wait_for(lambda: feed.seq == 69)
    whole, part = brisk.Live(), brisk.Live()
    for schema in records.SCHEMAS:
        whole.subscribe(dataset='XTKS.BRISK', schema=schema, start=0)
    part.subscribe(dataset='XTKS.BRISK', schema='trades', symbols=['167A'], start=at(25))
    got = collections.defaultdict(list)
    whole.add_callback(lambda r: got[r.schema].append(dict(r)))
    whole.start()
    later = []
    reader = threading.Thread(target=lambda: later.extend(part))
    reader.start()
    # Both subscriptions answered before the feed goes on: one that came after the day's end would get nothing.
    wait_for(lambda: len(feed.gateway._clients) == 2 and all(c.known is not None for c in feed.gateway._clients))
    release(70)
    whole.block_for_close(timeout=30)
    reader.join(30)
    feed.wait()
    for schema in records.SCHEMAS:
        expected = query(s3, OWNER, dataset='XTKS.BRISK', schema=schema, start='2026-10-13').to_pylist()
        assert (expected or schema in {'ohlcv-1h', 'ohlcv-1d'}) and got[schema] == expected, schema
    assert [bar['ts_event'].minute for bar in got['ohlcv-1m']] == [0, 0, 1, 1]   # 09:02 is not covered
    expected = query(s3, OWNER, dataset='XTKS.BRISK', schema='trades', symbols='167A', start=at(25)).to_pylist()
    assert expected and [dict(r) for r in later] == expected
    assert later[0].schema == 'trades' and later[0].price == later[0]['price'] and later[0].symbol == '167A'
    assert not whole.is_connected() and 'closed' in repr(whole) and list(whole) == []
    assert whole.symbology_map == {7203: '7203', 154694: '167A'}


def test_a_session_joining_later_starts_from_now(decoder):
    """Without start: the day's definitions, the current quotes if asked (as a snapshot), then what arrives."""
    batches = market()
    script, release = decoder(batches, holds=[2])
    feed = _live.Feed(command=['node', str(script)], upload=False, gateway=True).ready()
    wait_for(lambda: feed.seq == 1)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'definition', symbols='7203')
    live.subscribe('XTKS.BRISK', 'mbp-10', symbols=['7203'], snapshot=True)
    live.subscribe('XTKS.BRISK', 'trades')
    live.start()
    release(2)
    found = list(live)
    feed.wait()
    assert [r.schema for r in found] == ['definition', 'mbp-10', 'trades', 'trades', 'mbp-10']
    definition, snapshot, *trades, update = found
    assert definition.raw_symbol == '7203' and definition.high_limit_price == 13100.0
    # The snapshot is 7203's quote as it stood (frame 2), flagged as one; then only what came after joining.
    assert (snapshot.flags, snapshot.sequence, snapshot.bid_px_00) == (32, 2, None)
    assert [t.size for t in trades] == [2000, 3300] and [t.trade_id for t in trades] == ['3-0', '3-1']
    assert (update.flags, update.sequence) == (0, 3)
    # Broker records carry the local receipt time.
    assert update.ts_recv == dt.datetime.fromtimestamp(1632700800, dt.timezone.utc) != update.ts_event


def test_subscriptions_join_a_running_session(decoder):
    """Subscribing during a session: new symbols, a replay of the day for that subscription alone, and bars that
    begin once it has joined."""
    batches = session(0, 200)
    script, release = decoder(batches, holds=[1, 100])
    feed = broker_feed(script)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'trades', symbols='7203')
    got = collections.defaultdict(list)
    live.add_callback(lambda r: got[r.schema, r.symbol].append(r))
    live.start()
    wait_for(lambda: live._subscriptions[0].active)
    release(1)
    wait_for(lambda: len(got['trades', '7203']) == 9)
    live.subscribe('XTKS.BRISK', 'trades', symbols='167A', start=0)       # the day so far, for 167A alone
    live.subscribe('XTKS.BRISK', 'ohlcv-1m', symbols='7203')             # bars beginning after it joined
    live.subscribe('XTKS.BRISK', 'imbalance', symbols='167A', snapshot=True)
    release(100)
    live.block_for_close(timeout=30)
    feed.wait()
    assert [t.trade_id for t in got['trades', '167A']] == [f'{s + 1}-0' for s in range(30, 201, 30)]
    assert [t.sequence for t in got['trades', '7203']] == [s + 1 for s in range(10, 201, 10)]   # each once
    # Joined at 09:01:40: the 09:02 bar is the first it covers, and 09:03 is not, ending at 09:03:20.
    minute = [(price10 / 10, size) for s in range(120, 180) for i, price10, size in trades_at(s) if i == 0]
    assert [(b.ts_event.minute, b.open, b.close, b.volume) for b in got['ohlcv-1m', '7203']] == \
        [(2, minute[0][0], minute[-1][0], sum(size for _, size in minute))]
    assert got['imbalance', '167A'] == []    # no auction after the open; the snapshot's quote has none either
    assert live.symbology_map[154694] == '167A'


def test_a_quiet_bar_closes_on_this_computers_clock(decoder, monkeypatch):
    """No later batch need say a bar has ended: this computer's clock, less the feed's delay, does."""
    monkeypatch.setattr(_gateway, 'LATENCY_WINDOW_NS', 0)    # allow for the latest delay only
    script, release = decoder(session(0, 30), holds=[1, 11])
    feed = broker_feed(script)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'ohlcv-1s', symbols='7203')
    bars = []
    live.add_callback(bars.append)
    live.start()
    wait_for(lambda: live._subscriptions[0].active)
    release(1)      # up to 09:00:10, whose trade opens the 09:00:10 bar; then the feed is quiet
    wait_for(lambda: bars, timeout=5)
    assert feed.seq == 10 and [(b.ts_event.second, b.open, b.volume) for b in bars] == [(10, 3001.0, 200)]
    release(11)
    live.block_for_close(timeout=30)
    feed.wait()
    assert [b.ts_event.second for b in bars] == [10, 20]     # 09:00:30's bar ends after the session does


def test_a_late_trade_goes_into_the_next_open_bar(decoder):
    """A trade stamped 09:00:19.5 that arrives with the 09:00:21 batch: its own bar has ended, so it counts in the
    next one still open (09:00:20), whose open it is by time."""
    batches = session(0, 30)
    late = {'issue_id': 0, 'code': '7203', 'frame': 22, 'time_us': OPEN_US + 19_500_000, 'price10': 31000,
            'quantity': 100, 'side': 1}
    batches[21] = dict(batches[21], trades=[*batches[21]['trades'], late])
    script, release = decoder(batches, holds=[1])
    feed = broker_feed(script)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'ohlcv-1s', symbols='7203')
    live.start()
    wait_for(lambda: live._subscriptions[0].active)
    release(1)
    bars = [(b.ts_event.second, b.open, b.high, b.low, b.close, b.volume) for b in live]
    feed.wait()
    assert bars == [(10, 3001.0, 3001.0, 3001.0, 3001.0, 200), (20, 3100.0, 3100.0, 3002.0, 3002.0, 400)]


def test_one_session_spans_trading_days(decoder):
    batches = session(0, 10) + session(0, 12, date='20261014')
    script, release = decoder(batches, holds=[5])
    feed = broker_feed(script)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'definition')
    live.subscribe('XTKS.BRISK', 'trades', symbols='7203')
    live.start()
    release(5)
    found = list(live)
    feed.wait()
    days = collections.Counter((r.schema, r.ts_event.date().isoformat()) for r in found)
    assert days == {('definition', '2026-10-12'): 2, ('trades', '2026-10-13'): 1,
                    ('definition', '2026-10-13'): 2, ('trades', '2026-10-14'): 1}
    assert len(feed.recordings) == 2 and feed.recordings[0]['name'] is None   # served, not packaged


def test_the_demo_through_connect_and_the_cli(decoder, monkeypatch, capsys):
    script, release = decoder(market(), holds=[1])
    monkeypatch.setattr(_live, 'DECODER', script)
    monkeypatch.setattr(cli, 'package_parts', lambda *a, **k: 1 / 0)    # the gateway alone packages nothing
    errors = []
    runner = threading.Thread(target=lambda: errors.append(cli.main(['live', '--site', 'demo', '--cache', '.', '--no-upload'])))
    runner.start()
    wait_for(lambda: _gateway.gateway_path('XTKS.BRISKDEMO').exists())
    live = brisk.Live()
    live.subscribe('XTKS.BRISKDEMO', 'imbalance', symbols='7203', start='2021-09-27')
    live.subscribe('XTKS.BRISKDEMO', 'trades', symbols='7203', start='2021-09-27T09:00:00.00002')
    out = io.StringIO()
    live.add_stream(out)
    live.start()
    release(1)
    live.block_for_close(timeout=30)
    runner.join(30)
    assert errors == [None]
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    assert [line['symbol'] for line in lines] == ['7203'] * 3
    assert lines[0]['ind_match_price'] == 10150.0 and lines[0]['ts_recv'] == lines[0]['ts_event']   # the demo: BRiSK's stamp
    assert [line['size'] for line in lines if 'trade_id' in line] == [2000, 3300]
    printed = capsys.readouterr()
    assert printed.out == '' and 'Serving XTKS.BRISKDEMO to briskapi.Live() on this computer only (127.0.0.1:' in printed.err
    assert not _gateway.gateway_path('XTKS.BRISKDEMO').exists()

    script, release = decoder(market())
    monkeypatch.setattr(_live, 'DECODER', script)
    cli.main(['live', '--site', 'demo', '--cache', '.', '--no-gateway', '--no-upload', '--quotes'])
    printed = capsys.readouterr()
    assert 'briskapi.Live' not in printed.err and printed.out


def test_the_gateway_admits_only_its_key_on_this_computer():
    gateway = _gateway.Gateway('XTKS.BRISK')
    info = json.loads(gateway.path.read_text())
    assert gateway._server.getsockname()[0] == '127.0.0.1' and info['port'] == gateway.port and info['key'] == gateway.key
    if os.name == 'posix':
        assert gateway.path.stat().st_mode & 0o777 == 0o600

    def connect(*messages, port=gateway.port):
        sock = socket.create_connection(('127.0.0.1', port), timeout=5)
        for message in messages:
            sock.sendall(message if isinstance(message, bytes) else json.dumps(message).encode() + b'\n')
        return sock, sock.makefile('rb')

    def ask(*messages, port=gateway.port, lines=1):
        sock, reader = connect(*messages, port=port)
        with sock:
            answers = [json.loads(reader.readline()) for _ in range(lines)]
        return answers[0] if lines == 1 else answers
    good = {'version': 1, 'key': gateway.key, 'dataset': 'XTKS.BRISK'}
    assert ask({**good, 'key': 'guess'}) == {'gateway': 'error', 'error': 'Wrong key'}
    assert ask({**good, 'version': 2})['error'].startswith('This gateway speaks version 1')
    assert ask({**good, 'dataset': 'XTKS.BRISKDEMO'})['error'] == 'This gateway serves XTKS.BRISK'
    assert ask(b'not json\n') == {'gateway': 'error', 'error': 'This gateway speaks version 1; update briskapi'}
    assert ask({**good, 'probe': True}) == {'gateway': 'probe', 'dataset': 'XTKS.BRISK'}
    assert ask(good) == {'gateway': 'session', 'version': 1, 'dataset': 'XTKS.BRISK'}
    assert ask(good, {'subscribe': [0], 'symbols': ['../x']}, lines=2)[1] == {'gateway': 'error', 'error': "Invalid symbol '../x'"}
    assert ask(good, {'subscribe': 'all'}, lines=2)[1]['error'] == 'Expected {"subscribe": [ids], ...}'
    # Before the feed's first batch: nothing to give yet, and no replay.
    assert ask(good, {'subscribe': [0], 'symbols': None, 'replay': True, 'snapshot': True}, lines=2)[1] == \
        {'gateway': 'subscribed', 'id': [0], 'bootstrap': None, 'snapshot': None, 'replay': False}
    held, reader = connect(good)
    assert json.loads(reader.readline())['gateway'] == 'session'
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(_gateway, 'MAX_CLIENTS', 1)
        wait_for(lambda: len(gateway._clients) == 1)
        assert ask(good)['error'] == 'At most 1 Live sessions at once'
    held.close()
    with pytest.raises(brisk.BriskError, match='Wrong key'):
        live = brisk.Live(key='guess', port=gateway.port)
        live.subscribe('XTKS.BRISK', 'trades')
        live.start()
    # One gateway per dataset: another feed is refused while this one answers.
    with pytest.raises(brisk.BriskError, match='already serves XTKS.BRISK'):
        _gateway.Gateway('XTKS.BRISK')
    gateway.close()
    assert not gateway.path.exists()
    with pytest.raises(brisk.BriskError, match=r'No feed on this computer serves XTKS.BRISK: start one with `brisk live --site sbi`'):
        brisk.Live().subscribe('XTKS.BRISK', 'trades')
    # A file left by a feed that died is replaced, and a session finding no one there says so.
    stale = dict(info, port=_free_port())
    gateway.path.write_text(json.dumps(stale))
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'trades')
    with pytest.raises(brisk.BriskError, match='is `brisk live` still running'):
        live.start()
    replaced = _gateway.Gateway('XTKS.BRISK')
    assert json.loads(replaced.path.read_text())['key'] == replaced.key != stale['key']
    replaced.close('completed')
    assert ask({'version': 1, 'key': replaced.key, 'dataset': 'XTKS.BRISK'}, port=replaced.port)['status'] == 'completed'
    wait_for(lambda: replaced._server.fileno() == -1)     # and soon stops listening


def _free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def test_a_slow_session_is_dropped_not_waited_for(monkeypatch):
    monkeypatch.setattr(_gateway, 'MAX_BEHIND', 4 * 1024**2)
    gateway = _gateway.Gateway('XTKS.BRISKDEMO')
    sock = socket.create_connection(('127.0.0.1', gateway.port))
    sock.sendall(json.dumps({'version': 1, 'key': gateway.key, 'dataset': 'XTKS.BRISKDEMO'}).encode() + b'\n'
                 + json.dumps({'subscribe': [0], 'symbols': None}).encode() + b'\n')
    wait_for(lambda: gateway._clients and gateway._clients[0].codes is None)
    line = b'{"type":"quotes"' + b' ' * 1024**2 + b'}\n'
    client = gateway._clients[0]   # it may leave the list as soon as it is dropped
    started = time.monotonic()
    for _ in range(64):          # 64 MB to a session that reads nothing
        gateway.publish({'type': 'quotes', 'quotes': []}, line)
    assert time.monotonic() - started < 5 and client._last   # the feed went on; the session is dropped
    received = b''.join(iter(lambda: sock.recv(1 << 20), b''))
    assert json.loads(received.splitlines()[-1])['error'].startswith('This session fell more than 4 MB behind')
    wait_for(lambda: not gateway._clients)
    gateway.close()
    sock.close()


def test_live_api_errors_and_controls(decoder, tmp_path):
    script, release = decoder(market(), holds=[1, 3])
    feed = broker_feed(script)
    live = brisk.Live()
    for args, message in ((('XTKS.NOPE', 'trades'), 'Unknown dataset'), (('XTKS.BRISK', 'tbbo'), 'Unknown schema'),
                          (('XTKS.BRISK', 'trades', 'ALL_SYMBOLS', 'isin'), 'raw_symbol')):
        with pytest.raises(ValueError, match=message):
            live.subscribe(*args)
    with pytest.raises(ValueError, match='not both'):
        live.subscribe('XTKS.BRISK', 'mbp-10', start=0, snapshot=True)
    with pytest.raises(ValueError, match='snapshot applies'):
        live.subscribe('XTKS.BRISK', 'trades', snapshot=True)
    with pytest.raises(brisk.BriskError, match='Subscribe to something'):
        live.start()
    with pytest.raises(brisk.BriskError, match='not started'):
        live.block_for_close()
    live.subscribe('XTKS.BRISK', 'brisk-quotes', symbols=['7203'])
    with pytest.raises(ValueError, match='streams XTKS.BRISK'):
        live.subscribe('XTKS.BRISKDEMO', 'trades')
    failures, written = [], tmp_path / 'records.jsonl'
    live.add_callback(lambda r: 1 / 0, failures.append)
    live.add_callback(lambda r: 1 / 0)
    live.add_stream(written)
    with pytest.raises(FileExistsError):
        live.add_stream(written)
    with pytest.warns(UserWarning, match='Live callback failed: ZeroDivisionError'):
        live.start()
        with pytest.raises(brisk.BriskError, match='streams once'):
            live.start()
        wait_for(lambda: live._subscriptions[0].active)
        release(1)
        wait_for(lambda: written.read_text().count('\n') == 1)
    assert len(failures) == 1 and live.is_connected() and 'streaming' in repr(live)
    live.block_for_close(timeout=0.1)        # stops it: the feed goes on
    assert not live.is_connected() and feed.status == 'running'
    assert [json.loads(line)['sequence'] for line in written.read_text().splitlines()] == [2]
    with pytest.raises(brisk.BriskError, match='has ended'):
        live.subscribe('XTKS.BRISK', 'trades')
    release(3)
    feed.wait()


def test_a_failed_feed_fails_its_sessions(decoder):
    broken = market()[:3]
    broken[2] = dict(broken[2], seq=9)
    script, release = decoder(broken, holds=[2])
    feed = broker_feed(script)
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'trades')
    records_seen = []
    live.start()
    release(2)
    with pytest.raises(brisk.BriskError, match='The feed ended the session: Stream sequence error at 9'):
        records_seen.extend(live)
    with pytest.raises(brisk.BriskError, match='sequence error'):
        feed.wait()
    with pytest.raises(brisk.BriskError, match='sequence error'):
        live.block_for_close()


def test_a_session_whose_feed_vanishes_fails():
    gateway = _gateway.Gateway('XTKS.BRISKDEMO')
    live = brisk.Live()
    live.subscribe('XTKS.BRISKDEMO', 'trades')
    live.start()
    wait_for(lambda: gateway._clients)
    gateway._clients[0]._stop()       # as when the feed's process is killed
    with pytest.raises(brisk.BriskError, match='closed the connection without ending the session'):
        list(live)
    gateway.close()


def test_records_read_as_attributes():
    record = brisk.Record({'price': 1.5}, 'trades')
    assert record.price == record['price'] == 1.5 and record == {'price': 1.5} and record.schema == 'trades'
    assert repr(record) == "Record('trades', {'price': 1.5})"
    with pytest.raises(AttributeError):
        record.size


def _fast(batch, codes):
    """A frame's batch as a broker host's fast lane writes it: only these symbols' quotes and trades."""
    return {**{k: v for k, v in batch.items() if k not in ('quotes', 'trades')}, 'lane': 'fast',
            'quotes': [q for q in batch['quotes'] if q['code'] in codes],
            'trades': [t for t in batch['trades'] if t['code'] in codes]}


def _line_of(batch):
    return json.dumps(batch).encode() + b'\n'


def _records(schemas, symbols):
    """A Live session's records, as dicts, by schema and symbol."""
    live, got = brisk.Live(), collections.defaultdict(list)
    for schema in schemas:
        live.subscribe('XTKS.BRISK', schema, symbols=symbols)
    live.add_callback(lambda r: got[r.schema, r.symbol].append(dict(r)))
    live.start()
    return live, got


def test_a_session_on_the_fast_lane_gets_its_symbols_ahead_of_the_recording_and_the_same_records():
    batches = session(0, 60)
    told = []
    gateway = _gateway.Gateway('XTKS.BRISK', on_fast=told.append)
    fast, fast_got = _records(['trades', 'mbp-10'], ['7203'])   # before the day's session: waiting to know its lanes
    slow, slow_got = _records(['trades', 'mbp-10'], 'ALL_SYMBOLS')  # never on the fast lane
    wait_for(lambda: sorted(c.lane for c in gateway._clients) == ['pending', 'slow'])
    assert told[-1] == {'7203'}, 'the host is told which symbols the fast lane reads'
    gateway.lane({'type': 'lanes', 'fast': True, 'capacity': 500, 'codes': ['7203']}, None)
    assert sorted(c.lane for c in gateway._clients) == ['fast', 'slow']
    gateway.publish(batches[0], _line_of(batches[0]))
    # The whole day on the fast lane before any of the recording's batches (which wait behind the whole market).
    for b in batches[1:-1]:
        gateway.lane(_fast(b, {'7203'}), None)
    wait_for(lambda: len(fast_got['trades', '7203']) == 6 and len(fast_got['mbp-10', '7203']) == 61)   # with the bootstrap's
    assert not slow_got['trades', '7203'], 'nothing yet on the recording'
    for b in batches[1:]:
        gateway.publish(b, _line_of(b))
    wait_for(lambda: len(slow_got['mbp-10', '7203']) == 61)
    gateway.close()
    fast.block_for_close(timeout=10)
    slow.block_for_close(timeout=10)
    assert fast_got['trades', '7203'] == slow_got['trades', '7203'] and len(fast_got['trades', '7203']) == 6
    assert fast_got['mbp-10', '7203'] == slow_got['mbp-10', '7203'], 'each record once, the same as the recording\'s'
    assert not any(symbol == '167A' for _, symbol in fast_got)


def test_a_session_leaving_the_fast_lane_or_joining_it_a_symbol_misses_nothing_and_gets_nothing_twice():
    batches = session(0, 60)
    told = []
    gateway = _gateway.Gateway('XTKS.BRISK', on_fast=told.append)
    gateway.lane({'type': 'lanes', 'fast': True, 'capacity': 2, 'codes': []}, None)
    gateway.publish(batches[0], _line_of(batches[0]))
    joins, joins_got = _records(['mbp-10'], ['7203'])
    leaves, leaves_got = _records(['mbp-10'], ['7203'])
    reference, reference_got = _records(['mbp-10'], 'ALL_SYMBOLS')
    wait_for(lambda: len(gateway._clients) == 3 and sum(c.lane == 'joining' for c in gateway._clients) == 2
             and any(c.lane == 'slow' and c.codes is None for c in gateway._clients))
    gateway.lane({'lane': 'fast', 'type': 'join', 'seq': 0, 'quotes': [], 'codes': ['7203']}, None)
    wait_for(lambda: sorted(c.lane for c in gateway._clients) == ['fast', 'fast', 'slow'])
    # The fast lane is at batch 30, the recording at 10.
    for b in batches[1:11]:
        gateway.publish(b, _line_of(b))
    for b in batches[1:31]:
        gateway.lane(_fast(b, {'7203'}), None)
    wait_for(lambda: len(joins_got['mbp-10', '7203']) == 30 and len(leaves_got['mbp-10', '7203']) == 30)
    # One adds 167A, which fits (two symbols); the other asks for a replay of it, which the fast lane has not.
    joining, leaving = [c for c in gateway._clients if c.lane == 'fast']
    joins.subscribe('XTKS.BRISK', 'mbp-10', symbols=['167A'])
    wait_for(lambda: joining.lane == 'joining' or leaving.lane == 'joining')
    if leaving.lane == 'joining':
        joining, leaving = leaving, joining
    assert told[-1] == {'7203', '167A'}
    leaves.subscribe('XTKS.BRISK', 'mbp-10', symbols=['167A'], start=0)
    wait_for(lambda: leaving.lane == 'slow')
    assert (leaving.fast_until, leaving.old_codes) == (30, {'7203'})
    gateway.lane({'lane': 'fast', 'type': 'join', 'seq': 30, 'quotes': [batches[30]['quotes'][1]], 'codes': ['167A']}, None)
    wait_for(lambda: joining.lane == 'fast')
    for b in batches[31:-1]:
        gateway.lane(_fast(b, {'7203', '167A'}), None)
    for b in batches[11:]:
        gateway.publish(b, _line_of(b))
    wait_for(lambda: len(reference_got['mbp-10', '7203']) == 60)   # joined after the bootstrap
    gateway.close()
    for live in (joins, leaves, reference):
        live.block_for_close(timeout=10)
    # 7203 throughout, from either lane, exactly as the recording has it.
    assert joins_got['mbp-10', '7203'] == leaves_got['mbp-10', '7203'] == reference_got['mbp-10', '7203']
    # 167A: on the fast lane after its join; from the recording after the subscription for the one that left.
    after = lambda records, seq: [r for r in records if r['sequence'] > seq + 1]  # noqa: E731 - sequence is the frame
    assert joins_got['mbp-10', '167A'] == after(reference_got['mbp-10', '167A'], 30)
    assert leaves_got['mbp-10', '167A'] == after(reference_got['mbp-10', '167A'], 10)


HOST = """'use strict';
// The live host against a fake broker site serving the demo's frames, a few at a time, with its fast lane told
// on stdin, as the Python feed runs it.
const fs = require('fs'), path = require('path');
const { once } = require('events');
const { live, readLanes } = require(%(brisk)s);
const { frames } = require(%(decoder)s);
const cache = process.env.BRISK_MOCK_CACHE;
const files = Object.fromEntries(['fita.js', 'fita.wasm', 'master.dat', 'snapshot.dat', 'ws.dat'].map(n => [n, fs.readFileSync(path.join(cache, n))]));
const chunks = [...frames(files['ws.dat'])].slice(0, 600).map(f => f.data);
const routes = {
  '/api/frontend/boot': () => JSON.stringify({ api_token: 't', identity: 'a'.repeat(64) }),
  '/api/app/boot': () => JSON.stringify({ date: '2021-09-27', series: 0, master: 'm1', snapshot: 's1', ws_url: '/realtime/0?session=abc' }),
  '/': () => '<html><script src="/main.abc.js"></script></html>',
  '/main.abc.js': () => 'load("assets/wasm/fita.test.js");load("assets/wasm/fita.test.wasm")',
  '/assets/wasm/fita.test.js': () => files['fita.js'], '/assets/wasm/fita.test.wasm': () => files['fita.wasm'],
  '/api/master/m1': () => files['master.dat'], '/api/snapshot/s1': () => files['snapshot.dat'],
};
const fetchImpl = async url => new Response(routes[url.pathname] ? routes[url.pathname]() : '', { status: routes[url.pathname] ? 200 : 404 });
class Socket extends EventTarget {
  constructor() {
    super();
    let i = 0;
    const send = n => { for (const end = Math.min(chunks.length, i + n); i < end; i++) {
      const c = chunks[i]; this.dispatchEvent(new MessageEvent('message', { data: c.buffer.slice(c.byteOffset, c.byteOffset + c.length) })); } };
    setImmediate(() => send(3));
    // The stream waits at frame 100 until the test says go (a file), so the session that joins does so mid-stream however slow the computer.
    this.timer = setInterval(() => { if (process.env.GATE && i >= 100 && !fs.existsSync(process.env.GATE)) return; send(5); if (i >= chunks.length) { clearInterval(this.timer); setTimeout(() => this.close(1000), 50); } }, 25);
  }
  send() {}
  close(code = 1000) { clearInterval(this.timer); if (this.closed) return; this.closed = true; this.dispatchEvent(Object.assign(new Event('close'), { code, reason: '' })); }
}
live({ cookies: { s: 'v' }, protocolVersion: 16000, fetchImpl, WebSocketImpl: Socket, staleSeconds: 0, heartbeatMs: 1e9,
  lanes: readLanes(process.stdin),
  emit: async (record, line = JSON.stringify(record)) => { if (!process.stdout.write(line + '\\n')) await once(process.stdout, 'drain'); } })
  // Exit once stdout has taken every line: on macOS a pipe is written asynchronously, and exiting at once can cut
  // the last lines short.
  .then(() => process.stdout.write('', () => process.exit(0)), error => { console.error(error.message); process.exit(1); });
"""


@pytest.mark.skipif(not os.environ.get('BRISK_MOCK_CACHE'), reason='set BRISK_MOCK_CACHE for fixture replay')
def test_a_live_session_joins_the_fast_lane_of_a_running_host_through_the_feed(tmp_path):
    decoders = Path(_live.__file__).parent / 'decoder'
    script = tmp_path / 'host.cjs'
    script.write_text(HOST % {'brisk': json.dumps(str(decoders / 'brisk.cjs')), 'decoder': json.dumps(str(decoders / 'decoder.cjs'))})
    go = tmp_path / 'go'
    feed = _live.Feed(command=['node', str(script)], env={**os.environ, 'BRISK_WORKERS': '3', 'GATE': str(go)}, upload=False,
                      gateway=True).ready()
    assert feed.gateway._lanes == {'fast': True, 'capacity': 500}
    slow, slow_got = _records(['mbp-10'], 'ALL_SYMBOLS')
    wait_for(lambda: [c.lane for c in feed.gateway._clients] == ['slow'] and feed.gateway._clients[0].codes is None)
    fast, fast_got = _records(['mbp-10', 'trades'], ['7203'])
    wait_for(lambda: sorted(c.lane for c in feed.gateway._clients) == ['fast', 'slow'])   # joined: the host read it
    go.touch()   # the rest of the stream
    feed.wait()
    fast.block_for_close(timeout=30)
    slow.block_for_close(timeout=30)
    got, expected = fast_got['mbp-10', '7203'], slow_got['mbp-10', '7203']
    assert len(got) > 40 and got == expected[-len(got):], 'from its join, the recording\'s records, each once'
    assert all(symbol == '7203' for _, symbol in fast_got)


def test_two_feeds_starting_at_once_cannot_both_serve(monkeypatch):
    # Both pass the check that no live gateway is registered (the first feed has not written its file yet when
    # the second looks); only the first registers, the second is refused as a later one would be.
    first = _gateway.Gateway('XTKS.BRISK')
    real = _gateway._read_file
    looks = []
    monkeypatch.setattr(_gateway, '_read_file', lambda path: looks.append(path) or (None if len(looks) == 1 else real(path)))
    with pytest.raises(brisk.BriskError, match='Another feed on this computer already serves XTKS.BRISK'):
        _gateway.Gateway('XTKS.BRISK')
    assert real(first.path)['pid'] == os.getpid() and real(first.path)['port'] == first.port, 'the winner stays registered'
    assert not list(first.path.parent.glob('.*')), 'no staging file is left'
    first.close('completed', None)


def test_a_crashed_feeds_registration_is_taken_over(monkeypatch):
    first = _gateway.Gateway('XTKS.BRISK')
    path, stale = first.path, _gateway._read_file(first.path)
    first.close('completed', None)
    path.write_text(json.dumps({**stale, 'port': 1, 'pid': 2**22}))   # as a crashed feed leaves it: nothing answers
    taker = _gateway.Gateway('XTKS.BRISK')
    assert _gateway._read_file(path)['port'] == taker.port
    taker.close('completed', None)


# Live batches are projected record by record where it matters (Live._rows): the same records as the Arrow projection.

def _live_batches(seed, count=150):
    import random
    rng = random.Random(seed)
    master = [(0, '7203'), (1, '167A'), (2, '6758')]
    prices = [0, 1, 2, 4, 8, 18, 10, 11, 99, 9990, 10010, 123456, 30000, 2**31]
    now = OPEN_US = 9 * 3600 * 10**6
    out = [dict(type='bootstrap', seq=0, source='brisk_live', trading_date='20260311', source_time_us=OPEN_US,
                input_transport=dict(brisk_schema.BROKER_TRANSPORTS['sbi']), source_timestamp_origin='brisk_decoder_unverified',
                exchange_delay_ms=None, market_issue_count=3, quotes=[], trades=[],
                master=[{**dict.fromkeys(brisk_schema.MASTER_KEYS, 0), 'issue_id': i, 'code': c, 'name': c, 'lot_size': 100,
                         'limit_up10': 99999, 'limit_down10': 100} for i, c in master])]
    for seq in range(1, count):
        now += rng.randrange(1, 90_000)
        quotes, trades = [], []
        for issue, code in master:
            if rng.random() < .6:
                book = lambda: [[rng.choice(prices), rng.randrange(0, 10**7), rng.randrange(0, 1000)]  # noqa: E731
                                for _ in range(rng.choice([0, 1, 2, 5, 10, 11]))]
                quotes.append({**dict.fromkeys(brisk_schema.QUOTE_KEYS, 0), **{k: [] for k in brisk_schema.BOOK_KEYS}, 'issue_id': issue,
                               'code': code, 'frame': rng.randrange(1, 10**6), 'source_time_us': now - rng.randrange(0, 5000),
                               'bid_levels': book(), 'ask_levels': book(),
                               **{k: rng.choice(prices) for k in ('indicative_price10', 'closing_indicative_price10', 'reference_price10')},
                               **{k: rng.randrange(0, 10**6) for k in ('indicative_volume', 'closing_indicative_volume', 'market_buy_quantity',
                                                                          'market_sell_quantity', 'closing_market_buy_quantity',
                                                                          'closing_market_sell_quantity', 'bid_total_quantity',
                                                                          'ask_total_quantity', 'bid_closing_total_quantity',
                                                                          'ask_closing_total_quantity')},
                               **{k: rng.choice([0, 1]) for k in ('indicative_imbalance_side', 'closing_indicative_imbalance_side')},
                               'special_quote_time_us': rng.choice([0, 0, now - 1000])})
            for _ in range(rng.choice([0, 0, 1, 3])):
                trades.append({'issue_id': issue, 'code': code, 'frame': rng.choice([5, 5, 6, 7]), 'time_us': now,
                               'price10': rng.choice(prices[:-1]) + 1, 'quantity': rng.randrange(1, 10**6), 'side': rng.choice([0, 1, 2, 3])})
        rng.shuffle(trades)
        if rng.random() < .15:
            out.append(dict(type='history', seq=seq, source_time_us=now, trades=trades))
        else:
            out.append(dict(type='quotes', seq=seq, source_time_us=now, received_unix_ms=1773187200000 + now // 1000 + rng.choice([0, 20]),
                            decode_ns=1, replay_lateness_ms=None, quotes=quotes if rng.random() < .9 else [], trades=trades))
    return out


@pytest.mark.parametrize('seed, subscriptions', [
    (1, [('mbp-10', None, None)]),
    (2, [('trades', None, None)]),
    (3, [('mbp-10', {'167A', '7203'}, None), ('trades', {'6758'}, None)]),
    (4, [('mbp-10', {'7203'}, 9 * 3600 * 10**6 + 2_000_000 * 10**3), ('mbp-10', None, None), ('trades', None, None)]),
    (5, [('imbalance', None, None)]),
    (6, [('imbalance', {'167A'}, None), ('mbp-10', {'7203', '6758'}, None), ('trades', None, None)]),
])
def test_live_records_made_record_by_record_are_those_the_arrow_projection_makes(seed, subscriptions):
    from briskapi import _gateway
    batches = _live_batches(seed)
    sides = []
    for by_rows in (True, False):
        live = _gateway.Live()
        live.dataset, live._by_rows = 'XTKS.BRISK', by_rows
        live._subscriptions = [_gateway._Subscription(i, name, symbols, start, False) for i, (name, symbols, start) in enumerate(subscriptions)]
        for s in live._subscriptions:
            s.active = True
        live._new_day(batches[0])
        used = []
        real = live._rows
        live._rows = lambda *a: used.append(1) or real(*a)
        sides.append([live._project(b, live._subscriptions, live._day) for b in batches[1:]])
        assert bool(used) == by_rows, 'each way used its own projection'
    by_rows, by_arrow = sides
    assert sum(len(r) for r in by_arrow) > 100
    for mine, theirs in zip(by_rows, by_arrow):
        assert [(r.schema, dict(r)) for r in mine] == [(r.schema, dict(r)) for r in theirs]
        assert [[type(v) for v in r.values()] for r in mine] == [[type(v) for v in r.values()] for r in theirs]
        assert [list(r) for r in mine] == [list(r) for r in theirs]   # and in the same order of fields


@pytest.mark.parametrize('seed, subscriptions, replaying', [
    (11, [('mbp-10', None, 0)], [0]),
    (12, [('trades', {'7203', '6758'}, 0)], [0]),
    (13, [('imbalance', {'167A'}, 0), ('mbp-10', {'7203'}, 0), ('trades', None, 0)], [0, 1, 2]),
    (14, [('trades', {'6758'}, 0), ('mbp-10', None, None)], [0]),     # one subscription replays, another is live only
    (15, [('mbp-10', {'7203'}, 0), ('trades', None, None)], [0]),     # trade IDs come from every trade, whoever wants it
])
def test_a_replay_is_made_record_by_record_as_it_is_by_arrow_too(seed, subscriptions, replaying):
    from briskapi import _gateway
    batches = _live_batches(seed)
    sides = []
    for by_rows in (True, False):
        live = _gateway.Live()
        live.dataset, live._by_rows = 'XTKS.BRISK', by_rows
        live._subscriptions = [_gateway._Subscription(i, name, symbols, start, False) for i, (name, symbols, start) in enumerate(subscriptions)]
        takers = [live._subscriptions[i] for i in replaying]
        day = _gateway._Day(batches[0])   # a replay has a day of its own: its trade IDs carry on from its own batches
        used = []
        real = live._rows
        live._rows = lambda *a: used.append(1) or real(*a)
        sides.append([live._project(b, takers, day, replayed=True) for b in batches[1:]])
        assert bool(used) == by_rows, 'each way used its own projection'
    by_rows, by_arrow = sides
    assert sum(len(r) for r in by_arrow) > 50
    for mine, theirs in zip(by_rows, by_arrow):
        assert [(r.schema, dict(r)) for r in mine] == [(r.schema, dict(r)) for r in theirs]
        assert [[type(v) for v in r.values()] for r in mine] == [[type(v) for v in r.values()] for r in theirs]


def test_live_age_tool_splits_a_records_age_into_brisks_delay_and_briskapis_own(monkeypatch, capsys):
    import datetime as dt
    import importlib.util
    import time
    spec = importlib.util.spec_from_file_location('live_age', Path(__file__).resolve().parents[1] / 'tools' / 'live_age.py')
    tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tool)

    class FakeLive:
        def subscribe(self, **options):
            self.options = options

        def add_callback(self, callback):
            self.callback = callback

        def start(self):
            now = time.time()
            for delay, own in ((30, 5), (40, 5), (50, 10), (500, 20)):   # BRiSK's delay and briskapi's own, in milliseconds
                received = dt.datetime.fromtimestamp(now - own / 1000, dt.timezone.utc)
                self.callback({'ts_event': received - dt.timedelta(milliseconds=delay), 'ts_recv': received})

        def block_for_close(self, timeout=None):
            self.timeout = timeout
    fake = FakeLive()
    monkeypatch.setattr(tool.briskapi, 'Live', lambda: fake)
    import ast
    import sys
    monkeypatch.setattr(sys, 'argv', ['live_age.py', '--symbols', '7203,6758', '--minutes', '2'])
    tool.main()
    assert fake.options == {'dataset': 'XTKS.BRISK', 'schema': 'mbp-10', 'symbols': ['7203', '6758']} and fake.timeout == 120
    out = capsys.readouterr().out
    assert '4 records for 7203,6758' in out
    said = {line.split('  (')[0].strip(): ast.literal_eval(line.split('): ', 1)[1]) for line in out.splitlines()[1:]}
    assert set(said) == {'now - ts_event', 'ts_recv - ts_event', 'now - ts_recv'}
    assert 499 <= said['ts_recv - ts_event']['max'] <= 501 and 49 <= said['ts_recv - ts_event']['p50'] <= 51
    assert 20 <= said['now - ts_recv']['max'] <= 60 and said['now - ts_recv']['p50'] < 40
    assert said['now - ts_event']['max'] >= said['ts_recv - ts_event']['max']


def _lane_session(shift=0, frames=40):
    """A broker session's lines as a host with a fast lane writes them: its lanes line, its bootstrap, then each frame's lane
    line (7203's quotes) ahead of the recording's batch, and the end. `shift` moves its clock, to tell sessions apart."""
    batches = session(0, frames)
    lines = [{'type': 'lanes', 'fast': True, 'capacity': 500, 'codes': ['7203']}]
    for b in batches:
        b = {**b, **({'source_time_us': b['source_time_us'] + shift} if 'source_time_us' in b else {})}
        if b['type'] == 'quotes':
            lines.append(_fast(b, {'7203'}))
        lines.append(b)
    return lines


def test_a_quote_on_the_fast_lane_does_not_wait_for_the_feed_thread_to_get_to_it(decoder, monkeypatch):
    gate = threading.Event()
    real = _live.Feed._apply

    def slow(self, batch, new_day=False):
        if batch['seq'] == 3:
            gate.wait(30)   # the feed thread is busy with the recording's batch 3
        return real(self, batch, new_day)
    monkeypatch.setattr(_live.Feed, '_apply', slow)
    script, _ = decoder(_lane_session())
    feed = broker_feed(script)
    try:
        wait_for(lambda: feed.gateway._fast_seq >= 30)
        assert feed.seq == 2 and feed._lane_thread, 'the recording stands at batch 2 while the lane is at 30'
        assert feed.gateway._fast_quotes[feed.gateway._code_ids['7203']]['frame'] >= 30
    finally:
        gate.set()
    feed.wait(timeout=20)
    assert feed.status == 'completed' and feed.seq == 41


def test_the_lanes_line_bootstrap_and_end_keep_their_place_between_the_lane_thread_and_the_feed_thread(decoder, monkeypatch):
    import random
    rng = random.Random(7)
    events, lock = [], threading.Lock()
    real_lane, real_publish, real_apply = _gateway.Gateway.lane, _gateway.Gateway.publish, _live.Feed._apply

    def lane(self, batch, line):
        with lock:
            events.append(('lane', batch['type'], batch.get('seq'), batch.get('source_time_us', -1)))
        # and the lane thread sometimes behind the feed thread, at the last frames of a session most of all
        time.sleep(0.05 if batch.get('type') == 'quotes' and batch['seq'] >= 38 else rng.choice((0, 0, 0.002)))
        return real_lane(self, batch, line)

    def publish(self, batch, line, spool=None):
        with lock:
            events.append(('publish', batch['type'], batch['seq'], batch['source_time_us']))
        return real_publish(self, batch, line, spool)

    def apply(self, batch, new_day=False):
        time.sleep(rng.choice((0, 0, 0.001, 0.003)))   # the feed thread is sometimes behind the lane thread, sometimes not
        return real_apply(self, batch, new_day)
    monkeypatch.setattr(_gateway.Gateway, 'lane', lane)
    monkeypatch.setattr(_gateway.Gateway, 'publish', publish)
    monkeypatch.setattr(_live.Feed, '_apply', apply)
    shift = 10**11   # the second session's clock, a day later
    script, _ = decoder(_lane_session(0) + _lane_session(shift))
    broker_feed(script).wait(timeout=60)
    lanes = [i for i, e in enumerate(events) if e[:2] == ('lane', 'lanes')]
    boots = [i for i, e in enumerate(events) if e[:2] == ('publish', 'bootstrap')]
    ends = [i for i, e in enumerate(events) if e[:2] == ('publish', 'end')]
    assert len(lanes) == len(boots) == len(ends) == 2
    fast = [(i, e) for i, e in enumerate(events) if e[0] == 'lane' and e[1] == 'quotes']
    first = [i for i, e in fast if e[3] < shift // 2]
    second = [i for i, e in fast if e[3] >= shift // 2]
    assert len(first) == len(second) == 40
    # Each session's lanes line before its bootstrap, its bootstrap before its lane quotes, those before its end, and the second
    # session's lanes line only once the first has ended: as when one thread read the host's lines in turn.
    assert lanes[0] < boots[0] < min(first) and max(first) < ends[0] < lanes[1] < boots[1] < min(second) and max(second) < ends[1]


def test_a_failure_on_the_lane_thread_fails_the_feed_visibly(decoder, monkeypatch):
    real = _gateway.Gateway.lane

    def lane(self, batch, line):
        if batch.get('seq') == 7:
            raise RuntimeError('the lane broke')
        return real(self, batch, line)
    monkeypatch.setattr(_gateway.Gateway, 'lane', lane)
    script, _ = decoder(_lane_session(0, 60))
    feed = _live.Feed(command=['node', str(script)], upload=False, gateway=True)
    with pytest.raises(brisk.BriskError):
        feed.wait(timeout=20)
    assert feed.status == 'failed' and 'the lane broke' in str(feed.error)


def _latest_rows(table):
    """latest()'s rows as dicts keyed by symbol, without ts_recv (the feed's receipt time, which a record carries too)."""
    return {row['symbol']: {k: v for k, v in row.items() if k != 'ts_recv'} for row in table.to_pylist()}


def test_latest_gives_every_symbols_last_record_in_one_request_from_the_fast_lane_and_sends_no_record():
    batches = session(0, 60)
    gateway = _gateway.Gateway('XTKS.BRISK', on_fast=lambda codes: None)
    gateway.lane({'type': 'lanes', 'fast': True, 'capacity': 500, 'codes': []}, None)
    gateway.publish(batches[0], _line_of(batches[0]))
    codes = ['7203', '167A']
    keeper, kept = brisk.Live(), []
    keeper.subscribe('XTKS.BRISK', 'mbp-10', symbols=codes, stream=False)
    keeper.add_callback(kept.append)
    keeper.start()
    streamer, got = _records(['mbp-10'], codes)
    wait_for(lambda: {c.lane for c in gateway._clients} == {'joining'})
    gateway.lane({'lane': 'fast', 'type': 'join', 'seq': 0, 'quotes': [], 'codes': codes}, None)
    wait_for(lambda: {c.lane for c in gateway._clients} == {'fast'})
    for b in batches[1:-1]:
        fast = _fast(b, set(codes))
        gateway.lane(fast, None)
    wait_for(lambda: len(got['mbp-10', '7203']) == 60 and len(got['mbp-10', '167A']) == 60)
    table = keeper.latest()
    rows = _latest_rows(table)
    assert set(rows) == set(codes)
    for code in codes:   # the last record a stream got, flagged as the snapshot it is
        assert rows[code] == {**{k: v for k, v in got['mbp-10', code][-1].items() if k != 'ts_recv'}, 'flags': 32}
    assert table.column('ts_recv').null_count == 0 and not kept, 'no record was sent to the session that asked for none'
    one = keeper.latest(symbols=['7203'])
    assert one.num_rows == 1 and one.column('symbol').to_pylist() == ['7203']
    gateway.close()
    streamer.block_for_close(timeout=10)


def test_latest_reads_the_recordings_quotes_where_the_fast_lane_does_not_read_a_symbol_and_receipt_times_come_with_them():
    batches = session(0, 30)
    gateway = _gateway.Gateway('XTKS.BRISK')   # no fast lane: the recording's state answers
    gateway.publish(batches[0], _line_of(batches[0]))
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'mbp-10', symbols=['7203', '167A'], stream=False)
    live.start()
    wait_for(lambda: gateway._clients and gateway._clients[0].codes)
    for b in batches[1:-1]:
        gateway.publish(b, _line_of(b))
    table = live.latest()
    got = {row['symbol']: row for row in table.to_pylist()}
    last = {q['code']: (b, q) for b in batches[1:-1] for q in b.get('quotes', ())}
    assert set(got) == {'7203', '167A'}
    for code, row in got.items():
        batch, quote = last[code]
        assert row['sequence'] == quote['frame']
        if batch.get('received_unix_ms'):
            assert row['ts_recv'].timestamp() * 1000 == batch['received_unix_ms'], 'when the feed received that quote'
        else:
            assert row['ts_recv'] == row['ts_event']
    assert live.latest(symbols='ALL_SYMBOLS').num_rows == len({q['code'] for q in batches[0]['quotes']})
    gateway.close()


def test_latest_answers_the_other_quote_schemas_and_refuses_what_is_not_a_quote():
    batches = session(0, 30)
    gateway = _gateway.Gateway('XTKS.BRISK')
    gateway.publish(batches[0], _line_of(batches[0]))
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'trades', symbols=['7203'])
    live.start()
    wait_for(lambda: gateway._clients and gateway._clients[0].codes)
    for b in batches[1:-1]:
        gateway.publish(b, _line_of(b))
    assert live.latest().schema.names[:3] == live.latest('mbp-10').schema.names[:3], 'a trades session reads quotes as mbp-10'
    assert 'ind_match_price' in live.latest('imbalance', symbols=['7203']).schema.names
    assert 'last_price' in live.latest('brisk-quotes', symbols=['7203']).schema.names or live.latest('brisk-quotes').num_rows
    with pytest.raises(ValueError, match='reads quotes'):
        live.latest('trades')
    gateway.close()


def test_latest_before_the_day_begins_has_no_rows_in_every_quote_schema():
    # A Live session that connected before the day's bootstrap asked for imbalance records, and the gateway's thread for it
    # died projecting them without the day's equity table (AttributeError: 'NoneType' object has no attribute 'column').
    gateway = _gateway.Gateway('XTKS.BRISK')
    live = brisk.Live()
    live.subscribe('XTKS.BRISK', 'imbalance', symbols=['7203'], stream=False)
    live.start()
    wait_for(lambda: gateway._clients and gateway._clients[0].codes)
    for schema, column in (('imbalance', 'upper_collar'), ('mbp-10', 'bid_px_00'), ('brisk-quotes', 'last_price')):
        table = live.latest(schema)
        assert table.num_rows == 0 and column in table.schema.names, schema
    batches = session(0, 3)
    gateway.publish(batches[0], _line_of(batches[0]))
    for b in batches[1:-1]:
        gateway.publish(b, _line_of(b))
    assert live.latest('mbp-10').num_rows == 1, 'the same session reads the day once it begins'
    gateway.close()


def test_a_session_either_streams_or_keeps_quotes_for_latest_and_latest_needs_a_running_session():
    live = brisk.Live()
    with pytest.raises(brisk.BriskError, match='Start the session'):
        live.latest()
    gateway = _gateway.Gateway('XTKS.BRISK')
    try:
        with pytest.raises(ValueError, match='stream=False keeps quotes'):
            live.subscribe('XTKS.BRISK', 'trades', symbols=['7203'], stream=False)
        with pytest.raises(ValueError, match='stream=False keeps quotes'):
            live.subscribe('XTKS.BRISK', 'mbp-10', symbols=['7203'], stream=False, start=0)
        live.subscribe('XTKS.BRISK', 'mbp-10', symbols=['7203'], stream=False)
        with pytest.raises(ValueError, match='either streams'):
            live.subscribe('XTKS.BRISK', 'mbp-10', symbols=['6758'])
        other = brisk.Live()
        other.subscribe('XTKS.BRISK', 'mbp-10', symbols=['7203'])
        with pytest.raises(ValueError, match='either streams'):
            other.subscribe('XTKS.BRISK', 'mbp-10', symbols=['6758'], stream=False)
    finally:
        gateway.close()


def test_a_session_for_more_symbols_than_the_fast_lane_reads_is_warned_and_told_what_to_do():
    batches = session(0, 10)
    with pytest.warns(UserWarning, match=r'3 symbols on the fast lane, which reads 2 at most.*--fast-symbols 3'):
        # opened before the gateway's threads start: on a free-threaded build they inherit its warning context
        gateway = _gateway.Gateway('XTKS.BRISK', on_fast=lambda codes: None)
        gateway.lane({'type': 'lanes', 'fast': True, 'capacity': 2, 'codes': []}, None)
        gateway.publish(batches[0], _line_of(batches[0]))
        live = brisk.Live()
        live.subscribe('XTKS.BRISK', 'mbp-10', symbols=['7203', '6758', '167A'])
        live.start()
        wait_for(lambda: gateway._clients and gateway._clients[0].lane == 'slow')
    gateway.close()


def test_a_session_of_a_thousand_symbols_reads_only_the_lines_that_name_one_of_them():
    codes = [f'{1301 + n}' for n in range(1500)]
    live = brisk.Live()
    live._wanted = frozenset(c.encode() for c in codes)
    mentions = lambda line: any(code in live._wanted for code in _gateway._CODE.findall(line))  # noqa: E731
    wire = lambda value: json.dumps(value, separators=(',', ':')).encode()   # noqa: E731 - as the host writes a line
    assert mentions(wire({'type': 'quotes', 'quotes': [{'code': '9999'}, {'code': '1500'}]}))
    assert not mentions(wire({'type': 'quotes', 'quotes': [{'code': '9999'}, {'code': '8001'}], 'trades': []}))
    assert mentions(wire({'type': 'history', 'trades': [{'code': '2000'}]}))


def test_the_tenths_of_a_yen_arrow_rounds_down_are_those_the_records_say():
    import numpy as np
    import pyarrow.compute as pc
    from briskapi import _gateway, _parquet
    values = list(range(0, 400_000)) + [10**9 + n for n in range(1000)] + [2**53 - 1]
    arrow = pc.cast(_parquet._decimal(np.array(values, dtype=np.int64), 1), pa.float64()).to_pylist()
    assert [_gateway._yen(v) for v in values] == arrow


def test_pyarrows_first_conversion_of_python_objects_is_made_before_a_session_needs_it():
    """It imports pandas (over half a second, once): a Live session's first records were that late for it (_parquet.warm)."""
    import subprocess
    import sys
    code = ("import sys, time; import pyarrow as pa; from briskapi import _parquet; _parquet.warm(); "
            "began = time.perf_counter(); pa.array(['x', 'y'], pa.string()); pa.scalar(0, _parquet.TIME).as_py(); "
            "print(time.perf_counter() - began)")
    after = float(subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, check=True).stdout)
    assert after < 0.2, 'the first conversion after warm() is cheap (cold, it imports pandas: over half a second)'


@pytest.mark.skipif(sys.platform != 'win32', reason='process priority classes are Windows')
def test_the_host_runs_at_above_normal_priority():
    import ctypes
    ABOVE_NORMAL_PRIORITY_CLASS = 0x8000
    kernel32 = ctypes.WinDLL('kernel32')
    kernel32.GetPriorityClass.argtypes = [ctypes.c_void_p]
    feed = _live.Feed(command=['node', '-e', 'setTimeout(() => {}, 5000)'], upload=False)
    try:
        assert kernel32.GetPriorityClass(int(feed._process._handle)) == ABOVE_NORMAL_PRIORITY_CLASS
    finally:
        feed.close()
