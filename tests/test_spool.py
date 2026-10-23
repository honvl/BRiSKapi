"""The spool: a recording kept on disk, in Parquet parts, while it is made (briskapi._spool)."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import briskapi
import briskapi.cli as cli
import briskapi.schema as schema
from briskapi import _live, _native, _parquet, _spool

OPEN_US = 9 * 3600 * 10**6


def session(frames=40, source='brisk_live'):
    """A broker session of two issues: books, trades in every third batch, a history batch, a skipped issue."""
    executed = {0: [0, 0], 1: [0, 0]}   # the trades so far account for each quote's volume and turnover

    def quote(issue, code, i, now):
        return {**dict.fromkeys(schema.QUOTE_KEYS, 0), **{key: [] for key in schema.BOOK_KEYS}, 'issue_id': issue,
                'code': code, 'frame': i + 1, 'max_frame': i + 1, 'source_time_us': now, 'last_price10': 10000 + i,
                'volume': executed[issue][0], 'turnover10': executed[issue][1],
                'bid_levels': [[9990 + i, 100 * (i + 1), 1], [9980, 200, 2]], 'ask_levels': [[10010 + i, 300, 3]]}
    master = [{**dict.fromkeys(schema.MASTER_KEYS, 0), 'issue_id': issue, 'code': code, 'name': name, 'lot_size': 100}
              for issue, code, name in ((0, '7203', 'Toyota'), (1, '6758', 'Sony'))]
    transport = dict(schema.BROKER_TRANSPORTS['sbi'], decoder='/private/decoder.js', ws_host='stream.example',
                     setup_ms=1.5) if schema.broker(source) else {'kind': 'local_cache'}
    batches = [dict(type='bootstrap', seq=0, source=source, trading_date='20260311', source_time_us=OPEN_US,
                    input_transport=transport, source_timestamp_origin='brisk_decoder_unverified', exchange_delay_ms=None,
                    market_issue_count=2, master=list(reversed(master)),
                    quotes=[quote(1, '6758', 0, OPEN_US), quote(0, '7203', 0, OPEN_US)], trades=[]),
               dict(type='history', seq=1, source_time_us=OPEN_US,
                    trades=[dict(issue_id=0, code='7203', frame=1, time_us=OPEN_US - 5, price10=10000, quantity=100, side=1)])]
    for i in range(2, frames + 2):
        now = OPEN_US + i * 100_000
        trades = [dict(issue_id=issue, code=code, frame=i + 1, time_us=now, price10=10000 + i, quantity=100 * k, side=k)
                  for issue, code in ((1, '6758'), (0, '7203')) for k in (1, 2)] if i % 3 == 0 else []
        for t in trades:
            executed[t['issue_id']][0] += t['quantity']
            executed[t['issue_id']][1] += t['price10'] * t['quantity']
        b = dict(type='quotes', seq=i, source_time_us=now, received_unix_ms=1773187200000 + now // 1000, decode_ns=1000 + i,
                 replay_lateness_ms=None, quotes=[quote(1, '6758', i, now), quote(0, '7203', i, now)], trades=trades)
        if i == 7:
            b['quotes'], b['skipped_issues'] = b['quotes'][1:], [1]
        batches.append(b)
    return batches + [dict(type='end', seq=frames + 2, source_time_us=now, frames=frames + 2,
                           quote_updates=2 * frames - 1, replay_wall_ms=4_000.0)]


def lines(batches):
    return [json.dumps(b).encode() + b'\n' for b in batches]


def spooled(tmp_path, batches, part_bytes=1500):
    _spool.PART_BYTES, spool = part_bytes, _spool.Spool(root=tmp_path / 'spool')
    for b, line in zip(batches, lines(batches)):
        spool.add(b, line)
    return spool


@pytest.fixture(autouse=True)
def small_parts(monkeypatch):
    monkeypatch.setattr(_spool, 'PART_BYTES', _spool.PART_BYTES)


def many_parts(tmp_path, count, root=None):
    """A segment of `count` batches spooled a batch to a part, as a day of ten thousand updates a second is (a part every few
    seconds): its bootstrap through Spool.add, the other parts written by brisk-parquet on a few threads at once, and the spool
    reopened as a recovered one is. The paths of the parts, listed, run to more than a Windows command line holds."""
    from concurrent.futures import ThreadPoolExecutor
    batches = session(frames=count - 3)
    _spool.PART_BYTES, spool = 1, _spool.Spool(root=root or tmp_path / ('spool-' + 'x' * 40))
    spool.add(batches[0], lines(batches)[0])
    spool.close_part(wait=True)
    midnight = _parquet.midnight_ns(batches[0]['trading_date'])
    every = lines(batches)
    with ThreadPoolExecutor(8) as pool:
        list(pool.map(lambda n: _native.part([every[n]], spool.path / 'parts' / f'{n + 1:06d}', midnight), range(1, len(batches))))
    spool._writer.shutdown()
    spool = _spool.Spool.reopen(spool.path)
    assert len(spool.parts()) == len(batches) == count and sum(len(str(p)) + 1 for p in spool.parts()) > 32_767
    return spool, batches


def same_tables(left, right, names=_parquet.FILES):
    """Two recordings' tables hold the same rows (aggressor sides as the strings they stand for): the tables are written by
    brisk-parquet and by pyarrow, which do not write the same bytes."""
    plain = lambda table: table.set_column(2, 'aggressor_side', table.column(2).cast(pa.string()))  # noqa: E731
    for name in names:
        a, b = pq.read_table(left / name), pq.read_table(right / name)
        if name == _parquet.TRADES:
            a, b = plain(a), plain(b)
        assert a.equals(b), name


def test_a_spooled_segment_packages_to_the_same_tables_as_its_batches_at_once(tmp_path):
    # The package's summary is counted as it is written, and must be the one the archive's checks give (cli.package's).
    batches = session()
    (tmp_path / 'events.jsonl').write_bytes(b''.join(lines(batches)))
    whole = cli.package(tmp_path / 'events.jsonl', tmp_path / 'whole')
    spool = spooled(tmp_path, batches)
    spool.close_part(wait=True)
    assert len(spool.parts()) > 5 and spool.ended
    parts = spool.package(tmp_path / 'parts')
    assert parts['summary'] == whole['summary']
    same_tables(tmp_path / 'parts', tmp_path / 'whole')
    assert _parquet.inspect_package(tmp_path / 'parts', parts) == parts['summary']   # and the archive takes it
    spool.remove()
    assert not spool.path.exists()


def test_parts_hold_no_connection_diagnostics(tmp_path):
    spool = spooled(tmp_path, session())
    spool.close_part(wait=True)
    for folder in spool.parts():
        text = (folder / _parquet.BATCHES).read_bytes() + (spool.path / _parquet.EQUITY).read_bytes()
        assert b'stream.example' not in text and b'/private/decoder.js' not in text
    first = _parquet.pq.read_table(spool.parts()[0] / _parquet.BATCHES).column('input_transport')[0].as_py()
    assert json.loads(first) == schema.BROKER_TRANSPORTS['sbi']


@pytest.mark.parametrize('count', [0, 1, 7, 30, 43])
def test_a_replay_gives_exactly_the_first_lines(tmp_path, count):
    batches = session()
    spool = spooled(tmp_path, batches)
    canonical = [schema.canonical(b) for b in schema.canonical_batches(__import__('io').BytesIO(b''.join(lines(batches))))]
    replay = spool.replay(count)
    got = [json.loads(line) for line in replay]
    assert len(got) == count
    # Written parts come back canonical; lines not yet written come as the decoder wrote them.
    assert [schema.canonical(next(schema.canonical_batches(__import__('io').BytesIO(json.dumps(b).encode()))))
            for b in got] == canonical[:count]


def test_a_replay_keeps_the_spool_until_it_is_read(tmp_path):
    spool = spooled(tmp_path, session())
    spool.close_part(wait=True)
    replay = spool.replay(spool.count)
    spool.remove()
    assert spool.path.exists(), 'still being replayed'
    assert len(list(replay)) == spool.count
    assert not spool.path.exists()
    unread = spooled(tmp_path, session())
    unread.replay(3).close()
    unread.remove()
    assert not unread.path.exists()


def dead_pid():
    return int(subprocess.run([sys.executable, '-c', 'import os; print(os.getpid())'], capture_output=True, text=True).stdout)


def test_spool_files_can_be_flushed_to_disk(tmp_path):
    path = tmp_path / 'part'
    path.write_bytes(b'recording')
    _spool._fsync(path)
    assert path.read_bytes() == b'recording'


def test_process_probe_leaves_a_running_process_alive():
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    try:
        assert _spool._alive(child.pid)
        with pytest.raises(subprocess.TimeoutExpired):
            child.wait(timeout=0.1)
    finally:
        child.kill()
        child.wait()
    assert not _spool._alive(child.pid)


def test_leftovers_are_crashed_broker_spools_with_their_sessions_choices_and_a_demos_dropped(tmp_path):
    root, intent = tmp_path / 'spool', {'upload': True, 'output': str(tmp_path / 'days')}
    for source in ('brisk_live', 'historical_mock'):
        _spool.PART_BYTES, spool = 1500, _spool.Spool(root=root, intent=intent)
        for b, line in zip(session(source=source)[:25], lines(session(source=source)[:25])):
            spool.add(b, line)
        spool.close_part(wait=True)
        spool._writer.shutdown()
    for owner in root.glob('segment-*/owner.json'):
        owner.write_text(json.dumps({**json.loads(owner.read_text()), 'pid': dead_pid()}))
    live = _spool.Spool(root=root)   # a running process's own: left alone
    live.add(session()[0], lines(session())[0])
    (live.path / 'owner.json').write_text(json.dumps({'pid': os.getppid(), 'started': 0}))
    found, = _spool.leftovers(root)
    assert found.intent == intent and len(found.parts()) > 3 and repr(found).startswith('Spool(')
    assert sorted(p.name for p in root.iterdir()) == sorted([found.path.name, live.path.name]), 'the demo\'s is gone'
    m = found.package(tmp_path / 'package', repair=True)
    assert m['summary']['batches'] == 26 and m['summary']['source'] == 'brisk_live'
    recording = briskapi.Recording(tmp_path / 'package')
    assert len(briskapi.Market(recording).trades()) == 1 + 4 * 8   # the history's, and batches 3, 6, ... 24


def test_a_recovered_spool_is_named_and_reports_its_packaging_like_a_running_one(tmp_path):
    # A reopened spool has no header in memory: its site, day and the piece's start come from its first part.
    root = tmp_path / 'spool'
    _spool.PART_BYTES, spool = 1500, _spool.Spool(root=root, intent={'upload': False, 'output': str(tmp_path / 'days')})
    for b, line in zip(session()[:25], lines(session()[:25])):
        spool.add(b, line)
    spool.close_part(wait=True)
    spool._writer.shutdown()
    (spool.path / 'owner.json').write_text(json.dumps({**json.loads((spool.path / 'owner.json').read_text()), 'pid': dead_pid()}))
    found, = _spool.leftovers(root)
    assert found.header is None and found.piece_start is None
    out = io.StringIO()
    saving = cli.Saving(lambda: _live._saving_label(found), 0, out)
    found.package(tmp_path / 'package', repair=True, progress=saving.packaging)
    told = [line.split(': ', 1)[1] for line in out.getvalue().splitlines()]
    assert out.getvalue().startswith('[brisk] sbi 2026-03-11 from 09:00:00: preparing to save') and told[-1].startswith('finalizing')
    assert any(line.startswith('writing quotes') for line in told)


@pytest.mark.parametrize('cut', [1, 2, 10, 25, 41])
def test_a_repaired_end_is_one_the_archive_accepts(tmp_path, cut):
    batches = session()[:cut]
    spool = spooled(tmp_path, batches)
    m = spool.package(tmp_path / 'package', repair=True)
    _parquet.inspect_package(tmp_path / 'package', m)
    end = _parquet._decode_batches(_parquet.pq.read_table(tmp_path / 'package' / _parquet.BATCHES))[-1]
    quotes = [b for b in batches if b['type'] == 'quotes']
    received = [b['received_unix_ms'] for b in quotes]
    assert end == {'type': 'end', 'seq': cut, 'source_time_us': batches[-1]['source_time_us'], 'frames': cut - 1,
                   'quote_updates': sum(len(b['quotes']) for b in quotes),
                   'replay_wall_ms': float(received[-1] - received[0]) if received else 0.0}
    # One that ended needs no repair.
    whole = spooled(tmp_path / 'whole', session())
    whole.close_part(wait=True)
    assert whole.ship(tmp_path / 'ended', final=True, repair=True)[1] is False


def test_a_damaged_spool_is_left_for_a_look(tmp_path):
    spool = spooled(tmp_path, session()[:10])
    spool.close_part(wait=True)
    (spool.path / 'owner.json').write_text('not json')
    (spool.path / _parquet.EQUITY).unlink()
    with pytest.warns(UserWarning, match='Could not recover'):
        assert _spool.leftovers(tmp_path / 'spool') == []
    assert spool.path.exists(), 'kept for a look, not deleted'


def test_a_failed_part_write_stops_the_feed(tmp_path, monkeypatch):
    monkeypatch.setattr(_spool._native, 'part', lambda *a, **k: 1 / 0)
    _spool.PART_BYTES, spool = 1, _spool.Spool(root=tmp_path / 'spool')
    batches = session()
    spool.add(batches[0], lines(batches)[0])
    spool._writer.submit(lambda: None).result()
    with pytest.raises(ZeroDivisionError):
        spool.add(batches[1], lines(batches)[1])
    with pytest.raises(ZeroDivisionError):
        spool.close_part(wait=True)


def test_parts_close_on_time_too(tmp_path, monkeypatch):
    monkeypatch.setattr(_spool, 'PART_SECONDS', 0)
    spool = spooled(tmp_path, session()[:5], part_bytes=10**9)
    spool.close_part(wait=True)
    assert len(spool.parts()) == 5
    assert repr(spool).startswith('Spool(') and _spool.data_dir().name == 'brisk'


def _pieces(spool, tmp_path, final=True, repair=False):
    out = []
    while (shipped := spool.ship(tmp_path / f'piece{len(out)}', final=final, repair=repair)) is not None:
        out.append((tmp_path / f'piece{len(out)}', *shipped))
    return out


def _content(directory):
    """A recording's quotes (issue, frame, values) and trades, whatever its batches are numbered."""
    columns = _parquet.Columns.load(directory)
    quotes = {(q['issue_id'], q['frame']): q for b in columns.iter_batches() if b['type'] != 'bootstrap'
              for q in b.get('quotes', ())}
    trades = [(t['issue_id'], t['frame'], t['quantity']) for b in columns.iter_batches() for t in b.get('trades', ())]
    return quotes, trades


@pytest.mark.parametrize('rows', [1, 6, 15])
def test_a_day_ships_in_pieces_each_a_recording_the_archive_accepts_and_together_the_day(tmp_path, monkeypatch, rows):
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', rows)
    batches = session()
    # Issue 1's trades in batch 9 come without its quote (unreadable there): a piece may end there all the same.
    batches[9]['quotes'], batches[9]['skipped_issues'] = batches[9]['quotes'][1:], [1]
    batches[-1]['quote_updates'] -= 1
    (tmp_path / 'events.jsonl').write_bytes(b''.join(lines(batches)))
    whole = cli.package(tmp_path / 'events.jsonl', tmp_path / 'whole')
    spool = spooled(tmp_path, batches)
    pieces = _pieces(spool, tmp_path)
    assert len(pieces) > 1 and [repaired for *_, repaired in pieces] == [False] * len(pieces)
    for directory, manifest, _ in pieces:
        assert _parquet.inspect_package(directory, manifest) == manifest['summary']   # the archive's own checks
    quotes, trades = {}, []
    for directory, *_ in pieces:
        q, t = _content(directory)
        assert not set(q) & set(quotes), 'no quote twice'
        quotes.update(q)
        trades += t
    day_quotes, day_trades = _content(tmp_path / 'whole')
    assert quotes == day_quotes and sorted(trades) == sorted(day_trades)
    firsts = [m['summary']['first_source_time_us'] for _, m, _ in pieces]
    lasts = [m['summary']['last_source_time_us'] for _, m, _ in pieces]
    assert firsts[0] == whole['summary']['first_source_time_us'] and lasts[-1] == whole['summary']['last_source_time_us']
    assert firsts[1:] == lasts[:-1], 'each piece starts where the one before ended'
    assert spool.ship(tmp_path / 'more', final=True) is None


def _afternoon(batches, at):
    """The session with every batch from `at` on after the lunch break (three and a half hours later)."""
    later = 12_600 * 10**6
    out = []
    for i, b in enumerate(json.loads(json.dumps(batches))):
        if i >= at:
            b['source_time_us'] += later
            if 'received_unix_ms' in b:
                b['received_unix_ms'] += later // 1000
            for q in b.get('quotes', ()):
                q['source_time_us'] += later
            for t in b.get('trades', ()):
                t['time_us'] += later
        out.append(b)
    return out


def test_a_day_ships_its_morning_once_a_part_closes_after_noon_and_its_afternoon_at_its_end(tmp_path, monkeypatch):
    monkeypatch.setattr(_spool, 'LUNCH_US', OPEN_US + 3 * 3600 * 10**6)   # the session's clocks start at OPEN_US
    batches = _afternoon(session(), 20)
    spool = spooled(tmp_path, batches[:24])   # the day runs on past noon: the morning ships
    spool.close_part(wait=True)
    morning = spool.ship(tmp_path / 'morning')
    assert morning and morning[0]['summary']['last_source_time_us'] >= _spool.LUNCH_US > morning[0]['summary']['first_source_time_us']
    assert spool.piece_start == morning[0]['summary']['first_source_time_us']
    assert spool.ship(tmp_path / 'again') is None, 'the afternoon waits for the day\'s end'
    for b, line in zip(batches[24:], lines(batches[24:])):
        spool.add(b, line)
    afternoon = spool.ship(tmp_path / 'afternoon', final=True)
    assert afternoon and afternoon[0]['summary']['first_source_time_us'] == morning[0]['summary']['last_source_time_us']
    assert spool.piece_start == afternoon[0]['summary']['first_source_time_us'], 'a piece is named for where it begins'
    for directory, (manifest, _) in ((tmp_path / 'morning', morning), (tmp_path / 'afternoon', afternoon)):
        assert _parquet.inspect_package(directory, manifest) == manifest['summary']
    assert spool.ship(tmp_path / 'more', final=True) is None


def _reconnection(at):
    """What a feed's reconnection in the lunch break records before its segment ends: the book, the day's history, no update."""
    bootstrap, history, end = session()[:2] + session()[-1:]
    for b in (bootstrap, history, end):
        b['source_time_us'] += at - OPEN_US
    end['seq'], end['frames'], end['quote_updates'] = 2, 0, 0
    return [bootstrap, history, end]


def test_a_piece_the_stream_sent_no_update_to_is_not_made(tmp_path):
    at = 11 * 3600 * 10**6 + 2700 * 10**6   # 11:45
    spool = spooled(tmp_path, _reconnection(at))
    assert spool.ship(tmp_path / 'piece', final=True) is None
    assert not (tmp_path / 'piece').exists(), 'nothing was packaged'
    assert spool.skipped == [(at, at + 4_100_000)], 'from the book to the end'
    assert spool.shipped == len(spool.parts()), 'its parts are not tried again'
    assert spool.ship(tmp_path / 'again', final=True) is None and len(spool.skipped) == 1


def test_a_skipped_piece_does_not_stop_the_pieces_after_it_nor_cost_them_what_the_day_held(tmp_path, monkeypatch):
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', 1)   # the bootstrap's part alone is a piece, and the stream sent it nothing
    batches = session()
    spool = spooled(tmp_path, batches)
    pieces = _pieces(spool, tmp_path)
    # The bootstrap's part and, last, the end's alone (the segment's end goes with the rest only if some is left).
    assert [first for first, _ in spool.skipped] == [OPEN_US, OPEN_US + 41 * 100_000] and len(pieces) > 1
    quotes, trades = {}, []
    for directory, manifest, _ in pieces:
        assert _parquet.inspect_package(directory, manifest) == manifest['summary']   # the archive's own checks
        q, t = _content(directory)
        quotes.update(q)
        trades += t
    (tmp_path / 'events.jsonl').write_bytes(b''.join(lines(batches)))
    cli.package(tmp_path / 'events.jsonl', tmp_path / 'whole')
    day_quotes, day_trades = _content(tmp_path / 'whole')
    assert quotes == day_quotes and sorted(trades) == sorted(day_trades), 'what the stream sent is all there'


def _only(batch, codes):
    """A replayed batch with only these symbols' quotes and trades, or None when a quotes or history batch is left with none."""
    out = dict(batch)
    for name in ('quotes', 'trades'):
        if name in out:
            out[name] = [item for item in out[name] if item['code'] in codes]
    if out['type'] in ('quotes', 'history') and not out.get('quotes') and not out.get('trades'):
        return None
    return out


@pytest.mark.parametrize('count', [1, 5, 17, 30, 10**6])
@pytest.mark.parametrize('codes', [{'7203'}, {'6758'}, {'7203', '6758'}, {'0000'}])
def test_a_replay_of_some_symbols_is_the_replay_of_all_of_them_without_the_others(tmp_path, codes, count):
    batches = session()
    spool = spooled(tmp_path, batches[:-6])   # a day under way: closed parts, and lines not yet written
    spool._writer.submit(lambda: None).result()
    _spool.PART_BYTES = 10**9
    for b, line in zip(batches[-6:-1], lines(batches[-6:-1])):
        spool.add(b, line)
    assert spool.parts() and spool._lines
    whole = [json.loads(line) for line in spool.replay(count)]
    some = [json.loads(line) for line in spool.replay(count, codes)]
    assert all(b['seq'] < count for b in some)
    kept = [b for b in (_only(b, codes) for b in whole) if b is not None]
    assert [b for b in (_only(b, codes) for b in some) if b is not None] == kept, 'what these symbols have is all there, in order'
    closed = spool._parts[-1][2] if spool._parts else 0
    for b in some:   # what was read from closed parts has no other symbol's rows, and no batch without any
        if b['seq'] < closed and b['type'] in ('quotes', 'history'):
            assert {i['code'] for name in ('quotes', 'trades') for i in b.get(name, ())} <= codes
            assert b.get('quotes') or b.get('trades')
    if count >= closed and codes == {'0000'}:
        assert len([b for b in some if b['seq'] < closed]) <= 1, 'only the bootstrap of a day nobody asked about'


def test_a_replay_of_a_few_symbols_reads_the_rows_of_those_alone(tmp_path, monkeypatch):
    batches = session()
    spool = spooled(tmp_path, batches[:-1])
    spool._writer.submit(lambda: None).result()
    seen = []
    real = pq.read_table

    def spy(path, *args, **kwargs):
        seen.append((Path(path).as_posix().split('/parts/', 1)[-1].split('/', 1)[-1], kwargs.get('filters')))
        return real(path, *args, **kwargs)
    monkeypatch.setattr(_parquet.pq, 'read_table', spy)
    list(spool.replay(10**6, {'7203'}))
    ids = {m['issue_id'] for m in spool.master if m['code'] == '7203'}
    rows = [f for name, f in seen if name in (_parquet.DEPTHS, _parquet.TRADES)]
    assert rows and all(f == [('issue_id', 'in', sorted(ids))] for f in rows), 'a part\'s other issues are not read'


def test_a_recordings_bootstrap_is_read_without_decoding_its_batches(tmp_path, monkeypatch):
    # A day has millions of batches: decoding them all into dicts to look at the first took seconds and gigabytes of the feed's.
    (tmp_path / 'events.jsonl').write_bytes(b''.join(lines(session())))
    cli.package(tmp_path / 'events.jsonl', tmp_path / 'whole')
    wanted = _parquet.bootstrap_row(tmp_path / 'whole')
    assert wanted['type'] == 'bootstrap' and wanted['trading_date'] == '20260311' and wanted['input_transport']['kind']
    decoded, real = [], _parquet._decode_batches
    monkeypatch.setattr(_parquet, '_decode_batches', lambda table, midnight=None: decoded.append(table.num_rows) or real(table, midnight))
    assert _parquet.bootstrap_row(tmp_path / 'whole') == wanted
    assert decoded == [1], 'one row'
    empty = tmp_path / 'empty'
    (empty / _parquet.BATCHES).parent.mkdir(parents=True)
    pq.write_table(_parquet.BATCH_SCHEMA.empty_table(), empty / _parquet.BATCHES)
    with pytest.raises(Exception, match='no bootstrap'):
        _parquet.bootstrap_row(empty)


def test_a_segment_of_hundreds_of_parts_ships_and_is_kept_whatever_a_command_line_holds(tmp_path, monkeypatch):
    # A day's parts (1,670 by 11:11 on a 135-million-update day) listed as the program's arguments did not start it on
    # Windows (WinError 206, 32,767 characters), and the day was lost; they go on its standard input.
    spool, batches = many_parts(tmp_path, 500)
    commands = []
    real = _native.subprocess.Popen
    monkeypatch.setattr(_native.subprocess, 'Popen', lambda command, **options: commands.append(command) or real(command, **options))
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', 600)   # the first piece some 300 parts, the second after their cut
    pieces = _pieces(spool, tmp_path, final=True)
    assert len(pieces) == 2 and spool.shipped == 500
    for directory, manifest, _ in pieces:
        assert _parquet.inspect_package(directory, manifest) == manifest['summary']
    quotes = {k for d, *_ in pieces for k in _content(d)[0]}
    assert len(quotes) == sum(len(b.get('quotes', ())) for b in batches[1:])
    assert len(commands) >= 2 and all(sum(len(str(word)) + 1 for word in command) < 2000 for command in commands), \
        'the parts are not on the command line, whatever their number'
    assert not any(str(spool.parts()[-1]) in map(str, command) for command in commands)
    kept = spool.keep(tmp_path / 'kept')
    assert kept is not None and (kept / _parquet.DEPTHS).exists()


def _not_started(*arguments, **options):
    raise OSError(206, 'The filename or extension is too long')   # Windows, of a command line beyond 32,767 characters


def test_a_piece_that_could_not_be_packaged_is_tried_again_later_from_the_same_parts(tmp_path, monkeypatch):
    # The parts of a piece whose packaging failed before the program ran (or in it) stay unshipped, so nothing of the day is
    # lost; the piece is tried again, but after a while, a few times, and once more at the day's end: a day that failed at
    # every part written (every 0.7 s, 22,000 times) closed a part each time and never saved anything.
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', 8)
    batches = session()[:30]
    spool = spooled(tmp_path, batches)
    spool.close_part(wait=True)
    asked, real = [], _native.package
    monkeypatch.setattr(_native, 'package', lambda *a, **k: asked.append(a[1]) or _not_started())
    with pytest.raises(OSError):
        spool.ship(tmp_path / 'first')
    assert spool.shipped == 0 and spool.attempts == 1 and spool.last_parts == asked[0] and spool.piece_start == OPEN_US
    assert json.loads((spool.path / 'shipped.json').read_text())['parts'] == 0 if (spool.path / 'shipped.json').exists() else True
    assert spool.ship(tmp_path / 'again') is None and len(asked) == 1, 'not at the next part written: after RETRY_SECONDS'
    for attempt in range(2, _spool.MAX_ATTEMPTS + 1):
        spool._failed_at -= _spool.RETRY_SECONDS
        with pytest.raises(OSError):
            spool.ship(tmp_path / 'again')
        assert asked[-1] == asked[0] and spool.shipped == 0 and spool.attempts == attempt, 'the same piece, from the bootstrap'
    spool._failed_at -= _spool.RETRY_SECONDS
    assert spool.ship(tmp_path / 'again') is None and len(asked) == _spool.MAX_ATTEMPTS, 'tried enough for one day'
    monkeypatch.setattr(_native, 'package', real)
    pieces = _pieces(spool, tmp_path, final=True, repair=True)   # the day's end tries once more, and the program is back
    assert len(pieces) > 1 and spool.attempts == 0 and spool._failed_at is None and spool.shipped == len(spool.parts())
    assert pieces[0][1]['summary']['first_source_time_us'] == OPEN_US
    quotes = {k for d, *_ in pieces for k in _content(d)[0]}
    assert len(quotes) == sum(len(b.get('quotes', ())) for b in batches[1:]), 'nothing of the day is lost'


def test_a_refused_piece_is_not_tried_again_and_its_tables_are_kept(tmp_path, monkeypatch):
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', 8)
    batches = session()[:30]
    spool = spooled(tmp_path, batches)
    spool.close_part(wait=True)
    real = cli.package_parts
    monkeypatch.setattr(cli, 'package_parts', lambda *a, **k: schema.require(False, 'The package is 999 MiB; the archive accepts up to 1 MiB'))
    with pytest.raises(ValueError, match='archive accepts'):
        spool.ship(tmp_path / 'first')
    tried = spool.shipped
    assert tried == len(spool.last_parts) > 0 and spool.attempts == 1
    kept = spool.keep(tmp_path / 'kept')
    assert kept is not None and (kept / _parquet.DEPTHS).exists() and spool.shipped == tried
    monkeypatch.setattr(cli, 'package_parts', real)
    rest = _pieces(spool, tmp_path, final=True, repair=True)
    assert rest and rest[0][1]['summary']['first_source_time_us'] == _parquet.summarize(kept)['last_source_time_us'], \
        'the next piece starts where the refused one ended'
    quotes = {k for d in [kept, *(d for d, *_ in rest)] for k in _content(d)[0]}
    assert len(quotes) == sum(len(b.get('quotes', ())) for b in batches[1:])


def test_a_piece_whose_cut_cannot_be_made_is_kept_from_the_days_bootstrap(tmp_path, monkeypatch):
    # Where the shipped pieces left the book comes from their parts (brisk-parquet state); when that fails, what is kept is the
    # day from its bootstrap through the piece, the shipped morning included: more than once, never less.
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', 8)
    batches = session()[:30]
    spool = spooled(tmp_path, batches)
    spool.close_part(wait=True)
    first = spool.ship(tmp_path / 'first')
    assert first is not None
    spool._writer.shutdown()
    again = _spool.Spool.reopen(spool.path)   # as after a crash: the book where the morning left it is not in memory
    monkeypatch.setattr(_native, 'state', _not_started)
    with pytest.raises(OSError):
        again.ship(tmp_path / 'second', final=True, repair=True)
    assert again.shipped == spool.shipped and again.last_parts and again.piece_start is None
    kept = again.keep(tmp_path / 'kept')
    assert kept is not None and again.shipped == again.parts().index(again.last_parts[-1]) + 1
    bootstrap = _parquet.bootstrap_row(kept)
    assert bootstrap['source_time_us'] == OPEN_US and bootstrap['seq'] == 0, 'the day\'s own bootstrap, not a cut\'s'
    quotes = _content(kept)[0]
    assert set(_content(tmp_path / 'first')[0]) < set(quotes)
    assert len(quotes) == sum(len(b.get('quotes', ())) for b in batches[1:]) if again.shipped == len(again.parts()) else True


def test_keeping_a_running_days_piece_leaves_its_open_part_open_and_a_failed_one_no_staging(tmp_path, monkeypatch):
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', 8)
    batches = session()
    spool = spooled(tmp_path, batches[:25])
    spool.close_part(wait=True)
    _spool.PART_BYTES = 10**9
    spool.add(batches[25], lines(batches)[25])   # a part still open
    monkeypatch.setattr(cli, 'package_parts', lambda *a, **k: 1 / 0)
    with pytest.raises(ZeroDivisionError):
        spool.ship(tmp_path / 'first')
    closed = len(spool.parts())
    kept = spool.keep(tmp_path / 'kept')
    assert kept is not None and len(spool.parts()) == closed and spool._lines, 'the open part goes on filling'
    staging = lambda: [d.name for d in spool.path.iterdir() if d.name.startswith('tmp')]  # noqa: E731
    assert staging() == []
    monkeypatch.setattr(_native, 'package', lambda *a, **k: 1 // 0)
    with pytest.raises(ZeroDivisionError):
        spool.keep(tmp_path / 'kept2')
    assert staging() == []
    assert _spool.Spool.reopen(spool.path).keep(tmp_path / 'kept3') is None, 'its parts are kept: nothing is left to keep'


def test_a_crashed_spool_ships_what_is_left_from_where_it_was(tmp_path, monkeypatch):
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', 8)
    batches = session()[:30]   # cut short: no end
    spool = spooled(tmp_path, batches)
    spool.close_part(wait=True)
    first = spool.ship(tmp_path / 'first')
    assert first is not None and json.loads((spool.path / 'shipped.json').read_text())['parts'] == len(spool.last_parts)
    spool._writer.shutdown()
    again = _spool.Spool.reopen(spool.path)
    rest = _pieces(again, tmp_path, repair=True)
    assert rest and rest[-1][2] is True, 'the last piece repaired with the end its host never wrote'
    for directory, manifest, _ in rest:
        assert _parquet.inspect_package(directory, manifest) == manifest['summary']
    shipped = {k for d in [tmp_path / 'first', *(d for d, *_ in rest)] for k in _content(d)[0]}
    assert len(shipped) == sum(len(b.get('quotes', ())) for b in batches[1:])
