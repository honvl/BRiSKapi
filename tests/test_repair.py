"""Comparing two recordings of one day, and repairing one from the other (briskapi._repair)."""
import hashlib
import json
from pathlib import Path

import numpy as np
import pytest

import briskapi.cli as cli
import briskapi.schema as schema
from briskapi import _parquet, _repair

from test_spool import with_book_changes, lines

OPEN_US = 9 * 3600 * 10**6
TICK = 100_000
ISSUES = ((0, '7203'), (1, '6758'), (2, '9984'))


def clock(frame):
    return OPEN_US + frame * TICK


def updates(frame):
    """The securities that change in a frame of the market: the first's every second, the second's every third, the third's every
    fourth, and all of them in frame 0."""
    return [(i, code) for i, code in ISSUES if frame == 0 or frame % (i + 2) == 0]


def quote(issue, code, frame, price=0, offset=0):
    return {**dict.fromkeys(schema.QUOTE_KEYS, 0), **{key: [] for key in schema.BOOK_KEYS}, 'issue_id': issue, 'code': code,
            'frame': frame + offset, 'max_frame': frame + offset, 'source_time_us': clock(frame), 'last_price10': 10_000 + 7 * frame + issue + price,
            'bid_levels': [[9990 + frame, 100 * (frame + 1), 1], [9980, 200, 2]], 'ask_levels': [[10010 + frame, 300, 3]]}


def trades(frame, offset=0):
    """Two trades of the first security in every fifth frame."""
    return [dict(issue_id=0, code='7203', frame=frame + offset, time_us=clock(frame), price10=10_000 + frame, quantity=100 * k, side=k)
            for k in (1, 2)] if frame and frame % 5 == 0 else []


def feed(first, last, drop=(), received=0, price=None, site='sbi', issues=ISSUES, offset=0):
    """One feed's batches of the market from the frame after `first` (its book as of `first`, and the day's trades so far)
    to `last`, but for the frames it lost. `price` {(issue, frame): amount} makes it see a price differently; `issues`
    are the securities it records; `offset` numbers its frames differently."""
    price = price or {}
    master = [{**dict.fromkeys(schema.MASTER_KEYS, 0), 'issue_id': i, 'code': code, 'name': code, 'lot_size': 100} for i, code in issues]
    book = [quote(i, code, f, price.get((i, f), 0), offset) for i, code in issues
            for f in [max(f for f in range(first + 1) if (i, code) in updates(f))]]
    batches = [dict(type='bootstrap', seq=0, source='brisk_live', trading_date='20260311', source_time_us=clock(first),
                    input_transport=dict(schema.BROKER_TRANSPORTS[site]), source_timestamp_origin='brisk_decoder_unverified',
                    exchange_delay_ms=None, market_issue_count=len(issues), master=master, quotes=book, trades=[])]
    past = [t for f in range(first + 1) for t in trades(f, offset) if (t['issue_id'], t['code']) in issues]
    if past:
        batches.append(dict(type='history', seq=1, source_time_us=clock(first), trades=past))
    count = 0
    for f in range(first + 1, last + 1):
        if f in drop:
            continue
        rows = [quote(i, code, f, price.get((i, f), 0), offset) for i, code in updates(f) if (i, code) in issues]
        count += len(rows)
        batches.append(dict(type='quotes', seq=len(batches), source_time_us=clock(f),
                            received_unix_ms=1773187200000 + clock(f) // 1000 + received, decode_ns=1000 + f,
                            replay_lateness_ms=None, quotes=rows,
                            trades=[t for t in trades(f, offset) if (t['issue_id'], t['code']) in issues]))
    deltas = sum(b['type'] == 'quotes' for b in batches)
    return batches + [dict(type='end', seq=len(batches), source_time_us=clock(last), frames=deltas, quote_updates=count,
                           replay_wall_ms=1000.0)]


def package(tmp_path, name, *args, spoil=None, **options):
    """A feed as a package; `spoil(batches)` changes what it saw first."""
    batches = feed(*args, **options)
    if spoil:
        spoil(batches)
    (tmp_path / f'{name}.jsonl').write_bytes(b''.join(lines(batches)))
    cli.package(tmp_path / f'{name}.jsonl', tmp_path / name)
    return tmp_path / name


def rows(directory):
    """A recording's quote updates by (security, frame), and its trades by (security, frame, place)."""
    columns = _parquet.Columns.load(directory)
    quotes = {(q['issue_id'], q['frame']): q for q in columns.quotes(np.arange(len(columns)))}
    return quotes, {(t['issue_id'], t['frame'], i): t for i, t in enumerate(columns.trades_at(np.arange(len(columns.t_code))))}, columns


def fingerprint(directory):
    return {name: hashlib.sha256((directory / name).read_bytes()).hexdigest() for name in _parquet.FILES}


def forward(columns):
    """Every security's updates run forward in the recording, in every order a query puts them in: one to a batch, the batches in
    order, and sorted by receipt time, then market time, then frame (the order of a receipt-time query), the frames still go up."""
    q = columns.q
    received = np.array([b.get('received_unix_ms') or 0 for b in columns.batches])
    clocks = np.array([b['source_time_us'] for b in columns.batches])
    assert (np.diff(clocks) >= 0).all() and (clocks[q['seq']] >= q['source_time_us']).all()
    for issue in set(q['issue_id'].tolist()):
        rows = np.flatnonzero(q['issue_id'] == issue)
        assert (np.diff(q['seq'][rows]) > 0).all() and (np.diff(q['frame'][rows]) >= 0).all()
        order = np.lexsort((q['frame'][rows], q['source_time_us'][rows], received[q['seq'][rows]]))
        assert (np.diff(q['frame'][rows][order]) >= 0).all(), f'security {issue} runs backwards in receipt order'


def repair(a, b, output=None, **options):
    """`cli.repair` of one recording from one other: its entry with the one source's comparison merged in, as a report."""
    entry = cli.repair([a], [b], output, **options)['recordings'][0]
    return {**{k: v for k, v in entry.items() if k != 'sources'}, **entry['sources'][0]}


def lost(frames, first=0):
    """The quote updates and trades the frames hold."""
    return sum(len(updates(f)) for f in frames), sum(len(trades(f)) for f in frames)


@pytest.fixture(params=[_parquet.ROW_GROUP, 64], ids=['one row group', 'many row groups'])
def group(request, monkeypatch):
    """Recordings whose tables have a few rows to a group, so that a security's rows run over groups and repairs rewrite them."""
    monkeypatch.setattr(_parquet, 'ROW_GROUP', request.param)
    monkeypatch.setattr(_repair, 'ROW_GROUP', request.param)
    return request.param


def test_two_feeds_of_a_whole_day_are_alike_row_for_row_and_block_for_block(tmp_path, group):
    a, b = package(tmp_path, 'a', 0, 90), package(tmp_path, 'b', 0, 90, received=7, site='e-shiten')
    report = repair(a, b)
    assert report['recording']['site'] == 'sbi' and report['other']['site'] == 'e-shiten'
    for kind in ('quotes', 'trades'):
        for side in 'ab':
            rows_ = report[kind]['rows'][side]
            assert rows_['same'] == sum(rows_.values()) > 0
        assert report[kind]['rows_in_identical_blocks_pct'] == 100.0
    assert report['repair']['quotes'] == report['repair']['trades'] == report['repair']['batches'] == 0


def test_frames_a_feed_lost_are_added_from_the_other_with_its_receipt_time(tmp_path, group):
    gone = {20, 21, 22, 40, 41}
    a, b, whole = package(tmp_path, 'a', 0, 80, drop=gone), package(tmp_path, 'b', 0, 80, received=7), package(tmp_path, 'whole', 0, 80)
    before = fingerprint(a)
    report = repair(a, b, tmp_path / 'repaired')
    assert fingerprint(a) == before   # the recording itself is never touched
    quotes, trade_rows = lost(gone)
    assert (report['repair']['quotes'], report['repair']['trades']) == (quotes, trade_rows) == (6, 4)
    # A frame that changed nothing (41) left no batch to be missed: only the four with updates or trades come back.
    held = sorted(f for f in gone if updates(f) or trades(f))
    assert held == [20, 21, 22, 40]
    # A lost update goes in a batch of its own just before the batch of its security's next update (the same place, the same batch),
    # and a frame's trades in the other's own batch: 7203's updates of frames 20 and 22 and 9984's and 6758's of 20 and 21 come before
    # 24's batch (two batches, the second for 7203's second), 7203's of 40 before 42's and 9984's before 44's: four batches for the six
    # updates, and a batch each for the trades of frames 20 and 40.
    assert (report['repair']['borrowed_receipt'], report['repair']['batches']) == (6, 6)
    assert report['repair']['left_out'] == dict.fromkeys(('misplaced', 'after_end', 'restored', 'not_recorded'), 0)
    assert report['quotes']['rows']['b']['only'] == quotes and report['quotes']['rows']['a']['only'] == 0
    assert report['quotes']['blocks']['a_lacks'] >= 1 and report['quotes']['blocks']['conflict'] == 0
    # What it holds is now what a feed that lost nothing holds, ...
    got, expected = rows(tmp_path / 'repaired'), rows(whole)
    assert got[0] == expected[0] and got[1] == expected[1]
    # ... in batches that are the archive's to verify, in which every security runs forward.
    columns = got[2]
    assert [b['seq'] for b in columns.batches] == list(range(len(columns.batches))) == list(range(len(expected[2].batches) + 1))   # 5 lost batches, 6 made
    forward(columns)
    # An update's receipt time is that of the next update of its security the recording holds.
    q = columns.q
    receipt = {int(b['seq']): b['received_unix_ms'] for b in columns.batches if b['type'] == 'quotes'}
    at = lambda issue, frame: receipt[int(q['seq'][(q['issue_id'] == issue) & (q['frame'] == frame)][0])]
    for issue, frame, next_frame in ((0, 20, 24), (0, 22, 24), (2, 20, 24), (1, 21, 24), (0, 40, 42), (2, 40, 44)):
        assert at(issue, frame) == at(issue, next_frame) == 1773187200000 + clock(next_frame) // 1000
    # Its end counts what it holds: the frames it received and the ones it was given, and every update.
    end, whole_end = columns.batches[-1], expected[2].batches[-1]
    assert end['frames'] == whole_end['frames'] - 5 + report['repair']['batches'] and end['quote_updates'] == whole_end['quote_updates']
    assert {k: v for k, v in end.items() if k not in ('frames', 'seq')} == {k: v for k, v in whole_end.items() if k not in ('frames', 'seq')}
    assert report['package']['summary'] == _parquet.verify_broker(tmp_path / 'repaired')
    assert report['package']['summary']['quote_updates'] == expected[2].batches[-1]['quote_updates'] + len(ISSUES)   # and the opening book
    assert json.loads((tmp_path / 'repaired' / 'manifest.json').read_text()) == report['package']


def test_a_repair_of_recordings_with_book_changes_writes_each_securitys_changes_again(tmp_path, group, checkpoints):
    # Each recording's book changes are from its own previous update: a lost update added from the other feed, and the next of
    # its security, get theirs again from the books, and the checkpoints are where the rule puts them (verify replays both).
    gone = {20, 21, 22, 40, 41}
    a, b, whole = (package(tmp_path, name, 0, 80, spoil=with_book_changes, **options)
                   for name, options in (('a', {'drop': gone}), ('b', {'received': 7}), ('whole', {})))
    report = repair(a, b, tmp_path / 'repaired')
    assert report['repair']['quotes'] == 6
    assert report['package']['summary'] == _parquet.verify_broker(tmp_path / 'repaired')
    got, expected = rows(tmp_path / 'repaired'), rows(whole)
    assert got[0] == expected[0] and got[1] == expected[1]
    books = lambda d: [q for batch in _parquet.Columns.load(d).iter_batches() for q in batch.get('quotes', ())]   # noqa: E731
    repaired = {(q['issue_id'], q['frame']): q[schema.BOOK_CHANGES] for q in books(tmp_path / 'repaired')}
    lost_one = {(q['issue_id'], q['frame']): q[schema.BOOK_CHANGES] for q in books(a)}
    assert repaired[(0, 24)] != lost_one[(0, 24)], "7203's update after the ones it lost changes from the update before it now"
    plain = package(tmp_path, 'plain', 0, 80, received=7)
    with pytest.raises(ValueError, match='book changes'):
        repair(a, plain, tmp_path / 'mixed')


def stamped(first, last):
    """`spoil` for a feed that stamps the batches of frames first..last with the market clock of `last`, as one that sends a few frames
    together does (each update keeps its own time)."""
    def spoil(batches):
        for batch in batches:
            if batch['type'] == 'quotes' and first <= (batch['source_time_us'] - OPEN_US) // TICK <= last:
                batch['source_time_us'] = clock(last)
    return spoil


def later(ticks):
    """`spoil` for a feed whose batches all carry a market clock `ticks` frames later than their frames' own."""
    def spoil(batches):
        for batch in batches:
            if batch['type'] in ('quotes', 'end'):
                batch['source_time_us'] += ticks * TICK
    return spoil


@pytest.mark.parametrize('a_spoil, b_spoil', [
    (None, stamped(20, 22)),     # the other's batch of frame 20 has the clock of the batch of 22 that a holds
    (stamped(18, 21), None),     # a's batches around the lost update were stamped later than the other's
    (None, later(3)),            # all of the other's clocks run later than a's, as a feed behind it would
], ids=['same clock as the next', 'a stamped later', 'the other later'])
def test_a_lost_update_is_added_whatever_the_batch_clocks_of_the_two_feeds_say(tmp_path, group, a_spoil, b_spoil):
    a = package(tmp_path, 'a', 0, 80, drop={20}, spoil=a_spoil)
    b = package(tmp_path, 'b', 0, 80, received=250, site='e-shiten', spoil=b_spoil)   # 250 ms: the other receives it later than a's next
    whole = rows(package(tmp_path, 'whole', 0, 80))
    report = repair(a, b, tmp_path / 'repaired')
    assert report['repair']['quotes'] == len(updates(20)) == report['repair']['borrowed_receipt']
    assert report['repair']['left_out'] == dict.fromkeys(('misplaced', 'after_end', 'restored', 'not_recorded'), 0)
    got = rows(tmp_path / 'repaired')
    assert got[0] == whole[0] and got[1] == whole[1]
    forward(got[2])   # in every order, though the other received the update after a had received the next one
    assert report['package']['summary'] == _parquet.verify_broker(tmp_path / 'repaired')


def test_a_lost_update_that_would_have_to_come_before_its_neighbours_time_is_left_out(tmp_path):
    # a's own 7203 update of frame 22 is stamped before the other's of frame 20 (a conflict of its own): the other's 20 would run
    # backwards in time before it, so it is not added; 9984's update of the frame, whose neighbours agree, is.
    def early(batches):
        for batch in batches:
            for q in batch.get('quotes', []):
                if batch['source_time_us'] == clock(22) and q['issue_id'] == 0 and batch['type'] == 'quotes':
                    q['source_time_us'] = clock(19)
    a = package(tmp_path, 'a', 0, 80, drop={20}, spoil=early)
    b = package(tmp_path, 'b', 0, 80, received=7, site='e-shiten')
    report = repair(a, b, tmp_path / 'repaired')
    assert report['repair']['quotes'] == 1 and report['repair']['left_out']['misplaced'] == 1
    assert (2, 20) in rows(tmp_path / 'repaired')[0] and (0, 20) not in rows(tmp_path / 'repaired')[0]
    forward(rows(tmp_path / 'repaired')[2])


def test_with_no_output_the_report_says_what_would_be_added_and_nothing_is_written(tmp_path):
    a, b = package(tmp_path, 'a', 0, 80, drop={30}), package(tmp_path, 'b', 0, 80, received=7)
    report = repair(a, b)
    assert report['repair']['quotes'] == len(updates(30)) and 'package' not in report
    assert sorted(path.name for path in tmp_path.iterdir()) == ['a', 'a.jsonl', 'b', 'b.jsonl']


def test_two_feeds_that_see_a_frame_differently_keep_each_their_own(tmp_path, group):
    # a also lost frame 50: that is filled in, and frame 30, which b saw differently, stays as a has it.
    a = package(tmp_path, 'a', 0, 80, drop={50})
    b = package(tmp_path, 'b', 0, 80, received=7, price={(0, 30): 5, (1, 30): 5})
    whole = rows(package(tmp_path, 'whole', 0, 80))
    report = repair(a, b, tmp_path / 'repaired')
    assert report['quotes']['rows']['a']['conflict'] == report['quotes']['rows']['b']['conflict'] == 2
    assert report['quotes']['blocks']['conflict'] >= 1
    assert report['repair']['quotes'] == len(updates(50)) and report['repair']['batches'] == 2   # the update's and the trades'
    got = rows(tmp_path / 'repaired')
    assert got[0] == whole[0] and got[0][(0, 30)]['last_price10'] == 10_000 + 7 * 30   # a's, not b's
    assert report['package']['summary']['quote_updates'] == len(whole[2])


def _at(frame):
    return lambda batches: next(b for b in batches if b['type'] == 'quotes' and b['source_time_us'] == clock(frame))


@pytest.mark.parametrize('spoil, kind', [
    (lambda b: _at(30)(b)['quotes'][0]['ask_levels'][0].__setitem__(1, 301), 'quotes'),
    (lambda b: _at(30)(b)['quotes'][0]['bid_levels'].pop(), 'quotes'),
    (lambda b: _at(30)(b)['quotes'][0].__setitem__('special_quote_time_us', clock(30)), 'quotes'),
    (lambda b: _at(30)(b)['quotes'][0].__setitem__('turnover10', 1), 'quotes'),
    (lambda b: _at(30)(b)['quotes'][0]['bid_closing_levels'].append([9000, 1, 1]), 'quotes'),
    (lambda b: _at(30)(b)['trades'][1].__setitem__('quantity', 201), 'trades'),
    (lambda b: _at(30)(b)['trades'].pop(), 'trades'),
])
def test_a_difference_anywhere_in_a_row_or_a_frames_trades_is_a_conflict(tmp_path, spoil, kind):
    a = package(tmp_path, 'a', 0, 80)
    b = package(tmp_path, 'b', 0, 80, received=7, spoil=spoil)
    report = repair(a, b)
    assert report[kind]['rows']['a']['conflict'] >= 1 and report[kind]['rows']['b']['conflict'] >= 1
    assert report[kind]['blocks']['conflict'] == 1 and report['repair']['quotes'] == report['repair']['trades'] == 0
    other = 'trades' if kind == 'quotes' else 'quotes'
    assert report[other]['rows']['a']['conflict'] == 0 and report[other]['blocks']['conflict'] == 0


def test_a_feed_that_started_late_lacks_nothing_and_what_lies_outside_a_range_is_not_added(tmp_path, group):
    a, b = package(tmp_path, 'a', 0, 80), package(tmp_path, 'b', 30, 100, received=7)
    report = repair(a, b, tmp_path / 'once')
    assert report['quotes']['rows']['a']['outside'] > 0 and report['quotes']['rows']['b']['outside'] > 0
    assert report['quotes']['rows']['a']['only'] == report['quotes']['rows']['b']['only'] == 0   # each lacks only what it did not see
    assert report['repair']['quotes'] == 0 and not report['written'] and not (tmp_path / 'once').exists()
    # `until` goes past the end where nothing follows, as far as it is told.
    repaired = repair(a, b, tmp_path / 'until', until='09:00:09.000')
    assert repaired['reach'] == '09:00:09.000' and repaired['repair']['quotes'] == lost(range(81, 91))[0]
    assert rows(tmp_path / 'until')[0] == rows(package(tmp_path, 'whole', 0, 90))[0]


def test_the_gap_a_reconnect_leaves_between_two_recordings_is_closed_up_to_where_the_next_starts(tmp_path):
    first, second = package(tmp_path, 'first', 0, 40), package(tmp_path, 'second', 54, 100)
    b, whole = package(tmp_path, 'b', 0, 100, received=7), package(tmp_path, 'whole', 0, 53)
    assert repair(first, b)['repair']['quotes'] == 0   # nothing is known to follow it: its own range is all there is
    result = cli.repair([first, second], [b], tmp_path / 'closed')
    one, two = result['recordings']
    assert one['reach'] == '09:00:05.399' and two['reach'] is None   # a microsecond before the next recording's first (09:00:05.400)
    assert one['written'] and not two['written'] and not (tmp_path / 'closed' / 'second').exists()
    # b has the rest of the day, which the next recording already holds: only up to where it starts is added, so that nothing is
    # stored twice (its own first moment, which is its opening book, is not added to the first).
    assert one['sources'][0]['repair']['quotes'] == lost(range(41, 54))[0]
    got, expected = rows(tmp_path / 'closed' / 'first'), rows(whole)
    assert got[0] == expected[0] and got[1] == expected[1] and max(f for _, f in got[0]) == 52   # frame 53 changed nothing
    assert got[2].batches[-1]['source_time_us'] <= clock(54) - 1
    # What follows everything the first recording holds of a security is a batch of the other's own, with its receipt time (7).
    tail = {b['received_unix_ms'] - 1773187200000 - b['source_time_us'] // 1000 for b in got[2].batches
            if b['type'] == 'quotes' and b['source_time_us'] > clock(40)}
    assert tail == {7}
    forward(got[2])
    assert {k for k in got[0] if k[1] >= 54} == set()   # none of what the second recording holds is in the first
    # An `until` that goes into the next recording is refused, and so is one for several recordings at once.
    with pytest.raises(ValueError, match='store that time twice'):
        cli.repair([first], [b, second], until='09:00:08')
    with pytest.raises(ValueError, match='one recording at a time'):
        cli.repair([first, second], [b], until='09:00:08')


def test_morning_and_afternoon_pieces_are_repaired_each_from_its_own_half_of_the_other_feed(tmp_path):
    # b's pieces cover the same times as a's: each pairs with its own, never with the other (their times are all different), and
    # a repair does not go from one piece into the next.
    mine, theirs = tmp_path / 'mine', tmp_path / 'theirs'
    mine.mkdir(), theirs.mkdir()
    package(mine, 'am', 0, 40, drop={20}), package(mine, 'pm', 40, 80, drop={60})
    package(theirs, 'am', 0, 40, received=7), package(theirs, 'pm', 40, 80, received=7)
    result = cli.repair([mine], [theirs], tmp_path / 'repaired')   # each a folder of recordings
    assert [len(entry['sources']) for entry in result['recordings']] == [1, 1]
    assert [Path(entry['sources'][0]['path']).name for entry in result['recordings']] == ['am', 'pm']
    assert [entry['written'] for entry in result['recordings']] == [True, True]
    for name, (first, last) in {'am': (0, 40), 'pm': (40, 80)}.items():
        assert rows(tmp_path / 'repaired' / name)[:2] == rows(package(tmp_path, f'whole-{name}', first, last))[:2]


def test_a_recording_is_repaired_from_every_recording_that_covers_its_time(tmp_path):
    a = package(tmp_path, 'a', 0, 80, drop={20, 60})
    early, late = package(tmp_path, 'early', 0, 40, received=7), package(tmp_path, 'late', 40, 80, received=9)
    result = cli.repair([a], [early, late], tmp_path / 'repaired')
    entry = result['recordings'][0]
    assert [source['repair']['batches'] for source in entry['sources']] == [3, 4] and entry['written']   # updates' and trades'
    whole = rows(package(tmp_path, 'whole', 0, 80))
    got = rows(tmp_path / 'repaired')
    assert got[0] == whole[0] and got[1] == whole[1]
    received = {b['source_time_us']: b['received_unix_ms'] - 1773187200000 - b['source_time_us'] // 1000
                for b in got[2].batches if b['type'] == 'quotes'}
    assert (received[clock(20)], received[clock(60)], received[clock(30)]) == (7, 9, 0)


def test_recordings_that_share_little_of_their_time_are_not_partners(tmp_path):
    # b covers only the last tenth of a's time: it is the next piece of some feed, not another feed of the same stretch.
    a, b = package(tmp_path, 'a', 0, 80), package(tmp_path, 'b', 72, 100, received=7)
    with pytest.raises(ValueError, match='Nothing to repair from'):
        cli.repair([a], [b])


def test_a_recording_with_nothing_to_gain_is_not_written(tmp_path):
    a, b = package(tmp_path, 'a', 0, 80), package(tmp_path, 'b', 0, 80, received=7)
    result = cli.repair([a], [b], tmp_path / 'repaired')
    assert result['recordings'][0]['written'] is False and not (tmp_path / 'repaired').exists()


def test_only_what_fits_among_a_recordings_own_rows_is_added_to_it():
    # a's rows of a security (frames, source times, their batches) and the clocks of those batches; b's candidates (frames, times,
    # their batches' clocks), one each way a row can fail to fit.
    a = _repair.Rows(None, np.array([1, 5, 9]), np.array([10, 50, 90]), np.array([0, 1, 2]))
    a_clock = np.array([10, 50, 90])
    b = _repair.Rows(None, np.array([0, 3, 6, 7, 7, 12, 20]), np.array([5, 30, 60, 70, 95, 120, 400]), np.zeros(7, np.int64))
    b_clock = np.array([5, 30, 40, 70, 95, 120, 400])
    own, join, rank, misplaced, outside = _repair.fit(a, b, np.ones(7, bool), a_clock, b_clock, 10, 200)
    # Frame 0's batch (clock 5) starts before the range: outside. 3 fits between frames 1 and 5; 6's batch clock (40) is earlier than
    # the row before it (frame 5 in a batch at 50): misplaced; 7 at 70 fits; 7 at time 95 is later than the row after it (frame 9 at
    # 90): misplaced; 12 follows the last row: fits; 20's batch is past the end: outside.
    assert own.tolist() == [False, True, False, True, False, True, False] and (join == -1).all()
    assert misplaced == 2 and outside == 2
    # An update that a later one of its security follows goes in a batch before that one's (the batch of a it comes before, and its
    # place among those that come before the same batch: 6 and 7 are two rows between frames 5 and 9), whatever the clocks say;
    # one that nothing follows goes in a batch of its own, and 7's disagreeing time still does not fit.
    own, join, rank, misplaced, outside = _repair.fit(a, b, np.ones(7, bool), a_clock, b_clock, 10, 200, joining=True)
    assert own.tolist() == [False, False, False, False, False, True, False]
    assert join.tolist() == [-1, 1, 2, 2, -1, -1, -1] and rank.tolist() == [0, 0, 0, 1, 0, 0, 0]
    assert misplaced == 1 and outside == 2
    # A row at the clock of the row after it cannot have a batch of its own (it would come after that row's), but it can come before.
    equal = _repair.Rows(None, np.array([4]), np.array([45]), np.zeros(1, np.int64))
    assert _repair.fit(a, equal, np.ones(1, bool), a_clock, np.array([50]), 10, 200)[0].tolist() == [False]
    assert _repair.fit(a, equal, np.ones(1, bool), a_clock, np.array([50]), 10, 200, joining=True)[1].tolist() == [1]
    # After a's last row of the security nothing comes before the batch of: a clock before that row's batch cannot be a batch of its own
    # either, and the batch before would give the update a receipt time earlier than a could know it.
    tail = _repair.Rows(None, np.array([12]), np.array([120]), np.zeros(1, np.int64))
    own, join, _, misplaced, _ = _repair.fit(a, tail, np.ones(1, bool), a_clock, np.array([60]), 10, 200, joining=True)
    assert not own.any() and (join == -1).all() and misplaced == 1
    # A security with none of a's rows has nothing to be in order with: whatever is in range fits.
    none = _repair.fit(_repair.Rows(), b, np.ones(7, bool), np.zeros(0, np.int64), b_clock, 10, 200)
    assert none[0].tolist() == [False, True, True, True, True, True, False] and none[3:] == (0, 2)


def test_trades_a_recording_lost_all_of_are_added_to_a_security_it_does_record(tmp_path, group):
    # a never saw a trade of the first security, so it has none to put the other's among; it does record the security.
    def none_traded(batches):
        for b in batches:
            if b['type'] == 'quotes':
                b['trades'] = []
    a = package(tmp_path, 'a', 0, 80, spoil=none_traded)
    b, whole = package(tmp_path, 'b', 0, 80, received=7), package(tmp_path, 'whole', 0, 80)
    report = repair(a, b, tmp_path / 'repaired')
    assert report['repair']['trades'] == lost(range(81))[1] == 32 and report['repair']['left_out']['not_recorded'] == 0
    assert report['repair']['quotes'] == 0 and report['repair']['batches'] == 16
    got, expected = rows(tmp_path / 'repaired'), rows(whole)
    assert got[1] == expected[1] and got[0] == expected[0]


def test_securities_the_other_adds_to_a_table_that_lacks_them_come_in_order():
    chunks = iter([(1, 'one'), (3, 'three')])
    assert list(_repair.with_issues(chunks, {0, 2, 3, 4})) == [(0, None), (1, 'one'), (2, None), (3, 'three'), (4, None)]


def test_a_security_a_recording_does_not_hold_is_not_added_to_it(tmp_path):
    a = package(tmp_path, 'a', 0, 80, issues=ISSUES[:2])
    b = package(tmp_path, 'b', 0, 80, received=7)
    report = repair(a, b)
    assert report['repair']['quotes'] == 0 and report['repair']['left_out']['not_recorded'] == report['quotes']['rows']['b']['only'] > 0


def test_what_a_late_start_restores_is_not_a_lost_frame(tmp_path):
    # b started after trades happened: its history holds them (a lost them in frames 20 and 25), but it is not a frame of its own.
    a, b = package(tmp_path, 'a', 0, 80, drop={20, 25}), package(tmp_path, 'b', 30, 80, received=7)
    report = repair(a, b)
    assert report['repair']['trades'] == 0 and report['repair']['left_out']['restored'] == 4
    assert report['trades']['rows']['b']['only'] == 4


def test_recordings_of_different_days_are_not_compared(tmp_path):
    a = package(tmp_path, 'a', 0, 40)
    (tmp_path / 'b.jsonl').write_bytes(b''.join(lines([{**b, 'trading_date': '20260312'} if b['type'] == 'bootstrap' else b
                                                         for b in feed(0, 40)])))
    cli.package(tmp_path / 'b.jsonl', tmp_path / 'b')
    with pytest.raises(ValueError, match='Nothing to repair from'):   # no recording of its day to repair it from
        cli.repair([a], [tmp_path / 'b'])
    with pytest.raises(ValueError, match='different days'):
        _repair.compare(a, tmp_path / 'b')


def test_a_repair_that_cannot_be_verified_leaves_nothing_behind(tmp_path, monkeypatch):
    a, b = package(tmp_path, 'a', 0, 80, drop={30}), package(tmp_path, 'b', 0, 80, received=7)

    def refuse(directory, *args, **options):
        raise ValueError('Market clock regression')
    monkeypatch.setattr(_parquet, 'verify_broker', refuse)
    with pytest.raises(ValueError, match='Market clock regression'):
        repair(a, b, tmp_path / 'repaired')
    assert not any((tmp_path / 'repaired' / name).exists() for name in (*_parquet.FILES, 'manifest.json'))


def test_blocks_say_which_recording_lacks_what(tmp_path):
    a = package(tmp_path, 'a', 0, 80, drop={30})
    b = package(tmp_path, 'b', 0, 80, drop={50}, received=7)
    whole = package(tmp_path, 'whole', 0, 80)
    # One block for each security: the first lost a frame in each (30 and 50), the second only in a (30), the third none.
    both = repair(a, b)['quotes']['blocks']
    assert (both['each_lacks'], both['a_lacks'], both['identical']) == (1, 1, 1) and both['conflict'] == both['unshared'] == 0
    only = repair(whole, b)['quotes']
    assert (only['blocks']['b_lacks'], only['blocks']['identical']) == (1, 2) and only['rows']['a']['only'] == 1
    assert 0 < only['rows_in_identical_blocks_pct'] < 100


def test_a_security_only_one_recording_holds_is_only_that_ones(tmp_path):
    a, b = package(tmp_path, 'a', 0, 80), package(tmp_path, 'b', 0, 80, received=7, issues=ISSUES[:2])
    report = repair(a, b)
    assert report['quotes']['rows']['a']['only'] == 21 and report['quotes']['blocks']['b_lacks'] == 1
    assert report['trades']['rows']['a']['only'] == 0   # the third security has no trades
    nothing = repair(b, a)
    assert nothing['repair']['quotes'] == 0 and nothing['repair']['left_out']['not_recorded'] == 21


def test_what_came_before_a_recording_can_be_left_out_of_the_count(tmp_path):
    a, b = package(tmp_path, 'a', 0, 80), package(tmp_path, 'b', 30, 80, received=7)
    counted = _repair.compare(a, b)
    left_out = _repair.compare(a, b, snapshots=False)
    assert counted['quotes']['rows']['b']['snapshot'] == counted['trades']['rows']['b']['snapshot'] == 0
    # b's opening book (one row for each security) and its restored trades (those of frames 5 to 30), and a's opening book.
    assert left_out['quotes']['rows']['b']['snapshot'] == 3 and left_out['trades']['rows']['b']['snapshot'] == 12
    assert left_out['quotes']['rows']['a']['snapshot'] == 3 and left_out['trades']['rows']['a']['snapshot'] == 0


def test_a_failure_while_writing_leaves_no_package(tmp_path, monkeypatch):
    a, b = package(tmp_path, 'a', 0, 80, drop={30}), package(tmp_path, 'b', 0, 80, received=7)

    def fail(*args):
        raise RuntimeError('disk full')
    monkeypatch.setattr(_repair, 'merged', fail)
    with pytest.raises(RuntimeError, match='disk full'):
        repair(a, b, tmp_path / 'repaired')
    assert not any((tmp_path / 'repaired' / name).exists() for name in (*_parquet.FILES, 'manifest.json'))


def test_recordings_that_do_not_number_their_frames_alike_are_not_repaired(tmp_path):
    # A frame number only says which update it is if both feeds count the server's frames: with one's shifted a feed's rows would be
    # added again under other numbers. They match in what the report counts, and nothing is written.
    a, b = package(tmp_path, 'a', 0, 80, drop={30}), package(tmp_path, 'b', 0, 80, received=7, offset=3)
    report = repair(a, b)
    assert report['repair']['frame_numbers_match'] is False and report['quotes']['rows']['b']['same'] == 0
    with pytest.raises(ValueError, match='do not number their frames alike'):
        repair(a, b, tmp_path / 'repaired')
    assert not (tmp_path / 'repaired').exists()
    good = repair(a, package(tmp_path, 'c', 0, 80, received=7))
    assert good['repair']['frame_numbers_match'] is True


def test_the_command_reports_and_writes_the_package(tmp_path, capsys):
    a, b = package(tmp_path, 'a', 0, 80, drop={30, 31}), package(tmp_path, 'b', 0, 100, received=7)
    cli.main(['repair', str(a), '--from', str(b)])
    entry = json.loads(capsys.readouterr().out)['recordings'][0]
    report = entry['sources'][0]
    assert report['repair']['batches'] == 3 and report['repair']['quotes'] == len(updates(30)) and not entry['written']
    cli.main(['repair', str(a), '--from', str(b), '--output', str(tmp_path / 'out'), '--until', '09:00:09'])
    written = json.loads(capsys.readouterr().out)['recordings'][0]
    assert written['reach'] == '09:00:09.000' and written['written']
    assert written['sources'][0]['repair']['batches'] == 3 + len([f for f in range(81, 91) if updates(f) or trades(f)])
    assert (tmp_path / 'out' / 'manifest.json').exists()
    with pytest.raises(SystemExit):
        cli.main(['repair', str(a)])   # --from is not optional
