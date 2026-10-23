"""brisk-parquet (rust/brisk_parquet) against Python's rules for a broker's recording (tests/reference_verifier.py)."""
import copy
import decimal
import json
import random
import shutil

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import briskapi.schema as schema
from briskapi import _native, _parquet

import reference_verifier
from test_broker_verify import broker, spoiled  # noqa: F401  (the fixture and its helper)


def outcome(function, *arguments):
    """The summary a verifier gives, or the message of its refusal (what a library says of a value it cannot read, in
    the words brisk-parquet uses)."""
    try:
        return function(*arguments)
    except ValueError as error:
        text = str(error)
        if 'Rescaling Decimal' in text:
            return 'Invalid decimal value'
        if 'line 1 column' in text or 'Expecting' in text:
            return 'Invalid broker transport fields'
        return text


def test_the_rust_verifier_gives_the_summary_python_does(tmp_path, broker):  # noqa: F811
    m, _ = broker
    package = tmp_path / 'package'
    assert reference_verifier.verify_broker(package, m) == _parquet.verify_broker(package, m) == m['summary']
    assert json.loads(_native.run(['verify', package])) == m['summary']


def test_a_missing_program_or_table_is_told_plainly(tmp_path, broker, monkeypatch):  # noqa: F811
    m, _ = broker
    shutil.rmtree(tmp_path / 'package' / 'data' / 'custom')
    with pytest.raises(ValueError, match='Missing data/custom/BriskBatch.parquet'):
        _parquet.verify_broker(tmp_path / 'package', m)
    monkeypatch.setenv('BRISK_PARQUET', str(tmp_path / 'nowhere'))
    monkeypatch.setattr(_native, '_HERE', tmp_path)
    monkeypatch.setattr(_native.shutil, 'which', lambda name: None)
    with pytest.raises(RuntimeError, match='not installed'):
        _native.binary()


def test_progress_is_told_as_each_table_is_done(tmp_path, broker):  # noqa: F811
    m, _ = broker
    seen = []
    _parquet.verify_broker(tmp_path / 'package', m, seen.append)
    assert seen == ['verify: batches', 'verify: depths', 'verify: trades']


def rewrite(directory, name, table):
    path = directory / name
    pq.write_table(table, path, row_group_size=_parquet.ROW_GROUP, **_parquet.options(name))


def mutate(rng, table):
    """The table with one cell changed (or a list's level moved), and what was done."""
    names = table.schema.names
    c = rng.randrange(len(names))
    name, column = names[c], table.column(c).combine_chunks()
    row = rng.randrange(len(column))
    kind = column.type
    nullable = table.schema.field(c).nullable

    def pick(choices):
        return rng.choice([v for v in choices if v is not None or nullable])
    if pa.types.is_timestamp(kind):
        values = column.cast(pa.int64()).to_pylist()
        was = values[row]
        values[row] = pick([None, was + 1000, was - 1000, was + 1, 0, was + 10**15]) if was is not None else 1000
        mutated = pa.array(values, pa.int64()).cast(kind)
    elif pa.types.is_integer(kind) or pa.types.is_floating(kind):
        values = column.to_pylist()
        was = values[row]
        choices = [None, 0, 2**53, 2**53 - 1, 1] if was is None else [None, 0, was + 1, was + 1000, max(was - 1, 0), 2**53, 2**32]
        if pa.types.is_floating(kind):
            choices = [None, 0.0, -0.0, 1.0005, 1.5, 86400001.0, float('nan'), 12.345, 1e300]
        value = pick(choices)
        values[row] = min(value, 255) if kind == pa.uint8() and value is not None else value
        mutated = pa.array(values, kind)
    elif pa.types.is_decimal(kind):
        values = column.to_pylist()
        step = rng.choice([decimal.Decimal('0.1'), decimal.Decimal('-0.1'), decimal.Decimal('0.0000000000000001'),
                           decimal.Decimal('1'), decimal.Decimal('9007199254740993')])
        values[row] = pick([None, decimal.Decimal(0), (values[row] or decimal.Decimal(0)) + step])
        mutated = pa.array(values, kind)
    elif pa.types.is_string(kind):
        values = column.to_pylist()
        was = values[row]
        values[row] = pick([None, '', (was or '') + 'x', 'X' + (was or ''), 'BriskBatch', '7203.XTKS', '0-0', '1-1'])
        mutated = pa.array(values, kind)
    elif pa.types.is_dictionary(kind):
        values = column.cast(pa.string()).to_pylist()
        values[row] = rng.choice(['BUY', 'SELL', 'NO_AGGRESSOR'])
        mutated = pa.array(values, pa.string()).dictionary_encode().cast(kind)
    elif pa.types.is_list(kind):
        values = column.to_pylist()
        item = values[row]
        if item is None:
            values[row] = pick([None, []])
        else:
            move = rng.choice(['drop', 'swap', 'duplicate', 'empty', 'many', 'order_id', 'bump'])
            if move == 'drop' and item:
                item.pop()
            elif move == 'swap' and len(item) > 1:
                item[0], item[1] = item[1], item[0]
            elif move == 'duplicate' and item:
                item.append(copy.deepcopy(item[-1]))
            elif move == 'empty':
                values[row] = []
            elif move == 'many' and item and isinstance(item[0], dict):
                item.extend(copy.deepcopy(item[0]) for _ in range(11))
            elif move == 'order_id' and item and isinstance(item[0], dict):
                item[0]['order_id'] = 1
            elif move == 'bump' and item:
                if isinstance(item[0], dict):
                    item[0]['price'] += decimal.Decimal('0.1')
                else:
                    item[0] = item[0] + 1
        mutated = pa.array(values, kind)
    else:
        raise AssertionError(kind)
    return table.set_column(c, table.schema.field(c), mutated), f'{name}[{row}]'


@pytest.mark.parametrize('seed', range(8))
def test_a_cell_changed_anywhere_is_refused_by_both_or_neither(tmp_path, broker, seed):  # noqa: F811
    """Mutate one cell of one table, over and over: the Rust verifier and Python's rules must agree on every one (the
    summary, or the same refusal)."""
    m, _ = broker
    rng = random.Random(seed)
    disagreements, refused = [], 0
    for trial in range(40):
        target = tmp_path / f'case{trial}'
        shutil.copytree(tmp_path / 'package', target)
        name = rng.choice(_parquet.FILES)
        table, what = mutate(rng, pq.read_table(target / name))
        rewrite(target, name, table)
        python = outcome(reference_verifier.verify_broker, target, m)
        rust = outcome(_parquet.verify_broker, target, m)
        refused += isinstance(python, str)
        if python != rust:
            disagreements.append((name, what, python, rust))
        shutil.rmtree(target)
    assert not disagreements, disagreements
    assert refused > 10   # the mutations are not all harmless


def test_both_agree_on_what_the_summary_is_for_many_row_groups(tmp_path, broker, monkeypatch):  # noqa: F811
    m, columns = broker
    monkeypatch.setattr(_parquet, 'ROW_GROUP', 3)
    target = spoiled(tmp_path, columns, 'groups')
    assert reference_verifier.verify_broker(target) == _parquet.verify_broker(target) == m['summary']
    order = np.lexsort((columns.q['seq'], columns.q['issue_id']))[::-1]
    columns.order = None
    other = spoiled(tmp_path, columns.take(order), 'reversed')
    assert outcome(reference_verifier.verify_broker, other) == outcome(_parquet.verify_broker, other)


def test_a_package_must_be_in_the_format_its_manifest_names(tmp_path, broker):  # noqa: F811
    m, _ = broker
    with pytest.raises(ValueError, match='Unexpected schema'):
        _parquet.verify_broker(tmp_path / 'package', dict(m, schema=schema.LEGACY_SCHEMA))


def test_the_expected_schemas_are_python_s(tmp_path):
    """brisk-parquet writes an empty table of each kind: pyarrow reads exactly the schema and metadata Python defines."""
    _native.run(['schemas', tmp_path])
    for name, expected in _parquet.SCHEMAS.items():
        found = pq.ParquetFile(tmp_path / name)
        assert found.metadata.schema.to_arrow_schema().equals(expected, check_metadata=True), name
        assert found.metadata.num_rows == 0
        assert set(found.metadata.metadata) == {b'ARROW:schema', *expected.metadata}


# Packaging: parts of a spooled segment, as one recording

from test_spool import many_parts, same_tables, session, spooled  # noqa: E402  (the segment's fixtures)
import reference_package  # noqa: E402


def two_ways(tmp_path, spool, parts, k=None, end=None, range_rows=None):
    """Package `parts` with Python's write_piece and with brisk-parquet, the second after a piece of `parts[:k]` if `k`."""
    equity = spool.path / _parquet.EQUITY
    bootstrap = _parquet.bootstrap_row(spool.parts()[0])
    midnight = _parquet.midnight_ns(bootstrap['trading_date'])
    cut = state = clock = None
    mine, theirs = tmp_path / 'rust', tmp_path / 'python'
    if k:
        first = _native.package(spool.path, parts[:k], tmp_path / 'first', midnight, end='cut', state_out=tmp_path / 'state.parquet',
                                range_rows=range_rows)
        _, latest, clock, _ = reference_package.write_piece(equity, parts[:k], tmp_path / 'first-python', midnight, None, bootstrap, 'cut')
        same_tables(tmp_path / 'first', tmp_path / 'first-python')
        cut, state = (tmp_path / 'state.parquet', first['clock']), reference_package.Cut(latest, clock)
        assert first['clock'] == clock
        parts = parts[k:]
    result = _native.package(spool.path, parts, mine, midnight, day_start=spool.parts()[0], cut=cut, end=end or 'parts',
                             state_out=tmp_path / 'state2.parquet', range_rows=range_rows)
    summary, latest, clock, repaired = reference_package.write_piece(equity, parts, theirs, midnight, state, bootstrap, end)
    same_tables(mine, theirs)
    assert result['summary'] == summary and result['clock'] == clock and result['repaired'] == repaired
    after = pq.read_table(tmp_path / 'state2.parquet')
    assert after.column('issue_id').to_pylist() == latest.q['issue_id'].tolist()
    assert after.column('sequence').to_pylist() == latest.q['frame'].tolist()
    return result


@pytest.fixture
def segment(tmp_path):
    spool = spooled(tmp_path, session())
    spool.close_part(wait=True)
    assert len(spool.parts()) > 5
    return spool


def test_the_parts_go_on_the_programs_standard_input_not_its_command_line(tmp_path, segment, monkeypatch):
    # Windows' CreateProcess takes 32,767 characters: a day's 1,670 parts as arguments raised WinError 206 before the program
    # started. The list is read from --parts-file (a path a line; `-`: stdin), so the command line is the same for any count.
    commands = []
    real = _native.subprocess.Popen
    monkeypatch.setattr(_native.subprocess, 'Popen', lambda command, **options: commands.append(command) or real(command, **options))
    midnight = _parquet.midnight_ns(_parquet.bootstrap_row(segment.parts()[0])['trading_date'])
    parts = segment.parts()
    absent = [tmp_path / 'nowhere' / 'segment' / 'parts' / f'{n:06d}' for n in range(1, 10_001)]
    assert sum(len(str(p)) + 1 for p in absent) > 32_767
    with pytest.raises(ValueError, match='Missing'):   # the program ran, and read the list down to a part that is not there
        _native.state(segment.path, parts + absent, midnight, tmp_path / 'state.parquet', day_start=parts[0])
    with pytest.raises(ValueError, match='Missing'):
        _native.package(segment.path, parts + absent, tmp_path / 'out', midnight)
    assert len(commands) == 2
    for command in commands:
        assert command[-2:] == ['--parts-file', '-'] and sum(len(word) + 1 for word in command) < 1000
        assert str(parts[1]) not in command and str(absent[-1]) not in command   # --day-start names the first
    # And positional parts still work, for the program's other callers.
    out = json.loads(_native.run(['state', '--spool', segment.path, '--midnight', midnight, '--state-out', tmp_path / 'state.parquet',
                                  '--day-start', parts[0], *parts]))
    assert out['clock'] == _native.state(segment.path, parts, midnight, tmp_path / 'state2.parquet', day_start=parts[0])


def test_a_parts_file_is_read_as_the_positional_parts_are(tmp_path, segment):
    midnight = _parquet.midnight_ns(_parquet.bootstrap_row(segment.parts()[0])['trading_date'])
    parts = segment.parts()
    listing = tmp_path / 'parts.txt'
    listing.write_text('\ufeff' + '\r\n'.join(map(str, parts[:-1])) + '\r\n\r\n', encoding='utf-8')   # as a Windows editor leaves it
    arguments = ['package', '--spool', segment.path, '--midnight', midnight, '--day-start', parts[0]]
    by_file = json.loads(_native.run([*arguments, '--out', tmp_path / 'file', '--parts-file', listing, parts[-1]]))
    by_words = json.loads(_native.run([*arguments, '--out', tmp_path / 'words', *parts]))
    assert by_file == by_words
    same_tables(tmp_path / 'file', tmp_path / 'words')
    with pytest.raises(RuntimeError, match='cannot read the parts file'):
        _native.run([*arguments, '--out', tmp_path / 'missing', '--parts-file', tmp_path / 'nowhere.txt'])


def test_a_segment_of_hundreds_of_parts_is_packaged_as_python_does(tmp_path):
    spool, _ = many_parts(tmp_path, 500)
    parts = spool.parts()
    result = two_ways(tmp_path, spool, parts)
    assert result['summary']['batches'] == 500
    midnight = _parquet.midnight_ns(_parquet.bootstrap_row(parts[0])['trading_date'])
    clock = _native.state(spool.path, parts[:450], midnight, tmp_path / 'state450.parquet', day_start=parts[0])
    assert clock == _parquet.part_index(parts[449], midnight)['last']


def test_a_whole_segment_is_packaged_as_python_does(tmp_path, segment):
    two_ways(tmp_path, segment, segment.parts())
    result = json.loads(_native.run(['verify', tmp_path / 'rust']))
    assert result == json.loads((tmp_path / 'rust' / 'summary.json').read_text()) if (tmp_path / 'rust' / 'summary.json').exists() else True


@pytest.mark.parametrize('k', [1, 2, 3, 4])
def test_a_piece_after_a_cut_is_packaged_as_python_does(tmp_path, segment, k):
    result = two_ways(tmp_path, segment, segment.parts(), k=k)
    assert json.loads(_native.run(['verify', tmp_path / 'rust'])) == result['summary']
    assert json.loads(_native.run(['verify', tmp_path / 'first'])) is not None


def told(spool, parts, out, **options):
    seen = []
    midnight = _parquet.midnight_ns(_parquet.bootstrap_row(spool.parts()[0])['trading_date'])
    result = _native.package(spool.path, parts, out, midnight, progress=seen.append, **options)
    return result, [_native.parse_progress(label) for label in seen], seen


def test_packaging_tells_its_stages_and_the_rows_it_has_written(tmp_path, segment):
    result, seen, labels = told(segment, segment.parts(), tmp_path / 'out', range_rows=3)
    assert None not in seen and len(set(labels)) == len(labels), labels   # nothing but package labels, none told twice
    assert seen[0] == ('preparing', None, None) and seen[-1] == ('finalizing', None, None)
    # The rows of each table count up to its total, the last count always told (the rest at most twice a second).
    rows = {table: [(done, total) for name, done, total in seen if name == table] for table in ('batches', 'quotes', 'trades')}
    for table, name in (('batches', _parquet.BATCHES), ('quotes', _parquet.DEPTHS), ('trades', _parquet.TRADES)):
        written = pq.read_metadata(tmp_path / 'out' / name).num_rows
        assert written and rows[table][-1] == (written, written), table
        assert [done for done, _ in rows[table]] == sorted(done for done, _ in rows[table])
    assert rows['batches'][-1][1] == result['summary']['batches'] and rows['quotes'][-1][1] == result['summary']['quote_updates']
    # Batches, then the quotes and trades by range of issues, between the stages.
    names = [name for name, *_ in seen]
    assert names.index('batches') < names.index('quotes') < names.index('trades') < names.index('finalizing')


def test_a_piece_after_a_cut_counts_the_book_it_opens_with_among_its_quotes(tmp_path, segment):
    parts = segment.parts()
    midnight = _parquet.midnight_ns(_parquet.bootstrap_row(parts[0])['trading_date'])
    first = _native.package(segment.path, parts[:2], tmp_path / 'first', midnight, end='cut', state_out=tmp_path / 'state.parquet')
    _, seen, _ = told(segment, parts[2:], tmp_path / 'second', day_start=parts[0], cut=(tmp_path / 'state.parquet', first['clock']))
    written = pq.read_metadata(tmp_path / 'second' / _parquet.DEPTHS).num_rows
    assert [(done, total) for name, done, total in seen if name == 'quotes'][-1] == (written, written)


def test_the_book_alone_is_not_a_recording_and_tells_nothing(tmp_path, segment):
    seen = []
    midnight = _parquet.midnight_ns(_parquet.bootstrap_row(segment.parts()[0])['trading_date'])
    _native.run(['state', '--spool', segment.path, '--midnight', midnight, '--state-out', tmp_path / 'state.parquet',
                 *segment.parts()], seen.append)
    assert seen == []


def test_only_a_packages_own_labels_are_read():
    assert _native.parse_progress('package: preparing') == ('preparing', None, None)
    assert _native.parse_progress('package: quotes 12/340') == ('quotes', 12, 340)
    for other in ('verify: batches', 'package: sorting', 'package: quotes 12', 'package: quotes 1/2 more', ''):
        assert _native.parse_progress(other) is None


def test_a_piece_that_stops_early_gets_an_end(tmp_path, segment):
    result = two_ways(tmp_path, segment, segment.parts()[:3], end='cut')
    assert json.loads(_native.run(['verify', tmp_path / 'rust'])) == result['summary'] and not result['repaired']


def test_a_segment_cut_short_is_repaired(tmp_path):
    spool = spooled(tmp_path, session()[:30])
    spool.close_part(wait=True)
    result = two_ways(tmp_path, spool, spool.parts(), end='repair')
    assert result['repaired'] and json.loads(_native.run(['verify', tmp_path / 'rust'])) == result['summary']


def test_a_piece_written_a_range_of_issues_at_a_time_is_the_same_as_written_at_once(tmp_path, segment, monkeypatch):
    monkeypatch.setattr(reference_package, 'PIECE_ROWS', 3)   # many ranges, each a few issues' rows
    monkeypatch.setattr(reference_package, 'PART_ISSUES', 1)
    monkeypatch.setattr(reference_package, 'DECODE_ROWS', 4)
    result = two_ways(tmp_path, segment, segment.parts(), k=len(segment.parts()) // 2, range_rows=3)
    assert json.loads(_native.run(['verify', tmp_path / 'rust'])) == result['summary']


# Parts: the decoder's JSON lines as the tables of a spool part

from briskapi.schema import canonical_batches  # noqa: E402
import io  # noqa: E402


def python_part(tmp_path, lines_, header):
    """The part Python writes from these lines (trade IDs counted within the part)."""
    columns = reference_package.part_columns(lines_, header)()
    reference_package.write_part(columns, header, tmp_path / 'python', None)
    return tmp_path / 'python'


def part_header(first_line):
    header = {k: v for k, v in next(canonical_batches(io.BytesIO(first_line))).items() if k != schema.SKIPPED}
    return header | {'master': [], 'quotes': [], 'trades': []}


def test_a_part_is_the_same_tables_python_writes(tmp_path):
    from test_spool import lines as as_lines
    batches = session()
    every = as_lines(batches)
    header = part_header(every[0])
    midnight = _parquet.midnight_ns(header['trading_date'])
    for name, chunk in (('whole', every), ('later', every[5:20]), ('end', every[-4:]), ('one', every[:1])):
        target = tmp_path / name
        python = python_part(tmp_path / name, chunk, header)
        counts = _native.part(chunk, target / 'rust', midnight)
        same_tables(target / 'rust', python, (_parquet.BATCHES, _parquet.DEPTHS, _parquet.TRADES))
        assert counts['batches'] == len(chunk)


def test_a_line_that_is_not_a_batch_is_refused(tmp_path):
    from test_spool import lines as as_lines
    every = as_lines(session())
    midnight = _parquet.midnight_ns('20260311')
    for bad, message in ((b'{"type":"quotes"}\n', 'Unknown or missing batch fields'), (b'not json\n', 'Invalid batch'),
                         (every[3].replace(b'"issue_id": 0', b'"issue_id": -1', 1), 'negative')):
        with pytest.raises(ValueError, match=message):
            _native.part([every[2], bad], tmp_path / 'bad', midnight)


def test_a_program_an_installer_left_without_its_execute_bit_is_made_executable(tmp_path, monkeypatch):
    import os
    import stat
    program = tmp_path / 'brisk-parquet'
    program.write_bytes(b'#!/bin/sh\n')
    program.chmod(0o644)
    monkeypatch.setenv('BRISK_PARQUET', str(program))
    assert _native.binary() == str(program)
    if os.name != 'nt':
        assert program.stat().st_mode & stat.S_IXUSR
