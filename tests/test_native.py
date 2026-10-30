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

import briskapi.cli as cli
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


def mutations(tmp_path, m, seed, trials=40):
    """Mutate one cell of one table of the package in `tmp_path`, over and over: the verdicts of the Rust verifier and Python's
    rules on each (the summary, or the same refusal). Returns where they differ, and how many were refused."""
    rng = random.Random(seed)
    disagreements, refused = [], 0
    for trial in range(trials):
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
    return disagreements, refused


@pytest.mark.parametrize('seed', range(8))
def test_a_cell_changed_anywhere_is_refused_by_both_or_neither(tmp_path, broker, seed):  # noqa: F811
    """Mutate one cell of one table, over and over: the Rust verifier and Python's rules must agree on every one (the
    summary, or the same refusal)."""
    m, _ = broker
    disagreements, refused = mutations(tmp_path, m, seed)
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


def test_the_expected_schemas_with_book_changes_are_python_s(tmp_path):
    """And the depths table of quotes with book changes has their two columns more, nothing else different."""
    _native.run(['schemas', tmp_path, '--depths', 'changes'])
    expected = {**_parquet.SCHEMAS, _parquet.DEPTHS: _parquet.DEPTH_CHANGES_SCHEMA}
    for name, wanted in expected.items():
        found = pq.ParquetFile(tmp_path / name)
        assert found.metadata.schema.to_arrow_schema().equals(wanted, check_metadata=True), name
        assert set(found.metadata.metadata) == {b'ARROW:schema', *wanted.metadata}
    assert _parquet.DEPTH_CHANGES_SCHEMA.names == [*_parquet.DEPTH_SCHEMA.names, 'changes', 'checkpoint']
    with pytest.raises(RuntimeError, match='usage'):
        _native.run(['schemas', tmp_path, '--depths', 'other'])


# Book changes: a broker's quotes that say what of their books changed, and the checkpoints the depths table holds

from test_broker_verify import three_verdicts  # noqa: E402
from test_spool import lines as event_lines  # noqa: E402


@pytest.fixture
def changed(tmp_path, checkpoints):
    """A broker's recording with book changes, packaged (a checkpoint every few changes), and its tables in memory."""
    (tmp_path / 'events.jsonl').write_bytes(b''.join(event_lines(session(changes=True))))
    m = cli.package(tmp_path / 'events.jsonl', tmp_path / 'package')
    return m, _parquet.Columns.load(tmp_path / 'package')


def test_a_recording_with_book_changes_is_verified_as_python_does(tmp_path, changed):
    m, columns = changed
    package = tmp_path / 'package'
    depths = pq.read_table(package / _parquet.DEPTHS)
    assert depths.schema.equals(_parquet.DEPTH_CHANGES_SCHEMA, check_metadata=True)
    assert 0 < depths.column('checkpoint').null_count < len(depths), 'some rows are checkpoints, most are not'
    assert reference_verifier.verify_broker(package, m) == _parquet.verify_broker(package, m) == m['summary']
    assert json.loads(_native.run(['verify', package])) == m['summary']
    # The full checks agree, and so does the summary the columns give: the size of the changes is counted as Python does.
    assert schema.validate_batches(columns.iter_batches()) == m['summary'] == columns.summary()


@pytest.mark.parametrize('rows', [2, 3])
def test_row_groups_carry_each_issues_books_on_to_the_next(tmp_path, changed, monkeypatch, rows):
    m, columns = changed
    monkeypatch.setattr(_parquet, 'ROW_GROUP', rows)
    target = spoiled(tmp_path, columns, 'groups')
    assert pq.read_metadata(target / _parquet.DEPTHS).num_row_groups > 20
    assert reference_verifier.verify_broker(target) == _parquet.verify_broker(target) == m['summary']
    for threads in (1, 2, 3):   # groups are checked in waves of four to a thread: an issue's rows run on across waves
        assert json.loads(_native.run(['verify', target, '--row-group', rows, '--threads', threads])) == m['summary']


def test_the_default_checkpoint_interval_is_the_same_in_python_and_brisk_parquet(tmp_path):
    # No checkpoints fixture: a thousand changes of an issue make one.
    (tmp_path / 'events.jsonl').write_bytes(b''.join(event_lines(session(frames=600, changes=True))))
    m = cli.package(tmp_path / 'events.jsonl', tmp_path / 'package')
    points = pq.read_table(tmp_path / 'package' / _parquet.DEPTHS).column('checkpoint')
    assert 0 < len(points) - points.null_count < 10
    assert _parquet.verify_broker(tmp_path / 'package', m) == m['summary']


def first_change(columns, row=0):
    return int(columns.changes[0][row])


def spoil_a_quantity(c):
    c.changes[1][first_change(c, 5), 2] += 1   # a later quote's change shows a level of its list with another quantity


def spoil_an_emptying(c):
    c.changes[1][first_change(c), 2:] = 0      # the first quote empties a level the empty book does not have


def spoil_the_order(c):
    at = first_change(c)
    c.changes[1][[at, at + 1]] = c.changes[1][[at + 1, at]]


def spoil_a_shares_without_orders(c):
    c.changes[1][first_change(c), 3] = 0


def spoil_a_zero_price(c):
    c.changes[1][first_change(c), 1] = 0


@pytest.mark.parametrize('spoil, message', [
    (spoil_a_quantity, 'Book changes do not give the quote\'s book'),
    (spoil_an_emptying, 'Book change empties a level the book does not have'),
    (spoil_the_order, 'Book changes out of order'),
    (spoil_a_shares_without_orders, 'Invalid book change'),
    (spoil_a_zero_price, 'Invalid book change'),
])
def test_book_changes_that_break_a_rule_are_refused_with_the_same_words_by_all_three(tmp_path, changed, spoil, message):
    """Python's full rules (schema), brisk-parquet and the reference verifier, on changes written with the checkpoints the
    unspoiled ones had (as the rest of the recording stays, so only the changes are wrong)."""
    _, columns = changed
    points = columns.checkpoints(np.arange(len(columns)))
    spoil(columns)
    columns.checkpoints = lambda order: points
    assert three_verdicts(tmp_path, columns, 'spoiled') == (message,) * 3


def spoiled_table(tmp_path, m, change):
    """The verdicts of the reference verifier and brisk-parquet on the package with its depths table changed by `change`
    (a function of the table, which gives the new one)."""
    target = tmp_path / 'spoiled-table'
    shutil.rmtree(target, ignore_errors=True)
    shutil.copytree(tmp_path / 'package', target)
    rewrite(target, _parquet.DEPTHS, change(pq.read_table(target / _parquet.DEPTHS)))
    return outcome(reference_verifier.verify_broker, target, m), outcome(_parquet.verify_broker, target, m)


def with_cells(table, name, change):
    """The table with the cells of column `name` (a list of lists of dicts) changed by `change(cells)`."""
    cells = table.column(name).to_pylist()
    change(cells)
    at = table.schema.get_field_index(name)
    return table.set_column(at, table.schema.field(at), pa.array(cells, table.schema.field(at).type))


def a_checkpoint(cells, present=True):
    return next(i for i, cell in enumerate(cells) if i and (cell is not None) == present)


@pytest.mark.parametrize('how', ['dropped', 'added', 'size', 'short', 'swapped', 'emptied', 'longer'])
def test_a_checkpoint_where_it_should_not_be_or_not_what_it_should_be_is_refused(tmp_path, changed, how):
    m, _ = changed

    def change(cells):
        i = a_checkpoint(cells)
        if how == 'dropped':
            cells[i] = None
        elif how == 'added':
            cells[a_checkpoint(cells, present=False)] = cells[i]
        elif how == 'size':
            cells[i][0]['size'] += 1
        elif how == 'short':
            cells[i].pop()
        elif how == 'swapped':
            cells[i][0], cells[i][1] = cells[i][1], cells[i][0]
        elif how == 'emptied':
            cells[i] = []
        else:
            cells[i].append(cells[i][-1])
    verdicts = spoiled_table(tmp_path, m, lambda table: with_cells(table, 'checkpoint', change))
    assert verdicts == (f'Non-canonical {_parquet.DEPTHS}',) * 2


def test_a_book_that_is_not_one_of_the_four_is_refused(tmp_path, changed):
    m, _ = changed

    def change(cells):
        cells[3][0]['book'] = 9
    verdicts = spoiled_table(tmp_path, m, lambda table: with_cells(table, 'changes', change))
    assert verdicts == ('Invalid book change',) * 2


def test_a_depths_table_with_one_of_its_two_columns_more_is_not_the_schema(tmp_path, changed):
    m, _ = changed
    for drop in ('changes', 'checkpoint'):
        verdicts = spoiled_table(tmp_path, m, lambda table: table.drop_columns([drop]))
        assert verdicts == ('Unexpected schema in data/order_book_depths.parquet',) * 2


def test_the_formats_before_book_changes_do_not_take_them(tmp_path, changed):
    m, _ = changed
    with pytest.raises(ValueError, match='Unexpected schema'):
        _parquet.verify_broker(tmp_path / 'package', dict(m, schema=schema.LEGACY_SCHEMA))


@pytest.mark.parametrize('seed', range(8))
def test_a_cell_changed_anywhere_in_a_recording_with_book_changes_is_refused_by_both_or_neither(tmp_path, changed, seed):
    m, _ = changed
    disagreements, refused = mutations(tmp_path, m, seed)
    assert not disagreements, disagreements
    assert refused > 10


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
    # With book changes, a piece starts from each issue's whole books (all their levels, as from empty), and no checkpoint.
    assert ('changes' in after.column_names) == (latest.changes is not None)
    if latest.changes is not None:
        assert after.column('changes').to_pylist() == _parquet._change_list(*latest.changes).to_pylist()
        assert after.column('checkpoint').null_count == len(after)
    return result


@pytest.fixture(params=[False, True], ids=['plain', 'book changes'])
def segment(request, tmp_path):
    """A spooled segment of a session, with book changes (a checkpoint after every few) or without."""
    if request.param:
        request.getfixturevalue('checkpoints')
    spool = spooled(tmp_path, session(changes=request.param))
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


def test_a_segment_of_parts_with_and_without_quotes_is_packaged_as_python_does_with_book_changes(tmp_path, checkpoints):
    # A part of a history batch, or of the end alone, has no quotes: its depths table does not say whether they have book changes.
    spool, _ = many_parts(tmp_path, 500, changes=True)
    parts = spool.parts()
    assert any(pq.read_metadata(part / _parquet.DEPTHS).num_rows == 0 for part in parts)
    result = two_ways(tmp_path, spool, parts)
    assert result['summary']['batches'] == 500
    assert json.loads(_native.run(['verify', tmp_path / 'rust'])) == result['summary']


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


@pytest.mark.parametrize('changes', [False, True], ids=['plain', 'book changes'])
def test_a_segment_cut_short_is_repaired(tmp_path, changes, request):
    if changes:
        request.getfixturevalue('checkpoints')
    spool = spooled(tmp_path, session(changes=changes)[:30])
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


@pytest.mark.parametrize('changes', [False, True], ids=['plain', 'book changes'])
def test_a_part_is_the_same_tables_python_writes(tmp_path, changes):
    from test_spool import lines as as_lines
    batches = session(changes=changes)
    every = as_lines(batches)
    header = part_header(every[0])
    midnight = _parquet.midnight_ns(header['trading_date'])
    # (A part of a history batch alone has no quotes to say whether they have book changes: its depths table is the plain one.)
    for name, chunk in (('whole', every), ('later', every[5:20]), ('end', every[-4:]), ('one', every[:1]), ('history', every[1:2])):
        target = tmp_path / name
        python = python_part(tmp_path / name, chunk, header)
        counts = _native.part(chunk, target / 'rust', midnight)
        same_tables(target / 'rust', python, (_parquet.BATCHES, _parquet.DEPTHS, _parquet.TRADES))
        assert counts['batches'] == len(chunk)
        names = pq.read_schema(target / 'rust' / _parquet.DEPTHS).names
        assert ('changes' in names) == (changes and name != 'history') == ('checkpoint' in names)
        if 'changes' in names:
            assert pq.read_table(target / 'rust' / _parquet.DEPTHS).column('checkpoint').null_count == counts['quotes']


def test_quotes_with_and_without_book_changes_are_refused(tmp_path):
    from test_spool import lines as as_lines
    batches = session(changes=True)
    for q in batches[5]['quotes']:
        del q[schema.BOOK_CHANGES]
    every = as_lines(batches)
    midnight = _parquet.midnight_ns('20260311')
    with pytest.raises(ValueError, match='Quotes with and without book changes'):   # in lines read on other threads
        _native.part(every, tmp_path / 'many', midnight, threads=64)
    with pytest.raises(ValueError, match='Unknown or missing quote fields'):   # in the lines of one thread
        _native.part(every, tmp_path / 'one', midnight, threads=1)
    with pytest.raises(ValueError, match='Unknown or missing quote fields'):
        _parquet.Columns.from_batches(batches)
    for message, change in (('Invalid book change', lambda c: c.append([1, 2, 3])), ('Invalid book change', lambda c: c.__setitem__(0, [9, 1, 1, 1])),
                            ('Invalid negative value', lambda c: c.__setitem__(0, [0, -1, 1, 1]))):
        bad = session(changes=True)
        change(bad[3]['quotes'][0][schema.BOOK_CHANGES])
        with pytest.raises(ValueError, match=message):
            _native.part(as_lines(bad), tmp_path / 'bad', midnight)


def test_parts_with_and_without_book_changes_are_not_packaged_together(tmp_path):
    spool, batches = many_parts(tmp_path, 12, changes=True)
    midnight = _parquet.midnight_ns(batches[0]['trading_date'])
    plain = session(frames=9)
    from test_spool import lines as as_lines
    _native.part(as_lines(plain)[6:7], spool.path / 'parts' / '000007', midnight)   # a part of batch 6, without
    assert pq.read_schema(spool.path / 'parts' / '000007' / _parquet.DEPTHS).names == _parquet.DEPTH_SCHEMA.names
    with pytest.raises(ValueError, match='Quotes with and without book changes'):
        _native.package(spool.path, spool.parts(), tmp_path / 'out', midnight)
    with pytest.raises(ValueError, match='Quotes with and without book changes'):
        _native.state(spool.path, spool.parts(), midnight, tmp_path / 'state.parquet', day_start=spool.parts()[0])


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
