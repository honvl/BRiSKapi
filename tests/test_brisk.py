"""The BRiSK site client (SBI, e-shiten) with pybrisk's SBI sample payloads and a fake HTTP layer (no account needed)."""
import base64
import datetime as dt
import io
import json
import os
import re
from pathlib import Path
import stat
import sys
import threading
import time
import urllib.error
import warnings
import zlib

from briskapi.timing import TimingStats

import pytest

import briskapi
import briskapi.cli as cli
import briskapi.schema as schema
from briskapi import _live, _parquet, _spool, brisk
from briskapi._recording import JST, NotFoundError

CHECK_SESSION = brisk._check_session  # stubbed for every test below; the session tests put it back

BOOT = {'result': True, 'user_id': 'test-uuid', 'api_token': 'v2.local.testtoken', 'api_endpoint': 'https://api.brisk.jp'}
APP_BOOT = {'result': True, 'series': 0, 'date': '2026-03-11', 'session_status': 'running',
            'ws_url': '/realtime/0?session=abc', 'master': 'masterhash', 'snapshot': 'snaphash',
            'schedule_info': {'morning_session_pre_open_time': 28800000000, 'morning_session_open_time': 32400000000,
                              'morning_session_close_time': 41400000000, 'afternoon_session_pre_open_time': 43500000000,
                              'afternoon_session_open_time': 45000000000, 'afternoon_session_pre_close_time': 55500000000,
                              'afternoon_session_close_time': 55800000000, 'sq_jump_interval': 180}}
OHLC = {'ohlc5min': [{'date': '2026-03-11', 'index': 0, 'diff': 0, 'open_price': 2000, 'high_price': 2050,
                      'low_price': 1980, 'close_price': 2030, 'turnover': 100000000}],
        'ohlc1day': [{'date': '2026-03-11', 'open_price': 2000, 'high_price': 2100, 'low_price': 1950,
                      'close_price': 2080, 'turnover': 500000000}],
        'ohlc1week': [{'year': 2026, 'week': 10, 'open_price': 1, 'high_price': 2, 'low_price': 1, 'close_price': 2, 'turnover': 3}],
        'ohlc1month': [{'year': 2026, 'month': 3, 'open_price': 1, 'high_price': 2, 'low_price': 1, 'close_price': 2, 'turnover': 3}]}
JSFC = {'0': {'date': '2026-03-11', 'kakuhoLongShares': 100000, 'kakuhoShortShares': 200000, 'gyakuhibuFee': 0.05}}
MARKETS = {'market_conditions': [{'index': 0, 'issue_code': '3655', 'kind': 1, 'type': 6, 'price10': 26910,
                                  'value10': 2691000000, 'diff_bps_from_last': 0, 'time': '08:00:00.048598'}]}


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass


class Opener:
    def __init__(self, routes):
        self.routes, self.requests = routes, []

    def open(self, request, timeout):
        self.requests.append(request)
        path = request.full_url.removeprefix(brisk.ORIGIN).split('?')[0]
        value = self.routes.get(path, 404)
        if isinstance(value, int):
            raise urllib.error.HTTPError(request.full_url, value, 'error', {}, io.BytesIO(b'details'))
        return Response(value if isinstance(value, bytes) else json.dumps(value).encode())


def client(**routes):
    opener = Opener({'/api/frontend/boot': BOOT, '/api/app/boot': APP_BOOT, **routes})
    session = brisk.Session({'session_bfaf77a2': 'v2.local.cookie'}, rate_limit=0, opener=opener)
    return brisk.Client(session=session), opener


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.delenv('BRISK_SBI_COOKIES', raising=False)
    monkeypatch.delenv('BRISK_ESHITEN_COOKIES', raising=False)
    monkeypatch.delenv('BRISK_UPLOAD', raising=False)
    monkeypatch.setattr(brisk, '_client', None)
    monkeypatch.setattr(briskapi, '_current', None)
    monkeypatch.setattr(brisk, '_check_session', lambda session: None)  # fake hosts below have no HTTP site behind them


def test_boot_and_headers():
    c, opener = client()
    assert c.date == '2026-03-11' and c.boot['series'] == 0
    boot, app = opener.requests
    assert boot.get_header('Cookie') == 'session_bfaf77a2=v2.local.cookie' and not boot.has_header('Authorization')
    assert app.get_header('Authorization') == 'Bearer v2.local.testtoken'
    c.boot  # cached: no further requests
    assert len(opener.requests) == 2


def test_ticker_candles_and_margin():
    c, opener = client(**{'/api/ohlc/7203': OHLC, '/api/jsfc/7203': JSFC})
    t = briskapi.Ticker('7203', client=c)
    five = t.candles('5m')
    assert five == [{'date': '2026-03-11', 'bar': 0, 'open': 2000, 'high': 2050, 'low': 1980, 'close': 2030,
                     'turnover': 100000000}]
    assert t.candles()[0]['close'] == 2080 and t.candles('1w')[0]['week'] == 10 and t.candles('1mo')[0]['month'] == 3
    assert 'date=2026-03-11' in opener.requests[2].full_url
    with pytest.raises(ValueError, match='interval'):
        t.candles('1h')
    margin = t.margin(days=30)
    assert margin[0]['long_balance'] == 100000 and margin[0]['lending_fee'] == 0.05 and margin[0]['lending_fee_max'] is None
    assert 'count=30' in opener.requests[-1].full_url


def test_market_endpoints():
    groups = base64.b64encode(zlib.compress(json.dumps({'groups': [{'items': [{'code': '7203'}, {'x': 1}]}]}).encode()))
    flat = base64.b64encode(zlib.compress(json.dumps(['6758', {'code': '9984'}, {'x': 1}]).encode()))
    c, opener = client(**{
        '/api/stocks_info': [{'issue_code': '7203', 'turnover': 5000000000, 'calc_shares_outstanding': 1000000000}],
        '/api/stock_lists': {'version': '1', 'stock_lists': [{'id': 'nk225etf', 'name': 'NK225', 'issue_codes': ['1332', '7203']}]},
        '/api/markets': MARKETS,
        '/api/frontend/watchlist': {'empty': False, 'data': groups.decode()}})
    m = briskapi.Market(client=c)
    assert m.turnover() == [{'code': '7203', 'turnover': 5000000000, 'shares_outstanding': 1000000000}]
    assert m.lists() == {'nk225etf': ['1332', '7203']}
    event = m.events()[0]
    assert event['price'] == 2691.0 and event['value'] == 269100000.0 and event['code'] == '3655'
    assert event['change_bps'] == 0 and event['time'] == dt.datetime(2026, 3, 11, 8, 0, 0, 48598, tzinfo=JST)
    assert 'series=0' in opener.requests[-1].full_url and 'index_to=618' in opener.requests[-1].full_url
    schedule = m.schedule()
    assert schedule['status'] == 'running' and schedule['morning_open'] == dt.datetime(2026, 3, 11, 9, tzinfo=JST)
    assert schedule['afternoon_close'].hour == 15 and schedule['afternoon_close'].minute == 30
    assert m.watchlist() == ['7203']
    opener.routes['/api/frontend/watchlist'] = {'empty': False, 'data': flat.decode()}
    assert m.watchlist() == ['6758', '9984']
    opener.routes['/api/frontend/watchlist'] = {'empty': True}
    assert m.watchlist() == []


@pytest.mark.parametrize('status,error', [(401, brisk.SessionExpiredError), (302, brisk.SessionExpiredError),
                                          (404, briskapi.NotFoundError), (429, brisk.RateLimitError), (500, brisk.APIError)])
def test_errors(status, error):
    c, _ = client(**{'/api/ohlc/7203': status})
    with pytest.raises(error):
        c.candles('7203')
    assert str(brisk.APIError(500)) == 'HTTP 500'


def test_session_rules(monkeypatch):
    for cookies in ({}, None, {'session_bfaf77a2': ''}, []):
        with pytest.raises(brisk.SessionExpiredError, match='login'):
            brisk.Session(cookies)
    assert brisk._NoRedirect().redirect_request(None, None, 302, 'Found', {}, 'https://elsewhere.test/') is None
    slept = []
    monkeypatch.setattr(brisk.time, 'sleep', slept.append)
    session = brisk.Session({'a': 'b'}, rate_limit=2, opener=Opener({'/raw': b'\x00\x01'}))
    assert session.get('/raw', raw=True) == b'\x00\x01'
    session.get('/raw', raw=True)
    assert slept and 0 < slept[0] <= 0.5


def test_login_sources(monkeypatch, tmp_path):
    with pytest.raises(brisk.SessionExpiredError, match='needs a session'):
        briskapi.Ticker('7203').candles()
    with pytest.raises(brisk.SessionExpiredError):
        briskapi.login()
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"from": "env"}')
    assert briskapi.login().session.cookies == {'from': 'env'}
    monkeypatch.delenv('BRISK_SBI_COOKIES')
    briskapi.login(cookies={'saved': 'yes'}, remember=True)
    path = brisk.cookies_path()
    if os.name == 'posix':
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    assert json.loads(path.read_text()) == {'saved': 'yes'}
    assert briskapi.login().session.cookies == {'saved': 'yes'}
    brisk._client.session._opener = Opener({'/api/frontend/boot': BOOT, '/api/app/boot': APP_BOOT})
    brisk._client.session.rate_limit = 0
    assert briskapi.Market().schedule()['date'] == '2026-03-11'  # default session used
    briskapi.logout()
    assert not path.exists() and brisk._client is None


OPEN_US = 9 * 3600 * 1_000_000
MIDNIGHT_MS = int(dt.datetime(2026, 3, 11, tzinfo=JST).timestamp() * 1000)


def sbi_session(frames=150):
    """Bootstrap, `frames` one-quote batches 100 ms apart (20 ms old on receipt, one 1.5 s stall) and end."""
    q = {**dict.fromkeys(schema.QUOTE_KEYS, 0), **{key: [] for key in schema.BOOK_KEYS},
         'issue_id': 0, 'code': '7203', 'frame': 1, 'max_frame': 1, 'source_time_us': OPEN_US,
         'indicative_price10': 101500, 'market_buy_quantity': 3, 'market_sell_quantity': 1}
    master = {**dict.fromkeys(schema.MASTER_KEYS, 0), 'issue_id': 0, 'code': '7203', 'name': 'Toyota', 'lot_size': 100}
    batches = [dict(type='bootstrap', seq=0, source='brisk_live', trading_date='20260311', source_time_us=OPEN_US,
                    input_transport={**schema.BROKER_TRANSPORTS['sbi'], 'decoder': '/private/decoder.js', 'setup_ms': 1.123456},
                    source_timestamp_origin='brisk_decoder_unverified', exchange_delay_ms=None,
                    market_issue_count=1, master=[master], quotes=[q], trades=[])]
    for i in range(1, frames + 1):
        now = OPEN_US + i * 100_000 + (1_400_000 if i > 100 else 0)
        batches.append(dict(type='quotes', seq=i, source_time_us=now, received_unix_ms=MIDNIGHT_MS + now // 1000 + 20,
                            decode_ns=250_000 + i * 1000, replay_lateness_ms=None,
                            quotes=[{**q, 'frame': i + 1, 'max_frame': i + 1, 'source_time_us': now,
                                     'last_price10': 101500 + 100 * (i == frames)}], trades=[]))
    return batches + [dict(type='end', seq=frames + 1, source_time_us=now, frames=frames, quote_updates=frames,
                          replay_wall_ms=16_400)]


@pytest.fixture
def fake_host(tmp_path, monkeypatch):
    """A stand-in for brisk.cjs that checks its cookies and prints a short live session."""
    batches = sbi_session()
    script = tmp_path / 'sbi_fake.cjs'
    script.write_text(
        "const c = JSON.parse(process.env.BRISK_SBI_COOKIES); if (c.session_bfaf77a2 !== 'v') process.exit(9);\n"
        f"console.error(JSON.stringify(process.argv.slice(2)));\n"
        f"for (const b of {json.dumps(batches)}) console.log(JSON.stringify(b));\n")
    monkeypatch.setattr(brisk, 'DECODER', script)


def test_live_feed_via_node(fake_host, capfd):
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    feed = briskapi.connect(site='sbi', codes=['7203'], history=True, upload=False)
    assert briskapi.current() is feed and feed.source == 'brisk_live'
    feed.wait()
    assert briskapi.Ticker('7203').quote()['last_price'] == 10160.0 and feed.uploaded is None
    assert len(briskapi.Ticker('7203').history()) == 151 and feed.timing_uploaded is None
    # Cookies reach the host through its environment, never its (world-readable) arguments.
    assert capfd.readouterr().err.strip() == '["--codes","7203"]'


def test_connect_signs_in_from_the_sites_variable_when_login_has_not(fake_host, monkeypatch):
    briskapi.logout()
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    feed = briskapi.connect(site='sbi', upload=False).wait()
    assert feed.source == 'brisk_live' and brisk._default().site == 'sbi'


def test_connect_takes_a_site_and_only_its_options(tmp_path):
    for site in (None, 'web', 'rakuten'):
        with pytest.raises(ValueError, match="Choose a site: 'demo', 'sbi', 'e-shiten', 'matsui', 'monex', 'smbcnikko'"):
            briskapi.connect(site=site)
    with pytest.raises(ValueError, match='cache, speed: only for the demo'):
        briskapi.connect(site='sbi', cache=tmp_path, speed=0)
    with pytest.raises(ValueError, match='trace_protocol, upload_market_data: only for a broker site'):
        briskapi.connect(site='demo', cache=tmp_path, trace_protocol=True, upload_market_data=False)
    with pytest.raises(TypeError, match='cookies by name'):
        briskapi.login({'session_bfaf77a2': 'v'})


def test_protocol_options_reach_the_host_without_putting_the_profile_in_argv(tmp_path, monkeypatch, capfd):
    script = tmp_path / 'sbi_options.cjs'
    script.write_text(
        "console.error(JSON.stringify({argv: process.argv.slice(2), profile: process.env.BRISK_PROFILE || null}));\n"
        f"for (const b of {json.dumps(sbi_session())}) console.log(JSON.stringify(b));\n")
    monkeypatch.setattr(brisk, 'DECODER', script)
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    profile = {'connectQuery': {'_v': 'build-9'}, 'catchUp': 'issue-lists'}
    briskapi.connect(site='sbi', codes=['7203'], trace_protocol=True, profile=profile, upload=False).wait()
    seen = json.loads(capfd.readouterr().err.strip())
    assert seen['argv'] == ['--codes', '7203', '--trace-protocol']
    assert json.loads(seen['profile']) == profile
    assert 'build-9' not in ' '.join(seen['argv'])
    briskapi.connect(site='sbi', codes=['7203'], upload=False).wait()
    assert json.loads(capfd.readouterr().err.strip()) == {'argv': ['--codes', '7203'], 'profile': None}


def test_live_feed_failure_closes(fake_host, tmp_path, monkeypatch):
    briskapi.login(cookies={'session_bfaf77a2': 'wrong'})
    with pytest.raises(briskapi.BriskError, match='before bootstrap'):
        briskapi.connect(site='sbi')


def test_cli_live_sbi(fake_host, monkeypatch, capsys):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO('\n'))
    sent, market = [], []
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: sent.append(report) or {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload', lambda *args, **kwargs: market.append(args) or {'status': 'stored'})
    cli.main(['live', '--site', 'sbi', '--codes', '7203', '--quotes'])
    out = capsys.readouterr()
    lines = [json.loads(line) for line in out.out.splitlines()]
    # The one-time question names what a broker session uploads: a timing summary and the market recording.
    assert lines[-1]['last_price'] == 10160.0 and 'timing summary' in out.err and 'decoded market recording' in out.err
    assert cli.load_consent()['enabled'] and sent[0]['source'] == 'brisk_live' and len(market) == 1
    assert not {'quotes', 'master', 'code', 'price'} & set(json.dumps(sent[0]).replace('"', ' ').split())


def test_cli_live_says_what_it_saves_and_says_saved_only_once_it_is(fake_host, monkeypatch, capsys, tmp_path):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    monkeypatch.setattr(cli, 'SAVE_SECONDS', {True: 0, False: 0})
    briskapi.consent(accept=True)
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: {'status': 'stored'})

    def publish(directory, url, progress=None, **options):
        progress('preparing'); progress('uploading', 0, 4); progress('uploading', 4, 4); progress('validating')
        return {'status': 'stored'}
    monkeypatch.setattr(cli, 'upload', publish)
    saved = cli.Saving.saved

    def checked(self, path):
        assert (Path(path) / 'manifest.json').exists(), 'said saved before the recording was'
        saved(self, path)
    monkeypatch.setattr(cli.Saving, 'saved', checked)
    cli.main(['live', '--site', 'sbi', '--no-gateway', '--quotes', '--output', str(tmp_path / 'days')])
    out = capsys.readouterr()
    assert all(json.loads(line) for line in out.out.splitlines()), 'stdout stays the quotes, as JSON lines'
    told = [line.removeprefix('[brisk] sbi 2026-03-11 from 09:00:00: ') for line in out.err.splitlines()
            if line.startswith('[brisk] sbi 2026-03-11 from 09:00:00: ')]
    kept = tmp_path / 'days' / 'sbi_20260311_090000-090016'
    position = -1
    for fragment in ['preparing to save', 'writing batches', 'writing quotes', 'finalizing', f'saved to {kept} in ',
                     'preparing to upload', 'uploading ', 'sent; waiting for the archive', 'uploaded; the archive stored it']:
        position = next((i for i, line in enumerate(told) if i > position and fragment in line), None)   # in this order
        assert position is not None, (fragment, told)
    assert 'is recorded' not in out.err


def test_cli_live_says_a_reconnection_that_got_no_update_has_nothing_to_save(tmp_path, monkeypatch, capsys):
    # The stream was lost and came back in the lunch break: the segment is a book, the day's history and its end.
    clock = 11 * 3600 * 10**6 + 2700 * 10**6
    bootstrap, end = sbi_session()[0], sbi_session()[-1]
    bootstrap['source_time_us'] = end['source_time_us'] = clock
    end.update(seq=1, frames=0, quote_updates=0)
    script = tmp_path / 'sbi_fake.cjs'
    script.write_text(f"for (const b of {json.dumps([bootstrap, end])}) console.log(JSON.stringify(b));\n")
    monkeypatch.setattr(brisk, 'DECODER', script)
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    monkeypatch.setattr(cli, 'SAVE_SECONDS', {True: 0, False: 0})
    cli.main(['live', '--site', 'sbi', '--no-gateway', '--quotes', '--no-upload', '--output', str(tmp_path / 'days')])
    err = capsys.readouterr().err
    assert '[brisk] sbi 2026-03-11 from 11:45:00: no update came until 11:45:00; nothing to save' in err
    assert 'saved to' not in err and not (tmp_path / 'days').exists(), 'no recording was made of it'


def told_of_the_day(err):
    return [line.removeprefix('[brisk] sbi 2026-03-11 from 09:00:00: ') for line in err.splitlines()
            if line.startswith('[brisk] sbi 2026-03-11 from 09:00:00: ')]


def test_cli_live_says_a_save_failed_and_says_saved_only_where_it_kept_it_instead(fake_host, monkeypatch, capsys, tmp_path):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    monkeypatch.setattr(cli, 'package_parts', lambda *a, **k: 1 / 0)
    cli.main(['live', '--site', 'sbi', '--no-gateway', '--no-upload', '--output', str(tmp_path / 'days')])
    err = capsys.readouterr().err   # the feed's warning of it may not reach a terminal: these lines do
    told = told_of_the_day(err)
    kept = _spool.kept_dir() / 'sbi_20260311_090000-090016'
    assert re.fullmatch(r'Packaging failed after \d+s: division by zero', told[0]), told
    # The feed's own warning of it, raised on its worker thread long after connect() returned, is shown too.
    assert re.search(r'^Packaging failed: division by zero; the recording is kept in .*sbi_20260311_090000-090016$', err, re.M), err
    # What was recorded is then kept apart, with the progress of packaging it again, and only then is it saved: there.
    assert told[1].startswith(f'keeping what was recorded in {_spool.kept_dir()}') and told[2].startswith('preparing to save')
    assert re.fullmatch(rf'saved to {re.escape(str(kept))} in \d+s', told[-1]) and (kept / _parquet.DEPTHS).exists() and not (tmp_path / 'days').exists()
    assert not any('saved to' in line for line in told[:-1])


def test_cli_live_says_an_upload_failed_and_how_to_retry_it_from_where_the_package_is_kept(fake_host, monkeypatch, capsys):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    briskapi.consent(accept=True)
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: {'status': 'stored'})

    def refuse(directory, url, progress=None, **options):
        progress('preparing'); progress('validating')
        raise ValueError('Automatic validation rejected recording: bad')
    monkeypatch.setattr(cli, 'upload', refuse)
    cli.main(['live', '--site', 'sbi', '--no-gateway'])
    told = told_of_the_day(capsys.readouterr().err)
    kept = _spool.kept_dir() / 'sbi_20260311_090000-090016'
    in_order = iter(told)
    for fragment in ('preparing to save', 'finalizing', 'preparing to upload', 'sent; waiting for the archive',
                     'Automatic upload failed after', f'keeping what was recorded in {_spool.kept_dir()}',
                     f'saved to {kept} in '):
        assert any(fragment in line for line in in_order), (fragment, told)
    assert told[-1].endswith(f'; `brisk upload {kept}` retries the upload') and not any('stored' in line for line in told)


@pytest.mark.parametrize('keep,why', [(lambda self, root, progress=None: 1 // 0, r'(integer )?division( or modulo)? by zero'),
                                      (lambda self, root, progress=None: None, 'there is nothing of it to keep')])
def test_cli_live_says_when_even_keeping_a_failed_save_failed(fake_host, monkeypatch, capsys, tmp_path, keep, why):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    monkeypatch.setattr(cli, 'package_parts', lambda *a, **k: 1 / 0)
    monkeypatch.setattr(_spool.Spool, 'keep', keep)
    cli.main(['live', '--site', 'sbi', '--no-gateway', '--no-upload', '--output', str(tmp_path / 'days')])
    err = capsys.readouterr().err
    told = told_of_the_day(err)
    assert re.fullmatch(rf'Keeping the recording failed after \d+s: {why}', told[-1]), told
    assert not any('saved to' in line for line in told)
    left, = _spool.spool_root().glob('segment-*')   # nothing of the day was saved: its spool is not deleted
    assert (left / 'parts').is_dir() and f'its spool is left in {left}' in err


def test_a_day_nothing_of_which_could_be_saved_is_finished_by_the_next_feed_from_its_spool(fake_host, monkeypatch, capsys, tmp_path):
    # Both saves failed (10/23: the program never started on Windows, of too many parts), and `finally: spool.remove()` deleted
    # the day. Now the spool stays, and the next feed to start finishes it as a crashed session's (Feed._recover).
    import subprocess as sp
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    with pytest.MonkeyPatch.context() as broken:
        broken.setattr(cli, 'package_parts', lambda *a, **k: 1 / 0)
        broken.setattr(_spool.Spool, 'keep', lambda self, root, progress=None: 1 // 0)
        cli.main(['live', '--site', 'sbi', '--no-gateway', '--no-upload', '--output', str(tmp_path / 'days')])
    err = capsys.readouterr().err
    left, = _spool.spool_root().glob('segment-*')
    assert re.search(rf'^Nothing of the recording could be saved; its spool is left in {re.escape(str(left))}, which the next feed '
                     'to start finishes', err, re.M), err
    assert not (tmp_path / 'days').exists() and not any(_spool.kept_dir().glob('*'))
    dead = int(sp.run([sys.executable, '-c', 'import os; print(os.getpid())'], capture_output=True, text=True).stdout)
    owner = left / 'owner.json'
    owner.write_text(json.dumps({**json.loads(owner.read_text()), 'pid': dead}))
    cli.main(['live', '--site', 'sbi', '--no-gateway', '--no-upload', '--output', str(tmp_path / 'days2')])
    err = capsys.readouterr().err
    kept = tmp_path / 'days' / 'sbi_20260311_090000-090016'
    assert re.search(rf'stopped unexpectedly left sbi_20260311_090000-090016 behind: not uploaded, kept in {re.escape(str(kept))}', err), err
    assert (kept / 'manifest.json').exists() and (tmp_path / 'days2' / 'sbi_20260311_090000-090016' / 'manifest.json').exists()
    assert not any(_spool.spool_root().glob('segment-*'))


def test_a_feed_of_the_api_saves_quietly(fake_host, tmp_path, capsys):
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    feed = briskapi.connect(site='sbi', upload=False, output=tmp_path / 'days').wait()
    assert feed.recordings[-1]['path'] and '[brisk]' not in capsys.readouterr().err


def test_cli_live_shows_a_warning_as_it_is_raised_not_when_connect_returns(monkeypatch, capsys):
    # connect() waits for the next trading day when no session is running; what the feed's threads warn meanwhile (a spool
    # a stopped process left, recovered) must not wait for the day, nor be lost if the feed is stopped first.
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    seen = []

    def waiting(**options):
        thread = threading.Thread(target=lambda: warnings.warn('A feed that stopped unexpectedly left segment-x behind: kept'))
        thread.start(); thread.join()
        seen.append(capsys.readouterr().err)   # connect() has not returned
        raise KeyboardInterrupt
    monkeypatch.setattr(briskapi, 'connect', waiting)
    cli.main(['live', '--site', 'sbi', '--no-upload'])
    assert seen == ['A feed that stopped unexpectedly left segment-x behind: kept\n']
    assert capsys.readouterr().err.strip() == 'Stopped before a trading day began'


def test_brisk_live_puts_the_warnings_handler_back_and_leaves_other_warnings_to_it(monkeypatch, capsys):
    handled = []
    original = lambda message, category, filename, lineno, file=None, line=None: handled.append((str(message), category))  # noqa: E731
    monkeypatch.setattr(warnings, 'showwarning', original)
    with cli._warnings_on_stderr():
        warnings.warn('for the person at the terminal')
        warnings.warn('for the library to say', FutureWarning)
        assert warnings.showwarning is not original
    assert warnings.showwarning is original, 'catch_warnings does not restore it where warnings are kept per context'
    assert capsys.readouterr().err == 'for the person at the terminal\n'
    assert handled == [('for the library to say', FutureWarning)]


def test_cli_live_stopped_while_waiting_for_a_session_says_so_without_a_traceback(monkeypatch, capsys):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    def waiting(**options):
        raise KeyboardInterrupt   # Ctrl-C while connect() waits for the next trading day's session
    monkeypatch.setattr(briskapi, 'connect', waiting)
    cli.main(['live', '--site', 'sbi', '--no-upload'])
    assert capsys.readouterr().err.strip().endswith('Stopped before a trading day began')


def test_cli_trace_protocol_reaches_the_sbi_host(fake_host, monkeypatch, capfd):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    cli.main(['live', '--site', 'sbi', '--codes', '7203', '--trace-protocol', '--no-upload'])
    assert capfd.readouterr().err.count('["--codes","7203","--trace-protocol"]') == 1


def test_sbi_feed_uploads_timing(fake_host, monkeypatch):
    sent = []
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: sent.append((report, url)) or {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload', lambda *args, **kwargs: {'status': 'stored'})
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    with pytest.warns(UserWarning, match='by default'):
        feed = briskapi.connect(site='sbi', upload_market_data=False).wait()
    assert feed.timing_uploaded == {'status': 'stored'} and feed.uploaded is None
    report, url = sent[0]
    assert url == cli.settings()['api_url'] and 'contributor' not in report
    assert report['frames'] == 150 and report['stalls'] == 1 and report['trading_date'] == '20260311'
    assert report['first_minute'] == '09:00' and report['source_age_ms']['p50'] == 20
    assert report['interarrival_ms']['max'] == 1500 and report['decode_ms']['p50'] > 0.25
    assert briskapi.connect(site='sbi', upload=False).wait().timing_uploaded is None and len(sent) == 1
    monkeypatch.setattr(cli, 'upload_timing', lambda *a: 1 / 0)
    with pytest.warns(UserWarning, match='Timing upload failed'):
        assert briskapi.connect(site='sbi').wait().timing_uploaded['status'] == 'failed'


def test_cli_without_cookies_stops_before_prompt_or_decoder(monkeypatch, capsys):
    monkeypatch.setattr(cli, 'check_runtime', lambda *args: pytest.fail('decoder must not start'))
    monkeypatch.setattr(cli, 'ask_consent', lambda: pytest.fail('do not prompt without cookies'))
    with pytest.raises(SystemExit, match='No SBI BRiSK cookies'):
        cli.main(['live', '--site', 'sbi'])
    assert 'Decoder exited' not in capsys.readouterr().err


def test_an_expired_session_fails_in_one_line_before_the_decoder_starts(monkeypatch, capsys):
    import briskapi._live
    monkeypatch.setattr(brisk, '_check_session', CHECK_SESSION)
    monkeypatch.setattr(briskapi._live, 'Feed', lambda *a, **k: pytest.fail('decoder must not start'))
    opener = Opener({'/api/frontend/boot': 401})
    monkeypatch.setattr(brisk.urllib.request, 'build_opener', lambda *handlers: opener)
    briskapi.login(cookies={'session_bfaf77a2': 'expired'})
    with pytest.raises(brisk.SessionExpiredError, match='^SBI BRiSK session expired or invalid; log in again$'):
        briskapi.connect(site='sbi', codes=['7203'])
    assert [r.full_url for r in opener.requests] == [brisk.ORIGIN + '/api/frontend/boot']
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "expired"}')
    with pytest.raises(SystemExit) as stop:
        cli.main(['live', '--site', 'sbi', '--codes', '7203'])
    assert stop.value.code == 'brisk: error: SBI BRiSK session expired or invalid; log in again'
    assert 'Decoder exited' not in capsys.readouterr().err


def test_cookies_that_are_not_json_fail_in_one_line_without_their_value(monkeypatch):
    monkeypatch.setenv('BRISK_ESHITEN_COOKIES', '{session_bfaf77a2: secret-value}')
    with pytest.raises(brisk.SessionExpiredError, match='^BRISK_ESHITEN_COOKIES is not valid JSON') as error:
        briskapi.login(site='e-shiten')
    assert 'secret-value' not in str(error.value) and 'line 1 column 2' in str(error.value)
    with pytest.raises(SystemExit) as stop:
        cli.main(['live', '--site', 'e-shiten'])
    assert stop.value.code.startswith('brisk: error: BRISK_ESHITEN_COOKIES is not valid JSON') and '\n' not in stop.value.code
    monkeypatch.delenv('BRISK_ESHITEN_COOKIES')
    brisk.cookies_path().parent.mkdir(parents=True, exist_ok=True)
    brisk.cookies_path().write_text('session_bfaf77a2=secret-value')
    with pytest.raises(brisk.SessionExpiredError, match='is not valid JSON') as error:
        briskapi.login()
    assert str(brisk.cookies_path()) in str(error.value) and 'secret-value' not in str(error.value)


def test_sbi_market_upload_needs_uploads_on(fake_host, monkeypatch):
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(revoke=True)
    with pytest.raises(briskapi.BriskError, match='uploads on'):
        briskapi.connect(site='sbi', upload_market_data=True)
    # A choice made under an earlier policy no longer applies: the default does, and says so.
    cli.consent_path().write_text(json.dumps({'policy_version': 2, 'enabled': False}))
    monkeypatch.setattr(cli, 'upload', lambda *args, **kwargs: {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload_timing', lambda *args: {'status': 'stored'})
    with pytest.warns(UserWarning, match='by default'):
        assert briskapi.connect(site='sbi', upload_market_data=True).wait().uploaded == {'status': 'stored'}


def test_sbi_market_upload_follows_consent_and_opt_outs(fake_host, monkeypatch):
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    sent, timing = [], []
    def publish(directory, url, **options):
        m = json.loads((directory / 'manifest.json').read_text())
        _parquet.inspect_package(directory, m)
        sent.append(m)
        return {'status': 'stored'}
    monkeypatch.setattr(cli, 'upload', publish)
    monkeypatch.setattr(cli, 'upload_timing', lambda *args: timing.append(args) or {'status': 'stored'})
    # Once agreed, a clean session uploads its market recording too, without asking.
    feed = briskapi.connect(site='sbi').wait()
    assert feed.uploaded == {'status': 'stored'} and len(sent) == 1 and len(timing) == 1
    assert sent[0]['summary']['source'] == 'brisk_live' and set(sent[0]) == {'schema', 'sha256', 'bytes', 'summary'}
    assert not any(_spool.spool_root().glob('segment-*')), 'the day leaves no spool behind'
    assert briskapi.connect(site='sbi', upload_market_data=True).wait().uploaded == {'status': 'stored'} and len(sent) == 2
    # The opt-outs: the market recording alone, the whole session, or the whole process.
    feed = briskapi.connect(site='sbi', upload_market_data=False).wait()
    assert feed.uploaded is None and feed.timing_uploaded == {'status': 'stored'} and len(sent) == 2
    feed = briskapi.connect(site='sbi', upload=False).wait()
    assert feed.uploaded is None and feed.timing_uploaded is None and len(timing) == 3
    with pytest.raises(briskapi.BriskError, match='upload=False keeps the whole session local'):
        briskapi.connect(site='sbi', upload_market_data=True, upload=False)
    monkeypatch.setenv('BRISK_UPLOAD', '0')
    feed = briskapi.connect(site='sbi').wait()
    assert feed.uploaded is None and feed.timing_uploaded is None and len(sent) == 2 and len(timing) == 3
    briskapi.consent(revoke=True)
    monkeypatch.delenv('BRISK_UPLOAD')
    assert briskapi.connect(site='sbi').wait().uploaded is None and len(sent) == 2 and len(timing) == 3


@pytest.mark.parametrize('answer,uploaded', [('\n', True), ('y\n', True), ('n\n', False), ('', False)])
def test_cli_asks_once_then_uploads_sbi_captures_automatically(fake_host, monkeypatch, capsys, answer, uploaded):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    sent, timing = [], []
    monkeypatch.setattr(cli, 'upload', lambda *args, **kwargs: sent.append(args) or {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload_timing', lambda *args: timing.append(args) or {'status': 'stored'})
    for run in range(2):
        monkeypatch.setattr(cli.sys, 'stdin', io.StringIO(answer))
        cli.main(['live', '--site', 'sbi'])
        out = capsys.readouterr()
        assert ('Upload automatically to your private archive? [Y/n]' in out.err) == (run == 0)
        assert len(sent) == len(timing) == (run + 1 if uploaded else 0)
    assert cli.load_consent()['enabled'] == uploaded
    cli.main(['live', '--site', 'sbi', '--no-upload'])
    assert len(sent) == len(timing) == (2 if uploaded else 0)


@pytest.mark.parametrize('flags,market,timing', [((), 1, 1), (('--upload-market-data',), 1, 1),
                                                   (('--no-upload-market-data',), 0, 1), (('--no-upload',), 0, 0)])
def test_cli_sbi_upload_switches(fake_host, monkeypatch, flags, market, timing):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    briskapi.consent(accept=True)
    monkeypatch.setattr(cli, 'ask_consent', lambda: pytest.fail('asked once already'))
    sent = {'market': [], 'timing': []}
    monkeypatch.setattr(cli, 'upload', lambda *args, **kwargs: sent['market'].append(args) or {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload_timing', lambda *args: sent['timing'].append(args) or {'status': 'stored'})
    cli.main(['live', '--site', 'sbi', *flags])
    assert (len(sent['market']), len(sent['timing'])) == (market, timing)


def test_cleanup_of_an_exited_host_with_a_buffered_control_message():
    import subprocess
    process = subprocess.Popen([sys.executable, '-c', 'pass'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    process.wait(timeout=30)
    process.stdin.write(b'{"stop":true}\n')  # buffered until close, after the reader has exited
    _live._stop(process)
    assert process.stdin.closed and process.stdout.closed and process.returncode == 0


def test_a_feed_stopped_mid_day_ends_the_day_there_and_uploads_it(tmp_path, monkeypatch):
    # A host that, asked to stop over stdin, ends its day with its end, as brisk.cjs does on every OS.
    batches = sbi_session(frames=150)
    first = batches[:41]
    end = dict(batches[-1], seq=41, source_time_us=first[-1]['source_time_us'], frames=40, quote_updates=40)
    script = tmp_path / 'stoppable.cjs'
    script.write_text(f"""for (const b of {json.dumps(first)}) console.log(JSON.stringify(b));
require('node:readline').createInterface({{ input: process.stdin }}).on('line', line => {{
  if (JSON.parse(line).stop) process.stdout.write(JSON.stringify({json.dumps(end)}) + '\\n', () => process.exit(0));
}});
setInterval(() => {{}}, 1000);
""")
    monkeypatch.setattr(brisk, 'DECODER', script)
    sent = []
    def publish(directory, url, **options):
        m = json.loads((directory / 'manifest.json').read_text())
        _parquet.inspect_package(directory, m)
        sent.append(m['summary'])
        return {'status': 'stored'}
    monkeypatch.setattr(cli, 'upload', publish)
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: {'status': 'stored'})
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    feed = briskapi.connect(site='sbi', codes=['7203'], output=tmp_path / 'days')
    deadline = time.monotonic() + 20
    while feed.seq < 40:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    feed.close()
    assert feed.status == 'closed' and feed.uploaded == {'status': 'stored'}
    assert [s['batches'] for s in sent] == [42] and sent[0]['quote_updates'] == 41   # the bootstrap's, then 40
    name, = [day['name'] for day in feed.recordings]
    assert name == 'sbi_20260311_090000-090004' and (tmp_path / 'days' / name / 'manifest.json').exists()
    assert not any(_spool.kept_dir().glob('*')) and not any(_spool.spool_root().glob('segment-*'))


def test_a_day_whose_host_cannot_end_it_is_repaired_with_its_end_and_uploaded(tmp_path, monkeypatch):
    script = tmp_path / 'held.cjs'   # prints the bootstrap and a quote, then neither ends its day nor stops but by force
    script.write_text(''.join(f'console.log({json.dumps(json.dumps(b))});\n' for b in sbi_session()[:2]) + 'setInterval(() => {}, 1000);\n')
    monkeypatch.setattr(brisk, 'DECODER', script)
    sent = []
    def publish(directory, url, **options):
        m = json.loads((directory / 'manifest.json').read_text())
        _parquet.inspect_package(directory, m)
        sent.append(m['summary'])
        return {'status': 'stored'}
    monkeypatch.setattr(cli, 'upload', publish)
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    # Opened inside the check: on a free-threaded build, warnings are caught per context, which the feed's
    # threads inherit when they start.
    with pytest.warns(UserWarning, match='repaired with one and uploaded'):
        feed = briskapi.connect(site='sbi', upload_market_data=True)
        feed.close(timeout=0.5)
    assert feed.status == 'closed' and feed.uploaded == {'status': 'stored'}
    assert [s['batches'] for s in sent] == [3], 'the bootstrap, a quote and its repaired end'
    assert feed.recordings[-1]['repaired'] and feed.recordings[-1]['name'].startswith('sbi_20260311_')
    assert not any(_spool.kept_dir().glob('*')) and not any(_spool.spool_root().glob('segment-*'))


def test_a_day_whose_host_cannot_end_it_and_got_no_update_leaves_no_recording_to_repair(tmp_path, monkeypatch):
    script = tmp_path / 'held.cjs'   # the bootstrap only: no update ever came
    script.write_text(f'console.log({json.dumps(json.dumps(sbi_session()[0]))});\nsetInterval(() => {{}}, 1000);\n')
    monkeypatch.setattr(brisk, 'DECODER', script)
    monkeypatch.setattr(cli, 'upload', lambda *a, **k: pytest.fail('a book with no update is not uploaded'))
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    with warnings.catch_warnings(record=True) as caught:   # opened where the feed's threads inherit it (free-threaded builds)
        warnings.simplefilter('always')
        feed = briskapi.connect(site='sbi', upload_market_data=True)
        feed.close(timeout=0.5)
    assert feed.status == 'closed' and feed.recordings[-1]['name'] is None and feed.recordings[-1]['uploaded'] is None
    assert not [w for w in caught if 'ended early' in str(w.message)], 'nothing was repaired'
    assert not any(_spool.kept_dir().glob('*')) and not any(_spool.spool_root().glob('segment-*'))


def test_the_next_feed_repairs_and_uploads_a_crashed_sessions_day_as_that_session_would_have(fake_host, tmp_path, monkeypatch):
    import subprocess as sp
    dead = int(sp.run([sys.executable, '-c', 'import os; print(os.getpid())'], capture_output=True, text=True).stdout)
    def crashed(batches, intent):
        spool = _spool.Spool(intent=intent)
        for b in batches:
            spool.add(b, json.dumps(b).encode() + b'\n')
        spool.close_part(wait=True)
        spool._writer.shutdown()
        owner = spool.path / 'owner.json'
        owner.write_text(json.dumps({**json.loads(owner.read_text()), 'pid': dead}))
    crashed(sbi_session()[:30], {'upload': True, 'output': str(tmp_path / 'days')})
    crashed(sbi_session()[:20], {'upload': False, 'output': None})   # a gateway's only: its session kept nothing
    sent = []
    monkeypatch.setattr(cli, 'upload', lambda directory, url, **o: sent.append(
        json.loads((directory / 'manifest.json').read_text())['summary']['batches']) or {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: {'status': 'stored'})
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    with pytest.warns(UserWarning, match='stopped unexpectedly left sbi_20260311_090000-090002 behind: repaired'):
        briskapi.connect(site='sbi', codes=['7203']).wait()
    assert sorted(sent) == [31, 152], 'the crashed session\'s day, repaired, and this one\'s'
    assert [p.name for p in (tmp_path / 'days').iterdir()] == ['sbi_20260311_090000-090002']
    assert not any(_spool.spool_root().glob('segment-*'))


def two_days(tmp_path, monkeypatch, second=None):
    """A stand-in host that records two trading days back to back (each bootstrap ... end)."""
    first = sbi_session()
    second = second or [{**b, 'trading_date': '20260312'} if b['type'] == 'bootstrap' else
                        ({**b, 'quotes': [{**b['quotes'][0], 'last_price10': 102000}]} if b['type'] == 'quotes' else b)
                        for b in sbi_session(frames=120)]
    script = tmp_path / 'two_days.cjs'
    script.write_text(f'for (const b of {json.dumps(first + second)}) console.log(JSON.stringify(b));\n')
    monkeypatch.setattr(brisk, 'DECODER', script)


def later(batches, us):
    """The same session, `us` later on the clock (a reconnect's segment on the same trading day)."""
    def shift(item):
        item = dict(item)
        for key in ('source_time_us', 'time_us'):
            if key in item:
                item[key] += us
        if 'received_unix_ms' in item:
            item['received_unix_ms'] += us // 1000
        for key in ('quotes', 'trades'):
            if key in item:
                item[key] = [shift(x) for x in item[key]]
        return item
    return [shift(b) for b in batches]


def test_a_reconnect_keeps_and_uploads_the_segment_before_it_as_its_own_recording(tmp_path, monkeypatch):
    # The decoder closes a segment whose connection was lost with its own end, then records on from a new
    # snapshot of the same day: two recordings of that day, each named for its clocks.
    two_days(tmp_path, monkeypatch, second=later(sbi_session(frames=20), 3600 * 10**6))
    sent = []
    monkeypatch.setattr(cli, 'upload', lambda directory, url, **o: sent.append(directory.name) or {'status': 'stored'})
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: {'status': 'stored'})
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    feed = briskapi.connect(site='sbi', codes=['7203'], output=tmp_path / 'days').wait()
    assert [day['name'] for day in feed.recordings] == ['sbi_20260311_090000-090016', 'sbi_20260311_100000-100002']
    assert len(sent) == 2 and all(day['uploaded'] == {'status': 'stored'} for day in feed.recordings)
    assert not any(_spool.spool_root().glob('segment-*'))


def test_a_feed_keeps_and_uploads_each_trading_day_as_it_ends(tmp_path, monkeypatch):
    two_days(tmp_path, monkeypatch)
    sent, timing, days = [], [], []
    def publish(directory, url, **options):
        m = json.loads((directory / 'manifest.json').read_text())
        _parquet.inspect_package(directory, m)
        sent.append(m['summary']['trading_date'])
        return {'status': 'stored'}
    monkeypatch.setattr(cli, 'upload', publish)
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: timing.append(report['trading_date']) or {'status': 'stored'})
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    feed = briskapi.connect(site='sbi', codes=['7203'], output=tmp_path / 'days', on_day=days.append).wait()
    # One recording per day, named for its first and last source clock, kept and uploaded.
    assert sent == timing == ['20260311', '20260312'] and days == feed.recordings
    names = [day['name'] for day in feed.recordings]
    assert names == ['sbi_20260311_090000-090016', 'sbi_20260312_090000-090013']
    assert sorted(p.name for p in (tmp_path / 'days').iterdir()) == names
    assert briskapi.Recording(tmp_path / 'days' / names[0]).manifest['summary']['trading_date'] == '20260311'
    assert all(day['uploaded'] == day['timing_uploaded'] == {'status': 'stored'} for day in feed.recordings)
    # The feed's state is the latest day's.
    assert feed.trading_date == '20260312' and briskapi.Ticker('7203').quote()['last_price'] == 10200.0
    # Without uploads, days can still be kept; a new day must follow an end.
    briskapi.consent(revoke=True)
    feed = briskapi.connect(site='sbi', output=tmp_path / 'kept').wait()
    assert [day['uploaded'] for day in feed.recordings] == [None, None] and len(list((tmp_path / 'kept').iterdir())) == 2
    two_days(tmp_path, monkeypatch, second=sbi_session()[:3] + sbi_session())
    with pytest.raises(briskapi.BriskError, match='sequence error'):
        briskapi.connect(site='sbi', upload=False).wait()


def test_a_broker_day_ships_a_piece_at_a_time_each_a_recording_the_archive_accepts(fake_host, tmp_path, monkeypatch):
    # A whole-market day is far more than one recording holds, so it goes to the archive a piece at a time as it runs.
    monkeypatch.setattr(_spool, 'PART_BYTES', 3000)
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', 30)
    sent = []

    def publish(directory, url, **options):
        m = json.loads((directory / 'manifest.json').read_text())
        assert _parquet.inspect_package(directory, m) == m['summary']   # the archive's own checks
        sent.append(m['summary'])
        return {'status': 'stored'}
    monkeypatch.setattr(cli, 'upload', publish)
    monkeypatch.setattr(cli, 'upload_timing', lambda report, url: {'status': 'stored'})
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True)
    feed = briskapi.connect(site='sbi', output=tmp_path / 'days').wait()
    day = sbi_session()
    assert len(sent) > 2 and [r['uploaded'] for r in feed.recordings] == [{'status': 'stored'}] * len(sent)
    assert sent[0]['first_source_time_us'] == day[0]['source_time_us'] and sent[-1]['last_source_time_us'] == day[-1]['source_time_us']
    assert [s['first_source_time_us'] for s in sent[1:]] == [s['last_source_time_us'] for s in sent[:-1]]
    trades = [t['frame'] for d in sorted((tmp_path / 'days').iterdir()) for t in briskapi.Market(briskapi.Recording(d)).trades()]
    assert len(trades) == sum(len(b.get('trades', ())) for b in day) and len(list((tmp_path / 'days').iterdir())) == len(sent)
    assert [r['timing_uploaded'] for r in feed.recordings] == [None] * (len(sent) - 1) + [{'status': 'stored'}]
    assert not any(_spool.spool_root().glob('segment-*'))


@pytest.mark.parametrize('limit', [60, 80])
def test_a_broker_kept_without_uploads_splits_before_lunch_or_the_streams_end(tmp_path, monkeypatch, limit):
    # Scale the row threshold down; the host cannot end until a completed part has actually been kept.
    # The report's --no-upload/--output run must split while recording, not only when wait() finishes it.
    monkeypatch.setattr(_spool, 'MAX_PIECE_ROWS', limit)
    monkeypatch.setattr(_spool, 'PART_BYTES', 3000)
    batches = sbi_session(frames=100)
    script = tmp_path / 'still_recording.py'
    script.write_text(f'import json, sys\n'
                      f'for batch in {batches[:-1]!r}: print(json.dumps(batch), flush=True)\n'
                      f'sys.stdin.readline()\n'
                      f'print(json.dumps({batches[-1]!r}), flush=True)\n')
    kept = threading.Event()
    feed = _live.Feed(command=[sys.executable, '-u', str(script)], upload=False, output=tmp_path / 'days',
                      on_day=lambda result: kept.set())
    try:
        feed.ready(timeout=10)
        assert kept.wait(15), 'the size cutoff must save a piece while the host is still running'
        assert feed._process.poll() is None and not feed._day_over
        first, = feed.recordings
        assert first['path'] and first['error'] is None and first['uploaded'] is None
        m = json.loads((Path(first['path']) / 'manifest.json').read_text())
        assert _parquet.inspect_package(first['path'], m) == m['summary']
        assert m['summary']['last_source_time_us'] < _spool.LUNCH_US
    finally:
        feed.close()
    assert len(feed.recordings) == 2 and all(r['path'] and r['error'] is None for r in feed.recordings)
    assert not any(_spool.spool_root().glob('segment-*'))


def test_a_day_kept_but_not_uploaded_that_fails_to_package_says_so_not_that_an_upload_failed(fake_host, tmp_path, monkeypatch):
    monkeypatch.setattr(cli, 'package_parts', lambda *a, **k: 1 / 0)
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    with pytest.warns(UserWarning, match='^Packaging failed: division by zero'):
        feed = briskapi.connect(site='sbi', upload=False, output=tmp_path / 'days').wait()
    day = feed.recordings[-1]
    assert day['uploaded'] is None and day['error'] == 'Packaging failed: division by zero'


def test_between_days_the_status_line_is_quiet(fake_host):
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    feed = briskapi.connect(site='sbi', upload=False).wait()
    assert feed._progress() is None, 'the day has ended: nothing records until the next one begins'


def test_a_feed_with_no_session_to_join_waits_for_the_next_trading_days(tmp_path, monkeypatch):
    # The host says, outside the days' records, that the site has no session now (after the close, on a holiday);
    # that is kept nowhere, and the feed waits past its start timeout for the next day's session. The host is
    # driven by files the test makes, not by clocks, so a slow or loaded computer changes nothing it asserts.
    first = sbi_session()
    second = [{**b, 'trading_date': '20260312'} if b['type'] == 'bootstrap' else b for b in sbi_session(frames=20)]
    day_gate, stop_gate = tmp_path / 'day', tmp_path / 'stop'
    script = tmp_path / 'waiting.cjs'
    script.write_text(
        f"const fs = require('fs'); const say = b => console.log(JSON.stringify(b));\n"
        f"say({json.dumps({'type': 'waiting', 'date': '2026-03-10', 'session_status': 'done'})});\n"
        f"const go = () => fs.existsSync({json.dumps(str(day_gate))}) ? ({json.dumps(first)}.forEach(say), "
        f"say({json.dumps({'type': 'waiting', 'date': '2026-03-11', 'session_status': 'done'})}), "
        f"{json.dumps(second)}.forEach(say)) : setTimeout(go, 5);\n"
        "go();\n")
    monkeypatch.setattr(brisk, 'DECODER', script)
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    feed = _live.Feed(command=['node', str(script)], upload=False, output=tmp_path / 'days', gateway=True)
    with feed._changed:
        assert feed._changed.wait_for(lambda: feed.status == 'waiting', 30)
    assert feed.waiting == {'date': '2026-03-10', 'session_status': 'done'} and feed.seq == -1
    # The day begins only after the start timeout (0.2 s) has run out, and the feed was still waiting.
    threading.Timer(0.5, day_gate.touch).start()
    assert feed.ready(timeout=0.2) is feed and feed.status == 'running'
    feed.wait()
    assert [day['name'] for day in feed.recordings] == ['sbi_20260311_090000-090016', 'sbi_20260312_090000-090002']
    assert feed.trading_date == '20260312' and feed.waiting is None
    # A host that stops while waiting fails the start as any other would, after the start timeout has run out too.
    script.write_text(f"const fs = require('fs'); console.log(JSON.stringify({json.dumps({'type': 'waiting', 'date': '2026-03-10'})}));\n"
                      f"const go = () => fs.existsSync({json.dumps(str(stop_gate))}) ? process.exit(1) : setTimeout(go, 5);\n"
                      "go();\n")
    feed = _live.Feed(command=['node', str(script)], upload=False)
    with feed._changed:
        assert feed._changed.wait_for(lambda: feed.status == 'waiting', 30)
    threading.Timer(0.5, stop_gate.touch).start()
    with pytest.raises(briskapi.BriskError, match='before bootstrap'):
        feed.ready(timeout=0.1)


def test_e_shiten_has_its_own_address_cookies_and_variable(monkeypatch):
    with pytest.raises(brisk.SessionExpiredError, match="No e-shiten BRiSK Next cookies.*site='e-shiten'"):
        briskapi.login(site='e-shiten')
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"for": "sbi"}')
    with pytest.raises(brisk.SessionExpiredError):  # SBI's cookies never go to another site
        briskapi.login(site='e-shiten')
    monkeypatch.setenv('BRISK_ESHITEN_COOKIES', '{"from": "env"}')
    c = briskapi.login(site='e-shiten')
    assert c.site == 'e-shiten' and c.session.cookies == {'from': 'env'}
    opener = Opener({})
    c.session._opener, c.session.rate_limit = opener, 0
    with pytest.raises(NotFoundError):
        c.session.get('/api/frontend/boot')
    assert opener.requests[0].full_url == 'https://next.e-shiten.brisk.jp/api/frontend/boot'
    monkeypatch.delenv('BRISK_ESHITEN_COOKIES')
    briskapi.login(cookies={'saved': 'e'}, remember=True, site='e-shiten')
    briskapi.login(cookies={'saved': 's'}, remember=True)
    assert json.loads(brisk.cookies_path('e-shiten').read_text()) == {'saved': 'e'}
    assert json.loads(brisk.cookies_path().read_text()) == {'saved': 's'}
    briskapi.logout()
    assert not brisk.cookies_path('e-shiten').exists() and not brisk.cookies_path().exists()
    with pytest.raises(ValueError, match='Unknown BRiSK site'):
        briskapi.login(cookies={'a': 'b'}, site='rakuten')


@pytest.mark.parametrize('site,variable,name,host', [
    ('matsui', 'BRISK_MATSUI_COOKIES', 'Matsui BRiSK', 'matsui.brisk.jp'),
    ('monex', 'BRISK_MONEX_COOKIES', 'Monex BRiSK', 'monex.brisk.jp'),
    ('smbcnikko', 'BRISK_SMBCNIKKO_COOKIES', 'SMBC Nikko BRiSK', 'smbcnikko.brisk.jp')])
def test_matsui_monex_and_smbc_nikko_each_have_their_own_address_cookies_and_variable(monkeypatch, site, variable, name, host):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"for": "sbi"}')
    with pytest.raises(brisk.SessionExpiredError, match=f"No {name} cookies.*site='{site}'"):  # never SBI's
        briskapi.login(site=site)
    monkeypatch.setenv(variable, '{"from": "env"}')
    c = briskapi.login(site=site)
    assert c.site == site and c.session.cookies == {'from': 'env'}
    opener = Opener({})
    c.session._opener, c.session.rate_limit = opener, 0
    with pytest.raises(NotFoundError):
        c.session.get('/api/frontend/boot')
    assert opener.requests[0].full_url == f'https://{host}/api/frontend/boot'
    monkeypatch.delenv(variable)
    briskapi.login(cookies={'saved': site}, remember=True, site=site)
    assert brisk.cookies_path(site).name == f'{site}-cookies.json'
    assert json.loads(brisk.cookies_path(site).read_text()) == {'saved': site}
    briskapi.logout()
    assert not brisk.cookies_path(site).exists()


@pytest.fixture
def eshiten_host(tmp_path, monkeypatch):
    """A stand-in for brisk.cjs on e-shiten that reports the site it was asked for and checks that its
    cookies came in e-shiten's own variable."""
    batches = sbi_session()
    batches[0] = {**batches[0], 'input_transport': {
        **schema.BROKER_TRANSPORTS['e-shiten'], 'ws_host': 'api.brisk.jp', 'decoder': '/private/decoder.js'}}
    script = tmp_path / 'eshiten_fake.cjs'
    script.write_text('if (JSON.parse(process.env.BRISK_ESHITEN_COOKIES).s !== "v") process.exit(9);\n'
                      'console.error(process.env.BRISK_SITE);\n'
                      f'for (const b of {json.dumps(batches)}) console.log(JSON.stringify(b));\n')
    monkeypatch.setattr(brisk, 'DECODER', script)
    sent = {'timing': [], 'market': []}
    def publish(directory, url, **options):
        m = json.loads((directory / 'manifest.json').read_text())
        _parquet.inspect_package(directory, m)
        sent['market'].append(m)
        return {'status': 'stored'}
    monkeypatch.setattr(cli, 'upload', publish)
    monkeypatch.setattr(cli, 'upload_timing',
                        lambda report, url: sent['timing'].append(schema.validate_timing(report)) or {'status': 'stored'})
    return sent


class _Passkeys:
    """passkey.Passkeys with the saved logins in `state['saved']`, by broker."""
    def __init__(self, state):
        self.state = state

    def load(self, site):
        return self.state['saved'].get(site)


class _Closed:
    """A threading.Event that never blocks: it records the waits asked of it, and may do something while one 'passes'."""
    def __init__(self, during=None, is_set=False):
        self.waits, self.during, self.is_set = [], during, is_set

    def wait(self, seconds):
        self.waits.append(seconds)
        if self.during:
            self.during()
        return self.is_set


@pytest.fixture
def passkey_login(monkeypatch, tmp_path):
    """briskapi.passkey with a saved passkey for `sbi` (record in `state`), and a `login` that fails as often as `failures` says."""
    from briskapi import passkey
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    state = {'saved': {'sbi': {'site': 'sbi', 'credential': {}}}, 'failures': 0, 'logins': []}
    monkeypatch.setattr(passkey, 'Passkeys', lambda: _Passkeys(state))

    def login(**options):
        state['logins'].append(options)
        if state['failures']:
            state['failures'] -= 1
            raise passkey.PasskeyError('the login page did not load')
        briskapi.login(cookies={'session_bfaf77a2': 'new'}, remember=True)
    monkeypatch.setattr(passkey, 'login', login)
    return state


def test_a_feed_whose_session_expired_signs_in_again_with_the_saved_passkey(passkey_login):
    told, closed = [], _Closed()
    assert brisk._sign_in_again('sbi', closed, tell=told.append) is True
    assert passkey_login['logins'] == [{'remember': True, 'runtime': None, 'site': 'sbi'}] and closed.waits == [0]   # None: the passkey helper chooses its runtime as the host does
    assert told == ['the session has expired; signing in again with your saved passkey (Chrome opens)',
                    'signed in again; the feed goes on']
    assert json.loads(brisk.cookies_path('sbi').read_text()) == {'session_bfaf77a2': 'new'}, 'where the host reads them'


def test_signing_in_again_is_tried_a_few_times_further_apart_and_then_left_to_the_person(passkey_login):
    passkey_login['failures'] = 2
    told, closed = [], _Closed()
    assert brisk._sign_in_again('sbi', closed, waits=(0, 600, 1800), tell=told.append) is True
    assert closed.waits == [0, 600, 1800] and len(passkey_login['logins']) == 3
    assert [t for t in told if 'failed' in t] == ['signing in again failed: the login page did not load; trying again in 10 minutes',
                                                  'signing in again failed: the login page did not load; trying again in 30 minutes']
    passkey_login['failures'], passkey_login['logins'], told[:] = 9, [], []
    assert brisk._sign_in_again('sbi', _Closed(), waits=(0, 60), tell=told.append) is False
    assert len(passkey_login['logins']) == 2 and 'sign in again yourself (`brisk login`)' in told[-1]


def test_a_feed_without_a_passkey_for_its_site_tells_how_to_sign_in_and_does_not_try(passkey_login):
    for saved, said in (({}, 'no passkey is saved for sbi'),
                        ({'e-shiten': {'site': 'e-shiten', 'credential': {}}}, 'no passkey is saved for sbi')):   # another broker's is not its
        passkey_login['saved'], told = saved, []
        assert brisk._sign_in_again('sbi', _Closed(), tell=told.append) is False
        assert said in told[0] and "briskapi.login(site='sbi', cookies=..., remember=True)" in told[0] and 'brisk enroll --site sbi' in told[0]
    assert passkey_login['logins'] == [], 'another broker\'s passkey is never used'


def test_signing_in_again_stops_when_the_feed_closes_or_somebody_else_has_signed_in(passkey_login):
    assert brisk._sign_in_again('sbi', _Closed(is_set=True), tell=lambda text: None) is False
    first = _Closed(during=lambda: briskapi.login(cookies={'session_bfaf77a2': 'by hand'}, remember=True))
    assert brisk._sign_in_again('sbi', first, tell=lambda text: None) is False
    assert passkey_login['logins'] == [], 'neither signs in'
    assert brisk._sign_in_again('sbi', _Closed(), tell=lambda text: None) is True, 'with nothing changing, it does'


def test_a_saved_passkey_that_cannot_be_read_is_told_not_raised(monkeypatch):
    from briskapi import passkey
    monkeypatch.setattr(passkey, 'default_store', lambda site=None: (_ for _ in ()).throw(passkey.PasskeyError('no secure place')))
    told = []
    assert brisk._sign_in_again('sbi', _Closed(), tell=told.append) is False
    assert 'no secure place' in told[0]


def test_a_host_that_says_its_session_expired_has_the_feed_sign_in_again_and_a_failure_does_not_stop_it(tmp_path, monkeypatch, capsys):
    script = tmp_path / 'expired.cjs'
    script.write_text('console.log(JSON.stringify({type: "expired"}));\n'
                      f'for (const b of {json.dumps(sbi_session())}) console.log(JSON.stringify(b));\n')
    monkeypatch.setattr(brisk, 'DECODER', script)
    asked = []
    monkeypatch.setattr(brisk, '_sign_in_again', lambda site, closed, runtime=None: asked.append((site, closed)) or 1 / 0)
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    feed = briskapi.connect(site='sbi', codes=['7203'], upload=False).wait()
    assert feed.status == 'completed' and briskapi.Ticker('7203').quote()['last_price'] == 10160.0, 'the day was recorded all the same'
    deadline = time.time() + 5
    while not asked and time.time() < deadline:
        time.sleep(0.01)
    assert [site for site, _ in asked] == ['sbi'] and isinstance(asked[0][1], threading.Event)
    err, deadline = '', time.time() + 5
    while 'signing in again failed: division by zero' not in err and time.time() < deadline:
        time.sleep(0.01)
        err += capsys.readouterr().err
    assert 'signing in again failed: division by zero' in err, 'a failure is told, not raised in the feed'


def test_the_host_is_told_where_a_new_login_saves_its_cookies(tmp_path, monkeypatch):
    # A feed that waits overnight meets the 05:00 expiry: it reads the cookies `brisk login` saved after it.
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.setenv('BRISK_UPLOAD', '0')
    script = tmp_path / 'fake.cjs'
    script.write_text(f'for (const b of {json.dumps(sbi_session())}) console.log(JSON.stringify(b));\n')
    monkeypatch.setattr(brisk, 'DECODER', script)
    from briskapi import _live
    told, real = [], _live.Feed
    def spy(**kwargs):
        told.append(kwargs['env']['BRISK_COOKIES_FILE'])
        return real(**kwargs)
    monkeypatch.setattr(_live, 'Feed', spy)
    briskapi.login(cookies={'s': 'v'}, site='e-shiten')
    briskapi.connect(site='e-shiten', codes=['7203'], upload=False).wait()
    assert told == [str(tmp_path / 'config' / 'brisk' / 'e-shiten-cookies.json')]


def test_fast_symbols_reaches_the_host_and_is_for_a_broker_with_a_whole_number(tmp_path, monkeypatch, capsys):
    from briskapi import _live
    script = tmp_path / 'fake.cjs'
    script.write_text(f'for (const b of {json.dumps(sbi_session())}) console.log(JSON.stringify(b));\n')
    monkeypatch.setattr(brisk, 'DECODER', script)
    told, real = [], _live.Feed

    def spy(**kwargs):
        told.append(kwargs['env'].get('BRISK_FAST_SYMBOLS'))
        return real(**kwargs)
    monkeypatch.setattr(_live, 'Feed', spy)
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    briskapi.connect(site='sbi', codes=['7203'], upload=False, fast_symbols=1500).wait()
    briskapi.connect(site='sbi', codes=['7203'], upload=False).wait()
    assert told == ['1500', None]
    for wrong in (0, -3, 1.5, '1500'):
        with pytest.raises(ValueError, match='fast_symbols is the number of symbols'):
            briskapi.connect(site='sbi', upload=False, fast_symbols=wrong)
    with pytest.raises(ValueError, match='fast_symbols: only for a broker site'):
        briskapi.connect(site='demo', fast_symbols=500)
    with pytest.raises(SystemExit, match="--fast-symbols applies to a broker's site"):
        cli.main(['live', '--site', 'demo', '--fast-symbols', '500'])


def test_e_shiten_uploads_like_sbi_and_honors_opt_out(eshiten_host, monkeypatch, capfd):
    briskapi.login(cookies={'s': 'v'}, site='e-shiten')
    with pytest.warns(UserWarning, match='by default'):
        feed = briskapi.connect(site='e-shiten', codes=['7203']).wait()
    assert feed.source == 'brisk_live' and briskapi.Ticker('7203').quote()['last_price'] == 10160.0
    assert feed.timing_uploaded == {'status': 'stored'} and feed.uploaded == {'status': 'stored'}
    timing, market = eshiten_host['timing'][0], eshiten_host['market'][0]
    assert (timing['source'], timing['site'], market['summary']['source']) == ('brisk_live', 'e-shiten', 'brisk_live')
    assert feed.recordings[-1]['name'].startswith('e-shiten_20260311_'), 'kept apart from another broker\'s'
    assert 'e-shiten' in capfd.readouterr().err
    assert briskapi.connect(site='e-shiten', upload_market_data=False).wait().uploaded is None
    assert briskapi.connect(site='e-shiten', upload=False).wait().timing_uploaded is None
    monkeypatch.setenv('BRISK_UPLOAD', '0')
    feed = briskapi.connect(site='e-shiten').wait()
    assert feed.uploaded is None and feed.timing_uploaded is None
    assert len(eshiten_host['market']) == 1 and len(eshiten_host['timing']) == 2


@pytest.mark.parametrize('answer,uploaded', [('\n', True), ('n\n', False)])
def test_cli_asks_once_for_e_shiten_like_sbi(eshiten_host, monkeypatch, capsys, answer, uploaded):
    monkeypatch.setenv('BRISK_ESHITEN_COOKIES', '{"s": "v"}')
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO(answer))
    cli.main(['live', '--site', 'e-shiten', '--codes', '7203', '--quotes'])
    out = capsys.readouterr()
    assert json.loads(out.out.splitlines()[-1])['last_price'] == 10160.0
    assert 'Upload automatically to your private archive?' in out.err
    assert len(eshiten_host['market']) == len(eshiten_host['timing']) == uploaded
    cli.main(['live', '--site', 'e-shiten', '--no-upload-market-data'])
    assert len(eshiten_host['market']) == uploaded and len(eshiten_host['timing']) == 2 * uploaded
    monkeypatch.setenv('BRISK_UPLOAD', '0')
    cli.main(['live', '--site', 'e-shiten'])
    assert len(eshiten_host['timing']) == 2 * uploaded, 'the environment opt-out keeps even timing local'
    with pytest.raises(SystemExit):
        cli.main(['live', '--site', 'demo', '--upload-market-data'])
    assert '--upload-market-data is for a broker' in capsys.readouterr().err
    with pytest.raises(SystemExit):
        cli.main(['live', '--site', 'sbi', '--cache', '.'])
    assert '--cache is for the demo' in capsys.readouterr().err


def test_cli_live_reports_on_the_recording_instead_of_printing_its_quotes(monkeypatch, tmp_path, capsys):
    # A host that records for a while: each status line names the day, how long it has recorded, its updates
    # and their rate, and its latest data time; no quote is printed.
    batches = sbi_session(frames=40)
    script = tmp_path / 'slow.cjs'
    script.write_text(f"const b = {json.dumps(batches)}; let i = 0;\n"
                      "const t = setInterval(() => { console.log(JSON.stringify(b[i++])); if (i === b.length) clearInterval(t); }, 10);\n")
    monkeypatch.setattr(brisk, 'DECODER', script)
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    monkeypatch.setattr(cli, 'PROGRESS_SECONDS', {True: 0.1, False: 0.1})
    cli.main(['live', '--site', 'sbi', '--no-upload', '--no-gateway'])
    out = capsys.readouterr()
    assert out.out == ''
    reports = [line for line in out.err.splitlines() if line.startswith('[brisk] 2026-03-11: recording for 0:00:0')]
    assert reports, out.err
    assert re.search(r', \d[\d,]* updates, [\d,]+ a second, data at 09:00:\d\d JST$', reports[-1]), reports[-1]
    with pytest.raises(SystemExit):
        cli.main(['live', '--site', 'sbi', '--raw'])
    assert '--raw is for the quotes printed with --quotes' in capsys.readouterr().err


def test_a_broker_feed_on_a_python_with_the_gil_warns_that_it_can_fall_behind(fake_host, monkeypatch):
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    monkeypatch.setattr(brisk.sys, '_is_gil_enabled', lambda: True)   # the feed then reads its lines one at a time
    with pytest.warns(UserWarning, match=r'has the GIL.*free-threaded Python 3\.14'):
        feed = briskapi.connect(site='sbi', upload=False)
    assert feed.wait().source == 'brisk_live'


def test_two_brokers_recording_the_same_session_into_one_folder_keep_both_named_by_site(fake_host, tmp_path, monkeypatch):
    # The same market data through two brokers (as a tester compares them): one source, two recordings, each named
    # for its broker, neither in the other's way.
    briskapi.login(cookies={'session_bfaf77a2': 'v'})
    sbi = briskapi.connect(site='sbi', upload=False, output=tmp_path / 'days').wait()
    batches = sbi_session()
    batches[0] = {**batches[0], 'input_transport': {**schema.BROKER_TRANSPORTS['e-shiten'], 'decoder': '/private/decoder.js'}}
    script = tmp_path / 'eshiten_fake.cjs'
    script.write_text(f'for (const b of {json.dumps(batches)}) console.log(JSON.stringify(b));\n')
    monkeypatch.setattr(brisk, 'DECODER', script)
    briskapi.login(cookies={'s': 'v'}, site='e-shiten')
    eshiten = briskapi.connect(site='e-shiten', upload=False, output=tmp_path / 'days').wait()
    assert sorted(p.name for p in (tmp_path / 'days').iterdir()) == ['e-shiten_20260311_090000-090016', 'sbi_20260311_090000-090016']
    assert sbi.source == eshiten.source == 'brisk_live'
    for name in ('e-shiten_20260311_090000-090016', 'sbi_20260311_090000-090016'):
        assert briskapi.Recording(tmp_path / 'days' / name).manifest['summary']['source'] == 'brisk_live'


def test_every_broker_is_the_same_in_the_host_the_client_and_the_archive_schema():
    """decoder/brisk.cjs, briskapi.brisk and briskapi.schema each list the brokers; they must agree."""
    import subprocess
    host = json.loads(subprocess.run(['node', '-e', f"console.log(JSON.stringify(require({json.dumps(str(brisk.DECODER))}).SITES))"],
                                     capture_output=True, text=True, check=True).stdout)
    assert list(host) == list(brisk.SITES) == list(briskapi.SITES[1:])
    for site, ours in brisk.SITES.items():
        theirs = host[site]
        assert (theirs['origin'], theirs['name'], theirs['cookies']) == (ours.origin, ours.name, ours.cookies_env)
        assert schema.BROKER_TRANSPORTS[site] == {'kind': theirs['transport'], 'origin': ours.origin + '/'}
    assert set(schema.BROKER_TRANSPORTS) == set(brisk.SITES) and schema.BROKER_SOURCE in schema.SOURCES
    source = subprocess.run(['node', '-e', f"console.log(require({json.dumps(str(brisk.DECODER))}).SOURCE)"],
                            capture_output=True, text=True, check=True).stdout.strip()
    assert source == schema.BROKER_SOURCE
