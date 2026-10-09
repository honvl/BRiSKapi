"""SBI BRiSK client with pybrisk's sample payloads and a fake HTTP layer (no account needed)."""
import base64
import datetime as dt
import io
import json
import os
from pathlib import Path
import stat
import sys
import urllib.error
import zlib

from briskapi.timing import TimingStats

import pytest

import briskapi
import briskapi.cli as cli
import briskapi.schema as schema
from briskapi import sbi
from briskapi._recording import JST

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
        path = request.full_url.removeprefix(sbi.ORIGIN).split('?')[0]
        value = self.routes.get(path, 404)
        if isinstance(value, int):
            raise urllib.error.HTTPError(request.full_url, value, 'error', {}, io.BytesIO(b'details'))
        return Response(value if isinstance(value, bytes) else json.dumps(value).encode())


def client(**routes):
    opener = Opener({'/api/frontend/boot': BOOT, '/api/app/boot': APP_BOOT, **routes})
    session = sbi.Session({'session_bfaf77a2': 'v2.local.cookie'}, rate_limit=0, opener=opener)
    return sbi.Client(session=session), opener


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.delenv('BRISK_SBI_COOKIES', raising=False)
    monkeypatch.delenv('BRISK_CONTRIBUTE', raising=False)
    monkeypatch.setattr(sbi, '_client', None)
    monkeypatch.setattr(briskapi, '_current', None)


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
    t = briskapi.Ticker('7203', sbi=c)
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
    m = briskapi.Market(sbi=c)
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


@pytest.mark.parametrize('status,error', [(401, sbi.SessionExpiredError), (302, sbi.SessionExpiredError),
                                          (404, briskapi.NotFoundError), (429, sbi.RateLimitError), (500, sbi.APIError)])
def test_errors(status, error):
    c, _ = client(**{'/api/ohlc/7203': status})
    with pytest.raises(error):
        c.candles('7203')
    assert str(sbi.APIError(500)) == 'HTTP 500'


def test_session_rules(monkeypatch):
    for cookies in ({}, None, {'session_bfaf77a2': ''}, []):
        with pytest.raises(sbi.SessionExpiredError, match='login'):
            sbi.Session(cookies)
    assert sbi._NoRedirect().redirect_request(None, None, 302, 'Found', {}, 'https://elsewhere.test/') is None
    slept = []
    monkeypatch.setattr(sbi.time, 'sleep', slept.append)
    session = sbi.Session({'a': 'b'}, rate_limit=2, opener=Opener({'/raw': b'\x00\x01'}))
    assert session.get('/raw', raw=True) == b'\x00\x01'
    session.get('/raw', raw=True)
    assert slept and 0 < slept[0] <= 0.5


def test_login_sources(monkeypatch, tmp_path):
    with pytest.raises(sbi.SessionExpiredError, match='needs a session'):
        briskapi.Ticker('7203').candles()
    with pytest.raises(sbi.SessionExpiredError):
        sbi.login()
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"from": "env"}')
    assert sbi.login().session.cookies == {'from': 'env'}
    monkeypatch.delenv('BRISK_SBI_COOKIES')
    sbi.login({'saved': 'yes'}, remember=True)
    path = sbi.cookies_path()
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600 and json.loads(path.read_text()) == {'saved': 'yes'}
    assert sbi.login().session.cookies == {'saved': 'yes'}
    sbi._client.session._opener = Opener({'/api/frontend/boot': BOOT, '/api/app/boot': APP_BOOT})
    sbi._client.session.rate_limit = 0
    assert briskapi.Market().schedule()['date'] == '2026-03-11'  # default session used
    sbi.logout()
    assert not path.exists() and sbi._client is None


OPEN_US = 9 * 3600 * 1_000_000
MIDNIGHT_MS = int(dt.datetime(2026, 3, 11, tzinfo=JST).timestamp() * 1000)


def sbi_session(frames=150):
    """Bootstrap, `frames` one-quote batches 100 ms apart (20 ms old on receipt, one 1.5 s stall) and end."""
    q = {**dict.fromkeys(schema.QUOTE_KEYS - {'issue_status'}, 0),
         'issue_id': 0, 'code': '7203', 'frame': 1, 'max_frame': 1, 'source_time_us': OPEN_US,
         'indicative_price10': 101500, 'market_buy_quantity': 3, 'market_sell_quantity': 1}
    master = {**dict.fromkeys(schema.MASTER_KEYS, 0), 'issue_id': 0, 'code': '7203', 'name': 'Toyota', 'lot_size': 100}
    batches = [dict(type='bootstrap', seq=0, source='sbi_live', trading_date='20260311', source_time_us=OPEN_US,
                    input_transport={**schema.SBI_TRANSPORT, 'decoder': '/private/decoder.js', 'setup_ms': 1.123456},
                    source_timestamp_origin='brisk_decoder_unverified', exchange_delay_ms=None,
                    market_issue_count=1, master=[master], quotes=[q])]
    for i in range(1, frames + 1):
        now = OPEN_US + i * 100_000 + (1_400_000 if i > 100 else 0)
        batches.append(dict(type='quotes', seq=i, source_time_us=now, received_unix_ms=MIDNIGHT_MS + now // 1000 + 20,
                            decode_ns=250_000 + i * 1000, replay_lateness_ms=None,
                            quotes=[{**q, 'frame': i + 1, 'max_frame': i + 1, 'source_time_us': now,
                                     'last_price10': 101500 + 100 * (i == frames)}]))
    return batches + [dict(type='end', seq=frames + 1, source_time_us=now, frames=frames, quote_updates=frames,
                          replay_wall_ms=16_400)]


@pytest.fixture
def fake_host(tmp_path, monkeypatch):
    """A stand-in for sbi.cjs that checks its cookies and prints a short live session."""
    batches = sbi_session()
    script = tmp_path / 'sbi_fake.cjs'
    script.write_text(
        "const c = JSON.parse(process.env.BRISK_SBI_COOKIES); if (c.session_bfaf77a2 !== 'v') process.exit(9);\n"
        f"console.error(JSON.stringify(process.argv.slice(2)));\n"
        f"for (const b of {json.dumps(batches)}) console.log(JSON.stringify(b));\n")
    monkeypatch.setattr(sbi, 'DECODER', script)


def test_live_feed_via_node(fake_host, capfd):
    sbi.login({'session_bfaf77a2': 'v'})
    feed = sbi.connect(codes=['7203'], history=True)
    assert briskapi.current() is feed and feed.source == 'sbi_live'
    feed.wait()
    assert briskapi.Ticker('7203').quote()['last_price'] == 10160.0 and feed.contribution is None
    assert len(briskapi.Ticker('7203').history()) == 151 and feed.timing_contribution is None
    # Cookies reach the host through its environment, never its (world-readable) arguments.
    assert capfd.readouterr().err.strip() == '["--codes","7203"]'


def test_protocol_options_reach_the_host_without_putting_the_profile_in_argv(tmp_path, monkeypatch, capfd):
    script = tmp_path / 'sbi_options.cjs'
    script.write_text(
        "console.error(JSON.stringify({argv: process.argv.slice(2), profile: process.env.BRISK_SBI_PROFILE || null}));\n"
        f"for (const b of {json.dumps(sbi_session())}) console.log(JSON.stringify(b));\n")
    monkeypatch.setattr(sbi, 'DECODER', script)
    sbi.login({'session_bfaf77a2': 'v'})
    profile = {'connectQuery': {'_v': 'build-9'}, 'catchUp': 'json-vendor'}
    sbi.connect(codes=['7203'], trace_protocol=True, profile=profile).wait()
    seen = json.loads(capfd.readouterr().err.strip())
    assert seen['argv'] == ['--codes', '7203', '--trace-protocol']
    assert json.loads(seen['profile']) == profile
    assert 'build-9' not in ' '.join(seen['argv'])
    sbi.connect(codes=['7203']).wait()
    assert json.loads(capfd.readouterr().err.strip()) == {'argv': ['--codes', '7203'], 'profile': None}


def test_live_feed_failure_closes(fake_host, tmp_path, monkeypatch):
    sbi.login({'session_bfaf77a2': 'wrong'})
    with pytest.raises(briskapi.BriskError, match='before bootstrap'):
        sbi.connect()


def test_cli_live_sbi(fake_host, monkeypatch, capsys):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    monkeypatch.setattr(cli.sys, 'stdin', io.StringIO('\n'))
    sent = []
    monkeypatch.setattr(cli, 'contribute_timing', lambda report, url: sent.append(report) or {'status': 'published'})
    cli.main(['live', '--sbi', '--codes', '7203'])
    out = capsys.readouterr()
    lines = [json.loads(line) for line in out.out.splitlines()]
    # The question explains that SBI sessions share only a timing summary.
    assert lines[-1]['last_price'] == 10160.0 and 'timing summary' in out.err
    assert cli.load_consent()['enabled'] and sent[0]['source'] == 'sbi_live'
    assert not {'quotes', 'master', 'code', 'price'} & set(json.dumps(sent[0]).replace('"', ' ').split())


def test_cli_trace_protocol_reaches_the_sbi_host(fake_host, monkeypatch, capfd):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    cli.main(['live', '--sbi', '--codes', '7203', '--trace-protocol'])
    assert capfd.readouterr().err.count('["--codes","7203","--trace-protocol"]') == 1


def test_sbi_feed_contributes_timing_only(fake_host, monkeypatch):
    sent = []
    monkeypatch.setattr(cli, 'contribute_timing', lambda report, url: sent.append((report, url)) or {'status': 'published'})
    sbi.login({'session_bfaf77a2': 'v'})
    with pytest.warns(UserWarning, match='undecided'):
        assert sbi.connect().wait().timing_contribution is None and not sent
    briskapi.consent(accept=True, contributor='erin')
    feed = sbi.connect().wait()
    assert feed.timing_contribution == {'status': 'published'} and feed.contribution is None
    report, url = sent[0]
    assert url == cli.settings()['api_url'] and report['contributor'] == 'erin'
    assert report['frames'] == 150 and report['stalls'] == 1 and report['trading_date'] == '20260311'
    assert report['first_minute'] == '09:00' and report['source_age_ms']['p50'] == 20
    assert report['interarrival_ms']['max'] == 1500 and report['decode_ms']['p50'] > 0.25
    assert sbi.connect(contribute=False).wait().timing_contribution is None and len(sent) == 1
    monkeypatch.setattr(cli, 'contribute_timing', lambda *a: 1 / 0)
    with pytest.warns(UserWarning, match='Timing contribution failed'):
        assert sbi.connect().wait().timing_contribution['status'] == 'failed'


def test_cli_without_cookies_stops_before_prompt_or_decoder(monkeypatch, capsys):
    monkeypatch.setattr(cli, 'check_node', lambda *args: pytest.fail('decoder must not start'))
    monkeypatch.setattr(cli, 'ask_consent', lambda: pytest.fail('do not prompt without cookies'))
    with pytest.raises(SystemExit, match='No SBI BRiSK cookies'):
        cli.main(['live', '--sbi'])
    assert 'Decoder exited' not in capsys.readouterr().err


def test_sbi_market_sharing_requires_current_consent(fake_host):
    sbi.login({'session_bfaf77a2': 'v'})
    with pytest.raises(briskapi.BriskError, match='consent'):
        sbi.connect(share_market_data=True)
    cli.consent_path().parent.mkdir(parents=True, exist_ok=True)
    cli.consent_path().write_text(json.dumps({'policy_version': 2, 'enabled': True,
                                            'contributor': 'old', 'license': 'CC0-1.0'}))
    with pytest.raises(briskapi.BriskError, match='consent'):
        sbi.connect(share_market_data=True)


def test_sbi_market_sharing_is_explicit_and_honors_opt_out(fake_host, monkeypatch):
    sbi.login({'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True, contributor='owner')
    sent = []
    def publish(directory, url, **options):
        m = json.loads((directory / 'manifest.json').read_text())
        schema.inspect_package(directory / 'events.jsonl.gz', m)
        sent.append(m)
        return {'status': 'published'}
    monkeypatch.setattr(cli, 'contribute', publish)
    monkeypatch.setattr(cli, 'contribute_timing', lambda *args: {'status': 'published'})
    assert sbi.connect().wait().contribution is None and not sent
    feed = sbi.connect(share_market_data=True).wait()
    assert feed.contribution == {'status': 'published'} and len(sent) == 1
    assert sent[0]['summary']['source'] == 'sbi_live' and sent[0]['contributor'] == 'owner'
    assert not Path(feed._tmp.name).exists()
    assert sbi.connect(share_market_data=True, contribute=False).wait().contribution is None
    monkeypatch.setenv('BRISK_CONTRIBUTE', '0')
    feed = sbi.connect(share_market_data=True).wait()
    assert feed.contribution is None and feed.timing_contribution is None and len(sent) == 1


@pytest.mark.parametrize('answer,shared', [('\n', True), ('y\n', True), ('n\n', False), ('', False)])
def test_cli_asks_for_each_sbi_capture_and_enter_opts_in(fake_host, monkeypatch, capsys, answer, shared):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    briskapi.consent(accept=True, contributor='owner')
    monkeypatch.setattr(cli, 'interactive', lambda: True)
    sent = []
    monkeypatch.setattr(cli, 'contribute', lambda *args, **kwargs: sent.append(args) or {'status': 'published'})
    monkeypatch.setattr(cli, 'contribute_timing', lambda *args: {'status': 'published'})
    for run in range(2):
        monkeypatch.setattr(cli.sys, 'stdin', io.StringIO(answer))
        cli.main(['live', '--sbi'])
        out = capsys.readouterr()
        assert 'Share this SBI capture publicly?' in out.err and '[Y/n]' in out.err
        assert len(sent) == (run + 1 if shared else 0)
    assert cli.load_consent()['enabled']  # Per-session choice does not change the saved consent.


@pytest.mark.parametrize('flag,shared', [('--share-market-data', True), ('--no-share-market-data', False)])
def test_cli_sbi_market_sharing_flags(fake_host, monkeypatch, flag, shared):
    monkeypatch.setenv('BRISK_SBI_COOKIES', '{"session_bfaf77a2": "v"}')
    briskapi.consent(accept=True, contributor='owner')
    monkeypatch.setattr(cli, 'ask_sbi_market_sharing', lambda *args: pytest.fail('explicit flag suppresses prompt'))
    sent = []
    monkeypatch.setattr(cli, 'contribute', lambda *args, **kwargs: sent.append(args) or {'status': 'published'})
    monkeypatch.setattr(cli, 'contribute_timing', lambda *args: {'status': 'published'})
    cli.main(['live', '--sbi', flag])
    assert bool(sent) == shared


def test_opted_in_sbi_capture_closed_early_does_not_publish(tmp_path, monkeypatch):
    script = tmp_path / 'held.cjs'
    script.write_text(f'console.log({json.dumps(json.dumps(sbi_session()[0]))});\nsetInterval(() => {{}}, 1000);\n')
    monkeypatch.setattr(sbi, 'DECODER', script)
    monkeypatch.setattr(cli, 'contribute', lambda *args, **kwargs: pytest.fail('incomplete capture must stay local'))
    sbi.login({'session_bfaf77a2': 'v'})
    briskapi.consent(accept=True, contributor='owner')
    feed = sbi.connect(share_market_data=True)
    feed.close()
    assert feed.status == 'closed' and feed.contribution is None
    assert not Path(feed._tmp.name).exists()
