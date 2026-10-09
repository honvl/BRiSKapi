"""SBI BRiSK (sbi.brisk.jp) session: login, REST data and an experimental live feed.

The data is used through briskapi's own Ticker and Market:

    import briskapi
    from briskapi import sbi

    sbi.login(cookies={"session_bfaf77a2": "v2.local..."})   # from your logged-in browser
    briskapi.Ticker("7203").candles("5m").to_pandas()
    briskapi.Market().events()
    feed = sbi.connect(codes=["7203"])                      # live, via Node (experimental)

The endpoint sequence (cookie login, token boot, data endpoints) was learned from
pybrisk (https://github.com/obichan117/pybrisk), Copyright (c) 2026 obichan117,
MIT License; see LICENSE-pybrisk.txt in this package.

Your session cookies are credentials. They are sent only to sbi.brisk.jp and are
kept in memory unless you pass remember=True. Sharing SBI market data requires
share_market_data=True for that session and contribution consent.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib

from briskapi._recording import JST, BriskError, NotFoundError, Table
from briskapi.passkey import PasskeyError  # noqa: F401  (part of this module's API)

ORIGIN = 'https://sbi.brisk.jp'
DECODER = Path(__file__).resolve().parent / 'decoder' / 'sbi.cjs'
INTERVALS = ('5m', '1d', '1w', '1mo')
SCHEDULE = {'morning_pre_open': 'morning_session_pre_open_time', 'morning_open': 'morning_session_open_time',
            'morning_close': 'morning_session_close_time', 'afternoon_pre_open': 'afternoon_session_pre_open_time',
            'afternoon_open': 'afternoon_session_open_time', 'afternoon_pre_close': 'afternoon_session_pre_close_time',
            'afternoon_close': 'afternoon_session_close_time'}
MARGIN = {'long_balance': 'kakuhoLongShares', 'short_balance': 'kakuhoShortShares',
          'preliminary_long': 'sokuhoLongShares', 'preliminary_short': 'sokuhoShortShares',
          'standardized_long': 'standardizedLongShares', 'standardized_short': 'standardizedShortShares',
          'lending_fee': 'gyakuhibuFee', 'lending_fee_pct': 'gyakuhibuFeePercent',
          'lending_fee_days': 'gyakuhibuFeeDayCount', 'lending_fee_max': 'gyakuhibuMaxFee'}


class SessionExpiredError(BriskError):
    """The SBI BRiSK session is missing, invalid or expired. Log in again."""


class APIError(BriskError):
    """Unexpected HTTP status from SBI BRiSK."""

    def __init__(self, status, message=''):
        self.status = status
        super().__init__(f'HTTP {status}: {message}' if message else f'HTTP {status}')


class RateLimitError(APIError):
    """Too many requests (429)."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    # A redirect means the session went to a login page. Following it would also
    # forward cookies and the bearer token to wherever it points.
    def redirect_request(self, *args):
        return None


def cookies_path() -> Path:
    return Path(os.environ.get('XDG_CONFIG_HOME') or Path.home() / '.config') / 'brisk' / 'sbi-cookies.json'


class Session:
    """Cookie-authenticated HTTP to sbi.brisk.jp: rate limited, no redirects."""

    def __init__(self, cookies, rate_limit=1.0, timeout=30, opener=None):
        if not cookies or not isinstance(cookies, dict) or not any(cookies.values()):
            raise SessionExpiredError('No SBI BRiSK cookies: copy them from your logged-in browser and call sbi.login(), '
                                      'or run `brisk sbi login` (passkey)')
        self.cookies = dict(cookies)
        self.rate_limit, self.timeout = rate_limit, timeout
        self.token = None
        self._opener = opener or urllib.request.build_opener(_NoRedirect)
        self._last = 0.0

    @property
    def cookie_header(self) -> str:
        return '; '.join(f'{k}={v}' for k, v in self.cookies.items())

    def get(self, path, params=None, raw=False):
        if self.rate_limit > 0:
            wait = 1 / self.rate_limit - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
        url = ORIGIN + path + ('?' + urllib.parse.urlencode(params) if params else '')
        headers = {'Cookie': self.cookie_header, 'Accept': 'application/json'}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        try:
            with self._opener.open(urllib.request.Request(url, headers=headers), timeout=self.timeout) as response:
                body = response.read()
        except urllib.error.HTTPError as e:
            text = e.read(500).decode(errors='replace')
            if e.code in (301, 302, 303, 307, 308, 401, 403):
                raise SessionExpiredError('SBI BRiSK session expired or invalid; log in again') from None
            if e.code == 404:
                raise NotFoundError(f'{path}: {text}') from None
            if e.code == 429:
                raise RateLimitError(429, text) from None
            raise APIError(e.code, text) from None
        return body if raw else json.loads(body)


class Client:
    """One SBI BRiSK session. Booting exchanges cookies for an API token on first use."""

    def __init__(self, cookies=None, session=None):
        self.session = session or Session(cookies)
        self._boot = None

    @property
    def boot(self) -> dict:
        """The app boot document: trading date, series, schedule, WebSocket URL, master/snapshot hashes."""
        if self._boot is None:
            self.session.token = self.session.get('/api/frontend/boot')['api_token']
            self._boot = self.session.get('/api/app/boot')
        return self._boot

    @property
    def date(self) -> str:
        return self.boot['date']

    def candles(self, code, interval='1d') -> Table:
        """Price bars: 5m (today, numbered), 1d, 1w or 1mo."""
        if interval not in INTERVALS:
            raise ValueError(f"Invalid interval: {interval!r}. Use '5m', '1d', '1w', or '1mo'.")
        data = self.session.get(f'/api/ohlc/{urllib.parse.quote(str(code))}', {'date': self.date})
        key, period = {'5m': ('ohlc5min', {'date': 'date', 'bar': 'index'}), '1d': ('ohlc1day', {'date': 'date'}),
                       '1w': ('ohlc1week', {'year': 'year', 'week': 'week'}),
                       '1mo': ('ohlc1month', {'year': 'year', 'month': 'month'})}[interval]
        return Table({**{name: bar[src] for name, src in period.items()}, 'open': bar['open_price'],
                      'high': bar['high_price'], 'low': bar['low_price'], 'close': bar['close_price'],
                      'turnover': bar['turnover']} for bar in data.get(key, []))

    def margin(self, code, days=365) -> Table:
        """Margin balances and stock-lending fees (JSFC), one row per trading day."""
        data = self.session.get(f'/api/jsfc/{urllib.parse.quote(str(code))}', {'count': days})
        return Table({'date': e['date'], **{name: e.get(src) for name, src in MARGIN.items()}} for e in data.values())

    def turnover(self) -> Table:
        """Turnover and shares outstanding for every listed stock."""
        return Table({'code': i['issue_code'], 'turnover': i.get('turnover'),
                      'shares_outstanding': i.get('calc_shares_outstanding')}
                     for i in self.session.get('/api/stocks_info', {'date': self.date}))

    def lists(self) -> dict[str, list[str]]:
        """Curated stock lists (NK225, recent IPOs, …) by list ID."""
        data = self.session.get('/api/stock_lists', {'date': self.date})
        return {entry['id']: entry['issue_codes'] for entry in data['stock_lists']}

    def events(self, first=0, last=618) -> Table:
        """Market events: basket orders, limit up/down, volume surges and so on."""
        data = self.session.get('/api/markets', {'date': self.date, 'series': self.boot['series'],
                                                'index_from': first, 'index_to': last})
        return Table({'index': c['index'], 'code': c.get('issue_code', ''), 'kind': c.get('kind'),
                      'type': c.get('type', 0), 'price': _yen(c.get('price10')), 'value': _yen(c.get('value10')),
                      'change_bps': c.get('diff_bps_from_last'), 'time': _at(self.date, c.get('time', ''))}
                     for c in data['market_conditions'])

    def schedule(self) -> dict:
        """Trading date, session status and session times (JST datetimes)."""
        boot = self.boot
        info = boot['schedule_info']
        return {'date': boot['date'], 'status': boot['session_status'],
                **{name: _at(boot['date'], info[key]) for name, key in SCHEDULE.items()}}

    def watchlist(self) -> list[str]:
        """Codes saved in your BRiSK watchlist."""
        data = self.session.get('/api/frontend/watchlist')
        if data.get('empty'):
            return []
        content = json.loads(zlib.decompress(base64.b64decode(data['data'])))
        if isinstance(content, dict):
            return [item['code'] for group in content.get('groups', []) for item in group.get('items', []) if 'code' in item]
        return [item if isinstance(item, str) else item['code'] for item in content
                if isinstance(item, str) or (isinstance(item, dict) and 'code' in item)]


def _yen(value10):
    return None if value10 is None else value10 / 10


def _at(date, clock):
    """JST datetime for 'HH:MM:SS[.ffffff]' or microseconds since midnight on a YYYY-MM-DD date."""
    day = dt.datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=JST)
    if isinstance(clock, int):
        return day + dt.timedelta(microseconds=clock)
    if not clock:
        return None
    h, m, s = clock.split(':')
    return day + dt.timedelta(hours=int(h), minutes=int(m), seconds=float(s))


_client: Client | None = None


def login(cookies=None, remember=False) -> Client:
    """Use SBI BRiSK session cookies (from DevTools or pycookiecheat).

    Without cookies, uses BRISK_SBI_COOKIES (JSON) or cookies saved with remember=True.
    """
    global _client
    path = cookies_path()
    if cookies is None and os.environ.get('BRISK_SBI_COOKIES'):
        cookies = json.loads(os.environ['BRISK_SBI_COOKIES'])
    if cookies is None and path.exists():
        cookies = json.loads(path.read_text())
    _client = Client(cookies)
    if remember:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as f:
            json.dump(_client.session.cookies, f)
    return _client


def passkey_enroll(**options):
    """Capture the passkey you register at SBI in a Chrome window (once). See briskapi.passkey."""
    from briskapi import passkey
    return passkey.enroll(**options)


def passkey_login(**options) -> Client:
    """Sign in with the saved passkey in Chrome and use the BRiSK cookies, as login(). See briskapi.passkey."""
    from briskapi import passkey
    return passkey.login(**options)


def passkey_forget(**options):
    """Delete the saved passkey (it stays registered at SBI until you remove it there)."""
    from briskapi import passkey
    return passkey.forget(**options)


def logout():
    """Forget the session, including saved cookies."""
    global _client
    _client = None
    cookies_path().unlink(missing_ok=True)


def _default() -> Client:
    if _client is None:
        raise SessionExpiredError('SBI BRiSK data needs a session: call briskapi.sbi.login(cookies={...}) first')
    return _client


def connect(codes=None, history=False, node='node', timeout=120, contribute=None, trace_protocol=False, profile=None,
            share_market_data=False):
    """Experimental live SBI BRiSK feed: SBI's own WASM decoder under Node, never Chrome.

    Returns a briskapi.Feed (the default source for briskapi.Ticker and Market).
    The host follows the vendor client's connect sequence: it feeds the WASM from the
    first frame, catches the snapshot up to the stream, forwards the WASM's pings and
    watches the server heartbeat. It also joins SBI's Socket.IO namespace when the
    server speaks it. Three wire details are not public, so they can be set with
    `profile` (a dict, sent through the environment because it may name tokens):

        profile={"connectQuery": {"_v": "..."}, "startLive": {...}, "catchUp": "json-vendor"}

    `trace_protocol=True` prints the connection steps to stderr with every token
    redacted, to see what the server answers. The protocol has not been validated end to
    end against a live session; failures are explicit.
    With sharing on (briskapi.consent), a timing summary is contributed when the
    session ends. share_market_data=True additionally publishes the decoded market
    recording after a clean end, under your saved alias, license and redistribution
    declaration. It requires current consent; contribute=False or BRISK_CONTRIBUTE=0
    keeps both local. Cookies, tokens and connection diagnostics are never published.
    """
    from briskapi import load
    from briskapi._live import Feed
    session = _default().session
    command = [node, str(DECODER)]
    if codes:
        command += ['--codes', codes if isinstance(codes, str) else ','.join(map(str, codes))]
    if trace_protocol:
        command.append('--trace-protocol')
    # Cookies and the profile travel in the environment, never on the command line (visible to other users).
    env = {**os.environ, 'BRISK_SBI_COOKIES': json.dumps(session.cookies)}
    if profile:
        env['BRISK_SBI_PROFILE'] = json.dumps(profile)
    feed = Feed(command=command, env=env, history=history, contribute=contribute, timing='sbi_live',
                share_market_data=share_market_data)
    try:
        return load(feed.ready(timeout))
    except BaseException:
        feed.close()
        raise
