"""Python API for BRiSK auction data: recordings, your private archive and the public demo.

    import briskapi

    feed = briskapi.connect(site="demo")       # live: the demo replay ("sbi", "e-shiten", ...: your broker's BRiSK)
    briskapi.Ticker("7203").quote()            # current auction quote
    for q in feed.quotes("7203"): ...          # or feed.on_quote(callback)

    briskapi.pull(prefix)                      # or briskapi.load(path) / briskapi.record(...)
    briskapi.Ticker("7203").quote(at="08:59:59")
    briskapi.Market().imbalances(top=10).to_pandas()

    live = briskapi.Live()                     # Databento-style records from `brisk live` on this computer
    live.subscribe(dataset="XTKS.BRISK", schema="imbalance", symbols=["7203"])
    for record in live: ...
    briskapi.Historical().timeseries.get_range(...)   # the same records from your archive in the cloud
"""
from __future__ import annotations

import warnings

from briskapi import brisk as _brisk, cli as _cli
from briskapi._archive import Archive
from briskapi._gateway import Live, Record
from briskapi._historical import Historical
from briskapi._live import Feed, connect as _connect, record as _record, stream
from briskapi._market import Market, Ticker
from briskapi._recording import JST, BriskError, NotFoundError, Recording, Table

__version__ = '1.8.3'
__all__ = ['JST', 'Archive', 'BriskError', 'Feed', 'Historical', 'Live', 'Market', 'NotFoundError', 'Record', 'Recording',
           'Table', 'Ticker', 'connect', 'consent', 'current', 'load', 'login', 'logout', 'pull', 'record', 'recordings',
           'stream']

_current: Recording | Feed | None = None


def load(source) -> Recording | Feed:
    """Use a live Feed or a recording (path, directory or Recording) as the default for Ticker and Market."""
    global _current
    _current = source if isinstance(source, (Recording, Feed)) else Recording(source)
    return _current


def current() -> Recording | Feed:
    if _current is None:
        raise BriskError('Nothing loaded: use briskapi.connect(...), briskapi.load(path), briskapi.pull(prefix) or briskapi.record(...)')
    return _current


_DEMO_ONLY = {'cache': None, 'speed': 1, 'limit_frames': None}
_BROKER_ONLY = {'trace_protocol': False, 'profile': None, 'upload_market_data': None, 'fast_symbols': None}
SITES = ('demo', *_brisk.SITES)   # the demo, then the brokers' BRiSK


def connect(*, site=None, codes=None, history=False, upload=None, output=None, on_day=None, gateway=False, timeout=None,
            runtime=None, cache=None, speed=1, limit_frames=None, trace_protocol=False, profile=None,
            upload_market_data=None, fast_symbols=None) -> Feed:
    """Start a live feed and make it the default for Ticker and Market, as `brisk live --site SITE` does:

    - site="demo": the public BRiSK Next demo. Not a live connection: a recording of 27 September 2021
      that BRiSK's demo site serves as files, downloaded (or, with cache=DIR, a copy downloaded before)
      and replayed at its recorded pace (`speed`; `limit_frames` stops early).
    - site="sbi", "e-shiten", "matsui", "monex" or "smbcnikko": your broker's live BRiSK (Matsui's, Monex's
      and SMBC Nikko's are the same app, not yet tried live), signing in as login(site) does
      unless login already did (`trace_protocol`, `profile`, `upload_market_data`, `fast_symbols`).

    With gateway=True a broker feed reads some symbols' quotes for Live sessions ahead of the recording, on a fast lane
    of its own: by default 500 to 1,000 symbols, by the cores; `fast_symbols=1500` gives it workers for that many (the
    recording's get the rest of them: set BRISK_WORKERS for more in all).

    The feed records each trading day and uploads it to your private archive unless you opt out
    (upload=False), a broker's in two recordings, its morning and afternoon sessions; `output`
    also keeps each recording on disk, `on_day(result)` is called with each,
    `history=True` keeps every update for Ticker.history(), and gateway=True also serves the feed to
    briskapi.Live on this computer. `timeout` (default 60 s, 120 s for a broker) bounds the wait for the
    initial state. A broker feed started with no session to join (after the close, on a holiday) waits for
    the next trading day's, however long that is (status 'waiting'; `feed.waiting` says which day was over).
    """
    if site not in SITES:
        raise ValueError(f'Choose a site: {", ".join(map(repr, SITES))} (got {site!r})')
    common = dict(codes=codes, history=history, upload=upload, output=output, on_day=on_day, gateway=gateway, runtime=runtime)
    given = dict(cache=cache, speed=speed, limit_frames=limit_frames, trace_protocol=trace_protocol, profile=profile,
                 upload_market_data=upload_market_data, fast_symbols=fast_symbols)
    demo = site == 'demo'
    wrong = [name for name, default in (_BROKER_ONLY if demo else _DEMO_ONLY).items() if given[name] != default]
    if wrong:
        raise ValueError(f"{', '.join(wrong)}: only for {'a broker site' if demo else 'the demo'}")
    if demo:
        return load(_connect(cache=cache, speed=speed, limit_frames=limit_frames, timeout=timeout or 60, **common))
    return load(_brisk._connect(site, timeout=timeout or 120, trace_protocol=trace_protocol, profile=profile,
                                upload_market_data=upload_market_data, fast_symbols=fast_symbols, **common))


def login(site='sbi', *, cookies=None, remember=False) -> _brisk.Client:
    """Sign in to your broker's BRiSK, site "sbi" (the default), "e-shiten", "matsui", "monex" or "smbcnikko",
    with its session cookies: given (copied from your logged-in browser), from the site's variable
    (BRISK_SBI_COOKIES, BRISK_ESHITEN_COOKIES, BRISK_MATSUI_COOKIES, BRISK_MONEX_COOKIES, BRISK_SMBCNIKKO_COOKIES;
    JSON) or saved by remember=True or `brisk login` (passkey). Ticker's and Market's broker data
    (candles, margin, turnover, lists, events, schedule, watchlist) then come from it, and
    connect(site=...) uses it."""
    return _brisk._login(site, cookies, remember)


def logout():
    """Forget the broker session, and the cookies saved for every site."""
    _brisk._logout()


def recordings(date=None, source=None) -> Table:
    """Your archived recordings (see Archive.recordings)."""
    return Archive().recordings(date, source)


def pull(prefix, output=None) -> Recording:
    """Download and verify an archive recording, then make it the default."""
    return load(Archive().pull(prefix, output))


def record(output, **options) -> Recording:
    """Record the public demo (see briskapi._live.record), upload it per consent and make it the default."""
    return load(_record(output, **options))


def consent(accept=False, revoke=False) -> dict | None:
    """Show (no arguments), accept or revoke automatic uploads to your private archive (on by default:
    `default: True` until you decide). Accepting makes your archive identity if there is none and
    prints it to stderr. See PRIVACY.md."""
    if accept and revoke:
        raise ValueError('Choose accept or revoke')
    if accept:
        return _cli.accept()
    if revoke:
        return _cli.save_consent(False)
    saved = _cli.load_consent()
    if saved is None and (old := _cli.lapsed_consent()):
        warnings.warn(_cli.policy_changed(old) + 'Uploads are on by default until you decide: briskapi.consent(accept=True) '
                      'or briskapi.consent(revoke=True).', stacklevel=2)
    return saved or dict(_cli.DEFAULT)
