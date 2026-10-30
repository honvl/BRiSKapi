"""Ticker and Market: auction data from a recording or live feed, plus your broker's BRiSK data."""
from __future__ import annotations

from briskapi._recording import BriskError, NotFoundError, Recording, Table, master_view, quote_view, timestamp, trade_view
from briskapi.schema import BOOK_CHANGES, BOOKS, apply_changes, best

# Ticker.book's names for the books, as quote_view's.
_BOOK_NAMES = ('bids', 'asks', 'closing_bids', 'closing_asks')


def _default():
    from briskapi import current
    return current()


def _site(client):
    from briskapi import brisk
    return client or brisk._default()


class Ticker:
    """Per-security auction data.

        t = Ticker("7203")
        t.info(); t.quote(); t.quote(at="09:00:00"); t.book(at="09:00:00"); t.history().to_pandas(); t.trades()
        t.auction(); t.auction(closing=True)
        t.candles("5m"); t.margin()     # your broker's BRiSK session (briskapi.login)
    """

    def __init__(self, code, recording: Recording | None = None, client=None):
        self.code = str(code)
        self._recording = recording
        self._client = client

    def __repr__(self):
        return f'Ticker({self.code!r})'

    @property
    def recording(self) -> Recording:
        return self._recording or _default()

    def info(self) -> dict:
        """Master entry: name, lot size, tick type, base price and daily limits (yen)."""
        return master_view(self.recording.issue(self.code))

    def quote(self, at=None, raw=False) -> dict:
        """Latest auction quote at a JST time (default: end of recording)."""
        rec = self.recording
        issue = rec.issue(self.code)['issue_id']
        quote = rec.state(at).get(issue)
        if quote is None:
            raise NotFoundError(f'No quote for {self.code} at {at}')
        return quote if raw else quote_view(quote, rec.trading_date)

    def book(self, at=None, raw=False) -> dict:
        """Every price level of the security's books at a JST time (default: the latest), not only the ten best that
        quote() lists: its book changes replayed from the session's first update. A broker's recording or live feed
        (with history=True) from briskapi 1.9 on. {'code', 'time', 'frame', 'bids', 'asks', 'closing_bids',
        'closing_asks'}, each side best first as {'price', 'quantity', 'orders'}; raw: the decoder's [price10, quantity,
        orders] lists under its own names (bid_levels ...) and source_time_us."""
        rec = self.recording
        books, last = [{} for _ in BOOKS], None
        for quote in rec.book_updates(self.code, at):
            if BOOK_CHANGES not in quote:
                raise BriskError('This recording has no book changes: briskapi 1.9 or later records them, from a broker\'s BRiSK')
            apply_changes(books, quote[BOOK_CHANGES])
            last = quote
        if last is None:
            raise NotFoundError(f'No quote for {self.code} at {at}')
        if raw:
            return {'code': self.code, 'source_time_us': last['source_time_us'], 'frame': last['frame'],
                    **{name: best(books, k, None) for k, name in enumerate(BOOKS)}}
        return {'code': self.code, 'time': timestamp(rec.trading_date, last['source_time_us']), 'frame': last['frame'],
                **{name: [{'price': p / 10, 'quantity': q, 'orders': n} for p, q, n in best(books, k, None)]
                   for k, name in enumerate(_BOOK_NAMES)}}

    def history(self, start=None, end=None, raw=False) -> Table:
        """Every recorded update for this security, oldest first, optionally bounded by time.

        Reads the loaded recording (exactly its span) or the live feed (with history=True: every update since it
        connected); never the network. For more than one day, or bars, use briskapi.Historical."""
        rec = self.recording
        return Table(q if raw else quote_view(q, rec.trading_date) for q in rec.updates(self.code, start, end))

    def auction(self, at=None, closing=False) -> dict:
        """The auction at a time: indicative price/volume, market orders and the board's limit orders.

        closing=True gives the closing auction (closing-condition orders) instead of the session's.
        """
        return _auction(self.quote(at), closing)

    def trades(self, start=None, end=None, raw=False) -> Table:
        """Every trade (execution), oldest first, optionally bounded by time: price, shares and side
        ('buy' or 'sell' for the initiating side; None for most opening auction prints).

        Reads the loaded recording (exactly its span) or the live feed (with history=True: back to the day's start,
        as far as BRiSK's snapshot restores them); never the network. Across days: briskapi.Historical."""
        rec = self.recording
        return Table(t if raw else trade_view(t, rec.trading_date) for t in rec.trades(self.code, start, end))

    def candles(self, interval='1d') -> Table:
        """BRiSK's own price bars, from your broker's BRiSK servers: 5m (today only), 1d, 1w or 1mo. How far back
        1d, 1w and 1mo go is up to BRiSK and not yet known. Bars from your own recordings: briskapi.Historical."""
        return _site(self._client).candles(self.code, interval)

    def margin(self, days=365) -> Table:
        """Margin balances and stock-lending fees from your broker's BRiSK, one row per trading day."""
        return _site(self._client).margin(self.code, days)


class Market:
    """Market-wide auction data.

        m = Market()
        m.stocks(); m.snapshot(at="09:00:00"); m.imbalances(top=20)
        m.turnover(); m.lists(); m.events(); m.schedule(); m.watchlist()   # your broker's BRiSK session
    """

    def __init__(self, recording: Recording | None = None, client=None):
        self._recording = recording
        self._client = client

    @property
    def recording(self) -> Recording:
        return self._recording or _default()

    def stocks(self) -> Table:
        """Master for every security in the recording."""
        return Table(master_view(m) for m in self.recording.bootstrap['master'])

    def snapshot(self, at=None, raw=False) -> Table:
        """Latest quote of every security at a JST time (default: end of recording)."""
        rec = self.recording
        state = rec.state(at)
        return Table(state[i] if raw else quote_view(state[i], rec.trading_date) for i in sorted(state))

    def imbalances(self, at=None, top=None, closing=False) -> Table:
        """Securities ranked by absolute market-order imbalance (buy minus sell quantity); closing=True
        ranks the closing auction's market-on-close orders."""
        rows = [{key: value for key, value in _auction(q, closing).items() if key != 'time'} for q in self.snapshot(at)]
        rows.sort(key=lambda r: -abs(r['market_order_imbalance']))
        return Table(rows[:top] if top else rows)

    def special_quotes(self, at=None) -> Table:
        """Securities in a special quote (特別気配) at a time: its side, price and when its timer ends."""
        return Table({'code': q['code'], **q['special_quote'], 'indicative_price': q['indicative_price'],
                      'indicative_volume': q['indicative_volume']} for q in self.snapshot(at) if q['special_quote'])

    def trades(self, start=None, end=None, raw=False) -> Table:
        """Every security's trades in time order, optionally bounded by time (see Ticker.trades)."""
        rec = self.recording
        return Table(t if raw else trade_view(t, rec.trading_date) for t in rec.trades(None, start, end))

    def frames(self) -> Table:
        """Every batch (frame) of a recording: clocks, local timing, skipped issues, quote and trade counts."""
        rec = self.recording
        return Table({**f, 'time': timestamp(rec.trading_date, f['source_time_us'])} for f in rec.frames())

    def summary(self) -> dict:
        """Source, trading date, coverage and replay clock range."""
        rec = self.recording
        b = rec.bootstrap
        first, last = rec.clock_range
        info = {'source': b['source'], 'trading_date': b['trading_date'], 'securities': len(b['master']),
                'market_issue_count': b['market_issue_count'], 'start': timestamp(b['trading_date'], first),
                'end': timestamp(b['trading_date'], last)}
        if rec.manifest:
            info['batches'] = rec.manifest['summary']['batches']
        return info

    def turnover(self) -> Table:
        """Turnover and shares outstanding for every listed stock (your broker's BRiSK)."""
        return _site(self._client).turnover()

    def lists(self) -> dict[str, list[str]]:
        """Curated stock lists such as NK225 and recent IPOs (your broker's BRiSK)."""
        return _site(self._client).lists()

    def events(self, first=0, last=618) -> Table:
        """Market events: basket orders, limit up/down, volume surges (your broker's BRiSK)."""
        return _site(self._client).events(first, last)

    def schedule(self) -> dict:
        """Trading date, session status and session times (your broker's BRiSK)."""
        return _site(self._client).schedule()

    def watchlist(self) -> list[str]:
        """Codes saved in your BRiSK watchlist (your broker's BRiSK)."""
        return _site(self._client).watchlist()


def _auction(q: dict, closing: bool) -> dict:
    """One auction's state from a quote view: the session's, or with `closing` the closing auction's."""
    prefix, book = ('closing_', '_closing') if closing else ('', '')
    view = {'code': q['code'], 'time': q['time']}
    view |= {f'{prefix}{key}': q[f'{prefix}{key}'] for key in ('indicative_price', 'indicative_volume', 'indicative_imbalance_side')}
    if not closing:
        view['indicative_open_price'] = q['indicative_open_price']
    view |= {key: q[key] for key in (f'{prefix}market_buy_quantity', f'{prefix}market_sell_quantity',
                                     f'bid{book}_total_quantity', f'ask{book}_total_quantity')}
    return view | {'market_order_imbalance': q[f'{prefix}market_buy_quantity'] - q[f'{prefix}market_sell_quantity']}
