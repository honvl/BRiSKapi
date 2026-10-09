"""Ticker and Market: auction data from a recording or live feed, plus SBI BRiSK data."""
from __future__ import annotations

from briskapi._recording import NotFoundError, Recording, Table, master_view, quote_view, timestamp


def _default():
    from briskapi import current
    return current()


def _sbi(client):
    from briskapi import sbi
    return client or sbi._default()


class Ticker:
    """Per-security auction data.

        t = Ticker("7203")
        t.info(); t.quote(); t.quote(at="09:00:00"); t.history().to_pandas()
        t.candles("5m"); t.margin()     # SBI BRiSK session (briskapi.sbi.login)
    """

    def __init__(self, code, recording: Recording | None = None, sbi=None):
        self.code = str(code)
        self._recording = recording
        self._sbi = sbi

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

    def history(self, start=None, end=None, raw=False) -> Table:
        """Every recorded update for this security, oldest first, optionally bounded by time."""
        rec = self.recording
        return Table(q if raw else quote_view(q, rec.trading_date) for q in rec.updates(self.code, start, end))

    def auction(self, at=None) -> dict:
        """Indicative auction price/volume and market-order quantities at a time."""
        q = self.quote(at)
        return {key: q[key] for key in ('code', 'time', 'indicative_price', 'indicative_volume', 'indicative_side',
                                        'indicative_open_price', 'market_buy_quantity', 'market_sell_quantity')} | {
            'market_order_imbalance': q['market_buy_quantity'] - q['market_sell_quantity']}

    def candles(self, interval='1d') -> Table:
        """Price bars from SBI BRiSK: 5m (today), 1d, 1w or 1mo."""
        return _sbi(self._sbi).candles(self.code, interval)

    def margin(self, days=365) -> Table:
        """Margin balances and stock-lending fees from SBI BRiSK, one row per trading day."""
        return _sbi(self._sbi).margin(self.code, days)


class Market:
    """Market-wide auction data.

        m = Market()
        m.stocks(); m.snapshot(at="09:00:00"); m.imbalances(top=20)
        m.turnover(); m.lists(); m.events(); m.schedule(); m.watchlist()   # SBI BRiSK session
    """

    def __init__(self, recording: Recording | None = None, sbi=None):
        self._recording = recording
        self._sbi = sbi

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

    def imbalances(self, at=None, top=None) -> Table:
        """Securities ranked by absolute market-order imbalance (buy minus sell quantity)."""
        rows = [{'code': q['code'], 'indicative_price': q['indicative_price'], 'indicative_volume': q['indicative_volume'],
                 'market_buy_quantity': q['market_buy_quantity'], 'market_sell_quantity': q['market_sell_quantity'],
                 'market_order_imbalance': q['market_buy_quantity'] - q['market_sell_quantity']}
                for q in self.snapshot(at)]
        rows.sort(key=lambda r: -abs(r['market_order_imbalance']))
        return Table(rows[:top] if top else rows)

    def summary(self) -> dict:
        """Source, trading date, coverage and replay clock range."""
        rec = self.recording
        b = rec.bootstrap
        first, last = rec.clock_range
        info = {'source': b['source'], 'trading_date': b['trading_date'], 'securities': len(b['master']),
                'market_issue_count': b['market_issue_count'], 'start': timestamp(b['trading_date'], first),
                'end': timestamp(b['trading_date'], last)}
        if rec.manifest:
            info |= {'batches': rec.manifest['summary']['batches'], 'contributor': rec.manifest['contributor'],
                     'license': rec.manifest['license']}
        return info

    def turnover(self) -> Table:
        """Turnover and shares outstanding for every listed stock (SBI BRiSK)."""
        return _sbi(self._sbi).turnover()

    def lists(self) -> dict[str, list[str]]:
        """Curated stock lists such as NK225 and recent IPOs (SBI BRiSK)."""
        return _sbi(self._sbi).lists()

    def events(self, first=0, last=618) -> Table:
        """Market events: basket orders, limit up/down, volume surges (SBI BRiSK)."""
        return _sbi(self._sbi).events(first, last)

    def schedule(self) -> dict:
        """Trading date, session status and session times (SBI BRiSK)."""
        return _sbi(self._sbi).schedule()

    def watchlist(self) -> list[str]:
        """Codes saved in your BRiSK watchlist (SBI BRiSK)."""
        return _sbi(self._sbi).watchlist()
