"""Timing-only statistics shared by default for SBI BRiSK sessions."""
from __future__ import annotations

import datetime as dt
from importlib.metadata import PackageNotFoundError, version

from briskapi._recording import JST
from briskapi.schema import MIN_TIMING_FRAMES, QUANTILES, TIMING_BOUNDS, TIMING_SCHEMA, validate_timing

MAX_SAMPLES = 1_000_000
STALL_MS = 1000


def _client_version():
    try:
        return version('briskapi')
    except PackageNotFoundError:  # pragma: no cover - running from an uninstalled checkout
        return '0.0.0'


def _distribution(values, low, high):
    ordered = sorted(min(max(v, low), high) for v in values)
    pick = lambda q: ordered[min(len(ordered) - 1, int(q * len(ordered)))]
    return {name: round(value, 3) for name, value in zip(QUANTILES, (pick(.5), pick(.9), pick(.99), ordered[-1]))}


class TimingStats:
    """Accumulates per-frame decode time, data age at receipt and frame spacing.

    Only these local measurements are kept; prices, quantities and codes are not.
    """

    def __init__(self, source):
        self.source = source
        self.trading_date = None
        self.frames = self.stalls = 0
        self.first = self.last = None
        self.decode, self.age, self.gaps = [], [], []
        self._midnight_ms = None

    def add(self, batch):
        if batch['type'] == 'bootstrap':
            self.trading_date = batch['trading_date']
            day = dt.datetime.strptime(self.trading_date, '%Y%m%d').replace(tzinfo=JST)
            self._midnight_ms = day.timestamp() * 1000
            return
        received = batch.get('received_unix_ms')
        if batch['type'] != 'quotes' or received is None or self.frames >= MAX_SAMPLES:
            return
        self.frames += 1
        if self.last is not None:
            gap = received - self.last
            self.gaps.append(gap)
            self.stalls += gap > STALL_MS
        self.first = self.first if self.first is not None else received
        self.last = received
        self.decode.append(batch.get('decode_ns', 0) / 1e6)
        # Age of the feed's own clock at local receipt; only as accurate as both clocks.
        self.age.append(received - (self._midnight_ms + batch['source_time_us'] / 1000))

    def report(self, contributor, license):
        """The shareable report, or None when the session was too short to say anything."""
        if self.trading_date is None or self.frames < MIN_TIMING_FRAMES:
            return None
        minute = lambda ms: dt.datetime.fromtimestamp(ms / 1000, JST).strftime('%H:%M')
        report = dict(schema=TIMING_SCHEMA, source=self.source, trading_date=self.trading_date,
                      first_minute=minute(self.first), last_minute=minute(self.last), frames=self.frames,
                      stalls=self.stalls, client_version=_client_version(), contributor=contributor, license=license,
                      decode_ms=_distribution(self.decode, *TIMING_BOUNDS['decode_ms']),
                      source_age_ms=_distribution(self.age, *TIMING_BOUNDS['source_age_ms']),
                      interarrival_ms=_distribution(self.gaps or [0], *TIMING_BOUNDS['interarrival_ms']))
        return validate_timing(report)
