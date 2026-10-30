#!/usr/bin/env python3
"""Consume BRiSK auction data live, record it, upload it to your private archive and use that archive."""
import argparse
import contextlib
import datetime as dt
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import warnings

from briskapi import _https, _native, _parquet, _repair, identity
from briskapi._recording import BriskError, Recording
from briskapi.schema import (RECORDING_PREFIX, REFERENCES, SCHEMA, SOURCES, MAX_COMPRESSED, TIMING_KEY, canonical_batches,
                             encode, local_name, max_package, owner, recording_name, validate_manifest, validate_timing,
                             require)

PACKAGE = Path(__file__).resolve().parent
# BRiSK's own WASM decoder runs under Node or Bun; the package ships only this host and SHA-256 pins.
DECODER = PACKAGE / 'decoder' / 'decoder.cjs'
MIN_NODE = 22
MIN_BUN = (1, 4)    # the release it was tried on is 1.4.3
# Bump with any PRIVACY.md change to what is collected; saved choices then lapse.
POLICY_VERSION = 10
NOTICE = '''\
briskapi uploads your sessions to your private BRiSK archive: a free, off-computer copy
of every complete session, reachable from any computer with your identity and checked on
arrival (a complete demo replay must match the reference). Keep recordings there and free
your disk with `brisk prune`; `brisk pull` brings them back.
Only you can list, download or delete your uploads. They are filed under your identity, a
random key made on this computer and never registered, kept in your system's credential
store and in ~/.config/brisk/identity.json. The archive's operator can access what it
stores. What you delete can be restored for 30 days and is then removed.
The cloud API (`brisk query`) builds OHLC bars from your own broker recordings only, for
the securities and times they cover: no one else's uploads enter your results, and yours
enter no one else's.
Every clean, complete session uploads automatically (the default; you can decline); a
broker day uploads a few minutes at a time as it runs, and so does the part of one
recorded before a lost connection, a stop or a crash:
  - demo sessions: the decoded market data and local timing measurements (decode
    durations, replay lateness, asset download time and your computer's receipt clock,
    which shows when you recorded);
  - broker BRiSK sessions (SBI, Tachibana e-shiten, Matsui, Monex, SMBC Nikko):
    a timing summary (decode time, data age and frame spacing percentiles, stalls, frame
    count, date and start/end minute) and the decoded market recording: securities
    master, prices, quantities, every order-book price level on each side (and of the
    closing-condition orders), the board's order totals, every trade (time, price, shares
    and side), codes and per-frame timing.
Cookies, tokens and connection diagnostics are never uploaded. Your IP address is used
only for upload rate limiting. No account, file, hostname or system details are sent.
Policy: https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md
(日本語: https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.ja.md)
Opt out for one run with --no-upload (or --no-upload-market-data to keep a broker
capture's market data local), and for good with `brisk consent --revoke` or BRISK_UPLOAD=0.
'''

def pick_runtime(runtime=None):
    """The program that runs the decoder host: the one named, else Bun on macOS when it is installed, else Node.
    On a Mac Bun takes a busy opening about 11% faster than Node; on Windows Node is the faster of the two (ARCHITECTURE.md)."""
    if runtime:
        return runtime
    return (shutil.which('bun') if sys.platform == 'darwin' else None) or 'node'


def is_bun(runtime):
    return Path(str(runtime)).stem.lower() == 'bun'


def check_runtime(runtime=None):
    """Fail early, with the fix, when the host's runtime (Node, or Bun) is missing or older than the decoder hosts support."""
    runtime = pick_runtime(runtime)
    bun = is_bun(runtime)
    advice = (f'Install Bun {MIN_BUN[0]}.{MIN_BUN[1]} or newer from https://bun.sh, or pass the path to it.' if bun
              else f'Install Node.js {MIN_NODE} or newer from https://nodejs.org, or pass the path to it.')
    try:
        out = subprocess.run([runtime, '--version'], capture_output=True, text=True, timeout=30).stdout
    except FileNotFoundError:
        raise BriskError(f'Bun was not found (looked for {str(runtime)!r}). {advice}' if bun else
                         f'Node.js was not found (looked for {str(runtime)!r}). BRiSK\'s decoder runs under Node. {advice}') from None
    except (OSError, subprocess.SubprocessError) as error:
        raise BriskError(f'Could not run {str(runtime)!r} --version: {error}. {advice}') from error
    if bun:
        found = re.match(r'(\d+)\.(\d+)', out.strip())
        if found and (int(found[1]), int(found[2])) < MIN_BUN:
            raise BriskError(f'Bun {out.strip()} is too old; the decoder hosts need Bun {MIN_BUN[0]}.{MIN_BUN[1]}+. {advice}')
        return
    found = re.match(r'v(\d+)\.', out.strip())
    if found and int(found[1]) < MIN_NODE:
        raise BriskError(f'Node.js {out.strip()} is too old; the decoder hosts need Node {MIN_NODE}+. {advice}')


def settings(path=None):
    return json.loads((path or PACKAGE / 'archive.json').read_text())

def consent_path():
    return Path(os.environ.get('XDG_CONFIG_HOME') or Path.home() / '.config') / 'brisk' / 'uploads.json'

def load_consent():
    """The saved upload choice under the current policy, or None if none is saved yet."""
    try:
        choice = json.loads(consent_path().read_text())
    except (OSError, ValueError):
        return None
    return choice if isinstance(choice, dict) and choice.get('policy_version') == POLICY_VERSION else None

def lapsed_consent():
    """The choice saved under an earlier policy version, which no longer applies (a refusal as much as an
    acceptance), or None."""
    try:
        choice = json.loads(consent_path().read_text())
    except (OSError, ValueError):
        return None
    if isinstance(choice, dict) and isinstance(choice.get('policy_version'), int) and choice['policy_version'] != POLICY_VERSION:
        return choice
    return None

def policy_changed(old):
    """What to tell someone whose saved choice lapsed with a new policy version ('' if none did): which choice, and
    that the policy (PRIVACY.md) says what is different."""
    if not old:
        return ''
    chose = 'upload' if old.get('enabled') else 'not upload'
    return (f'The upload policy has changed (version {old["policy_version"]}, now {POLICY_VERSION}): your earlier choice '
            f'to {chose} no longer applies; PRIVACY.md says what is different. ')

def save_consent(enabled):
    choice = dict(policy_version=POLICY_VERSION, enabled=bool(enabled),
                  decided_at=dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'))
    path = consent_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(choice, indent=2) + '\n')
    return choice

def interactive():
    return sys.stdin.isatty() and sys.stderr.isatty()

def ask_consent():
    """One-time first-run question; Enter accepts. Prompts go to stderr, never stdout. Accepting makes the
    identity (if there is none) and shows it."""
    sys.stderr.write(policy_changed(lapsed_consent()) + '\n' * bool(lapsed_consent()) + NOTICE
                     + 'Upload automatically to your private archive? [Y/n] ')
    sys.stderr.flush()
    answer = sys.stdin.readline()  # end of input is no answer, not Enter
    return accept(bool(answer) and answer.strip().lower() in {'', 'y', 'yes'})

def accept(enabled=True):
    """Save the upload choice; turning uploads on makes the identity if needed and shows it on stderr."""
    choice = save_consent(enabled)
    if enabled:
        show_identity(*identity.ensure())
    return choice

def show_identity(value, created):
    sys.stderr.write(f'Your archive identity: owner {owner(value)}.\n')
    if created and interactive():
        sys.stderr.write(f'Recovery code (write it down; `brisk identity --recover CODE` restores your archive on any '
                         f'computer): {identity.recovery_code(value)}\n')
    elif created:
        sys.stderr.write('Write down its recovery code (`brisk identity --recovery-code`): it restores your archive on '
                         'any computer.\n')
    sys.stderr.flush()

def opted_out(upload=None):
    """True when this process or run opts out of uploads (BRISK_UPLOAD=0, --no-upload, upload=False)."""
    return upload is False or os.environ.get('BRISK_UPLOAD') == '0'

DEFAULT = {'policy_version': POLICY_VERSION, 'enabled': True, 'default': True}
_told = False

def consented(upload=None, ask=True, notify=None):
    """The choice for a run: the saved one; with none, the CLI asks once on a terminal (`ask`), and a run that
    can't ask (the Python API, a CLI without a terminal) uploads by default, saying so once per process."""
    choice = load_consent()
    if choice is not None:
        return choice
    if opted_out(upload):
        return dict(DEFAULT)  # packaged as usual, not uploaded
    if ask and interactive():
        return ask_consent()
    global _told
    if not _told:
        _told = True
        (notify or tell_default)(*identity.ensure(), lapsed_consent())
    return dict(DEFAULT)

def tell_default(value, created, old=None):
    sys.stderr.write(policy_changed(old) + '\n' * bool(old) + NOTICE + 'Nothing here can ask, so uploads are on by '
                     'default until you decide (`brisk consent --accept` or `--revoke`).\n')
    show_identity(value, created)

def declaration(args):
    """Whether this run's recording is packaged for upload: the saved choice (asked once, interactively)."""
    choice = consented(args.upload)
    return bool(choice and choice['enabled'])

def save_recording(events, output):
    """Keep decoder JSONL locally as a recording directory (Parquet tables, no manifest)."""
    output.mkdir(parents=True, exist_ok=True)
    require(not any((output / name).exists() for name in _parquet.FILES), 'Recording output already exists')
    try:
        with events.open('rb') as source:
            _parquet.write(canonical_batches(source), output)
    except BaseException:
        for name in _parquet.FILES:
            (output / name).unlink(missing_ok=True)
        raise

def package(events, output):
    """Encode decoder JSONL as a package: the recording's tables plus a manifest of their zip."""
    return _package(output, lambda: save_recording(events, output))

def package_parts(equity, parts, output, repair=False, piece=None, progress=None):
    """A spooled segment (briskapi._spool) as a package: the same tables and manifest as package() of its batches, written by
    brisk-parquet a range of issues at a time (`equity` is the segment's master table, in its folder's data/). With `repair`, a
    segment cut short first gets the end its host never wrote. `piece` ({midnight, day_start, cut, state_out, end}: Spool.ship's)
    makes a piece of a day instead, and gets back where it leaves the day ({clock}, and the book in the file `state_out`) and
    whether it was repaired. `progress(label)` is told how brisk-parquet goes (see _native.package)."""
    piece = {} if piece is None else piece

    def write():
        midnight = piece.get('midnight') or _parquet.midnight_ns(_parquet.bootstrap_row(parts[0])['trading_date'])
        result = _native.package(Path(equity).parent.parent, parts, output, midnight, day_start=piece.get('day_start'),
                                 cut=piece.get('cut'), end=piece.get('end') or ('repair' if repair else 'parts'),
                                 state_out=piece.get('state_out'), threads=_native.threads(), progress=progress)
        piece['clock'], piece['repaired'] = result['clock'], result['repaired']
        return result['summary']   # not checked here: the archive checks every upload, and a day took minutes
    return _package(output, write)

def _package(output, write):
    """Package what write() puts in `output`; it returns the summary, or None to have the tables checked for it."""
    output.mkdir(parents=True, exist_ok=True)
    require(not any((output / name).exists() for name in (*_parquet.FILES, 'manifest.json')), 'Package output already exists')
    try:
        summary = write()
        if summary is None:
            summary = _parquet.summarize(output)
        zipped = output.with_name(f'.{output.name}.zip')
        try:
            sha256, size = _parquet.pack_to(output, zipped)
        finally:
            zipped.unlink(missing_ok=True)
        limit = max_package(summary['source'])
        require(size <= limit, f'The package is {size / 2**20:.0f} MiB; the archive accepts up to {limit // 2**20} MiB')
        m = validate_manifest(dict(schema=SCHEMA, sha256=sha256, bytes=size, summary=summary))
    except BaseException:
        for name in _parquet.FILES:
            (output / name).unlink(missing_ok=True)
        raise
    (output / 'manifest.json').write_text(json.dumps(m, indent=2) + '\n')
    return m

def secure(url):
    """Refuse to send the identity anywhere but HTTPS (or plain HTTP to this computer, for a local service)."""
    parts = urllib.parse.urlsplit(url)
    require(parts.scheme == 'https' or (parts.scheme == 'http' and parts.hostname in {'localhost', '127.0.0.1', '::1'}),
            f'Refusing to send your identity to {parts.scheme}://{parts.hostname}: use an https URL')
    return url

def request_json(url, data=None, secret=None):
    request = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode(),
                                     headers={'Content-Type': 'application/json'})
    if secret:
        secure(url)
        # Unredirected: urllib would copy an ordinary header to wherever a redirect points.
        request.add_unredirected_header('Authorization', f'Bearer {secret}')
    with urllib.request.urlopen(request, timeout=60, context=_https.context()) as response:
        return json.load(response)

def api(api_url, action, **fields):
    """One archive request as this computer's identity (made on first use)."""
    return request_json(api_url, {'action': action, **fields}, identity.secret())

class _Body(io.RawIOBase):
    """An upload's multipart body: its head, a file read as it is sent, and its tail."""

    def __init__(self, head, path, tail, progress=None):
        super().__init__()
        self._parts = [io.BytesIO(head), open(path, 'rb'), io.BytesIO(tail)]
        self.size = len(head) + Path(path).stat().st_size + len(tail)
        self._progress, self._sent = progress, 0   # progress(sent, size) as the body is read

    def readable(self):
        return True

    def readinto(self, buffer):
        while self._parts:
            count = self._parts[0].readinto(buffer)
            if count:
                self._sent += count
                if self._progress:
                    self._progress(self._sent, self.size)
                return count
            self._parts.pop(0).close()
        return 0

    def close(self):
        for part in self._parts:
            part.close()
        self._parts = []
        super().close()


def upload(directory, api_url, timeout=1500, verify=True, out=None, market=None, progress=None):
    """Upload a package to your private archive and wait for its validation (a broker's trading session takes
    the archive some minutes).

    A demo (or synthetic) replay's market data is the same for everyone and the archive keeps it once,
    so such a package sends only its timing table once the archive holds that copy; `market=True`
    sends the whole recording anyway.

    `progress(stage, done=None, total=None)` is told of 'preparing' (the zip it sends), 'uploading' with the bytes sent of the
    total, and 'validating' at each check of the archive's.
    """
    progress = progress or (lambda *told: None)
    m = json.loads((directory / 'manifest.json').read_text())
    progress('preparing')
    if verify:
        _parquet.inspect_package(directory, m)
    with tempfile.TemporaryDirectory() as tmp:
        zipped = Path(tmp) / 'recording.zip'
        sha256, size = _parquet.pack_to(directory, zipped)
        require(size == m['bytes'] and sha256 == m['sha256'], 'Size/hash mismatch')
        ticket = None
        if not market and REFERENCES.get(m['summary']['source']):
            timing = Path(tmp) / 'timing.zip'
            sha256, size = _parquet.pack_to(directory, timing, (_parquet.BATCHES,))
            try:
                ticket = api(api_url, 'upload', manifest=m, part={'sha256': sha256, 'bytes': size})
                zipped = timing
            except urllib.error.HTTPError as error:
                if error.code != 409:  # 409: the archive has no copy of the market data yet
                    raise
        if ticket is None:
            ticket = api(api_url, 'upload', manifest=m, part=None)
        # S3 browser POST policies bind key, encryption, content type and exact size; the file is read as it is
        # sent, never whole in memory.
        boundary = uuid.uuid4().hex
        head = b''.join(f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode()
                        for key, value in ticket['upload']['fields'].items())
        head += (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="recording.zip"\r\n'
                 'Content-Type: application/zip\r\n\r\n').encode()
        with _Body(head, zipped, f'\r\n--{boundary}--\r\n'.encode(), lambda sent, size: progress('uploading', sent, size)) as body:
            progress('uploading', 0, body.size)
            request = urllib.request.Request(ticket['upload']['url'], data=body, method='POST',
                                             headers={'Content-Type': f'multipart/form-data; boundary={boundary}',
                                                      'Content-Length': str(body.size)})
            with urllib.request.urlopen(request, timeout=120, context=_https.context()) as response:
                response.read()
    status_url = api_url.rstrip('/') + '/?ticket=' + ticket['ticket']
    print(json.dumps({'ticket': ticket['ticket'], 'status_url': status_url}), file=out, flush=True)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        progress('validating')
        status = request_json(status_url)
        if status['status'] == 'stored':
            return status
        require(status['status'] != 'rejected', 'Automatic validation rejected recording: ' + status.get('reason', 'unknown'))
        time.sleep(3)
    raise TimeoutError(f'Upload still being validated; check {status_url}')

def upload_timing(report, api_url):
    """Keep a timing-only report (no market data) in your archive; returns the service's response."""
    return api(api_url, 'timing', report=report)

def _listing(api_url, kind, date):
    require(date is None or (len(date) == 8 and date.isdigit()), 'Date must be YYYYMMDD')
    after = None
    while True:
        page = api(api_url, 'list', kind=kind, date=date, after=after)
        yield from page['items']
        if not (after := page['next']):
            return

def timing_reports(api_url, date=None):
    """Your timing reports, oldest dates first."""
    who = identity.owner_id()
    for item in _listing(api_url, 'timing', date):
        found = TIMING_KEY.fullmatch(item['key'])
        require(found and found[1] == who, 'Archive identity mismatch')
        yield item['key'], validate_timing(item['report'])

def manifests(api_url, date=None):
    """Your recordings, as (prefix, manifest); invalid or unsupported ones are skipped with a warning."""
    who = identity.owner_id()
    for item in _listing(api_url, 'recordings', date):
        try:
            yield item['prefix'], _check_manifest(item['prefix'], item['manifest'], who)
        except (ValueError, TypeError, KeyError, RecursionError):
            warnings.warn(f'Skipping invalid or unsupported archive manifest: {item.get("prefix")}', stacklevel=2)

def _check_manifest(prefix, manifest, who):
    m = validate_manifest(manifest)
    require(isinstance(prefix, str) and RECORDING_PREFIX.fullmatch(prefix) and prefix == f'archive/{who}/{recording_name(m)}',
            'Archive identity mismatch')
    return m

def pull(api_url, prefix, output):
    """Download one of your recordings, verify it and keep its tables and manifest in `output`."""
    require(isinstance(prefix, str) and RECORDING_PREFIX.fullmatch(prefix), 'Invalid archive prefix')
    require(not output.exists(), 'Download output already exists')
    found = api(api_url, 'download', prefix=prefix)
    # Refuse unsupported sources and identities before downloading payloads.
    m = _check_manifest(prefix, found['manifest'], identity.owner_id())
    urls = found['files']
    require(isinstance(urls, dict) and set(urls) == set(_parquet.FILES)
            and all(isinstance(url, str) and url.startswith('https://') for url in urls.values()), 'Invalid download URLs')
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent) as tmp:
        directory, total = Path(tmp), 0
        for name in _parquet.FILES:
            (directory / name).parent.mkdir(parents=True, exist_ok=True)
            with urllib.request.urlopen(urls[name], timeout=120, context=_https.context()) as body, \
                    (directory / name).open('wb') as sink:
                while chunk := body.read(2**20):
                    total += len(chunk)
                    require(total <= m['bytes'], 'Size/hash mismatch')
                    sink.write(chunk)
        # The tables pack to the zip the manifest hashes, byte for byte (inspect_package), and pass its checks.
        _parquet.inspect_package(directory, m)
        (directory / 'manifest.json').write_text(json.dumps(m, indent=2) + '\n')
        shutil.move(str(directory), str(output))
    return m

def delete(api_url, prefix):
    """Remove one of your recordings (or timing reports) from your archive; `restore` brings it back within
    30 days; after that it is removed."""
    return api(api_url, 'delete', prefix=prefix)

def deleted(api_url, date=None):
    """What you deleted and can still restore: recordings (prefix, manifest) and timing reports (key, report),
    each with deleted_at and restore_until."""
    who = identity.owner_id()
    for item in _listing(api_url, 'deleted', date):
        found = RECORDING_PREFIX.fullmatch(item.get('prefix', '')) or TIMING_KEY.fullmatch(item.get('key', ''))
        require(found and found[1] == who, 'Archive identity mismatch')
        yield item

def restore(api_url, prefix):
    """Bring back one of your recordings (or timing reports) deleted in the last 30 days."""
    return api(api_url, 'restore', prefix=prefix)

def prune(api_url, directory):
    """Free disk space: remove a local package (tables and manifest) once your archive holds it, verified by its
    SHA-256. `brisk pull` brings it back. Anything else in the folder is left alone."""
    directory = Path(directory)
    require((directory / 'manifest.json').exists(), f'{directory} has no manifest: only an uploaded package can be pruned')
    m = validate_manifest(json.loads((directory / 'manifest.json').read_text()))
    require(_parquet.digest(_parquet.pack(directory)) == m['sha256'], f'{directory} differs from its manifest; not pruned')
    listed = list(manifests(api_url, m['summary']['trading_date']))
    held = [prefix for prefix, stored in listed if stored['sha256'] == m['sha256']]
    if not held and REFERENCES.get(m['summary']['source']):
        # The archive keeps a demo replay's market tables once, so they may differ from these byte for byte (not in
        # content: both are verified); what is this recording's own is its timing table.
        timing = (directory / _parquet.BATCHES).read_bytes()
        held = [prefix for prefix, stored in listed if stored['summary'] == m['summary'] and _stored_table(api_url, prefix) == timing]
    require(held, f'Your archive does not hold {directory} yet: upload it first (`brisk upload {directory}`)')
    files = [directory / name for name in (*_parquet.FILES, 'manifest.json')]
    freed = sum(f.stat().st_size for f in files)
    for f in files:
        f.unlink()
    for folder in (directory / 'data' / 'custom', directory / 'data', directory):
        if folder.is_dir() and not any(folder.iterdir()):
            folder.rmdir()
    return {'pruned': str(directory), 'bytes': freed, 'prefix': held[0]}

def _stored_table(api_url, prefix, name=_parquet.BATCHES):
    url = api(api_url, 'download', prefix=prefix)['files'][name]
    require(isinstance(url, str) and url.startswith('https://'), 'Invalid download URL')
    with urllib.request.urlopen(url, timeout=120, context=_https.context()) as body:
        return body.read(MAX_COMPRESSED + 1)

def export(source, nautilus=None, jsonl=None):
    """Write a recording as a NautilusTrader v2 catalog or as canonical JSONL (for brisk_recording)."""
    recording = Recording(source)
    if nautilus:
        require(recording.format == 'parquet', 'Export a Parquet recording (record, pull or package one)')
        require(not (nautilus / 'data').exists(), 'Catalog output already exists')
        return {'files': len(_parquet.export_nautilus(recording.directory, nautilus)), 'catalog': str(nautilus)}
    require(not jsonl.exists(), 'JSONL output already exists')
    with jsonl.open('wb') as out:
        for batch in recording.batches():
            out.write(encode(batch))
    return {'jsonl': str(jsonl)}

def repair(recordings, others, output=None, until=None, window=5.0):
    """Repair recordings from other feeds' recordings of the same day (see briskapi._repair.repair); each is a recording's folder
    or a folder of recordings. With `output`, each recording that gained something is written there, verified as the archive would
    and packaged for upload: one recording's into `output` itself, several into a folder of them by name. Their entries in the
    report have the `package`."""
    recordings, others = [Path(p) for p in recordings], [Path(p) for p in others]
    if output is None:
        return _repair.repair(recordings, others, None, until, window)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    single = len(recordings) == 1 and (recordings[0] / _parquet.DEPTHS).is_file()

    def deliver(recording, tables, entry):
        destination = output if single else output / recording.directory.name

        def write():
            for name in _parquet.FILES:
                (destination / name).parent.mkdir(parents=True, exist_ok=True)
                shutil.move(tables / name, destination / name)
            return _parquet.verify_broker(destination)
        entry['package'] = _package(destination, write)
    return _repair.repair(recordings, others, deliver, until, window, output.parent)

def record_events(events, cache=None, codes=None, limit_frames=None, speed=1, binary=None, runtime=None):
    """Replay the pinned demo (downloaded, or a copy in `cache`) and save its decoded batches.

    Needs only Node (or Bun). A Rust collector binary (`brisk_quote_ingest`) is optional; it
    records the same batches and adds its own state validation and latency display.
    """
    runtime = pick_runtime(runtime)
    check_runtime(runtime)
    options = ['--web'] if cache is None else ['--cache', str(cache)]
    options += ['--speed', str(speed)]
    if codes:
        options += ['--codes', codes if isinstance(codes, str) else ','.join(codes)]
    if limit_frames:
        options += ['--limit-frames', str(limit_frames)]
    if binary:
        with tempfile.TemporaryDirectory() as tmp:
            subprocess.run([str(binary), '--decoder', str(DECODER), '--events', str(events),
                            '--latest', str(Path(tmp) / 'latest.json'), *options], check=True)
        return
    with open(events, 'wb') as out:
        subprocess.run([runtime, str(DECODER), *options], check=True, stdout=out)

def live(args):
    """Record the feed, reporting how it goes on stderr (with --quotes, print each quote update as one JSON line),
    and serve it to briskapi.Live on this computer (unless --no-gateway); a complete session is uploaded per consent."""
    global SAVE_EVERY
    SAVE_EVERY = SAVE_SECONDS[sys.stderr.isatty()]
    try:
        with _warnings_on_stderr():
            _live(args)
    finally:
        SAVE_EVERY = None


@contextlib.contextmanager
def _warnings_on_stderr():
    """Show each warning the moment it is raised, on any thread, as its message: `brisk live` runs for days, and a warning held
    until connect() returns (which waits for the next trading day when no session is running), or recorded in a context that has
    ended, reaches no one (a feed's threads keep the context they were started in)."""
    with warnings.catch_warnings():
        warnings.simplefilter('always', UserWarning)
        shown = warnings.showwarning

        def show(message, category, filename, lineno, file=None, line=None):
            if issubclass(category, UserWarning):
                print(message, file=sys.stderr, flush=True)
            else:
                shown(message, category, filename, lineno, file, line)
        warnings.showwarning = show
        try:
            yield
        finally:
            warnings.showwarning = shown   # catch_warnings does not, where warnings are kept per context (free-threaded 3.14)


def _live(args):
    from briskapi import connect, login  # The API package builds on this module.
    codes = args.codes.split(',') if args.codes else None
    if args.site == 'demo' and args.fast_symbols is not None:
        raise BriskError("--fast-symbols applies to a broker's site")
    if args.site != 'demo':
        login(args.site)  # Missing cookies fail before the notice, questions or decoder startup.
    consented(args.upload)
    # `kill` and a shutdown stop it as Ctrl-C does: the running day ends where it is, and is kept and uploaded.
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _interrupt)
    report = lambda day: print(json.dumps({'day': day}), file=sys.stderr, flush=True)  # noqa: E731
    source = (dict(cache=args.cache, speed=args.speed, limit_frames=args.limit_frames) if args.site == 'demo' else
              dict(trace_protocol=args.trace_protocol, upload_market_data=args.upload_market_data,
                   fast_symbols=args.fast_symbols))
    try:
        # With no session to join (after the close, on a holiday), a broker feed waits here for the next one's.
        feed = connect(site=args.site, codes=codes, upload=args.upload, output=args.output, on_day=report,
                       gateway=args.gateway, **source)
    except KeyboardInterrupt:  # connect() has stopped the feed: no day had begun
        print('Stopped before a trading day began', file=sys.stderr)
        return
    if feed.gateway:
        print(f'Serving {feed.gateway.dataset} to briskapi.Live() on this computer only '
              f'(127.0.0.1:{feed.gateway.port}; --no-gateway turns this off)', file=sys.stderr, flush=True)
    try:
        if args.quotes:
            for quote in feed.quotes(raw=args.raw):
                print(json.dumps(quote, ensure_ascii=False, default=lambda value: value.isoformat()), flush=True)
        else:
            _progress(feed, PROGRESS_SECONDS[sys.stderr.isatty()])
        feed.wait()
    except KeyboardInterrupt:
        print('Stopping: the running day ends here, then it is kept and uploaded as chosen '
              '(Ctrl-C again quits at once)', file=sys.stderr, flush=True)
    finally:
        feed.close()
    # A day that could not end (a demo replay, or a host that did not stop in time) uploads only its timing summary.
    if feed.timing_uploaded and not (feed.recordings and feed.recordings[-1]['timing_uploaded'] is feed.timing_uploaded):
        print(json.dumps({'timing_uploaded': feed.timing_uploaded}), file=sys.stderr)

# How often `brisk live` reports on the recording: on a terminal, and when its stderr goes to a file or a log.
PROGRESS_SECONDS = {True: 10, False: 60}
# The same for a recording being saved (see Saving). SAVE_EVERY is set while `brisk live` runs: a feed of the API saves quietly.
SAVE_SECONDS = {True: 5, False: 30}
SAVE_EVERY = None


def _progress(feed, every):
    """Until the feed ends, a line on stderr every `every` seconds while a day records: its date, how long it has
    recorded, its quote updates and their rate since the last line, and its latest data time."""
    last = None
    while feed._thread.is_alive():
        feed._thread.join(every)
        now = feed._progress()
        if now is None or not feed._thread.is_alive():
            continue
        if last is None or last['trading_date'] != now['trading_date'] or now['updates'] < last['updates']:
            last = {**now, 'seconds': 0, 'updates': 0}
        rate = (now['updates'] - last['updates']) / max(now['seconds'] - last['seconds'], 1e-9)
        recorded = int(now['seconds'])
        clock = now['source_time_us'] // 1_000_000 if now['source_time_us'] is not None else None
        at = f', data at {clock // 3600:02d}:{clock // 60 % 60:02d}:{clock % 60:02d} JST' if clock is not None else ''
        date = now['trading_date']
        print(f'[brisk] {date[:4]}-{date[4:6]}-{date[6:]}: recording for {recorded // 3600}:{recorded // 60 % 60:02d}:{recorded % 60:02d}, '
              f'{now["updates"]:,} updates, {rate:,.0f} a second{at}', file=sys.stderr, flush=True)
        last = now


def _duration(seconds):
    seconds = int(seconds)
    hours, minutes, seconds = seconds // 3600, seconds // 60 % 60, seconds % 60
    return f'{hours}h{minutes:02d}m{seconds:02d}s' if hours else f'{minutes}m{seconds:02d}s' if minutes else f'{seconds}s'


def _of(done, total, unit=lambda n: f'{n:,}'):
    """`done of total`, and its percentage: only a total that is known has one."""
    return f'{unit(done)} of {unit(total)} ({done * 100 // total}%)' if total else unit(done)


def _mib(size):
    return f'{size / 2**20:,.1f} MiB'


class Saving:
    """What `brisk live` tells on stderr of one recording it saves: its site and session (`label`: a string, or a function
    giving it once there is something to say), the stage it is in (preparing, writing batches, quotes and trades,
    finalizing) with the rows done of their total, then where it is saved and how long it took, or why it is not. Its
    upload is told apart: the bytes sent, then the archive's checks. Each stage's first line and each result come at once,
    the rest at most every `every` seconds; with `every` None nothing is said. Telling never fails the save."""

    def __init__(self, label, every=None, out=None, clock=time.monotonic):
        self._label, self._every, self._out, self._clock = label, every, out, clock
        self._began, self._upload_began, self._last, self._said = clock(), None, None, set()
        self._rows, self._lock = {}, threading.Lock()

    def _say(self, text, stage=None, final=False, since=None):
        if self._every is None:
            return
        with self._lock:
            now = self._clock()
            first = stage not in self._said
            self._said.add(stage)
            if not (first or final or now - self._last >= self._every):
                return
            self._last = now
            try:
                if callable(self._label):
                    self._label = self._label()
            except Exception:  # noqa: BLE001 - a label that cannot be made must not lose the line
                self._label = 'recording'
            began = self._began if since is None else since
            ending = '' if final else f', {_duration(now - began)} elapsed'
            try:
                print(f'[brisk] {self._label}: {text}{ending}', file=self._out or sys.stderr, flush=True)
            except (OSError, ValueError):   # a stderr that is closed or full is not the recording's trouble
                pass

    def packaging(self, label):
        """brisk-parquet's `progress: label` lines (see _native.package)."""
        told = _native.parse_progress(label)
        if told is None:
            return
        table, done, total = told
        if done is None:
            self._say('preparing to save' if table == 'preparing' else 'finalizing', stage=table)
            return
        self._rows[table] = (done, total)
        if table == 'batches':
            self._say(f'writing batches {_of(done, total)}', stage='batches')
        else:
            tables = [f'{name} {_of(*self._rows[name])}' for name in ('quotes', 'trades') if name in self._rows]
            self._say('writing ' + ', '.join(tables), stage='tables')

    def stage(self, text):
        """A stage that has no rows to count (Python's own packaging)."""
        self._say(text, stage=text)

    def saved(self, path, hint=None):
        """The recording is on disk at `path`: only now is it saved."""
        self._say(f'saved to {path} in {_duration(self._clock() - self._began)}' + (f'; {hint}' if hint else ''), final=True)

    def empty(self, until):
        """The piece was empty: the stream sent no update until `until`, so there is nothing to save (what it held, the book
        and the day's history, the recording before it has)."""
        self._say(f'no update came until {until}; nothing to save', stage='empty', final=True)

    def keeping(self, where):
        """A save that failed is being kept in `where` instead: told afresh, timed from here."""
        self._began, self._last, self._said, self._rows = self._clock(), None, set(), {}
        self._say(f'keeping what was recorded in {where}', stage='keeping')

    def uploading(self, stage, done=None, total=None):
        """`upload`'s progress(stage, done, total): its own lines, timed from its own start."""
        if self._upload_began is None:
            self._upload_began = self._clock()
        since = self._upload_began
        if stage == 'uploading':
            self._say(f'uploading {_of(done, total, _mib)}' if total else 'uploading', stage='uploading', since=since)
        elif stage == 'validating':
            self._say('sent; waiting for the archive to check it', stage='validating', since=since)
        else:
            self._say('preparing to upload', stage='preparing upload', since=since)

    def uploaded(self):
        self._say(f'uploaded; the archive stored it ({_duration(self._clock() - (self._upload_began or self._began))})', final=True)

    def failed(self, step, error):
        self._say(f'{step} failed after {_duration(self._clock() - self._began)}: {error}', final=True)


def saving(label):
    """The report of a recording being saved: spoken while `brisk live` runs, else quiet."""
    return Saving(label, SAVE_EVERY)


def _telling(label):
    """The report of a command that packages or uploads (`brisk record`, `package`, `upload`): always spoken, on stderr."""
    return Saving(label, SAVE_SECONDS[sys.stderr.isatty()])


def _packaged(events, output):
    """`package(events, output)` as a command line says it: its stage, then `saved to` once it is. Returns the manifest and
    the report, which goes on to tell the upload."""
    save = _telling(output.resolve().name)
    save.stage('packaging')
    m = package(events, output)
    save.saved(output)
    return m, save


def brisk_sites():
    """The brokers' BRiSK sites briskapi has a data client for (briskapi.brisk.SITES)."""
    from briskapi import brisk  # brisk builds on this module
    return tuple(brisk.SITES)


def _interrupt(signum, frame):
    raise KeyboardInterrupt

def passkey_command(args):
    """`brisk sites|enroll|login|forget`: sign in to your broker's BRiSK with a passkey held in a Chrome virtual authenticator."""
    from briskapi import passkey, sites
    if args.command == 'sites':
        for site in sites.load_sites().values():
            client = 'data client: yes' if site.client else 'data client: no'
            print(f'{site.id:<10} {site.name:<24} {site.cookie_host:<22} {client:<16} {site.source}')
    elif args.command == 'enroll':
        print(json.dumps(passkey.enroll(site=args.site, login_url=args.login_url, chrome=args.chrome, profile_dir=args.profile_dir,
                                        replace=args.replace)))
    elif args.command == 'login':
        signin = passkey.login(remember=args.remember, login_url=args.login_url, launch_url=args.launch_url,
                               passkey_button=args.passkey_button, chrome=args.chrome, profile_dir=args.profile_dir,
                               headless=args.headless, site=args.site)
        saved = f'saved to {signin.saved_to}' if signin.saved_to else 'not saved'
        print(f'Signed in to {signin.site.name}; session cookies {saved}.', file=sys.stderr)
        if signin.client is None:
            print('briskapi has no data client for this site yet: the cookies are for your own use.', file=sys.stderr)
    else:
        site = passkey.forget(site=args.site)
        print(f'Passkey {"for " + site + " " if site else ""}deleted here. It stays registered at your broker until you remove '
              'it in its security settings.', file=sys.stderr)

def main(argv=None):
    try:
        _main(argv)
    except BriskError as error:
        sys.exit(f'brisk: error: {error}')


def _main(argv):
    from briskapi import __version__
    parser = argparse.ArgumentParser(prog='brisk', description=__doc__)
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    parser.add_argument('--config', type=Path, default=PACKAGE / 'archive.json')
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('record', 'package'):
        p = sub.add_parser(name, help='Record a demo replay' if name == 'record' else 'Package a recording')
        p.add_argument('--output', type=Path, required=True)
        p.add_argument('--upload', action=argparse.BooleanOptionalAction, default=None,
                       help='Upload to your private archive after packaging (default: yes once upload consent is saved)')
        if name == 'record':
            p.add_argument('--cache', type=Path, metavar='DIR',
                           help="Replay a copy of the demo's files downloaded before (default: download them)")
            p.add_argument('--codes')
            p.add_argument('--limit-frames', type=int)
            p.add_argument('--speed', type=float, default=1)
            p.add_argument('--binary', type=Path, help='Optional Rust collector (brisk_quote_ingest) for state validation and latency display')
        else:
            p.add_argument('--events', type=Path, required=True)
    p = sub.add_parser('live', help='Record the live feed day after day, and serve it to briskapi.Live()')
    p.add_argument('--cache', type=Path, metavar='DIR',
                   help="With --site demo: replay a copy of the demo's files downloaded before (default: download them)")
    p.add_argument('--codes', help='Comma-separated security codes (default: whole market)')
    p.add_argument('--speed', type=float, default=1)
    p.add_argument('--limit-frames', type=int)
    p.add_argument('--quotes', action='store_true',
                   help='Print each quote update as one JSON line (default: a line on stderr now and then on the recording)')
    p.add_argument('--raw', action='store_true', help='With --quotes: vendor fields (price10, microseconds) instead of yen/ISO times')
    p.add_argument('--gateway', action=argparse.BooleanOptionalAction, default=True,
                   help='Serve the feed to briskapi.Live() on this computer only (127.0.0.1, with a key in an owner-only '
                        'file; default: yes)')
    p.add_argument('--output', type=Path, metavar='DIR',
                   help='Keep each recording here as SOURCE_YYYYMMDD_FROM-TO, a broker\'s SITE_YYYYMMDD_FROM-TO (its first and last clock, JST HHMMSS); a broker\'s '
                        'day comes a few minutes at a time')
    p.add_argument('--site', choices=('demo', *brisk_sites()), required=True,
                   help="demo: the public BRiSK Next demo, a 2021 recording replayed here; sbi: SBI Securities' live BRiSK, "
                        'cookies from BRISK_SBI_COOKIES, saved with briskapi.login(remember=True) or by `brisk login`; '
                        "e-shiten (Tachibana e-shiten's BRiSK Next), matsui, monex, smbcnikko: the same with their own "
                        "variable (BRISK_ESHITEN_COOKIES, BRISK_MATSUI_COOKIES, ...) and briskapi.login('matsui', remember=True); "
                        "Matsui's, Monex's and SMBC Nikko's not yet tried live")
    p.add_argument('--fast-symbols', type=int, metavar='N',
                   help="With a broker's site: how many symbols Live sessions will want on the fast lane (default: 500 to 1,000, by "
                        'the cores); each 500 takes a worker from the recording, so also set BRISK_WORKERS for a system of '
                        'a thousand or more')
    p.add_argument('--trace-protocol', action='store_true',
                   help="With a broker's site: print the connection steps to stderr, every token redacted (wire details in BRISK_PROFILE)")
    p.add_argument('--upload', action=argparse.BooleanOptionalAction, default=None,
                   help='Upload this session to your private archive (default: yes once you agreed); --no-upload keeps all of it local, timing summary included')
    p.add_argument('--upload-market-data', action=argparse.BooleanOptionalAction, default=None,
                   help="With a broker's site: upload the decoded market recording too (default: yes unless you opt out); --no-upload-market-data keeps it local")
    sub.add_parser('sites', help='List the brokers you can sign in to (built in, plus your own in sites.json)')
    for name, text in (('enroll', 'Once: pick your broker, sign in by hand and register a passkey; it is saved to the Keychain'),
                       ('login', 'Sign in with the saved passkey in Chrome and keep the BRiSK session cookies')):
        p = sub.add_parser(name, help=text)
        p.add_argument('--login-url', help='Where to start (default: the chosen broker\'s login page)')
        p.add_argument('--chrome', type=Path, help='Chrome or Chromium executable (default: found automatically, or BRISK_CHROME)')
        p.add_argument('--profile-dir', type=Path, help='Keep Chrome\'s profile here instead of a temporary one deleted afterwards')
        if name == 'enroll':
            p.add_argument('--site', help='Your broker (see `brisk sites`); asked interactively if omitted')
            p.add_argument('--replace', action='store_true', help="Enroll this broker's passkey again, replacing the saved one")
        else:
            p.add_argument('--site', help='Which broker, when passkeys are saved for more than one (see `brisk sites`)')
            p.add_argument('--launch-url', help='Open this BRiSK address after signing in (otherwise open BRiSK from the broker\'s site yourself)')
            p.add_argument('--passkey-button', help='Text of the passkey sign-in control on the login page')
            p.add_argument('--headless', action='store_true', help='No Chrome window: needs --launch-url and a login page that works unattended')
            p.add_argument('--remember', action=argparse.BooleanOptionalAction, default=True,
                           help='Save the cookies (default: yes; owner-only file); `brisk live --site` with that broker reads them')
    p = sub.add_parser('forget', help='Delete a saved passkey (it stays registered at your broker until you remove it there)')
    p.add_argument('--site', help='Which broker, when passkeys are saved for more than one')
    p = sub.add_parser('upload', help='Upload a prepared package to your private archive'); p.add_argument('directory', type=Path)
    p = sub.add_parser('list', help='List your archived recordings (or timing reports)')
    p.add_argument('--date'); p.add_argument('--source', choices=sorted(SOURCES))
    p.add_argument('--timing', action='store_true', help='Your timing reports instead')
    p.add_argument('--deleted', action='store_true', help='What you deleted and can still restore (30 days)')
    p = sub.add_parser('pull', help='Download and verify one of your recordings'); p.add_argument('prefix'); p.add_argument('--output', type=Path, required=True)
    p = sub.add_parser('delete', help='Remove one of your recordings (or timing reports) from your archive; restorable for 30 days')
    p.add_argument('prefix')
    p = sub.add_parser('restore', help='Bring back a recording (or timing report) you deleted in the last 30 days')
    p.add_argument('prefix')
    p = sub.add_parser('query', help='Query your archive in the cloud, Databento-style (XTKS.BRISK, or XTKS.BRISKDEMO for anyone)')
    p.add_argument('dataset', choices=['XTKS.BRISK', 'XTKS.BRISKDEMO'])
    p.add_argument('--schema', default='trades',
                   help='trades, mbp-10, book-deltas, definition, imbalance, brisk-quotes, ohlcv-1s/1m/1h/1d')
    p.add_argument('--snapshot', action='store_true', help="book-deltas: each security's whole books as of --start first")
    p.add_argument('--symbols', default='ALL_SYMBOLS', help='Comma-separated JPX codes')
    p.add_argument('--start', required=True, help='ISO 8601 (JST unless it says otherwise) or a trading date')
    p.add_argument('--end', help='Exclusive; default: the end of the start\'s trading day')
    p.add_argument('--limit', type=int)
    p.add_argument('--encoding', choices=['csv', 'json', 'parquet'], default='csv')
    p.add_argument('--output', type=Path, help='Write here instead of stdout (needed for parquet)')
    p = sub.add_parser('prune', help='Free disk space: remove local packages your archive holds (brisk pull brings them back)')
    p.add_argument('directories', type=Path, nargs='+')
    p = sub.add_parser('identity', help='Show your archive identity, its recovery code, or restore it on this computer')
    group = p.add_mutually_exclusive_group()
    group.add_argument('--recovery-code', action='store_true', help='Print the recovery code (whoever holds it is you to the archive)')
    group.add_argument('--export', action='store_true', help='Print the identity secret itself')
    group.add_argument('--recover', metavar='CODE', help='Use this recovery code (or secret) here (- reads it from stdin)')
    p.add_argument('--replace', action='store_true', help='With --recover: replace a different identity already here')
    p = sub.add_parser('export', help='Write a recording as a NautilusTrader v2 catalog or as JSONL')
    p.add_argument('recording', type=Path)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--nautilus', type=Path, metavar='DIR', help='Catalog directory (data/<type>/<CODE>.XTKS/...)')
    group.add_argument('--jsonl', type=Path, metavar='FILE', help='Canonical decoded batches, one per line')
    p = sub.add_parser('repair', help="Fill recordings' gaps from other feeds' recordings of the same day (or just compare them)")
    p.add_argument('recordings', nargs='+', type=Path, metavar='RECORDING',
                   help='Recordings to repair, or folders of them (such as a brisk live --output folder): they are never changed')
    p.add_argument('--from', dest='others', nargs='+', type=Path, required=True, metavar='OTHER',
                   help="Another feed's recordings of the same day (another broker's, or another computer's), or folders of them")
    p.add_argument('--output', type=Path, help='Write what is repaired here, packaged for `brisk upload` (one recording: this folder; '
                                               'several: a folder of them); without it, only report')
    p.add_argument('--until', metavar='TIME',
                   help="For one recording that nothing follows (a feed that stopped early): go past its end to this time (HH:MM:SS, "
                        "Tokyo's). Where another recording follows, a repair already goes up to where it starts, and no further")
    p.add_argument('--window', type=float, default=5.0, metavar='MINUTES', help='Length of the blocks the report counts (default: 5)')
    p = sub.add_parser('consent', help='Show or change automatic uploads to your private archive')
    group = p.add_mutually_exclusive_group()
    group.add_argument('--accept', action='store_true')
    group.add_argument('--revoke', action='store_true')
    args = parser.parse_args(argv)
    if args.command == 'live' and args.upload_market_data and args.site == 'demo':
        parser.error('--upload-market-data is for a broker: --site with one other than demo')
    if args.command == 'live' and args.cache and args.site != 'demo':
        parser.error('--cache is for the demo: --site demo')
    if args.command == 'live' and args.raw and not args.quotes:
        parser.error('--raw is for the quotes printed with --quotes')
    config = settings(args.config)
    if args.command == 'consent':
        if args.accept:
            sys.stderr.write(NOTICE)
            choice = accept()
        else:
            choice = save_consent(False) if args.revoke else load_consent() or DEFAULT
            if not args.revoke and choice is DEFAULT:
                sys.stderr.write(policy_changed(lapsed_consent()) + '\n' * bool(lapsed_consent()))
        print(json.dumps(choice))
    elif args.command in {'record', 'package'}:
        partial = args.command == 'record' and args.limit_frames is not None
        require(not (partial and args.upload), 'Only complete replays can be uploaded; omit --limit-frames')
        uploading = False if partial else declaration(args)
        require(uploading or not args.upload, f'Uploading needs `{parser.prog} consent --accept`')
        if args.command == 'record':
            with tempfile.TemporaryDirectory() as tmp:
                events = Path(tmp) / 'events.jsonl'
                record_events(events, args.cache, args.codes, args.limit_frames, args.speed, args.binary)
                if not uploading:
                    save_recording(events, args.output)
                    why = 'partial replays stay local' if partial else f'uploads are off (`{parser.prog} consent`)'
                    print(f'Saved {args.output}; not uploaded: {why}.', file=sys.stderr)
                    return
                m, save = _packaged(events, args.output)
        else:
            m, save = _packaged(args.events, args.output)
        print(json.dumps(m))
        if uploading and args.upload is not False and os.environ.get('BRISK_UPLOAD') != '0':
            stored = upload(args.output, config['api_url'], verify=False, progress=save.uploading)
            save.uploaded()
            print(json.dumps(stored))
    elif args.command == 'live':
        live(args)
    elif args.command in {'sites', 'enroll', 'login', 'forget'}:
        passkey_command(args)
    elif args.command == 'upload':
        save = _telling(args.directory.resolve().name)
        stored = upload(args.directory, config['api_url'], progress=save.uploading)
        save.uploaded()
        print(json.dumps(stored))
    elif args.command == 'list' and args.deleted:
        for item in deleted(config['api_url'], args.date):
            print(json.dumps(item))
    elif args.command == 'list' and args.timing:
        for key, report in timing_reports(config['api_url'], args.date):
            print(json.dumps({'key': key, **report}))
    elif args.command == 'list':
        for prefix, m in manifests(config['api_url'], args.date):
            if args.source is None or m['summary']['source'] == args.source:
                print(json.dumps({'prefix': prefix, **m}))
    elif args.command == 'pull':
        print(json.dumps(pull(config['api_url'], args.prefix, args.output)))
    elif args.command == 'delete':
        print(json.dumps(delete(config['api_url'], args.prefix)))
    elif args.command == 'restore':
        print(json.dumps(restore(config['api_url'], args.prefix)))
    elif args.command == 'query':
        require(args.output or args.encoding != 'parquet', 'Parquet needs --output FILE')
        from briskapi._historical import Historical
        body, _ = Historical(gateway=config.get('query_url'))._call('timeseries.get_range', dict(
            dataset=args.dataset, schema=args.schema, symbols=args.symbols, start=args.start, end=args.end,
            limit=args.limit, encoding=args.encoding, **({'snapshot': True} if args.snapshot else {})), post=True)
        if args.output:
            args.output.write_bytes(body)
        else:
            sys.stdout.write(body.decode())
    elif args.command == 'prune':
        for directory in args.directories:
            print(json.dumps(prune(config['api_url'], directory)))
    elif args.command == 'identity':
        if args.recover:
            code = sys.stdin.readline() if args.recover == '-' else args.recover
            identity.save(identity.from_recovery_code(code), replace=args.replace)
        value, created = identity.ensure()
        if args.export or args.recovery_code:
            print(value if args.export else identity.recovery_code(value))
            return
        if created:
            show_identity(value, created)
        print(json.dumps({'owner': owner(value), 'kept_in': identity.where()}))
    elif args.command == 'export':
        print(json.dumps(export(args.recording, args.nautilus, args.jsonl)))
    elif args.command == 'repair':
        print(json.dumps(repair(args.recordings, args.others, args.output, args.until, args.window), indent=2))

if __name__ == '__main__':
    main()
