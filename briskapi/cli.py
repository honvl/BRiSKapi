#!/usr/bin/env python3
"""Consume BRiSK auction data live, record and automatically contribute it, and use the shared archive."""
import argparse
import datetime as dt
import gzip
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
import warnings

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from briskapi._recording import BriskError
from briskapi.schema import (SCHEMA, MAX_TIMING_BYTES, canonical_lines, compress, digest, inspect_package, validate_manifest,
                             validate_stream, validate_timing, require)

PACKAGE = Path(__file__).resolve().parent
# BRiSK's own WASM decoder runs under Node; the package ships only this host and SHA-256 pins.
DECODER = PACKAGE / 'decoder' / 'decoder.cjs'
MIN_NODE = 22
LICENSES = ('CC0-1.0', 'CC-BY-4.0')
# Bump with any PRIVACY.md change to what is collected; saved choices then lapse.
POLICY_VERSION = 3
NOTICE = '''\
Sessions are contributed to the shared public BRiSK archive automatically.
After each clean, complete demo replay the contribution contains:
  - the decoded market data you recorded (checked against the reference replay);
  - local timing measurements: decode durations, replay lateness, asset download
    time and your computer's receipt clock, which shows when you recorded;
  - your public alias and data license, in the published manifest.
SBI BRiSK sessions contribute a timing summary: decode time, data age and frame
spacing percentiles, stalls, frame count, date and start/end minute. At the start
of each interactive SBI capture, a separate question asks whether to publish its
decoded market recording too; Enter accepts for that session only. Scripts can
opt in with --share-market-data or sbi.connect(share_market_data=True). This
publishes its securities master, prices, quantities, codes and per-frame timing.
Cookies, tokens and connection diagnostics are never published.
Contributions are public and permanent. Your IP address is used only for
upload rate limiting. No account, file, hostname or system details are sent.
Accepting declares that you may redistribute these recordings under that license.
Policy: https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md
Opt out at any time: `brisk consent --revoke` or BRISK_CONTRIBUTE=0.
'''

def check_node(node='node'):
    """Fail early, with the fix, when Node is missing or older than the decoder hosts support."""
    advice = f'Install Node.js {MIN_NODE} or newer from https://nodejs.org, or pass the path to it.'
    try:
        out = subprocess.run([node, '--version'], capture_output=True, text=True, timeout=30).stdout
    except FileNotFoundError:
        raise BriskError(f'Node.js was not found (looked for {str(node)!r}). BRiSK\'s decoder runs under Node. {advice}') from None
    except (OSError, subprocess.SubprocessError) as error:
        raise BriskError(f'Could not run {str(node)!r} --version: {error}. {advice}') from error
    found = re.match(r'v(\d+)\.', out.strip())
    if found and int(found[1]) < MIN_NODE:
        raise BriskError(f'Node.js {out.strip()} is too old; the decoder hosts need Node {MIN_NODE}+. {advice}')


def settings(path=None):
    return json.loads((path or PACKAGE / 'archive.json').read_text())

def consent_path():
    return Path(os.environ.get('XDG_CONFIG_HOME') or Path.home() / '.config') / 'brisk' / 'contribution.json'

def load_consent():
    """The saved contribution choice under the current policy, or None if undecided."""
    try:
        choice = json.loads(consent_path().read_text())
    except (OSError, ValueError):
        return None
    return choice if isinstance(choice, dict) and choice.get('policy_version') == POLICY_VERSION else None

def save_consent(enabled, contributor=None, license=None):
    choice = dict(policy_version=POLICY_VERSION, enabled=bool(enabled),
                  decided_at=dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'))
    if enabled:
        choice.update(contributor=contributor or f'anon-{secrets.token_hex(4)}', license=license or 'CC0-1.0')
        require(re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', choice['contributor']), 'Alias: 1-64 letters, digits, _ . -')
        require(choice['license'] in LICENSES, 'Unsupported data license')
    path = consent_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(choice, indent=2) + '\n')
    return choice

def interactive():
    return sys.stdin.isatty() and sys.stderr.isatty()

def ask_consent():
    """One-time first-run question; Enter accepts. Prompts go to stderr, never stdout."""
    alias = f'anon-{secrets.token_hex(4)}'
    sys.stderr.write(NOTICE + f"Contribute automatically as '{alias}' under CC0-1.0? [Y/n] ")
    sys.stderr.flush()
    accepted = sys.stdin.readline().strip().lower() in {'', 'y', 'yes'}
    return save_consent(accepted, alias)

def ask_sbi_market_sharing(choice):
    """An explicit choice at the beginning of each SBI capture; never saved."""
    sys.stderr.write('Share this SBI capture publicly? This publishes the securities master, codes, prices, '
                     'quantities and local per-frame timing, after a clean session end. Cookies, tokens and '
                     'connection diagnostics are excluded. '
                     f"Publish as '{choice['contributor']}' under {choice['license']}? [Y/n] ")
    sys.stderr.flush()
    answer = sys.stdin.readline()
    return bool(answer) and answer.strip().lower() in {'', 'y', 'yes'}

def declaration(args):
    """Alias and license for this run, or None to keep the recording local."""
    if args.contributor or args.license or args.redistribution_permitted:
        require(args.contributor and args.license and args.redistribution_permitted,
                'Use --contributor, --license and --redistribution-permitted together')
        return dict(contributor=args.contributor, license=args.license)
    choice = load_consent()
    if choice is None and args.upload is not False and os.environ.get('BRISK_CONTRIBUTE') != '0' and interactive():
        choice = ask_consent()
    return {k: choice[k] for k in ('contributor', 'license')} if choice and choice['enabled'] else None

def package(events, output, contributor, license):
    output.mkdir(parents=True, exist_ok=True)
    target = output / 'events.jsonl.gz'
    require(not target.exists(), 'Package output already exists')
    try:
        with events.open('rb') as source:
            compress(canonical_lines(source), target)
        with gzip.open(target, 'rb') as stream:
            summary = validate_stream(stream)
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    m = validate_manifest(dict(schema=SCHEMA, sha256=digest(target), bytes=target.stat().st_size, summary=summary,
                               contributor=contributor, license=license, redistribution_permitted=True))
    (output / 'manifest.json').write_text(json.dumps(m, indent=2) + '\n')
    return m

def request_json(url, data=None):
    request = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode(),
                                     headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)

def contribute(directory, api_url, timeout=660, verify=True, out=None):
    m = json.loads((directory / 'manifest.json').read_text())
    path = directory / 'events.jsonl.gz'
    if verify:
        inspect_package(path, m)
    else:
        require(path.stat().st_size == m['bytes'] and digest(path) == m['sha256'], 'Size/hash mismatch')
    ticket = request_json(api_url, m)
    # S3 browser POST policies bind key, encryption, content type and exact size.
    # The archive's 64 MiB cap bounds the multipart body below 65 MiB.
    boundary = uuid.uuid4().hex
    parts = []
    for key, value in ticket['upload']['fields'].items():
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode())
    parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="events.jsonl.gz"\r\nContent-Type: application/gzip\r\n\r\n'.encode())
    parts.extend([path.read_bytes(), f'\r\n--{boundary}--\r\n'.encode()])
    request = urllib.request.Request(ticket['upload']['url'], data=b''.join(parts),
             headers={'Content-Type': f'multipart/form-data; boundary={boundary}'}, method='POST')
    with urllib.request.urlopen(request, timeout=120) as response:
        response.read()
    status_url = api_url.rstrip('/') + '/?ticket=' + ticket['ticket']
    print(json.dumps({'ticket': ticket['ticket'], 'status_url': status_url}), file=out, flush=True)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = request_json(status_url)
        if status['status'] == 'published':
            return status
        require(status['status'] != 'rejected', 'Automatic validation rejected recording: ' + status.get('reason', 'unknown'))
        time.sleep(3)
    raise TimeoutError(f'Publication still pending; check {status_url}')

def client(config):
    return boto3.client('s3', region_name=config['region'], config=Config(signature_version=UNSIGNED))

def contribute_timing(report, api_url):
    """Publish a timing-only report (no market data); returns the service's response."""
    return request_json(api_url, {'timing': report})

def timing_reports(s3, bucket, date=None):
    """Published timing reports, newest dates last."""
    prefix = 'timing/'
    if date:
        require(len(date) == 8 and date.isdigit(), 'Date must be YYYYMMDD')
        prefix += date + '/'
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get('Contents', []):
            body = s3.get_object(Bucket=bucket, Key=item['Key'])['Body']
            with body:
                yield item['Key'], validate_timing(json.loads(body.read(MAX_TIMING_BYTES + 1)))

def manifests(s3, bucket, date=None):
    prefix = 'archive/'
    if date:
        require(len(date) == 8 and date.isdigit(), 'Date must be YYYYMMDD')
        prefix += date + '/'
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get('Contents', []):
            if item['Key'].endswith('/manifest.json'):
                entry_prefix = item['Key'].rsplit('/', 1)[0]
                try:
                    m = _read_manifest(s3, bucket, entry_prefix)
                except (ValueError, TypeError, KeyError, RecursionError):
                    warnings.warn(f'Skipping invalid or unsupported archive manifest: {item["Key"]}', stacklevel=2)
                    continue
                yield entry_prefix, m

def _read_manifest(s3, bucket, prefix):
    obj = s3.get_object(Bucket=bucket, Key=f'{prefix}/manifest.json')
    with obj['Body'] as body:
        require(obj['ContentLength'] <= 65536, 'Remote manifest exceeds limit')
        data = body.read(65537)
        require(len(data) <= 65536, 'Remote manifest exceeds limit')
    m = validate_manifest(json.loads(data))
    require(prefix == f"archive/{m['summary']['trading_date']}/{m['sha256']}", 'Archive identity mismatch')
    return m

def pull(s3, bucket, prefix, output):
    require(re.fullmatch(r'archive/\d{8}/[0-9a-f]{64}', prefix), 'Invalid archive prefix')
    require(not output.exists(), 'Download output already exists')
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent) as tmp:
        directory = Path(tmp)
        # Refuse unsupported sources and identities before downloading payloads.
        m = _read_manifest(s3, bucket, prefix)
        (directory / 'manifest.json').write_text(json.dumps(m, indent=2) + '\n')
        path = directory / 'events.jsonl.gz'
        obj = s3.get_object(Bucket=bucket, Key=f'{prefix}/events.jsonl.gz')
        limit = 64 * 1024**2
        with obj['Body'] as body, path.open('wb') as target:
            require(obj['ContentLength'] <= limit, 'Remote object exceeds limit')
            size = 0
            while chunk := body.read(1024 * 1024):
                size += len(chunk)
                require(size <= limit, 'Remote object exceeds limit')
                target.write(chunk)
        inspect_package(path, m)
        with gzip.open(path, 'rb') as source, (directory / 'events.jsonl').open('wb') as target:
            shutil.copyfileobj(source, target)
        shutil.move(str(directory), str(output))
    return m

def record_events(events, web=False, cache=None, codes=None, limit_frames=None, speed=1, binary=None, node='node'):
    """Replay the pinned demo and save its decoded batches.

    Needs only Node. A Rust collector binary (`brisk_quote_ingest`) is optional; it
    records the same batches and adds its own state validation and latency display.
    """
    check_node(node)
    options = ['--web'] if web else ['--cache', str(cache)]
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
        subprocess.run([node, str(DECODER), *options], check=True, stdout=out)

def live(args):
    """Print each quote update as one JSON line; a complete session is contributed per consent."""
    from briskapi import connect, sbi  # The API package builds on this module.
    codes = args.codes.split(',') if args.codes else None
    if args.sbi:
        sbi.login()  # Missing cookies fail before consent prompts or decoder startup.
    if load_consent() is None and os.environ.get('BRISK_CONTRIBUTE') != '0' and interactive():
        ask_consent()
    share_market_data = args.share_market_data
    choice = load_consent()
    if (args.sbi and share_market_data is None and choice and choice['enabled']
            and os.environ.get('BRISK_CONTRIBUTE') != '0' and interactive()):
        share_market_data = ask_sbi_market_sharing(choice)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        if args.sbi:
            feed = sbi.connect(codes=codes, trace_protocol=args.trace_protocol, share_market_data=bool(share_market_data))
        else:
            feed = connect(web=args.web, cache=args.cache, codes=codes, speed=args.speed, limit_frames=args.limit_frames)
    for warning in caught:
        print(warning.message, file=sys.stderr)
    try:
        for quote in feed.quotes(raw=args.raw):
            print(json.dumps(quote, ensure_ascii=False, default=lambda value: value.isoformat()), flush=True)
        feed.wait()
    finally:
        feed.close()
    if feed.contribution:
        print(json.dumps({'contribution': feed.contribution}), file=sys.stderr)
    if feed.timing_contribution:
        print(json.dumps({'timing_contribution': feed.timing_contribution}), file=sys.stderr)

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
                               headless=args.headless)
        saved = f'saved to {signin.saved_to}' if signin.saved_to else 'not saved'
        print(f'Signed in to {signin.site.name}; session cookies {saved}.', file=sys.stderr)
        if signin.client is None:
            print('briskapi has no data client for this site yet: the cookies are for your own use.', file=sys.stderr)
    else:
        passkey.forget()
        print('Passkey deleted here. It stays registered at your broker until you remove it in its security settings.',
              file=sys.stderr)

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
        p.add_argument('--contributor', help='Public alias (defaults to the saved consent choice)')
        p.add_argument('--license', choices=LICENSES)
        p.add_argument('--redistribution-permitted', action='store_true',
                       help='Declare permission to redistribute this recording under the selected data license')
        p.add_argument('--upload', action=argparse.BooleanOptionalAction, default=None,
                       help='Contribute after packaging (default: yes once contribution consent is saved)')
        if name == 'record':
            group = p.add_mutually_exclusive_group(required=True)
            group.add_argument('--web', action='store_true')
            group.add_argument('--cache', type=Path)
            p.add_argument('--codes')
            p.add_argument('--limit-frames', type=int)
            p.add_argument('--speed', type=float, default=1)
            p.add_argument('--binary', type=Path, help='Optional Rust collector (brisk_quote_ingest) for state validation and latency display')
        else:
            p.add_argument('--events', type=Path, required=True)
    p = sub.add_parser('live', help='Stream live quote updates as JSON lines')
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--web', action='store_true')
    group.add_argument('--cache', type=Path)
    p.add_argument('--codes', help='Comma-separated security codes (default: whole market)')
    p.add_argument('--speed', type=float, default=1)
    p.add_argument('--limit-frames', type=int)
    p.add_argument('--raw', action='store_true', help='Vendor fields (price10, microseconds) instead of yen/ISO times')
    group.add_argument('--sbi', action='store_true',
                       help='Live SBI BRiSK (experimental); cookies from BRISK_SBI_COOKIES, saved with sbi.login(remember=True) or by `brisk login`')
    p.add_argument('--trace-protocol', action='store_true',
                   help='With --sbi: print the connection steps to stderr, every token redacted (wire details in BRISK_SBI_PROFILE)')
    p.add_argument('--share-market-data', action=argparse.BooleanOptionalAction, default=None,
                   help='With --sbi and consent: share this decoded market session (interactive default: ask, Enter accepts)')
    sub.add_parser('sites', help='List the brokers you can sign in to (built in, plus your own in sites.json)')
    for name, text in (('enroll', 'Once: pick your broker, sign in by hand and register a passkey; it is saved to the Keychain'),
                       ('login', 'Sign in with the saved passkey in Chrome and keep the BRiSK session cookies')):
        p = sub.add_parser(name, help=text)
        p.add_argument('--login-url', help='Where to start (default: the chosen broker\'s login page)')
        p.add_argument('--chrome', type=Path, help='Chrome or Chromium executable (default: found automatically, or BRISK_CHROME)')
        p.add_argument('--profile-dir', type=Path, help='Keep Chrome\'s profile here instead of a temporary one deleted afterwards')
        if name == 'enroll':
            p.add_argument('--site', help='Your broker (see `brisk sites`); asked interactively if omitted')
            p.add_argument('--replace', action='store_true', help='Replace the passkey already saved')
        else:
            p.add_argument('--launch-url', help='Open this BRiSK address after signing in (otherwise open BRiSK from the broker\'s site yourself)')
            p.add_argument('--passkey-button', help='Text of the passkey sign-in control on the login page')
            p.add_argument('--headless', action='store_true', help='No Chrome window: needs --launch-url and a login page that works unattended')
            p.add_argument('--remember', action=argparse.BooleanOptionalAction, default=True,
                           help='Save the cookies (default: yes; owner-only file); SBI\'s feed `brisk live --sbi` reads them')
    sub.add_parser('forget', help='Delete the saved passkey (it stays registered at your broker until you remove it there)')
    p = sub.add_parser('upload', help='Contribute a prepared package'); p.add_argument('directory', type=Path)
    p = sub.add_parser('list', help='List published recordings'); p.add_argument('--date'); p.add_argument('--source', choices=['historical_mock','synthetic_test','sbi_live'])
    p = sub.add_parser('pull', help='Download and verify a recording'); p.add_argument('prefix'); p.add_argument('--output', type=Path, required=True)
    p = sub.add_parser('consent', help='Show or change automatic contribution')
    group = p.add_mutually_exclusive_group()
    group.add_argument('--accept', action='store_true')
    group.add_argument('--revoke', action='store_true')
    p.add_argument('--contributor'); p.add_argument('--license', choices=LICENSES)
    args = parser.parse_args(argv)
    if args.command == 'live' and args.share_market_data and not args.sbi:
        parser.error('--share-market-data requires --sbi')
    config = settings(args.config)
    if args.command == 'consent':
        if args.accept:
            sys.stderr.write(NOTICE)
            choice = save_consent(True, args.contributor, args.license)
        else:
            choice = save_consent(False) if args.revoke else load_consent() or {'policy_version': POLICY_VERSION, 'enabled': None}
        print(json.dumps(choice))
    elif args.command in {'record', 'package'}:
        partial = args.command == 'record' and args.limit_frames is not None
        require(not (partial and args.upload), 'Only complete replays can be contributed; omit --limit-frames')
        found = None if partial else declaration(args)
        require(found or not args.upload, f'Uploading needs `{parser.prog} consent --accept` or --contributor/--license/--redistribution-permitted')
        if args.command == 'record':
            with tempfile.TemporaryDirectory() as tmp:
                events = Path(tmp) / 'events.jsonl'
                record_events(events, args.web, args.cache, args.codes, args.limit_frames, args.speed, args.binary)
                if found is None:
                    args.output.mkdir(parents=True, exist_ok=True)
                    require(not (args.output / 'events.jsonl').exists(), 'Recording output already exists')
                    shutil.move(str(events), str(args.output / 'events.jsonl'))
                    why = 'partial replays stay local' if partial else f'contribution is off (`{parser.prog} consent`)'
                    print(f'Saved {args.output / "events.jsonl"}; not contributed: {why}.', file=sys.stderr)
                    return
                m = package(events, args.output, **found)
        else:
            require(found, f'Packaging needs `{parser.prog} consent --accept` or --contributor/--license/--redistribution-permitted')
            m = package(args.events, args.output, **found)
        print(json.dumps(m))
        if args.upload is not False and os.environ.get('BRISK_CONTRIBUTE') != '0':
            print(json.dumps(contribute(args.output, config['api_url'], verify=False)))
    elif args.command == 'live':
        live(args)
    elif args.command in {'sites', 'enroll', 'login', 'forget'}:
        passkey_command(args)
    elif args.command == 'upload':
        print(json.dumps(contribute(args.directory, config['api_url'])))
    elif args.command == 'list':
        for prefix, m in manifests(client(config), config['bucket'], args.date):
            if args.source is None or m['summary']['source'] == args.source:
                print(json.dumps({'prefix': prefix, **m}))
    elif args.command == 'pull':
        print(json.dumps(pull(client(config), config['bucket'], args.prefix, args.output)))

if __name__ == '__main__':
    main()
