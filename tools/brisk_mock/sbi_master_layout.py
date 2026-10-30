"""Check briskapi on SBI's decoder of March 2026, offline and without an account: the 18000 master record
layout, the session schedule and the catch-up.

Downloads pinned copies from pybrisk's research (https://github.com/obichan117/pybrisk): SBI's decoder
and, from pybrisk's capture of sbi.brisk.jp, only the master file it served, the boot's schedule_info,
and the catch-up request's issue lists with the reply. The capture also holds pybrisk's own (expired)
session, so those are taken out of it in memory and nothing else is kept. It also downloads the public
archive's copy of pybrisk's recording of SBI's market stream that day (the `sample_data` wire capture),
pinned by its hash. None of this belongs in Git: the cache is yours. Then runs briskapi's Decoder on them under Node's permission model (reads only, no
child processes) through sbi_master_layout.cjs.

    python tools/brisk_mock/sbi_master_layout.py --cache /tmp/sbi-master-cache
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
PYBRISK = 'https://raw.githubusercontent.com/obichan117/pybrisk/ca4c77d1553dfd4debdad3de41dbe77ef66dc104/research/'
CAPTURE = (PYBRISK + 'sbi.brisk.jp.har', '59cdf8088b7c0eeac0902d88dbb03f1657fb997667aa2c72ad7df92e0f2adc74')
# The archive's legacy wire capture: pybrisk's 30,505 binary messages of SBI's stream, 2026-03-11
# (content addressed; the same bytes as pybrisk's per-message files).
STREAM = ('https://brisk-recordings-honvl-tokyo.s3.ap-northeast-1.amazonaws.com/samples/20260311/'
          'cca870a51e7f96f16009c3709c3f597d314b22ed8922bb3ea7fa1acc67a0f85f/events.jsonl.gz',
          'cca870a51e7f96f16009c3709c3f597d314b22ed8922bb3ea7fa1acc67a0f85f')
DECODER = {
    'fita.wasm': (PYBRISK + 'ws_frames/fita.wasm', '5ee4f6010b825924da53aeb878616fc04370ff6ae875a031f82128c3ddcd6572'),
    'fita.js': (PYBRISK + 'ws_frames/fita_wasm.js', '7e0bec622c2526bbd7c2ee91e3e07452be04effcf0264a67aef5712ae6748b24'),
}
# sbi.brisk.jp names a master by its SHA-256: /api/master/<hash>.
MASTER = 'f7b029c68758670afe9ac226db629fceab7e56076dcbe703e6d6891234945a72'
# The binary reply to the capture's catch-up (POST /api/stocks_update/<series>).
CATCH_UP = '9579d467868fe32345c1f4f1fb918f76cc9f1cb9f4c49133b1e7d257fe67da53'
SCHEDULE_KEYS = ('morning_session_pre_open_time', 'morning_session_open_time', 'morning_session_close_time',
                 'afternoon_session_pre_open_time', 'afternoon_session_open_time', 'afternoon_session_pre_close_time',
                 'afternoon_session_close_time', 'sq_jump_interval')
LISTS = ('issue_ids', 'update_numbers_from', 'update_numbers_to')


def fetch(url: str, sha256: str) -> bytes:
    with urlopen(url, timeout=120) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != sha256:
        raise ValueError(f'{url} changed; re-audit before changing the pin')
    return data


def master_from_capture(capture: bytes) -> bytes:
    for entry in json.loads(capture)['log']['entries']:
        if entry['request']['url'].split('?')[0].endswith('/api/master/' + MASTER):
            data = base64.b64decode(entry['response']['content'].get('text') or '')
            if hashlib.sha256(data).hexdigest() == MASTER:
                return data
    raise ValueError('The capture holds no master file with the pinned hash')


def _entries(capture: bytes, path: str):
    for entry in json.loads(capture)['log']['entries']:
        if path in entry['request']['url'].split('?')[0]:
            yield entry


def schedule_from_capture(capture: bytes) -> bytes:
    """Only schedule_info from the app boot (the rest of it names the session)."""
    for entry in _entries(capture, '/api/app/boot'):
        schedule = json.loads(entry['response']['content'].get('text') or '{}').get('schedule_info')
        if isinstance(schedule, dict) and all(isinstance(schedule.get(key), (int, float)) for key in SCHEDULE_KEYS):
            return json.dumps({key: schedule[key] for key in SCHEDULE_KEYS}).encode()
    raise ValueError('The capture holds no app boot with a complete schedule_info')


def catch_up_from_capture(capture: bytes) -> tuple[bytes, bytes]:
    """The catch-up request's three issue lists and its pinned binary reply."""
    for entry in _entries(capture, '/api/stocks_update/'):
        content = entry['response']['content']
        reply = base64.b64decode(content.get('text') or '') if content.get('encoding') == 'base64' else b''
        if hashlib.sha256(reply).hexdigest() != CATCH_UP:
            continue
        sent = json.loads((entry['request'].get('postData') or {}).get('text') or '{}')
        if not all(isinstance(sent.get(key), list) and len(sent[key]) == len(sent['issue_ids']) for key in LISTS):
            raise ValueError('The capture\'s catch-up request is not three issue lists of one length')
        return json.dumps({key: sent[key] for key in LISTS}).encode(), reply
    raise ValueError('The capture holds no catch-up reply with the pinned hash')


def download(cache: Path) -> None:
    cache.mkdir(parents=True, exist_ok=True)
    capture = None

    def from_capture() -> bytes:
        nonlocal capture
        capture = capture or fetch(*CAPTURE)
        return capture

    # Files with a pin are re-checked; the two small JSON files are derived from the pinned capture.
    files = {**{name: (sha, lambda url=url, sha=sha: fetch(url, sha)) for name, (url, sha) in DECODER.items()},
             'master.dat': (MASTER, lambda: master_from_capture(from_capture())),
             'stocks_update.dat': (CATCH_UP, lambda: catch_up_from_capture(from_capture())[1]),
             'stocks_update_request.json': (None, lambda: catch_up_from_capture(from_capture())[0]),
             'schedule_info.json': (None, lambda: schedule_from_capture(from_capture())),
             'stream.jsonl.gz': (STREAM[1], lambda: fetch(*STREAM))}
    for name, (sha256, make) in files.items():
        dest = cache / name
        if dest.exists() and (sha256 is None or hashlib.sha256(dest.read_bytes()).hexdigest() == sha256):
            continue
        data = make()
        temp = dest.with_suffix(dest.suffix + '.tmp')
        temp.write_bytes(data)
        temp.replace(dest)
        print(f'{name}: {len(data):,} bytes', file=sys.stderr)


def check(cache: Path, node: str = 'node', demo_cache: Path | None = None) -> int:
    script = Path(__file__).with_name('sbi_master_layout.cjs')
    demo = [f'--allow-fs-read={demo_cache.resolve()}'] if demo_cache else []
    return subprocess.run([node, '--permission', f'--allow-fs-read={cache.resolve()}', *demo, f'--allow-fs-read={ROOT / "briskapi"}',
                           f'--allow-fs-read={script}', str(script), str(cache.resolve()),
                           *([str(demo_cache.resolve())] if demo_cache else [])], check=False).returncode


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--cache', required=True, type=Path)
    parser.add_argument('--node', default='node')
    parser.add_argument('--demo-cache', type=Path, default=os.environ.get('BRISK_MOCK_CACHE'),
                        help="the demo's assets (download_mock.py; default $BRISK_MOCK_CACHE): also load the demo's snapshot")
    args = parser.parse_args()
    download(args.cache.expanduser())
    demo = Path(args.demo_cache).expanduser() if args.demo_cache else None
    if demo and not (demo / 'snapshot.dat').exists():
        sys.exit(f'{demo} has no demo snapshot; run tools/brisk_mock/download_mock.py --cache {demo} first')
    sys.exit(check(args.cache.expanduser(), args.node, demo))
