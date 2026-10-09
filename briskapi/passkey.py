"""Sign in to SBI BRiSK with a passkey, through Chrome's virtual authenticator.

    brisk sbi enroll      # once: sign in by hand, register a passkey in SBI's security settings
    brisk sbi login       # each session: Chrome signs in with the saved passkey, cookies are kept
    brisk live --sbi      # then uses those cookies

or from Python:

    from briskapi import sbi
    sbi.passkey_login()   # returns the same client as sbi.login(cookies=...)

enroll opens a Chrome whose virtual authenticator captures the passkey SBI registers. login starts
Chrome again with that passkey, answers the site's passkey sign-in without a touch and reads the
BRiSK session cookies (only those of the BRiSK host). Chrome runs under Node over a private pipe
(briskapi/decoder/passkey.cjs); the profile is temporary and deleted afterwards.

The passkey's private key IS the credential: whoever holds it can sign in to your SBI account with
no further check. It is kept in the macOS Keychain (or, only if you set BRISK_PASSKEY_STORE=file,
an owner-only file) and is never put on a command line, in a log or in the repository. A passkey
also carries a sign counter that SBI may check, so every change is saved the moment it happens.
"""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.parse

from briskapi._recording import BriskError

HELPER = Path(__file__).resolve().parent / 'decoder' / 'passkey.cjs'
SERVICE = 'briskapi-sbi-passkey'
ACCOUNT = 'default'
SECURITY = '/usr/bin/security'
_NAME = re.compile(r'[A-Za-z0-9._-]+')


class PasskeyError(BriskError):
    """Passkey enrollment or sign-in failed."""


def _config_dir() -> Path:
    return Path(os.environ.get('XDG_CONFIG_HOME') or Path.home() / '.config') / 'brisk'


class FileStore:
    """The passkey in an owner-only file. Weaker than the Keychain: anyone who can read your files can sign in."""

    def __init__(self, path=None):
        self.path = Path(path) if path else _config_dir() / 'sbi-passkey.json'
        self.description = str(self.path)

    def load(self):
        return json.loads(self.path.read_text()) if self.path.exists() else None

    def save(self, credential):
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = self.path.with_name(self.path.name + '.tmp')
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as f:
            json.dump(credential, f)
        os.replace(temporary, self.path)

    def delete(self):
        self.path.unlink(missing_ok=True)


class KeychainStore:
    """The passkey in the macOS Keychain, through the `security` tool.

    `security add-generic-password -w SECRET` would put the secret on the command line, where other
    users can read it. `security -i` reads its commands from stdin instead, so only `security -i` is
    ever visible. The secret is base64 JSON, which needs no quoting.
    """

    def __init__(self, service=SERVICE, account=ACCOUNT, security=SECURITY):
        for name in (service, account):
            if not _NAME.fullmatch(name):
                raise PasskeyError('Keychain service and account names may only use letters, digits, dots, dashes and underscores')
        self.service, self.account, self.security = service, account, security
        self.description = f'macOS Keychain item {service}'

    def load(self):
        done = subprocess.run([self.security, 'find-generic-password', '-s', self.service, '-a', self.account, '-w'],
                              capture_output=True, text=True)
        if done.returncode == 44:  # errSecItemNotFound
            return None
        if done.returncode != 0:
            raise PasskeyError(f'Could not read the passkey from the Keychain (security exit {done.returncode})')
        try:
            return json.loads(base64.b64decode(done.stdout.strip(), validate=True))
        except ValueError as error:
            raise PasskeyError('The Keychain item is not a passkey saved by briskapi') from error

    def save(self, credential):
        secret = base64.b64encode(json.dumps(credential).encode()).decode()
        done = subprocess.run([self.security, '-i'], capture_output=True, text=True,
                              input=f'add-generic-password -U -s {self.service} -a {self.account} -w {secret}\n')
        # `security -i` exits 0 even when the command fails, so the write is checked by reading it back.
        if self.load() != credential:
            code = re.search(r'returned (-?\d+)', done.stdout + done.stderr)
            raise PasskeyError('Could not save the passkey to the Keychain' + (f' (error {code.group(1)})' if code else ''))

    def delete(self):
        subprocess.run([self.security, 'delete-generic-password', '-s', self.service, '-a', self.account],
                       capture_output=True, text=True)


def default_store():
    kind = os.environ.get('BRISK_PASSKEY_STORE') or ('keychain' if sys.platform == 'darwin' else '')
    if kind == 'keychain':
        if sys.platform != 'darwin':
            raise PasskeyError('The Keychain store needs macOS')
        return KeychainStore()
    if kind == 'file':
        return FileStore()
    raise PasskeyError('There is no secure place for the passkey on this system. Set BRISK_PASSKEY_STORE=file to keep it '
                       'in an owner-only file (weaker: anyone who can read your files can sign in)')


def _request(**options):
    names = {'login_url': 'loginUrl', 'launch_url': 'launchUrl', 'passkey_button': 'passkeyButton', 'cookie_host': 'cookieHost',
             'profile_dir': 'profileDir', 'chrome': 'chrome', 'headless': 'headless'}
    paths = ('profile_dir', 'chrome')
    return {names[key]: str(value) if key in paths else value for key, value in options.items() if value is not None}


def _send(proc, text):
    try:
        proc.stdin.write(text)
        proc.stdin.flush()
    except BrokenPipeError:
        pass  # The helper already stopped; its exit is reported below.


def _run(mode, request, node, on_credential, on_registered=None):
    """Run the helper; its progress goes to our stderr, its credentials and result come back as JSON lines."""
    from briskapi import cli
    cli.check_node(node)
    proc = subprocess.Popen([node, str(HELPER), mode], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    result = None
    try:
        _send(proc, json.dumps({'mode': mode, **request}) + '\n')
        for line in proc.stdout:
            try:
                message = json.loads(line)
            except ValueError:
                continue
            kind = message.get('type')
            if kind == 'credential':
                on_credential(message['credential'])
            elif kind == 'registered' and on_registered:
                on_registered()
                _send(proc, 'done\n')
            elif kind == 'result':
                result = message
        code = proc.wait()
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(15)
            except subprocess.TimeoutExpired:
                proc.kill()
    if result is None:
        raise PasskeyError(f'The passkey helper stopped unexpectedly (exit {code})')
    if not result.get('ok'):
        raise PasskeyError(result.get('error') or 'The passkey helper failed')
    return result


def _confirm():
    if not sys.stdin.isatty():
        raise PasskeyError('Enrolling needs an interactive terminal: it waits for you to finish the registration on SBI')
    print('Finish registering the passkey on SBI in the Chrome window. When the site says it is registered,\n'
          'press Enter here to save the passkey and close Chrome.', file=sys.stderr)
    input()


def enroll(login_url=None, chrome=None, profile_dir=None, replace=False, store=None, node='node', confirm=None, headless=False):
    """Capture the passkey SBI registers in a Chrome you drive by hand, and save it. Returns a public summary."""
    store = store or default_store()
    if not replace and store.load() is not None:
        raise PasskeyError('A passkey is already saved. Use --replace to enroll another (the old one stays registered '
                           'at SBI until you remove it in its security settings)')
    latest = []
    _run('enroll', _request(login_url=login_url, chrome=chrome, profile_dir=profile_dir, headless=headless or None), node,
         on_credential=lambda credential: latest.append(credential), on_registered=confirm or _confirm)
    if not latest:
        raise PasskeyError('No passkey was captured')
    store.save(latest[-1])  # Only now: a failed enrollment must not replace a passkey that still works.
    return {'rp_id': latest[-1]['rpId'], 'user_name': latest[-1].get('userName'), 'stored_in': store.description}


def login(remember=True, login_url=None, launch_url=None, passkey_button=None, cookie_host=None, chrome=None, profile_dir=None,
          headless=False, store=None, node='node'):
    """Sign in with the saved passkey and use the BRiSK session cookies (as sbi.login). Returns the client."""
    from briskapi import sbi
    # The SBI client sends its cookies to one host. Cookies of any other site must never be handed to it.
    host = urllib.parse.urlsplit(sbi.ORIGIN).hostname
    if cookie_host not in (None, host):
        raise PasskeyError(f'Cookies of {cookie_host} are never given to the SBI client, which only talks to {host}')
    store = store or default_store()
    credential = store.load()
    if credential is None:
        raise PasskeyError('No passkey is saved. Run `brisk sbi enroll` first')
    request = _request(login_url=login_url, launch_url=launch_url, passkey_button=passkey_button, cookie_host=host,
                       chrome=chrome, profile_dir=profile_dir, headless=headless or None)
    result = _run('login', {**request, 'credential': credential}, node, on_credential=store.save)
    return sbi.login(cookies=result['cookies'], remember=remember)


def forget(store=None):
    """Delete the saved passkey. It stays registered at SBI until you remove it in its security settings."""
    (store or default_store()).delete()
