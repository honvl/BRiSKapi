"""Sign in to your broker's BRiSK with a passkey, through Chrome's virtual authenticator.

    brisk enroll      # once: pick your broker, sign in by hand, register a passkey in its settings
    brisk login       # each session: Chrome signs in with that passkey and the BRiSK cookies are kept
    brisk forget      # delete the saved passkey
    brisk sites       # the brokers you can pick (SBI, Matsui, Monex, SMBC Nikko, or your own)

or from Python:

    from briskapi import passkey
    passkey.enroll(site="sbi")
    signin = passkey.login()          # signin.cookies; signin.client for a site with a data client (SBI)

How it works. A passkey is a key pair: the broker keeps the public half and your device keeps the
private half, which proves it is you by signing a challenge the site sends. Normally that private half
lives in your phone, laptop or password manager and needs a fingerprint or PIN to use. Here it lives
in a Chrome that briskapi starts, which has a *virtual authenticator*: software that plays the part of
the device and signs without asking. So:

  * enroll opens that Chrome at your broker's login page. You sign in as usual and, in the broker's
    security settings, register a passkey as you would on a new phone. The virtual authenticator
    receives it, briskapi saves it, and Chrome closes. Your phone's and password manager's passkeys
    are not touched; the broker's list of passkeys just shows one more entry, which you can delete.
  * login opens the same Chrome with the saved passkey loaded, goes to the login page, presses the
    passkey button and the virtual authenticator signs. You are signed in without a touch. Chrome
    then reads the BRiSK session cookies (only those of the BRiSK host), saves them and closes.

The saved login is the broker you chose plus the passkey. That passkey's private key IS the credential:
whoever has it can sign in to your account with no further check, so it is kept in the macOS Keychain (or,
only if you set BRISK_PASSKEY_STORE=file, an owner-only file) and is never put on a command line, in a
log or in the repository. A passkey also carries a sign counter that a broker may check, so every change is
saved the moment it happens. Chrome runs under Node over a private pipe (briskapi/decoder/passkey.cjs)
with a temporary profile that is deleted afterwards. Chrome tells the site it is being automated, so a
broker may refuse it. See ARCHITECTURE.md for the details.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.parse

from briskapi._recording import BriskError
from briskapi.sites import Site, config_dir, get_site, load_sites

HELPER = Path(__file__).resolve().parent / 'decoder' / 'passkey.cjs'
SERVICE = 'briskapi-brisk-passkey'
ACCOUNT = 'default'
SECURITY = '/usr/bin/security'
_NAME = re.compile(r'[A-Za-z0-9._-]+')


class PasskeyError(BriskError):
    """Passkey enrollment or sign-in failed."""


@dataclass
class SignIn:
    """The result of `login`: the site, its BRiSK cookies, and the data client if briskapi has one for it."""
    site: Site
    cookies: dict
    client: object = None
    saved_to: Path | None = None


class FileStore:
    """The saved login in an owner-only file. Weaker than the Keychain: anyone who can read your files can sign in."""

    def __init__(self, path=None):
        self.path = Path(path) if path else config_dir() / 'passkey.json'
        self.description = str(self.path)

    def load(self):
        return json.loads(self.path.read_text()) if self.path.exists() else None

    def save(self, record):
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = self.path.with_name(self.path.name + '.tmp')
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as f:
            json.dump(record, f)
        os.replace(temporary, self.path)

    def delete(self):
        self.path.unlink(missing_ok=True)


class KeychainStore:
    """The saved login in the macOS Keychain, through the `security` tool.

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

    def save(self, record):
        secret = base64.b64encode(json.dumps(record).encode()).decode()
        done = subprocess.run([self.security, '-i'], capture_output=True, text=True,
                              input=f'add-generic-password -U -s {self.service} -a {self.account} -w {secret}\n')
        # `security -i` exits 0 even when the command fails, so the write is checked by reading it back.
        if self.load() != record:
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


def cookies_path(site_id) -> Path:
    """Where `login` keeps the BRiSK cookies of a site that has no data client (SBI's go to sbi.cookies_path())."""
    return config_dir() / 'cookies' / f'{site_id}.json'


def _load_record(store):
    record = store.load()
    if record is None:
        return None
    if not (isinstance(record, dict) and isinstance(record.get('site'), str) and isinstance(record.get('credential'), dict)):
        raise PasskeyError('The saved passkey is in an unknown format; run `brisk enroll --replace`')
    return record


def _request(site, login_url=None, launch_url=None, passkey_button=None, chrome=None, profile_dir=None, headless=None):
    request = {'loginUrl': login_url or site.login_url, 'launchUrl': launch_url or site.launch_url, 'cookieHost': site.cookie_host,
               'cookiePrefix': site.cookie_prefix, 'passkeyButton': passkey_button or site.passkey_button,
               'chrome': str(chrome) if chrome else None, 'profileDir': str(profile_dir) if profile_dir else None,
               'headless': headless}
    return {key: value for key, value in request.items() if value is not None}


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
        raise PasskeyError('Enrolling needs an interactive terminal: it waits for you to finish the registration at the broker')
    print('Finish registering the passkey in the Chrome window. When the site says it is registered,\n'
          'press Enter here to save the passkey and close Chrome.', file=sys.stderr)
    input()


def _choose(sites):
    if not sys.stdin.isatty():
        raise PasskeyError('Choose your broker with --site (see `brisk sites`)')
    ordered = list(sites.values())
    print('Which broker do you use BRiSK with?', file=sys.stderr)
    for number, site in enumerate(ordered, 1):
        print(f'  {number}) {site.name} ({site.id})', file=sys.stderr)
    answer = input('Number or id: ').strip().lower()
    if answer.isdigit() and 1 <= int(answer) <= len(ordered):
        return ordered[int(answer) - 1].id
    return answer


def _check_client(site):
    """A data client sends its cookies to one host. Cookies of any other site must never be handed to it."""
    if site.client == 'sbi':
        from briskapi import sbi
        host = urllib.parse.urlsplit(sbi.ORIGIN).hostname
        if site.cookie_host != host:
            raise PasskeyError(f'Cookies of {site.cookie_host} are never given to the SBI client, which only talks to {host}')


def _deliver(site, cookies, remember):
    if site.client == 'sbi':
        from briskapi import sbi
        client = sbi.login(cookies=cookies, remember=remember)
        return SignIn(site, cookies, client, sbi.cookies_path() if remember else None)
    path = None
    if remember:
        path = cookies_path(site.id)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as f:
            json.dump(cookies, f)
    return SignIn(site, cookies, None, path)


def enroll(site=None, login_url=None, chrome=None, profile_dir=None, replace=False, store=None, node='node', confirm=None,
           headless=False, choose=None, sites_file=None):
    """Capture the passkey your broker registers in a Chrome you drive by hand, and save it with the site.

    Returns a public summary (never the private key)."""
    store = store or default_store()
    existing = _load_record(store)
    if existing and not replace:
        raise PasskeyError(f'A passkey for {existing["site"]} is already saved. Use --replace to enroll another (the old one '
                           'stays registered at the broker until you remove it in its security settings)')
    sites = load_sites(sites_file)
    chosen = get_site(sites, site or (choose or _choose)(sites))
    latest = []
    _run('enroll', _request(chosen, login_url=login_url, chrome=chrome, profile_dir=profile_dir, headless=headless or None), node,
         on_credential=latest.append, on_registered=confirm or _confirm)
    if not latest:
        raise PasskeyError('No passkey was captured')
    store.save({'site': chosen.id, 'credential': latest[-1]})  # Only now: a failed enrollment must not replace a working one.
    return {'site': chosen.id, 'rp_id': latest[-1]['rpId'], 'user_name': latest[-1].get('userName'), 'stored_in': store.description}


def login(remember=True, login_url=None, launch_url=None, passkey_button=None, chrome=None, profile_dir=None, headless=False,
          store=None, node='node', sites_file=None) -> SignIn:
    """Sign in to the saved site with the saved passkey and keep its BRiSK cookies."""
    store = store or default_store()
    record = _load_record(store)
    if record is None:
        raise PasskeyError('No passkey is saved. Run `brisk enroll` first')
    site = get_site(load_sites(sites_file), record['site'])
    _check_client(site)
    request = {**_request(site, login_url, launch_url, passkey_button, chrome, profile_dir, headless or None),
               'credential': record['credential']}
    result = _run('login', request, node, on_credential=lambda credential: store.save({'site': site.id, 'credential': credential}))
    return _deliver(site, result['cookies'], remember)


def forget(store=None):
    """Delete the saved passkey. It stays registered at the broker until you remove it in its settings."""
    (store or default_store()).delete()
