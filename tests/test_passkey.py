"""Passkey sign-in: stores, the Python side of the helper protocol, the CLI, and a full-stack run in a real Chrome."""
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import time
import urllib.request

import pytest

import briskapi
import briskapi.cli as cli
from briskapi import passkey, sbi

REPO = Path(__file__).resolve().parent.parent
PASSKEY = {'credentialId': 'Y3JlZA==', 'isResidentCredential': True, 'rpId': 'sbisec.co.jp', 'privateKey': 'PRIVATE-KEY-MATERIAL',
           'userHandle': 'AQID', 'signCount': 4, 'userName': 'trader'}


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.delenv('BRISK_PASSKEY_STORE', raising=False)
    monkeypatch.setattr(sbi, '_client', None)


# ---- stores ----

def test_file_store_keeps_the_passkey_in_an_owner_only_file(tmp_path):
    store = passkey.FileStore(tmp_path / 'deep' / 'passkey.json')
    assert store.load() is None
    store.save(PASSKEY)
    assert store.load() == PASSKEY
    assert stat.S_IMODE(store.path.stat().st_mode) == 0o600
    assert stat.S_IMODE(store.path.parent.stat().st_mode) == 0o700
    store.save({**PASSKEY, 'signCount': 5})
    assert store.load()['signCount'] == 5
    assert [p.name for p in store.path.parent.iterdir()] == ['passkey.json']  # no temporary file left behind
    store.delete()
    store.delete()
    assert store.load() is None
    assert passkey.FileStore().path == tmp_path / 'config' / 'brisk' / 'sbi-passkey.json'


FAKE_SECURITY = r'''#!/bin/sh
dir="$FAKE_SECURITY_DIR"
echo "$@" >> "$dir/argv"
case "$1" in
  -i)
    read -r line
    echo "$line" >> "$dir/stdin"
    if [ -z "$FAKE_SECURITY_FAIL" ]; then printf '%s' "${line##* }" > "$dir/item"; else echo "add-generic-password: returned -25299"; fi ;;
  find-generic-password)
    [ -n "$FAKE_SECURITY_BROKEN" ] && exit 1
    if [ -f "$dir/item" ]; then cat "$dir/item"; echo; else exit 44; fi ;;
  delete-generic-password) rm -f "$dir/item" ;;
esac
'''


@pytest.fixture
def keychain(tmp_path, monkeypatch):
    script = tmp_path / 'security'
    script.write_text(FAKE_SECURITY)
    script.chmod(0o755)
    (tmp_path / 'kc').mkdir()
    monkeypatch.setenv('FAKE_SECURITY_DIR', str(tmp_path / 'kc'))
    return passkey.KeychainStore(security=str(script)), tmp_path / 'kc'


def test_keychain_store_never_puts_the_passkey_on_a_command_line(keychain):
    store, kc = keychain
    assert store.load() is None
    store.save(PASSKEY)
    assert store.load() == PASSKEY
    argv = (kc / 'argv').read_text()
    assert 'PRIVATE-KEY-MATERIAL' not in argv
    assert 'add-generic-password' not in argv, 'a write on the command line would show the secret to other users'
    assert '-i' in argv.splitlines()
    assert all(line.startswith(('-i', 'find-generic-password')) for line in argv.splitlines())
    written = (kc / 'stdin').read_text()
    assert written.startswith('add-generic-password -U -s briskapi-sbi-passkey -a default -w ')
    assert 'PRIVATE-KEY-MATERIAL' not in written  # base64 of the JSON, not the text itself
    store.delete()
    assert store.load() is None


def test_keychain_store_checks_that_the_write_really_happened(keychain, monkeypatch):
    store, _ = keychain
    monkeypatch.setenv('FAKE_SECURITY_FAIL', '1')
    with pytest.raises(passkey.PasskeyError, match=r'Could not save the passkey to the Keychain \(error -25299\)'):
        store.save(PASSKEY)
    monkeypatch.delenv('FAKE_SECURITY_FAIL')
    store.save(PASSKEY)
    monkeypatch.setenv('FAKE_SECURITY_BROKEN', '1')
    with pytest.raises(passkey.PasskeyError, match=r'Could not read the passkey from the Keychain \(security exit 1\)'):
        store.load()


def test_keychain_store_rejects_foreign_items_and_unsafe_names(keychain, tmp_path):
    store, kc = keychain
    (kc / 'item').write_text('not base64 json!')
    with pytest.raises(passkey.PasskeyError, match='not a passkey saved by briskapi'):
        store.load()
    for name in ('a b', 'x;rm', '', 'a\nb'):
        with pytest.raises(passkey.PasskeyError, match='may only use letters'):
            passkey.KeychainStore(service=name)
        with pytest.raises(passkey.PasskeyError, match='may only use letters'):
            passkey.KeychainStore(account=name)


def test_default_store_follows_the_platform_and_the_environment(monkeypatch):
    monkeypatch.setattr(passkey.sys, 'platform', 'darwin')
    assert isinstance(passkey.default_store(), passkey.KeychainStore)
    monkeypatch.setattr(passkey.sys, 'platform', 'linux')
    with pytest.raises(passkey.PasskeyError, match='BRISK_PASSKEY_STORE=file'):
        passkey.default_store()
    monkeypatch.setenv('BRISK_PASSKEY_STORE', 'file')
    assert isinstance(passkey.default_store(), passkey.FileStore)
    monkeypatch.setenv('BRISK_PASSKEY_STORE', 'keychain')
    with pytest.raises(passkey.PasskeyError, match='needs macOS'):
        passkey.default_store()


# ---- the helper protocol, against a scripted helper ----

FAKE_HELPER = r'''
const fs = require('fs');
const readline = require('readline');
const scenario = process.env.FAKE_SCENARIO;
const out = (m) => process.stdout.write(JSON.stringify(m) + '\n', () => { if (m.type === 'result') process.exit(0); });
const rl = readline.createInterface({ input: process.stdin });
const fresh = { credentialId: 'bmV3', isResidentCredential: true, rpId: 'sbisec.co.jp', privateKey: 'PRIV-NEW', userHandle: 'AQID', signCount: 1, userName: 'trader' };
let request = null;
rl.on('line', (line) => {
  if (!request) {
    request = JSON.parse(line);
    fs.writeFileSync(process.env.FAKE_LOG, JSON.stringify({ argv: process.argv.slice(2), request, pid: process.pid }));
    const credential = request.credential && { ...request.credential, signCount: request.credential.signCount + 1 };
    if (scenario === 'login-ok') { out({ type: 'credential', credential }); out({ type: 'result', ok: true, cookies: { session_x: 'cookie-value', other: 'o' } }); }
    else if (scenario === 'noise') { console.log('some stray output'); out({ type: 'credential', credential }); out({ type: 'result', ok: true, cookies: { session_x: 'v' } }); }
    else if (scenario === 'login-refused') { out({ type: 'credential', credential }); out({ type: 'result', ok: false, error: 'The login was refused' }); }
    else if (scenario === 'crash') { out({ type: 'credential', credential }); process.exit(3); }
    else if (scenario === 'hang') { out({ type: 'credential', credential }); setInterval(() => {}, 1000); }
    else if (scenario === 'enroll-ok') { out({ type: 'credential', credential: fresh }); out({ type: 'registered' }); }
    else if (scenario === 'enroll-refused') { out({ type: 'credential', credential: fresh }); out({ type: 'result', ok: false, error: 'No registration happened' }); }
    else if (scenario === 'enroll-empty') { out({ type: 'result', ok: true }); }
    else if (scenario === 'early-exit') { process.exit(4); }
  } else if (line.trim() === 'done') {
    fs.writeFileSync(process.env.FAKE_LOG + '.done', 'done');
    out({ type: 'credential', credential: { ...fresh, signCount: 2 } });
    out({ type: 'result', ok: true });
  }
});
'''


@pytest.fixture
def helper(tmp_path, monkeypatch):
    script = tmp_path / 'fake_passkey.cjs'
    script.write_text(FAKE_HELPER)
    monkeypatch.setattr(passkey, 'HELPER', script)
    log = tmp_path / 'helper.json'
    monkeypatch.setenv('FAKE_LOG', str(log))

    def scenario(name):
        monkeypatch.setenv('FAKE_SCENARIO', name)
        return log
    return scenario


def saved(tmp_path, credential=PASSKEY):
    store = passkey.FileStore(tmp_path / 'store.json')
    store.save(credential)
    return store


def test_login_signs_in_saves_the_advanced_passkey_and_uses_the_cookies(tmp_path, helper):
    log = helper('login-ok')
    store = saved(tmp_path)
    client = passkey.login(store=store, login_url='https://x.example/login', launch_url='https://x.example/brisk',
                           passkey_button='Passkey', headless=True, chrome=Path('/opt/chrome'), profile_dir=tmp_path / 'profile')
    assert store.load()['signCount'] == 5, 'the sign counter must be saved'
    assert client.session.cookies == {'session_x': 'cookie-value', 'other': 'o'}
    seen = json.loads(log.read_text())
    assert seen['argv'] == ['login'], 'nothing but the mode may be on the command line'
    request = seen['request']
    assert request['credential'] == PASSKEY and request['mode'] == 'login'
    assert {k: request[k] for k in ('loginUrl', 'launchUrl', 'passkeyButton', 'headless', 'chrome', 'profileDir', 'cookieHost')} == {
        'loginUrl': 'https://x.example/login', 'launchUrl': 'https://x.example/brisk', 'passkeyButton': 'Passkey',
        'headless': True, 'chrome': '/opt/chrome', 'profileDir': str(tmp_path / 'profile'), 'cookieHost': 'sbi.brisk.jp'}
    cookie_file = tmp_path / 'config' / 'brisk' / 'sbi-cookies.json'
    assert json.loads(cookie_file.read_text()) == {'session_x': 'cookie-value', 'other': 'o'}
    assert stat.S_IMODE(cookie_file.stat().st_mode) == 0o600


def test_cookies_of_another_site_are_never_given_to_the_sbi_client(tmp_path, helper):
    log = helper('login-ok')
    store = saved(tmp_path)
    for host in ('matsui.example', 'brisk.jp', 'sbi.brisk.jp.evil.example'):
        with pytest.raises(passkey.PasskeyError, match=r'never given to the SBI client, which only talks to sbi\.brisk\.jp'):
            passkey.login(store=store, cookie_host=host)
    assert not log.exists(), 'the refusal must come before Chrome is started'
    assert store.load() == PASSKEY, 'no sign-in happened, so the counter is unchanged'
    assert passkey.login(store=store, cookie_host='sbi.brisk.jp', remember=False).session.cookies['session_x'] == 'cookie-value'


def test_login_without_remember_keeps_the_cookies_in_memory(tmp_path, helper):
    helper('login-ok')
    passkey.login(store=saved(tmp_path), remember=False)
    assert not (tmp_path / 'config' / 'brisk' / 'sbi-cookies.json').exists()
    assert sbi._default().session.cookies['session_x'] == 'cookie-value'


def test_stray_helper_output_is_ignored(tmp_path, helper):
    helper('noise')
    assert passkey.login(store=saved(tmp_path), remember=False).session.cookies == {'session_x': 'v'}


def test_a_refused_login_is_an_error_but_the_passkey_counter_is_still_saved(tmp_path, helper):
    helper('login-refused')
    store = saved(tmp_path)
    with pytest.raises(passkey.PasskeyError, match='The login was refused'):
        passkey.login(store=store)
    assert store.load()['signCount'] == 5
    assert sbi._client is None


def test_a_helper_that_dies_is_reported_and_its_last_passkey_is_kept(tmp_path, helper):
    helper('crash')
    store = saved(tmp_path)
    with pytest.raises(passkey.PasskeyError, match=r'stopped unexpectedly \(exit 3\)'):
        passkey.login(store=store)
    assert store.load()['signCount'] == 5
    helper('early-exit')
    with pytest.raises(passkey.PasskeyError, match=r'stopped unexpectedly \(exit 4\)'):
        passkey.login(store=store)


def test_if_the_passkey_cannot_be_saved_the_helper_is_stopped(tmp_path, helper):
    log = helper('hang')

    class Broken:
        def load(self):
            return PASSKEY

        def save(self, credential):
            raise passkey.PasskeyError('Keychain is locked')

    with pytest.raises(passkey.PasskeyError, match='Keychain is locked'):
        passkey.login(store=Broken())
    pid = json.loads(log.read_text())['pid']
    for _ in range(50):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    else:
        os.kill(pid, 9)
        pytest.fail('the helper (and its Chrome) was left running')


def test_login_needs_a_saved_passkey_and_node(tmp_path, helper):
    with pytest.raises(passkey.PasskeyError, match='Run `brisk sbi enroll` first'):
        passkey.login(store=passkey.FileStore(tmp_path / 'none.json'))
    for call in (lambda: passkey.login(store=saved(tmp_path), node=str(tmp_path / 'missing')),
                 lambda: passkey.enroll(store=passkey.FileStore(tmp_path / 'e.json'), node=str(tmp_path / 'missing'))):
        with pytest.raises(briskapi.BriskError, match='not found'):
            call()


def test_enroll_saves_the_final_passkey_after_the_user_confirms(tmp_path, helper):
    log = helper('enroll-ok')
    store = passkey.FileStore(tmp_path / 'store.json')
    confirmed = []
    summary = passkey.enroll(store=store, confirm=lambda: confirmed.append(True), login_url='https://x.example/', replace=False)
    assert confirmed == [True]
    assert Path(str(log) + '.done').exists(), 'the helper must be told when to close Chrome'
    assert store.load()['signCount'] == 2 and store.load()['privateKey'] == 'PRIV-NEW'
    assert summary == {'rp_id': 'sbisec.co.jp', 'user_name': 'trader', 'stored_in': str(store.path)}
    assert 'PRIV' not in json.dumps(summary)
    assert json.loads(log.read_text())['argv'] == ['enroll']


def test_enroll_does_not_replace_a_working_passkey_unless_asked_or_until_it_succeeds(tmp_path, helper):
    helper('enroll-ok')
    store = saved(tmp_path)
    with pytest.raises(passkey.PasskeyError, match='Use --replace'):
        passkey.enroll(store=store, confirm=lambda: None)
    assert store.load() == PASSKEY
    helper('enroll-refused')
    with pytest.raises(passkey.PasskeyError, match='No registration happened'):
        passkey.enroll(store=store, confirm=lambda: None, replace=True)
    assert store.load() == PASSKEY, 'a failed enrollment must leave the old passkey alone'
    helper('enroll-empty')
    with pytest.raises(passkey.PasskeyError, match='No passkey was captured'):
        passkey.enroll(store=store, confirm=lambda: None, replace=True)
    helper('enroll-ok')
    passkey.enroll(store=store, confirm=lambda: None, replace=True)
    assert store.load()['privateKey'] == 'PRIV-NEW'


def test_enroll_waits_for_a_person_and_stops_chrome_when_there_is_none(tmp_path, helper, monkeypatch):
    log = helper('enroll-ok')
    store = passkey.FileStore(tmp_path / 'store.json')
    monkeypatch.setattr(passkey.sys, 'stdin', type('NoTty', (), {'isatty': lambda self: False})())
    with pytest.raises(passkey.PasskeyError, match='needs an interactive terminal'):
        passkey.enroll(store=store)
    assert store.load() is None
    pid = json.loads(log.read_text())['pid']
    for _ in range(50):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    else:
        os.kill(pid, 9)
        pytest.fail('the helper was left running')


def test_confirm_prompts_on_stderr_and_waits_for_enter(monkeypatch, capsys):
    monkeypatch.setattr(passkey.sys, 'stdin', type('Tty', (), {'isatty': lambda self: True})())
    monkeypatch.setattr('builtins.input', lambda: 'ok')
    passkey._confirm()
    captured = capsys.readouterr()
    assert 'press Enter here' in captured.err and captured.out == ''


def test_forget_deletes_the_saved_passkey(tmp_path):
    store = saved(tmp_path)
    passkey.forget(store)
    assert store.load() is None


def test_the_sbi_module_offers_the_same_flows(tmp_path, helper):
    helper('login-ok')
    store = saved(tmp_path)
    assert sbi.passkey_login(store=store, remember=False).session.cookies['session_x'] == 'cookie-value'
    sbi.passkey_forget(store=store)
    assert store.load() is None
    helper('enroll-ok')
    assert sbi.passkey_enroll(store=store, confirm=lambda: None)['rp_id'] == 'sbisec.co.jp'
    assert sbi.PasskeyError is passkey.PasskeyError


# ---- command line ----

def test_cli_enroll_login_and_forget(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(sbi, 'passkey_enroll', lambda **kw: calls.append(('enroll', kw)) or {'rp_id': 'r', 'stored_in': 'kc'})
    monkeypatch.setattr(sbi, 'passkey_login', lambda **kw: calls.append(('login', kw)))
    monkeypatch.setattr(sbi, 'passkey_forget', lambda **kw: calls.append(('forget', kw)))
    cli.main(['sbi', 'enroll', '--login-url', 'https://x/', '--chrome', '/c', '--profile-dir', '/p', '--replace'])
    assert calls[-1] == ('enroll', {'login_url': 'https://x/', 'chrome': Path('/c'), 'profile_dir': Path('/p'), 'replace': True})
    assert json.loads(capsys.readouterr().out) == {'rp_id': 'r', 'stored_in': 'kc'}
    cli.main(['sbi', 'login'])
    assert calls[-1] == ('login', {'remember': True, 'login_url': None, 'launch_url': None, 'passkey_button': None, 'chrome': None,
                                   'profile_dir': None, 'headless': False})
    assert 'session cookies saved to' in capsys.readouterr().err
    cli.main(['sbi', 'login', '--no-remember', '--headless', '--launch-url', 'https://b/', '--passkey-button', 'Passkey'])
    assert calls[-1][1]['remember'] is False and calls[-1][1]['headless'] is True and calls[-1][1]['launch_url'] == 'https://b/'
    assert 'not saved' in capsys.readouterr().err
    cli.main(['sbi', 'forget'])
    assert calls[-1] == ('forget', {})
    assert 'stays registered at SBI' in capsys.readouterr().err
    with pytest.raises(SystemExit):
        cli.main(['sbi'])


def test_cli_reports_passkey_errors_without_a_traceback(tmp_path, monkeypatch):
    monkeypatch.setenv('BRISK_PASSKEY_STORE', 'file')
    with pytest.raises(SystemExit) as stopped:
        cli.main(['sbi', 'login'])
    assert str(stopped.value.code) == 'brisk: error: No passkey is saved. Run `brisk sbi enroll` first'


# ---- everything together: Python, the Node host, a real Chrome and a fake SBI ----

def chrome_available():
    done = subprocess.run(['node', '-e', f'require({str(REPO / "briskapi/decoder/passkey.cjs")!r}).findChrome()'], capture_output=True)
    return done.returncode == 0


@pytest.mark.skipif(shutil.which('node') is None or not chrome_available(), reason='needs Node and Chrome')
def test_full_stack_enroll_then_login_against_a_fake_sbi(tmp_path, monkeypatch):
    monkeypatch.setattr(sbi, 'ORIGIN', 'https://brisk.localhost')
    site = subprocess.Popen(['node', str(REPO / 'tools' / 'brisk_mock' / 'fake_sbi_passkey.cjs')], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, text=True)
    try:
        origins = json.loads(site.stdout.readline())

        def state():
            with urllib.request.urlopen(origins['mainOrigin'] + '/__state', timeout=10) as response:
                return json.load(response)

        def registered():
            for _ in range(200):
                if state()['registered']:
                    return
                time.sleep(0.05)
            raise AssertionError('the site never saw the registration')

        store = passkey.FileStore(tmp_path / 'passkey.json')
        summary = passkey.enroll(store=store, login_url=origins['mainOrigin'] + '/enroll', headless=True, confirm=registered)
        assert summary['rp_id'] == 'main.localhost'
        enrolled = store.load()
        assert enrolled['privateKey'] and enrolled['isResidentCredential'] is True

        client = passkey.login(store=store, login_url=origins['mainOrigin'] + '/login', headless=True, remember=False)
        seen = state()
        assert client.session.cookies['session_fake'] == seen['briskCookieValue']
        assert 'main_session' not in client.session.cookies
        assert seen['accepted'] == 1
        assert store.load()['signCount'] > enrolled['signCount']
        assert store.load()['signCount'] == seen['counter'], 'the saved counter must be the one the site last saw'

        second = passkey.login(store=store, login_url=origins['mainOrigin'] + '/login', headless=True, remember=False)
        assert state()['accepted'] == 2 and second.session.cookies['session_fake'] == state()['briskCookieValue']
    finally:
        site.stdin.close()
        site.wait(timeout=15)
