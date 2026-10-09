"""Passkey sign-in: stores, the Python side of the helper protocol, site selection, the CLI, and full-stack runs in a real Chrome."""
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
from briskapi import passkey, sbi, sites

REPO = Path(__file__).resolve().parent.parent
PASSKEY = {'credentialId': 'Y3JlZA==', 'isResidentCredential': True, 'rpId': 'sbisec.co.jp', 'privateKey': 'PRIVATE-KEY-MATERIAL',
           'userHandle': 'AQID', 'signCount': 4, 'userName': 'trader'}
RECORD = {'site': 'sbi', 'credential': PASSKEY}


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.delenv('BRISK_PASSKEY_STORE', raising=False)
    monkeypatch.setattr(sbi, '_client', None)


# ---- stores ----

def test_file_store_keeps_the_saved_login_in_an_owner_only_file(tmp_path):
    store = passkey.FileStore(tmp_path / 'deep' / 'passkey.json')
    assert store.load() is None
    store.save(RECORD)
    assert store.load() == RECORD
    assert stat.S_IMODE(store.path.stat().st_mode) == 0o600
    assert stat.S_IMODE(store.path.parent.stat().st_mode) == 0o700
    store.save({**RECORD, 'site': 'matsui'})
    assert store.load()['site'] == 'matsui'
    assert [p.name for p in store.path.parent.iterdir()] == ['passkey.json']  # no temporary file left behind
    store.delete()
    store.delete()
    assert store.load() is None
    assert passkey.FileStore().path == tmp_path / 'config' / 'brisk' / 'passkey.json'


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
    store.save(RECORD)
    assert store.load() == RECORD
    argv = (kc / 'argv').read_text()
    assert 'PRIVATE-KEY-MATERIAL' not in argv
    assert 'add-generic-password' not in argv, 'a write on the command line would show the secret to other users'
    assert '-i' in argv.splitlines()
    assert all(line.startswith(('-i', 'find-generic-password')) for line in argv.splitlines())
    written = (kc / 'stdin').read_text()
    assert written.startswith('add-generic-password -U -s briskapi-brisk-passkey -a default -w ')
    assert 'PRIVATE-KEY-MATERIAL' not in written  # base64 of the JSON, not the text itself
    store.delete()
    assert store.load() is None


def test_keychain_store_checks_that_the_write_really_happened(keychain, monkeypatch):
    store, _ = keychain
    monkeypatch.setenv('FAKE_SECURITY_FAIL', '1')
    with pytest.raises(passkey.PasskeyError, match=r'Could not save the passkey to the Keychain \(error -25299\)'):
        store.save(RECORD)
    monkeypatch.delenv('FAKE_SECURITY_FAIL')
    store.save(RECORD)
    monkeypatch.setenv('FAKE_SECURITY_BROKEN', '1')
    with pytest.raises(passkey.PasskeyError, match=r'Could not read the passkey from the Keychain \(security exit 1\)'):
        store.load()


def test_keychain_store_rejects_foreign_items_and_unsafe_names(keychain):
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


def saved(tmp_path, record=RECORD):
    store = passkey.FileStore(tmp_path / 'store.json')
    store.save(record)
    return store


def wait_gone(pid):
    for _ in range(50):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.1)
    os.kill(pid, 9)
    pytest.fail('the helper (and its Chrome) was left running')


def test_login_signs_in_saves_the_advanced_passkey_and_uses_the_cookies(tmp_path, helper):
    log = helper('login-ok')
    store = saved(tmp_path)
    signin = passkey.login(store=store, login_url='https://x.example/login', launch_url='https://x.example/brisk',
                           passkey_button='Passkey', headless=True, chrome=Path('/opt/chrome'), profile_dir=tmp_path / 'profile')
    assert store.load() == {'site': 'sbi', 'credential': {**PASSKEY, 'signCount': 5}}, 'the sign counter must be saved'
    assert signin.site.id == 'sbi' and signin.cookies == {'session_x': 'cookie-value', 'other': 'o'}
    assert signin.client.session.cookies == signin.cookies and sbi._default() is signin.client
    seen = json.loads(log.read_text())
    assert seen['argv'] == ['login'], 'nothing but the mode may be on the command line'
    request = seen['request']
    assert request['credential'] == PASSKEY and request['mode'] == 'login'
    assert {k: request[k] for k in ('loginUrl', 'launchUrl', 'passkeyButton', 'headless', 'chrome', 'profileDir', 'cookieHost',
                                    'cookiePrefix')} == {
        'loginUrl': 'https://x.example/login', 'launchUrl': 'https://x.example/brisk', 'passkeyButton': 'Passkey',
        'headless': True, 'chrome': '/opt/chrome', 'profileDir': str(tmp_path / 'profile'), 'cookieHost': 'sbi.brisk.jp',
        'cookiePrefix': 'session_'}
    cookie_file = tmp_path / 'config' / 'brisk' / 'sbi-cookies.json'
    assert signin.saved_to == cookie_file
    assert json.loads(cookie_file.read_text()) == signin.cookies
    assert stat.S_IMODE(cookie_file.stat().st_mode) == 0o600


def test_the_site_supplies_the_defaults_and_the_chosen_site_is_remembered(tmp_path, helper):
    log = helper('login-ok')
    store = saved(tmp_path, {'site': 'matsui', 'credential': PASSKEY})
    passkey.login(store=store, remember=False)
    request = json.loads(log.read_text())['request']
    matsui = sites.BUILTIN['matsui']
    assert (request['loginUrl'], request['cookieHost'], request['passkeyButton'], request['cookiePrefix']) == (
        matsui.login_url, 'matsui.brisk.jp', 'パスキーでログイン', '')
    assert 'launchUrl' not in request and 'chrome' not in request and 'headless' not in request
    assert store.load()['site'] == 'matsui'


def test_a_site_without_a_data_client_keeps_its_cookies_in_a_file_and_leaves_the_sbi_client_alone(tmp_path, helper):
    helper('login-ok')
    store = saved(tmp_path, {'site': 'monex', 'credential': PASSKEY})
    signin = passkey.login(store=store)
    assert signin.site.id == 'monex' and signin.client is None and sbi._client is None
    file = tmp_path / 'config' / 'brisk' / 'cookies' / 'monex.json'
    assert signin.saved_to == file and json.loads(file.read_text()) == {'session_x': 'cookie-value', 'other': 'o'}
    assert stat.S_IMODE(file.stat().st_mode) == 0o600 and stat.S_IMODE(file.parent.stat().st_mode) == 0o700
    assert not (tmp_path / 'config' / 'brisk' / 'sbi-cookies.json').exists()
    assert passkey.login(store=store, remember=False).saved_to is None


def test_login_without_remember_keeps_the_sbi_cookies_in_memory(tmp_path, helper):
    helper('login-ok')
    signin = passkey.login(store=saved(tmp_path), remember=False)
    assert signin.saved_to is None and not (tmp_path / 'config' / 'brisk' / 'sbi-cookies.json').exists()
    assert sbi._default().session.cookies['session_x'] == 'cookie-value'


def test_cookies_of_another_site_are_never_given_to_the_sbi_client(tmp_path, helper):
    log = helper('login-ok')
    store = saved(tmp_path)
    for host in ('matsui.brisk.jp', 'brisk.jp', 'sbi.brisk.jp.evil.example'):
        edited = tmp_path / 'sites.json'
        edited.write_text(json.dumps({'sites': [{'id': 'sbi', 'cookie_host': host}]}))
        with pytest.raises(passkey.PasskeyError, match=r'never given to the SBI client, which only talks to sbi\.brisk\.jp'):
            passkey.login(store=store, sites_file=edited)
    assert not log.exists(), 'the refusal must come before Chrome is started'
    assert store.load() == RECORD, 'no sign-in happened, so the counter is unchanged'
    assert sbi._client is None


def test_stray_helper_output_is_ignored(tmp_path, helper):
    helper('noise')
    assert passkey.login(store=saved(tmp_path), remember=False).cookies == {'session_x': 'v'}


def test_a_refused_login_is_an_error_but_the_passkey_counter_is_still_saved(tmp_path, helper):
    helper('login-refused')
    store = saved(tmp_path)
    with pytest.raises(passkey.PasskeyError, match='The login was refused'):
        passkey.login(store=store)
    assert store.load()['credential']['signCount'] == 5
    assert sbi._client is None


def test_a_helper_that_dies_is_reported_and_its_last_passkey_is_kept(tmp_path, helper):
    helper('crash')
    store = saved(tmp_path)
    with pytest.raises(passkey.PasskeyError, match=r'stopped unexpectedly \(exit 3\)'):
        passkey.login(store=store)
    assert store.load()['credential']['signCount'] == 5
    helper('early-exit')
    with pytest.raises(passkey.PasskeyError, match=r'stopped unexpectedly \(exit 4\)'):
        passkey.login(store=store)


def test_if_the_passkey_cannot_be_saved_the_helper_is_stopped(helper):
    log = helper('hang')

    class Broken:
        def load(self):
            return RECORD

        def save(self, record):
            raise passkey.PasskeyError('Keychain is locked')

    with pytest.raises(passkey.PasskeyError, match='Keychain is locked'):
        passkey.login(store=Broken())
    wait_gone(json.loads(log.read_text())['pid'])


def test_login_needs_a_usable_saved_login_and_node(tmp_path, helper):
    with pytest.raises(passkey.PasskeyError, match='Run `brisk enroll` first'):
        passkey.login(store=passkey.FileStore(tmp_path / 'none.json'))
    for odd in ({'credential': PASSKEY}, {'site': 'sbi'}, PASSKEY, ['x']):
        with pytest.raises(passkey.PasskeyError, match=r'unknown format; run `brisk enroll --replace`'):
            passkey.login(store=saved(tmp_path, odd))
    with pytest.raises(sites.SiteError, match="Unknown site 'gone'"):
        passkey.login(store=saved(tmp_path, {'site': 'gone', 'credential': PASSKEY}))
    for call in (lambda: passkey.login(store=saved(tmp_path), node=str(tmp_path / 'missing')),
                 lambda: passkey.enroll(site='sbi', store=passkey.FileStore(tmp_path / 'e.json'), node=str(tmp_path / 'missing'))):
        with pytest.raises(briskapi.BriskError, match='not found'):
            call()


def test_enroll_saves_the_chosen_site_with_the_final_passkey_after_the_user_confirms(tmp_path, helper):
    log = helper('enroll-ok')
    store = passkey.FileStore(tmp_path / 'store.json')
    confirmed = []
    summary = passkey.enroll(site='matsui', store=store, confirm=lambda: confirmed.append(True))
    assert confirmed == [True]
    assert Path(str(log) + '.done').exists(), 'the helper must be told when to close Chrome'
    record = store.load()
    assert record['site'] == 'matsui' and record['credential']['signCount'] == 2 and record['credential']['privateKey'] == 'PRIV-NEW'
    assert summary == {'site': 'matsui', 'rp_id': 'sbisec.co.jp', 'user_name': 'trader', 'stored_in': str(store.path)}
    assert 'PRIV' not in json.dumps(summary)
    seen = json.loads(log.read_text())
    assert seen['argv'] == ['enroll'] and seen['request']['loginUrl'] == sites.BUILTIN['matsui'].login_url
    passkey.enroll(site='sbi', store=store, confirm=lambda: None, replace=True, login_url='https://x.example/start')
    assert json.loads(log.read_text())['request']['loginUrl'] == 'https://x.example/start'


def test_enroll_asks_which_broker_when_no_site_is_given(tmp_path, helper):
    log = helper('enroll-ok')
    store = passkey.FileStore(tmp_path / 'store.json')
    asked = []
    passkey.enroll(store=store, confirm=lambda: None, choose=lambda found: asked.append(list(found)) or 'smbcnikko')
    assert asked == [['sbi', 'matsui', 'monex', 'smbcnikko']] and store.load()['site'] == 'smbcnikko'
    store.delete()
    log.unlink()
    with pytest.raises(passkey.PasskeyError, match=r'Choose your broker with --site \(see `brisk sites`\)'):
        passkey.enroll(store=store)  # no terminal in the tests, so it cannot ask
    with pytest.raises(sites.SiteError, match="Unknown site 'nowhere'"):
        passkey.enroll(site='nowhere', store=store)
    assert not log.exists() and store.load() is None


def test_the_interactive_choice_takes_a_number_or_an_id(monkeypatch, capsys):
    monkeypatch.setattr(passkey.sys, 'stdin', type('Tty', (), {'isatty': lambda self: True})())
    for typed, expected in (('2', 'matsui'), (' Monex ', 'monex'), ('9', '9'), ('0', '0')):
        monkeypatch.setattr('builtins.input', lambda prompt='', typed=typed: typed)
        assert passkey._choose(sites.BUILTIN) == expected
    err = capsys.readouterr().err
    assert '1) SBI Securities (sbi)' in err and '4) SMBC Nikko Securities (smbcnikko)' in err


def test_enroll_does_not_replace_a_working_passkey_unless_asked_or_until_it_succeeds(tmp_path, helper):
    helper('enroll-ok')
    store = saved(tmp_path)
    with pytest.raises(passkey.PasskeyError, match=r'A passkey for sbi is already saved. Use --replace'):
        passkey.enroll(site='sbi', store=store, confirm=lambda: None)
    assert store.load() == RECORD
    helper('enroll-refused')
    with pytest.raises(passkey.PasskeyError, match='No registration happened'):
        passkey.enroll(site='sbi', store=store, confirm=lambda: None, replace=True)
    assert store.load() == RECORD, 'a failed enrollment must leave the old passkey alone'
    helper('enroll-empty')
    with pytest.raises(passkey.PasskeyError, match='No passkey was captured'):
        passkey.enroll(site='sbi', store=store, confirm=lambda: None, replace=True)
    helper('enroll-ok')
    passkey.enroll(site='monex', store=store, confirm=lambda: None, replace=True)
    assert store.load()['site'] == 'monex' and store.load()['credential']['privateKey'] == 'PRIV-NEW'


def test_enroll_waits_for_a_person_and_stops_chrome_when_there_is_none(tmp_path, helper, monkeypatch):
    log = helper('enroll-ok')
    store = passkey.FileStore(tmp_path / 'store.json')
    monkeypatch.setattr(passkey.sys, 'stdin', type('NoTty', (), {'isatty': lambda self: False})())
    with pytest.raises(passkey.PasskeyError, match='needs an interactive terminal'):
        passkey.enroll(site='sbi', store=store)
    assert store.load() is None
    wait_gone(json.loads(log.read_text())['pid'])


def test_confirm_prompts_on_stderr_and_waits_for_enter(monkeypatch, capsys):
    monkeypatch.setattr(passkey.sys, 'stdin', type('Tty', (), {'isatty': lambda self: True})())
    monkeypatch.setattr('builtins.input', lambda: 'ok')
    passkey._confirm()
    captured = capsys.readouterr()
    assert 'press Enter here' in captured.err and captured.out == ''


def test_forget_deletes_the_saved_login(tmp_path):
    store = saved(tmp_path)
    passkey.forget(store)
    assert store.load() is None


def test_nothing_in_the_sbi_module_is_about_passkeys():
    assert not [name for name in dir(sbi) if 'passkey' in name.lower()]


# ---- command line ----

def test_cli_sites_lists_the_built_ins_and_your_own(tmp_path, capsys):
    cli.main(['sites'])
    lines = capsys.readouterr().out.splitlines()
    assert [line.split()[0] for line in lines] == ['sbi', 'matsui', 'monex', 'smbcnikko']
    assert 'data client: yes' in lines[0] and all('data client: no' in line for line in lines[1:])
    path = tmp_path / 'config' / 'brisk' / 'sites.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'sites': [{'id': 'mine', 'login_url': 'https://m.example/', 'cookie_host': 'mine.brisk.jp'}]}))
    cli.main(['sites'])
    last = capsys.readouterr().out.splitlines()[-1]
    assert last.split()[0] == 'mine' and 'mine.brisk.jp' in last and 'data client: no' in last and last.endswith('sites.json')


def test_cli_enroll_login_and_forget(monkeypatch, capsys):
    calls = []
    sbi_site, matsui_site = sites.BUILTIN['sbi'], sites.BUILTIN['matsui']
    results = iter([passkey.SignIn(sbi_site, {}, object(), Path('/c/sbi-cookies.json')), passkey.SignIn(matsui_site, {}, None, None)])
    monkeypatch.setattr(passkey, 'enroll', lambda **kw: calls.append(('enroll', kw)) or {'site': 'matsui', 'stored_in': 'kc'})
    monkeypatch.setattr(passkey, 'login', lambda **kw: calls.append(('login', kw)) or next(results))
    monkeypatch.setattr(passkey, 'forget', lambda **kw: calls.append(('forget', kw)))
    cli.main(['enroll', '--site', 'matsui', '--login-url', 'https://x/', '--chrome', '/c', '--profile-dir', '/p', '--replace'])
    assert calls[-1] == ('enroll', {'site': 'matsui', 'login_url': 'https://x/', 'chrome': Path('/c'), 'profile_dir': Path('/p'), 'replace': True})
    assert json.loads(capsys.readouterr().out) == {'site': 'matsui', 'stored_in': 'kc'}
    cli.main(['enroll'])
    assert calls[-1][1]['site'] is None and calls[-1][1]['replace'] is False
    capsys.readouterr()
    cli.main(['login'])
    assert calls[-1] == ('login', {'remember': True, 'login_url': None, 'launch_url': None, 'passkey_button': None, 'chrome': None,
                                   'profile_dir': None, 'headless': False})
    err = capsys.readouterr().err
    assert 'Signed in to SBI Securities; session cookies saved to /c/sbi-cookies.json.' in err and 'no data client' not in err
    cli.main(['login', '--no-remember', '--headless', '--launch-url', 'https://b/', '--passkey-button', 'Passkey'])
    assert calls[-1][1]['remember'] is False and calls[-1][1]['headless'] is True and calls[-1][1]['launch_url'] == 'https://b/'
    err = capsys.readouterr().err
    assert 'Signed in to Matsui Securities; session cookies not saved.' in err and 'no data client for this site yet' in err
    cli.main(['forget'])
    assert calls[-1] == ('forget', {})
    assert 'stays registered at your broker' in capsys.readouterr().err


def test_no_command_names_a_broker(capsys):
    with pytest.raises(SystemExit):
        cli.main(['sbi', 'login'])
    assert 'invalid choice' in capsys.readouterr().err


def test_cli_reports_passkey_errors_without_a_traceback(monkeypatch):
    monkeypatch.setenv('BRISK_PASSKEY_STORE', 'file')
    with pytest.raises(SystemExit) as stopped:
        cli.main(['login'])
    assert str(stopped.value.code) == 'brisk: error: No passkey is saved. Run `brisk enroll` first'
    with pytest.raises(SystemExit) as stopped:
        cli.main(['enroll', '--site', 'nowhere'])
    assert str(stopped.value.code).startswith("brisk: error: Unknown site 'nowhere'")


# ---- everything together: Python, the Node host, a real Chrome and a fake broker ----

def chrome_available():
    done = subprocess.run(['node', '-e', f'require({str(REPO / "briskapi/decoder/passkey.cjs")!r}).findChrome()'], capture_output=True)
    return done.returncode == 0


needs_chrome = pytest.mark.skipif(shutil.which('node') is None or not chrome_available(), reason='needs Node and Chrome')


class FakeBroker:
    """The Node fake SBI as a subprocess: a main site plus a BRiSK host, which verifies WebAuthn like a real site."""

    def __enter__(self):
        self.process = subprocess.Popen(['node', str(REPO / 'tools' / 'brisk_mock' / 'fake_sbi_passkey.cjs')],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.origins = json.loads(self.process.stdout.readline())
        return self

    def __exit__(self, *exc):
        self.process.stdin.close()
        self.process.wait(timeout=15)

    def state(self):
        with urllib.request.urlopen(self.origins['mainOrigin'] + '/__state', timeout=10) as response:
            return json.load(response)

    def registered(self):
        for _ in range(200):
            if self.state()['registered']:
                return
            time.sleep(0.05)
        raise AssertionError('the site never saw the registration')


def define(tmp_path, *entries):
    path = tmp_path / 'config' / 'brisk' / 'sites.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'sites': list(entries)}))


@needs_chrome
def test_full_stack_a_site_you_define_enroll_then_login(tmp_path):
    with FakeBroker() as broker:
        main = broker.origins['mainOrigin']
        define(tmp_path, {'id': 'fake', 'name': 'Fake Broker', 'login_url': main + '/login', 'cookie_host': 'brisk.localhost',
                          'passkey_button': 'パスキー認証でログイン', 'cookie_prefix': 'session_'})
        store = passkey.FileStore(tmp_path / 'passkey.json')
        summary = passkey.enroll(site='fake', store=store, login_url=main + '/enroll', headless=True, confirm=broker.registered)
        assert summary['site'] == 'fake' and summary['rp_id'] == 'main.localhost'
        enrolled = store.load()['credential']
        assert enrolled['privateKey'] and enrolled['isResidentCredential'] is True

        signin = passkey.login(store=store, headless=True)
        seen = broker.state()
        assert signin.site.id == 'fake' and signin.client is None and sbi._client is None
        assert signin.cookies['session_fake'] == seen['briskCookieValue'] and 'main_session' not in signin.cookies
        assert json.loads(signin.saved_to.read_text()) == signin.cookies and signin.saved_to.name == 'fake.json'
        assert seen['accepted'] == 1
        assert store.load()['credential']['signCount'] > enrolled['signCount']
        assert store.load()['credential']['signCount'] == seen['counter'], 'the saved counter must be the one the site last saw'

        again = passkey.login(store=store, headless=True, remember=False)
        assert broker.state()['accepted'] == 2 and again.cookies['session_fake'] == broker.state()['briskCookieValue']


@needs_chrome
def test_full_stack_sbi_signs_in_and_its_data_client_gets_the_cookies(tmp_path, monkeypatch):
    monkeypatch.setattr(sbi, 'ORIGIN', 'https://brisk.localhost')
    with FakeBroker() as broker:
        main = broker.origins['mainOrigin']
        define(tmp_path, {'id': 'sbi', 'login_url': main + '/login', 'cookie_host': 'brisk.localhost'})
        store = passkey.FileStore(tmp_path / 'passkey.json')
        passkey.enroll(site='sbi', store=store, login_url=main + '/enroll', headless=True, confirm=broker.registered)
        signin = passkey.login(store=store, headless=True)
        assert signin.site.id == 'sbi' and sbi._default() is signin.client
        assert signin.client.session.cookies['session_fake'] == broker.state()['briskCookieValue']
        assert signin.saved_to == sbi.cookies_path() and broker.state()['accepted'] == 1
