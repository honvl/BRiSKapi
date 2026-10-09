"""The list of BRiSK sites you can sign in to: built-in brokers and your own sites.json."""
import json
from urllib.parse import urlsplit

import pytest

from briskapi import sites


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))


def write(tmp_path, document):
    path = tmp_path / 'sites.json'
    path.write_text(document if isinstance(document, str) else json.dumps(document))
    return path


def test_the_built_in_sites_are_four_brokers_and_only_sbi_has_a_data_client():
    assert list(sites.BUILTIN) == ['sbi', 'matsui', 'monex', 'smbcnikko']
    for site in sites.BUILTIN.values():
        assert site.cookie_host == f'{site.id}.brisk.jp'
        assert urlsplit(site.login_url).scheme == 'https' and site.passkey_button and site.source == 'built in'
    assert {site.id for site in sites.BUILTIN.values() if site.client} == {'sbi'}
    assert sites.BUILTIN['sbi'].cookie_prefix == 'session_'


def test_without_a_sites_file_the_built_ins_are_all_there_is(tmp_path):
    assert sites.load_sites(tmp_path / 'missing.json') == sites.BUILTIN
    assert sites.load_sites() == sites.BUILTIN  # the default path is under XDG_CONFIG_HOME
    assert sites.sites_path() == tmp_path / 'config' / 'brisk' / 'sites.json'


def test_a_new_site_needs_a_login_url_and_a_cookie_host(tmp_path):
    path = write(tmp_path, {'sites': [{'id': 'mybroker', 'login_url': 'https://broker.example/login',
                                       'cookie_host': 'mybroker.brisk.jp', 'passkey_button': 'Use a passkey',
                                       'cookie_prefix': 'sess', 'launch_url': 'https://mybroker.brisk.jp/'}]})
    found = sites.load_sites(path)
    assert list(found) == [*sites.BUILTIN, 'mybroker']
    mine = found['mybroker']
    assert (mine.name, mine.passkey_button, mine.cookie_prefix, mine.launch_url, mine.client, mine.source) == (
        'mybroker', 'Use a passkey', 'sess', 'https://mybroker.brisk.jp/', None, 'sites.json')
    path = write(tmp_path, {'sites': [{'id': 'bare', 'name': 'Bare', 'login_url': 'http://x.example/', 'cookie_host': 'x.example'}]})
    assert sites.load_sites(path)['bare'].passkey_button == 'パスキー'
    with pytest.raises(sites.SiteError, match=r'\(bare\): a new site needs login_url and cookie_host'):
        sites.load_sites(write(tmp_path, {'sites': [{'id': 'bare'}]}))
    with pytest.raises(sites.SiteError, match='a new site needs cookie_host'):
        sites.load_sites(write(tmp_path, {'sites': [{'id': 'bare', 'login_url': 'https://x.example/'}]}))


def test_a_built_in_site_is_edited_by_giving_only_the_keys_that_change(tmp_path):
    path = write(tmp_path, {'sites': [{'id': 'matsui', 'passkey_button': 'Passkey login', 'login_url': 'https://www.matsui.co.jp/new/'}]})
    edited = sites.load_sites(path)['matsui']
    assert (edited.passkey_button, edited.login_url) == ('Passkey login', 'https://www.matsui.co.jp/new/')
    assert edited.cookie_host == 'matsui.brisk.jp' and edited.name == 'Matsui Securities' and edited.source == 'sites.json'
    sbi = sites.load_sites(write(tmp_path, {'sites': [{'id': 'sbi', 'cookie_host': 'other.example'}]}))['sbi']
    assert sbi.client == 'sbi', 'editing a site must not remove its data client; the host guard handles the mismatch'


@pytest.mark.parametrize('document, message', [
    ('not json', 'is not valid JSON'),
    ([], 'must be an object with a "sites" list'),
    ({'sites': 'x'}, 'must be an object with a "sites" list'),
    ({'sites': ['x']}, 'each site must be an object'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'client': 'sbi'}]}, 'unknown key.*client'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'typo': 1}]}, 'unknown key.*typo'),
    ({'sites': [{'id': 'Bad Id', 'login_url': 'https://a/', 'cookie_host': 'a.x'}]}, 'id must be lower-case'),
    ({'sites': [{'login_url': 'https://a/', 'cookie_host': 'a.x'}]}, 'id must be lower-case'),
    ({'sites': [{'id': 'a', 'login_url': 'ftp://a/', 'cookie_host': 'a.x'}]}, 'login_url must be an http'),
    ({'sites': [{'id': 'a', 'login_url': 'nope', 'cookie_host': 'a.x'}]}, 'login_url must be an http'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'launch_url': 'file:///etc/passwd'}]}, 'launch_url must be an http'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'https://a.x/path'}]}, 'cookie_host must be a host name'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 5}]}, 'cookie_host must be a host name'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'name': ' '}]}, 'name must be non-empty text'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'passkey_button': 3}]}, 'passkey_button must be non-empty text'),
    ({'sites': [{'id': 'a', 'login_url': 'https://a/', 'cookie_host': 'a.x', 'cookie_prefix': 3}]}, 'cookie_prefix must be text'),
])
def test_a_bad_sites_file_is_refused_with_a_clear_reason(tmp_path, document, message):
    with pytest.raises(sites.SiteError, match=message):
        sites.load_sites(write(tmp_path, document))


def test_an_unknown_site_lists_the_choices():
    with pytest.raises(sites.SiteError, match=r"Unknown site 'nope'. Choose one of: sbi, matsui, monex, smbcnikko \(see `brisk sites`\)"):
        sites.get_site(sites.BUILTIN, 'nope')
    assert sites.get_site(sites.BUILTIN, 'monex').name == 'Monex Securities'
