import urllib.parse
import urllib.request

import pytest

import briskapi.cli as cli
from briskapi import _native


@pytest.fixture(autouse=True)
def identity_in_a_file(monkeypatch):
    """Tests never touch the real Keychain or Credential Manager."""
    monkeypatch.setenv('BRISK_IDENTITY_STORE', 'file')


@pytest.fixture(autouse=True)
def data_in_a_temporary_folder(monkeypatch, tmp_path_factory):
    """Spools and kept recordings (briskapi._spool) never reach the real ~/.local/share/brisk."""
    monkeypatch.setenv('XDG_DATA_HOME', str(tmp_path_factory.mktemp('data')))


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    """No test reaches the archive (or any other remote host); local test servers stay reachable."""
    reach = urllib.request.urlopen

    def local_only(request, *args, **kwargs):
        url = request.full_url if isinstance(request, urllib.request.Request) else request
        if urllib.parse.urlsplit(url).hostname not in {'127.0.0.1', 'localhost', '::1'}:
            raise RuntimeError(f'Tests must not reach the network ({url}); patch urlopen or the archive call')
        return reach(request, *args, **kwargs)
    monkeypatch.setattr(urllib.request, 'urlopen', local_only)


@pytest.fixture(autouse=True)
def untold(monkeypatch):
    """Each test starts in a fresh process's state: the upload-by-default notice not yet shown."""
    monkeypatch.setattr(cli, '_told', False)


@pytest.fixture
def checkpoints(monkeypatch):
    """A checkpoint after every few book changes of an issue (a recording's holds one after a thousand), in Python and in
    brisk-parquet, which has the number from the environment (a hidden setting: BRISK_PARQUET_CHECKPOINT_CHANGES)."""
    from briskapi import _parquet, schema
    monkeypatch.setattr(schema, 'CHECKPOINT_CHANGES', 4)
    monkeypatch.setattr(_parquet, 'CHECKPOINT_CHANGES', 4)
    monkeypatch.setenv('BRISK_PARQUET_CHECKPOINT_CHANGES', '4')


def _program_missing():
    """Why brisk-parquet cannot be run, or None."""
    try:
        _native.binary()
    except RuntimeError as error:
        return str(error)


BUILD = ('From a source checkout, build it first (CONTRIBUTING.md):\n'
         '    cargo build --release --locked --manifest-path rust/brisk_parquet/Cargo.toml\n'
         'or set BRISK_PARQUET to a program built elsewhere (an installed wheel holds one in briskapi/_bin).')


def pytest_report_header(config):
    """Most tests run brisk-parquet, and without it a feed turns the missing program into a "Packaging failed" warning, so the
    tests that need it fail on what that leaves out. Say what is wrong at the top, and again under the failures. Nothing is
    stopped: a run of tests that do not need it (the Windows passkey job's) is fine without."""
    if _program_missing():
        return 'WARNING: brisk-parquet is not built, so the tests that run it will fail. ' + BUILD.replace('\n', ' ')


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if terminalreporter.stats.get('failed') and _program_missing():
        terminalreporter.write_sep('=', 'brisk-parquet is not built')
        terminalreporter.write_line(BUILD)
