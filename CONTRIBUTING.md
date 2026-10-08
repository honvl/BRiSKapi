# Contributing

Software contributions: open a pull request with the behavior change, relevant
verification and updated docs. Keep vendor assets, captures, account/session
material, build products and cloud credentials out of Git. Do not commit AWS
upload tickets; they are short-lived bearer credentials.

Recording contributions are automatic once a user accepts the one-time prompt
(`brisk consent`); see `PRIVACY.md`. Any change to what is collected must update
that policy and bump `POLICY_VERSION` in `briskapi/cli.py`, so saved choices lapse
and users are asked again. No maintainer approval is required for recordings: the
service checks canonical encoding, the schema, integrity, continuity, the
reference replay and the contributor declaration, then publishes its own
compression of accepted recordings. Do not include account, authentication or
order information. The accepted decoded schema is documented in
`briskapi/schema.py`; it contains market/auction data and local timing only.

Data quality metadata includes source, JST trading date and source-time range,
security coverage, batches, quote count and expanded size. The SHA-256 refers
to the published gzip object, which the service creates deterministically.
Distinct receipt clocks remain distinct recordings; identical payloads
deduplicate by hash. `briskapi/references/historical_mock.json` pins the demo replay; it
changes only with a re-audited asset pin (`tools/brisk_mock/build_reference.py`).

## Development

```sh
.venv/bin/python -m pip install -r requirements-dev.txt -e .
.venv/bin/python -m pytest tests \
  --cov=archive_service --cov=briskapi --cov-fail-under=85
.venv/bin/python tools/brisk_mock/download_mock.py --cache /tmp/brisk-mock-cache
BRISK_MOCK_CACHE=/tmp/brisk-mock-cache .venv/bin/python -m pytest tools/brisk_mock/test_reference.py
BRISK_MOCK_CACHE=/tmp/brisk-mock-cache node --test tools/brisk_mock/*.test.cjs
BRISK_MOCK_CACHE=/tmp/brisk-mock-cache cargo test --locked \
  --manifest-path rust/brisk_quote_ingest/Cargo.toml
cargo fmt --manifest-path rust/brisk_quote_ingest/Cargo.toml --check
cargo clippy --locked --manifest-path rust/brisk_quote_ingest/Cargo.toml --all-targets -- -D warnings
```

Ordinary tests are local and never touch the cloud. `BRISK_MOCK_CACHE` opts into
fixture replay. CI runs all of the above. Aim for at least 85% coverage of new
behavior.

Keep `README.md` and `README.ja.md` in step; internals belong in `ARCHITECTURE.md`.
Everything the installed package needs at runtime lives in `briskapi/` (package
data is listed in `pyproject.toml`); CI installs the wheel outside the repository
and runs it. SBI tests use pybrisk's sample payloads and a fake server. SBI market data
must never reach the archive; SBI sessions contribute timing reports only
(`briskapi/timing.py`, validated by `validate_timing` in `briskapi/schema.py`).

## Releases

1. Once, on pypi.org: add a trusted publisher for project `briskapi`, repository
   `honvl/BRiSKapi`, workflow `release.yml`, environment `pypi`. In the GitHub
   repository settings, create the `pypi` environment.
2. Set `version` in `pyproject.toml`, commit, then push a matching tag
   (`git tag v0.2.0 && git push origin v0.2.0`).

`.github/workflows/release.yml` tests and builds the wheel and sdist, builds the
Rust tools for Linux (x86-64, ARM64), macOS (Apple silicon) and Windows with
their decoder host, creates the GitHub Release with checksums and publishes to
PyPI. Running the workflow manually is a dry run that publishes nothing.

`cloud_smoke.py` is an opt-in real cloud test against the configured archive. It
publishes only the fixed synthetic fixture and checks that tampered content is
rejected. Archive changes reach contributors only after `infra/deploy.py` runs
(see `infra/README.md`).
