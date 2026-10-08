BRiSK is a product of its respective owners. This independent project is not
endorsed by BRiSK, Tachibana, SBI, TSE or JPX.

The pinned public demo at https://next-demo.brisk.jp/ supplies proprietary WASM
and historical market data. Those files are fetched at runtime, are not included
here and are not covered by this repository's MIT software license.
Recording redistribution depends on the applicable data/provider permissions.

The SBI BRiSK session (`briskapi/sbi.py`) uses the endpoint sequence documented by
pybrisk (https://github.com/obichan117/pybrisk), Copyright (c) 2026 obichan117,
MIT License; its notice ships as `briskapi/LICENSE-pybrisk.txt`. The live transport
(`briskapi/decoder/engineio.cjs`, `sbi.cjs`) also relies on pybrisk's published
protocol research (the Engine.IO/Socket.IO layer, frame types, heartbeat and the
market token) and on the public Next demo's client behavior. briskapi's own calls
replace pybrisk's interface. No vendor bundles or raw captures from that
repository are bundled here. SBI BRiSK's decoder is downloaded at runtime
with the user's own session and is never stored or redistributed by this project.
Dependencies retain their own licenses. Cargo.lock records Rust dependencies;
boto3 uses Apache-2.0.
