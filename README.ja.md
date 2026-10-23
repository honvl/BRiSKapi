# briskapi

[English](README.md) | [日本語](README.ja.md) | [ドキュメント](https://honvl.github.io/BRiSKapi/ja/)

BRiSK の板寄せデータを扱う、非公式の Python API と `brisk` コマンドラインツールです。
Databento 風の `Live` と `Historical` クライアントを備えています。ライブフィードの購読、記録データの任意時点での照会、
自分専用の非公開アーカイブへの保存に加え、ご自身の口座で証券会社（SBI証券、立花証券
e支店の BRiSK Next、松井証券、マネックス証券、SMBC日興証券）の BRiSK も利用できます（パスキーでのサインインにも対応）。本プロジェクトは独立したもので、BRiSK、各証券会社、
東京証券取引所（TSE）、日本取引所グループ（JPX）とは提携しておらず、承認も
受けていません。

> **データ源:**
> - **口座不要:** 2021年9月27日の公開 BRiSK Next デモ（寄り前のスナップショット
>   1件と寄付から3分間）を、記録時のペースで再生したもの。リアルタイムの市場
>   データではありません。
> - **お使いの証券会社の BRiSK（SBI証券、立花証券 e支店の BRiSK Next、松井証券、
>   マネックス証券、SMBC日興証券）を使える場合:** BRiSK の市場データ（ローソク足、信用残、アラート、取引
>   スケジュール、ウォッチリスト）と、ライブフィード。

## どの API を使うか

| したいこと | 使うもの | 読むもの |
| --- | --- | --- |
| 気配・約定・板寄せをリアルタイムで | `briskapi.connect()` のあと `Ticker`/`Market` | ライブフィード |
| 同じものを Databento 風のレコード（`trades`、`mbp-10`、`imbalance` など）として、このコンピューター上のどのプログラムからでも | `briskapi.Live()` | このコンピューターで `brisk live` が動かしているフィード（自身のログインは不要） |
| 手元の記録をオフラインで、または NautilusTrader で | `briskapi.load(dir)` か `briskapi.pull(prefix)` のあと `Ticker`/`Market`、`brisk export` | その記録（記録したとおり） |
| 複数日にわたる履歴や OHLCV バーを、ダウンロードせずに | `briskapi.Historical()`、`brisk query` | クラウドにある自分のアップロード（デモは誰でも） |
| BRiSK 自身のローソク足、信用残、売買代金、リスト、イベント | `Ticker.candles()`、`.margin()`、`Market().turnover()` など | 証券会社の BRiSK のサーバー（ログインが必要） |
| アップロードしたものの管理 | `briskapi.Archive()`、`brisk list`、`pull`、`delete`、`restore`、`prune` | 自分専用のアーカイブ |

記録はこの表にありません。`brisk live`、`brisk record` とその Python 版が動きながら記録し、
記録したものを上の呼び出しで読みます（[記録する](#記録する)）。

`Ticker.history()` と `.trades()` はネットワークを使いません。読み込んだ記録かライブフィードを
読みます。
- 記録なら、その記録の範囲がそのまま返ります。
- ライブフィードは `history=True` が必要です。接続してからの更新をすべて保持し、約定は BRiSK の
  スナップショットが復元できる範囲でその日の始めまでさかのぼります。

証券会社の BRiSK に問い合わせるのは `Ticker.candles()` です。`5m` は当日のみで、`1d`・`1w`・
`1mo` がどこまでさかのぼるかは BRiSK 次第で、まだ分かっていません。自分の記録から複数日の
バーがほしい場合は `Historical`（`ohlcv-1s` 〜 `ohlcv-1d`）を使ってください。

手元の呼び出しとクラウド API では、同じデータでも名前が異なります（たとえば手元の
`indicative_price` はクラウドでは `ind_match_price`）。対応表は
[docs/data-format.ja.md](docs/data-format.ja.md#手元とクラウドの名前)にあります。`briskapi.Live` は
クラウド API の名前とレコードを使います。

## インストール

フリースレッド版の Python 3.14（`python3.14t`）と Node 22 以上（または Bun 1.4 以上。Bun が入っている Mac では BRiSK は Bun を使います）が
必要です。フィードは全銘柄を記録します。
全銘柄の寄付では数秒のうちに何千もの板が変わるため、読み込み・解析・書き込みを複数のコアで同時に行いますが、
Python がそうできるのは GIL がない場合だけです（GIL のある Python でも動きますが、寄付で遅れることがあると
警告します）。BRiSK 自身のデコーダーは Node 上で動く WebAssembly モジュールです。

```sh
pip install 'briskapi[pandas]'      # `import briskapi` と `brisk` コマンド
```

BRiSK のデコーダーとデモデータは実行時にダウンロードされ、パッケージには
含まれていません。Linux、macOS、Windows 向けのビルド済み Rust ツール
（`brisk_quote_ingest`、`brisk_recording`）は任意で、各
[GitHub リリース](https://github.com/honvl/BRiSKapi/releases)に添付されています。
ソースから使う場合は、リポジトリをクローンして `pip install -e '.[pandas]'` を
実行してください。

Windows では PowerShell から、Windows 側の Python と Node を使います。

```powershell
py install 3.14t
py -3.14t -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e '.[pandas]'
.\.venv\Scripts\brisk.exe live --site demo --codes 7203 --speed 0 --limit-frames 3 --no-upload --quotes
```

## 記録する

記録は3つ目の API ではありません。ライブの呼び出しが動きながら記録し、`brisk live` と
`brisk record` はそのコマンドライン版です（コード、オプション、アップロードの選択は同じです）。

| コマンドライン | Python | 記録するもの | ディスクに残るもの | アップロード |
| --- | --- | --- | --- | --- |
| `brisk live --site sbi`（または `e-shiten`、`matsui`、`monex`、`smbcnikko`） | `briskapi.connect(site="sbi")` | 証券会社のライブフィード。止めるまで、各取引日を通常2つの記録（前場と後場）として | `--output DIR`（`output=`）を指定すると、記録ごと | できた記録から順に（オプトアウトした場合を除く） |
| `brisk live --site demo` | `briskapi.connect(site="demo")` | デモの再生（JSON の行として出力） | `--output DIR` を指定した場合 | 再生が最後まで終わった場合 |
| `brisk record --output DIR` | `briskapi.record(DIR)` | デモの再生 | 常に `DIR` に | 再生が最後まで終わった場合 |

記録したものは、残りの2つの API で読みます。
- **手元で:** `briskapi.load("days/sbi_20261013_085800-113000")` のあと `Ticker` と `Market`
  （記録したとおり）。NautilusTrader や JSONL には `brisk export`、または Parquet のテーブルを直接
  読みます（[データ形式](docs/data-format.ja.md)）。アップロードした記録は `brisk pull`
  （`briskapi.pull()`）でディスクに戻せます。
- **クラウドで:** `briskapi.Historical()` または `brisk query XTKS.BRISK` は、アップロードした
  証券会社の記録の全日を日をまたいでまとめて読み、OHLCV バーも返します。デモの記録は
  `XTKS.BRISK` に入りません。同じデモは誰でも `XTKS.BRISKDEMO` として照会できます。

動いている間、`brisk live` はフィードをこのコンピューターの `briskapi.Live()` にも配信します
（[ライブのレコード](#ライブのレコードdatabento-風)。記録ではなく、いま流れているレコードです）。

日の途中も、届いたそばからディスクに書き込まれます。スプールフォルダー（`~/.local/share/brisk/spool`）の
Parquet ファイルで、大きさは完成した記録とほぼ同じなので、クラッシュしても失われるのは最後の30秒ほどだけです。
証券会社の日は、動いている間に通常2つの記録になります。前場の記録は昼休みに入った時点で保存・アップロードされ、
後場の記録は前場の記録が終わった時点の板のスナップショットから始まり、日が終わった時点で保存・アップロードされます。
更新が多いセッションは、書き終えたスプールのパートで気配が8,000万行に達すると、さらに記録を分けます。
デモの再生は1つの記録です。

- **`--output` を指定しない場合**、記録は[自分のアーカイブ](#自分専用のアーカイブ)に送られ、手元の
  コピーは削除されます。さらにアップロードもオフにすると（`--no-upload`、または証券会社の市場データの記録なら
  `--no-upload-market-data`）、その日はどこにも残らないので、残すには `--output DIR` を付けてください。
- **接続が切れても**、その日は失われません。`brisk live --site sbi`（またはほかの証券会社のサイト）は、5秒後、続いて
  10秒、30秒、1分、以後2分ごとに自動で再接続します。切れる前に記録した分はそれだけで完結した記録
  （`..._085800-101500`）として、日と同じように保存・アップロードされ、その日の残りは新しいスナップショットから
  記録されます（`..._102000-153512`）。欠けるのは途切れていた間だけで、クラウド API は2つをまとめて返します。
  その間を別のフィードが記録していれば、`brisk repair` で埋められます
  （[別のフィードの記録で補修する](#別のフィードの記録で補修する)）。再接続しても直らない場合（セッションの期限切れ、
  自分のアカウントで BRiSK を別の画面で開いた、サイトが変わった）は、そこで止まります。
- **止めた場合**（Ctrl-C、`kill`、コンピューターのシャットダウン、`feed.close()`）、その日はそこで、取引終了と
  同じように終わります。それまでの分は完結した記録（`..._085800-121500`）として、ほかの日と同じように保存・
  アップロードされます。あとで再び起動すれば、その日の残りは別の記録になり、クラウド API が2つをまとめます。
- **クラッシュや強制終了**ではその日を終えられないので、修復されます。その日のそれまでの分（スプールにある、
  最後の30秒ほどを除くすべて）に、ホストが書けなかった終わりの印を付け、そのセッションの設定どおりに保存・
  アップロードします。クラッシュの場合は次にフィードを起動したとき、止めるよう求めてから30秒以内に終わらなかった
  ホストの場合はすぐに行います。修復した終わりの印は、その日の気配の更新数を正確に数えます。ホストとともに
  失われたフレーム数と所要時間は、記録から組み立て直します。
- **保存に失敗した場合**も、残せるものは残します。アップロードに失敗した記録はマニフェストとともに
  `~/.local/share/brisk/recordings` に残り、`brisk upload DIR` でやり直せます。パッケージにできなかった記録は、
  同じデータから5分後にもう一度試し、日中に3回まで、日の終わりにもう一度試します。それでも失敗すれば、
  そのテーブルだけを同じ場所に残します。その日の記録を何も残せなかった場合は、スプールのフォルダーをそのまま残して
  警告でその場所を知らせ、次にフィードを起動したときに仕上げます（そのフォルダーを消せば、その日はあきらめたことになります）。

## ライブフィード

```python
import briskapi

feed = briskapi.connect(site="demo", codes=["7203", "6758"])   # 初期状態を受信してから戻る
toyota = briskapi.Ticker("7203")
toyota.quote()        # 現在の気配（フレームが届くたびに更新）
toyota.auction()      # 予想約定価格・数量と成行注文の売買差

feed.on_quote(lambda q: print(q["code"], q["indicative_price"]), codes="6758")
for q in feed.quotes("7203"):       # 更新ごとに1件。セッションが終わると終了
    if q["last_price"]:
        print("寄付:", q["last_price"], q["time"])
        break

briskapi.Market().imbalances(top=10).to_pandas()
feed.wait()           # または feed.close()。`with briskapi.connect(...) as feed:` も使えます
```

`briskapi.connect()` は、`brisk live --site` と同じく、サイトを1つ取ります。
- `site="demo"`: 公開の BRiSK Next デモ。ライブの接続ではありません。デモサイトは2021年9月27日の記録を
  ファイル（BRiSK のデコーダー、マスター、スナップショット、ストリーム）として配信しており、それを
  ダウンロードし、固定したハッシュと照合して、記録されたペースで再生します（`speed`: `1` で実時間、
  `0` で最速。`limit_frames` で途中で止める）。`cache=DIR` を指定すると、以前ダウンロードした同じファイルの
  コピーから再生します。
- `site="sbi"`、`"e-shiten"`、`"matsui"`、`"monex"`、`"smbcnikko"`: お使いの証券会社のライブの BRiSK（[下記](#お使いの証券会社の-brisk)）。

そのほかのオプション: `codes`（銘柄コード）、`history=True`（`Ticker.history()` のために更新を保持）、
`upload`、`output`、`on_day`、`gateway`。コールバックと
イテレーターは、まず各銘柄の現在の気配を受け取り、その後すべての更新を順番
どおりに受け取ります。受け取る側の処理が遅い場合、更新を捨てずにフィードの
ほうが待ちます。

## ライブのレコード（Databento 風）

`brisk live` は、そのフィードをこのコンピューター上の他のプログラムにも、クラウド API が返すのと
同じレコードとして配信します。`briskapi.Live` は Databento のライブクライアントと同じ形で、
キーもサインインも要りません。証券会社にサインインするのはフィードだけです。

```sh
brisk live --site sbi   # 止めるまで記録・アップロードし、XTKS.BRISK を配信（--site demo ならデモの XTKS.BRISKDEMO）
```

```python
import briskapi

live = briskapi.Live()          # このコンピューターで動いているフィードを見つける
live.subscribe(dataset="XTKS.BRISK", schema="imbalance", symbols=["7203", "6758"])
live.subscribe(dataset="XTKS.BRISK", schema="trades", symbols=["7203"], start=0)   # その日のここまで、続けてライブ
live.subscribe(dataset="XTKS.BRISK", schema="ohlcv-1m", symbols=["7203"])          # 1分ごとのバーを、その1分が終わりしだい
for record in live:             # または live.add_callback(fn); live.start(); live.block_for_close()
    print(record.schema, record.symbol, record.ts_event)
```

- **スキーマ:** `trades`、`mbp-10`、`imbalance`、`definition`、`brisk-quotes`、OHLCV バーの
  `ohlcv-1s` 〜 `ohlcv-1d`。同じセッションなら `Historical` が返すのと同じ列・同じ値です。`Live` は
  受け取った約定から、クラウドと同じ規則でバーを作ります。
- **バー**は、その区間がフィードの時計で終わりしだい届きます。終わりより後の時刻のバッチが届いたとき、
  またはこのコンピューターの時計から、BRiSK からこのセッションまでに測った遅延（直近10秒の最大値）を
  引いた時刻が終わりを過ぎたときです。自分のバーが終わったあとに届いた約定は、まだ終わっていない次の
  バーに入ります（クラウドでは約定自身の時刻のバーに入ります）。どの約定も数えられ、バーは順に届きます。
  バーは、セッションがその取引時間内の全期間を見ている場合にだけ渡します。
  `start` を指定すればフィードの開始から、指定しなければ購読より後に始まるバーです。
- **セッションの始まり:** その日の銘柄定義、続いて届いた更新を順に受け取ります。`start`（時刻、
  日付、UTC ナノ秒、またはフィードが持つすべてを表す 0）を指定すると、まずフィードのその取引日を
  その時点から再生します。フィードが持っているのは、起動してからの分と、スナップショットが復元
  したその日の約定です。`snapshot=True` を指定すると、`mbp-10`、`imbalance`、`brisk-quotes` を
  各銘柄の現在の気配から始めます（`flags` は 32）。
- **レコード**は辞書で、属性としても読めます（`record.price`）。スキーマは `record.schema` です。
  フィードがバッチを受け取った順に届き、バッチ内は `Historical` と同じ順です。
  `add_callback(fn, exception_callback)` と `add_stream(path)`（JSON Lines。Databento では DBN）
  でも受け取れます。`block_for_close(timeout)` はセッションの終わりを待ち、`stop()` は終わらせます。
  購読は `start()` の前でも後でもかまいません。後からの購読にもそれぞれの再生やスナップショットがあり、
  その銘柄はその時点から届きます。1つのセッションの購読は同じデータセットを共有します。
  `symbology_map` は `instrument_id` から銘柄コードへの対応です。
- **高速レーン:** 証券会社のフィードは全銘柄を記録するため、遅めのコンピューターでは寄付で
  ストリームから数秒遅れることがあります。`start` を付けずに銘柄を指定したセッションは高速レーンで
  受け取ります。フレームが届くたびに読み、記録より先に、同じレコードを届けます。高速レーンが扱えるのは
  そのワーカー1つにつき約 500 銘柄です（フィードのワーカーの4分の1なので、既定では 500〜1,000 銘柄）。
  `ALL_SYMBOLS`、`start` による再生、扱える数を超える銘柄は、記録と同じ速さで届きます（警告で、その旨と
  必要な数が表示されます）。後の購読で収まらなくなったセッションは、レコードを欠かすことも重複させることも
  なく、そちらに移ります。1,000 銘柄以上のシステムでは、`brisk live --site sbi --fast-symbols 1500`
  （`briskapi.connect(fast_symbols=1500)`）でフィードを起動すると、高速レーンにその銘柄数のワーカーが
  割り当てられます。500 銘柄ごとに記録用のワーカーを 1 つ使うので、コアの少ないコンピューターでは
  `BRISK_WORKERS` も指定してください。
- **多数の銘柄の最新の気配を一度に:** 決まった時刻（オークションの数秒前の締め切りなど）に判断するプログラムが
  必要なのは、その時点の各銘柄の最新の気配であって、途中のすべての更新ではありません。
  `live.subscribe("XTKS.BRISK", "mbp-10", symbols=codes, stream=False)` は、フィードにそれらの銘柄を高速レーンで
  新しく保たせ、何も送りません。`live.latest()` は 1 回のリクエストで、それらの最新の気配を pyarrow のテーブルで
  返します（`mbp-10`、`imbalance`、`brisk-quotes` のレコード。`ts_recv` はフィードがその気配を受け取った時刻なので、
  その古さは `now - ts_recv` です）。1,500 銘柄で約 18 ms で、ストリームが混んでいても変わりません。
  1つのセッションは、ストリームを受け取るか、`latest()` のために気配を保つかのどちらかです。
  `tools/live_age.py` は、ストリームのレコードの古さを、BRiSK 側の遅れと briskapi 自身の分に分けて表示します。
- **このコンピューターだけ:** フィードは 127.0.0.1 でのみ待ち受け、所有者だけが読めるファイル
  （`~/.config/brisk/gateway/XTKS.BRISK.json`）にポート番号とともに保存したキーを持つセッション
  だけを受け入れます。`Live()` はこのファイルを読みます。配信する内容がこのコンピューターの外に
  出ることはありません。1 GB を超えて遅れたセッションは、フィードやその記録を遅らせる代わりに
  切断されます。
- **配信:** `brisk live` は `--no-gateway` を指定しない限り配信します。1つのデータセットを配信できる
  フィードは同時に1つです。Python では `briskapi.connect()` に `gateway=True` を渡します。

## 記録データと自分のアーカイブ

```python
recordings = briskapi.recordings(source="historical_mock")   # 自分の記録だけ。アカウント不要
if recordings:
    briskapi.pull(recordings[0]["prefix"])      # 検証・展開・キャッシュし、既定のデータにする
else:
    briskapi.record("recordings/my-session")   # デモを記録（オプトアウトしない限りアップロード）
# または briskapi.load("recordings/my-session") でローカルの記録フォルダー（以前の events.jsonl[.gz] も可）を読む。

briskapi.Ticker("7203").quote(at="08:59:59.9999")             # 寄り前の気配（日本時間）
briskapi.Ticker("7203").history(start="09:00", end="09:01")   # 期間内のすべての更新
briskapi.Market().snapshot(at="09:00:00").to_pandas()
```

記録は NautilusTrader v2 のカタログと同じ構成の Parquet テーブルのフォルダーです。
`data/order_book_depths.parquet`（すべての気配更新。v2 の `OrderBookDepth` に BRiSK の
ほかの気配項目を加えたもの）、`data/trades.parquet`（すべての約定、v2 の `TradeTick`）、
`data/equity.parquet`（銘柄マスター、v2 の `Equity`）、
`data/custom/BriskBatch.parquet`（フレームごとの計測値）からなります。デモ全体で約
29 MB です。pandas、Polars、DuckDB でそのまま読めます。`brisk export` で
NautilusTrader のカタログや JSONL に書き出せます。全列の説明と pandas・Polars・DuckDB での読み方は
[docs/data-format.ja.md](docs/data-format.ja.md)にあります。

時刻の照会には、バッチではなく各気配のタイムスタンプを使います。デモの
7203 の最初の気配は日本時間 08:59:59.993551 で、それより前は `NotFoundError`
になります。`Market().summary()` はローカルの記録やライブフィードでも `start`
と `end` を返します。アーカイブには自分がアップロードしたものだけが、自分の ID（お使いの
コンピューターで作られ、どこにも登録しないランダムな鍵。[自分専用のアーカイブ](#自分専用のアーカイブ)
を参照）の下に保存されます。一覧では、不正または未対応の記録を警告付きでスキップします。
サンプルが1つだけ、誰でもダウンロードできるよう公開されています。[通信をそのまま記録したもの](https://brisk-recordings-honvl-tokyo.s3.ap-northeast-1.amazonaws.com/samples/20260311/cca870a51e7f96f16009c3709c3f597d314b22ed8922bb3ea7fa1acc67a0f85f/events.jsonl.gz)
（2026-03-11、日本時間 12:12:40 から）で、このツールがもう読まない旧形式
`brisk-sbi-wire-jsonl-v1` です。

## お使いの証券会社の BRiSK

同じクライアントで、SBI証券の BRiSK（[sbi.brisk.jp](https://sbi.brisk.jp)、既定のサイト）と
立花証券 e支店、松井証券、マネックス証券、SMBC日興証券の BRiSK（専用の項が続きます）を使えます。証券会社と BRiSK の契約が
必要です。SBI では、ブラウザで sbi.brisk.jp にログインし、そのセッション Cookie を
渡します。Cookie は DevTools からコピーするか、`pycookiecheat` の
`chrome_cookies("https://sbi.brisk.jp")` で取得できます。パスキーで `briskapi` に
サインインさせることもできます（[下記](#パスキーでのサインイン試験的)）。

```python
briskapi.login(cookies={"session_bfaf77a2": "v2.local..."})   # remember=True で保存（本人のみ読み取り可）
toyota = briskapi.Ticker("7203")
toyota.candles("5m").to_pandas()   # ローソク足: 5分（当日）、日、週、月（5m, 1d, 1w, 1mo）
toyota.margin(days=30)             # 信用残と貸株料
market = briskapi.Market()
market.turnover()     # 全銘柄の売買代金と発行済株式数
market.lists()        # 日経225、直近 IPO など
market.events()       # バスケット注文、ストップ高・安、出来高急増などのイベント
market.schedule()     # 取引日、状態、取引時間
market.watchlist()    # 保存済みの銘柄コード

feed = briskapi.connect(site="sbi", codes=["7203"])   # ライブ。オプトアウトしない限り、前場・後場ごとにアップロード
toyota.quote()                              # 他のフィードと同じ呼び出し
# upload_market_data=False で市場データの記録を、upload=False ですべてをアップロードしない（残すには output="days"）
```

証券会社のフィードは連続して記録し、日をまたいでも動き続けます。各日は通常2つの記録として
パッケージ化・アップロードされ、前場の記録は昼休みに入った時点で、後場の記録（前場が終わった時点の板の
スナップショットから始まります）は日が終わった時点です。更新が多いセッションは、書き終えたスプールのパートで
気配が8,000万行に達すると、さらに記録を分けます（クラウド API では1日として読めます）。サイトのセッションが終了（done）したとき、またはサイトが次の取引日を
配信し始めたとき（10分ごとに確認）、あるいはサーバーが市場の終了後にストリームを閉じたときにその日が終わり、
最後の記録がその日のタイミングの要約とともにパッケージ化・アップロードされ（オプトアウトした場合を除く）、
フィードは次のセッションを待って、新しいスナップショットから記録を始めます。セッションが開いていないとき（取引終了後や休日）に始めると、
フィードは次の取引日のセッションを待ちます。`connect()` はその日が始まってから戻ります
（待っている間の `feed.status` は `"waiting"`）。証券会社は毎朝 05:00 にセッションを失効させるので、
一晩待っているフィードはセッションが失効しても待ち続け（`the session has expired; waiting for new cookies` と表示します）、
その後に行った `brisk login` の Cookie を、再起動なしで使い始めます。その証券会社のパスキーが保存されていれば
（`brisk enroll`）、フィード自身が `brisk login` と同じようにサインインし直します（Chrome が開きます）。試すのは
最大4回、1時間40分の間で、それでだめなら人に任せます。パスキーは証券会社ごとなので、2社のフィードはそれぞれの
パスキーでサインインし、パスキーがない証券会社のフィードは、サインインのしかたを表示します。`output=DIR`
（`briskapi.connect(output=...)`、`brisk live --output DIR`）を指定すると、
記録ごとに、`SITE_YYYYMMDD_FROM-TO`（証券会社のサイトと、その最初と最後のソース時刻、日本時間 HHMMSS）という
名前のフォルダー（例: `sbi_20261013_085800-113000`）が DIR に残ります。どの証券会社の記録も BRiSK の同じ市場データ
なのでデータ源は同じ `brisk_live` で、フォルダー名がどの証券会社から記録したかを示すので、同じセッションを2社で
記録して比べても互いにぶつかりません。`feed.recordings` はその一覧
（`trading_date`、`name`、`path`、`uploaded`、`timing_uploaded`。タイミングの要約は各日の最後の記録に。`repaired`、そして `error` は失敗したのがパッケージ化かアップロードか）で、
`on_day=callback` は記録ごとに呼ばれます。`output` を指定しないと、記録は自分のアーカイブにだけ残り、
アップロードもオフならどこにも残りません。フィードを止めると（`feed.close()`、Ctrl-C）その日はそこで終わり、
残りの分がほかの記録と同じように保存・アップロードされます。接続が切れたときは自動で再接続します
（[記録する](#記録する)）。SBI と e支店の BRiSK は1ユーザー1セッションまでです（ほかの証券会社も同じと考えてください）。フィードを始める前にブラウザの BRiSK を閉じてください。開いたままだと、
数秒のうちに互いを切断します。

結果は下記の規約に従います。エラーは `briskapi.brisk.SessionExpiredError`（再ログインが
必要）、`briskapi.NotFoundError`、`briskapi.brisk.RateLimitError`、`briskapi.brisk.APIError` です。
リクエストは1秒に1回までに制限しています。

ライブフィードは、ご自身のセッションでダウンロードした BRiSK 自身のデコーダーを
Node 上で動かします。ブラウザは使いません。ベンダーのクライアントと同じ接続手順
（最初のフレームからデコーダーに入力し、スナップショットをストリームに追いつかせ、
サーバーの ping にデコーダーを通じて応答し、サーバーのハートビートを監視）に従い
ます。まだ実際の SBI セッションでは検証できていません。ストリームは Next のクライ
アントと同じく素のバイナリ WebSocket と想定しています。サーバーが Socket.IO で始
まる場合は、未検証のパラメーターで `/v2/user` 名前空間に参加します（パラメーター
は `briskapi.connect(site="sbi", profile={...})` で設定できます）。`trace_protocol=True`（トークン
はすべて伏せ字）でサーバーの応答を確認してください。想定外の応答は、推測で動かさ
ず、明示的なエラーで止まります。
結果をぜひお知らせください。実際のセッションでは現在、サインイン、データの取得、
デコーダーのダウンロード、銘柄マスター、スナップショット（セッションのスケジュール
付きで読み込み）までは通ります。その後、`api.brisk.jp` への WebSocket の Upgrade が
HTTP 502 で返されました。それらのセッションは、ベンダーのクライアントが求める
サブプロトコル `flex` を求めていませんでした。このバージョンは求めますが、それで
足りるかはまだ分かっていません。SBI の Cookie は sbi.brisk.jp にのみ送られます。

### 立花証券 e支店の BRiSK Next

e支店の BRiSK Next（[next.e-shiten.brisk.jp](https://next.e-shiten.brisk.jp)）は
同じアプリなので、同じクライアントで使えます。ブラウザでログインし、その Cookie を
`site="e-shiten"` を付けて渡すか、e支店のパスキーで `briskapi` にサインインさせてください
（[下記](#パスキーでのサインイン試験的)）。初回のみ `brisk enroll --site e-shiten`、その後は
`brisk live --site e-shiten` の前に `brisk login` を実行します。

```python
briskapi.login("e-shiten", cookies={...})   # または BRISK_ESHITEN_COOKIES。remember=True で SBI とは別に保存
feed = briskapi.connect(site="e-shiten", codes=["7203"])   # ライブ
```

上記の説明は記録とアップロードも含めてそのまま当てはまります（できた市場データの記録から順に、
そして各日のタイミングの要約をアップロードします）。Cookie は
next.e-shiten.brisk.jp にのみ送られます。e支店の
BRiSK は同時に1セッションまでで、別のタブやブラウザで開くと先に開いていた方が
止まりました（メッセージ 10011）。そのため、フィードとブラウザの BRiSK は互いに
相手を止めることがあります。記録やアップロードの前に、立花証券の BRiSK Next 利用規定で
データの蓄積や二次利用についての定めを確認してください。

### 松井証券、マネックス証券、SMBC日興証券

松井証券（[matsui.brisk.jp](https://matsui.brisk.jp)）、マネックス証券
（[monex.brisk.jp](https://monex.brisk.jp)）、SMBC日興証券
（[smbcnikko.brisk.jp](https://smbcnikko.brisk.jp)）の BRiSK は、同じベンダーのアプリが各社のホストで
動いているものなので、同じクライアントで使えます。`site="matsui"`、`"monex"`、`"smbcnikko"` を付けて、
ブラウザの Cookie、サイトごとの環境変数（`BRISK_MATSUI_COOKIES`、`BRISK_MONEX_COOKIES`、
`BRISK_SMBCNIKKO_COOKIES`。JSON）、またはパスキーでのサインイン（初回のみ `brisk enroll --site matsui`、
その後は `brisk login`）を使います。

```python
briskapi.login("matsui", cookies={...})   # remember=True でほかの証券会社とは別に保存
feed = briskapi.connect(site="matsui", codes=["7203"])   # ライブ
```

上記の説明は記録とアップロードも含めてそのまま当てはまり（データ源はどの証券会社とも同じ `brisk_live` で、
フォルダー名は `matsui_…`、`monex_…`、`smbcnikko_…`）、各サイトの Cookie はそのサイトのホストにのみ送られます。この3社は
まだ口座で試していません。サイトのアプリが SBI のものと違う場合、フィードは疑わしいものを記録せず、
何を認識できなかったかを示すエラーで止まります（`--trace-protocol` で接続を表示）。SBI や e支店と同じく
1ユーザー1セッションまでと考え、先にブラウザの BRiSK を閉じてください。記録やアップロードの前に、
お使いの証券会社の BRiSK の規定でデータの蓄積や二次利用についての定めを確認してください。

## パスキーでのサインイン（試験的）

近年は、パスキーでのサインインを求める証券会社が増えています。お使いの証券会社がそうなら、
Cookie をブラウザからコピーする代わりに、`briskapi` がサインインして BRiSK のセッション
Cookie を保存できます。SBI証券、松井証券、マネックス証券、SMBC日興証券、立花証券 e支店の
BRiSK に対応し、他の証券会社も追加できます。Chrome、Node 22 以上（または Bun 1.4 以上）、そしてパスキーの安全な保管先（macOS のキー
チェーンまたは Windows の資格情報マネージャー。Linux では選択式の本人のみ読み取り可のファイル）が
必要です。macOS と Windows で動作し（Windows では PowerShell か `cmd` から実行し、WSL ではなく Windows 側の
Chrome と Node を使います）、Linux でもテストしています。

### 仕組み

パスキーは1組の鍵です。公開鍵は証券会社が持ち、秘密鍵は通常、スマートフォンやパソコン、
パスワードマネージャーの中にあります。指紋や PIN で秘密鍵が使われ、証券会社が送る
チャレンジに署名することで、本人であることを証明します。`briskapi` はこの仕組みを借ります。
専用の Chrome を起動し、その Chrome が持つ*仮想オーセンティケーター*（スマートフォンの代わりを
務め、確認なしで署名するソフトウェア）が署名します。証券会社のサイトは、その違いを見分け
られません。

1. **初回のみ `brisk enroll`。** 証券会社を選ぶと、別の Chrome ウィンドウがそのログインページで
   開きます（普段の Chrome ではなく、一時的なプロファイルです）。いつもどおりサインインし、
   新しいスマートフォンに登録するときと同じように、証券会社のセキュリティ設定でパスキーを
   登録します。仮想オーセンティケーターがそれを受け取ります。サイトに登録完了と表示された
   ら、ターミナルで Enter を押してください。`briskapi` がパスキーをその証券会社の名前でキー
   チェーン（Windows では資格情報マネージャー）に保存し、Chrome は閉じます。（以前のバージョンが保存した
   1つだけのパスキーは、最初に読み込まれるときにその証券会社の名前に移されます。）
   e支店では、パスキーの前にユーザIDを入力するフォームのため、`enroll` が最初にユーザIDを尋ね、
   パスキーと一緒に保存します。ウィンドウは通常のログイン（ユーザID、暗証番号、電話番号認証）で
   開きます。パスキーはログイン後に表示される登録画面か、お客様情報 › 設定情報 › パスキー認証
   から登録してください。
2. **毎回 `brisk login`。** 保存したパスキーを読み込んだ同じ Chrome が開き、ログインページで
   パスキーのボタンを押すと、仮想オーセンティケーターが署名します。そのウィンドウで証券会社の
   サイトから BRiSK を開いてください（`--launch-url` を指定すれば自動で開きます）。`briskapi` が
   BRiSK のセッション Cookie を読み取って保存し、Chrome を閉じます。
3. **あとは使うだけ。** 5社のどれでも、保存した Cookie を `brisk live --site sbi`（またはその証券会社の
   サイト）と `briskapi.brisk` API が使います。ご自身で追加した証券会社には `briskapi` の
   データクライアントがないため、Cookie はご自身の利用のために保存される
   （`~/.config/brisk/cookies/`）だけです。

スマートフォンやパスワードマネージャーにある既存のパスキーには手を加えません。証券会社の
一覧にパスキーが1つ増えるだけで、セキュリティ設定からいつでも削除できます。

### 注意点

- **保存したパスキーは認証情報です。** 持っている人は誰でも、追加の確認なしにあなたの
  証券口座へサインインできます。パスキーはキーチェーン／資格情報マネージャーにのみ保存され（`BRISK_PASSKEY_STORE=file`
  なら本人のみ読み取り可のファイルに保存しますが、安全性は下がります。Windows ではそのファイルの
  保護はユーザープロファイルのフォルダー（本人と管理者のみ読み取り可）に依存します）、お使いのマシンの外には
  出ず、ログにも表示されません。e支店ではユーザIDも同じ場所に一緒に保存します。`brisk forget` で
  ここから削除できます（複数保存してあるときは `--site` で指定します）。証券会社側でも削除してください。
- **試験的な機能です。** 各証券会社のログインページ、パスキーボタンの文言、BRiSK の起動方法は
  公開ページをもとにしたもので、実際の口座では試していません。`--login-url`、`--passkey-button`、
  `--launch-url` や下記の `sites.json` で上書きでき、うまくいかない点はぜひお知らせください。
- **証券会社に拒否される可能性があります。** Chrome は自動操作されていることをサイトに伝えます。
  利用前に、証券会社の規約でこの使い方が認められているかご確認ください。

### コマンド

```sh
brisk sites                    # 選べる証券会社と BRiSK のアドレス
brisk enroll [--site matsui]   # 証券会社ごとに初回のみ。指定しなければ証券会社を選ぶよう求められます
brisk login [--site matsui]    # 毎回。2社以上のパスキーを保存してあるときは --site を指定します
brisk forget [--site matsui]   # 保存したパスキーを削除
```

パスキーは証券会社ごとに保存されるので、2社目を enroll しても1社目はそのままです（`--replace` は同じ証券会社を
もう一度 enroll し直すときに使います）。登録が1社だけなら、`brisk login` と `brisk forget` に `--site` は要りません。

Python では `passkey.enroll(site="matsui")`（e支店では `passkey.enroll(site="e-shiten", user_id="...")`）
と `signin = passkey.login()`（複数保存してあるときは `passkey.login(site="matsui")`）を使います。`signin.cookies` が
BRiSK の Cookie、`signin.client` がデータクライアントのある証券会社でのクライアントです。2つのサインインが同時に
起きても（セッションが同時に失効した2つのフィードなど）、1つずつ順番に行われ、それぞれ自分の Chrome を使います。

証券会社を追加したり、組み込みの設定を直したりするには、`~/.config/brisk/sites.json` を編集します。

```json
{"sites": [{"id": "mybroker", "name": "My Broker", "login_url": "https://broker.example/login",
            "cookie_host": "mybroker.brisk.jp", "passkey_button": "パスキーでログイン"}]}
```

ほかのキー: `cookie_prefix`、`launch_url`、`enroll_url`（初回のサインインがパスキーのページとは
別のページの場合に `enroll` を始める場所）、`user_field`（パスキーのボタンを押す前に入力が必要な
ユーザID欄の CSS セレクター。指定すると `enroll` がユーザIDを尋ねます）。

内部の仕組みは [ARCHITECTURE.md](https://github.com/honvl/BRiSKapi/blob/main/ARCHITECTURE.md#passkey-sign-in) を参照してください。

## API リファレンス

| 呼び出し | 戻り値 |
| --- | --- |
| `briskapi.connect(site="demo" \| "sbi" \| "e-shiten" \| "matsui" \| "monex" \| "smbcnikko")` | デモまたは証券会社の BRiSK のライブの `Feed`（既定のデータ源になる。`gateway=True` で `Live` にも配信） |
| `briskapi.Live()` | このコンピューターのフィードから Databento 風のライブのレコード: `subscribe(dataset, schema, symbols='ALL_SYMBOLS', start=None, snapshot=False, stream=True)`、`latest(schema=None, symbols=None)`（最新の気配のテーブル）のあと、イテレーション、または `add_callback(fn)` / `add_stream(path)` と `start()`、`block_for_close(timeout)`。`stop()`、`symbology_map`（[上記](#ライブのレコードdatabento-風)） |
| `Ticker(code).info()` | 銘柄名、売買単位、呼値の種別、基準値、値幅制限 |
| `Ticker(code).quote(at=None)` | 買い・売り気配とその数量・件数、板の各側の上位10本（`bids`、`asks`）と引け条件付き注文の上位10本（`closing_bids`、`closing_asks`）、板全体の注文合計、予想約定価格・数量、成行・引け条件付きの数量、直近の約定、売買代金、銘柄の状態、`special_quote`（特別気配の側・価格・タイマーの終了時刻） |
| `Ticker(code).auction(at=None, closing=False)` | 板寄せの予想状態、成行注文、板の合計と `market_order_imbalance`（成行の買い数量 − 売り数量）。`closing=True` で引けの板寄せ |
| `Ticker(code).history(start, end)` | すべての更新（時系列順）。読み込んだ記録かフィードから（ネットワークは使わない） |
| `Ticker(code).trades(start, end)` | すべての約定（歩み値）：時刻、価格、株数、仕掛けた側（`buy`、`sell`。寄付きの約定の多くは None）。読み込んだ記録かフィードから |
| `Market().stocks()` | 全銘柄のマスター |
| `Market().snapshot(at=None)` | 全銘柄の気配 |
| `Market().imbalances(at=None, top=None, closing=False)` | 成行注文の売買差（絶対値）が大きい順の銘柄（引けの板寄せも可） |
| `Market().special_quotes(at=None)` | 特別気配の銘柄：側、価格、タイマーの終了時刻 |
| `Market().trades(start, end)` | 全銘柄の約定（時刻順） |
| `Market().frames()` | 記録のバッチ：時刻、計測値、気配と約定の件数 |
| `Market().summary()` | データ源、日付、銘柄数、時刻の範囲 |
| `Feed.quotes(codes)` / `Feed.on_quote(fn, codes)` | 届いた順のライブ更新 |
| `Feed.recordings` | ライブフィードのこれまでの記録の一覧：`trading_date`、`name`、`path`、`uploaded`、`timing_uploaded`、`repaired`、`error` |
| `briskapi.recordings()` / `.pull()` / `.load()` | 自分のアーカイブの一覧、検証付きダウンロード、ローカルファイル |
| `briskapi.Archive().upload(dir)` / `.delete(prefix)` / `.deleted()` / `.restore(prefix)` / `.timing()` / `.owner` | アップロード、削除、過去30日間に削除したもの（`deleted_at`、`restore_until` 付き）、元に戻す、自分のタイミングレポート、自分の owner ID |
| `briskapi.record(output, cache=None, ...)` | デモの記録（設定に従ってアップロード） |
| `briskapi.consent(...)` | アップロードの設定 |
| `briskapi.Historical()` | クラウド API のクライアント（Databento 風）。API キーは自分のアーカイブの ID（[下記](#クラウドでアーカイブを照会する)） |
| `Historical().timeseries.get_range(dataset, start, end=None, symbols='ALL_SYMBOLS', schema='trades', limit=None, path=None)` | `XTKS.BRISK`（自分の記録）または `XTKS.BRISKDEMO` のレコードを返すストア：Databento と同じ変換：`to_df()`（`ts_recv` をインデックスにする。バーは `ts_event`）、`to_ndarray()`、`to_csv(path)`、`to_json(path)`、`to_parquet(path)`、`to_file(path)`、`replay(callback)`、反復、`len()` |
| `Historical().metadata.list_datasets()` / `.list_schemas(dataset)` / `.list_fields(schema)` / `.list_publishers()` / `.get_dataset_range(dataset)` | カタログ、データセットにデータのある最初と最後の日付 |
| `Historical().metadata.get_dataset_condition(dataset, start_date, end_date)` | 各日付が available か missing か、自分の記録がカバーする時間帯（日本時間） |
| `Historical().metadata.get_record_count(...)` / `.get_cost(...)` | リクエストが返すレコード数、そのコスト（常に 0。無料で、上限あり） |
| `Ticker(code).candles(interval)` / `.margin(days)` | 証券会社の BRiSK 自身のローソク足（`5m` は当日のみ、`1d`/`1w`/`1mo` がどこまでさかのぼるかは BRiSK 次第）、信用残と貸株料 |
| `Market().turnover()` / `.lists()` / `.events()` / `.schedule()` / `.watchlist()` | 証券会社の BRiSK の市場データ |
| `briskapi.login(site=..., cookies=None)` / `briskapi.logout()` | 証券会社の BRiSK のセッション（既定は SBI）。上の証券会社のデータと `connect(site=...)` が使う |
| `briskapi.passkey.enroll()` / `.login()` / `.forget()` | 証券会社の BRiSK へのパスキーでのサインイン |

価格は円単位の浮動小数点数で、ベンダーの「値なし」（0）は `None` になります。
時刻は取引日の日本時間の `datetime` です。数量は株数で、売買区分・フラグ・
ステータスはベンダーの値のままです。`raw=True` を指定すると、ベンダー形式
（`*_price10` は0.1円単位、`*_us` は日本時間0時からのマイクロ秒）で返します。
表形式の結果は dict のリストで、`.to_pandas()` で DataFrame に変換できます。
エラーは `briskapi.BriskError` と `briskapi.NotFoundError` です。最初の市場全体の
照会で記録のテーブルをメモリーに読み込みます（デモ全体で約2秒）。1銘柄の履歴は
その銘柄の分だけを読みます。

## 別のフィードの記録で補修する

同じ取引日の2つのフィード（2台のコンピューター、または2つの証券会社）は同じ市場を記録しますが、途中で切れた
フィードや遅れて始めたフィードには、もう一方にあるものの一部が欠けます。`brisk repair` は同じ日の記録を比べ、
別のフィードにだけあるものを足した、手元の記録の写しを書き出します:

```sh
brisk repair recordings/sbi-am --from recordings/eshiten-am     # 比べるだけ: 件数を表示し、何も書かない
brisk repair recordings/sbi-am --from recordings/eshiten-am --output recordings/sbi-am-repaired
brisk repair days/sbi --from days/eshiten --output days/repaired    # 記録のフォルダー: 時間帯で自動的に組み合わせる
```

- **どの記録がどの記録を補修するか。** 記録は、同じ日の別のフィードの記録のうち、ほぼ同じ時間帯をカバーするもの
  （短いほうの半分以上）から補修されます。前場と後場のように時間帯が違う記録は、前後に続く別の部分であって、
  補修し合う相手ではありません。前場が後場で補修されることはありません。フォルダーを渡すと（`brisk live --output`
  のフォルダーには1部分ごとに1つ記録があります）、各記録が自分の相手を1つまたは複数見つけ、順にそれぞれから
  補修されます。
- **更新の突き合わせ。** 気配の更新は、銘柄と BRiSK のフレーム番号で識別します（約定はフレームごとにまとめて）。
  件数は、各記録の更新のうち、*same*（相手にも同じものがある）、*only*（相手は動いていたのにない）、*conflict*
  （相手に同じフレームがあるが内容が違う）、*outside*（相手が始まる前か終わった後）がいくつか、また
  ブロック（銘柄 × `--window` 分、既定は5分）のうち、同一、相手にあるものが欠けている、双方に欠けている、
  不一致がいくつかを示します。
- **書き出すもの。** 手元の記録はそのままに、相手にだけあるものを、それぞれ独自のバッチとして加えます。
  アーカイブが証券会社の記録を調べるのと同じ検査をしてパッケージにするので、`brisk upload` で送れます。
  食い違うところは手元の記録のままです。手元の記録の中で時刻が逆戻りしてしまう更新は、報告の `left_out` に
  数えるだけで加えません。何も増えない記録は書き出しません。記録が1つなら `--output` に、複数なら
  名前ごとのフォルダーに入ります。
- **どこまで届くか。** 加えるのは手元の記録の時間内のフレームだけです。ただし、その日の別の記録がその後に続く場合
  （フォルダーの中、またはコマンドラインで渡した部分）は、その手前までの空白を埋めます。続く記録が始まる1マイクロ秒
  前までで、中には入らないので、同じ時間が二重に保存されることはありません。続く記録が分かっていない記録は延ばされ
  ません。そのような1つの記録（早く止まったフィード）には `--until 15:30:00`（日本時間）でそこまで延ばせますが、
  続く記録の中には入りません。
- **受信時刻。** 手元の記録が動いている間に取りこぼした更新（その銘柄の後の更新を手元の記録が持っているもの）は、
  その後の更新のバッチのすぐ前に独自のバッチとして入り、そのバッチの受信時刻が付きます。これで銘柄の更新は、
  市場時刻でも、フレーム番号でも、受信時刻の順でも前へ進み、手元のコンピューターが次の更新を知るより前に
  受信したことにはなりません。受信時刻を借りた更新の数は、報告の `borrowed_receipt` です（1つのバッチに入れられる
  銘柄の更新は1件だけなので隣のバッチには空きがなく、また2つのフィードは数十ミリ秒ずれることがあるため、相手の
  バッチの時刻が手元の次の更新より後になることもあります）。その銘柄について手元の記録が持つものより後
  （次の片との間の空白、早く止まったフィード）や約定は、相手の記録の受信時刻（相手のコンピューターの時計）が付いた
  相手のバッチとして入ります。手元のものは手元のままです。
- **断る場合。** 組み合わせた2つがフレームを同じように番号付けていない場合（手元にフレームがある相手の更新の
  大半が同じでなければなりません）、どのフレームが欠けているかを言えないので、件数だけ数えて何も書きません。
  記録は同じ日で、現在の形式（briskapi 1.5 以降）であり、補修する記録は終わっている必要があります。どの記録も
  変更されません。

## コマンドライン

```sh
brisk live --site demo --codes 7203,6758 --quotes   # 気配の更新ごとに JSON を1行出力（--raw でベンダー形式）
brisk sites | enroll | login | forget       # Chrome 経由のパスキーで証券会社の BRiSK にサインイン（試験的）
brisk live --site sbi --codes 7203          # SBI証券の BRiSK。Cookie は BRISK_SBI_COOKIES（JSON）または `brisk login` が保存したもの。--trace-protocol で接続手順を表示（通信の詳細は BRISK_PROFILE）
brisk live --site e-shiten --codes 7203     # 立花証券 e支店の BRiSK Next。Cookie は BRISK_ESHITEN_COOKIES（JSON）または `brisk login` が保存したもの
brisk live --site matsui --codes 7203       # 松井証券（または --site monex、--site smbcnikko）。Cookie は BRISK_MATSUI_COOKIES（JSON）または `brisk login` が保存したもの。まだ口座で未検証
brisk live --site sbi --output days         # 日をまたいで動き続け、記録ごとに days/ に残し（例: sbi_20261013_085800-113000）、stderr に {"day": {...}} を1行出力
brisk live --site sbi                       # 記録・アップロードし、briskapi.Live() に配信（--no-gateway で配信しない）。10秒ごと（ログでは1分ごと）に stderr に1行: その日、記録した時間、更新数とその速さ。記録の保存中は、その進み具合も
brisk record --output recordings/s1         # デモを記録（オプトアウトしない限りアップロード）。stderr にパッケージ化と保存の完了、アップロードの様子を表示
brisk list --date 20210927 --source historical_mock   # 自分の記録（--timing で自分のタイミングレポート、--deleted で過去30日間に削除したもの）
brisk pull PREFIX --output recordings/downloaded   # brisk list が返した prefix を使う
brisk delete PREFIX                         # 自分のアップロードを自分のアーカイブから削除（30日間は復元可能）
brisk restore PREFIX                        # 過去30日間に削除したアップロードを元に戻す（タイミングレポートのキーも可）
brisk prune recordings/s1                   # ディスクを空ける: アーカイブにある記録のローカルのコピーを削除
brisk export recordings/s1 --nautilus catalog      # NautilusTrader v2 のカタログを銘柄ごとに書き出し（または --jsonl FILE。記録全体をメモリに読むので、証券会社の前場・後場では数十 GB）
brisk repair recordings/a --from recordings/b --output recordings/a-repaired   # その日の別のフィードの記録にだけあるものを足す（--output なしなら比べるだけ。記録のフォルダーも可）
brisk query XTKS.BRISK --schema ohlcv-1m --symbols 7203 --start 2026-10-13   # クラウド API：CSV を標準出力へ（--encoding json|parquet、--output FILE、--end、--limit）
brisk consent [--accept | --revoke]         # 自動アップロードの設定の表示・変更（--no-upload: 今回だけ）
brisk upload recordings/s1                  # 記録のアップロードを再試行（stderr: 送ったバイト数、アーカイブの確認、保存完了）
brisk identity [--recovery-code | --recover CODE]  # 自分の owner ID、復元コード、別のコンピューターで使う
```

各コマンドの詳細は `--help` で確認できます。`pull` はすべて検証してから
書き込み、既存のフォルダーを上書きすることはありません。

各コマンドは、Python の呼び出しのコマンドライン版です。

| コマンド | Python |
| --- | --- |
| `brisk live --site demo`（`--cache DIR`） | `briskapi.connect(site="demo", gateway=True)`（`cache=DIR`。`--no-gateway` なら `gateway` なし） |
| `brisk live --site sbi`（またはほかの証券会社） | `briskapi.connect(site="sbi", gateway=True)` |
| `brisk record` | `briskapi.record()` |
| `brisk query` | `briskapi.Historical().timeseries.get_range()` |
| `brisk list`（`--deleted`、`--timing`） | `briskapi.recordings()`（`Archive().deleted()`、`.timing()`） |
| `brisk pull`、`upload`、`delete`、`restore`、`prune` | `briskapi.pull()`、`Archive().upload()`、`.delete()`、`.restore()`、`.prune()` |
| `brisk consent` | `briskapi.consent()` |
| `brisk enroll`、`login`、`forget` | `briskapi.passkey.enroll()`、`.login()`、`.forget()` |
| `brisk export`、`brisk repair`、`brisk identity` | コマンドラインのみ |

`brisk live --site sbi`（またはほかの証券会社のサイト）は、止めるまで動き続け、各取引日を通常は前場と後場の2つの記録にします。
気配が8,000万行に達すると、さらに記録を分けます。
取引終了後や休日に始めると、そう表示して次の取引日を待ちます。
オプトアウトしない限り、市場データの記録をできたものから順に、各日のタイミングの要約とともに確認なしで
アップロードします。`--no-upload-market-data` で記録を、`--no-upload` で両方をアップロードしなくなります
（ディスクに残すには `--output DIR`）。`brisk list --source brisk_live`（どの証券会社のものも）で自分の証券会社の記録を
一覧できます。どの `brisk live` も、`--no-gateway` を指定しない限り、このコンピューターの
`briskapi.Live()` にフィードを配信します（[ライブのレコード](#ライブのレコードdatabento-風)）。

記録の保存には数分かかるので、その様子を表示します。セッションが終わると、記録の保存が続いている間に、そう表示します
（`session ended; waiting for the next trading day`）。保存の様子は stderr に出ます
（端末では5秒ごと、ログでは30秒ごと。`--quotes` の標準出力は気配のままです）: どの記録か（`sbi 2026-10-13 from 09:00:00`）、
段階（準備、バッチ・気配・約定の書き込み、仕上げ）と書き込んだ行数と全体の行数、ディスクに書き終えて初めて
`saved to PATH in 2m35s`、失敗したときは `Packaging failed after ...` とその理由です
（昼休みの再接続のように、ストリームから何も来なかった区間は `no update came until 12:00:03; nothing to save`）。アップロードには別の行があります:
送ったバイト数、アーカイブの確認、`uploaded; the archive stored it`。
保存に失敗したものは `~/.local/share/brisk/recordings` に残り、同じ形で表示されます（`keeping what was recorded in ...`、
続いて `saved to ...`。アップロードの失敗なら `brisk upload PATH` で再試行できます）。`brisk record`、`package`、`upload` も
パッケージ化とアップロードの様子を同様に stderr に出し、JSON の結果は標準出力のままです。

## 自分専用のアーカイブ

アップロードすると、最後まで正常に終わったすべてのセッションのコピーを、お使いの
コンピューターの外に無料で持てます（到着時に検証し、デモの完全な再生は基準と一致する
必要があります）。アーカイブに入った記録は、`brisk prune DIR` でローカルのコピーを
削除でき（先に SHA-256 をアーカイブと照合します）、`brisk pull` でこのコンピューターにも
別のコンピューターにも戻せます。アップロードすると、[クラウド API](#クラウドでアーカイブを照会する)と、
クラウドにしかない OHLCV バーも使えます。アップロードは `archive/OWNER/YYYYMMDD/FROM-TO-SHA256`
（`brisk list` が返す prefix）の下に保存されます。FROM と TO は記録の最初と最後のソース時刻
（日本時間、HHMMSS）なので、その日の記録は時刻順に並びます。

アップロードしたものは非公開で、一覧・ダウンロード・削除ができるのはご自身だけです。
アカウントはありません。必要になったとき、ツールがあなたの **ID**（ランダムな秘密の値）を
作り、アーカイブはその一方向ハッシュである owner ID の下にアップロードを保存します。
秘密の値は、システムの資格情報ストア（macOS のキーチェーンまたは Windows の資格情報
マネージャー）と、本人だけが読めるファイル `~/.config/brisk/identity.json` の2か所に
保存され、一方が失われてももう一方から戻ります。作成時には **復元コード**（英数字28文字）が
表示されるので、書き留めてください。`brisk identity --recovery-code` で再表示でき、
`brisk identity --recover CODE` で別のコンピューターでも使えます（`BRISK_IDENTITY` でも
指定可）。秘密の値を持つ人はアーカイブにとってあなた本人なので、他人に渡さないでください。
これも保存したコピーもなくすと、アップロードにはアクセスできなくなります。ほかの
ホスティングサービスと同様に、アーカイブの運営者は保存されたデータにアクセスできます。
詳しくは [PRIVACY.ja.md](PRIVACY.ja.md) をご覧ください（相違がある場合は英語版の [PRIVACY.md](PRIVACY.md) が優先します）。

アップロードは既定でオンです。コマンドラインで初めて記録またはライブセッションを始めると、
アップロードされる内容が表示され、一度だけ確認されます（Enter で同意、`n` で拒否）。答えは
保存され、それ以降は最後まで正常に終わったセッション（ライブフィードでは各記録）が、改めて
確認することなく自動的にアップロードされます。確認できない場面（Python API や端末のないコマンド実行）では、
`brisk consent --accept` または `--revoke`（`briskapi.consent(accept=True)` または
`revoke=True`）で決めるまで、セッションは既定でアップロードされ、プロセスごとに一度その旨を
表示します（Python では警告）。

- **デモのセッションでアップロードされる内容:** ローカルの計測値（お使いの
  コンピューターの時計を含み、記録した日時がわかります）。デモの再生の市場データは誰にとっても
  同じなので、アーカイブは1つだけ保持します。デモのアップロードで送るのは自分のタイミングの
  テーブルだけ（約29 MB の代わりに約0.2 MB）で、アーカイブはそれを、検証済みのデモの市場データの
  テーブルと結び付け、`brisk pull` はこれまでどおり検証したうえで完全な記録を返します
  （そのアーカイブへの最初のデモのアップロードだけは、記録全体を送ります）。IP アドレスは
  リクエスト回数などの制限にのみ、鍵付きハッシュとして使います。
- **証券会社のセッションでアップロードされる内容:** タイミングの要約（デコード
  時間、受信時のデータの遅れ、フレーム間隔のパーセンタイル、停滞回数、フレーム数、取引日、
  最初と最後の分。`briskapi.Archive().timing()` で一覧できます）と、取引日が終わった時点で
  その日のデコードした市場データの記録（銘柄マスター、価格、数量、板、約定、銘柄コード、
  ローカルの計測値）。Cookie、トークン、接続の診断情報はアップロードしません。その日の約定からは、
  クラウドで自分の記録だけから OHLCV バーが作られます（[下記](#クラウドでアーカイブを照会する)）。
- **アップロードをやめる:** 1回だけなら `--no-upload`（Python では `upload=False`）。
  `--no-upload-market-data`（`upload_market_data=False`）は証券会社のセッションのタイミングの
  要約だけをアップロードし、市場データの記録はアップロードしません（残すには `--output DIR`）。その環境のすべての実行では `BRISK_UPLOAD=0`、恒久的には
  `brisk consent --revoke`。
- **削除と復元:** `brisk delete PREFIX` でアップロードを自分のアーカイブから削除し、一覧にも
  ダウンロードにも出なくなります。30日間は元に戻せます。`brisk list --deleted` で削除したもの
  （`deleted_at` と `restore_until` 付き）を一覧でき、`brisk restore PREFIX`（タイミングレポートの
  キーも可）で復元します。30日を過ぎると復元できなくなり、削除されます。
- 途中までの再生（`--limit-frames`）はアップロードされません。日の途中で止めた証券会社のフィードは、その日を
  そこで終えて残りの分をアップロードし、接続が切れる前の分はフィードが再接続した時点でアップロードされます。クラッシュや
  強制終了で途切れた日も、終わりの印を付けて修復し、アップロードします（クラッシュの場合は次にフィードを起動した
  とき）。

## クラウドでアーカイブを照会する

アップロードすると、自分の記録を対象にしたクラウド API も使えます。Databento の履歴データ API の
形（名前もパラメーターも同じ）に倣っています。API キーは自分のアーカイブの ID なので、サインアップも
キーの作成も要らず、設定もありません。`briskapi.Historical()` はアップロードのときと同じ方法で ID を
見つけます（なければ新しく作ります）。

```python
import briskapi

client = briskapi.Historical()
trades = client.timeseries.get_range("XTKS.BRISK", start="2026-10-13", end="2026-10-17",
                                     symbols=["7203"], schema="trades")
trades.to_df()      # ts_recv をインデックスにした pandas の DataFrame。to_ndarray()、to_csv()、to_json()、to_parquet()、replay() も使えます
client.timeseries.get_range("XTKS.BRISK", start="2026-10-13", symbols=["7203"],
                            schema="ohlcv-1m").to_df()     # 1分のバー（下記）
client.timeseries.get_range("XTKS.BRISKDEMO", start="2021-09-27T08:59", end="2021-09-27T09:03",
                            symbols=["7203"], schema="imbalance")    # デモ。どの ID でも照会できます
client.metadata.get_dataset_condition("XTKS.BRISK", start_date="2026-10-01")   # 各日付と、記録した時間帯
```

`get_range(dataset, start, end=None, symbols='ALL_SYMBOLS', schema='trades', limit=None,
path=None, ts_filter='ts_event', skew=10.0)` はストアを返します。`path` を指定すると Parquet ファイルにも
書き出します。サーバーは BRiSK のタイムスタンプ（`ts_event`）で絞り込みますが、ライブフィードのレコードは
少し遅れて手元のコンピューターに届きます（`ts_recv`）。先読みをしてはならないテストでは、
`ts_filter='ts_recv'` で受信時刻による絞り込みと並べ替えができます（前後 `skew` 秒を広げて要求し、
タイムスタンプがそれより先行していたら警告します）。`get_snapshot(dataset, at, symbols='ALL_SYMBOLS',
schema='brisk-quotes', since=None)` は、`at` 時点の各銘柄の最新のレコードを1銘柄1行で返します（既定は
受信時刻基準で、`at` までに受信したもののうちタイムスタンプとフレームが最新のもの。`ts_filter='ts_event'` も指定できます）。
`client.metadata` には `list_datasets()`、`list_schemas(dataset)`、`list_fields(schema)`、
`list_publishers()`、`get_dataset_range(dataset)`、`get_dataset_condition(dataset, start_date, end_date)`
（各日付が available か missing か、自分の記録がカバーする時間帯を日本時間で返します）、
`get_record_count(...)`、`get_cost(...)` があります。`get_cost` は常に 0 です。クラウド API は無料で、
1つの ID あたり1分に60リクエスト、1日に 2 GB まで、1つの IP アドレスあたり1分に120リクエスト、
1日に 4 GB までの上限があります。

- **データセット**は Databento の `VENUE.FEED` という命名に従います（XTKS は東京証券取引所）。
  `XTKS.BRISK` は自分のライブ記録で、どの証券会社の BRiSK 経由でも同じです。`XTKS.BRISKDEMO` は
  公開デモ（2021-09-27）で、どの ID からでも照会できます。
- **スキーマ:** `trades`、`mbp-10`（各側10本。BRiSK は注文イベントではなく板を返すので、`action` と
  `side` は `N`）、`definition`、`imbalance`（寄付きと引けの板寄せの状態：予想約定価格・数量、
  成行注文の売買差）、`brisk-quotes`（BRiSK の気配のすべての項目）、そして OHLCV バーの
  `ohlcv-1s`、`ohlcv-1m`、`ohlcv-1h`、`ohlcv-1d`。`list_fields(schema)` でスキーマの列を確認できます。
- **時刻:** 日付はその日の日本時間の取引日、タイムゾーンのない時刻は日本時間、整数は UTC のナノ秒を
  表します。`end` は含まず、省略すると start の取引日の終わりまでです。1回のリクエストで扱えるのは
  最大31日、2,000銘柄までです。
- **銘柄**は JPX の銘柄コードです。`instrument_id` はコードごとに固定です。数字だけのコードはその数値
  （7203）、それ以外は 100,000 にコードの36進数の値を足したものです（`167A` は 154694）。

**`Historical` の OHLCV バーはクラウドにしかなく、** 手元のファイルから作ることはありません（`Live` は
ストリームから作ります）。アーカイブは取引日ごとに、自分の現在のライブ記録から
バーを作ります（各約定は1回だけ数えます）。記録をアップロード、削除、復元するたびに作り直すので、
照会では作成済みのバーを読むだけです。含まれるのは、記録がその銘柄と、取引時間（前場 09:00〜11:30 と
後場 12:30〜15:30、日本時間）内のそのバーの全期間をカバーしているバーだけです。たとえば 08:55 から
11:31 までの記録なら、午前の1分と1時間のバーは受け取れますが、1日のバーは受け取れません。つまり、
バーを得るにはアップロードが必要です。他の人のアップロードが自分のバーに入ることはなく、自分の
アップロードが他の人のバーに入ることもありません（`XTKS.BRISKDEMO` のバーは、デモ自身の約定から
作ります）。詳しくは [PRIVACY.ja.md](PRIVACY.ja.md)（ポリシーバージョン8）をご覧ください。`Ticker.candles()` は別のもので、
証券会社の BRiSK 自身のバーです（証券会社へのログインが必要）。

コマンドラインでは、`brisk query` は CSV を標準出力に書きます。`--encoding json` または `parquet` で
変更でき（Parquet は `--output FILE` が必要）、`--output FILE` でファイルに書き、`--end` と `--limit` で
リクエストの範囲を絞ります。

```sh
brisk query XTKS.BRISK --schema ohlcv-1m --symbols 7203 --start 2026-10-13 > toyota-1m.csv
```

その裏にある HTTP API は [docs/cloud-api.ja.md](docs/cloud-api.ja.md)に説明があります。同じ
レコードは、手元のフィードから `briskapi.Live` でライブでも受け取れます（[上記](#ライブのレコードdatabento-風)）。

## その他のドキュメント

[ドキュメントサイト](https://honvl.github.io/BRiSKapi/ja/)には、これらの文書と、呼び出しごとの例と
その結果を載せた API リファレンスがあります。

- [docs/data-format.ja.md](docs/data-format.ja.md): 記録とクラウド API のレコードの全列、その読み方
- [docs/cloud-api.ja.md](docs/cloud-api.ja.md): クラウド API の仕組み
- [PRIVACY.ja.md](PRIVACY.ja.md): プライバシーポリシー（相違がある場合は英語版の [PRIVACY.md](PRIVACY.md) が優先）

英語のみ:

- [ARCHITECTURE.md](ARCHITECTURE.md): 仕組み、データ形式、アーカイブの改ざん対策と制限
- [CONTRIBUTING.md](CONTRIBUTING.md): 開発、テスト、リリース
- [tools/brisk_mock/README.md](tools/brisk_mock/README.md): フィールド定義、タイミングとレイテンシー、SBI のマスター配置の確認、任意の Rust コレクター
- [infra/README.md](infra/README.md): アーカイブのデプロイ
- [THIRD_PARTY.md](THIRD_PARTY.md): デコーダー、データ、pybrisk の権利表示

ソフトウェアは MIT ライセンスです。
