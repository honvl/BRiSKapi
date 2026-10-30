# データ形式

[English](data-format.md) | [日本語](data-format.ja.md)

briskapi が返す Parquet（および CSV と JSON）の中身を、列ごとに説明します。データの出どころは
2つあります。

- **クラウド API**（`briskapi.Historical`、`brisk query`）：Databento の形のレコードで、
  リクエストごとに1つのスキーマ。[クラウド API のレコード](#クラウド-api-のレコード)。`briskapi.Live` は
  同じレコードを、お使いのコンピューターのフィードからライブで返します。同じセッションなら、列、型、値は
  同じです（ただし、バーが終わった後にこのコンピューターに届いた約定は、ライブでは次のバーに数えられ、
  クラウドではその約定自身の時刻で置かれます）。
- **お使いのコンピューター上の記録**（`brisk record`、`brisk pull`、`brisk live --output`）：
  NautilusTrader v2 の構成の Parquet テーブル4つのフォルダーで、デコーダーが出力したすべてを
  含みます。[記録](#記録)。

`client.metadata.list_fields(schema)` は、以下の表と同じ列を、型と意味とともに返します。

## 規約

- **時刻**は UTC のナノ秒です（Parquet の `timestamp[ns, UTC]`）。東京は UTC+9 なので、
  `to_df(tz="Asia/Tokyo")` や `.dt.tz_convert("Asia/Tokyo")` を使うと、取引所と同じ時刻で表示されます。
  すべてのレコードに `ts_event` があり、これはそのフレームが届いたときの BRiSK のサーバーの
  タイムスタンプです（マイクロ秒精度）。それが取引所自身のイベント時刻かどうかは確認できていません。
  レコードには `ts_recv` もあり、これは記録したコンピューターがそのフレームを受信した時刻です
  （ミリ秒精度）。デモの再生には意味のある受信時刻がないため、デモでは `ts_recv` は `ts_event` と
  等しくなります。
- **価格**は円単位です。クラウド API は float64 で返します（BRiSK の分解能は 0.1 円）。
  `to_df(price_type="fixed")` を指定すると、Databento と同じく 1e-9 円単位の整数で返します。
  記録では `decimal128(38, 16)` で正確な値を保持します。価格が null なのは、BRiSK が値を示さなかった
  こと（BRiSK の「値なし」を表す 0）を意味します。
- **数量**（`size`）は株数、**件数**（`count`）は注文数です。
- **銘柄コードと ID。** `symbol`（または `raw_symbol`）は JPX の銘柄コードで、`7203` のような数字だけの
  もののほか、`167A` のように英数字のものもあります。`instrument_id` は毎日同じ値です。数字だけの
  コードはその数値（7203）、それ以外は 100,000 にコードの36進数の値を足したものです（`167A` は
  154,694）。記録の `issue_id` はその日の銘柄マスター内の番号にすぎないので、代わりに
  `identifier`（`7203.XTKS`）か銘柄コードを使ってください。
- **BRiSK の値そのまま**の列（`issue_status`、`quote_flag`、`issue_type`、`tick_type`）は、BRiSK が
  送ってきたままの値で保存されます。その意味はすべて分かっているわけではありません。

## クラウド API のレコード

バー以外のすべてのスキーマは、同じ4列で始まり、`symbol` で終わります。

| 列 | 型 | 意味 |
| --- | --- | --- |
| `ts_recv` | timestamp (ns, UTC) | アップロードしたコンピューターがフレームを受信した時刻（デモは `ts_event`） |
| `ts_event` | timestamp (ns, UTC) | フレームに付けられた BRiSK サーバーのタイムスタンプ |
| `publisher_id` | uint16 | `XTKS.BRISK` は 1、`XTKS.BRISKDEMO` は 2（`metadata.list_publishers`） |
| `instrument_id` | uint32 | 上記のとおり、コードごとに固定 |
| `symbol` | string | JPX の銘柄コード（`map_symbols=False` では省かれます） |

レコードは時刻順（`ts_event`、次に `instrument_id`、次に `sequence`）に並びます。自分の複数の記録が
重なっている場合も、各レコードは1回だけ現れます（`book-deltas` はそのうち1つを読みます。そちらを参照）。
`briskapi.Live` は、フィードがバッチを受け取った順に、各バッチのレコードをこの順で届けます。

### `trades`

| 列 | 型 | 意味 |
| --- | --- | --- |
| `action` | string | 'T' |
| `side` | string | 仕掛けた側：'B' 買い手が仕掛けた、'A' 売り手が仕掛けた、'N' なし（寄付きの約定の多く） |
| `depth` | uint8 | 0 |
| `price` | float64 | 約定価格（円） |
| `size` | uint64 | 株数 |
| `flags` | uint8 | 0 |
| `sequence` | uint64 | BRiSK のフレーム番号 |
| `trade_id` | string | FRAME-PLACE：フレームと、その中での約定の位置。銘柄ごとに一意 |

### `mbp-10`

| 列 | 型 | 意味 |
| --- | --- | --- |
| `action` | string | 'N'：BRiSK が送るのは板であり、板を変えた注文イベントではありません |
| `side` | string | 'N' |
| `depth` | uint8 | 0 |
| `price` | float64 | null |
| `size` | uint64 | 0 |
| `flags` | uint8 | セッションで各銘柄の最初の更新は 32（スナップショット）、それ以外は 0 |
| `sequence` | uint64 | BRiSK のフレーム番号 |
| `bid_px_NN` | float64 | 買い板の NN 本目の価格（円。NN は最良の 00 から 09 まで）。最後の本より先は null |
| `bid_sz_NN` | uint64 | 買い板の NN 本目の株数 |
| `bid_ct_NN` | uint32 | 買い板の NN 本目の注文件数 |
| `ask_px_NN` | float64 | 売り板の NN 本目の価格（円。NN は最良の 00 から 09 まで）。最後の本より先は null |
| `ask_sz_NN` | uint64 | 売り板の NN 本目の株数 |
| `ask_ct_NN` | uint32 | 売り板の NN 本目の注文件数 |

### `book-deltas`

銘柄の板のすべての価格帯を、一日を通して、変わったところとして表します。Databento の MBO のレコードを、
BRiSK が与える粒度（価格帯ごとの株数と注文件数。BRiSK に注文 ID はありません）にしたものです。記録の中で
銘柄の最初の更新は、板を消去し（`R`）すべての価格帯を加え（`A`）、32 の印が付きます。その後の各更新は、
現れた価格帯（`A`）、変わった価格帯（`M`）、空になった価格帯（`C`）ごとにレコードを持ち、最後のものに 128 の
印が付きます。順に適用すると、各更新の時点の板全体（通常の買い板・売り板と、引け条件付き注文の板
（`closing`））になります。`snapshot=True`（Historical の `get_range`、`brisk query --snapshot`、`start` を
付けた `Live.subscribe`）では、開始時点の各銘柄の板が 32 の印付きで先に来るので、その後のレコードを既知の板に
適用できます。`Historical.timeseries.get_snapshot(schema="book-deltas")` はそれだけを返します。クラウド API は、
記録が 1,000 件の変更ごとに持つチェックポイントから組み立て、その日の始めからは読み直しません。briskapi 1.9
より前の記録とデモの記録にはありません。その日の自分の記録が時間的に重なる場合は、長いほうを読みます。
記録の変更は、それぞれその記録自身の直前の更新からのものだからです。

| 列 | 型 | 意味 |
| --- | --- | --- |
| `action` | string | 'A' 価格帯が現れる、'M' 株数か注文件数が変わる、'C' 空になる、'R' 銘柄の板を消去する（スナップショットの価格帯が続く） |
| `side` | string | 'B' 買いの価格帯、'A' 売りの価格帯、'R' では 'N' |
| `closing` | bool | 通常の板ではなく、引け条件付き注文の板 |
| `price` | float64 | 価格帯の価格（円）。'R' では null |
| `size` | uint64 | この変更後の価格帯の株数（C では 0） |
| `count` | uint32 | この変更後の価格帯の注文件数（C では 0） |
| `flags` | uint8 | 更新の最後のレコードに 128。スナップショットのレコード（銘柄の最初の更新、または snapshot=true での開始時点の板）に 32 |
| `sequence` | uint64 | BRiSK のフレーム番号 |

### `definition`

| 列 | 型 | 意味 |
| --- | --- | --- |
| `ts_recv` | timestamp (ns, UTC) | 取引日の日本時間0時 |
| `ts_event` | timestamp (ns, UTC) | 取引日の日本時間0時 |
| `publisher_id` | uint16 | `XTKS.BRISK` は 1、`XTKS.BRISKDEMO` は 2（`metadata.list_publishers`） |
| `instrument_id` | uint32 | コードごとに固定：数字だけのコードはその数値、それ以外は 100,000 に36進数の値を足したもの |
| `raw_symbol` | string | JPX の銘柄コード |
| `exchange` | string | 'XTKS' |
| `currency` | string | 'JPY' |
| `min_lot_size_round_lot` | int64 | 売買単位あたりの株数 |
| `min_price_increment` | float64 | BRiSK の価格の分解能（0.1 円）。呼値の単位（価格と `tick_type` によって決まる）ではありません |
| `high_limit_price` | float64 | その日の値幅制限の上限（円） |
| `low_limit_price` | float64 | その日の値幅制限の下限（円） |
| `base_price` | float64 | その日の基準値（円） |
| `name` | string | 銘柄名 |
| `issue_type` | uint64 | BRiSK の銘柄種別（値のまま） |
| `tick_type` | uint64 | BRiSK の呼値の種別（値のまま） |
| `symbol` | string | JPX の銘柄コード（`map_symbols=false` では省かれます） |

### `imbalance`

| 列 | 型 | 意味 |
| --- | --- | --- |
| `auction_type` | string | 'O' セッションの板寄せ、'C' 引け条件付き注文の板寄せ |
| `ref_price` | float64 | 予想約定の算出の起点となる価格（円。`reference_price` を参照） |
| `ind_match_price` | float64 | 予想約定価格（円） |
| `paired_qty` | uint64 | その価格で約定する株数 |
| `side` | string | その価格で売りが買いを上回る（または何も約定しない）ときは 'A'、それ以外は 'B' |
| `market_buy_qty` | uint64 | 成行買い注文（株数） |
| `market_sell_qty` | uint64 | 成行売り注文（株数） |
| `market_imbalance_qty` | uint64 | 成行買いと成行売りの差の絶対値（株数） |
| `market_imbalance_side` | string | 'B' 成行の買いが多い、'A' 売りが多い、'N' 同数 |
| `bid_total_qty` | uint64 | 板全体の買い気配の株数 |
| `ask_total_qty` | uint64 | 板全体の売り気配の株数 |
| `upper_collar` | float64 | その日の値幅制限の上限（円） |
| `lower_collar` | float64 | その日の値幅制限の下限（円） |
| `auction_time` | timestamp (ns, UTC) | 特別気配のタイマーの終了時刻。特別気配がなければ null |
| `sequence` | uint64 | BRiSK のフレーム番号 |

### `brisk-quotes`

| 列 | 型 | 意味 |
| --- | --- | --- |
| `bids` | list of {price, size, count} | 買い板の上位10本（最良が先頭）：{price（円）、size（株数）、count（注文件数）} |
| `asks` | list of {price, size, count} | 売り板の上位10本（最良が先頭）：{price、size、count} |
| `flags` | uint8 | 16。セッションで各銘柄の最初の更新には 32 が加わります（スナップショット） |
| `sequence` | uint64 | BRiSK のフレーム番号 |
| `last_price` | float64 | 直近の約定価格（円）。BRiSK が値を示さないときは null |
| `open_price` | float64 | セッションの始値（円） |
| `bid_price` | float64 | 最良買い気配（円） |
| `ask_price` | float64 | 最良売り気配（円） |
| `indicative_price` | float64 | 板寄せの予想約定価格（円）。引け条件付き注文を含まない |
| `closing_indicative_price` | float64 | 引け条件付き注文の予想約定価格（円） |
| `indicative_open_price` | float64 | 予想始値（円） |
| `reference_price` | float64 | 予想約定の算出の起点：特別気配の価格、なければ直近の約定価格、それもなければ基準値（円） |
| `max_frame` | uint64 | この更新の時点で BRiSK が受け取っていた最新のフレーム |
| `indicative_volume` | uint64 | 予想約定価格で約定する株数 |
| `indicative_imbalance_side` | uint64 | 予想約定価格で売りが買いを上回る（または何も約定しない）ときは 1、それ以外は 0 |
| `closing_indicative_volume` | uint64 | 引け条件付き注文についての同じ値 |
| `closing_indicative_imbalance_side` | uint64 | 引け条件付き注文についての同じ値 |
| `quote_flag` | uint64 | 特別気配のフラグ（BRiSK の値のまま） |
| `quote_side` | uint64 | 特別気配の側（BRiSK の値のまま：0 は買い、1 は売り） |
| `volume` | uint64 | その日の累計出来高（株数） |
| `issue_status` | uint64 | 証券会社の銘柄の状態（BRiSK の値のまま） |
| `market_buy_quantity` | uint64 | 成行買い注文（株数） |
| `market_sell_quantity` | uint64 | 成行売り注文（株数） |
| `closing_market_buy_quantity` | uint64 | 引け条件付きの成行買い注文（株数） |
| `closing_market_sell_quantity` | uint64 | 引け条件付きの成行売り注文（株数） |
| `bid_quantity` | uint64 | 最良買い気配の株数 |
| `bid_orders` | uint64 | 最良買い気配の注文件数 |
| `ask_quantity` | uint64 | 最良売り気配の株数 |
| `ask_orders` | uint64 | 最良売り気配の注文件数 |
| `bid_total_quantity` | uint64 | 板全体の買い気配の株数 |
| `bid_total_orders` | uint64 | 板全体の買い気配の注文件数 |
| `bid_closing_total_quantity` | uint64 | 引け条件付きの買い気配の株数 |
| `bid_closing_total_orders` | uint64 | 引け条件付きの買い気配の注文件数 |
| `ask_total_quantity` | uint64 | 板全体の売り気配の株数 |
| `ask_total_orders` | uint64 | 板全体の売り気配の注文件数 |
| `ask_closing_total_quantity` | uint64 | 引け条件付きの売り気配の株数 |
| `ask_closing_total_orders` | uint64 | 引け条件付きの売り気配の注文件数 |
| `special_quote_time` | timestamp (ns, UTC) | 特別気配のタイマーの終了時刻。特別気配がなければ null |
| `turnover` | float64 | その日の累計売買代金（円） |
| `closing_bids` | list of {price, size, count} | 引け条件付き注文の買い板。`bids` と同じ形式 |
| `closing_asks` | list of {price, size, count} | 引け条件付き注文の売り板 |

### `ohlcv-1s`, `ohlcv-1m`, `ohlcv-1h`, `ohlcv-1d`

| 列 | 型 | 意味 |
| --- | --- | --- |
| `ts_event` | timestamp (ns, UTC) | バーの開始時刻（日本時間に合わせた区切り。1日のバーは取引日の日本時間0時から始まります） |
| `open` | float64 | 最初の約定価格（円） |
| `high` | float64 | 高値（円） |
| `low` | float64 | 安値（円） |
| `close` | float64 | 最後の約定価格（円） |
| `volume` | uint64 | 約定した株数 |

バーが存在するのは約定のあった区間だけで、自分のアップロードだけから作られます。バーを受け取れるのは、
自分のアップロードがその銘柄と、取引時間（前場 09:00〜11:30 と後場 12:30〜15:30、日本時間）内のその
バーの全期間をカバーしている場合だけです。`XTKS.BRISKDEMO` のバーは誰でも使えます。

### CSV と JSON

`encoding="csv"` または `"json"`（1行に JSON オブジェクト1つ）でも、同じ列が得られます。

- **タイムスタンプ**は ISO 8601 の文字列で、`pretty_ts=false` では UTC のナノ秒です。
- **価格**は円単位の数値で、`pretty_px=false` では 1e-9 円単位の整数です。
- **`brisk-quotes` の板の列**は JSON 配列です。JSON ではそのまま、CSV では JSON のテキストとして入ります。
- **`compression=zstd`** はレスポンス全体を圧縮します。

## 手元とクラウドの名前

`Ticker` と `Market`（ライブフィードか読み込んだ記録に対するもの）とクラウド API は、同じデータを
異なる名前で返します。手元の呼び出しは BRiSK の名前のまま、円と東京の時刻で返し、クラウド API は
Databento の名前に従います。

| 手元（`Ticker`、`Market`） | クラウド API | 備考 |
| --- | --- | --- |
| `code` | `symbol`（`definition` では `raw_symbol`） | JPX の銘柄コード |
| `issue_id` | `instrument_id` | 手元ではその日の銘柄マスター内の番号、クラウドではコードごとに固定 |
| `time` | `ts_event` | 手元では日本時間の `datetime`、クラウドでは UTC のナノ秒（`to_df(tz="Asia/Tokyo")`） |
| `received_unix_ms`（`Market().frames()`） | `ts_recv` | 記録したコンピューターがフレームを受信した時刻 |
| `frame` | `sequence` | BRiSK のフレーム番号 |
| `trades()`: `price`, `quantity`, `side` (`buy`, `sell`, None) | `trades`: `price`, `size`, `side` (`B`, `A`, `N`) | |
| `bids`, `asks`: `[{price, quantity, orders}]` | `mbp-10`: `bid_px_NN`, `bid_sz_NN`, `bid_ct_NN`（`ask_…` も同様）; `brisk-quotes`: `bids`, `asks` を `[{price, size, count}]` として | 最良の本が先頭 |
| `indicative_price`, `closing_indicative_price` | `imbalance`: `ind_match_price`（`auction_type` が `O` と `C` の行） | |
| `indicative_volume`, `closing_indicative_volume` | `imbalance`: `paired_qty` | |
| `indicative_imbalance_side`（1 または 0） | `imbalance`: `side`（`A` または `B`） | 1 は `A`：売りが買いを上回る |
| `market_buy_quantity`, `market_sell_quantity`（と `closing_…`） | `imbalance`: `market_buy_qty`, `market_sell_qty` | |
| `auction()`: `market_order_imbalance`（買い − 売り） | `imbalance`: `market_imbalance_qty` と `market_imbalance_side` | 手元では符号付き、クラウドでは数量と側に分かれる |
| `bid_total_quantity`, `ask_total_quantity`（と `bid_closing_…`） | `imbalance`: `bid_total_qty`, `ask_total_qty` | 引け条件付きの値は `C` の行に入る |
| `reference_price` | `imbalance`: `ref_price` | |
| `special_quote`: `{side, price, until}` | `imbalance`: `auction_time`; `brisk-quotes`: `quote_side`, `special_quote_time` | |
| `stocks()`: `name`, `lot_size`, `base_price`, `limit_up`, `limit_down`, `tick_type`, `issue_type` | `definition`: `name`, `min_lot_size_round_lot`, `base_price`, `high_limit_price`, `low_limit_price`, `tick_type`, `issue_type` | |
| その他の気配の項目（`last_price`、`volume`、`turnover` など） | `brisk-quotes`、同じ名前 | |

OHLCV バー（`ohlcv-1s` 〜 `ohlcv-1d`）は、クラウド API から、または `briskapi.Live` でライブで得られ、
手元にはありません。`Ticker.candles()` は別のもので、
証券会社の BRiSK 自身のバーです。

## 記録

記録は、`data/order_book_depths.parquet`、`data/trades.parquet`、`data/equity.parquet`、
`data/custom/BriskBatch.parquet`（パッケージ化されると `manifest.json` も加わります）を収めた
フォルダーです。テーブルは NautilusTrader v2 のスキーマに従い、その後ろに BRiSK の項目が続きます。
行は銘柄（issue）、次にバッチの順に並び、65,536行ずつの行グループに分かれているので、1銘柄を読む
ときはその部分だけが読まれます。`briskapi.Recording(path)`、`Ticker`、`Market` がこれらを読んで
くれます。`brisk export DIR --nautilus CATALOG` は NautilusTrader v2 が読み込めるカタログを書き出します。

| ファイル | 1行の単位 | 列 |
| --- | --- | --- |
| `data/order_book_depths.parquet` | 気配更新 | `bids`, `asks`（最大10本、最良が先頭：`price`, `size`, `count`、および v2 の `order_id`（常に 0））, `flags`, `sequence`（フレーム）, `ts_event`, `identifier`（`CODE.XTKS`）, `issue_id`, `seq`（バッチ）。続いて、上記の `brisk-quotes` と同じ気配の項目（価格は decimal）。briskapi 1.9 以降の証券会社の記録では、さらに `changes`（銘柄の直前の更新から4種類の板の何が変わったか：`book` は 0 が買い板、1 が売り板、2 と 3 が引け条件付き注文の板、`price`, `size`, `count`。空になった価格帯は 0 株。最初の更新は板全体を持つ）と `checkpoint`（null。ただし銘柄の変更 1,000 件ごとに、その時点の板全体を同じ形で） |
| `data/trades.parquet` | 約定 | `price`, `size`, `aggressor_side`（`BUY`, `SELL`, `NO_AGGRESSOR`）, `trade_id`, `ts_event`, `identifier`, `issue_id`, `seq`, `frame`, `side`（BRiSK の種別そのまま：1 は買い手が仕掛けた、2 は売り手が仕掛けた） |
| `data/equity.parquet` | 銘柄 | v2 の `Equity`: `id`（`CODE.XTKS`）, `raw_symbol`（銘柄コード）, `currency`, `price_precision`, `price_increment`, `lot_size`, `max_price`/`min_price`（その日の値幅制限）, `ts_event`/`ts_init`（日本時間0時）。続いて `issue_id`, `tick_type`, `issue_type`, `name`, `base_price` |
| `data/custom/BriskBatch.parquet` | デコードしたバッチ（フレーム） | `seq`, `kind`（`bootstrap`, `history`, `quotes`, `end`）, `received_unix_ms`（ローカルの受信時刻）, `decode_ns`, `replay_lateness_ms`, `skipped_issues`, `frames`, `quote_updates`, `replay_wall_ms`（クラッシュ後に修復した日では、`end` の `frames` と `replay_wall_ms` は記録から組み立て直したもの）。`bootstrap` の行には `source`, `trading_date`, `input_transport`, `market_issue_count` |

受信時刻は `BriskBatch` にあります。行の `seq` をバッチの `received_unix_ms` に結合してください。更新の表の時刻は
`ts_event` の1つだけです。briskapi 1.5 より前の記録（形式 `brisk-nautilus-parquet-v2`）にはその写しの `ts_init`
もあり、Nautilus への書き出しは NautilusTrader のためにそれを書きます。
`history` のバッチには、セッションのスナップショットがその日の早い時間から復元した約定が入っていて、
すべて `bootstrap` の時刻になっています。`brisk repair` で補修した記録には、ほかのバッチに混じって、別のフィードの
記録から補った更新が、それぞれ独自のバッチとして入ります。記録が動いている間に取りこぼした更新は、その銘柄の次の更新の
バッチのすぐ前に入り、そのバッチの `received_unix_ms` と、隣のバッチの間に収まるよう決めた市場時刻が付くので、銘柄の
更新は市場時刻でも、フレーム番号でも、受信時刻の順でも前へ進みます。その銘柄について記録が持つものより後の更新や約定は、
相手の記録の `received_unix_ms`（別のコンピューターの時計）が付いたバッチに入るので、そこでは受信時刻が連続しているとは
限りません。

### 読み方

```python
import pandas as pd
trades = pd.read_parquet("recordings/s1/data/trades.parquet")
toyota = trades[trades["identifier"] == "7203.XTKS"]

import polars as pl
books = pl.read_parquet("recordings/s1/data/order_book_depths.parquet").filter(pl.col("identifier") == "7203.XTKS")

import duckdb
duckdb.sql("""
    select t.identifier, t.price, t.size, t.ts_event, b.received_unix_ms
    from 'recordings/s1/data/trades.parquet' t
    join 'recordings/s1/data/custom/BriskBatch.parquet' b using (seq)
    order by t.ts_event
""")
```

decimal は pandas では `decimal.Decimal` として読み込まれます。`pd.read_parquet(...).astype({"price": float})`
や Polars の `cast(pl.Float64)` で浮動小数点数になります。
