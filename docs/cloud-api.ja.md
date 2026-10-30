# クラウド API

[English](cloud-api.md) | [日本語](cloud-api.ja.md)

アップロードした人それぞれの自分専用のアーカイブに対する、Databento 風の履歴データ API です。
記録全体をダウンロードせずに、データセット、銘柄、スキーマ、期間を指定して、日をまたいで自分の
記録を照会できます。状況: フェーズ 1 は 1.0 向けに実装済みで（[フェーズ](#フェーズ)を参照）、
フェーズ 2 と 3 は計画です。`infra/deploy.py` が `brisk-recorder-query` 関数をデプロイすると応答します。
以下の Databento に関する事実は、2026-10-13 に Databento のドキュメントと `databento-python` の
ソースで確認しました。コードは `query_engine.py`、`query_service.py`、`briskapi/_historical.py` で、
スキーマとその射影は `briskapi/records.py`（`briskapi.Live` と共通）にあります。
内部の仕組みは [ARCHITECTURE.md](../ARCHITECTURE.md)（英語）、使い方は [README](../README.ja.md) に
あります。

決定事項（2026-10-13）:
- Databento のバイナリ形式 DBN ではなく、独自の形式（Parquet、CSV、JSON）を使います。
- 無料で、上限を設けます。
- 自分のアーカイブの ID が API キーです。
- データセットは `XTKS.BRISK` と `XTKS.BRISKDEMO` です。
- OHLCV バーは取引日ごとに、すべてのアップロードから1回作ります（共通バー）。ID が受け取るのは、
  自分のアップロードがカバーするバーだけです（フェーズ 1 に前倒ししました）。
- （1.0.0 の後）各 owner のバーは、その owner 自身のアップロードだけから、照会に先立って作ります。
  共通バーでは、でっち上げのアップロードによって、ほかの owner のバーを受け取れてしまううえ、
  書き換えることもできたためです（[OHLCV バー](#ohlcv-バー)を参照）。
- リクエストは `ts_event` で絞り込み・並べ替えを行い、タイムゾーンのない時刻は日本時間として扱います。

## 目標

- **自分のデータを照会できる。** 1回のリクエストで、たとえば1週間分の自分の BRiSK のセッションにわたる
  トヨタの約定や10本の板を、統合し時刻順に並べた、DataFrame にそのまま使える表として返します。
- **Databento と同じ形。** エンドポイント、パラメーター、データセットの命名、レコードのスキーマを
  同じにして、コードも使い慣れた操作もそのまま引き継げるようにします。
- **設計の段階から非公開。** リクエストが届くのは、その owner の記録とそこから作ったバー、そして
  どの ID からでも使える公開デモだけです。複数の owner のアップロードを組み合わせて何かを作ることは
  ありません。
- **低コストで運用できる。** サーバーレスで、リクエストに必要な Parquet の行グループだけを読み、
  データベースはありません。

対象外: クラウドからのライブストリーミング（ライブのレコードは、お使いのコンピューターのフィードから
`briskapi.Live` で、バーも含めて同じスキーマで届きます）、ほかの owner の記録、SQL、当面は DBN も。

## Databento の履歴データ API と、実装した範囲

ベース URL は `https://hist.databento.com/v0/` で、API キーは HTTP Basic 認証のユーザー名（パスワードは
空）です。こちらは関数 URL で、`/v0/` 以下のパスもキーの扱いも同じです（Bearer トークンも使えます）。

| エンドポイント | 用途 | ここでの対応 |
| --- | --- | --- |
| `POST timeseries.get_range`（フォーム本文） | データ本体。`dataset`、`symbols`（カンマ区切り、最大 2,000、または `ALL_SYMBOLS`）、`schema`、`start`（含む）、`end`（含まない）、`stype_in`、`stype_out`、`limit`、`encoding`（`dbn`、`csv`、`json`）、`compression`、CSV/JSON では `pretty_px`、`pretty_ts`、`map_symbols`。スキーマに `ts_recv` があればそれで、なければ `ts_event` で絞り込む。 | 実装済み（POST フォームまたは GET）。`stype_out` はなく（`stype_in` は `raw_symbol` のみ）、`dbn` もない。`encoding` は `parquet`（既定）、`csv`、`json`。`ts_event` で絞り込む。 |
| `GET metadata.list_datasets`、`list_schemas`、`list_fields`、`list_publishers` | カタログ。パブリッシャーはデータセットと取引所を対応付ける: `{publisher_id, dataset, venue, description}`。 | 実装済み。`list_fields` は各列の名前、型、意味を返す（[data-format.ja.md](data-format.ja.md)）。 |
| `GET metadata.get_dataset_range`、`get_dataset_condition` | 利用できる日付、日ごとの品質。 | 実装済み。範囲（range）はデータのある最初と最後の日付、状態（condition）は日付ごとの `available` か `missing` で、自分の記録がカバーする時間帯（日本時間）が付く。 |
| `GET metadata.get_record_count`、`get_billable_size`、`get_cost` | 実行前にリクエストのサイズと料金を知る。 | `get_record_count` はクエリを実行して行数を数える。`get_cost` は 0。`get_billable_size` はフェーズ 3。 |
| `GET symbology.resolve` | ある期間にわたって、銘柄の種別（`raw_symbol`、`instrument_id` など）を相互に変換する。 | フェーズ 2。 |
| `POST batch.submit_job`、`GET batch.list_jobs`、`batch.list_files`（+ ダウンロード） | 大きなリクエストを非同期のジョブとして実行し、ファイルを後でダウンロードする。 | フェーズ 3。 |

データセット名は `VENUE.FEED` の形で、取引所の ISO 10383 市場識別コード（MIC）のあとにフィードが
続きます。例: `XNAS.ITCH`（Nasdaq TotalView-ITCH）、`GLBX.MDP3`（CME Globex MDP 3.0）、`XNYS.PILLAR`、
`XCIS.TRADESBBO`。レコードには `ts_event` と、取得できている場合は `ts_recv`（UTC のナノ秒）、
`instrument_id`（uint32）、`publisher_id` が含まれます。

## データセット、銘柄、インストゥルメント

この命名に従い、取引所は東京証券取引所（MIC は `XTKS`。NautilusTrader のインストゥルメント ID の
`7203.XTKS` などと同じです）、フィードは BRiSK です。

| データセット | データ | 照会できる人 |
| --- | --- | --- |
| `XTKS.BRISK` | アップロードしたすべてのライブ記録（どの証券会社の BRiSK 経由でも）と、そこから作った OHLCV バー | 自分のみ |
| `XTKS.BRISKDEMO` | 公開 BRiSK Next デモ（東証、2021-09-27）。アーカイブが1つだけ持つ検証済みのコピーから | すべての ID |

- **ライブのデータセットは1つ。** 記録がどの証券会社のサイトを通ったかは関係ありません。どのサイトも
  同じ BRiSK アプリを動かしており、起動情報（boot）、マスター、スナップショット、デコーダーも同じものが
  配信されます（2026-10-08 に確認）。同じ日の重なった記録は、どのサイト経由でも統合され、重複が
  取り除かれます（下記を参照）。
- **公開デモはすべての ID に開かれています。** `XTKS.BRISKDEMO` は、全員のために1つだけ保持している
  市場データのテーブル `reference/historical_mock/TIMELINE/` を読むので、再生をアップロードしたかどうかに
  かかわらず、どの ID でも照会できます。再生にはローカルの受信時刻として意味のあるものがないため、
  レコードの `ts_recv` は `ts_event` と同じ値になります（`ts_recv = ts_event`）。このコピーは、いずれかの
  ID による、検証済みの記録全体のデモの最初のアップロードとともに作られます。デプロイ時には
  `deploy.py --seed-demo PACKAGE` で作ります。それまでは `XTKS.BRISKDEMO` は HTTP 404 を返します。
  バーはデモの約定からその場で作り、カバー範囲による制限（ゲート）はかけません。
- `list_datasets` は常に両方のデータセットを返します。`list_publishers` は Databento と同じ形式です。

  | `publisher_id` | `dataset` | `venue` | `description` |
  | --- | --- | --- | --- |
  | 1 | `XTKS.BRISK` | `XTKS` | `Tokyo Stock Exchange via BRiSK` |
  | 2 | `XTKS.BRISKDEMO` | `XTKS` | `Tokyo Stock Exchange, BRiSK Next demo replay` |

- `raw_symbol` は JPX の銘柄コード（`7203`、`167A`）で、4文字または5文字の 0〜9 と A〜Z です。
  `instrument_id` は日をまたいで同じでなければなりませんが、記録の `issue_id` はその日のマスターの中での
  位置を示す番号にすぎません。そのため ID は銘柄コードから導きます。
  - 数字だけのコードは、その数値になります（トヨタは 7203 のまま）。
  - 英数字のコード（JPX は 2024年から発行しています）は、100,000 にコードの36進数の値を足したものです
    （`167A` → 100,000 + 54,694 = 154,694）。

  コードは最大5文字なので、この値は一意で、uint32 に収まります。`symbology.resolve`（フェーズ 2）は
  日付ごとに両方を返す予定です。

## BRiSK の記録から作るスキーマ

必要なものはすべて記録に含まれているので、各スキーマは、記録から必要な列を取り出すか、少し計算して
導いたものです。フィールド名は Databento のレコード定義に従うので、`trades` や `mbp-10` の DataFrame には
Databento のユーザーが期待する列が並びます。バー以外のスキーマはすべて、`ts_recv`、`ts_event`、
`publisher_id`、`instrument_id` で始まり、`symbol` で終わります。

| スキーマ | 元 | 対応 |
| --- | --- | --- |
| `trades` | `data/trades.parquet` | `action` は T。価格、数量。`side` は仕掛けた側から決める（`B` は買い手主導、`A` は売り手主導、それ以外は `N`。寄付きの約定の多くがこれ）。`sequence` = フレーム。`trade_id` |
| `mbp-10` | `data/order_book_depths.parquet` | 気配の更新ごとに、更新後の各側の上位10本を `bid_px_00`、`bid_sz_00`、`bid_ct_00` … `ask_ct_09` に展開（その側の最後の本より先は null）。BRiSK が返すのは板であって、板を変えた注文ではないため、`action` と `side` は `N`、`depth` は 0、`price` は null、`size` は 0。最初のバッチ（ブートストラップ）の行にはスナップショットのフラグ（`flags` 32）が付く。 |
| `book-deltas` | `data/order_book_depths.parquet` の `changes` と `checkpoint` | 両方の板（買い・売り、通常と引け条件付き）のすべての価格帯を一日を通して、Databento の MBO のレコードを価格帯の粒度にしたもので表す（BRiSK に注文 ID はない）。`action` は `A` 価格帯が現れる、`M` 変わる、`C` 空になる、`R` スナップショットの前に銘柄の板を消去する。`side`、`closing`、`price`、変更後の `size` と `count`（注文件数）。`flags` は更新の最後のレコードに 128、スナップショットのレコードに 32。記録の中で銘柄の最初の更新はスナップショット（`R` と価格帯）。リクエストごとに、エンジンは行グループの統計とチェックポイントの列から、`start` より前の各銘柄の最後のチェックポイント（パッケージ化のときに変更 1,000 件ごとに板全体を書く）を見つけ、多くともその数の変更を再生して、範囲内のレコードを返す。`snapshot=true` では、`start` 時点の各銘柄の板を先頭に置く。同じ日の重なる記録は統合しない（記録の変更はそれぞれその記録の直前の更新からのもの）。最も長い記録を読む。1.9 以降の証券会社の記録。デモにはない。 |
| `imbalance` | 板寄せの項目 | BRiSK ならではのデータ。更新ごとに、寄付きの予想約定価格が表示されていれば寄付き（`O`）の行、引けの予想約定価格が表示されていれば引け（`C`）の行を、`auction_type` に出す。`ind_match_price` = 予想約定価格、`paired_qty` = 予想約定数量、`market_imbalance_qty`/`_side` = 成行の買い数量 − 売り数量、`bid_total_qty`/`ask_total_qty` = 板の合計（`C` では引けのもの）、collar = その日の値幅制限の価格、`auction_time` = 特別気配のタイマー。 |
| `definition` | `data/equity.parquet` | 記録ごと・銘柄ごとに1行。タイムスタンプはその取引日の日本時間 0時: 銘柄コード、銘柄名、売買単位、呼値の単位、基準値、値幅制限、銘柄の種別と呼値の種別 |
| `brisk-quotes`（独自） | depths のすべての列 | 更新ごとに BRiSK が記録するすべての項目: 引け条件付き注文の板、板の合計、特別気配の時刻（小数は float64） |
| `ohlcv-1s/1m/1h/1d` | 約定 | Databento と同様に約定から集計。[下記](#ohlcv-バー)を参照 |

フェーズ 2 の予定:

| スキーマ | 元 | 対応 |
| --- | --- | --- |
| `mbp-1`、`bbo-1s/1m`、`tbbo` | 同じ depths | 最上位の本。一定間隔のサンプリング。各約定とその直前の板 |
| `status` | `issue_status`、特別気配 | 取引状態の変化（特別気配の開始・解除、BRiSK が報告する売買停止） |
| `statistics` | 始値、直近の価格、出来高、売買代金 | 適合するものは Databento の `stat_type` を使い、売買代金は BRiSK 独自の拡張として追加 |

**エンコーディング。**
- `parquet`（こちらのクライアントの既定）: タイムスタンプは UTC のナノ秒（`timestamp[ns, UTC]`）、
  価格は float64 の円。常に zstd で、`compression` は適用されません。
- `csv` と `json`（1行に1レコード）。Databento と同じ切り替えとして、`pretty_px`（価格を円の数値で出力。
  オフにすると 1e-9 円単位の整数で、Databento の固定小数点）、`pretty_ts`（ナノ秒付きの ISO 8601 形式の
  UTC タイムスタンプ。オフにすると整数のナノ秒）、`map_symbols`（銘柄コードを残す）があります。
  `brisk-quotes` の入れ子の列は、CSV では JSON テキストになります。
- `compression=zstd` は CSV と JSON で任意に指定できます。

**タイムスタンプと時刻。**
- `ts_event` は、フレームに対する BRiSK のサーバーのタイムスタンプです（約定では、その約定自身の
  時刻）。これが取引所のイベント時刻かどうかは確認できていません（ARCHITECTURE.md を参照。英語）。
  ドキュメントにもそのことを明記しなければなりません。
- `ts_recv` は、アップロードした人のコンピューターでの受信時刻（フレームのバッチの
  `received_unix_ms`、ミリ秒精度）です。ない場合は `ts_event` になります。
- リクエストは、Databento のように `ts_recv` ではなく `ts_event` で絞り込み・並べ替えを行います。
  同じ日の重なった記録どうしは BRiSK のタイムスタンプが一致し、受信時刻だけが異なるので、同じ範囲を
  指定すれば、どの記録に含まれていても同じ行が返ります。同時刻のものは `instrument_id`、次に
  `sequence` の順に並べます。
- 複数の記録に含まれるレコードは、その受信時刻のうち最も早いものを持ちます。どの記録のプログラムでも、
  それを知り得た最初の時点です。
- `start` と `end` は ISO 8601 形式の日付・日時、または整数です。日付は、その日本時間の取引日の0時を
  表します（つまり、`end` に日付を指定するとその日は含まれません）。タイムゾーンのない時刻は日本時間で、
  `Z` やオフセットがあればそれに従います。10桁以上の整数は UTC のナノ秒です。`end` は含まず、省略すると
  start の取引日の終わりまでです。1回のリクエストが対象にできる日本時間の取引日は最大31日です。

## OHLCV バー

`Historical` のバーはクラウドでのみ得られます。エンジン（`query_engine.py`）はサーバー側にあり、`briskapi` の
wheel には含まれていません（`briskapi.Live` は、お使いのコンピューターでストリームから同じ規則でバーを作ります）。

- **作り方。** 約定から、銘柄と間隔ごとに作ります。間隔は日本時間に揃えます（1日のバーは日本時間の
  0時から次の0時まで）。バーの `ts_event` はその開始時刻で、約定のない間隔にはバーがありません。
  始値と終値は、`ts_event`、次にフレーム、次にフレーム内の位置の順で並べた最初と最後の約定です。
  `volume` は数量の合計（uint64）です。板寄せの約定も、ほかの約定と同じ約定として扱います。
  列: `ts_event`、`publisher_id`、`instrument_id`、`open`、`high`、`low`、`close`、`volume`、`symbol`。
- **owner ごとに、あらかじめ作る。** `XTKS.BRISK` では、ある取引日の owner のバーを、その owner 自身の
  その日の現在のライブ記録から作ります。各約定は1回だけ数え（銘柄と約定 ID で重複を除去）、
  `bars/OWNER/YYYYMMDD/ohlcv-1s|1m|1h|1d.parquet` に書き出します。銘柄順に並べてあるので、照会では
  対象の銘柄の行グループだけを読みます。アーカイブのサービスは、それらの記録のいずれかが保存、削除、
  復元されたときにバーを作り直します。記録が1つも残らなければファイルは削除され、作り直しに失敗した
  場合もファイルを削除します（その場合、照会は記録からバーを作るので、遅くなりますが結果は同じです）。
- **カバー範囲。** 1日分のファイルに入るのは、バーの期間を東京証券取引所の取引時間（日本時間の
  09:00〜11:30:01 と 12:30〜15:30:01。半開区間で、余分な1秒には引けの板寄せの約定が入ります）で切り取った
  範囲が、その銘柄を含む owner の記録の、統合した時刻の範囲（最初から最後のソース時刻）にすべて
  収まっているバーだけです。それらが一部しかカバーしていないバーは、約定が欠けてしまうからです。
  どの取引時間にも入らないバーは、期間全体で判定します。たとえば 08:55 から 11:31 までの記録なら、
  午前の1分と1時間のバーは得られますが、1日のバーは得られません。つまり、バーを得るにはアップロードが
  必要です。
- **自分のアップロードだけ。** カバー範囲の中では、owner 自身の記録にすべての約定が含まれています。
  取り込み（ingest）の際に、気配と気配のあいだの各銘柄の累計出来高と売買代金の変化を、約定がすべて
  説明できていない記録は拒否されるので、約定を取りこぼした記録が保存されることはありません。
  ほかの owner の記録は、そこには何も足せず、食い違うだけです。1.0.0 では、すべての owner の記録から、
  1日あたり1組の共通バーを作っていました。そのため、でっち上げの記録が1日全体のカバー範囲を主張して
  全員のバーを受け取ったり、並べ替えで先頭になる owner ID からのアップロードで、全員のバーに含まれる
  実際の約定を置き換えたりできました（`test_no_upload_reveals_or_changes_another_owners_bars`）。
- **速度。** 3,774銘柄で約定が 1,000,000 件ある日の場合（ローカルで、S3 の遅延なし）: owner の4つの
  ファイルを作るのに約 0.8 秒かかります。照会は、1銘柄の1分のバーを約 1 ms、全銘柄では約 60 ms で
  読みます（1秒のバーでは 2 ms と 140 ms）。これに対し、リクエストのたびに記録からバーを作ると約 0.5 秒
  かかります。1.0.0 は照会時に、銘柄ごとに1回の処理でカバー範囲を適用していて、その日の4種類のバーで
  約 25 秒かかりました。アップロードする人に向けたバーの説明は [PRIVACY.ja.md](../PRIVACY.ja.md)
  （ポリシーバージョン 7）にあります。

## リクエストの流れ

```
client ──HTTPS──▶ Lambda function URL "brisk-recorder-query" (Python 3.12 arm64, 3,008 MB, pyarrow layer)
                    │ 1 authenticate: the identity (Basic username or Bearer) → owner ID; count the request
                    │ 2 plan: for each JST trading date in [start, end), list archive/OWNER/DATE/ and read
                    │   each live recording's manifest and files.json
                    │   (XTKS.BRISKDEMO: the reference copy instead)
                    │ 3 resolve symbols per recording from its equity table (code → issue_id)
                    │ 4 read: pyarrow over S3 ranged GETs (4 MiB blocks), only row groups whose issue_id and
                    │   ts_event statistics can match (tables are sorted by issue, then batch);
                    │   bars: the owner's built file for the day, row groups pruned by symbol
                    │ 5 merge recordings, deduplicate, sort by ts_event, project to the schema, encode
                    │ 6 count the result's bytes against the daily quota
                    ▼
              ≤ 4 MiB: inline response (Parquet as base64)
              larger: results/OWNER/<uuid>.<ext> in S3, then 303 to a 15-minute presigned URL
```

- **同じ日の複数の記録**（再起動、2台のコンピューター、2つのサイト）は重なることがあります。行は統合し、
  最初のものを残して重複を除去します。キーは、`trades` では（銘柄コード、約定 ID）、`definition` では
  （銘柄コード、時刻）、`imbalance` では（銘柄コード、フレーム、板寄せの種別）、それ以外は（銘柄コード、
  フレーム）です。BRiSK のフレーム番号はサーバーが付けたものなので、ストリームの重なった記録は
  一致するはずです。ライブデータで、サイトをまたいだ場合も含めて確認する必要があります。
- **行グループの絞り込み。** Parquet のページインデックス（`write_page_index`）を書き出せば、1銘柄の
  読み取りはさらに細かく絞り込めます。ただし正規のエンコーディングが変わるため、クライアントと
  サービスでスキーマバージョンを同時に上げる必要があります。
- **ストリーミングではなく 303 にする理由。** 関数 URL がバッファリングして返せるのは最大 6 MB で、
  base64 にすると Parquet は3分の1大きくなるため、インラインは 4 MiB までです。レスポンスの
  ストリーミングには Node.js ランタイムかカスタムランタイムが必要で（再確認が必要）、このコードは
  Python と pyarrow です。S3 への 303 ならどんなサイズでも返せ、HTTP クライアントはそれをたどります。
  `requests` はホストをまたぐリダイレクトで Authorization ヘッダーを外します。これは S3 の署名付き URL が
  求める動作で、`briskapi.Historical` はキーを付けずに自分でリダイレクトをたどります。結果は1日後に
  期限切れになり（ライフサイクル）、大きな結果はそれぞれ別のオブジェクトになります。
- **Lambda の制限。** 1リクエストあたり 300 秒と 3,008 MB（アカウントの上限）です。それを超えるものは
  バッチジョブになります（フェーズ 3）。範囲内に記録がない場合、結果には共通の列と `symbol` だけが
  入ります。

## 認証と分離

- **ID が API キーです。** HTTP Basic 認証のユーザー名（パスワードは空）として送るか（Databento の
  慣習なので `curl -u $KEY:` が使えます）、`Authorization: Bearer` として送ります（アーカイブ API と
  同じです）。別にキーを作る必要はありません。`brisk identity --export` で表示でき、
  `briskapi.Historical()` はアーカイブと同じ方法で見つけます（`BRISK_IDENTITY`、資格情報ストア、または
  ファイル）。同じキーでアップロード、削除、復元もできるので、パスワードと同じように保管してください。
  キーの登録簿はありません。形式が正しい ID ならどれでも認証を通りますが、記録のない ID から見えるのは
  デモだけです。キーがない、または形式が正しくない場合は HTTP 400 です。キーの推測は、キーを読む前に
  数えるアドレスごとの上限で制限されます。2^128 通りの秘密の値の中から、記録のあるものを見つけるのは
  現実的に不可能です。クライアントは HTTPS でしかキーを送らず、`https` の結果への 303 は、キーなしで
  たどります。
- **分離。** 照会関数のロールが読めるのは `archive/*`、`reference/*`、`bars/*`、`results/*`、書けるのは
  `results/*` だけで、それ以外にはアクセスできません。自分の記録とバーへのパスはすべて、認証済みの
  owner ID から組み立てられ、リクエストの入力から作られることはありません。リクエストが読むもう1つの
  プレフィックスは、`XTKS.BRISKDEMO` のためのデモの参照コピーです。

## 上限とコスト

- **無料。ただし上限あり:** 1つの ID あたり、1分に60リクエスト、UTC の1日に 2 GB まで返します。
  ID は無料でいくつでも作れるので、1つの IP アドレスあたり（IPv6 アドレスは /64 単位。リクエストは
  キーを読む前に数えます）、1分に120リクエスト、1日に 4 GB まで、さらに全体で1日に 20 GB までという
  上限もあります。これらはアップロードと同じ DynamoDB のカウンターを使います（上限を超えたリクエストは
  HTTP 429 になります）。1リクエストあたり最大 2,000銘柄と31日までで、`limit` も使えます。4 MiB までの
  結果はインラインで返されます。
- `get_cost` は 0 を返します。`get_record_count` はクエリを実行して実際の件数を返すため、運営者には
  クエリ1回分のコストがかかりますが、返却されるバイトはありません。`get_billable_size`（フェーズ 3）は
  Parquet のメタデータから求める予定です。
- **1リクエストあたりの運営者のコスト**（東京リージョンの定価）:
  - Lambda の arm64 は 1 GB 秒あたり約 $0.0000133 なので、3 GB で 1 秒なら約 $0.00004 です。
  - S3 の GET は 1,000 件あたり $0.00037 です。
  - データ転送（アウト）は、AWS の月 100 GB の無料枠を超えた分が 1 GB あたり約 $0.114 です。
  - 支配的なのは転送量です。毎日上限いっぱいまで使うユーザーは、月に約 $7 です。

## Python クライアント

```python
import briskapi

client = briskapi.Historical()                 # 自分の ID がキー
data = client.timeseries.get_range(dataset="XTKS.BRISK", symbols=["7203"], schema="trades",
                                   start="2026-10-13", end="2026-10-17")
data.to_df()                                   # ts_recv をインデックスにする（バーは ts_event）。Databento のほかの変換も使えます
client.timeseries.get_range(dataset="XTKS.BRISK", symbols=["7203"], schema="ohlcv-1m",
                            start="2026-10-13")                 # 自分のアップロードがカバーしている範囲のバー
client.metadata.get_dataset_condition("XTKS.BRISK", start_date="2026-10-01")   # 日ごとに記録した時間帯
client.timeseries.get_range(dataset="XTKS.BRISKDEMO", symbols=["7203"], schema="imbalance",
                            start="2021-09-27T08:59+09:00", end="2021-09-27T09:03+09:00")   # どの ID でも
```

名前もパラメーターも `databento.Historical` と同じで、`to_df()`、`to_ndarray()`、`to_csv()`、`to_json()`、
`to_parquet()`、`to_file()`、`replay()`、反復、`len()` を備えたストアを返します。`to_df()` では
`price_type`、`pretty_ts`、`map_symbols`、`tz`、`count` が使えます。通信には Parquet を使うので、新しい
依存パッケージは要りません。ゲートウェイは `archive.json` の `query_url` から取得します。

Databento のパラメーターに2つ加えています。先読みをしてはならないテストのためのもので、クライアント側の
機能です（エンドポイントは増えません）。サーバーは `ts_event` で絞り込みますが、ライブフィードのレコードは、
記録するコンピューターに少し遅れて届きます（`ts_recv`）。

```python
client.timeseries.get_range(dataset="XTKS.BRISK", schema="brisk-quotes", start="2026-10-13T08:55",
                            end="2026-10-13T08:59:58", ts_filter="ts_recv")      # 08:59:58 までに受信したもの
client.timeseries.get_snapshot("XTKS.BRISK", at="2026-10-13T08:59:58", schema="brisk-quotes")   # その時点の各銘柄の最新レコード
```

- `get_range(..., ts_filter="ts_recv", skew=10.0)` は、サーバーに対して期間の前後を `skew` 秒ずつ広げて
  要求し、こちらで `ts_recv` による絞り込みと並べ替えを行います（`limit` はそのあとに適用）。BRiSK の
  タイムスタンプが受信側の時計より `skew` の半分を超えて先行していたときは警告します（`skew` を広げて
  ください。広げないと、`end` より前に受信したのにタイムスタンプが `end` よりかなり後のレコードが欠けます）。
  OHLCV バーには使えません。
- `get_snapshot(dataset, at, symbols, schema="brisk-quotes", since=None, ts_filter="ts_recv", skew=10.0)`
  は、銘柄ごとに1行、`instrument_id` 順で、`at` 以前の最新のレコードを返します（既定の時計では、`at` までに
  受信したもののうち BRiSK のタイムスタンプとフレームが最新のもので、最後に受信したものではありません。同じ日の
  2つの記録は、銘柄のフレームを実際に起きた順と違う受信順で持つことがあるためです）。BRiSK は更新のたびに
  銘柄の状態全体を送るので、それがその時点の板です。`since`（既定: `at` の取引日の日本時間0時）以降に
  レコードがない銘柄は含まれません。取引時間中は、より遅い `since` を渡してください（渡さないと午前の
  すべてを読みます）。
- デモの再生には受信時計がなく、`ts_recv` は `ts_event` と同じです。そのため、どちらの絞り込みでも結果は一致します。
- `get_range(..., schema="book-deltas", snapshot=True)`（フォームでは `snapshot=true`）は、`start` 時点の各銘柄の
  板全体から始まります。`get_snapshot(dataset, at, symbols, schema="book-deltas")` はそれだけ（消去と価格帯、
  32 の印付き）を、BRiSK のタイムスタンプで、各銘柄の最後のチェックポイントからサーバー側で組み立てて返します。
  `since` は使いません。

コマンドラインでは、`brisk query XTKS.BRISK --schema ohlcv-1m --symbols 7203 --start 2026-10-13` が
CSV を標準出力に書きます（`--encoding json|parquet`、`--output FILE`、`--end`、`--limit`、`book-deltas` では
`--snapshot`）。素の HTTP で
呼ぶ場合は、`-L` を付けると大きな結果の 303 をたどり、その途中で curl はキーを外します。

```sh
curl -L -u "$KEY:" -d dataset=XTKS.BRISK -d schema=ohlcv-1m -d symbols=7203 \
     -d start=2026-10-13 -d encoding=csv "$QUERY_URL/v0/timeseries.get_range"
```

エラーは `{"detail": ...}` の形で、HTTP 400（入力が不正、またはキーがない）、404（データがない、または
デモのコピーがまだない）、429（上限超過）のいずれかです。

## フェーズ

1. **コア（1.0 向けに実装済み）。**
   - `trades`、`mbp-10`、`definition`、`imbalance`、`brisk-quotes`、`ohlcv-1s/1m/1h/1d` の
     `timeseries.get_range`。形式は Parquet、CSV、JSON。
   - バー: 共通バー（保存、削除、復元に合わせて更新）と、ID ごとのカバー範囲のルール（フェーズ 2 から
     前倒し）。1.0.0 の後は、各 owner 自身のバーをあらかじめ作る。
   - metadata のエンドポイント（`list_*`、`get_dataset_range`、`get_dataset_condition`、
     `get_record_count`、`get_cost`）。
   - ID による認証、インラインまたは 303 の結果、上限、`briskapi.Historical`、`brisk query`、
     デプロイ時に投入するデモの参照コピー（`deploy.py --seed-demo`）。
2. **スキーマの追加。** `mbp-1`、`tbbo`、`bbo-*`、`status`、`statistics`、そして `symbology.resolve`。
3. **大きなリクエスト。**
   - `batch.submit_job`、`list_jobs`、`list_files`: Lambda を非同期に呼び出し、結果を
     `results/OWNER/JOB/` に書き出します。
   - `get_billable_size` と Parquet のページインデックス。

将来、必要になれば: `databento-dbn` による DBN エンコーディング。これにより、公式の `databento`
クライアントからアーカイブを直接使えるようになります（`databento.Historical(key, gateway=URL)`）。
このクライアントは、空でないキーと、任意の https のゲートウェイを受け付けます。

フェーズ 1 の既知の課題:
- owner のある日のバーの作り直しは直列化されていません。実行中にその owner のその日の記録が変わった
  ことを検出した作り直しは、最大3回までやり直します。
- 重なった記録がサイトをまたいでも一致することは、ライブデータではまだ確認できていません。

## インフラ

- 2つ目の関数 `brisk-recorder-query` を追加します。専用のロール（`brisk-recorder-query`）、ロググループ、
  関数 URL を持ち、アーカイブ関数と同じ zip とレイヤーを使います。メモリーは 3,008 MB（アカウントの
  上限）、タイムアウトは 300 秒、予約済みの同時実行数は、アカウントで許可されていれば8です。取り込みは、
  owner のバーを最新の状態に保つ点を除いて変わりません。そのため、アーカイブのロールには、`bars/` 以下の
  現行のオブジェクトの読み取り、書き込み、削除の権限が加わります。
- バケット: `results/` プレフィックスに1日のライフサイクル（`ExpireResults`）を設定し、`bars/` の
  非現行バージョンは1日後に期限切れになります（`ExpireReplacedBars`）。照会ロールは `archive/`、
  `reference/`、`bars/`、`results/` を読み、`results/` に書き込みます。
- DynamoDB: 照会の上限（クォータ）は既存のテーブルで管理します。
- `deploy.py` は両方の関数をデプロイし、`archive.json` に `query_url` を書き込みます。
  `--seed-demo PACKAGE` は、運営者として完全なデモの記録をアップロードし、`XTKS.BRISKDEMO` にデータを
  持たせます（`brisk record --no-upload --output DIR` で作れます）。`cloud_smoke.py` は使い捨ての
  ID として `list_datasets` と `list_publishers` を呼び、キーのないリクエストが拒否されることを確認します。
  データの照会は行いません。合成データの確認用の記録は、どちらのデータセットにも属さないためです。
  デプロイ手順は [infra/README.md](../infra/README.md)（英語）にあります。
