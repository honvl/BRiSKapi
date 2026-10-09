# briskapi

[English](README.md) | [日本語](README.ja.md)

BRiSK の板寄せデータを扱う、非公式の pybrisk 風 Python API と `brisk`
コマンドラインツールです。ライブフィードの購読、記録データの任意時点での照会、
公開アーカイブからの共有記録の取得に加え、ご自身の口座で SBI BRiSK も利用
できます。本プロジェクトは独立したもので、BRiSK、立花証券、SBI証券、
東京証券取引所（TSE）、日本取引所グループ（JPX）とは提携しておらず、承認も
受けていません。

> **データ源:**
> - **口座不要:** 2021年9月27日の公開 BRiSK Next デモ（寄り前のスナップショット
>   1件と寄付から3分間）を、記録時のペースで再生したもの。リアルタイムの市場
>   データではありません。
> - **SBI証券の BRiSK 契約がある場合:** SBI BRiSK の市場データ（ローソク足、
>   信用残、アラート、取引スケジュール、ウォッチリスト）と、試験的なライブ
>   フィード。

## インストール

Python 3.12 以上が必要です。ライブフィードと記録には Node 22 以上も必要です
（BRiSK 自身のデコーダーが Node 上で動く WebAssembly モジュールのため）。

```sh
pip install 'briskapi[pandas]'      # `import briskapi` と `brisk` コマンド
```

BRiSK のデコーダーとデモデータは実行時にダウンロードされ、パッケージには
含まれていません。Linux、macOS、Windows 向けのビルド済み Rust ツール
（`brisk_quote_ingest`、`brisk_recording`）は任意で、各
[GitHub リリース](https://github.com/honvl/BRiSKapi/releases)に添付されています。
ソースから使う場合は、リポジトリをクローンして `pip install -e '.[pandas]'` を
実行してください。

## ライブフィード

```python
import briskapi

feed = briskapi.connect(web=True, codes=["7203", "6758"])   # 初期状態を受信してから戻る
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

主なオプション: `web=True`（デモサイトから取得）または `cache=DIR`（デモ
データのローカルコピー）、`codes`（銘柄コード）、`speed`（`1` で実時間、`0` で
最速）、`history=True`（`Ticker.history()` のために更新を保持）。コールバックと
イテレーターは、まず各銘柄の現在の気配を受け取り、その後すべての更新を順番
どおりに受け取ります。受け取る側の処理が遅い場合、更新を捨てずにフィードの
ほうが待ちます。

## 記録データとアーカイブ

```python
recordings = briskapi.recordings(source="historical_mock")   # AWS アカウント不要
if recordings:
    briskapi.pull(recordings[0]["prefix"])      # 検証・展開・キャッシュし、既定のデータにする
else:
    briskapi.record("recordings/my-session", web=True)   # アーカイブが空ならデモを自分で記録
# または briskapi.load("recordings/my-session") でローカルの events.jsonl[.gz] やフォルダーを読む。

briskapi.Ticker("7203").quote(at="08:59:59.9999")             # 寄り前の気配（日本時間）
briskapi.Ticker("7203").history(start="09:00", end="09:01")   # 期間内のすべての更新
briskapi.Market().snapshot(at="09:00:00").to_pandas()
```

時刻の照会には、バッチではなく各気配のタイムスタンプを使います。デモの
7203 の最初の気配は日本時間 08:59:59.993551 で、それより前は `NotFoundError`
になります。`Market().summary()` はローカルの記録やライブフィードでも `start`
と `end` を返します。アーカイブの一覧では、不正または未対応の記録を警告付きで
スキップします。デモの記録がない場合もあり、`synthetic_test` は公開処理のテスト用です。
既存の [サンプルデータ](https://brisk-recordings-honvl-tokyo.s3.ap-northeast-1.amazonaws.com/archive/20260311/cca870a51e7f96f16009c3709c3f597d314b22ed8922bb3ea7fa1acc67a0f85f/events.jsonl.gz)
はプロトコル調査用の SBI 通信形式で、通信形式に対応したデコーダーが必要です。

## SBI BRiSK

BRiSK を契約している SBI証券のお客様向けです。ブラウザで
[sbi.brisk.jp](https://sbi.brisk.jp) にログインし、そのセッション Cookie を
渡します。Cookie は DevTools からコピーするか、`pycookiecheat` の
`chrome_cookies("https://sbi.brisk.jp")` で取得できます。

```python
from briskapi import sbi

sbi.login(cookies={"session_bfaf77a2": "v2.local..."})   # remember=True で保存（本人のみ読み取り可）
toyota = briskapi.Ticker("7203")
toyota.candles("5m").to_pandas()   # ローソク足: 5分（当日）、日、週、月（5m, 1d, 1w, 1mo）
toyota.margin(days=30)             # 信用残と貸株料
market = briskapi.Market()
market.turnover()     # 全銘柄の売買代金と発行済株式数
market.lists()        # 日経225、直近 IPO など
market.events()       # バスケット注文、ストップ高・安、出来高急増などのイベント
market.schedule()     # 取引日、状態、取引時間
market.watchlist()    # 保存済みの銘柄コード

feed = sbi.connect(codes=["7203"])          # ライブ（試験的）。タイミング共有は保存済みの同意に従う
toyota.quote()                              # 他のフィードと同じ呼び出し
# このセッションの市場データも共有する場合は、まず現行ポリシーに同意する:
# briskapi.consent(accept=True, contributor="your-alias", license="CC0-1.0")
# feed = sbi.connect(codes=["7203"], share_market_data=True)
```

結果は下記の規約に従います。エラーは `sbi.SessionExpiredError`（再ログインが
必要）、`briskapi.NotFoundError`、`sbi.RateLimitError`、`sbi.APIError` です。
リクエストは1秒に1回までに制限しています。

ライブフィードは、ご自身のセッションでダウンロードした SBI 自身のデコーダーを
Node 上で動かします。ブラウザは使いません。ベンダーのクライアントと同じ接続手順
（最初のフレームからデコーダーに入力し、スナップショットをストリームに追いつかせ、
デコーダー自身の ping を送り、サーバーのハートビートを監視し、サーバーが Socket.IO
を使う場合はその名前空間に参加）に従います。まだ実際の SBI セッションでは検証
できておらず、公開されていない通信の詳細が3つあります（Socket.IO の接続パラメー
ター、`startLive` のペイロード、追いつき要求の本文）。`sbi.connect(profile={...})`
で設定し、`trace_protocol=True`（トークンはすべて伏せ字）でサーバーの応答を確認
してください。正しくなるまでは、推測で動かさず、明示的なエラーで止まります。
結果をぜひお知らせください。Cookie は sbi.brisk.jp にのみ送られます。SBI の市場
データは、その記録の開始時に明示的に同意した場合だけ共有します。Python API では
`share_market_data=True` が必要です。共有がオンなら、市場データの共有を断っても
タイミングの要約は共有します。

## API リファレンス

| 呼び出し | 戻り値 |
| --- | --- |
| `briskapi.connect(...)` | ライブの `Feed`（既定のデータ源になる） |
| `Ticker(code).info()` | 銘柄名、売買単位、呼値の種別、基準値、値幅制限 |
| `Ticker(code).quote(at=None)` | 買い・売り気配、予想約定価格・数量、成行・引け条件付きの数量、直近の約定 |
| `Ticker(code).auction(at=None)` | 板寄せの予想状態と `market_order_imbalance`（成行の買い数量 − 売り数量） |
| `Ticker(code).history(start, end)` | すべての更新（時系列順） |
| `Market().stocks()` | 全銘柄のマスター |
| `Market().snapshot(at=None)` | 全銘柄の気配 |
| `Market().imbalances(at=None, top=None)` | 成行注文の売買差（絶対値）が大きい順の銘柄 |
| `Market().summary()` | データ源、日付、銘柄数、時刻の範囲 |
| `Feed.quotes(codes)` / `Feed.on_quote(fn, codes)` | 届いた順のライブ更新 |
| `briskapi.recordings()` / `.pull()` / `.load()` | アーカイブの一覧、検証付きダウンロード、ローカルファイル |
| `briskapi.record(output, web=True, ...)` | デモの記録（共有設定に従って共有） |
| `briskapi.consent(...)` | 共有の設定 |
| `Ticker(code).candles(interval)` / `.margin(days)` | SBI BRiSK のローソク足、信用残と貸株料 |
| `Market().turnover()` / `.lists()` / `.events()` / `.schedule()` / `.watchlist()` | SBI BRiSK の市場データ |
| `briskapi.sbi.login()` / `.connect()` | SBI BRiSK のセッションとライブフィード |

価格は円単位の浮動小数点数で、ベンダーの「値なし」（0）は `None` になります。
時刻は取引日の日本時間の `datetime` です。数量は株数で、売買区分・フラグ・
ステータスはベンダーの値のままです。`raw=True` を指定すると、ベンダー形式
（`*_price10` は0.1円単位、`*_us` は日本時間0時からのマイクロ秒）で返します。
表形式の結果は dict のリストで、`.to_pandas()` で DataFrame に変換できます。
エラーは `briskapi.BriskError` と `briskapi.NotFoundError` です。市場全体の照会
では記録を1回読み込みます（デモ全体の 420 MB で約6秒）。

## コマンドライン

```sh
brisk live --web --codes 7203,6758          # 気配の更新ごとに JSON を1行出力（--raw でベンダー形式）
brisk live --sbi --codes 7203               # SBI BRiSK。Cookie は BRISK_SBI_COOKIES（JSON）から。--trace-protocol で接続手順を表示（通信の詳細は BRISK_SBI_PROFILE）
brisk record --web --output recordings/s1   # デモを記録（同意済みなら共有）
brisk list --date 20210927 --source historical_mock
brisk pull PREFIX --output recordings/downloaded   # brisk list が返した prefix を使う
brisk consent [--accept | --revoke]         # 共有設定の表示・変更
brisk upload recordings/s1                  # 記録の共有を再試行
```

各コマンドの詳細は `--help` で確認できます。`pull` はすべて検証してから
書き込み、既存のフォルダーを上書きすることはありません。

共有がオンの場合、対話型の `brisk live --sbi` は記録開始時に毎回、そのセッションの
市場データを公開するか確認します。Enter で今回だけ同意し、この選択は保存しません。
スクリプトでは `--share-market-data` で同意でき、`--no-share-market-data` なら確認
せずに断れます。公開にはセッションの正常終了が必要です。`brisk list --source sbi_live`
で共有された SBI の記録を一覧できます。市場データの正確性は提供者の申告に基づきます。

## 記録の共有

コマンドラインで初めてデモの記録またはライブセッションを始めると、共有される
内容が表示され、一度だけ確認されます（Enter で同意）。それ以降は、最後まで
正常に終わったデモのセッションが自動的にアップロードされ、公開されます。
Python API から確認を求めることはありません。決めるまでは、セッションは
お使いのコンピューターにだけ保存されます。

- **SBI のタイミング共有:** デコード時間、受信時のデータの
  遅れ、フレーム間隔のパーセンタイル、停滞回数、フレーム数、取引日、最初と
  最後の分、エイリアスとライセンスです。`briskapi.Archive().timing()` で全員の
  レポートを一覧できます。
- **任意の SBI 市場データ共有:** 記録開始時に毎回明示的に同意すると、デコードした
  銘柄マスター、価格、数量、銘柄コード、ローカルの計測値も公開アーカイブに追加
  します。Cookie、トークン、接続の診断情報は含めません。
- **デモのセッションで共有される内容:** 記録した市場データ、ローカルの計測値（お使いの
  コンピューターの時計を含み、記録した日時がわかります）、公開エイリアス
  （既定はランダムな `anon-…`）とライセンス。IP アドレスはアップロード回数の
  制限にのみ使います。
- **公開範囲:** 公開された記録は誰でも閲覧でき、永続的に残り、ご自身では削除
  できません。詳しくは [PRIVACY.md](PRIVACY.md)（英語）をご覧ください。
- **共有をやめる:** `brisk consent --revoke`、環境変数 `BRISK_CONTRIBUTE=0`、
  または1回だけなら `--no-upload`。
- **ライセンス:** 同意すると、選んだデータライセンス（CC0-1.0 または
  CC-BY-4.0）で記録を再配布する権利があると宣言したことになります。本
  プロジェクトのオープンソースライセンスは、ベンダーや取引所のデータに関する
  権利を与えるものではありません。宣言できない場合は共有をオフにしてください。
- 途中までの再生（`--limit-frames`）や途中で閉じたセッションは共有されません。

## その他のドキュメント（英語）

- [ARCHITECTURE.md](ARCHITECTURE.md): 仕組み、データ形式、アーカイブの改ざん対策と制限
- [PRIVACY.md](PRIVACY.md): プライバシーポリシー
- [CONTRIBUTING.md](CONTRIBUTING.md): 開発、テスト、リリース
- [tools/brisk_mock/README.md](tools/brisk_mock/README.md): Rust コレクター、フィールド定義、タイミングとレイテンシー
- [infra/README.md](infra/README.md): 独自アーカイブのデプロイ
- [THIRD_PARTY.md](THIRD_PARTY.md): デコーダー、データ、pybrisk の権利表示

ソフトウェアは MIT ライセンスです。
