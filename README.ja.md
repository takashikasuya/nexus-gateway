# nexus-gateway

**ビル設備(BMS・IoT・フィールドプロトコル)を [Building OS](https://github.com/takashikasuya/gutp-building-os-oss) に接続するエッジ統合ゲートウェイ。**

*[English](README.md) / 日本語*

設備データを収集し、制御を中継し、プロトコル差異を吸収して、すべてを Building OS
の共通データモデルへ正規化します。Building OS が **System of Record(記録の正本)**
であり、本ゲートウェイの責務は **接続と変換** のみです。

> **ステータス: MVP 直前。** MVP のスコープ(対象/対象外)・合格条件・残ギャップは
> **[MVP_READINESS.md](MVP_READINESS.md)** に固定しています。基準プロトコルは OPC-UA、
> BACnet・Envoy mTLS・cosign 本番運用は MVP+1 です。

---

## なぜ作ったか

> 📄 詳細な位置づけ、類似システム比較(EdgeX / Azure IoT Edge / Kura / Hono /
> ThingsBoard / EMQX Neuron / OpenRemote)、Building OS / SBCO との責務分担、
> 技術課題の分析は **[docs/background.ja.md](docs/background.ja.md)**
> ([English](docs/background.md))を参照してください。

ビルには BACnet・OPC-UA・MQTT・Modbus など多数の設備プロトコルがあり、それぞれ独自
のアドレッシングとセマンティクスを持ちます。Building OS は `(gateway_id, point_id)`
を鍵とする単一の正規 telemetry/control 契約を望みます。このプロトコル多様性をエッジ
で吸収する何かが必要です。

### なぜ EdgeX Foundry を採用しないのか

EdgeX Foundry は優れた **汎用 IoT エッジプラットフォーム** です。ビル・工場・エネル
ギー・小売・ヘルスケアを等しく対象とし、Device Service・Core Metadata・Core Command・
Application Service・Message Bus・Security スタックを備えます。最小構成でも容易に
**10〜20 コンテナ** になります。

本プロジェクトにとって、その汎用性は利点ではなくコストです。EdgeX の **Core Metadata**
(Device/Profile レジストリ、Provision Watcher)と **Core Command**(REST → Device
Service)は、**Building OS が既に所有する責務**を二重化するからです。すなわち、Digital
Twin(REC/Brick/Ditto)が機器・ポイントのレジストリであり、制御経路は Building OS →
gRPC → ゲートウェイです。EdgeX をそのまま採用すると、Building OS 側に既に存在する
レジストリと Command Service をもう一組運用することになり、これが「重い」と評価する
最大の理由です。

そのため nexus-gateway は、フル IoT プラットフォームよりも **Azure IoT Edge + プロト
コルアダプタ + gRPC アップリンク** に近い設計を意図しています。EdgeX の良い思想 ——
*Device Service 構造*・*コネクタ分離*・*Common Event → パイプライン* の流れ —— は
プラットフォームの重さを伴わずに**借用**しています。コネクタ契約は本質的に次の形です。

```
discover() → Stream[Device]
subscribe() → Stream[Telemetry]
write(cmd)  → Result
```

下回りには実績ある各プロトコル別 OSS を用います: **Eclipse Milo**(OPC-UA)、
**BACpypes3**(BACnet)、**Eclipse Paho**(MQTT)。

---

## アーキテクチャ

```
   フィールド設備 / シミュレータ
        │  BACnet/IP · OPC-UA · MQTT
        ▼
  ┌─────────────┐   evt.<proto>.<id>   ┌────────────┐  TelemetryFrame  ┌──────────────────┐
  │ Connectors  │ ───────────────────▶ │ Normalizer │ ───────────────▶ │ Store-and-Forward │
  │ (1/protocol)│   NATS JetStream     │ local_id→  │   (point_id)     │ (SQLite ring buf) │
  └─────────────┘   stream EVENTS      │  point_id  │                  └────────┬─────────┘
        ▲                              └────────────┘                            │ gRPC stream
        │ cmd.<proto>.<id>  (core NATS request-reply)                            ▼
  ┌─────────────┐        ┌──────────┐  ControlCommand  ┌────────────┐  GatewayIngress/StreamTelemetry
  │ Egress      │ ◀───── │ Dispatch │ ◀────────────────│ Building OS │ ◀─────────────────────────────
  │ agent       │  gRPC GatewayEgress/Connect          └────────────┘  (Envoy エッジで mTLS 終端)
  └─────────────┘
```

- **Connectors**(プロトコルごとに 1 つの独立コンテナ)が設備と通信し、*ネイティブ
  アドレッシングのみ* を載せた **Common Event** を発行します。正規 ID は持ちません
  ([ADR-0001](docs/adr/0001-telemetry-pipeline-shape.md))。
- **Normalizer** は `evt.>` 上の唯一の durable consumer。**Point List** を結合して
  `local_id → point_id` を解決し、**TelemetryFrame**(`gateway_id` + `point_id` +
  値 + タイムスタンプ)を発行します。
- **Store-and-Forward** は有界 SQLite リングバッファ。best-effort・drop-oldest・
  at-least-once で Building OS へ送信します
  ([ADR-0002](docs/adr/0002-best-effort-store-and-forward.md))。
- **Ingress Uplink** がフレームを Building OS の `GatewayIngress` サービスへストリーム
  し、**Egress agent** が `GatewayEgress` ストリームを保持して、受信した **Control
  Command** を、期限付き・冪等(`control_id`)な core-NATS request-reply でコネクタへ
  ディスパッチします([ADR-0004](docs/adr/0004-control-path-reliable-within-window.md))。

### 主要な設計判断(ADR)

| ADR | 決定 |
|-----|------|
| [0001](docs/adr/0001-telemetry-pipeline-shape.md) | コネクタはネイティブアドレッシングを発行し、`local_id → point_id` は Normalizer が所有。ワイヤ上の ID は `(gateway_id, point_id)` のみ。 |
| [0002](docs/adr/0002-best-effort-store-and-forward.md) | Store-and-Forward は best-effort(有界リングバッファ・drop-oldest・at-least-once)。 |
| [0003](docs/adr/0003-point-list-source-of-truth.md) | Point List の正本は Building OS twin。ゲートウェイは差分で同期。provisioning 同期 > file/CSV bootstrap。 |
| [0004](docs/adr/0004-control-path-reliable-within-window.md) | 制御は real-time-or-fail。期限付き core-NATS request-reply、`control_id` で冪等。 |
| [0005](docs/adr/0005-jetstream-topology-bounded-replay.md) | JetStream を Normalizer の前段に置き、durable な replay/back-pressure 境界とする。 |
| [0006](docs/adr/0006-connector-distribution-signed-oci.md) | コネクタは署名済み OCI イメージ、digest 固定で実行、Connector Catalog 経由で cosign 検証 + rollback。 |
| [0007](docs/adr/0007-transport-security-mtls-at-edge.md) | ゲートウェイ↔Building OS の gRPC は Building OS の Envoy エッジで mTLS 終端(`gateway_id` ↔ クライアント証明書の CN/SAN)。クラスタ内は h2c。 |

---

## 特徴

- **プロトコルコネクタ** — BACnet(Python/BACpypes3)、OPC-UA(Java/Eclipse Milo)、
  MQTT(Go/Paho)、加えて smoke 用のゼロ依存 `sim` コネクタ。各々が Building OS の
  ドメインモデルを持たない独立コンテナ。
- **Telemetry + 制御** を 1 ゲートウェイで提供。アップリンクストリーミングと書込経路
  (BACnet WriteProperty、OPC-UA Write/Method、MQTT publish)。
- **Point List 同期** — Building OS(または file/CSV スタンドイン)から差分収束で同期。
  ほぼ不変なので初回同期後はゆっくりポーリング。
- **耐障害性** — 有界 Store-and-Forward が Building OS 障害をやり過ごす。Normalizer は
  poison / point-list-miss を drop-and-meter(`normalizer_invalid_total`、
  `normalizer_unresolved_total`)。
- **セキュリティ** — Building OS への設定駆動 **TLS/mTLS**。Admin API & UI は
  **Keycloak/OIDC**(operator/viewer ロール)で保護。
- **Admin UI**(Next.js)— ダッシュボード + コネクタライフサイクル(start/stop/restart/
  upgrade)、OIDC 背後。
- **ライフサイクル管理** — Docker Engine API 経由。**署名済み OCI** によるコネクタ配布を
  Connector Catalog 経由で実施(digest 固定・cosign 検証・stop→replace→health→rollback)。

---

## クイックスタート

> 🚀 はじめての方へ：**[はじめにガイド](docs/getting-started.ja.md)**
> ([English](docs/getting-started.md))が、`compose up` からテレメトリの観察、
> Admin API でのコネクタ操作までを、機器なし・約 10 分で案内します。

```bash
# フルスタック: NATS + mock Building OS + gateway + Keycloak + Admin UI
docker compose up --build
```

- Admin UI: http://localhost:13000(Keycloak realm `nexus-gateway`、ユーザ
  `operator`/`operator`、`viewer`/`viewer`)
- Gateway Admin API: http://localhost:18080(`/health`、`/metrics`、`/connectors`)
- Keycloak: http://localhost:18090(管理者 `admin`/`admin`)

ゲートウェイバイナリを直接実行:

```bash
go run ./cmd/gateway --dev-sim   # 設備不要の smoke 実行用に in-process sim コネクタを起動
```

### 設定(フラグ / 環境変数)

| フラグ | 環境変数 | 既定値 | 用途 |
|--------|----------|--------|------|
| `--nats` | `NATS_URL` | `nats://localhost:4222` | NATS URL |
| `--bos` | `BOS_ADDR` | `localhost:50051` | Building OS の gRPC アドレス |
| `--gateway-id` | `GATEWAY_ID` | `gw-001` | ゲートウェイ ID(mTLS 証明書の CN/SAN にも対応) |
| `--bos-insecure` | `BOS_INSECURE` | `false` | Building OS へ平文 h2c — dev/CI のみ(ADR-0007) |
| `--bos-ca` / `--bos-cert` / `--bos-key` | `BOS_CA_FILE` / … | – | TLS/mTLS 資材 |
| `--provisioning-url` | `PROVISIONING_URL` | – | Building OS の Point List provisioning API |
| `--provisioning-file` | `PROVISIONING_FILE` | – | file/CSV ベースの Point List(dev/E2E) |
| `--point-sync-interval` | – | `10m` | 初回同期後の Point List ポーリング間隔 |
| `--admin-jwks-url` | `KEYCLOAK_JWKS_URL` | – | Keycloak JWKS(空 = Admin API 認証無効) |
| `--dev-sim` | `DEV_SIM` | `false` | in-process sim コネクタを起動(非本番) |

### 本番: Building OS への TLS/mTLS(ADR-0007)

`BOS_INSECURE=true`(平文 h2c)は **dev/CI 専用**です(エッジプロキシの無いローカル用)。
本番では `--bos-insecure` を外し、CA + クライアント証明書/鍵を渡します。gRPC リンクは
Building OS の Envoy エッジで **mTLS 終端**され、`gateway_id` がクライアント証明書の
CN/SAN に束縛されます。

```bash
GATEWAY_ID=GW-SOS-001 \
BOS_ADDR=bos.example.com:443 \
BOS_CA_FILE=/etc/nexus/tls/ca.pem \
BOS_CERT_FILE=/etc/nexus/tls/gateway.crt \   # CN/SAN に GATEWAY_ID を埋め込む
BOS_KEY_FILE=/etc/nexus/tls/gateway.key \
BOS_SERVER_NAME=bos.example.com \            # 任意: SNI/検証名の上書き
PROVISIONING_URL=https://bos.example.com/provisioning \
go run ./cmd/gateway
```

- `--bos-cert`/`--bos-key` を省くと **サーバ認証のみ**(CA 検証)、付けると **mTLS**。
- CN/SAN ↔ `gateway_id` の束縛が Building OS の所有権チェックの前提です。
  [SECURITY.md](SECURITY.md) と
  [ADR-0007](docs/adr/0007-transport-security-mtls-at-edge.md) を参照。

### シミュレータ統合(設備なし)

隣接リポ `../bacnet-sim-gateway` と `../opcua-sim-gateway` が標準準拠の BACnet/IP・
OPC-UA シミュレータを提供します。詳細は
[`fixtures/integration/`](fixtures/integration/README.md):

```bash
docker compose -f docker-compose.yml -f docker-compose.integration.yml --profile opcua up
```

---

## 拡張: プロトコルコネクタの追加

コネクタは次を行う独立コンテナです。

1. Point List から、ポーリング/購読すべき **ネイティブアドレス** のみを読む。
2. **Common Event** を JetStream subject `evt.<protocol>.<connector_id>` に発行する。
   `protocol` + ネイティブ `local_id` + 値/単位/品質/タイムスタンプを載せ、**`point_id`
   は載せない**(`point_id` は Normalizer が割り当てる)。
3. `cmd.<protocol>.<connector_id>` を購読して **Control Command** を受け、型付き結果を
   `control_id` で冪等に返す。

各言語のリファレンスコネクタ(`connector/{bacnet,opcua,mqtt}`)を雛形として利用して
ください。署名済み OCI イメージとしてパッケージし、Connector Catalog に登録すると、
Core Agent が digest 固定で実行します(ADR-0006)。

---

## 開発

```bash
go build ./...
go test -race ./...           # Go パイプライン + コネクタ
cd admin-ui && npm run type-check && npm run build
```

CI(`.github/workflows/ci.yml`)は PR ごとに Go の build/test と Admin UI の
type-check/build を実行します。

### モジュールのシーム(テスト容易性)

ADR が定める振る舞いは **深いモジュール**(小さなインターフェース＝ユニットテスト面)に
集約され、各々が NATS/gRPC/Docker の実スタックなしで in-process に検証できます
([EP-011](docs/backlog/epic/EP-011-architecture-deepening.md)):

| モジュール | シーム | 責務 |
|------------|--------|------|
| `uplink.Forwarder` | `FrameSink`(`Send` + `Checkpoint`) | ADR-0002 のチェックポイント/前進/ドリフト/再送ポリシー。gRPC クライアントストリーミングは `grpcSink` アダプタ。 |
| `lifecycle.HealthMonitor` | `GatewayMetrics` + `ConnectorProber` | ホスト統計(uptime/mem/disk)とコンテナ生存性を分離し、各々を独立にテスト。 |
| `pointlist.Resolver` / `ReverseResolver` | 順引き + 逆引き | 単一の Point List。Normalizer(順)と制御 Dispatch(逆)が消費。 |
| `adminapi` | `NewServer` / `NewSecureServer` + `ServerOptions` | 認証なし/JWT の 2 コンストラクタを共有ビルダーで。任意ソースは 1 構造体に集約。 |

E2E テストは `test/e2e/`(`//go:build e2e`)にあり、
実シミュレータ / Building OS スタックがある環境で `-tags e2e` + 環境変数を与えて実行
します。学術評価（E1〜E7）の実験設計は
[`docs/evaluation-plan.ja.md`](docs/evaluation-plan.ja.md) を参照してください。

---

## 貢献 & セキュリティ

- **貢献** — 開発セットアップ・テストゲート・PR 規約は
  [`CONTRIBUTING.md`](CONTRIBUTING.md) に。まずは
  [はじめにガイド](docs/getting-started.ja.md)から。
- **セキュリティ** — 脆弱性は [`SECURITY.md`](SECURITY.md)(GitHub 限定アドバイザリ)
  で非公開に報告してください。公開 Issue は作成しないでください。

---

## ライセンス

Apache-2.0(SBCO / Building OS 隣接プロジェクトと統一)。[`LICENSE`](LICENSE) を参照。
