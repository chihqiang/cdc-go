# cdc-go

![Go 1.25+](https://img.shields.io/badge/Go-1.25%2B-00ADD8?logo=go&logoColor=white)
![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)

A lightweight Change Data Capture (CDC) service for MySQL, written in Go. It
tails the MySQL binary log, converts every row change into one stable JSON
format, and publishes it to the middleware you choose. The binlog position is
stored in a file or Redis, so a restart resumes where it stopped instead of
replaying the log.

The repository ships two binaries:

| Binary | Purpose |
| --- | --- |
| `cdc-go` | Reads MySQL binlogs and publishes change events. |
| [`cdc-go-task`](./cmd/cdc-go-task/README.md) | Consumes those events and runs the tasks that watch each table. |

## How it works

```text
MySQL binlog ──> cdc-go ──> middleware ──> cdc-go-task ──> tasks
                            (Kafka, Redis,
                             RabbitMQ, ...)
```

1. `cdc-go` connects to MySQL as a replication client and reads the binlog.
2. Every row change becomes a `types.EventData` event (see
   [Event format](#event-format)).
3. The event is sent to the configured producer: stdout, Redis, Kafka,
   RabbitMQ, RocketMQ or Pulsar.
4. `cdc-go-task` reads the same middleware and routes each event to the tasks
   watching its `database.table`.

## Features

- **Real-time capture** — row-level changes are read from the MySQL binlog.
- **Unified event format** — every source produces the same JSON structure.
- **Multiple producers** — stdout, Redis, Kafka, RabbitMQ, RocketMQ and Pulsar.
- **Checkpoint resume** — the binlog position is persisted in a file or Redis.
- **Table filtering** — include and exclude tables with regular expressions.
- **Reliable publishing** — failed sends are retried with exponential backoff.
- **Backpressure by design** — a slow producer slows the binlog reader instead
  of dropping events.
- **Graceful shutdown** — `SIGINT`/`SIGTERM` stop the source, flush the current
  position and close resources cleanly.
- **Task runner** — `cdc-go-task` fans events out to application tasks, and can
  replay a row, a time range or the whole configuration.
- **One configuration file** — each binary reads a single YAML file, with
  `${VAR}` / `${VAR:-fallback}` substitution for per-environment values.

## Supported components

| Role | Options |
| --- | --- |
| Data source | MySQL |
| Offset store | File, Redis |
| Producer (`cdc-go`) | `stdout`, Redis, Kafka, RabbitMQ, RocketMQ, Pulsar |
| Consumer (`cdc-go-task`) | Redis, Kafka, RabbitMQ, RocketMQ, Pulsar |

## Requirements

- **Go 1.25+** to build from source.
- A **MySQL** server with binary logging enabled and a user allowed to read the
  binlog (see [MySQL setup](#mysql-setup)).
- Optional: the middleware you publish to, plus Redis if the offset store is
  configured as `redis`.

## Quick start

### Build from source

```bash
git clone https://github.com/chihqiang/cdc-go.git
cd cdc-go

make build          # builds ./cdc-go
make build-task     # builds ./cdc-go-task
make build-all      # builds both
```

To install the publisher through the Go toolchain instead:

```bash
go install github.com/chihqiang/cdc-go/cmd/cdc-go@latest
```

### Configure

Copy the shipped example and adjust the values for your environment:

```bash
cp config.yml config.local.yml
```

The file is the only source of configuration; starting without a readable file
fails. Use `-c` / `--config` to select a file (default: `config.yml`):

```bash
./cdc-go -c config.local.yml
# `listen` is the same action, and can be given explicitly:
./cdc-go listen -c config.local.yml
```

With the default `stdout` producer, captured changes are printed as indented
JSON. Point `producer.type` at a middleware to publish them elsewhere.

## Configuration

The publisher reads one YAML file made of three sections:

| Section | Purpose |
| --- | --- |
| `store` | Where the binlog offset is persisted: `file` or `redis`. |
| `source` | Where changes are read from: currently `mysql`. |
| `producer` | Where events are sent: `stdout`, `redis`, `kafka`, `rabbitmq`, `rocketmq` or `pulsar`. |

The complete example is [`config.yml`](./config.yml). Only the block selected
by each `type` field is used.

### Environment substitution

Values in the file can reference environment variables:

```yaml
source:
  mysql:
    addr: "${SOURCE_MYSQL_ADDR:-127.0.0.1:3306}"  # overridable, with a fallback
    user: "${SOURCE_MYSQL_USER:-root}"
    password: "${SOURCE_MYSQL_PASSWORD}"          # required from the environment
```

```bash
SOURCE_MYSQL_ADDR=mysql:3306 SOURCE_MYSQL_PASSWORD=secret ./cdc-go -c config.yml
```

| Syntax | Meaning |
| --- | --- |
| `${VAR}` | The value of `VAR`, or an empty string when it is unset. |
| `${VAR:-fallback}` | The value of `VAR`, or `fallback` when `VAR` is unset or empty. |
| `$$` | An escaped `$`. |

A missing key falls back to the default declared in the code. A required key
missing from both the file and the defaults fails at startup with the field path
in the error.

> Expansion only fills a string or a key. A list value such as
> `exclude_table_regex` must be written as a list in the file; it cannot be
> supplied through a comma-separated environment variable.

### `store`

| Key | Default | Description |
| --- | --- | --- |
| `type` | required | `file` or `redis`. |
| `file.dir` | `runtime` | Directory the offset file is written to. |
| `redis.addr` | `127.0.0.1:6379` | Redis address. |
| `redis.password` | empty | Redis password. |
| `redis.db` | `0` | Redis database number. |

The file store writes atomically (temporary file + rename), so a crash cannot
leave a half-written position. Use Redis when several processes or pods must
share a position, or when the local filesystem is ephemeral.

### `source`

| Key | Default | Description |
| --- | --- | --- |
| `type` | required | `mysql`. |
| `mysql.addr` | `127.0.0.1:3306` | MySQL address (`host:port`). |
| `mysql.user` | `root` | MySQL user. |
| `mysql.password` | — | MySQL password. Prefer a secret from the environment. |
| `mysql.server_id` | `0` | Replication server id. `0` picks a random id in 1001–2000. |
| `mysql.exclude_table_regex` | system schemas (see example) | Tables to skip. |
| `mysql.include_table_regex` | empty (all) | When non-empty, only matching tables are captured. |

Filtering order:

1. Tables matching `exclude_table_regex` are skipped.
2. If `include_table_regex` is not empty, only matching tables are kept.
3. With neither configured, every table is captured.

### `producer`

| `type` | Keys |
| --- | --- |
| `stdout` | None. Prints indented JSON. |
| `redis` | `addr`, `password`, `db`, `key` (list key, default `cdc-go-events`). |
| `kafka` | `brokers` (default `127.0.0.1:9092`), `topic` (default `cdc-go-events`). |
| `rabbitmq` | `url`, `exchange`, `exchange_type` (`direct`/`fanout`/`topic`), `routing_key`, `queue`, `durable`, `auto_delete`, `auto_ack`, `exclusive`, `no_wait`. |
| `rocketmq` | `servers`, `topic`, `group`, `namespace`, `access_key`, `secret_key`, `retry`. |
| `pulsar` | `url`, `topic`, `token`, `operation_timeout`, `connection_timeout`. |

The RabbitMQ producer declares the exchange and queue, and binds the queue with
`routing_key` on startup. The Redis producer pushes each event with `LPUSH` onto
the configured list, so the consumer reads with `BRPOP`.

### Environment variables read by the example file

```text
Store:    STORE_TYPE, STORE_FILE_DIR, STORE_REDIS_ADDR, STORE_REDIS_PASSWORD,
          STORE_REDIS_DB
Source:   SOURCE_TYPE, SOURCE_MYSQL_ADDR, SOURCE_MYSQL_USER,
          SOURCE_MYSQL_PASSWORD, SOURCE_MYSQL_SERVER_ID
Producer: PRODUCER_TYPE,
          PRODUCER_KAFKA_BROKERS, PRODUCER_KAFKA_TOPIC,
          PRODUCER_RABBITMQ_URL, PRODUCER_RABBITMQ_EXCHANGE,
          PRODUCER_RABBITMQ_EXCHANGE_TYPE, PRODUCER_RABBITMQ_ROUTING_KEY,
          PRODUCER_RABBITMQ_QUEUE,
          PRODUCER_REDIS_ADDR, PRODUCER_REDIS_PASSWORD, PRODUCER_REDIS_DB,
          PRODUCER_REDIS_KEY,
          PRODUCER_ROCKETMQ_SERVERS, PRODUCER_ROCKETMQ_TOPIC,
          PRODUCER_ROCKETMQ_GROUP, PRODUCER_ROCKETMQ_NAMESPACE,
          PRODUCER_ROCKETMQ_ACCESS_KEY, PRODUCER_ROCKETMQ_SECRET_KEY,
          PRODUCER_PULSAR_URL, PRODUCER_PULSAR_TOPIC, PRODUCER_PULSAR_TOKEN
```

## Event format

Each change is published as one JSON document:

```json
{
  "time": "2026-10-21T10:30:00.123456789+08:00",
  "server_id": 1,
  "pos": 194512345,
  "row": {
    "time": 1792549800,
    "database": "app",
    "table": "user",
    "type": "update",
    "data": {
      "id": 42,
      "name": "Alice",
      "active": 1,
      "balance": "12.34",
      "created_at": "2026-10-21 10:29:58"
    },
    "old": {
      "id": 42,
      "name": "alice",
      "active": 1,
      "balance": "12.34",
      "created_at": "2026-10-21 10:29:58"
    }
  }
}
```

| Field | Description |
| --- | --- |
| `time` | When `cdc-go` captured the event. |
| `server_id` | MySQL server id from the binlog header. |
| `pos` | Binlog position of the event. |
| `row.time` | Binlog event timestamp in Unix seconds. |
| `row.database` / `row.table` | Source table. |
| `row.type` | `insert`, `update` or `delete`. |
| `row.data` | Row values after the change; for a delete, the deleted row. |
| `row.old` | Row values before an update. Omitted for inserts and deletes. |

Values follow the type the MySQL driver decodes: integer and float columns
become JSON numbers, while `DECIMAL`, date/time and JSON columns are emitted as
strings (`DATETIME` keeps MySQL's `YYYY-MM-DD HH:MM:SS` form). `BIT` is emitted
as `0` or `1`, and text/binary columns become strings. Every binlog position is
persisted after it has been handled, which is what makes resumption possible.

## Docker

The image is published to Docker Hub and Aliyun Container Registry:

- `zhiqiangwang/app:cdc-go`
- `registry.cn-hangzhou.aliyuncs.com/buildx/app:cdc-go`

It ships `/app/config.yml` and starts with `cdc-go -c /app/config.yml`. Every
`-e` below fills a `${VAR:-fallback}` reference in that file, so the same image
can target another database or middleware without a rebuild.

```bash
# MySQL -> stdout
docker run -it --rm \
  --name cdc-go \
  -e SOURCE_MYSQL_ADDR="127.0.0.1:3306" \
  -e SOURCE_MYSQL_USER="root" \
  -e SOURCE_MYSQL_PASSWORD="123456" \
  zhiqiangwang/app:cdc-go

# MySQL -> Redis, with the offset stored in Redis as well
docker run -it --rm \
  --name cdc-go \
  -e SOURCE_MYSQL_ADDR="127.0.0.1:3306" \
  -e SOURCE_MYSQL_USER="root" \
  -e SOURCE_MYSQL_PASSWORD="123456" \
  -e STORE_TYPE="redis" \
  -e STORE_REDIS_ADDR="127.0.0.1:6379" \
  -e PRODUCER_TYPE="redis" \
  -e PRODUCER_REDIS_ADDR="127.0.0.1:6379" \
  -e PRODUCER_REDIS_KEY="cdc-go-events" \
  zhiqiangwang/app:cdc-go

# MySQL -> Kafka, with the offset stored in Redis
docker run -it --rm \
  --name cdc-go \
  -e SOURCE_MYSQL_ADDR="127.0.0.1:3306" \
  -e SOURCE_MYSQL_USER="root" \
  -e SOURCE_MYSQL_PASSWORD="123456" \
  -e STORE_TYPE="redis" \
  -e STORE_REDIS_ADDR="127.0.0.1:6379" \
  -e PRODUCER_TYPE="kafka" \
  -e PRODUCER_KAFKA_BROKERS="127.0.0.1:9092" \
  -e PRODUCER_KAFKA_TOPIC="cdc-go-events" \
  zhiqiangwang/app:cdc-go

# MySQL -> RabbitMQ, with the offset stored in Redis
docker run -it --rm \
  --name cdc-go \
  -e SOURCE_MYSQL_ADDR="127.0.0.1:3306" \
  -e SOURCE_MYSQL_USER="root" \
  -e SOURCE_MYSQL_PASSWORD="123456" \
  -e STORE_TYPE="redis" \
  -e STORE_REDIS_ADDR="127.0.0.1:6379" \
  -e PRODUCER_TYPE="rabbitmq" \
  -e PRODUCER_RABBITMQ_URL="amqp://guest:guest@127.0.0.1:5672/" \
  -e PRODUCER_RABBITMQ_EXCHANGE="cdc-go-exchange" \
  -e PRODUCER_RABBITMQ_ROUTING_KEY="cdc-go-events" \
  -e PRODUCER_RABBITMQ_QUEUE="cdc-go-events" \
  zhiqiangwang/app:cdc-go

# MySQL -> RocketMQ, with the offset stored in Redis
docker run -it --rm \
  --name cdc-go \
  -e SOURCE_MYSQL_ADDR="127.0.0.1:3306" \
  -e SOURCE_MYSQL_USER="root" \
  -e SOURCE_MYSQL_PASSWORD="123456" \
  -e STORE_TYPE="redis" \
  -e STORE_REDIS_ADDR="127.0.0.1:6379" \
  -e PRODUCER_TYPE="rocketmq" \
  -e PRODUCER_ROCKETMQ_SERVERS="127.0.0.1:9876" \
  -e PRODUCER_ROCKETMQ_TOPIC="cdc-go-events" \
  -e PRODUCER_ROCKETMQ_GROUP="cdc-go-group" \
  zhiqiangwang/app:cdc-go

# MySQL -> Pulsar, with the offset stored in Redis
docker run -it --rm \
  --name cdc-go \
  -e SOURCE_MYSQL_ADDR="127.0.0.1:3306" \
  -e SOURCE_MYSQL_USER="root" \
  -e SOURCE_MYSQL_PASSWORD="123456" \
  -e STORE_TYPE="redis" \
  -e STORE_REDIS_ADDR="127.0.0.1:6379" \
  -e PRODUCER_TYPE="pulsar" \
  -e PRODUCER_PULSAR_URL="pulsar://127.0.0.1:6650" \
  -e PRODUCER_PULSAR_TOPIC="cdc-go-events" \
  zhiqiangwang/app:cdc-go
```

To use your own file instead of the bundled one:

```bash
docker run -it --rm \
  --name cdc-go \
  -v "$PWD/config.yml:/app/config.yml:ro" \
  zhiqiangwang/app:cdc-go
```

Build the image locally with:

```bash
docker build -t cdc-go .
```

## Kubernetes and local middleware

The repository includes manifests for the application and the supported
middleware:

```bash
# Everything: namespace, middleware StatefulSets and the cdc-go Deployment
kubectl apply -k deploy/k8s/

# Only the application
kubectl apply -f deploy/k8s/cdc-go.yml
```

For local development, [`deploy/compose/services.yml`](./deploy/compose/services.yml)
brings up MySQL, Redis, Kafka, RabbitMQ, RocketMQ and Pulsar:

```bash
docker compose -f deploy/compose/services.yml up -d
```

> The bundled `cdc-go` Deployment uses `replicas: 1` and the `file` offset
> store. If the pod is recreated, mount a volume for `STORE_FILE_DIR` or switch
> `store.type` to `redis` to keep the position.

## Consuming events with `cdc-go-task`

`cdc-go-task` consumes the events published by `cdc-go` and routes each one to
the tasks watching its table. It is configured separately by
[`config.task.yml`](./config.task.yml), which must point at the same middleware
destination as the publisher (for example, the same Kafka brokers and topic).

```bash
make build-task
./cdc-go-task -c config.task.yml
```

Available commands:

| Command | Description |
| --- | --- |
| `run` | Continuously consume events and run the matching tasks. This is the default command. |
| `run-by-pk <db.table> <pk>` | Run the tasks watching a table for one main-table row. |
| `run-by-range <db.table> <start> <end>` | Run the tasks for rows in a half-open time range. |
| `run-all-by-range <start> <end>` | Run every configured task over every watched table for a time range. |

```bash
# One row
./cdc-go-task -c config.task.yml run-by-pk app.user 42

# One day, half-open interval [start, end)
./cdc-go-task -c config.task.yml run-by-range app.user \
  "2026-10-17 00:00:00" "2026-10-18 00:00:00"

# Backfill everything the configuration watches
./cdc-go-task -c config.task.yml run-all-by-range \
  "2026-10-17 00:00:00" "2026-10-18 00:00:00"
```

See [`cmd/cdc-go-task/README.md`](./cmd/cdc-go-task/README.md) for the task
configuration, routing rules and the `task.ITask` interface.

## MySQL setup

### Binary logging

The server must run with row-based binary logging, and the replication client
needs its own server id:

```ini
[mysqld]
log-bin       = mysql-bin
binlog_format = ROW
server-id     = 1
```

### Permissions

Create a dedicated account with replication and read access:

```sql
CREATE USER 'cdcgo'@'%' IDENTIFIED BY 'strong_password';

-- Required to read the binlog
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdcgo'@'%';

-- Required if cdc-go performs metadata queries
GRANT SELECT ON *.* TO 'cdcgo'@'%';

FLUSH PRIVILEGES;
```

### Selecting a replication server id

Two replication clients that connect with the same `server_id` disconnect each
other and reconnect in a loop. Every consumer of the same MySQL instance — a
replica, Maxwell, another `cdc-go` — therefore needs its own id. Inspect the
server before choosing one:

```sql
-- Replicas that register themselves (MySQL 8.0.22+; older: SHOW SLAVE HOSTS)
SHOW REPLICAS;

-- Id of a server you know, run on that server
SELECT @@server_id;

-- Everything connected right now, including clients that do not register:
-- one binlog dump thread per replication client
SHOW PROCESSLIST;   -- Command = 'Binlog Dump' or 'Binlog Dump GTID'
```

MySQL does not expose the id of a connected dump thread, so keep a record of
the id assigned to each consumer and pick one that appears in neither query.

## Development

```bash
make build        # build ./cdc-go
make build-task   # build ./cdc-go-task
make build-all    # build both binaries
make test         # go test -race -cover over ./... (also fails on a dirty working tree)
make check        # golangci-lint, errcheck, go fmt and go mod tidy
```

Run a focused test directly with the Go toolchain:

```bash
go test ./task/... ./store/...
```

### Project layout

| Path | Contents |
| --- | --- |
| [`cmd/cdc-go`](./cmd/cdc-go) | Publisher entry point. |
| [`cmd/cdc-go-task`](./cmd/cdc-go-task) | Consumer/task entry point and its documentation. |
| [`cmd`](./cmd) | Shared CLI wiring for both binaries. |
| [`config`](./config) | Configuration loading and environment expansion. |
| [`source`](./source) | Data sources; MySQL binlog implementation. |
| [`store`](./store) | Offset stores: file and Redis. |
| [`queue/producer`](./queue/producer) | Event producers for each middleware. |
| [`queue/consumer`](./queue/consumer) | Event consumers for `cdc-go-task`. |
| [`task`](./task) | Task runner, routing and the `ITask` interface. |
| [`types`](./types) | Event and primary-key types. |
| [`deploy`](./deploy) | Kubernetes manifests and Docker Compose services. |

## License

Licensed under the [Apache License 2.0](./LICENSE).
