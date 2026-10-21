<div align="center">
<h1>cdc-go</h1>

<p>
 cdc-go is an efficient Go-based Change Data Capture (CDC) tool that real-time monitors database changes, parses and processes events, and sends them to message queues or other downstream systems.
</p>

</div>

## Project Overview

## Features

- **Real-time Capture**: Monitor database change events in real-time through binlog parsing
- **Unified Event Format**: Convert changes from different databases into a consistent JSON format
- **Multiple Producer Support**: Send events to various downstream systems including stdout, Redis, Kafka, RabbitMQ, RocketMQ, and Pulsar
- **Checkpoint Resumption**: Store synchronization positions to achieve breakpoint resumption
- **Extensible Architecture**: Easy to extend with new data sources and producer types
- **Worker Pool Processing**: Process events efficiently with worker goroutines
- **Graceful Shutdown**: Properly handle context cancellation and resource cleanup
- **Single Source Configuration**: One YAML file per deployment, with `${VAR}` / `${VAR:-fallback}` environment substitution inside it

## Supported Components

### Data Sources

- MySQL (via binlog parsing)

### Producers

- Standard Output (stdout)
- [Redis](https://redis.io/)
- [Kafka](https://kafka.apache.org/)
- [RabbitMQ](https://www.rabbitmq.com/)
- [RocketMQ](https://rocketmq.apache.org/)
- [Pulsar](https://pulsar.apache.org/)

### Storage

- File Storage
- Redis Storage

## Quick Start

### Prerequisites

- Go 1.23+ environment
- MySQL server with binary logging enabled
- Correct database access permissions (MySQL user needs binlog read permissions)
- If using other producer components, ensure corresponding services are available

### Installation

### Prerequisites

- Go **1.23+** (latest version recommended)
- `$GOPATH/bin` added to your `$PATH`

### Option 1: Install via `go install` (Recommended)

This is the simplest and recommended way to install **cdc-go**:

```bash
go install github.com/chihqiang/cdc-go/cmd/cdc-go@latest
```

### Option 2: Build from Source

If you want to modify the source code or contribute to development, build from source:

```bash
# Clone the repository
git clone https://github.com/chihqiang/cdc-go.git
cd cdc-go && make build
cp ./cdc-go /usr/local/bin/
```

### Usage Example

1. Create configuration file `config.yml`: (see Configuration File Description section for details)

2. Run the program:

```bash
# Using the default config.yml
cdc-go
# Using a specific config file
cdc-go -c path/to/config.yml
# Explicitly using the listen command
cdc-go listen -c path/to/config.yml
```

> The configuration file is required: the file is the only source of configuration, and starting
> without a readable file fails with an error. Values that differ per environment are not passed as
> environment variables directly, but referenced from inside the file — see
> [Environment Variables](#environment-variables).

## Configuration File Description

The configuration file uses YAML format and consists of three main parts: `store` (offset storage), `source` (data source), and `producer` (producer destination).

### Example Configuration

The complete configuration file is [`config.yml`](./config.yml) in the repository root — it is the
file the container image ships and the only configuration the program reads. Copy it, adjust it and
run `cdc-go -c config.yml`; every value can also be overridden from the environment (see
[Environment Variables](#environment-variables)).

It has three top-level sections, each with its own block in the file:

| Section | What it configures |
| --- | --- |
| `store` | Where the binlog offset is kept: `file` (a directory) or `redis` |
| `source` | `mysql`: `addr`, `user`, `password`, `server_id`, and the `exclude_table_regex` / `include_table_regex` table filters |
| `producer` | Where the events go: `stdout`, `kafka`, `redis`, `rabbitmq`, `rocketmq` or `pulsar` — `type` picks the block that is read |

### Environment Variables

The file is the only source of configuration — there is no environment variable fallback for the
settings themselves. Environment variables are referenced **from inside the file** with `${VAR}`
or `${VAR:-fallback}`, which is how one file serves a laptop, a container and Kubernetes:

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
| `${VAR}` | The value of `VAR`, or an empty string when it is unset |
| `${VAR:-fallback}` | The value of `VAR`, or `fallback` when `VAR` is unset or empty |
| `$$` | An escaped `$` (use it for a literal `${...}`) |

A key that is missing from the file falls back to the default written in the tag (`default=`), and a
key that is required but missing from both the file and the defaults fails at startup with the field
path in the error.

> **Note:** Expansion only ever fills a string or a key, so a value that should be a list — e.g.
> `exclude_table_regex` — has to be written as a list in the file; it cannot be passed as a
> comma-separated environment variable.

## Docker Deployment

You can use Docker to run cdc-go in containerized environments. Here's how to build and run cdc-go with Docker:

The image ships `/app/config.yml` (the file from the repository root) and is started with
`cdc-go -c /app/config.yml`. Every `-e` below fills a `${VAR:-fallback}` reference in that file,
so the same image can be pointed at another database, storage or producer without a rebuild:

> The image is published to both Docker Hub (`zhiqiangwang/app:cdc-go`) and Aliyun Container
> Registry (`registry.cn-hangzhou.aliyuncs.com/buildx/app:cdc-go`). The examples below use the
> Docker Hub tag; substitute the Aliyun tag if that registry is closer to you.

```bash
# =========================
# 1️⃣ MySQL only (read from MySQL)
# =========================
docker run -it --rm \
    --name cdc-go \
    -e SOURCE_MYSQL_ADDR="127.0.0.1:3306" \
    -e SOURCE_MYSQL_USER="root" \
    -e SOURCE_MYSQL_PASSWORD="123456" \
    zhiqiangwang/app:cdc-go

# =========================
# 2️⃣ MySQL → Redis & Redis
# =========================
docker run -it --rm \
    --name cdc-go \
    -e SOURCE_MYSQL_ADDR="127.0.0.1:3306" \
    -e SOURCE_MYSQL_USER="root" \
    -e SOURCE_MYSQL_PASSWORD="123456" \
    -e STORE_TYPE="redis" \
    -e STORE_REDIS_ADDR="127.0.0.1:6379" \
    -e STORE_REDIS_PASSWORD="123456" \
    -e STORE_REDIS_DB="1" \
    -e PRODUCER_TYPE="redis" \
    -e PRODUCER_REDIS_ADDR="127.0.0.1:6379" \
    -e PRODUCER_REDIS_PASSWORD="123456" \
    -e PRODUCER_REDIS_DB="1" \
    -e PRODUCER_REDIS_KEY="cdc-go-events" \
    zhiqiangwang/app:cdc-go
```

To use your own configuration file instead, mount it over the bundled one:

```bash
docker run -it --rm \
    --name cdc-go \
    -v "$PWD/config.yml:/app/config.yml:ro" \
    zhiqiangwang/app:cdc-go
```

The environment variables the bundled `config.yml` reads are `STORE_TYPE`, `STORE_FILE_DIR`,
`STORE_REDIS_ADDR`, `STORE_REDIS_PASSWORD`, `STORE_REDIS_DB`, `SOURCE_TYPE`, `SOURCE_MYSQL_ADDR`,
`SOURCE_MYSQL_USER`, `SOURCE_MYSQL_PASSWORD`, `SOURCE_MYSQL_SERVER_ID`, `PRODUCER_TYPE`, `PRODUCER_KAFKA_BROKERS`,
`PRODUCER_KAFKA_TOPIC`, `PRODUCER_RABBITMQ_URL`, `PRODUCER_RABBITMQ_EXCHANGE`, `PRODUCER_RABBITMQ_EXCHANGE_TYPE`,
`PRODUCER_RABBITMQ_ROUTING_KEY`, `PRODUCER_RABBITMQ_QUEUE`,
`PRODUCER_REDIS_ADDR`, `PRODUCER_REDIS_PASSWORD`, `PRODUCER_REDIS_DB`, `PRODUCER_REDIS_KEY`,
`PRODUCER_ROCKETMQ_SERVERS`, `PRODUCER_ROCKETMQ_TOPIC`, `PRODUCER_ROCKETMQ_GROUP`,
`PRODUCER_ROCKETMQ_NAMESPACE`, `PRODUCER_ROCKETMQ_ACCESS_KEY`, `PRODUCER_ROCKETMQ_SECRET_KEY`,
`PRODUCER_PULSAR_URL`, `PRODUCER_PULSAR_TOPIC` and `PRODUCER_PULSAR_TOKEN`.

## Notes

1. **MySQL Configuration Requirements**:
   - Binary logging must be enabled (`log-bin=ON`)
   - Server ID must be set (`server-id=1`)
   - Binlog format should be `ROW` (`binlog_format=ROW`)
   - Every replication client reading this server needs its own replication server id. cdc-go reports a
     random id in 1001-2000 unless `source.mysql.server_id` is set; when another client (a replica,
     Maxwell, a second cdc-go) uses the same id, MySQL drops the older connection and the two reconnect
     in a loop. See [Selecting a replication server id](#selecting-a-replication-server-id) for the
     ids already in use.
2. **Permission Requirements**: When using MySQL data source, ensure the database user has sufficient permissions:

```sql
-- Create an account
CREATE USER 'cdcgo'@'%' IDENTIFIED BY 'strong_password';

-- Authorization (the REPLICATION permission is required to read the binlog)
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdcgo'@'%';

-- If cdc-go needs to do metadata queries, it also needs read permissions
GRANT SELECT ON *.* TO 'cdcgo'@'%';

-- Refresh permissions
FLUSH PRIVILEGES;
```

### Selecting a replication server id

Two replication clients that connect with one `server_id` disconnect each other, so the id cdc-go
uses has to be free. MySQL shows what it knows:

```sql
-- Replicas that register themselves (they must run with report_host set).
-- MySQL 8.0.22+; older versions: SHOW SLAVE HOSTS;
SHOW REPLICAS;

-- The id of each server you know of, run on that server
SELECT @@server_id;

-- Everything connected right now: one binlog dump thread per replication
-- client, whether or not it is listed above.
SHOW PROCESSLIST;   -- Command = 'Binlog Dump' or 'Binlog Dump GTID'
```

`SHOW REPLICAS` lists the ids of the real replicas only: canal, Maxwell and cdc-go do not register
themselves, so their dump threads are only visible — and only countable — in `SHOW PROCESSLIST`.
MySQL does not expose the id of a connected dump thread, so keep the id given to each consumer
(Maxwell's `server_id`, `source.mysql.server_id` here) and pick one that appears in neither query.
