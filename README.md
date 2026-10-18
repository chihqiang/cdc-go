<div align="center">
<h1>cdc-go</h1>

[![Auth](https://img.shields.io/badge/Auth-chihqiang-ff69b4)](https://github.com/chihqiang)
[![GitHub Pull Requests](https://img.shields.io/github/issues-pr/chihqiang/cdc-go)](https://github.com/chihqiang/cdc-go/pulls)
[![Release](https://img.shields.io/github/release/chihqiang/cdc-go.svg?style=flat-square)](https://github.com/chihqiang/cdc-go/releases)
[![GitHub Pull Requests](https://img.shields.io/github/stars/chihqiang/cdc-go)](https://github.com/chihqiang/cdc-go/stargazers)
[![HitCount](https://views.whatilearened.today/views/github/chihqiang/cdc-go.svg)](https://github.com/chihqiang/cdc-go)
[![GitHub license](https://img.shields.io/github/license/chihqiang/cdc-go)](https://github.com/chihqiang/cdc-go/blob/main/LICENSE)

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

```yaml
# ==========================================
# cdc-go Configuration File Example (YAML)
# ==========================================
# The full annotated file is available at ./config.yml in the repository root.

# ---------- Offset Storage Configuration ----------
store:
  type: "${STORE_TYPE:-file}"              # Storage type: file / redis

  file:
    dir: "${STORE_FILE_DIR:-runtime}"      # Directory for storing offset files

  redis:
    addr: "${STORE_REDIS_ADDR:-127.0.0.1:6379}"   # Redis address
    password: "${STORE_REDIS_PASSWORD:-}"         # Redis password (leave empty if none)
    db: "${STORE_REDIS_DB:-0}"                    # Redis database number (default 0)

# ---------- Data Source Configuration ----------
source:
  type: "${SOURCE_TYPE:-mysql}"            # Data source type: mysql

  mysql:
    addr: "${SOURCE_MYSQL_ADDR:-127.0.0.1:3306}"  # Database address (host:port)
    user: "${SOURCE_MYSQL_USER:-root}"            # Database username (recommended to use a dedicated account in production)
    password: "${SOURCE_MYSQL_PASSWORD:-123456}"  # Database password
    exclude_table_regex:              # Tables to exclude (regex patterns)
      - "mysql.*"
      - "information_schema.*"
      - "performance_schema.*"
      - "sys.*"
    include_table_regex:              # Tables to include (regex patterns, empty = all except excluded)
      # - "cdcgo.*"              # Example: only listen to cdcgo tables

# ---------- Producer Configuration ----------
producer:
  type: "${PRODUCER_TYPE:-stdout}"       # Producer type: stdout / kafka / redis / rabbitmq / rocketmq / pulsar

  # Kafka settings
  kafka:
    brokers:
      - "${PRODUCER_KAFKA_BROKERS:-127.0.0.1:9092}"  # Kafka broker list
    topic: "${PRODUCER_KAFKA_TOPIC:-cdc-go-events}"  # Kafka topic name

  # RabbitMQ settings
  rabbitmq:
    url: "${PRODUCER_RABBITMQ_URL:-amqp://guest:guest@127.0.0.1:5672/}" # RabbitMQ connection URL
    exchange: "${PRODUCER_RABBITMQ_EXCHANGE:-cdc-go-exchange}" # Exchange name (declared and bound on startup)
    exchange_type: "${PRODUCER_RABBITMQ_EXCHANGE_TYPE:-direct}"     # Exchange type: direct / fanout / topic (headers is refused)
    routing_key: "${PRODUCER_RABBITMQ_ROUTING_KEY:-cdc-go-events}" # Publish key, and the key the queue is bound with
    queue: "${PRODUCER_RABBITMQ_QUEUE:-cdc-go-events}"      # Queue name
    durable: true              # Whether the queue should survive server restarts (also makes messages persistent)
    auto_delete: false         # Whether the queue should auto-delete when unused
    auto_ack: false            # Whether to auto-acknowledge messages (unused when publishing)
    exclusive: false           # Whether the queue is exclusive to this connection
    no_wait: false             # Whether to wait for the server to confirm queue declaration

  # Redis settings
  redis:
    addr: "${PRODUCER_REDIS_ADDR:-127.0.0.1:6379}"        # Redis address
    password: "${PRODUCER_REDIS_PASSWORD:-}"              # Redis password
    db: "${PRODUCER_REDIS_DB:-0}"                         # Redis database number
    key: "${PRODUCER_REDIS_KEY:-cdc-go-events}"      # Redis key for storing events

  # RocketMQ settings
  rocketmq:
    servers:
      - "${PRODUCER_ROCKETMQ_SERVERS:-127.0.0.1:9876}"  # RocketMQ NameServer address
    topic: "${PRODUCER_ROCKETMQ_TOPIC:-cdc-go-events}"  # RocketMQ topic name
    group: "${PRODUCER_ROCKETMQ_GROUP:-cdc-go-group}"   # Producer group name
    namespace: "${PRODUCER_ROCKETMQ_NAMESPACE:-}"        # Namespace
    access_key: "${PRODUCER_ROCKETMQ_ACCESS_KEY:-}"      # Access key
    secret_key: "${PRODUCER_ROCKETMQ_SECRET_KEY:-}"      # Secret key
    retry: 3                   # Retry count on failure

  # Pulsar settings
  pulsar:
    url: "${PRODUCER_PULSAR_URL:-pulsar://127.0.0.1:6650}"  # Pulsar broker URL
    topic: "${PRODUCER_PULSAR_TOPIC:-cdc-go-events}"   # Pulsar topic name
    token: "${PRODUCER_PULSAR_TOKEN:-}"        # Optional authentication token
    operation_timeout: 30           # Operation timeout in seconds
    connection_timeout: 30          # Connection timeout in seconds
```

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
`SOURCE_MYSQL_USER`, `SOURCE_MYSQL_PASSWORD`, `PRODUCER_TYPE`, `PRODUCER_KAFKA_BROKERS`,
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
