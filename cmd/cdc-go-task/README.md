# cdc-go-task

`cdc-go-task` consumes CDC events published by `cdc-go` and routes
each event to the configured tasks that watch its database table. It can also
run those tasks directly for a primary-key value or a time range.

The two commands have different jobs:

```text
MySQL binlog -> cdc-go -> middleware (Kafka, Redis, ...)
                          |
                          v
                          cdc-go-task -> configured tasks
```

`cdc-go` reads the source database and publishes events according to
[`config.yml`](../../config.yml). `cdc-go-task` reads those events and
routes them according to [`config.task.yml`](../../config.task.yml). Both
configurations must point at the same middleware destination (for example, the
same Kafka brokers and topic).

## Build

From the repository root:

```bash
make build-task
```

This builds `./cdc-go-task`. To build both binaries, use `make build-all`.
You can also run the task command directly from source:

```bash
go run ./cmd/cdc-go-task --help
```

## Commands

```text
cdc-go-task [--task-config FILE] [command] [command options]
```

The task configuration defaults to `config.task.yml`. Use `-c` or
`--task-config` to select another file. The flag works before or after a
subcommand:

```bash
./cdc-go-task -c config.task.yml run
./cdc-go-task run -c config.task.yml
```

When no subcommand is supplied, the command defaults to `run`:

```bash
./cdc-go-task -c config.task.yml
```

### `run`

Continuously consumes events from the middleware and routes them to tasks
watching each event's `database.table`:

```bash
./cdc-go-task -c config.task.yml run
```

For Kafka, the producer and consumer must use the same broker and topic. For
example, configure `producer.type: kafka` and `producer.kafka` in `config.yml`, and
configure `consumer.type: kafka` and `consumer.kafka` in `config.task.yml`:

```yaml
# config.yml
producer:
  type: kafka
  kafka:
    brokers:
      - 127.0.0.1:9092
    topic: cdc-go-events
```

```yaml
# config.task.yml
consumer:
  type: kafka
  kafka:
    brokers:
      - 127.0.0.1:9092
    topic: cdc-go-events
    group: cdc-go-consumer-group
    start_offset: first
```

The `group` identifies the Kafka consumer group. `start_offset` (`first` or
`last`) is used when the group has no committed offset. The checked-in task
configuration also shows settings for Redis, RabbitMQ, RocketMQ, and Pulsar.
Environment references in either configuration use `${VAR}` or
`${VAR:-fallback}` syntax.

### `run-by-pk`

Runs watching tasks for one main-table row:

```bash
./cdc-go-task -c config.task.yml run-by-pk app.user 42
```

The table must be written as `database.table`. For a main table, the value is
its `primary_key`. For a sub table, the value is the owning main row's
`ref_column` value. For example, `app.user_log 42` runs the task for user 42.

### `run-by-range`

Runs watching tasks for rows in the half-open interval `[start, end)`:

```bash
./cdc-go-task -c config.task.yml run-by-range app.user \
  "2026-10-17 00:00:00" "2026-10-18 00:00:00"
```

Times must use `2006-01-02 15:04:05`. The task decides which timestamp column
to use and performs any database work; the runner passes it the table and
range.

## Task configuration

`config.task.yml` contains a `consumer` section and a `tasks` list. Each task
declares one main table and zero or more related sub tables:

```yaml
tasks:
  - main:
      database: app
      name: user
      primary_key: id
    sub:
      - database: app
        name: user_log
        primary_key: id
        ref_column: user_id
```

The runner indexes these declarations when it starts. A change to `app.user`
is routed using `id`; a change to `app.user_log` is routed using `user_id`.
The event's `data` must contain the relevant key. Tables that no task watches
are ignored.

## Implementation

The executable entry point is [`main.go`](./main.go). It creates the CLI
commands, attaches the task configuration flag and loader, and installs
SIGINT/SIGTERM cancellation.

The command actions and task configuration type are in
[`../taskrun.go`](../taskrun.go):

1. `loadTaskConfig` loads the YAML file with environment expansion.
2. `TaskConfig.BuildTasks` turns the configured task definitions into
   `task.ITask` implementations.
3. `TaskRun` creates the configured middleware consumer and a `task.Runner`,
   then starts consuming until the process is canceled or the consumer exits.
4. The runner matches each event's database and table to the task declarations.
   It reads a main table's `primary_key` or a sub table's `ref_column`, then
   calls `RunByPrimaryKey` on the matching task or tasks.
5. The manual commands build a runner without a consumer and call
   `RunByPrimaryKey` or `RunByTimeRange` directly.

Tasks implement the [`task.ITask`](../../task/task.go) interface: they declare
their main and sub tables and implement the primary-key and time-range runs.
Implementations should be safe for concurrent use, repeatable when an event is
delivered again, and should return when their context is canceled so shutdown
can finish cleanly.

The bundled `task.LogTask` is a demonstration task: it logs which row or range
was routed to it. It does **not** query the source database or perform business
work. Implement a task that satisfies `task.ITask` and configure it in the
task configuration to add application-specific behavior.
