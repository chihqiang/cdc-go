# cdc-go-task

`cdc-go-task` is a reference consumer for the events published by `cdc-go`. It
reads each event from the configured middleware and routes it to the tasks that
watch its database table; the bundled `LogTask` only logs it. It can also run
those tasks directly for a primary-key value or a time range.

Use it to verify that a pipeline is wired correctly, and as a starting point for
your own consumer. It is not a production feature.

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
`--task-config` to select another file. The configuration is one setting for the
whole program, so the flag is global: write it before the subcommand name.

```bash
./cdc-go-task -c config.task.yml run
./cdc-go-task -c other.task.yml run-all-by-range "2026-10-17 00:00:00" "2026-10-18 00:00:00"
```

A subcommand does not declare the flag again, so `run -c other.task.yml` is
rejected as an unknown flag rather than silently using the default file.

`--verbose`, `--quiet`, and `--no-interaction` differ: they are builtin global
flags and work on either side of the subcommand name.

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

Runs the named task for one row of its main table. The first argument is the
task name from the configuration, not a table name:

```bash
./cdc-go-task -c config.task.yml run-by-pk user-sync 42
```

The primary-key value is for the task's main table. For a task whose main table
is `app.user`, `user-sync 42` runs that task for user 42, whether or not it also
watches sub tables. To run every task watching the same table, name each task
separately.

### `run-by-range`

Runs the named task for rows of its main table in the half-open interval
`[start, end)`:

```bash
./cdc-go-task -c config.task.yml run-by-range user-sync \
  "2026-10-17 00:00:00" "2026-10-18 00:00:00"
```

Times must use `2006-01-02 15:04:05`. The task decides which timestamp column
to use and performs any database work; the runner passes it its main table and
the range. Sub tables are processed by `run-all-by-range`.

### `run-all-by-range`

Runs every configured task for every table it watches, for rows in the
half-open interval `[start, end)`:

```bash
./cdc-go-task -c config.task.yml run-all-by-range \
  "2026-10-17 00:00:00" "2026-10-18 00:00:00"
```

Unlike `run-by-range`, no table is named: the whole configuration is worked
through, main and sub tables alike, so a task watching one main table and two
sub tables runs once per table. This is the command for a backfill that has to
cover everything the tasks handle. Times use the same `2006-01-02 15:04:05`
format.

To leave tasks out of the run, name them with `--exclude` (`-x`). The whole task
is left out, its sub tables included, and the flag repeats:

```bash
./cdc-go-task -c config.task.yml run-all-by-range \
  "2026-10-17 00:00:00" "2026-10-18 00:00:00" --exclude order-sync
```

With the checked-in configuration that runs `app.user`, `app.user_log`, and
`app.user_token` through `user-sync`, and leaves out `app.order` together with
`app.order_item`, which `order-sync` watches. A comma-separated list works too
(`-x order-sync,user-sync`). A name no task answers to is ignored, so one
exclude list can be shared by configurations that declare different tasks.

## Task configuration

`config.task.yml` contains a `consumer` section and a `tasks` list. Each task
has a name, and declares one main table and zero or more related sub tables:

```yaml
tasks:
  - name: user-sync
    main:
      database: app
      name: user
      primary_key: id
    sub:
      - database: app
        name: user_log
        primary_key: id
        ref_column: user_id
```

The name is required and cannot be shared: it is what `run-by-pk` and
`run-by-range` select a task with, and what `run-all-by-range -x` leaves one out
by. A configuration that names a task with nothing, or gives two tasks the
same name, is refused when the runner is built.

The runner indexes these declarations when it starts. A change to `app.user`
is routed using `id`; a change to `app.user_log` is routed using `user_id`.
The event's `data` must contain the relevant key. Tables that no task watches
are ignored.

## Implementation

The executable entry point is [`main.go`](./main.go). It creates the CLI
commands and attaches the task configuration flag. Its `Before` hook loads the
YAML file and supplies the tasks to `TaskCommand`; it creates the consumer only
for the continuous `run` command.

The YAML configuration type and loader are in [`../taskrun.go`](../taskrun.go).
The command actions are in [`../taskcommand.go`](../taskcommand.go):

1. `LoadTaskConfig` reads the YAML file with environment expansion once per
   invocation; `TaskConfig.BuildTasks` turns the configured task definitions into
   `task.ITask` implementations.
2. `TaskCommand` holds the consumer, tasks and its `[]*cli.Command`. The
   `NewTaskCommand` constructor creates `run`, `run-by-pk`, `run-by-range`,
   `run-all-by-range` and `list-tasks`.
3. The `run` command creates a `task.Runner` and starts consuming until the
   process is canceled or the consumer exits.
4. The runner matches each event's database and table to the task declarations.
   It reads a main table's `primary_key` or a sub table's `ref_column`, then
   calls `RunByPrimaryKey` on the matching task or tasks, passing the event's
   row along with the value.
5. The manual commands build a runner without a consumer and call
   `RunTaskByPrimaryKey` or `RunTaskByTimeRange` by task name, or
   `RunAllByTimeRange` for all declared tasks. The first two operate on the
   named task's main table; sub tables are processed by `RunAllByTimeRange`. A
   row named on the command line has no change behind it, so the task receives
   an empty `types.EventRowData`. `RunAllByTimeRange` takes the names of the
   tasks to leave out, so one backfill can skip a task it does not need.

Tasks implement the [`task.ITask`](../../task/task.go) interface: they declare
their name, their main and sub tables, and implement the primary-key and
time-range runs.
The primary-key run receives the change the run comes from as a
`types.EventRowData`, so a task can read what happened (`Type`), the values the
row has now (`Data`), and the ones an update moved away from (`Old`); it is
empty when the run was asked for by a caller rather than read from a change.
The row itself is named by a `types.PrimaryKey`, which keeps the value as the
source reported it and reads it as the type the task knows its key to be:
`Int()`, `Int64()`, `Uint64()`, `Float64()`, `Bool()`, and `String()`, with
`Value()` for the original. A value that does not fit the requested type
reports an error instead of being silently converted.
Implementations should be safe for concurrent use, repeatable when an event is
delivered again, and should return when their context is canceled so shutdown
can finish cleanly.

The bundled `task.LogTask` is a demonstration task: it logs which row or range
was routed to it, and the change the row came from when there was one. It does
**not** query the source database or perform business work. Implement a task
that satisfies `task.ITask` and configure it in the task configuration to add
application-specific behavior.

### Registering tasks from Go

Applications that need custom task logic can build the same command set with a
consumer and `task.ITask` implementations:

```go
taskCommand := cmd.NewTaskCommand()
taskCommand.SetConsumer(eventConsumer)
if err := taskCommand.AppendTasks(userSyncTask, userAuditTask); err != nil {
	return err
}

if err := taskCommand.AppendCommands(customCommand); err != nil {
	return err
}
app.Subcommands = taskCommand.Commands()
```

`TaskCommand` provides `run`, `run-by-pk`, `run-by-range` and
`run-all-by-range`, and `list-tasks` to list registered tasks with their watched
tables. The `cdc-go-task` executable loads its YAML configuration in `main.go`
and supplies the resulting tasks and consumer to this same command set.
