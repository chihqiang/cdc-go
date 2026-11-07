package cmd

import (
	"context"
	"fmt"

	"github.com/chihqiang/cdc-go/queue/consumer"
	"github.com/chihqiang/cdc-go/task"
	"github.com/chihqiang/cli"
	"github.com/chihqiang/readin"
)

const TaskConfigFlag = "task-config"

// TaskConfig is the YAML configuration used by cdc-go-task.
type TaskConfig struct {
	Consumer consumer.Config `json:"consumer"`
	Tasks    []task.LogTask  `json:"tasks"`
}

// BuildTasks returns the configured tasks as task interfaces.
func (c *TaskConfig) BuildTasks() []task.ITask {
	tasks := make([]task.ITask, 0, len(c.Tasks))
	for i := range c.Tasks {
		tasks = append(tasks, &c.Tasks[i])
	}
	return tasks
}

// TaskFlags returns the root flag that selects the YAML configuration file.
func TaskFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    TaskConfigFlag,
			Aliases: []string{"c"},
			Usage:   "Load task configuration from `FILE`",
			Value:   "config.task.yml",
		},
	}
}

// LoadTaskConfig reads a task configuration file with environment expansion.
func LoadTaskConfig(path string) (*TaskConfig, error) {
	cfg := &TaskConfig{}
	if err := readin.New(readin.WithEnvExpansion()).LoadFile(path, cfg); err != nil {
		return nil, fmt.Errorf("failed to load task configuration from %s: %w", path, err)
	}
	return cfg, nil
}

// TaskBefore Builds the root Before hook of cdc-go-task: it loads the task
// configuration, adds the tasks it declares to taskCommand, and -- for the run
// command, which is the one that consumes events -- raises the consumer those
// tasks read from. The other subcommands only need the tasks.
func TaskBefore(taskCommand *TaskCommand) cli.BeforeFunc {
	return func(ctx context.Context, in *cli.Input, _ *cli.Output) (context.Context, error) {
		if NeedsNoConfig(in) {
			return ctx, nil
		}
		cfg, err := LoadTaskConfig(in.String(TaskConfigFlag))
		if err != nil {
			return ctx, err
		}
		if err := taskCommand.AppendTasks(cfg.BuildTasks()...); err != nil {
			return ctx, err
		}
		if !NeedsConsumer(in) {
			return ctx, nil
		}
		eventConsumer, err := consumer.NewConsumer(cfg.Consumer)
		if err != nil {
			return ctx, err
		}
		taskCommand.SetConsumer(eventConsumer)
		return ctx, nil
	}
}
