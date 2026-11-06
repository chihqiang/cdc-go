package cmd

import (
	"fmt"

	"github.com/chihqiang/cdc-go/queue/consumer"
	"github.com/chihqiang/cdc-go/task"
	"github.com/chihqiang/cli"
	"github.com/chihqiang/readin"
)

const (
	TaskConfigFlag = "task-config"
	flagExclude    = "exclude"
)

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

// LoadTaskConfig reads a task configuration file with environment expansion.
func LoadTaskConfig(path string) (*TaskConfig, error) {
	cfg := &TaskConfig{}
	if err := readin.New(readin.WithEnvExpansion()).LoadFile(path, cfg); err != nil {
		return nil, fmt.Errorf("failed to load task configuration from %s: %w", path, err)
	}
	return cfg, nil
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
