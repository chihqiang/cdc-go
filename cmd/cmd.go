package cmd

import "github.com/chihqiang/cli"

// NeedsNoConfig reports whether the command line asks for the builtin help or
// version output. They print from the command tree alone, so a configuration file
// that is missing or unreadable should not stop them.
func NeedsNoConfig(in *cli.Input) bool {
	switch in.Args().First() {
	case "help", "version":
		return true
	}
	return false
}

// NeedsConsumer reports whether the subcommand the command line asks for reads
// events from the consumer.
//
// It is the run command -- and no subcommand at all, which runs it -- that does;
// the others work from the rows and ranges a caller names, so they are wired
// with the tasks alone and never call SetConsumer.
func NeedsConsumer(in *cli.Input) bool {
	switch in.Args().First() {
	case "", CmdTaskRun:
		return true
	}
	return false
}
