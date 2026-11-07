package cmd

import "github.com/chihqiang/cli"

// needsNoConfig reports whether the command line asks for the builtin help or
// version output. They print from the command tree alone, so a configuration file
// that is missing or unreadable should not stop them.
func needsNoConfig(in *cli.Input) bool {
	switch in.Args().First() {
	case "help", "version":
		return true
	}
	return false
}
