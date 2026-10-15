package cmdx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandExists_True(t *testing.T) {
	// "ls" is available on all Unix-like systems.
	assert.True(t, CommandExists("ls"))
}

func TestCommandExists_False(t *testing.T) {
	assert.False(t, CommandExists("this-command-does-not-exist-12345"))
}

func TestCommandExists_EmptyString(t *testing.T) {
	assert.False(t, CommandExists(""))
}
