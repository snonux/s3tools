package internal

import (
	"context"
	"fmt"
)

// Run executes s3rm with positional arguments.
func Run(_ context.Context, args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", args)
	}

	return fmt.Errorf("s3rm is not configured yet")
}
