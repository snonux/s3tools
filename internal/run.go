package internal

import (
	"context"
	"fmt"
)

// Run executes s3rm with positional arguments.
func Run(_ context.Context, args []string) error {
	_, remaining, err := LoadConfig(args)
	if err != nil {
		return err
	}
	if len(remaining) != 0 {
		return fmt.Errorf("unexpected arguments: %v", remaining)
	}

	return fmt.Errorf("s3rm is not configured yet")
}
