// s3rm deletes selected S3 key prefixes.
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/snonux/s3tools/internal"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	if len(args) == 1 && args[0] == "-version" {
		if _, err := fmt.Fprintln(output, internal.Version); err != nil {
			return fmt.Errorf("print version: %w", err)
		}
		return nil
	}

	return internal.Run(context.Background(), args)
}
