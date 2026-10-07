package internal

import (
	"context"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	if Version == "" {
		t.Fatal("Version must not be empty")
	}
}

func TestRun(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "no arguments",
			want: "s3rm is not configured yet",
		},
		{
			name: "positional arguments rejected",
			args: []string{"bucket"},
			want: "unexpected positional arguments: [bucket]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Run(context.Background(), test.args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Run() error = %v, want %q", err, test.want)
			}
		})
	}
}
