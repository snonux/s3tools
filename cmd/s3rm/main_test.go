package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/snonux/s3tools/internal"
)

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestRun(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantOut string
		wantErr string
	}{
		{
			name:    "prints version",
			args:    []string{"-version"},
			wantOut: internal.Version + "\n",
		},
		{
			name:    "forwards future config flag to application",
			args:    []string{"-config", "config.toml"},
			wantErr: "read config",
		},
		{
			name:    "does not treat version with another argument as version request",
			args:    []string{"-version", "bucket"},
			wantErr: "bucket is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			err := run(test.args, &output)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("run() error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("run() error = %v", err)
			}
			if output.String() != test.wantOut {
				t.Errorf("run() output = %q, want %q", output.String(), test.wantOut)
			}
		})
	}
}

func TestRunVersionWriteFailure(t *testing.T) {
	err := run([]string{"-version"}, errorWriter{})
	if err == nil || !strings.Contains(err.Error(), "print version: write failed") {
		t.Fatalf("run() error = %v, want version write error", err)
	}
}
