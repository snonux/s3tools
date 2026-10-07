package internal

import (
	"os"
	"testing"
)

func TestIsTerminalRejectsNullDevice(t *testing.T) {
	file, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open null device: %v", err)
	}
	defer func() { _ = file.Close() }()

	if isTerminal(file) {
		t.Fatal("null device must not be treated as a terminal")
	}
}
