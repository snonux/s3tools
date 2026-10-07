//go:build mage

// Mage targets for the s3rm project.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

const binaryName = "s3rm"

// Default builds s3rm.
func Default() {
	mg.Deps(Build)
}

// Build builds the s3rm binary.
func Build() error {
	return sh.RunV("go", "build", "-o", binaryName, "./cmd/s3rm")
}

// Test runs the Go test suite.
func Test() error {
	return sh.RunV("go", "test", "./...")
}

// Install builds s3rm and copies it to GOPATH/bin.
func Install() error {
	mg.Deps(Build)

	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("resolve home directory: %w", err)
		}
		goPath = filepath.Join(home, "go")
	}

	binDir := filepath.Join(goPath, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return fmt.Errorf("create Go bin directory: %w", err)
	}

	return sh.RunV("cp", "-v", binaryName, filepath.Join(binDir, binaryName))
}
