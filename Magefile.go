//go:build mage

// Mage targets for the s3rm project.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

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

// Check runs the static analysis and test guardrails.
func Check() error {
	if err := sh.RunV("go", "vet", "./..."); err != nil {
		return err
	}
	if err := sh.RunV("go", "test", "-race", "./..."); err != nil {
		return err
	}
	if err := checkGofmt(); err != nil {
		return err
	}
	return checkErrcheck()
}

func checkGofmt() error {
	output, err := sh.Output("gofmt", "-l", ".")
	if err != nil {
		return fmt.Errorf("check Go formatting: %w", err)
	}
	if files := strings.TrimSpace(output); files != "" {
		return fmt.Errorf("gofmt found files that need formatting:\n%s\nrun gofmt -w on the listed files", files)
	}
	return nil
}

func checkErrcheck() error {
	if _, err := exec.LookPath("errcheck"); err != nil {
		return fmt.Errorf("errcheck is required for mage check; install it with `go install github.com/kisielk/errcheck@latest`: %w", err)
	}
	return sh.RunV("errcheck", "./...")
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
