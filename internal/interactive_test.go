package internal

import (
	"bufio"
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestParsePrefixSelection(t *testing.T) {
	choices := []PrefixChoice{{Prefix: "alpha/"}, {Prefix: "keep/", Protected: true}, {Prefix: "zeta/"}}
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr string
	}{
		{name: "multiple selections", input: "3, 1", want: []string{"zeta/", "alpha/"}},
		{name: "deduplicates", input: "1,1 3", want: []string{"alpha/", "zeta/"}},
		{name: "protected prefix", input: "2", wantErr: "protected"},
		{name: "out of range", input: "4", wantErr: "invalid prefix selection"},
		{name: "invalid number", input: "one", wantErr: "invalid prefix selection"},
		{name: "empty cancels", input: "", wantErr: "cancelled"},
		{name: "quit cancels", input: "q", wantErr: "cancelled"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParsePrefixSelection(test.input, choices)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("ParsePrefixSelection() error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ParsePrefixSelection() = %#v, %v; want %#v, nil", got, err, test.want)
			}
		})
	}
}

func TestConfirmBucket(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  error
	}{
		{name: "exact match", input: "archive", want: nil},
		{name: "surrounding whitespace cancels", input: " archive ", want: errInteractiveCancelled},
		{name: "case mismatch cancels", input: "Archive", want: errInteractiveCancelled},
		{name: "empty cancels", input: "", want: errInteractiveCancelled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ConfirmBucket(test.input, "archive")
			if !errors.Is(err, test.want) {
				t.Fatalf("ConfirmBucket() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPrefixChoicesShowsProtectedPrefixes(t *testing.T) {
	got := prefixChoices([]string{"zeta/", "keep/", "alpha/"}, []string{"keep/"})
	want := []PrefixChoice{{Prefix: "alpha/"}, {Prefix: "keep/", Protected: true}, {Prefix: "zeta/"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("prefixChoices() = %#v, want %#v", got, want)
	}
}

func TestInteractivePrompts(t *testing.T) {
	choices := []PrefixChoice{{Prefix: "alpha/"}, {Prefix: "keep/", Protected: true}}
	input := bufio.NewReader(strings.NewReader("1\narchive\n"))
	var output bytes.Buffer
	selected, err := selectPrefixes(input, &output, choices)
	if err != nil || !reflect.DeepEqual(selected, []string{"alpha/"}) {
		t.Fatalf("selectPrefixes() = %#v, %v", selected, err)
	}
	if err := confirmBucket(input, &output, "archive"); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.Contains(text, "2. keep/ (protected)") || !strings.Contains(text, "Type bucket name \"archive\"") {
		t.Fatalf("prompt output = %q", text)
	}
}
