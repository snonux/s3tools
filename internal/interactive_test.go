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
	if err := confirmBucket(input, &output, "archive", selected); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.Contains(text, `2. "keep/" (protected)`) ||
		!strings.Contains(text, "Selected prefixes:\n- \"alpha/\"\nType bucket name \"archive\"") {
		t.Fatalf("prompt output = %q", text)
	}
}

func TestInteractivePrefixDisplayEscapesControlsAndPreservesKeys(t *testing.T) {
	choices := []PrefixChoice{
		{Prefix: "important/\n2. harmless/"},
		{Prefix: "carriage\rreturn"},
		{Prefix: "escape\x1b[31m"},
		{Prefix: "unicode\u202econtrol"},
	}
	input := bufio.NewReader(strings.NewReader("1,2,3,4\narchive\n"))
	var output bytes.Buffer
	selected, err := selectPrefixes(input, &output, choices)
	if err != nil {
		t.Fatalf("selectPrefixes() error = %v", err)
	}
	if !reflect.DeepEqual(selected, []string{
		"important/\n2. harmless/",
		"carriage\rreturn",
		"escape\x1b[31m",
		"unicode\u202econtrol",
	}) {
		t.Fatalf("selected = %#v, want original prefix keys", selected)
	}
	if err := confirmBucket(input, &output, "archive", selected); err != nil {
		t.Fatalf("confirmBucket() error = %v", err)
	}

	text := output.String()
	for _, prefix := range []string{
		`1. "important/\n2. harmless/"`,
		`2. "carriage\rreturn"`,
		`3. "escape\x1b[31m"`,
		`4. "unicode\u202econtrol"`,
		`- "important/\n2. harmless/"`,
		`- "carriage\rreturn"`,
		`- "escape\x1b[31m"`,
		`- "unicode\u202econtrol"`,
	} {
		if !strings.Contains(text, prefix) {
			t.Errorf("prompt output does not contain escaped prefix %q: %q", prefix, text)
		}
	}
	for _, control := range []string{"\n2. harmless/", "\r", "\x1b", "\u202e"} {
		if strings.Contains(text, control) {
			t.Errorf("prompt output contains raw control %q: %q", control, text)
		}
	}
}

func TestSelectPrefixesStopsBucketScanWithoutCancellingDeletion(t *testing.T) {
	choices := []PrefixChoice{{Prefix: "alpha/"}}
	input := bufio.NewReader(strings.NewReader("s\n1\n"))
	var output bytes.Buffer
	cancelled := false
	scan := &bucketStatsScan{cancel: func() { cancelled = true }}
	selected, err := selectPrefixesWithStats(input, &output, choices, scan)
	if err != nil || !reflect.DeepEqual(selected, []string{"alpha/"}) || !cancelled {
		t.Fatalf("selectPrefixesWithStats() = %#v, %v; cancelled = %t", selected, err, cancelled)
	}
	if !strings.Contains(output.String(), "Bucket statistics scan cancelled") {
		t.Fatalf("output = %q", output.String())
	}
}

func TestReportBucketStatsLabelsOnlyCompleteScanAsTotal(t *testing.T) {
	tests := []struct {
		name  string
		stats BucketStats
		want  string
	}{
		{name: "complete", stats: BucketStats{Complete: true, Objects: 2, Bytes: 3}, want: "Bucket total: 2 objects (3 B)"},
		{name: "incomplete", stats: BucketStats{Objects: 2, Bytes: 3, Incomplete: "page limit"}, want: "scan incomplete (page limit)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			results := make(chan bucketStatsResult, 1)
			results <- bucketStatsResult{stats: test.stats}
			var output bytes.Buffer
			reportBucketStats(&output, &bucketStatsScan{result: results})
			if !strings.Contains(output.String(), test.want) {
				t.Fatalf("output = %q, want %q", output.String(), test.want)
			}
		})
	}
}
