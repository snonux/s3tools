package internal

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name      string
		contents  string
		args      []string
		env       map[string]string
		want      Config
		wantArgs  []string
		wantError string
	}{
		{
			name:     "file config and CLI exclusions",
			contents: "bucket = 'photos'\nregion = 'eu-west-1'\nendpoint = 'https://s3.example.test'\naccess_key_id = 'file-key'\nsecret_access_key = 'file-secret'\nforce_path_style = true\nuse_virtual_host_style = true\nexclude = ['keep/', 'root.txt']\n",
			args:     []string{"-config", "CONFIG", "-exclude=logs/", "-prefix", "photos/"},
			want: Config{
				Bucket: "photos", Region: "eu-west-1", Endpoint: "https://s3.example.test", AccessKeyID: "file-key", SecretAccessKey: "file-secret", ForcePathStyle: true, UseVirtualHostStyle: true,
				Exclude: []string{"keep/", "root.txt", "logs/"},
			},
			wantArgs: []string{"-prefix", "photos/"},
		},
		{
			name:     "environment overrides file",
			contents: "bucket = 'file-bucket'\nregion = 'file-region'\naccess_key_id = 'file-key'\nsecret_access_key = 'file-secret'\n",
			args:     []string{"-config=CONFIG"},
			env: map[string]string{
				"S3RM_BUCKET": "env-bucket", "S3RM_REGION": "env-region", "S3RM_ENDPOINT": "https://garage.example.test",
				"AWS_ACCESS_KEY_ID": "env-key", "AWS_SECRET_ACCESS_KEY": "env-secret", "AWS_SESSION_TOKEN": "token",
			},
			want:     Config{Bucket: "env-bucket", Region: "env-region", Endpoint: "https://garage.example.test", AccessKeyID: "env-key", SecretAccessKey: "env-secret", SessionToken: "token"},
			wantArgs: []string{},
		},
		{
			name:     "session token overrides file credentials",
			contents: "bucket = 'file-bucket'\nregion = 'file-region'\naccess_key_id = 'file-key'\nsecret_access_key = 'file-secret'\nsession_token = 'old-token'\n",
			args:     []string{"-config=CONFIG"},
			env:      map[string]string{"AWS_SESSION_TOKEN": "new-token"},
			want:     Config{Bucket: "file-bucket", Region: "file-region", AccessKeyID: "file-key", SecretAccessKey: "file-secret", SessionToken: "new-token"},
			wantArgs: []string{},
		},
		{
			name:     "environment keys clear file session token",
			contents: "bucket = 'file-bucket'\nregion = 'file-region'\naccess_key_id = 'file-key'\nsecret_access_key = 'file-secret'\nsession_token = 'old-token'\n",
			args:     []string{"-config=CONFIG"},
			env:      map[string]string{"AWS_ACCESS_KEY_ID": "env-key", "AWS_SECRET_ACCESS_KEY": "env-secret"},
			want:     Config{Bucket: "file-bucket", Region: "file-region", AccessKeyID: "env-key", SecretAccessKey: "env-secret"},
			wantArgs: []string{},
		},
		{
			name:      "missing access key pair",
			contents:  "bucket = 'photos'\nregion = 'garage'\n",
			args:      []string{"-config", "CONFIG"},
			env:       map[string]string{"AWS_ACCESS_KEY_ID": "only-key"},
			wantError: "access_key_id and secret_access_key must be set together",
		},
		{
			name:      "missing config value",
			args:      []string{"-config"},
			wantError: "-config requires a path",
		},
		{
			name:      "missing config value before another option",
			args:      []string{"-config", "-prefix", "photos/"},
			wantError: "-config requires a path",
		},
		{
			name:      "empty attached config value",
			args:      []string{"-config=", "bucket"},
			wantError: "-config requires a path",
		},
		{
			name:      "missing exclude value before another option",
			args:      []string{"-exclude", "-prefix", "photos/"},
			wantError: "-exclude requires a path",
		},
		{
			name:      "empty attached exclude value",
			args:      []string{"-exclude=", "prefix"},
			wantError: "-exclude requires a path",
		},
		{
			name:      "explicit missing config file",
			args:      []string{"-config", "CONFIG"},
			wantError: "read config",
		},
		{
			name:      "unknown TOML field",
			contents:  "bucket = 'photos'\nregion = 'garage'\nbuket = 'typo'\n",
			args:      []string{"-config", "CONFIG"},
			wantError: "parse config",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("S3RM_BUCKET", "")
			t.Setenv("S3RM_REGION", "")
			t.Setenv("S3RM_ENDPOINT", "")
			t.Setenv("AWS_ACCESS_KEY_ID", "")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "")
			t.Setenv("AWS_SESSION_TOKEN", "")
			for name, value := range test.env {
				t.Setenv(name, value)
			}

			directory := t.TempDir()
			path := filepath.Join(directory, "config.toml")
			if test.contents != "" {
				if err := os.WriteFile(path, []byte(test.contents), 0o600); err != nil {
					t.Fatalf("write config: %v", err)
				}
			}
			args := replaceConfigPath(test.args, path)

			got, remaining, err := LoadConfig(args)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("LoadConfig() error = %v, want %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("LoadConfig() config = %#v, want %#v", got, test.want)
			}
			if !reflect.DeepEqual(remaining, test.wantArgs) {
				t.Errorf("LoadConfig() remaining = %#v, want %#v", remaining, test.wantArgs)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		wantError string
	}{
		{name: "missing bucket", config: Config{Region: "garage"}, wantError: "bucket is required"},
		{name: "missing region", config: Config{Bucket: "photos"}, wantError: "region is required"},
		{name: "lone session token", config: Config{Bucket: "photos", Region: "garage", SessionToken: "token"}, wantError: "session_token requires"},
		{name: "complete", config: Config{Bucket: "photos", Region: "garage"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.wantError == "" && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Fatalf("Validate() error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func replaceConfigPath(args []string, path string) []string {
	replaced := make([]string, len(args))
	for index, argument := range args {
		replaced[index] = strings.ReplaceAll(argument, "CONFIG", path)
	}
	return replaced
}
