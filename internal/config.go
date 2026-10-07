package internal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// Config contains the S3 connection and deletion protection settings.
type Config struct {
	Bucket              string   `toml:"bucket"`
	Region              string   `toml:"region"`
	Endpoint            string   `toml:"endpoint"`
	AccessKeyID         string   `toml:"access_key_id"`
	SecretAccessKey     string   `toml:"secret_access_key"`
	SessionToken        string   `toml:"session_token"`
	ForcePathStyle      bool     `toml:"force_path_style"`
	UseVirtualHostStyle bool     `toml:"use_virtual_host_style"`
	Exclude             []string `toml:"exclude"`
}

// LoadConfig loads connection settings and returns unconsumed application arguments.
func LoadConfig(args []string) (Config, []string, error) {
	configPath, explicitConfig, excludes, remaining, err := parseConfigArgs(args)
	if err != nil {
		return Config{}, nil, err
	}

	config, err := readConfig(configPath, explicitConfig)
	if err != nil {
		return Config{}, nil, err
	}
	applyEnvironment(&config)
	config.Exclude = append(config.Exclude, excludes...)

	if err := config.Validate(); err != nil {
		return Config{}, nil, err
	}

	return config, remaining, nil
}

// Validate checks that Config has the required connection settings.
func (config Config) Validate() error {
	if config.Bucket == "" {
		return errors.New("bucket is required")
	}
	if config.Region == "" {
		return errors.New("region is required")
	}
	if (config.AccessKeyID == "") != (config.SecretAccessKey == "") {
		return errors.New("access_key_id and secret_access_key must be set together")
	}
	if config.SessionToken != "" && config.AccessKeyID == "" {
		return errors.New("session_token requires access_key_id and secret_access_key")
	}

	return nil
}

func parseConfigArgs(args []string) (string, bool, []string, []string, error) {
	configPath := ""
	explicitConfig := false
	excludes := make([]string, 0)
	remaining := make([]string, 0, len(args))

	for index := 0; index < len(args); index++ {
		argument := args[index]
		value, found, attached := optionValue(argument, "-config")
		if found {
			if value == "" {
				if attached {
					return "", false, nil, nil, errors.New("-config requires a path")
				}
				index++
				if index == len(args) || strings.HasPrefix(args[index], "-") {
					return "", false, nil, nil, errors.New("-config requires a path")
				}
				value = args[index]
			}
			configPath = value
			explicitConfig = true
			continue
		}
		value, found, attached = optionValue(argument, "-exclude")
		if found {
			if value == "" {
				if attached {
					return "", false, nil, nil, errors.New("-exclude requires a path")
				}
				index++
				if index == len(args) || strings.HasPrefix(args[index], "-") {
					return "", false, nil, nil, errors.New("-exclude requires a path")
				}
				value = args[index]
			}
			excludes = append(excludes, value)
			continue
		}
		remaining = append(remaining, argument)
	}

	if configPath == "" {
		path, err := defaultConfigPath()
		if err != nil {
			return "", false, nil, nil, err
		}
		configPath = path
	}

	return configPath, explicitConfig, excludes, remaining, nil
}

func readConfig(path string, required bool) (Config, error) {
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if required {
			return Config{}, fmt.Errorf("read config %q: %w", path, err)
		}
		return Config{}, nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	var config Config
	decoder := toml.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}

	return config, nil
}

func applyEnvironment(config *Config) {
	config.Bucket = firstEnvironment("S3RM_BUCKET", config.Bucket)
	config.Region = firstEnvironment("S3RM_REGION", config.Region)
	config.Endpoint = firstEnvironment("S3RM_ENDPOINT", config.Endpoint)

	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKeyID != "" || secretAccessKey != "" {
		config.AccessKeyID = accessKeyID
		config.SecretAccessKey = secretAccessKey
		config.SessionToken = os.Getenv("AWS_SESSION_TOKEN")
		return
	}
	if sessionToken := os.Getenv("AWS_SESSION_TOKEN"); sessionToken != "" {
		config.SessionToken = sessionToken
	}
}

func firstEnvironment(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func optionValue(argument, name string) (string, bool, bool) {
	if argument == name {
		return "", true, false
	}
	if strings.HasPrefix(argument, name+"=") {
		return strings.TrimPrefix(argument, name+"="), true, true
	}

	return "", false, false
}

func defaultConfigPath() (string, error) {
	directory, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve config directory: %w", err)
	}

	return filepath.Join(directory, "s3rm", "config.toml"), nil
}
