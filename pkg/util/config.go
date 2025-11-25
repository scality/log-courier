package util

import (
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// ConfigSpec represents a full configuration specification as a map
// of configuration item names associated with their description
type ConfigSpec map[string]ConfigVarSpec

// ConfigVarSpec describes a configuration item in a ConfigSpec
type ConfigVarSpec struct {
	ParseFunc    func(any) (any, error)
	DefaultValue interface{}
	Help         string
	EnvVar       string
}

// LoadConfiguration loads and checks a hierarchy of configuration
// values based on the specification. Configuration values are taken:
//
// - from a configuration file in YAML format (if configPath is not an empty string)
//
// - and/or from environment variables
//
// - and/or from a flag set (typically generated from command-line flags)
//
// configPath is the path to the configuration file, or an empty
// string if there is no configuration file to load.
func (configSpec *ConfigSpec) LoadConfiguration(configPath string,
	configPrefix string, requiredPrefixes []string) error {
	if configPath != "" {
		viper.SetConfigType("yaml")
		viper.SetConfigFile(configPath)
		err := viper.ReadInConfig()
		if err != nil {
			return fmt.Errorf("cannot read config: %w", err)
		}
	}
	for configVarName, configVarSpec := range *configSpec {
		viper.SetDefault(configVarName, configVarSpec.DefaultValue)
		if configVarSpec.EnvVar != "" {
			_ = viper.BindEnv(configVarName, configVarSpec.EnvVar)
		}
		if configVarSpec.ParseFunc != nil {
			rawValue := viper.Get(configVarName)
			parsedValue, err := configVarSpec.ParseFunc(rawValue)
			if err != nil {
				return fmt.Errorf("failed to parse config %s: %w", configVarName, err)
			}
			viper.Set(configVarName, parsedValue)
		}
	}
	return nil
}

// AddFlag creates and binds a new flag in a pflags.FlagSet to the
// running configuration. It takes precedence over environment
// variables or configuration files.
//
// Example:
//
//	configSpec.AddFlag(pflag.CommandLine, "my-flag", "myprog.my-flag")
func (configSpec *ConfigSpec) AddFlag(flags *pflag.FlagSet, flagName, configVarName string) {
	configVarSpec := (*configSpec)[configVarName]
	switch defaultValue := configVarSpec.DefaultValue.(type) {
	case string:
		flags.String(flagName, defaultValue, configVarSpec.Help)
	case int:
		flags.Int(flagName, defaultValue, configVarSpec.Help)
	default:
		panic(fmt.Sprintf("invalid config var type: var=%s type=%T",
			configVarName, configVarSpec.DefaultValue))
	}
	_ = viper.BindPFlag(configVarName, flags.Lookup(flagName))
}

// GetString returns a single running configuration value of type string
func (configSpec *ConfigSpec) GetString(varName string) string {
	return viper.GetString(varName)
}

// GetInt returns a single running configuration value of type int
func (configSpec *ConfigSpec) GetInt(varName string) int {
	return viper.GetInt(varName)
}

// GetStringSlice returns a running configuration value of type []string
func (configSpec *ConfigSpec) GetStringSlice(varName string) []string {
	return viper.GetStringSlice(varName)
}

// GetFloat64 returns a single running configuration value of type float64
func (configSpec *ConfigSpec) GetFloat64(varName string) float64 {
	return viper.GetFloat64(varName)
}

// Set sets a configuration value
func (configSpec *ConfigSpec) Set(varName string, value interface{}) {
	viper.Set(varName, value)
}

// SetDefault sets a default value for a configuration variable
func (configSpec *ConfigSpec) SetDefault(varName string, value interface{}) {
	viper.SetDefault(varName, value)
}

// Reset resets the configuration values (only for testing)
func (configSpec *ConfigSpec) Reset() {
	viper.Reset()
}
