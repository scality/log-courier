package main

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"github.com/scality/log-courier/pkg/logcourier"
)

func main() {
	logcourier.ConfigSpec.AddFlag(pflag.CommandLine, "log-level", "log-level")

	configFileFlag := pflag.String("config-file", "", "Path to configuration file")
	pflag.Parse()

	configFile := *configFileFlag
	if configFile == "" {
		configFile = os.Getenv("LOG_COURIER_CONFIG_FILE")
	}

	err := logcourier.ConfigSpec.LoadConfiguration(configFile, "", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		pflag.Usage()
		os.Exit(2)
	}

	err = logcourier.ValidateConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration validation error: %v\n", err)
		pflag.Usage()
		os.Exit(2)
	}

	// TODO: Initialize and run consumer
	fmt.Println("log-courier started (stub)")
}
