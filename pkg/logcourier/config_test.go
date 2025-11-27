package logcourier_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/pflag"
	"github.com/scality/log-courier/pkg/logcourier"
)

var _ = Describe("Configuration", Ordered, func() {
	AfterEach(func() {
		logcourier.ConfigSpec.Reset()
		pflag.CommandLine = pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
		_ = os.Unsetenv("LOG_COURIER_LOG_LEVEL")
		_ = os.Unsetenv("LOG_COURIER_CONSUMER_COUNT_THRESHOLD")
		_ = os.Unsetenv("LOG_COURIER_CONSUMER_TIME_THRESHOLD_SECONDS")
	})

	Describe("ConfigSpec", func() {
		It("should have default log-level", func() {
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			logLevel := logcourier.ConfigSpec.GetString("log-level")
			expectedDefault := logcourier.ConfigSpec["log-level"].DefaultValue.(string)
			Expect(logLevel).To(Equal(expectedDefault))
		})

		It("should load log-level from environment variable", func() {
			Expect(os.Setenv("LOG_COURIER_LOG_LEVEL", "debug")).To(Succeed())

			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			logLevel := logcourier.ConfigSpec.GetString("log-level")
			Expect(logLevel).To(Equal("debug"))
		})

		It("should load log-level from file", func() {
			tmpFile, err := os.CreateTemp("", "config-*.yaml")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = os.Remove(tmpFile.Name()) }()

			_, err = tmpFile.WriteString("log-level: error\n")
			Expect(err).NotTo(HaveOccurred())
			Expect(tmpFile.Close()).To(Succeed())

			err = logcourier.ConfigSpec.LoadConfiguration(tmpFile.Name(), "", nil)
			Expect(err).NotTo(HaveOccurred())

			logLevel := logcourier.ConfigSpec.GetString("log-level")
			Expect(logLevel).To(Equal("error"))
		})

		It("should override file with environment variable", func() {
			tmpFile, err := os.CreateTemp("", "config-*.yaml")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = os.Remove(tmpFile.Name()) }()

			_, err = tmpFile.WriteString("log-level: error\n")
			Expect(err).NotTo(HaveOccurred())
			Expect(tmpFile.Close()).To(Succeed())

			Expect(os.Setenv("LOG_COURIER_LOG_LEVEL", "warn")).To(Succeed())

			err = logcourier.ConfigSpec.LoadConfiguration(tmpFile.Name(), "", nil)
			Expect(err).NotTo(HaveOccurred())

			logLevel := logcourier.ConfigSpec.GetString("log-level")
			Expect(logLevel).To(Equal("warn"))
		})

		It("should override environment with flag", func() {
			Expect(os.Setenv("LOG_COURIER_LOG_LEVEL", "warn")).To(Succeed())

			logcourier.ConfigSpec.AddFlag(pflag.CommandLine, "log-level", "log-level")
			err := pflag.CommandLine.Set("log-level", "debug")
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			logLevel := logcourier.ConfigSpec.GetString("log-level")
			Expect(logLevel).To(Equal("debug"))
		})

		It("should parse comma-separated clickhouse URLs", func() {
			Expect(os.Setenv("LOG_COURIER_CLICKHOUSE_URL", "host1:9000,host2:9000,host3:9000")).To(Succeed())
			defer func() { _ = os.Unsetenv("LOG_COURIER_CLICKHOUSE_URL") }()

			logcourier.ConfigSpec.Reset()
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			hosts := logcourier.ConfigSpec.GetStringSlice("clickhouse.url")
			Expect(hosts).To(HaveLen(3))
			Expect(hosts).To(ConsistOf("host1:9000", "host2:9000", "host3:9000"))
		})

		It("should handle single URL as array", func() {
			Expect(os.Setenv("LOG_COURIER_CLICKHOUSE_URL", "localhost:9002")).To(Succeed())
			defer func() { _ = os.Unsetenv("LOG_COURIER_CLICKHOUSE_URL") }()

			logcourier.ConfigSpec.Reset()
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			hosts := logcourier.ConfigSpec.GetStringSlice("clickhouse.url")
			Expect(hosts).To(HaveLen(1))
			Expect(hosts[0]).To(Equal("localhost:9002"))
		})

		It("should trim whitespace from host addresses", func() {
			Expect(os.Setenv("LOG_COURIER_CLICKHOUSE_URL", "host1:9000 , host2:9000 , host3:9000")).To(Succeed())
			defer func() { _ = os.Unsetenv("LOG_COURIER_CLICKHOUSE_URL") }()

			logcourier.ConfigSpec.Reset()
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			hosts := logcourier.ConfigSpec.GetStringSlice("clickhouse.url")
			Expect(hosts).To(ConsistOf("host1:9000", "host2:9000", "host3:9000"))
		})
	})

	Describe("Consumer Configuration", func() {
		It("should have default count threshold", func() {
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			countThreshold := logcourier.ConfigSpec.GetInt("consumer.count-threshold")
			expectedDefault := logcourier.ConfigSpec["consumer.count-threshold"].DefaultValue.(int)
			Expect(countThreshold).To(Equal(expectedDefault))
		})

		It("should have default time threshold", func() {
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			timeThreshold := logcourier.ConfigSpec.GetInt("consumer.time-threshold-seconds")
			expectedDefault := logcourier.ConfigSpec["consumer.time-threshold-seconds"].DefaultValue.(int)
			Expect(timeThreshold).To(Equal(expectedDefault))
		})

		It("should load count threshold from environment variable", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_COUNT_THRESHOLD", "500")).To(Succeed())

			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			countThreshold := logcourier.ConfigSpec.GetInt("consumer.count-threshold")
			Expect(countThreshold).To(Equal(500))
		})

		It("should load time threshold from environment variable", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_TIME_THRESHOLD_SECONDS", "300")).To(Succeed())

			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			timeThreshold := logcourier.ConfigSpec.GetInt("consumer.time-threshold-seconds")
			Expect(timeThreshold).To(Equal(300))
		})

		It("should load thresholds from file", func() {
			tmpFile, err := os.CreateTemp("", "config-*.yaml")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = os.Remove(tmpFile.Name()) }()

			_, err = tmpFile.WriteString("consumer:\n  count-threshold: 2000\n  time-threshold-seconds: 600\n")
			Expect(err).NotTo(HaveOccurred())
			Expect(tmpFile.Close()).To(Succeed())

			err = logcourier.ConfigSpec.LoadConfiguration(tmpFile.Name(), "", nil)
			Expect(err).NotTo(HaveOccurred())

			countThreshold := logcourier.ConfigSpec.GetInt("consumer.count-threshold")
			Expect(countThreshold).To(Equal(2000))

			timeThreshold := logcourier.ConfigSpec.GetInt("consumer.time-threshold-seconds")
			Expect(timeThreshold).To(Equal(600))
		})
	})

	Describe("ValidateConfig", func() {
		It("should accept valid log level: info", func() {
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).NotTo(HaveOccurred())
		})

		It("should accept valid log level: debug", func() {
			Expect(os.Setenv("LOG_COURIER_LOG_LEVEL", "debug")).To(Succeed())
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).NotTo(HaveOccurred())
		})

		It("should accept valid log level: warn", func() {
			Expect(os.Setenv("LOG_COURIER_LOG_LEVEL", "warn")).To(Succeed())
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).NotTo(HaveOccurred())
		})

		It("should accept valid log level: error", func() {
			Expect(os.Setenv("LOG_COURIER_LOG_LEVEL", "error")).To(Succeed())
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject invalid log level", func() {
			Expect(os.Setenv("LOG_COURIER_LOG_LEVEL", "invalid")).To(Succeed())
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid log-level"))
			Expect(err.Error()).To(ContainSubstring("invalid"))
		})

		It("should reject zero count threshold", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_COUNT_THRESHOLD", "0")).To(Succeed())
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("consumer.count-threshold must be positive"))
		})

		It("should reject negative count threshold", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_COUNT_THRESHOLD", "-10")).To(Succeed())
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("consumer.count-threshold must be positive"))
		})

		It("should reject zero time threshold", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_TIME_THRESHOLD_SECONDS", "0")).To(Succeed())
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("consumer.time-threshold-seconds must be positive"))
		})

		It("should reject negative time threshold", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_TIME_THRESHOLD_SECONDS", "-60")).To(Succeed())
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("consumer.time-threshold-seconds must be positive"))
		})

		It("should accept count threshold at maximum limit", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_COUNT_THRESHOLD", "100000")).To(Succeed())
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject count threshold exceeding maximum", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_COUNT_THRESHOLD", "100001")).To(Succeed())
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exceeds maximum allowed"))
		})

		It("should reject zero min discovery interval", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_MIN_DISCOVERY_INTERVAL_SECONDS", "0")).To(Succeed())
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("consumer.min-discovery-interval-seconds must be positive"))
		})

		It("should reject zero max discovery interval", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_MIN_DISCOVERY_INTERVAL_SECONDS", "30")).To(Succeed())
			Expect(os.Setenv("LOG_COURIER_CONSUMER_MAX_DISCOVERY_INTERVAL_SECONDS", "0")).To(Succeed())
			defer func() { _ = os.Unsetenv("LOG_COURIER_CONSUMER_MIN_DISCOVERY_INTERVAL_SECONDS") }()
			defer func() { _ = os.Unsetenv("LOG_COURIER_CONSUMER_MAX_DISCOVERY_INTERVAL_SECONDS") }()

			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("consumer.max-discovery-interval-seconds must be positive"))
		})

		It("should reject min > max discovery interval", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_MIN_DISCOVERY_INTERVAL_SECONDS", "120")).To(Succeed())
			Expect(os.Setenv("LOG_COURIER_CONSUMER_MAX_DISCOVERY_INTERVAL_SECONDS", "30")).To(Succeed())
			defer func() { _ = os.Unsetenv("LOG_COURIER_CONSUMER_MIN_DISCOVERY_INTERVAL_SECONDS") }()
			defer func() { _ = os.Unsetenv("LOG_COURIER_CONSUMER_MAX_DISCOVERY_INTERVAL_SECONDS") }()

			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must be <= consumer.max-discovery-interval-seconds"))
		})

		It("should accept min <= max discovery interval", func() {
			Expect(os.Setenv("LOG_COURIER_CONSUMER_MIN_DISCOVERY_INTERVAL_SECONDS", "30")).To(Succeed())
			Expect(os.Setenv("LOG_COURIER_CONSUMER_MAX_DISCOVERY_INTERVAL_SECONDS", "120")).To(Succeed())
			defer func() { _ = os.Unsetenv("LOG_COURIER_CONSUMER_MIN_DISCOVERY_INTERVAL_SECONDS") }()
			defer func() { _ = os.Unsetenv("LOG_COURIER_CONSUMER_MAX_DISCOVERY_INTERVAL_SECONDS") }()

			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			err = logcourier.ValidateConfig()
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
