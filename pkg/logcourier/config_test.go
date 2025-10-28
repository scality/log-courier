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
	})

	Describe("ConfigSpec", func() {
		It("should have default log-level", func() {
			err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
			Expect(err).NotTo(HaveOccurred())

			logLevel := logcourier.ConfigSpec.GetString("log-level")
			Expect(logLevel).To(Equal("info"))
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
	})
})
