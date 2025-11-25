package util_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/util"
)

var _ = Describe("StartMetricsServerIfEnabled", func() {
	var (
		logger     *slog.Logger
		configSpec util.ConfigSpec
	)

	BeforeEach(func() {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelError, // Only log errors
		}))

		// Create a fresh config spec for each test
		configSpec = util.ConfigSpec{
			"test-metrics.enabled": util.ConfigVarSpec{
				DefaultValue: true,
			},
			"test-metrics.listen-address": util.ConfigVarSpec{
				DefaultValue: "127.0.0.1",
			},
			"test-metrics.listen-port": util.ConfigVarSpec{
				DefaultValue: 0, // Use port 0 to get a random available port
			},
		}

		configSpec.SetDefault("test-metrics.enabled", true)
		configSpec.SetDefault("test-metrics.listen-address", "127.0.0.1")
		configSpec.SetDefault("test-metrics.listen-port", 0)
	})

	AfterEach(func() {
		configSpec.Reset()
	})

	Context("when metrics server is disabled", func() {
		It("should return nil server", func() {
			configSpec.Set("test-metrics.enabled", false)

			server, err := util.StartMetricsServerIfEnabled(configSpec, "test-metrics", logger)

			Expect(err).ToNot(HaveOccurred())
			Expect(server).To(BeNil())
		})
	})

	Context("when metrics server is enabled", func() {
		var server *http.Server

		AfterEach(func() {
			if server != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = server.Shutdown(ctx)
			}
		})

		It("should start server and return non-nil", func() {
			configSpec.Set("test-metrics.enabled", true)
			configSpec.Set("test-metrics.listen-address", "127.0.0.1")
			configSpec.Set("test-metrics.listen-port", 0)

			var err error
			server, err = util.StartMetricsServerIfEnabled(configSpec, "test-metrics", logger)

			Expect(err).ToNot(HaveOccurred())
			Expect(server).ToNot(BeNil())
		})

		It("should respond to /metrics endpoint", func() {
			configSpec.Set("test-metrics.enabled", true)
			configSpec.Set("test-metrics.listen-address", "127.0.0.1")
			configSpec.Set("test-metrics.listen-port", 0)

			var err error
			server, err = util.StartMetricsServerIfEnabled(configSpec, "test-metrics", logger)
			Expect(err).ToNot(HaveOccurred())
			Expect(server).ToNot(BeNil())

			time.Sleep(100 * time.Millisecond)

			resp, err := http.Get(fmt.Sprintf("http://%s/metrics", server.Addr))
			Expect(err).ToNot(HaveOccurred())
			defer func() {
				_ = resp.Body.Close()
			}()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			Expect(err).ToNot(HaveOccurred())
			// Verify response contains Prometheus metrics format
			Expect(string(body)).To(ContainSubstring("# HELP"))
		})

		It("should return 404 for non-metrics paths", func() {
			configSpec.Set("test-metrics.enabled", true)
			configSpec.Set("test-metrics.listen-address", "127.0.0.1")
			configSpec.Set("test-metrics.listen-port", 0)

			var err error
			server, err = util.StartMetricsServerIfEnabled(configSpec, "test-metrics", logger)
			Expect(err).ToNot(HaveOccurred())
			Expect(server).ToNot(BeNil())

			time.Sleep(100 * time.Millisecond)

			resp, err := http.Get(fmt.Sprintf("http://%s/nonexistent", server.Addr))
			Expect(err).ToNot(HaveOccurred())
			defer func() {
				_ = resp.Body.Close()
			}()

			Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
		})

		It("should properly shutdown when closed", func() {
			configSpec.Set("test-metrics.enabled", true)
			configSpec.Set("test-metrics.listen-address", "127.0.0.1")
			configSpec.Set("test-metrics.listen-port", 0)

			var err error
			server, err = util.StartMetricsServerIfEnabled(configSpec, "test-metrics", logger)
			Expect(err).ToNot(HaveOccurred())
			Expect(server).ToNot(BeNil())

			time.Sleep(100 * time.Millisecond)

			resp, err := http.Get(fmt.Sprintf("http://%s/metrics", server.Addr))
			Expect(err).ToNot(HaveOccurred())
			_ = resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err = server.Shutdown(ctx)
			Expect(err).ToNot(HaveOccurred())

			_, err = http.Get(fmt.Sprintf("http://%s/metrics", server.Addr))
			Expect(err).To(HaveOccurred())

			server = nil
		})
	})

	Context("when listen address is invalid", func() {
		It("should return error", func() {
			configSpec.Set("test-metrics.enabled", true)
			configSpec.Set("test-metrics.listen-address", "invalid-address")
			configSpec.Set("test-metrics.listen-port", 9999)

			server, err := util.StartMetricsServerIfEnabled(configSpec, "test-metrics", logger)

			Expect(err).To(HaveOccurred())
			Expect(server).To(BeNil())
		})
	})
})
