package util_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/util"
)

func TestUtil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Util Suite")
}

var _ = Describe("ParseCommaSeparatedHosts", func() {
	It("should parse comma-separated hosts", func() {
		result := util.ParseCommaSeparatedHosts("host1:9000,host2:9000,host3:9000")
		Expect(result).To(Equal([]string{"host1:9000", "host2:9000", "host3:9000"}))
	})

	It("should handle single host", func() {
		result := util.ParseCommaSeparatedHosts("localhost:9002")
		Expect(result).To(Equal([]string{"localhost:9002"}))
	})

	It("should trim whitespace", func() {
		result := util.ParseCommaSeparatedHosts(" host1:9000 , host2:9000 , host3:9000 ")
		Expect(result).To(Equal([]string{"host1:9000", "host2:9000", "host3:9000"}))
	})

	It("should handle empty string", func() {
		result := util.ParseCommaSeparatedHosts("")
		Expect(result).To(BeEmpty())
	})

	It("should skip empty parts", func() {
		result := util.ParseCommaSeparatedHosts("host1:9000,,host2:9000")
		Expect(result).To(Equal([]string{"host1:9000", "host2:9000"}))
	})
})
