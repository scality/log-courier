package logcourier_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLogCourier(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LogCourier Suite")
}
