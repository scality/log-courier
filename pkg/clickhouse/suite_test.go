package clickhouse_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestClickHouse(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ClickHouse Suite")
}
