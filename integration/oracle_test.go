package integration

import (
	"context"

	"github.com/markberger/yaks/internal/oracle"
	"github.com/stretchr/testify/require"
)

func (s *IntegrationTestsSuite) TestConcurrentProducerOracle() {
	s.NewAgent()

	cfg := oracle.DefaultConfig()
	cfg.DataDir = s.T().TempDir()

	result, err := oracle.Run(context.Background(), cfg)
	require.NoError(s.T(), err)
	s.T().Logf("Total records verified: %d across %d partitions from %d producers",
		result.TotalRecords, result.NumPartitions, result.NumProducers)
}
