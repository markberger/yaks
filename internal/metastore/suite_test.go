package metastore

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type MetastoreTestSuite struct {
	suite.Suite
	*TestDB
}

func (s *MetastoreTestSuite) SetupSuite() {
	s.TestDB = NewTestDB()
}

func (s *MetastoreTestSuite) TearDownSuite() {
	s.TestDB.Close()
}

func TestMetastoreTestSuite(t *testing.T) {
	suite.Run(t, new(MetastoreTestSuite))
}
