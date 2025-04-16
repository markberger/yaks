package handlers

import (
	"testing"

	"github.com/markberger/yaks/internal/metastore"
	"github.com/stretchr/testify/suite"
)

type HandlersTestSuite struct {
	suite.Suite
	*metastore.TestDB
}

func (s *HandlersTestSuite) SetupSuite() {
	s.TestDB = metastore.NewTestDB()
}

func (s *HandlersTestSuite) TearDownSuite() {
	s.TestDB.Close()
}

func TestHandlersTestSuite(t *testing.T) {
	suite.Run(t, new(HandlersTestSuite))
}
