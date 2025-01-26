package testutil

import (
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"os"
	"testing"
)

// createTempFile creates a temporary test file with cleanup
func CreateTempFile(t *testing.T, content, pattern string) (string, func()) {
	tmpFile, err := os.CreateTemp("", pattern)
	require.NoError(t, err, "Failed to create temp file")

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err, "Failed to write test content")

	err = tmpFile.Close()
	require.NoError(t, err, "Failed to close temp file")

	return tmpFile.Name(), func() {
		os.Remove(tmpFile.Name())
	}
}

func GetTestLogger(t *testing.T) *zap.Logger {
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	log, err := config.Build()
	require.NoError(t, err)
	return log
}
