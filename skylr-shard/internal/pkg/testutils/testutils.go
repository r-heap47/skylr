package testutils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MustParseDate returns time.Time for provided string in date format
func MustParseDate(t *testing.T, date string) time.Time {
	parsed, err := time.Parse(time.DateOnly, date)
	require.NoError(t, err)

	return parsed
}
