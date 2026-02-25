package main

import (
	"context"
	"testing"
	"time"

	"github.com/r-heap47/skylr/skylr-client"
	pbshard "github.com/r-heap47/skylr/skylr-client/internal/pb/skylr-shard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatEntryValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		entry *pbshard.Entry
		want  string
	}{
		{"nil", nil, ""},
		{"nil value", &pbshard.Entry{Key: "k"}, ""},
		{"str", &pbshard.Entry{Key: "k", Value: &pbshard.Entry_ValueStr{ValueStr: "v"}}, "v"},
		{"int32", &pbshard.Entry{Key: "k", Value: &pbshard.Entry_ValueInt32{ValueInt32: 42}}, "42"},
		{"int64", &pbshard.Entry{Key: "k", Value: &pbshard.Entry_ValueInt64{ValueInt64: 123}}, "123"},
		{"float32", &pbshard.Entry{Key: "k", Value: &pbshard.Entry_ValueFloat{ValueFloat: 1.5}}, "1.5"},
		{"float64", &pbshard.Entry{Key: "k", Value: &pbshard.Entry_ValueDouble{ValueDouble: 2.5}}, "2.5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatEntryValue(tt.entry)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRunGet_MissingArgs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c, err := client.New(ctx, "localhost:1")
	require.NoError(t, err)
	defer c.Close()

	err = runGet(ctx, c, []string{}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "usage")
}

func TestRunSet_MissingArgs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c, err := client.New(ctx, "localhost:1")
	require.NoError(t, err)
	defer c.Close()

	err = runSet(ctx, c, []string{"key"}, time.Minute, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "usage")
}

func TestRunDelete_MissingArgs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c, err := client.New(ctx, "localhost:1")
	require.NoError(t, err)
	defer c.Close()

	err = runDelete(ctx, c, []string{}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "usage")
}
