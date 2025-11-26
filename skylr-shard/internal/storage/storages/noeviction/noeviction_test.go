package noeviction

import (
	"context"
	"testing"
	"time"

	"github.com/cutlery47/skylr/skylr-shard/internal/pkg/testutils"
	"github.com/cutlery47/skylr/skylr-shard/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		var (
			ctx = t.Context()
			now = testutils.MustParseDate(t, "2025-01-01")
			ttl = 5 * time.Second
		)

		store, err := New[string](Config{
			CurTime: func(_ context.Context) time.Time {
				return now
			},
		})
		require.NoError(t, err)
		require.NotNil(t, store)

		entry := storage.Entry[string]{
			K:   "key",
			V:   "value",
			Exp: now.Add(ttl),
		}

		stored, err := store.Set(ctx, entry)
		require.NoError(t, err)
		require.NotNil(t, stored)
		require.Equal(t, entry, *stored)

		got, err := store.Get(ctx, stored.K)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, entry, *got)
	})

	t.Run("error: ctx done", func(t *testing.T) {
		t.Parallel()

		var (
			ctx, cancel = context.WithCancel(t.Context())
			now         = testutils.MustParseDate(t, "2025-01-01")
			ttl         = 5 * time.Second
		)

		store, err := New[string](Config{})
		require.NoError(t, err)
		require.NotNil(t, store)

		entry := storage.Entry[string]{
			K:   "key",
			V:   "value",
			Exp: now.Add(ttl),
		}

		// canelling context
		cancel()

		stored, err := store.Set(ctx, entry)
		require.Error(t, err)
		require.ErrorIs(t, err, storage.ErrCtxDone)
		require.Nil(t, stored)
	})
}
