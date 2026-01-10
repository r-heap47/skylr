package noeviction

import (
	"context"
	"testing"
	"time"

	"github.com/r-heap47/skylr/skylr-shard/internal/pkg/errors"
	"github.com/r-heap47/skylr/skylr-shard/internal/pkg/testutils"
	"github.com/r-heap47/skylr/skylr-shard/internal/pkg/utils"
	"github.com/r-heap47/skylr/skylr-shard/internal/storage"
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

		store := New[string](Config{
			CurTime: utils.Const(now),
		})
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
	})

	t.Run("error: ctx done", func(t *testing.T) {
		t.Parallel()

		var (
			ctx, cancel = context.WithCancel(t.Context())
			now         = testutils.MustParseDate(t, "2025-01-01")
			ttl         = 5 * time.Second
		)

		store := New[string](Config{})
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
		require.ErrorContains(t, err, "context canceled")
		require.Nil(t, stored)
	})
}

func TestGet(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		var (
			ctx = t.Context()
			now = testutils.MustParseDate(t, "2025-01-01")
			ttl = 5 * time.Second
		)

		store := New[string](Config{
			CurTime: utils.Const(now),
		})
		require.NotNil(t, store)

		entry := storage.Entry[string]{
			K:   "key",
			V:   "value",
			Exp: now.Add(ttl),
		}

		_, err := store.Set(ctx, entry)
		require.NoError(t, err)

		got, err := store.Get(ctx, entry.K)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, entry, *got)
	})

	t.Run("error: ctx done", func(t *testing.T) {
		t.Parallel()

		var (
			ctx, cancel = context.WithCancel(t.Context())
		)

		store := New[string](Config{})
		require.NotNil(t, store)

		entry := storage.Entry[string]{
			K: "key",
		}

		// canelling context
		cancel()

		got, err := store.Get(ctx, entry.K)
		require.Nil(t, got)
		require.Error(t, err)
		require.ErrorContains(t, err, "context canceled")
	})

	t.Run("error: not found", func(t *testing.T) {
		t.Parallel()

		var (
			ctx = t.Context()
		)

		store := New[string](Config{})
		require.NotNil(t, store)

		got, err := store.Get(ctx, "k")
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrNotFound)
		require.Nil(t, got)
	})

	t.Run("error: not found (expired)", func(t *testing.T) {
		t.Parallel()

		var (
			ctx = t.Context()
			exp = testutils.MustParseDate(t, "2025-01-01")
		)

		store := New[string](Config{
			CurTime: utils.Const(exp.Add(time.Second)),
		})
		require.NotNil(t, store)

		entry := storage.Entry[string]{
			K:   "key",
			V:   "value",
			Exp: exp,
		}

		_, err := store.Set(ctx, entry)
		require.NoError(t, err)

		got, err := store.Get(ctx, entry.K)
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrNotFound)
		require.Nil(t, got)
	})
}
