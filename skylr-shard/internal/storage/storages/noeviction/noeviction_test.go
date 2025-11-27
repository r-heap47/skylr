package noeviction

import (
	"context"
	stderrs "errors"
	"sync"
	"testing"
	"time"

	"github.com/cutlery47/skylr/skylr-shard/internal/pkg/errors"
	"github.com/cutlery47/skylr/skylr-shard/internal/pkg/testutils"
	"github.com/cutlery47/skylr/skylr-shard/internal/pkg/utils"
	"github.com/cutlery47/skylr/skylr-shard/internal/storage"
	"github.com/samber/lo"
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
			CurTime: utils.Const(now),
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
		require.ErrorIs(t, err, errors.ErrCtxDone)
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

		store, err := New[string](Config{
			CurTime: utils.Const(now),
		})
		require.NoError(t, err)
		require.NotNil(t, store)

		entry := storage.Entry[string]{
			K:   "key",
			V:   "value",
			Exp: now.Add(ttl),
		}

		_, err = store.Set(ctx, entry)
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

		store, err := New[string](Config{})
		require.NoError(t, err)
		require.NotNil(t, store)

		entry := storage.Entry[string]{
			K: "key",
		}

		// canelling context
		cancel()

		got, err := store.Get(ctx, entry.K)
		require.Nil(t, got)
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrCtxDone)
	})

	t.Run("error: not found", func(t *testing.T) {
		t.Parallel()

		var (
			ctx = t.Context()
		)

		store, err := New[string](Config{})
		require.NoError(t, err)
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

		store, err := New[string](Config{
			CurTime: utils.Const(exp.Add(time.Second)),
		})
		require.NoError(t, err)
		require.NotNil(t, store)

		entry := storage.Entry[string]{
			K:   "key",
			V:   "value",
			Exp: exp,
		}

		_, err = store.Set(ctx, entry)
		require.NoError(t, err)

		got, err := store.Get(ctx, entry.K)
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrNotFound)
		require.Nil(t, got)
	})
}

func TestClean(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		var (
			ctx = t.Context()
			now = testutils.MustParseDate(t, "2025-01-01")

			exp1 = now.Add(-2 * time.Second)
			exp2 = now.Add(-1 * time.Second)
			exp3 = now.Add(time.Second)
		)

		store := &noeviction[string]{
			store:   make(map[string]storage.Entry[string]),
			mu:      &sync.RWMutex{},
			curTime: utils.Const(now),
		}

		entry1 := storage.Entry[string]{
			K:   "key1",
			V:   "value",
			Exp: exp1,
		}

		entry2 := storage.Entry[string]{
			K:   "key2",
			V:   "value",
			Exp: exp2,
		}

		entry3 := storage.Entry[string]{
			K:   "key3",
			V:   "value",
			Exp: exp3,
		}

		entries := []storage.Entry[string]{
			entry1, entry2, entry3,
		}

		for _, el := range entries {
			_, err := store.Set(ctx, el)
			require.NoError(t, err)
		}

		store.clean()

		for _, el := range entries {
			wantErr := el.Exp.Before(now)

			_, err := store.Get(ctx, el.K)
			require.True(t, (err != nil) == wantErr)
			if wantErr {
				require.ErrorIs(t, err, errors.ErrNotFound)
			}
		}
	})
}

func TestCleanup(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		var (
			ctx = t.Context()

			dur = 10 * time.Millisecond
			now = lo.ToPtr(testutils.MustParseDate(t, "2025-01-01"))

			start = make(chan struct{})
			done  = make(chan struct{})
		)

		store := &noeviction[string]{
			store:  make(map[string]storage.Entry[string]),
			mu:     &sync.RWMutex{},
			start:  start,
			done:   done,
			clnDur: utils.Const(dur),
			curTime: func(_ context.Context) time.Time {
				return *now
			},
		}

		entry1 := storage.Entry[string]{
			K:   "key1",
			V:   "value",
			Exp: *now,
		}

		entry2 := storage.Entry[string]{
			K:   "key2",
			V:   "value",
			Exp: now.Add(dur),
		}

		entries := []storage.Entry[string]{
			entry1, entry2,
		}

		for _, el := range entries {
			_, err := store.Set(ctx, el)
			require.NoError(t, err)
		}

		go store.cleanup()
		start <- struct{}{}

		require.Eventually(t, func() bool {
			*now = now.Add(dur)

			for _, el := range entries {
				_, err := store.Get(ctx, el.K)
				if !stderrs.Is(err, errors.ErrNotFound) {
					return false
				}
			}

			return true
		},
			1*time.Second,
			10*time.Millisecond,
		)

		done <- struct{}{}
	})
}
