package boot

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cutlery47/skylr/skylr-shard/internal/app"
	"github.com/cutlery47/skylr/skylr-shard/internal/shard"
	"github.com/cutlery47/skylr/skylr-shard/internal/storage/storages/noeviction"
	pbshard "github.com/cutlery47/skylr/skylr-shard/pkg/pb/skylr-shard"
	"google.golang.org/grpc"
)

// nolint: revive
func Run() error {
	var (
		startCh = make(chan struct{})
	)

	curTime := func(ctx context.Context) time.Time {
		return time.Now()
	}

	cleanupTimeout := func(ctx context.Context) time.Duration {
		return 5 * time.Second
	}

	storageStr := noeviction.New[string](noeviction.Config{
		CurTime:        curTime,
		CleanupTimeout: cleanupTimeout,
		Start:          startCh,
	})

	storageInt64 := noeviction.New[int64](noeviction.Config{
		CurTime:        curTime,
		CleanupTimeout: cleanupTimeout,
		Start:          startCh,
	})

	storageInt32 := noeviction.New[int32](noeviction.Config{
		CurTime:        curTime,
		CleanupTimeout: cleanupTimeout,
		Start:          startCh,
	})

	storageFloat64 := noeviction.New[float64](noeviction.Config{
		CurTime:        curTime,
		CleanupTimeout: cleanupTimeout,
		Start:          startCh,
	})

	storageFloat32 := noeviction.New[float32](noeviction.Config{
		CurTime:        curTime,
		CleanupTimeout: cleanupTimeout,
		Start:          startCh,
	})

	shard := shard.New(shard.Config{
		StorageStr:     storageStr,
		StorageInt64:   storageInt64,
		StorageInt32:   storageInt32,
		StorageFloat64: storageFloat64,
		StorageFloat32: storageFloat32,

		CurTime: curTime,
	})

	impl := app.New(app.Config{
		Shard: shard,
	})

	lis, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return fmt.Errorf("net.Listen: %w", err)
	}

	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		return fmt.Errorf("net.SplitHostPort: %w", err)
	}

	srv := grpc.NewServer()

	go func() {
		log.Printf("[GRPC] grpc server is set up on port %s...\n", port)

		pbshard.RegisterShardServer(srv, impl)
		startCh <- struct{}{} // initializing storages within shard

		err := srv.Serve(lis)
		if err != nil {
			log.Fatalf("[GRPC] grpc server error: %s", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	log.Println("[GRPC] grpc server shutdown...")
	srv.GracefulStop()

	return nil
}
