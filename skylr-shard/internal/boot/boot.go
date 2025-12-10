package boot

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cutlery47/skylr/skylr-shard/internal/app"
	"github.com/cutlery47/skylr/skylr-shard/internal/shard"
	"github.com/cutlery47/skylr/skylr-shard/internal/storage/storages/noeviction"
	pbshard "github.com/cutlery47/skylr/skylr-shard/pkg/pb/skylr-shard"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// nolint: revive
func Run() error {
	var (
		initCtx = context.Background()

		grpcEndpoint = "0.0.0.0:8000"
		gwEndpoint   = "0.0.0.0:8001"

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
	})

	storageInt64 := noeviction.New[int64](noeviction.Config{
		CurTime:        curTime,
		CleanupTimeout: cleanupTimeout,
	})

	storageInt32 := noeviction.New[int32](noeviction.Config{
		CurTime:        curTime,
		CleanupTimeout: cleanupTimeout,
	})

	storageFloat64 := noeviction.New[float64](noeviction.Config{
		CurTime:        curTime,
		CleanupTimeout: cleanupTimeout,
	})

	storageFloat32 := noeviction.New[float32](noeviction.Config{
		CurTime:        curTime,
		CleanupTimeout: cleanupTimeout,
	})

	shard := shard.New(shard.Config{
		StorageStr:     storageStr,
		StorageInt64:   storageInt64,
		StorageInt32:   storageInt32,
		StorageFloat64: storageFloat64,
		StorageFloat32: storageFloat32,

		CurTime:         curTime,
		CleanupTimeout:  cleanupTimeout,
		CleanupCooldown: cleanupTimeout,

		Start: startCh,
	})

	impl := app.New(app.Config{
		Shard: shard,
	})

	// === GRPC SERVER SETUP ===

	grpcServer := grpc.NewServer()

	lis, err := net.Listen("tcp", grpcEndpoint)
	if err != nil {
		return fmt.Errorf("net.Listen: %w", err)
	}

	go func() {
		pbshard.RegisterShardServer(grpcServer, impl)

		log.Printf("[GRPC] grpc server is set up on %s\n", grpcEndpoint)
		startCh <- struct{}{} // initializing storages within shard

		err := grpcServer.Serve(lis)
		if err != nil {
			log.Fatalf("grpcServer.Serve: %s", err)
		}
	}()

	// === GRPC-GATEWAY (HTTP) SERVER SETUP

	gwMux := runtime.NewServeMux()

	httpServer := &http.Server{
		Addr:    gwEndpoint,
		Handler: gwMux,
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	err = pbshard.RegisterShardHandlerFromEndpoint(initCtx, gwMux, grpcEndpoint, opts)
	if err != nil {
		return fmt.Errorf("pbshard.RegisterShardHandlerFromEndpoint: %w", err)
	}

	go func() {
		log.Printf("[GRPC] grpc-gateway server is set up on %s\n", gwEndpoint)

		err := http.ListenAndServe(gwEndpoint, httpServer.Handler)
		if err != nil {
			log.Fatalf("http.ListenAndServe: %s", err)
		}
	}()

	// === GRACEFUL SHUTDOWN ===

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	log.Println("[GRPC] shutting down grpc server...")
	grpcServer.GracefulStop()

	log.Println("[GRPC] shutting down grpc-gateway server...")
	shutdownCtx, cancel := context.WithTimeout(initCtx, 5*time.Second)
	defer cancel()

	err = httpServer.Shutdown(shutdownCtx)
	if err != nil {
		log.Fatalf("httpServer.Shutdown: %s", err)
	}

	log.Println("[GRPC] shutdown success")

	return nil
}
