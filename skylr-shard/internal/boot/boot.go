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

	"github.com/cutlery47/skylr/skylr-shard/internal/api"
	pbshard "github.com/cutlery47/skylr/skylr-shard/internal/pb/skylr-shard"
	"github.com/cutlery47/skylr/skylr-shard/internal/shard"
	"github.com/cutlery47/skylr/skylr-shard/internal/storage/storages/noeviction"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// nolint: revive
// TODO: proper configuration
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

	cleanupCooldown := func(ctx context.Context) time.Duration {
		return 5 * time.Second
	}

	storageStr := noeviction.New[string](noeviction.Config{
		CurTime: curTime,
	})

	storageInt64 := noeviction.New[int64](noeviction.Config{
		CurTime: curTime,
	})

	storageInt32 := noeviction.New[int32](noeviction.Config{
		CurTime: curTime,
	})

	storageFloat64 := noeviction.New[float64](noeviction.Config{
		CurTime: curTime,
	})

	storageFloat32 := noeviction.New[float32](noeviction.Config{
		CurTime: curTime,
	})

	shard := shard.New(shard.Config{
		StorageStr:     storageStr,
		StorageInt64:   storageInt64,
		StorageInt32:   storageInt32,
		StorageFloat64: storageFloat64,
		StorageFloat32: storageFloat32,

		CurTime:         curTime,
		CleanupTimeout:  cleanupTimeout,
		CleanupCooldown: cleanupCooldown,

		Start: startCh,
	})

	impl := api.New(api.Config{
		Shard: shard,
	})

	// === GRPC SERVER SETUP ===

	grpcServer := grpc.NewServer()

	// TODO: properly configure network interface -- rm nolint
	lis, err := net.Listen("tcp", grpcEndpoint) // nolint: gosec
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

	// TODO: add timeouts to prevent Slowloris Attack -- rm nolint
	// nolint: gosec
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

		// TODO: move from ListenAndServe to support server timeouts -- rm nolint
		// nolint: gosec
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
