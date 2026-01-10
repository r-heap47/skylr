package boot

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	grpcV1 "github.com/r-heap47/skylr/skylr-shard/internal/api/grpc/v1"
	"github.com/r-heap47/skylr/skylr-shard/internal/metrics"
	pbovr "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-overseer"
	pbshard "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-shard"
	"github.com/r-heap47/skylr/skylr-shard/internal/shard"
	"github.com/r-heap47/skylr/skylr-shard/internal/storage/storages/noeviction"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	grpcPort = flag.String("grpc-port", "5000", "Port for grpc-server to be run on (default: 5000)")
	gwPort   = flag.String("grpc-gw-port", "5001", "Port for grpc-gateway-server to be run on (default: 5001)")
	graceful = flag.Bool("graceful", true, "Bool flag for switching graceful shutdown (default: true)")
)

// nolint: revive
// TODO: proper configuration
func Run() error {
	var (
		initCtx = context.Background()

		grpcHost = "localhost"
		gwHost   = "localhost"

		grpcEndpoint string
		gwEndpoint   string

		startCh = make(chan struct{})
	)

	// parsing cmd flags
	flag.Parse()

	grpcEndpoint = fmt.Sprintf("%s:%s", grpcHost, *grpcPort)
	gwEndpoint = fmt.Sprintf("%s:%s", gwHost, *gwPort)

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

	collector := &metrics.Collector{
		StorageStr:     storageStr,
		StorageInt64:   storageInt64,
		StorageInt32:   storageInt32,
		StorageFloat64: storageFloat64,
		StorageFloat32: storageFloat32,
	}

	impl := grpcV1.New(grpcV1.Config{
		Shard:        shard,
		Collector:    collector,
		MetricsDelay: func(_ context.Context) time.Duration { return time.Second }, // TODO: proper configuration
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

	// === DIALING OVERSEER ===

	ovrConn, err := grpc.NewClient(
		"127.0.0.1:9000",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("[GRPC] couldn't connect to Overseer: %w", err)
	}

	ovrClient := pbovr.NewOverseerClient(ovrConn)

	// trying to register on overseer right away
	_, err = ovrClient.Register(initCtx, &pbovr.RegisterRequest{
		Address: grpcEndpoint,
	})
	if err != nil {
		return fmt.Errorf("[GRPC] couldn't register on Overseer: %w", err)
	}

	// === GRACEFUL SHUTDOWN ===

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	log.Println("[GRPC] shutting down grpc server...")
	if *graceful {
		grpcServer.GracefulStop()
	} else {
		grpcServer.Stop()
	}

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
