package boot

import (
	"context"
	"errors"
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
	"github.com/r-heap47/skylr/skylr-shard/internal/config"
	"github.com/r-heap47/skylr/skylr-shard/internal/metrics"
	pbovr "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-overseer"
	pbshard "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-shard"
	"github.com/r-heap47/skylr/skylr-shard/internal/shard"
	"github.com/r-heap47/skylr/skylr-shard/internal/storage/storages/noeviction"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	configPath   = flag.String("config", "config/config.yaml", "Path to YAML config file")
	grpcHost     = flag.String("grpc-host", "localhost", "gRPC server host")
	grpcPort     = flag.String("grpc-port", "", "gRPC server port")
	gatewayHost  = flag.String("gateway-host", "localhost", "HTTP gateway host")
	gatewayPort  = flag.String("gateway-port", "", "HTTP gateway port")
	overseerAddr = flag.String("overseer", "", "Overseer gRPC address (e.g. 127.0.0.1:9000)")
)

// Run starts the shard application: loads config, initialises storage, gRPC and HTTP servers.
func Run() error {
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("config.Load: %w", err)
	}

	if *grpcPort == "" {
		return fmt.Errorf("-grpc-port is required")
	}
	if *gatewayPort == "" {
		return fmt.Errorf("-gateway-port is required")
	}
	if *overseerAddr == "" {
		return fmt.Errorf("-overseer is required")
	}

	var (
		initCtx = context.Background()

		grpcEndpoint = fmt.Sprintf("%s:%s", *grpcHost, *grpcPort)
		gwEndpoint   = fmt.Sprintf("%s:%s", *gatewayHost, *gatewayPort)

		startCh = make(chan struct{})
	)

	curTime := func(_ context.Context) time.Time {
		return time.Now()
	}

	cleanupTimeout := func(_ context.Context) time.Duration {
		return cfg.Storage.CleanupTimeout.Duration
	}

	cleanupCooldown := func(_ context.Context) time.Duration {
		return cfg.Storage.CleanupCooldown.Duration
	}

	storage := noeviction.New(noeviction.Config{
		CurTime:         curTime,
		CleanupTimeout:  cleanupTimeout,
		CleanupCooldown: cleanupCooldown,
		Start:           startCh,
	})

	shardSvc := shard.New(shard.Config{
		Storage: storage,
		CurTime: curTime,
	})

	collector := metrics.NewCollector(time.Now())

	impl := grpcV1.New(grpcV1.Config{
		Shard:     shardSvc,
		Collector: collector,
	})

	// === GRPC SERVER SETUP ===

	grpcServer := grpc.NewServer()

	// errCh collects fatal errors from server goroutines; buffered by the number
	// of goroutines that write to it so that a late writer never blocks.
	errCh := make(chan error, 2)

	// TODO: properly configure network interface -- rm nolint
	lis, err := net.Listen("tcp", grpcEndpoint) // nolint: gosec
	if err != nil {
		return fmt.Errorf("net.Listen: %w", err)
	}

	go func() {
		pbshard.RegisterShardServer(grpcServer, impl)

		log.Printf("[GRPC] grpc server is set up on %s\n", grpcEndpoint)
		startCh <- struct{}{} // initializing storages within shard

		if err := grpcServer.Serve(lis); err != nil {
			errCh <- fmt.Errorf("grpcServer.Serve: %w", err)
		}
	}()

	// === GRPC-GATEWAY (HTTP) SERVER SETUP ===

	gwMux := runtime.NewServeMux()

	// nolint: gosec
	httpServer := &http.Server{
		Addr:         gwEndpoint,
		Handler:      gwMux,
		ReadTimeout:  cfg.Gateway.ReadTimeout.Duration,
		WriteTimeout: cfg.Gateway.WriteTimeout.Duration,
		IdleTimeout:  cfg.Gateway.IdleTimeout.Duration,
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

		// nolint: gosec
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("httpServer.ListenAndServe: %w", err)
		}
	}()

	// === DIALING OVERSEER ===

	ovrConn, err := grpc.NewClient(
		*overseerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("[GRPC] couldn't connect to Overseer: %w", err)
	}

	ovrClient := pbovr.NewOverseerClient(ovrConn)

	_, err = ovrClient.Register(initCtx, &pbovr.RegisterRequest{
		Address: grpcEndpoint,
	})
	if err != nil {
		return fmt.Errorf("[GRPC] couldn't register on Overseer: %w", err)
	}

	// === GRACEFUL SHUTDOWN ===

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		log.Printf("[GRPC] received signal %s, shutting down...\n", sig)
	case err := <-errCh:
		log.Printf("[GRPC] fatal server error: %s, shutting down...\n", err)
	}

	log.Println("[GRPC] shutting down grpc server...")
	if cfg.Graceful {
		grpcServer.GracefulStop()
	} else {
		grpcServer.Stop()
	}

	log.Println("[GRPC] shutting down grpc-gateway server...")
	shutdownCtx, cancel := context.WithTimeout(initCtx, cfg.Gateway.ShutdownTimeout.Duration)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("httpServer.Shutdown: %w", err)
	}

	log.Println("[GRPC] shutdown success")

	return nil
}
