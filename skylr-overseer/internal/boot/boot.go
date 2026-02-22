package boot

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	v1 "github.com/r-heap47/skylr/skylr-overseer/internal/api/grpc/v1"
	"github.com/r-heap47/skylr/skylr-overseer/internal/config"
	"github.com/r-heap47/skylr/skylr-overseer/internal/overseer"
	pbovr "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-overseer"
	"github.com/r-heap47/skylr/skylr-overseer/internal/pkg/utils"
	"google.golang.org/grpc"
)

var configPath = flag.String("config", "config/config.yaml", "Path to YAML config file")

// Run .
func Run() error {
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("config.Load: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcEndpoint := fmt.Sprintf("%s:%s", cfg.GRPC.Host, cfg.GRPC.Port)

	ovr := overseer.New(ctx, overseer.Config{
		CheckForShardFailuresDelay: utils.Const(cfg.Overseer.CheckForShardFailuresDelay.Duration),
		ObserverDelay:              utils.Const(cfg.Overseer.ObserverDelay.Duration),
		ObserverMetricsTimeout:     utils.Const(cfg.Overseer.ObserverMetricsTimeout.Duration),
		ObserverErrorThreshold:     utils.Const(cfg.Overseer.ObserverErrorThreshold),
	})

	impl := v1.New(&v1.Config{
		Ovr: ovr,
	})

	// === GRPC SERVER SETUP ===

	grpcServer := grpc.NewServer()
	pbovr.RegisterOverseerServer(grpcServer, impl)

	lis, err := net.Listen("tcp", grpcEndpoint)
	if err != nil {
		return fmt.Errorf("net.Listen: %w", err)
	}

	go func() {
		log.Printf("[GRPC] grpc server is set up on %s\n", grpcEndpoint)

		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpcServer.Serve: %s", err)
		}
	}()

	// === GRACEFUL SHUTDOWN ===

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	log.Println("[GRPC] shutting down grpc server...")
	grpcServer.GracefulStop()

	// cancel root context — stops checkForShardFailures and all observer goroutines
	cancel()

	return nil
}
