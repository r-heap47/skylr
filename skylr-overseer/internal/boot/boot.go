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
	"time"

	v1 "github.com/r-heap47/skylr/skylr-overseer/internal/api/grpc/v1"
	"github.com/r-heap47/skylr/skylr-overseer/internal/config"
	"github.com/r-heap47/skylr/skylr-overseer/internal/overseer"
	pbovr "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-overseer"
	"github.com/r-heap47/skylr/skylr-overseer/internal/pkg/utils"
	"github.com/r-heap47/skylr/skylr-overseer/internal/provisioner"
	"github.com/r-heap47/skylr/skylr-overseer/internal/provisioner/provisioners/process"
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
		VirtualNodesPerShard:       utils.Const(cfg.Overseer.VirtualNodesPerShard),
	})

	var prov provisioner.ShardProvisioner
	if cfg.Provisioner.Type == "process" {
		pc := cfg.Provisioner.Process
		if pc.BinaryPath == "" || pc.ConfigPath == "" || pc.OverseerAddress == "" {
			return fmt.Errorf("provisioner.process requires binary_path, config_path, overseer_address")
		}
		if pc.GRPCPortMin <= 0 || pc.GRPCPortMax <= pc.GRPCPortMin {
			return fmt.Errorf("provisioner.process requires grpc_port_min < grpc_port_max")
		}
		if pc.MaxShards <= 0 {
			return fmt.Errorf("provisioner.process requires max_shards > 0")
		}
		if pc.GRPCHost == "" {
			pc.GRPCHost = "localhost"
		}
		prov = process.New(process.Config{
			BinaryPath:            pc.BinaryPath,
			ConfigPath:            pc.ConfigPath,
			OverseerAddress:       pc.OverseerAddress,
			GRPCHost:              pc.GRPCHost,
			GRPCPortMin:           pc.GRPCPortMin,
			GRPCPortMax:           pc.GRPCPortMax,
			MaxShards:             pc.MaxShards,
			RegistrationTimeout:   pc.RegistrationTimeout.Duration,
			PostRegistrationDelay: pc.PostRegistrationDelay.Duration,
			ShardCount:            ovr.ShardCount,
			IsShardRegistered:     ovr.HasShard,
		})
		log.Printf("[INFO] process provisioner enabled: binary=%s max_shards=%d", pc.BinaryPath, pc.MaxShards)
	}

	impl := v1.New(&v1.Config{
		Ovr:         ovr,
		Provisioner: prov,
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

	// kill all provisioned shard processes (e.g. process provisioner subprocesses)
	if sh, ok := prov.(provisioner.Shutdowner); ok {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()

		_ = sh.Shutdown(shutdownCtx)
	}

	return nil
}
