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

	"golang.org/x/sync/errgroup"

	v1 "github.com/r-heap47/skylr/skylr-overseer/internal/api/grpc/v1"
	ascaler "github.com/r-heap47/skylr/skylr-overseer/internal/autoscaler"
	"github.com/r-heap47/skylr/skylr-overseer/internal/config"
	"github.com/r-heap47/skylr/skylr-overseer/internal/overseer"
	pbovr "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-overseer"
	"github.com/r-heap47/skylr/skylr-overseer/internal/pkg/utils"
	"github.com/r-heap47/skylr/skylr-overseer/internal/provisioner"
	k8sprov "github.com/r-heap47/skylr/skylr-overseer/internal/provisioner/provisioners/kubernetes"
	"github.com/r-heap47/skylr/skylr-overseer/internal/provisioner/provisioners/process"
	"github.com/r-heap47/skylr/skylr-overseer/internal/reprovisioner"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
		LogStorageOnMetrics:        utils.Const(cfg.Overseer.LogStorageOnMetrics),
	})

	var prov provisioner.ShardProvisioner
	switch cfg.Provisioner.Type {
	case "process":
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
		if pc.InitialShards < 0 || pc.InitialShards > pc.MaxShards {
			return fmt.Errorf("provisioner.process requires 0 <= initial_shards <= max_shards, got initial_shards=%d max_shards=%d", pc.InitialShards, pc.MaxShards)
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
	case "kubernetes":
		kc := cfg.Provisioner.Kubernetes
		if kc.Image == "" || kc.OverseerAddress == "" || kc.Namespace == "" {
			return fmt.Errorf("provisioner.kubernetes requires image, overseer_address, namespace")
		}
		if kc.MaxShards <= 0 {
			return fmt.Errorf("provisioner.kubernetes requires max_shards > 0")
		}
		if kc.InitialShards < 0 || kc.InitialShards > kc.MaxShards {
			return fmt.Errorf("provisioner.kubernetes requires 0 <= initial_shards <= max_shards, got initial_shards=%d max_shards=%d", kc.InitialShards, kc.MaxShards)
		}

		client, err := k8sprov.NewClientset(kc.Kubeconfig)
		if err != nil {
			return fmt.Errorf("kubernetes clientset: %w", err)
		}

		resources, err := parseResourceCfg(kc.Resources)
		if err != nil {
			return fmt.Errorf("provisioner.kubernetes resources: %w", err)
		}

		prov = k8sprov.New(k8sprov.Config{
			Client:                client,
			Namespace:             kc.Namespace,
			Image:                 kc.Image,
			OverseerAddress:       kc.OverseerAddress,
			GRPCPort:              kc.GRPCPort,
			GatewayPort:           kc.GatewayPort,
			MaxShards:             kc.MaxShards,
			RegistrationTimeout:   kc.RegistrationTimeout.Duration,
			PostRegistrationDelay: kc.PostRegistrationDelay.Duration,
			ShardCount:            ovr.ShardCount,
			IsShardRegistered:     ovr.HasShard,
			Resources:             resources,
			ImagePullPolicy:       corev1.PullPolicy(kc.ImagePullPolicy),
		})
		log.Printf("[INFO] kubernetes provisioner enabled: image=%s namespace=%s max_shards=%d", kc.Image, kc.Namespace, kc.MaxShards)
	}

	// === AUTOSCALER ===

	if prov != nil && cfg.Autoscaler.Enabled {
		ac := cfg.Autoscaler

		var rules []ascaler.ScalingRule
		if ac.Rules.ItemCount.Enabled {
			rules = append(rules, ascaler.ItemCountRule{Threshold: ac.Rules.ItemCount.Threshold})
		}

		sustainedFor := ac.SustainedFor
		if sustainedFor <= 0 {
			sustainedFor = 1
		}

		as := ascaler.New(prov, ascaler.Config{
			EvalInterval:   ac.EvalInterval.Duration,
			Cooldown:       ac.Cooldown.Duration,
			SustainedFor:   sustainedFor,
			Rules:          rules,
			CollectMetrics: ovr.CollectAggregatedMetrics,
		})

		go as.Run(ctx)
		log.Printf("[INFO] autoscaler enabled: eval_interval=%s cooldown=%s sustained_for=%d",
			ac.EvalInterval.Duration, ac.Cooldown.Duration, sustainedFor)
	}

	// === REPROVISIONER ===

	if prov != nil && cfg.Reprovisioner.Enabled {
		rc := cfg.Reprovisioner

		initialDelay := rc.InitialRetryDelay.Duration
		if initialDelay <= 0 {
			initialDelay = 1 * time.Second
		}
		maxDelay := rc.MaxRetryDelay.Duration
		if maxDelay <= 0 {
			maxDelay = 30 * time.Second
		}

		rp := reprovisioner.New(prov, reprovisioner.Config{
			Failures:          ovr.FailureNotifications(),
			MaxRetries:        rc.MaxRetries,
			InitialRetryDelay: initialDelay,
			MaxRetryDelay:     maxDelay,
		})

		go rp.Run(ctx)
		log.Printf("[INFO] reprovisioner enabled: max_retries=%d initial_delay=%s max_delay=%s",
			rc.MaxRetries, initialDelay, maxDelay)
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

	serveReady := make(chan struct{})
	go func() {
		close(serveReady)
		log.Printf("[GRPC] grpc server is set up on %s\n", grpcEndpoint)

		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpcServer.Serve: %s", err)
		}
	}()

	// === INITIAL SHARDS (after gRPC is listening so shards can register) ===

	initialShards := 0
	switch cfg.Provisioner.Type {
	case "process":
		initialShards = cfg.Provisioner.Process.InitialShards
	case "kubernetes":
		initialShards = cfg.Provisioner.Kubernetes.InitialShards
	}

	if prov != nil && initialShards > 0 {
		<-serveReady

		g, gCtx := errgroup.WithContext(ctx)
		for i := 0; i < initialShards; i++ {
			g.Go(func() error {
				_, err := prov.Provision(gCtx)
				return err
			})
		}

		if err := g.Wait(); err != nil {
			return fmt.Errorf("initial shard provisioning: %w", err)
		}
		log.Printf("[INFO] provisioned %d initial shards", initialShards)
	}

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

func parseResourceCfg(r config.PodResourcesCfg) (k8sprov.ResourcesConfig, error) {
	parse := func(s string) (*resource.Quantity, error) {
		if s == "" {
			return nil, nil
		}
		q, err := resource.ParseQuantity(s)
		if err != nil {
			return nil, err
		}
		return &q, nil
	}

	cpuReq, err := parse(r.CPURequest)
	if err != nil {
		return k8sprov.ResourcesConfig{}, fmt.Errorf("cpu_request %q: %w", r.CPURequest, err)
	}
	cpuLim, err := parse(r.CPULimit)
	if err != nil {
		return k8sprov.ResourcesConfig{}, fmt.Errorf("cpu_limit %q: %w", r.CPULimit, err)
	}
	memReq, err := parse(r.MemoryRequest)
	if err != nil {
		return k8sprov.ResourcesConfig{}, fmt.Errorf("memory_request %q: %w", r.MemoryRequest, err)
	}
	memLim, err := parse(r.MemoryLimit)
	if err != nil {
		return k8sprov.ResourcesConfig{}, fmt.Errorf("memory_limit %q: %w", r.MemoryLimit, err)
	}

	return k8sprov.ResourcesConfig{
		CPURequest:    cpuReq,
		CPULimit:      cpuLim,
		MemoryRequest: memReq,
		MemoryLimit:   memLim,
	}, nil
}
