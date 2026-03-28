package kubernetes

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Config holds configuration for the Kubernetes provisioner.
type Config struct {
	// Client is the Kubernetes clientset used to manage pods.
	Client kubernetes.Interface
	// Namespace is the Kubernetes namespace where shard pods are created.
	Namespace string
	// Image is the container image for shard pods.
	Image string
	// OverseerAddress is the overseer gRPC address reachable from within the cluster.
	OverseerAddress string
	// GRPCPort is the gRPC port used inside each shard container.
	GRPCPort int
	// GatewayPort is the HTTP gateway port used inside each shard container.
	GatewayPort int
	// MaxShards is the maximum number of shard pods that can exist at once.
	MaxShards int
	// ShardCount returns the current number of registered shards.
	ShardCount func() int
	// IsShardRegistered returns true when the shard at addr has registered with the overseer.
	IsShardRegistered func(addr string) bool
	// RegistrationPollInterval is how often to poll for shard registration (default: 200ms).
	RegistrationPollInterval time.Duration
	// RegistrationTimeout is the maximum time to wait for a shard to register (default: 60s).
	RegistrationTimeout time.Duration
	// PostRegistrationDelay is an optional pause after registration before returning.
	PostRegistrationDelay time.Duration
	// Resources defines optional resource requests/limits for shard pods.
	Resources ResourcesConfig
}

// ResourcesConfig holds parsed Kubernetes resource requests and limits.
type ResourcesConfig struct {
	CPURequest    *resource.Quantity
	CPULimit      *resource.Quantity
	MemoryRequest *resource.Quantity
	MemoryLimit   *resource.Quantity
}

// Provisioner provisions shards as Kubernetes Pods.
type Provisioner struct {
	cfg  Config
	mu   sync.Mutex
	pods map[string]string // addr -> podName
}

// New creates a new Kubernetes provisioner and applies defaults for optional fields.
func New(cfg Config) *Provisioner {
	if cfg.RegistrationPollInterval <= 0 {
		cfg.RegistrationPollInterval = 200 * time.Millisecond
	}
	if cfg.RegistrationTimeout <= 0 {
		cfg.RegistrationTimeout = 60 * time.Second
	}
	if cfg.PostRegistrationDelay < 0 {
		cfg.PostRegistrationDelay = 0
	}

	return &Provisioner{
		cfg:  cfg,
		pods: make(map[string]string),
	}
}

// NewClientset builds a Kubernetes clientset.
// If kubeconfig is non-empty it loads that file; otherwise it tries in-cluster config,
// then falls back to the default kubeconfig location (~/.kube/config).
func NewClientset(kubeconfig string) (kubernetes.Interface, error) {
	var restCfg *rest.Config
	var err error

	if kubeconfig != "" {
		restCfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("build config from kubeconfig %q: %w", kubeconfig, err)
		}
	} else {
		restCfg, err = rest.InClusterConfig()
		if err != nil {
			// Fall back to default kubeconfig location.
			restCfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
			if err != nil {
				return nil, fmt.Errorf("build config (tried in-cluster and %s): %w",
					clientcmd.RecommendedHomeFile, err)
			}
		}
	}

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes.NewForConfig: %w", err)
	}

	return client, nil
}

// Provision creates a new shard Pod and returns its address once registered.
func (p *Provisioner) Provision(ctx context.Context) (string, error) {
	// Check max-shards limit before creating the pod.
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cfg.ShardCount != nil && p.cfg.ShardCount() >= p.cfg.MaxShards {
		return "", fmt.Errorf("max shards (%d) reached", p.cfg.MaxShards)
	}

	podName, err := generatePodName()
	if err != nil {
		return "", fmt.Errorf("generate pod name: %w", err)
	}
	pod := p.buildPod(podName)

	_, err = p.cfg.Client.CoreV1().Pods(p.cfg.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("create pod %q: %w", podName, err)
	}

	// Ensure the pod is deleted if anything goes wrong after creation.
	var addr string
	cleanup := func() {
		deleteCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = p.cfg.Client.CoreV1().Pods(p.cfg.Namespace).Delete(deleteCtx, podName, metav1.DeleteOptions{})
	}

	// Wait for the pod to be running and have an IP.
	podIP, err := p.waitForPodReady(ctx, podName)
	if err != nil {
		cleanup()
		return "", fmt.Errorf("wait for pod %q ready: %w", podName, err)
	}
	addr = net.JoinHostPort(podIP, strconv.Itoa(p.cfg.GRPCPort))

	// Track addr → podName so Deprovision can find the pod.
	p.mu.Lock()
	p.pods[addr] = podName
	p.mu.Unlock()

	// Wait for the shard to register with the overseer.
	if err := p.waitForRegistration(ctx, addr); err != nil {
		p.mu.Lock()
		delete(p.pods, addr)
		p.mu.Unlock()
		cleanup()
		return "", err
	}

	return addr, nil
}

// Deprovision deletes the shard Pod associated with addr.
func (p *Provisioner) Deprovision(ctx context.Context, addr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	podName, ok := p.pods[addr]
	if !ok {
		return fmt.Errorf("shard %q not found", addr)
	}
	delete(p.pods, addr)

	err := p.cfg.Client.CoreV1().Pods(p.cfg.Namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete pod %q: %w", podName, err)
	}

	return nil
}

// Shutdown deletes all provisioned shard pods. Call when the overseer exits.
func (p *Provisioner) Shutdown(ctx context.Context) error {
	for _, addr := range p.addrs() {
		_ = p.Deprovision(ctx, addr)
	}

	return nil
}

func (p *Provisioner) addrs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	addrs := make([]string, 0, len(p.pods))
	for addr := range p.pods {
		addrs = append(addrs, addr)
	}

	return addrs
}

// buildPod constructs the Pod spec for a shard.
func (p *Provisioner) buildPod(name string) *corev1.Pod {
	grpcPort := strconv.Itoa(p.cfg.GRPCPort)
	gatewayPort := strconv.Itoa(p.cfg.GatewayPort)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: p.cfg.Namespace,
			Labels: map[string]string{
				"app":        "skylr-shard",
				"managed-by": "skylr-overseer",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:  "shard",
					Image: p.cfg.Image,
					// POD_IP is injected via Downward API and expanded in Args.
					// The shard binds on its own PodIP and registers that address
					// with the overseer — avoiding the 0.0.0.0 ambiguity.
					Env: []corev1.EnvVar{
						{
							Name: "POD_IP",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: "status.podIP",
								},
							},
						},
					},
					Args: []string{
						"-config", "/app/config/config.yaml",
						"-grpc-host", "$(POD_IP)",
						"-grpc-port", grpcPort,
						"-gateway-host", "$(POD_IP)",
						"-gateway-port", gatewayPort,
						"-overseer", p.cfg.OverseerAddress,
					},
					Ports: []corev1.ContainerPort{
						{Name: "grpc", ContainerPort: int32(p.cfg.GRPCPort), Protocol: corev1.ProtocolTCP},       //nolint:gosec // port numbers fit in int32
						{Name: "gateway", ContainerPort: int32(p.cfg.GatewayPort), Protocol: corev1.ProtocolTCP}, //nolint:gosec // port numbers fit in int32
					},
					Resources: p.buildResourceRequirements(),
				},
			},
		},
	}
	return pod
}

// buildResourceRequirements constructs corev1.ResourceRequirements from config.
// Fields with nil Quantity are omitted.
func (p *Provisioner) buildResourceRequirements() corev1.ResourceRequirements {
	req := corev1.ResourceRequirements{}

	if p.cfg.Resources.CPURequest != nil || p.cfg.Resources.MemoryRequest != nil {
		req.Requests = corev1.ResourceList{}
		if p.cfg.Resources.CPURequest != nil {
			req.Requests[corev1.ResourceCPU] = *p.cfg.Resources.CPURequest
		}
		if p.cfg.Resources.MemoryRequest != nil {
			req.Requests[corev1.ResourceMemory] = *p.cfg.Resources.MemoryRequest
		}
	}

	if p.cfg.Resources.CPULimit != nil || p.cfg.Resources.MemoryLimit != nil {
		req.Limits = corev1.ResourceList{}
		if p.cfg.Resources.CPULimit != nil {
			req.Limits[corev1.ResourceCPU] = *p.cfg.Resources.CPULimit
		}
		if p.cfg.Resources.MemoryLimit != nil {
			req.Limits[corev1.ResourceMemory] = *p.cfg.Resources.MemoryLimit
		}
	}

	return req
}

// waitForPodReady polls until the pod has a non-empty PodIP and its container is running.
func (p *Provisioner) waitForPodReady(ctx context.Context, podName string) (string, error) {
	deadline := time.NewTimer(p.cfg.RegistrationTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(p.cfg.RegistrationPollInterval)
	defer ticker.Stop()

	for {
		pod, err := p.cfg.Client.CoreV1().Pods(p.cfg.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("get pod: %w", err)
		}
		if pod.Status.PodIP != "" && containerIsRunning(pod) {
			return pod.Status.PodIP, nil
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return "", fmt.Errorf("pod %q entered terminal phase %s", podName, pod.Status.Phase)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
			return "", fmt.Errorf("pod %q did not become ready within %v", podName, p.cfg.RegistrationTimeout)
		case <-ticker.C:
		}
	}
}

// containerIsRunning returns true when the shard container has started running.
func containerIsRunning(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == "shard" {
			return cs.State.Running != nil
		}
	}

	return false
}

// waitForRegistration polls until the shard at addr registers with the overseer.
func (p *Provisioner) waitForRegistration(ctx context.Context, addr string) error {
	deadline := time.NewTimer(p.cfg.RegistrationTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(p.cfg.RegistrationPollInterval)
	defer ticker.Stop()

	for {
		if p.cfg.IsShardRegistered(addr) {
			log.Printf("[INFO] provisioner: shard %s registered", addr)
			if p.cfg.PostRegistrationDelay > 0 {
				if err := sleep(ctx, p.cfg.PostRegistrationDelay); err != nil {
					return err
				}
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("shard %s did not register within %v", addr, p.cfg.RegistrationTimeout)
		case <-ticker.C:
		}
	}
}

// sleep waits for d or until ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// generatePodName returns a unique shard pod name like "skylr-shard-a3f9c12b".
func generatePodName() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "skylr-shard-" + hex.EncodeToString(b), nil
}
