package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	testNS          = "test-ns"
	testImage       = "skylr-shard:test"
	testOverseer    = "overseer:50051"
	testGRPCPort    = 9000
	testGatewayPort = 8080
)

// fastConfig returns a Config with millisecond-scale timeouts for fast tests.
func fastConfig(client kubernetes.Interface) Config {
	return Config{
		Client:                   client,
		Namespace:                testNS,
		Image:                    testImage,
		OverseerAddress:          testOverseer,
		GRPCPort:                 testGRPCPort,
		GatewayPort:              testGatewayPort,
		MaxShards:                5,
		ShardCount:               func() int { return 0 },
		IsShardRegistered:        func(string) bool { return true },
		RegistrationPollInterval: time.Millisecond,
		RegistrationTimeout:      200 * time.Millisecond,
	}
}

// makeRunningPod transitions a pod to Running state with the given IP.
// The pod must already exist in the fake client (created by Provision).
func makeRunningPod(t *testing.T, client kubernetes.Interface, ns, podName, podIP string) {
	t.Helper()

	pod, err := client.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{})
	require.NoError(t, err)

	pod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		PodIP: podIP,
		ContainerStatuses: []corev1.ContainerStatus{
			{Name: "shard", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
		},
	}
	_, err = client.CoreV1().Pods(ns).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{})
	require.NoError(t, err)
}

// makeTerminalPod transitions a pod to the given terminal phase (Failed or Succeeded).
func makeTerminalPod(t *testing.T, client kubernetes.Interface, ns, podName string, phase corev1.PodPhase) {
	t.Helper()

	pod, err := client.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{})
	require.NoError(t, err)

	pod.Status = corev1.PodStatus{Phase: phase}
	_, err = client.CoreV1().Pods(ns).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{})
	require.NoError(t, err)
}

// firstPodName returns the name of the first pod in the namespace, polling until one appears.
func firstPodName(t *testing.T, client kubernetes.Interface, ns string) string {
	t.Helper()

	var name string
	require.Eventually(t, func() bool {
		list, err := client.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
		if err != nil || len(list.Items) == 0 {
			return false
		}
		name = list.Items[0].Name
		return true
	}, 500*time.Millisecond, time.Millisecond, "pod should be created")

	return name
}

// podExists returns true if the pod currently exists in the fake client.
func podExists(client kubernetes.Interface, ns, podName string) bool {
	_, err := client.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{})
	return err == nil
}

// =============================================================================
// New — defaults
// =============================================================================

func TestNew_DefaultsApplied(t *testing.T) {
	t.Parallel()

	p := New(Config{
		RegistrationPollInterval: 0,
		RegistrationTimeout:      0,
		PostRegistrationDelay:    -1,
	})

	assert.Equal(t, 200*time.Millisecond, p.cfg.RegistrationPollInterval)
	assert.Equal(t, 60*time.Second, p.cfg.RegistrationTimeout)
	assert.Equal(t, time.Duration(0), p.cfg.PostRegistrationDelay)
}

func TestNew_CustomValuesPreserved(t *testing.T) {
	t.Parallel()

	p := New(Config{
		RegistrationPollInterval: 5 * time.Second,
		RegistrationTimeout:      10 * time.Second,
		PostRegistrationDelay:    3 * time.Second,
	})

	assert.Equal(t, 5*time.Second, p.cfg.RegistrationPollInterval)
	assert.Equal(t, 10*time.Second, p.cfg.RegistrationTimeout)
	assert.Equal(t, 3*time.Second, p.cfg.PostRegistrationDelay)
}

// =============================================================================
// generatePodName
// =============================================================================

func TestGeneratePodName_Format(t *testing.T) {
	t.Parallel()

	for range 10 {
		name, err := generatePodName()
		require.NoError(t, err)
		assert.Regexp(t, `^skylr-shard-[0-9a-f]{8}$`, name)
	}
}

func TestGeneratePodName_Uniqueness(t *testing.T) {
	t.Parallel()

	a, err := generatePodName()
	require.NoError(t, err)
	b, err := generatePodName()
	require.NoError(t, err)
	assert.NotEqual(t, a, b)
}

// =============================================================================
// buildPod
// =============================================================================

func TestBuildPod_Metadata(t *testing.T) {
	t.Parallel()

	p := New(fastConfig(fake.NewSimpleClientset()))
	pod := p.buildPod("skylr-shard-aabbccdd")

	assert.Equal(t, "skylr-shard-aabbccdd", pod.Name)
	assert.Equal(t, testNS, pod.Namespace)
	assert.Equal(t, "skylr-shard", pod.Labels["app"])
	assert.Equal(t, "skylr-overseer", pod.Labels["managed-by"])
}

func TestBuildPod_ContainerSpec(t *testing.T) {
	t.Parallel()

	p := New(fastConfig(fake.NewSimpleClientset()))
	pod := p.buildPod("skylr-shard-aabbccdd")

	require.Len(t, pod.Spec.Containers, 1)
	c := pod.Spec.Containers[0]

	assert.Equal(t, "shard", c.Name)
	assert.Equal(t, testImage, c.Image)
	assert.Equal(t, corev1.RestartPolicyNever, pod.Spec.RestartPolicy)

	// Args contain correct port and overseer values.
	assert.Contains(t, c.Args, fmt.Sprintf("%d", testGRPCPort))
	assert.Contains(t, c.Args, fmt.Sprintf("%d", testGatewayPort))
	assert.Contains(t, c.Args, testOverseer)

	// POD_IP injected via Downward API.
	require.Len(t, c.Env, 1)
	env := c.Env[0]
	assert.Equal(t, "POD_IP", env.Name)
	require.NotNil(t, env.ValueFrom)
	require.NotNil(t, env.ValueFrom.FieldRef)
	assert.Equal(t, "status.podIP", env.ValueFrom.FieldRef.FieldPath)

	// Ports declared.
	assert.Len(t, c.Ports, 2)
}

func TestBuildPod_Resources_AllSet(t *testing.T) {
	t.Parallel()

	cpuReq := resource.MustParse("100m")
	cpuLim := resource.MustParse("500m")
	memReq := resource.MustParse("64Mi")
	memLim := resource.MustParse("256Mi")

	cfg := fastConfig(fake.NewSimpleClientset())
	cfg.Resources = ResourcesConfig{
		CPURequest:    &cpuReq,
		CPULimit:      &cpuLim,
		MemoryRequest: &memReq,
		MemoryLimit:   &memLim,
	}
	p := New(cfg)
	pod := p.buildPod("skylr-shard-aabbccdd")

	res := pod.Spec.Containers[0].Resources
	assert.Equal(t, cpuReq, res.Requests[corev1.ResourceCPU])
	assert.Equal(t, memReq, res.Requests[corev1.ResourceMemory])
	assert.Equal(t, cpuLim, res.Limits[corev1.ResourceCPU])
	assert.Equal(t, memLim, res.Limits[corev1.ResourceMemory])
}

func TestBuildPod_Resources_Partial(t *testing.T) {
	t.Parallel()

	cpuReq := resource.MustParse("100m")
	memLim := resource.MustParse("256Mi")

	cfg := fastConfig(fake.NewSimpleClientset())
	cfg.Resources = ResourcesConfig{CPURequest: &cpuReq, MemoryLimit: &memLim}
	p := New(cfg)
	pod := p.buildPod("skylr-shard-aabbccdd")

	res := pod.Spec.Containers[0].Resources
	assert.Equal(t, cpuReq, res.Requests[corev1.ResourceCPU])
	_, hasMemReq := res.Requests[corev1.ResourceMemory]
	assert.False(t, hasMemReq, "memory request should not be set")
	assert.Equal(t, memLim, res.Limits[corev1.ResourceMemory])
	_, hasCPULim := res.Limits[corev1.ResourceCPU]
	assert.False(t, hasCPULim, "cpu limit should not be set")
}

func TestBuildPod_Resources_NoneSet(t *testing.T) {
	t.Parallel()

	p := New(fastConfig(fake.NewSimpleClientset()))
	pod := p.buildPod("skylr-shard-aabbccdd")

	res := pod.Spec.Containers[0].Resources
	assert.Nil(t, res.Requests)
	assert.Nil(t, res.Limits)
}

// =============================================================================
// containerIsRunning — table-driven
// =============================================================================

func TestContainerIsRunning(t *testing.T) {
	t.Parallel()

	running := &corev1.ContainerStateRunning{}

	cases := []struct {
		name     string
		statuses []corev1.ContainerStatus
		want     bool
	}{
		{
			name:     "no containers",
			statuses: nil,
			want:     false,
		},
		{
			name:     "wrong container name",
			statuses: []corev1.ContainerStatus{{Name: "other", State: corev1.ContainerState{Running: running}}},
			want:     false,
		},
		{
			name:     "shard container not running",
			statuses: []corev1.ContainerStatus{{Name: "shard", State: corev1.ContainerState{Running: nil}}},
			want:     false,
		},
		{
			name:     "shard container running",
			statuses: []corev1.ContainerStatus{{Name: "shard", State: corev1.ContainerState{Running: running}}},
			want:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pod := &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: tc.statuses}}
			assert.Equal(t, tc.want, containerIsRunning(pod))
		})
	}
}

// =============================================================================
// Provision — happy path
// =============================================================================

func TestProvision_HappyPath(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	p := New(fastConfig(client))

	go func() {
		podName := firstPodName(t, client, testNS)
		makeRunningPod(t, client, testNS, podName, "10.0.0.1")
	}()

	addr, err := p.Provision(context.Background())
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("10.0.0.1:%d", testGRPCPort), addr)
}

func TestProvision_AddrTracked(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	p := New(fastConfig(client))

	go func() {
		podName := firstPodName(t, client, testNS)
		makeRunningPod(t, client, testNS, podName, "10.0.0.2")
	}()

	addr, err := p.Provision(context.Background())
	require.NoError(t, err)

	p.mu.Lock()
	podName, ok := p.pods[addr]
	p.mu.Unlock()

	assert.True(t, ok)
	assert.Regexp(t, `^skylr-shard-[0-9a-f]{8}$`, podName)
}

// =============================================================================
// Provision — max shards
// =============================================================================

func TestProvision_MaxShardsReached(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	cfg := fastConfig(client)
	cfg.MaxShards = 2
	p := New(cfg)

	// Simulate two already-tracked pods to fill the limit.
	p.mu.Lock()
	p.pods["10.0.1.1:9000"] = "skylr-shard-aaaa0001"
	p.pods["10.0.1.2:9000"] = "skylr-shard-aaaa0002"
	p.mu.Unlock()

	_, err := p.Provision(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max shards")

	list, _ := client.CoreV1().Pods(testNS).List(context.Background(), metav1.ListOptions{})
	assert.Empty(t, list.Items, "no pod should be created when max shards is reached")
}

func TestProvision_MaxShardsNotReached(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	cfg := fastConfig(client)
	cfg.MaxShards = 2
	p := New(cfg)

	// One slot already occupied — one slot still free.
	p.mu.Lock()
	p.pods["10.0.1.1:9000"] = "skylr-shard-aaaa0001"
	p.mu.Unlock()

	go func() {
		podName := firstPodName(t, client, testNS)
		makeRunningPod(t, client, testNS, podName, "10.0.0.3")
	}()

	_, err := p.Provision(context.Background())
	require.NoError(t, err)
}

func TestProvision_NilShardCount_SkipsCheck(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	cfg := fastConfig(client)
	cfg.ShardCount = nil
	p := New(cfg)

	go func() {
		podName := firstPodName(t, client, testNS)
		makeRunningPod(t, client, testNS, podName, "10.0.0.4")
	}()

	_, err := p.Provision(context.Background())
	require.NoError(t, err)
}

// =============================================================================
// Provision — create error
// =============================================================================

func TestProvision_CreatePodFails(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("quota exceeded")
	})
	p := New(fastConfig(client))

	_, err := p.Provision(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create pod")
}

// =============================================================================
// waitForPodReady — state transitions
// =============================================================================

func TestProvision_PodTimeout(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	cfg := fastConfig(client)
	cfg.RegistrationTimeout = 20 * time.Millisecond
	p := New(cfg)

	// Pod is never transitioned to Running — timeout fires.
	_, err := p.Provision(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not become ready")

	// cleanup() is called inline before Provision returns, so the pod is already gone.
	list, _ := client.CoreV1().Pods(testNS).List(context.Background(), metav1.ListOptions{})
	assert.Empty(t, list.Items, "cleanup should have deleted the pod")
}

func TestProvision_PodFailedPhase(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	p := New(fastConfig(client))

	go func() {
		podName := firstPodName(t, client, testNS)
		makeTerminalPod(t, client, testNS, podName, corev1.PodFailed)
	}()

	_, err := p.Provision(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "terminal phase")
}

func TestProvision_PodSucceededPhase(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	p := New(fastConfig(client))

	go func() {
		podName := firstPodName(t, client, testNS)
		makeTerminalPod(t, client, testNS, podName, corev1.PodSucceeded)
	}()

	_, err := p.Provision(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "terminal phase")
}

func TestProvision_ContextCancelledDuringPodWait(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	p := New(fastConfig(client))

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		firstPodName(t, client, testNS) // wait for pod creation
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	_, err := p.Provision(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestWaitForPodReady_GetError(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("api server down")
	})

	cfg := fastConfig(client)
	cfg.RegistrationTimeout = 50 * time.Millisecond
	p := New(cfg)

	_, err := p.waitForPodReady(context.Background(), "skylr-shard-test0001")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get pod")
}

// =============================================================================
// waitForRegistration
// =============================================================================

func TestWaitForRegistration_Succeeds(t *testing.T) {
	t.Parallel()

	p := New(fastConfig(fake.NewSimpleClientset()))
	err := p.waitForRegistration(context.Background(), "10.0.0.1:9000")
	require.NoError(t, err)
}

func TestWaitForRegistration_Timeout(t *testing.T) {
	t.Parallel()

	cfg := fastConfig(fake.NewSimpleClientset())
	cfg.IsShardRegistered = func(string) bool { return false }
	cfg.RegistrationTimeout = 20 * time.Millisecond
	p := New(cfg)

	err := p.waitForRegistration(context.Background(), "10.0.0.1:9000")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not register")
}

func TestWaitForRegistration_ContextCancelled(t *testing.T) {
	t.Parallel()

	cfg := fastConfig(fake.NewSimpleClientset())
	cfg.IsShardRegistered = func(string) bool { return false }
	p := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	err := p.waitForRegistration(ctx, "10.0.0.1:9000")
	require.ErrorIs(t, err, context.Canceled)
}

func TestProvision_RegistrationTimeout_CleanupDeletesPod(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	cfg := fastConfig(client)
	cfg.IsShardRegistered = func(string) bool { return false }
	cfg.RegistrationTimeout = 50 * time.Millisecond
	p := New(cfg)

	go func() {
		podName := firstPodName(t, client, testNS)
		makeRunningPod(t, client, testNS, podName, "10.0.0.5")
	}()

	_, err := p.Provision(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not register")

	// Pod must be cleaned up.
	pods, _ := client.CoreV1().Pods(testNS).List(context.Background(), metav1.ListOptions{})
	assert.Eventually(t,
		func() bool {
			list, _ := client.CoreV1().Pods(testNS).List(context.Background(), metav1.ListOptions{})
			return len(list.Items) == 0
		},
		500*time.Millisecond, time.Millisecond,
		"cleanup should delete the pod, got %d pods", len(pods.Items),
	)

	p.mu.Lock()
	podsLen := len(p.pods)
	p.mu.Unlock()
	assert.Equal(t, 0, podsLen)
}

func TestProvision_PostRegistrationDelay_ContextCancelled(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	cfg := fastConfig(client)
	cfg.PostRegistrationDelay = 200 * time.Millisecond
	p := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		podName := firstPodName(t, client, testNS)
		makeRunningPod(t, client, testNS, podName, "10.0.0.6")
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	_, err := p.Provision(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestProvision_PostRegistrationDelay_Respected(t *testing.T) {
	t.Parallel()

	const delay = 30 * time.Millisecond
	client := fake.NewSimpleClientset()
	cfg := fastConfig(client)
	cfg.PostRegistrationDelay = delay
	p := New(cfg)

	go func() {
		podName := firstPodName(t, client, testNS)
		makeRunningPod(t, client, testNS, podName, "10.0.0.7")
	}()

	start := time.Now()
	_, err := p.Provision(context.Background())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), delay)
}

// =============================================================================
// Deprovision
// =============================================================================

func TestDeprovision_HappyPath(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	p := New(fastConfig(client))

	const (
		addr    = "10.0.1.1:9000"
		podName = "skylr-shard-deprov01"
	)

	// Pre-create the pod in the fake client.
	_, err := client.CoreV1().Pods(testNS).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: testNS},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	p.mu.Lock()
	p.pods[addr] = podName
	p.mu.Unlock()

	require.NoError(t, p.Deprovision(context.Background(), addr))

	assert.False(t, podExists(client, testNS, podName))

	p.mu.Lock()
	_, still := p.pods[addr]
	p.mu.Unlock()
	assert.False(t, still)
}

func TestDeprovision_UnknownAddr(t *testing.T) {
	t.Parallel()

	p := New(fastConfig(fake.NewSimpleClientset()))
	err := p.Deprovision(context.Background(), "not-tracked:9000")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDeprovision_DeleteError(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	client.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("forbidden")
	})

	p := New(fastConfig(client))

	const addr = "10.0.1.2:9000"
	p.mu.Lock()
	p.pods[addr] = "skylr-shard-deleteerr"
	p.mu.Unlock()

	err := p.Deprovision(context.Background(), addr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete pod")

	// addr is removed from the map regardless of delete error.
	p.mu.Lock()
	_, still := p.pods[addr]
	p.mu.Unlock()
	assert.False(t, still)
}

// =============================================================================
// Shutdown
// =============================================================================

func TestShutdown_DeletesAllPods(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	p := New(fastConfig(client))

	pods := map[string]string{
		"10.0.2.1:9000": "skylr-shard-shut0001",
		"10.0.2.2:9000": "skylr-shard-shut0002",
		"10.0.2.3:9000": "skylr-shard-shut0003",
	}
	for addr, podName := range pods {
		_, err := client.CoreV1().Pods(testNS).Create(context.Background(), &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: testNS},
		}, metav1.CreateOptions{})
		require.NoError(t, err)

		p.mu.Lock()
		p.pods[addr] = podName
		p.mu.Unlock()
	}

	require.NoError(t, p.Shutdown(context.Background()))

	for _, podName := range pods {
		assert.False(t, podExists(client, testNS, podName), "pod %s should be deleted", podName)
	}

	p.mu.Lock()
	remaining := len(p.pods)
	p.mu.Unlock()
	assert.Equal(t, 0, remaining)
}

func TestShutdown_Empty(t *testing.T) {
	t.Parallel()

	p := New(fastConfig(fake.NewSimpleClientset()))
	require.NoError(t, p.Shutdown(context.Background()))
}

func TestShutdown_PartialFailures_Ignored(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	p := New(fastConfig(client))

	const (
		addrMissing = "10.0.3.1:9000"
		podMissing  = "skylr-shard-missing1"
		addrPresent = "10.0.3.2:9000"
		podPresent  = "skylr-shard-present1"
	)

	// Only the second pod exists in the fake client.
	_, err := client.CoreV1().Pods(testNS).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podPresent, Namespace: testNS},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	p.mu.Lock()
	p.pods[addrMissing] = podMissing
	p.pods[addrPresent] = podPresent
	p.mu.Unlock()

	// Shutdown ignores individual Deprovision errors.
	require.NoError(t, p.Shutdown(context.Background()))
	assert.False(t, podExists(client, testNS, podPresent), "existing pod should be deleted")
}

// =============================================================================
// Concurrency
// =============================================================================

func TestProvision_Concurrent_MaxShardsBoundary(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	cfg := fastConfig(client)
	cfg.MaxShards = 1

	var provisionedCount atomic.Int32
	cfg.ShardCount = func() int { return int(provisionedCount.Load()) }
	p := New(cfg)

	// Background goroutine: as soon as a pod appears, transition it to Running.
	go func() {
		for {
			list, _ := client.CoreV1().Pods(testNS).List(context.Background(), metav1.ListOptions{})
			if len(list.Items) > 0 {
				pod := list.Items[0]
				if pod.Status.PodIP == "" {
					makeRunningPod(t, client, testNS, pod.Name, "10.0.4.1")
					provisionedCount.Add(1)
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	const goroutines = 5
	results := make([]error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			_, results[i] = p.Provision(context.Background())
		}(i)
	}
	wg.Wait()

	successes := 0
	for _, err := range results {
		if err == nil {
			successes++
		}
	}
	assert.Equal(t, 1, successes, "exactly one provision should succeed when MaxShards=1")
}

func TestDeprovision_Concurrent_NoDataRace(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	p := New(fastConfig(client))

	addrs := make([]string, 5)
	for i := range addrs {
		addr := fmt.Sprintf("10.0.5.%d:9000", i+1)
		podName := fmt.Sprintf("skylr-shard-race%04d", i+1)
		addrs[i] = addr

		_, err := client.CoreV1().Pods(testNS).Create(context.Background(), &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: testNS},
		}, metav1.CreateOptions{})
		require.NoError(t, err)

		p.mu.Lock()
		p.pods[addr] = podName
		p.mu.Unlock()
	}

	var wg sync.WaitGroup
	wg.Add(len(addrs))
	for _, addr := range addrs {
		go func(a string) {
			defer wg.Done()
			_ = p.Deprovision(context.Background(), a)
		}(addr)
	}
	wg.Wait()

	p.mu.Lock()
	remaining := len(p.pods)
	p.mu.Unlock()
	assert.Equal(t, 0, remaining)
}

// =============================================================================
// addrs
// =============================================================================

func TestAddrs_ReturnsAllTracked(t *testing.T) {
	t.Parallel()

	p := New(fastConfig(fake.NewSimpleClientset()))

	p.mu.Lock()
	p.pods["10.0.6.1:9000"] = "pod-a"
	p.pods["10.0.6.2:9000"] = "pod-b"
	p.pods["10.0.6.3:9000"] = "pod-c"
	p.mu.Unlock()

	assert.ElementsMatch(t, []string{"10.0.6.1:9000", "10.0.6.2:9000", "10.0.6.3:9000"}, p.addrs())
}

func TestAddrs_EmptyMap(t *testing.T) {
	t.Parallel()

	p := New(fastConfig(fake.NewSimpleClientset()))
	assert.Empty(t, p.addrs())
}
