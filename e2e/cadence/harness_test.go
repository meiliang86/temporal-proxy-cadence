// Package cadence_test drives the Cadence compatibility frontend end to end:
// unmodified Cadence Go SDK Clients and Workers talk to the proxy over gRPC or
// TChannel, and the proxy runs them on a real Temporal Service.
//
// The Temporal Service is the Temporal CLI's dev server. The tests start one
// when a `temporal` binary is on PATH (mise installs it), or use the one at
// TEMPORAL_ADDRESS, and skip otherwise.
package cadence_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/compatibility"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/grpc"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/zap"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane/dataplanetest"
)

// domain is the Cadence Domain the tests run in: the dev server's default
// Namespace, which has the Cadence-style custom search attributes registered.
const domain = "default"

var (
	temporalOnce sync.Once
	temporalAddr string
	temporalErr  error
	devServer    *exec.Cmd
)

// TestMain stops the dev server, if a test started one, once every test is done.
func TestMain(m *testing.M) {
	code := m.Run()

	if devServer != nil {
		_ = devServer.Process.Signal(os.Interrupt)

		done := make(chan struct{})
		go func() { _ = devServer.Wait(); close(done) }()

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = devServer.Process.Kill()
		}
	}

	os.Exit(code)
}

// temporal returns the address of a running Temporal Service, starting a dev
// server on first use. It skips the test when there is none to be had.
func temporal(t *testing.T) string {
	t.Helper()

	temporalOnce.Do(func() {
		if addr := os.Getenv("TEMPORAL_ADDRESS"); addr != "" {
			temporalAddr = addr
			return
		}

		bin, err := exec.LookPath("temporal")
		if err != nil {
			temporalErr = fmt.Errorf("no temporal CLI on PATH and TEMPORAL_ADDRESS unset: %w", err)
			return
		}

		port := freePort(t)
		cmd := exec.Command(bin, "server", "start-dev", //nolint:gosec // A test fixture.
			"--headless",
			"--ip", "127.0.0.1",
			"--port", strconv.Itoa(port),
			"--http-port", strconv.Itoa(freePort(t)),
			"--metrics-port", strconv.Itoa(freePort(t)),
			"--log-level", "error",
			"--dynamic-config-value", "system.forceSearchAttributesCacheRefreshOnRead=true",
		)
		// Left unset, the dev server's output is discarded; a pipe to the test's own
		// would keep `go test` waiting on it.
		if err := cmd.Start(); err != nil {
			temporalErr = err
			return
		}

		devServer = cmd
		temporalAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		temporalErr = waitForTemporal(temporalAddr)
	})

	if temporalErr != nil {
		t.Skipf("Temporal Service unavailable: %v", temporalErr)
	}

	return temporalAddr
}

func waitForTemporal(addr string) error {
	conn, err := grpclib.NewClient(addr, grpclib.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	c := workflowservice.NewWorkflowServiceClient(conn)
	deadline := time.Now().Add(90 * time.Second)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = c.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: domain})

		cancel()

		if err == nil {
			return registerSearchAttributes(conn)
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("dev server at %s not ready: %w", addr, err)
		}

		time.Sleep(250 * time.Millisecond)
	}
}

// cadenceSearchAttributes are the custom search attributes a Cadence cluster
// ships with. A Temporal Namespace has none of them; registering the ones a
// Domain uses is a one-time step of moving it onto Temporal.
var cadenceSearchAttributes = map[string]enumspb.IndexedValueType{
	"CustomKeywordField":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	"CustomStringField":   enumspb.INDEXED_VALUE_TYPE_TEXT,
	"CustomIntField":      enumspb.INDEXED_VALUE_TYPE_INT,
	"CustomDoubleField":   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
	"CustomBoolField":     enumspb.INDEXED_VALUE_TYPE_BOOL,
	"CustomDatetimeField": enumspb.INDEXED_VALUE_TYPE_DATETIME,
}

func registerSearchAttributes(conn *grpclib.ClientConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	op := operatorservice.NewOperatorServiceClient(conn)

	existing, err := op.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: domain})
	if err != nil {
		return err
	}

	missing := map[string]enumspb.IndexedValueType{}
	for k, v := range cadenceSearchAttributes {
		if _, ok := existing.CustomAttributes[k]; !ok {
			missing[k] = v
		}
	}

	if len(missing) == 0 {
		return nil
	}

	_, err = op.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace:        domain,
		SearchAttributes: missing,
	})

	return err
}

func freePort(t *testing.T) int {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := lis.Addr().(*net.TCPAddr).Port
	require.NoError(t, lis.Close())

	return port
}

// transport is how a Cadence SDK reaches its frontend.
type transport string

const (
	transportGRPC     transport = "grpc"
	transportTChannel transport = "tchannel"
)

// env is one proxy with the Cadence frontend on, and a Cadence service client
// for one transport.
type env struct {
	t        *testing.T
	service  workflowserviceclient.Interface
	client   client.Client
	taskList string
}

// startProxy runs the whole proxy, its Cadence frontend serving both
// transports, in front of the Temporal Service.
func startProxy(t *testing.T) (grpcAddr, tchannelAddr string) {
	t.Helper()

	upstream := temporal(t)
	grpcAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t)))
	tchannelAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t)))

	dataplanetest.Start(t, &config.Config{
		Routing: config.Routing{DefaultUpstream: "temporal", SystemUpstream: "temporal"},
		Upstreams: config.UpstreamList{{
			Name:   "temporal",
			Listen: config.ListenConfig{HostPort: upstream, Insecure: true},
		}},
		Cadence: &config.Cadence{
			GRPC:     &config.ListenConfig{HostPort: grpcAddr},
			TChannel: &config.TChannelListen{HostPort: tchannelAddr},
		},
	})

	return grpcAddr, tchannelAddr
}

// newService dials the frontend the way a Cadence application does.
func newService(t *testing.T, tr transport, addr string) workflowserviceclient.Interface {
	t.Helper()

	var outbound yarpc.Outbounds

	switch tr {
	case transportGRPC:
		outbound = yarpc.Outbounds{"cadence-frontend": {Unary: grpc.NewTransport().NewSingleOutbound(addr)}}
	case transportTChannel:
		ch, err := tchannel.NewChannelTransport(tchannel.ServiceName("cadence-e2e-client"))
		require.NoError(t, err)

		outbound = yarpc.Outbounds{"cadence-frontend": {Unary: ch.NewSingleOutbound(addr)}}
	}

	d := yarpc.NewDispatcher(yarpc.Config{Name: "cadence-e2e-client", Outbounds: outbound})
	require.NoError(t, d.Start())
	t.Cleanup(func() { _ = d.Stop() })

	cc := d.ClientConfig("cadence-frontend")

	if tr == transportTChannel {
		return workflowserviceclient.New(cc)
	}

	return compatibility.NewThrift2ProtoAdapter(compatibility.AdapterClients{
		Domain:     apiv1.NewDomainAPIYARPCClient(cc),
		Workflow:   apiv1.NewWorkflowAPIYARPCClient(cc),
		Worker:     apiv1.NewWorkerAPIYARPCClient(cc),
		Visibility: apiv1.NewVisibilityAPIYARPCClient(cc),
	})
}

// newEnv starts a proxy, a Client, and a Worker on a fresh Task List with
// every test Workflow and Activity registered.
func newEnv(t *testing.T, tr transport) *env {
	t.Helper()

	grpcAddr, tchannelAddr := startProxy(t)

	addr := grpcAddr
	if tr == transportTChannel {
		addr = tchannelAddr
	}

	service := newService(t, tr, addr)
	e := &env{
		t:        t,
		service:  service,
		taskList: "cadence-e2e-" + uuid.NewString(),
		client: client.NewClient(service, domain, &client.Options{
			ContextPropagators: []workflow.ContextPropagator{propagator{}},
		}),
	}

	w, err := worker.NewV2(service, domain, e.taskList, worker.Options{
		Logger:              zap.NewNop(),
		EnableSessionWorker: true,
		ContextPropagators:  []workflow.ContextPropagator{propagator{}},
	})
	require.NoError(t, err)
	registerAll(w)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	return e
}

func (e *env) ctx() context.Context {
	ctx, cancel := context.WithTimeout(e.t.Context(), 2*time.Minute)
	e.t.Cleanup(cancel)

	return ctx
}

func (e *env) options() client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:                              "wf-" + uuid.NewString(),
		TaskList:                        e.taskList,
		ExecutionStartToCloseTimeout:    2 * time.Minute,
		DecisionTaskStartToCloseTimeout: 10 * time.Second,
	}
}

// run starts wf and waits for its result.
func (e *env) run(result any, wf any, args ...any) (client.WorkflowRun, error) {
	e.t.Helper()

	run, err := e.client.ExecuteWorkflow(e.ctx(), e.options(), wf, args...)
	require.NoError(e.t, err)

	return run, run.Get(e.ctx(), result)
}

// replay replays a closed Workflow's history, fetched through the frontend,
// against the same Workflow code. It fails when the translated history is not
// one the code could have produced, which is the property every decision task
// in production depends on.
func (e *env) replay(run client.WorkflowRun) {
	e.t.Helper()

	r := worker.NewWorkflowReplayer()
	registerReplayer(r)

	err := r.ReplayWorkflowExecution(e.ctx(), e.service, zap.NewNop(), domain,
		workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()})
	require.NoError(e.t, err, "replaying %s", run.GetID())
}

// eventually retries cond until it holds, for visibility, which is eventually
// consistent.
func (e *env) eventually(cond func() bool, msg string) {
	e.t.Helper()
	require.Eventually(e.t, cond, 30*time.Second, 200*time.Millisecond, msg)
}
