package cadence

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceserver"
	"go.uber.org/cadence/compatibility"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/grpc"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
)

const (
	// DefaultServiceName is the YARPC service every Cadence SDK addresses.
	DefaultServiceName = "cadence-frontend"

	// maxMessageSize bounds one gRPC message each way. A decision task carries
	// a page of history, which can far exceed gRPC's 4 MiB default.
	maxMessageSize = 128 * 1024 * 1024
)

// FrontendOptions configures the listeners a [Frontend] serves.
type FrontendOptions struct {
	// GRPCAddress is where to serve the proto API over gRPC; empty disables it.
	GRPCAddress string
	// GRPCTLS, when set, terminates TLS on the gRPC listener.
	GRPCTLS *tls.Config
	// TChannelAddress is where to serve the Thrift API over TChannel; empty
	// disables it.
	TChannelAddress string
	// ServiceName is the YARPC service name; empty means [DefaultServiceName].
	ServiceName string
}

// Frontend serves a [Handler] to Cadence SDKs over gRPC, TChannel, or both.
type Frontend struct {
	dispatcher *yarpc.Dispatcher
	grpcLis    net.Listener
	tchannel   *tchannel.ChannelTransport
}

// NewFrontend builds a Frontend for h. It binds the gRPC listener immediately,
// so [Frontend.GRPCAddr] is known before Start; TChannel binds on Start.
func NewFrontend(h *Handler, opts FrontendOptions) (*Frontend, error) {
	if opts.GRPCAddress == "" && opts.TChannelAddress == "" {
		return nil, errors.New("cadence frontend: no listener configured")
	}

	name := opts.ServiceName
	if name == "" {
		name = DefaultServiceName
	}

	f := &Frontend{}
	cfg := yarpc.Config{Name: name, Logging: yarpc.LoggingConfig{Zap: zap.NewNop()}}

	if opts.GRPCAddress != "" {
		lis, err := net.Listen("tcp", opts.GRPCAddress)
		if err != nil {
			return nil, fmt.Errorf("cadence frontend: listen on %s: %w", opts.GRPCAddress, err)
		}

		f.grpcLis = lis

		inOpts := []grpc.InboundOption{}
		if opts.GRPCTLS != nil {
			inOpts = append(inOpts, grpc.InboundCredentials(credentials.NewTLS(opts.GRPCTLS)))
		}

		t := grpc.NewTransport(grpc.ServerMaxRecvMsgSize(maxMessageSize), grpc.ServerMaxSendMsgSize(maxMessageSize))
		cfg.Inbounds = append(cfg.Inbounds, t.NewInbound(lis, inOpts...))
	}

	if opts.TChannelAddress != "" {
		t, err := tchannel.NewChannelTransport(tchannel.ServiceName(name), tchannel.ListenAddr(opts.TChannelAddress))
		if err != nil {
			f.closeListener()
			return nil, fmt.Errorf("cadence frontend: tchannel: %w", err)
		}

		f.tchannel = t
		cfg.Inbounds = append(cfg.Inbounds, t.NewInbound())
	}

	d := yarpc.NewDispatcher(cfg)

	// The proto API, for SDKs on the gRPC transport.
	d.Register(apiv1.BuildDomainAPIYARPCProcedures(h))
	d.Register(apiv1.BuildWorkflowAPIYARPCProcedures(h))
	d.Register(apiv1.BuildWorkerAPIYARPCProcedures(h))
	d.Register(apiv1.BuildVisibilityAPIYARPCProcedures(h))
	d.Register(apiv1.BuildMetaAPIYARPCProcedures(h))

	// The Thrift API, for SDKs on TChannel. It reaches the same handlers through
	// the Cadence client's own Thrift-to-proto adapter, so both transports share
	// one translation.
	d.Register(workflowserviceserver.New(thriftServer{c: compatibility.NewThrift2ProtoAdapter(compatibility.AdapterClients{
		Domain:     localDomainClient{s: h},
		Workflow:   localWorkflowClient{s: h},
		Worker:     localWorkerClient{s: h},
		Visibility: localVisibilityClient{s: h},
		Schedule:   unsupportedScheduleClient{},
	})}))

	f.dispatcher = d

	return f, nil
}

// GRPCAddr is the bound gRPC address, or nil when gRPC is disabled.
func (f *Frontend) GRPCAddr() net.Addr {
	if f.grpcLis == nil {
		return nil
	}

	return f.grpcLis.Addr()
}

// TChannelAddr is the bound TChannel address once started, or "" when TChannel
// is disabled.
func (f *Frontend) TChannelAddr() string {
	if f.tchannel == nil {
		return ""
	}

	return f.tchannel.ListenAddr()
}

// Start begins serving.
func (f *Frontend) Start(context.Context) error {
	if err := f.dispatcher.Start(); err != nil {
		f.closeListener()
		return fmt.Errorf("cadence frontend: %w", err)
	}

	return nil
}

// Stop stops serving. In-flight long polls are abandoned; their Workers retry.
func (f *Frontend) Stop(context.Context) error {
	return f.dispatcher.Stop()
}

func (f *Frontend) closeListener() {
	if f.grpcLis != nil {
		_ = f.grpcLis.Close()
	}
}
