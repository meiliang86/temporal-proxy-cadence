package cadence

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/fx"
	"go.uber.org/yarpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
)

// Module runs the Cadence frontend when the config has a cadence block, and is
// inert otherwise. It starts after the dataplane, whose gateway is the default
// destination for translated calls.
var Module = fx.Invoke(register)

// forwardedHeaders are the inbound Cadence request headers carried onto the
// translated Temporal call, so the gateway's inbound authentication sees the
// caller's own credential.
var forwardedHeaders = []string{"authorization"}

type params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    *config.Config
	Dataplane *dataplane.Dataplane
	Logger    logger.Logger
}

func register(p params) error {
	cfg := p.Config.Cadence
	if !cfg.Enabled() {
		return nil
	}

	log := p.Logger.With(tag.Component("cadence"))

	var (
		conn     *grpc.ClientConn
		frontend *Frontend
	)

	// Stopping cancels every translated call still in flight. Workers hold long
	// polls open for a minute or more, which a graceful stop would otherwise wait
	// out well past the application's stop budget; a cancelled poll is retried by
	// its Worker.
	inflight, shutdown := context.WithCancel(context.Background())

	p.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			var err error

			conn, err = dialTemporal(cfg, p.Dataplane.Addr(), inflight)
			if err != nil {
				return err
			}

			h := NewHandler(
				workflowservice.NewWorkflowServiceClient(conn),
				operatorservice.NewOperatorServiceClient(conn),
				log,
			)

			opts := FrontendOptions{ServiceName: cfg.ServiceName}

			if cfg.GRPC != nil {
				opts.GRPCAddress = cfg.GRPC.HostPort
				if opts.GRPCTLS, err = listenerTLS(cfg.GRPC); err != nil {
					return err
				}
			}

			if cfg.TChannel != nil {
				opts.TChannelAddress = cfg.TChannel.HostPort
			}

			if frontend, err = NewFrontend(h, opts); err != nil {
				return err
			}

			if err := frontend.Start(ctx); err != nil {
				return err
			}

			log.Info("Cadence frontend started",
				tag.String("grpc", addrString(frontend.GRPCAddr())),
				tag.String("tchannel", frontend.TChannelAddr()),
				tag.String("temporal", conn.Target()))

			return nil
		},
		OnStop: func(ctx context.Context) error {
			shutdown()

			var err error
			if frontend != nil {
				err = frontend.Stop(ctx)
			}

			if conn != nil {
				_ = conn.Close()
			}

			return err
		},
	})

	return nil
}

func addrString(a net.Addr) string {
	if a == nil {
		return ""
	}

	return a.String()
}

// dialTemporal connects to the configured Temporal target, or to the gateway
// at gateway in plaintext when none is configured. Every call on the
// connection is cancelled once inflight is.
func dialTemporal(cfg *config.Cadence, gateway net.Addr, inflight context.Context) (*grpc.ClientConn, error) {
	target := cfg.Temporal
	if target == nil {
		target = &config.ListenConfig{HostPort: dialable(gateway), Insecure: true}
	}

	host, _, err := net.SplitHostPort(target.HostPort)
	if err != nil {
		return nil, fmt.Errorf("cadence: temporal target %q: %w", target.HostPort, err)
	}

	creds, err := target.Dialer().DialOption(serverName(target, host))
	if err != nil {
		return nil, fmt.Errorf("cadence: temporal target credentials: %w", err)
	}

	return grpc.NewClient(target.HostPort,
		creds,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
		grpc.WithChainUnaryInterceptor(cancelWith(inflight), forwardHeaders),
	)
}

// cancelWith cancels each call when inflight is cancelled.
func cancelWith(inflight context.Context) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		stop := context.AfterFunc(inflight, cancel)
		defer stop()

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func serverName(target *config.ListenConfig, host string) string {
	if target.TLS != nil && target.TLS.ServerName != "" {
		return target.TLS.ServerName
	}

	return host
}

// dialable turns a listener's bound address into one a client can dial: an
// unspecified IP becomes loopback.
func dialable(a net.Addr) string {
	tcp, ok := a.(*net.TCPAddr)
	if !ok {
		return a.String()
	}

	ip := tcp.IP
	if ip == nil || ip.IsUnspecified() {
		ip = net.IPv4(127, 0, 0, 1)
	}

	return net.JoinHostPort(ip.String(), fmt.Sprint(tcp.Port))
}

func listenerTLS(l *config.ListenConfig) (*tls.Config, error) {
	if l.Insecure || l.TLS == nil {
		return nil, nil
	}

	return l.Listener().TLSConfig()
}

// forwardHeaders copies [forwardedHeaders] from the inbound YARPC call onto the
// outgoing Temporal call.
func forwardHeaders(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	if call := yarpc.CallFromContext(ctx); call != nil {
		for _, h := range forwardedHeaders {
			if v := call.Header(h); v != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, h, v)
			}
		}
	}

	return invoker(ctx, method, req, reply, cc, opts...)
}
