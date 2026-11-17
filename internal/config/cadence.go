package config

import (
	"errors"

	"go.temporal.io/proxy/pkg/validation"
)

type (
	// Cadence configures the Cadence compatibility frontend: listeners that
	// speak the Cadence frontend API and translate every call onto the Temporal
	// WorkflowService, so Cadence Clients and Workers run against Temporal
	// without code changes. The frontend is on when either listener is set.
	//
	// Translated calls go to Temporal, by default the gateway of this proxy, so
	// routing, Namespace translation, encryption, and outbound credentials all
	// apply to them exactly as to a Temporal SDK's calls. A Cadence Domain is the
	// Temporal Namespace of the same name.
	Cadence struct {
		// GRPC serves the Cadence proto API (uber.cadence.api.v1) over gRPC, the
		// transport of Cadence Go SDK v1 and later on gRPC and of the Java SDK on
		// gRPC. Cadence's own frontend listens on port 7833.
		GRPC *ListenConfig `yaml:"grpc"`

		// TChannel serves the Cadence Thrift API over TChannel, the transport of
		// older Go SDK setups and the Java SDK by default. Cadence's own frontend
		// listens on port 7933. TChannel has no TLS.
		TChannel *TChannelListen `yaml:"tchannel"`

		// Temporal is where translated calls go. Absent, they go to this proxy's
		// own gateway, which must then be plaintext.
		Temporal *ListenConfig `yaml:"temporal"`

		// ServiceName is the YARPC service Cadence SDKs address. Absent, it is
		// "cadence-frontend", which is what every Cadence SDK uses by default.
		ServiceName string `yaml:"serviceName"`
	}

	// TChannelListen is a TChannel listen address.
	TChannelListen struct {
		HostPort string `yaml:"hostPort"`
	}
)

// cadenceRules validates a Cadence block against the gateway it may default to.
type cadenceRules struct {
	cadence *Cadence
	gateway *ListenConfig
}

func (r cadenceRules) Validate() error { return r.cadence.validate(r.gateway) }

// Enabled reports whether any Cadence listener is configured.
func (c *Cadence) Enabled() bool {
	return c != nil && (c.GRPC != nil || c.TChannel != nil)
}

// validate checks each configured listener and the Temporal target. gateway is
// the proxy's own listener, which is the default target and so must be
// plaintext when no target is given.
func (c *Cadence) validate(gateway *ListenConfig) error {
	if !c.Enabled() {
		return nil
	}

	return validation.Validate(
		"",
		validation.WhenNested(func() bool { return c.GRPC != nil }, "grpc", c.GRPC),
		validation.WhenRules(func() bool { return c.TChannel != nil },
			validation.Field("tchannel.hostPort", c.TChannel.hostPort(), validation.IsHostPort()),
		),
		validation.WhenNested(func() bool { return c.Temporal != nil }, "temporal", c.Temporal),
		validation.Field("temporal", c.Temporal, func(t *ListenConfig) error {
			if t == nil && gateway.TLS != nil {
				return errors.New("required when the gateway serves TLS: the frontend cannot dial it in plaintext")
			}

			return nil
		}),
	)
}

func (t *TChannelListen) hostPort() string {
	if t == nil {
		return ""
	}

	return t.HostPort
}
