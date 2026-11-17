package config_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/config"
)

func TestCadence_Load(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(strings.NewReader("hostPort: :8080\n" + upstreamYAML + `
cadence:
  grpc:
    hostPort: :7833
  tchannel:
    hostPort: :7933
`))
	require.NoError(t, err)
	require.True(t, cfg.Cadence.Enabled())
	require.Equal(t, ":7833", cfg.Cadence.GRPC.HostPort)
	require.Equal(t, ":7933", cfg.Cadence.TChannel.HostPort)
	require.Nil(t, cfg.Cadence.Temporal, "absent, translated calls go to the gateway")
}

func TestCadence_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*config.Config)
		wantErr string
	}{
		{
			name:   "absent is off and valid",
			mutate: func(*config.Config) {},
		},
		{
			name:   "a block with no listener is off and valid",
			mutate: func(c *config.Config) { c.Cadence = &config.Cadence{} },
		},
		{
			name: "gRPC alone is valid",
			mutate: func(c *config.Config) {
				c.Cadence = &config.Cadence{GRPC: &config.ListenConfig{HostPort: ":7833"}}
			},
		},
		{
			name: "a bad TChannel address is rejected",
			mutate: func(c *config.Config) {
				c.Cadence = &config.Cadence{TChannel: &config.TChannelListen{HostPort: "nope"}}
			},
			wantErr: "tchannel.hostPort: is not a valid host:port",
		},
		{
			name: "a TLS gateway needs an explicit Temporal target",
			mutate: func(c *config.Config) {
				c.Listen.TLS = &config.TLSConfig{Cert: "server.crt", Key: "server.key"}
				c.Cadence = &config.Cadence{GRPC: &config.ListenConfig{HostPort: ":7833"}}
			},
			wantErr: "temporal: required when the gateway serves TLS",
		},
		{
			name: "an explicit Temporal target is checked",
			mutate: func(c *config.Config) {
				c.Cadence = &config.Cadence{
					GRPC:     &config.ListenConfig{HostPort: ":7833"},
					Temporal: &config.ListenConfig{HostPort: "bad"},
				}
			},
			wantErr: "cadence.temporal: hostPort: is not a valid host:port",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := minimalConfig()
			tt.mutate(cfg)

			err := cfg.Prepare()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
