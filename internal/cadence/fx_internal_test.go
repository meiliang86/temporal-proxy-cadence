package cadence

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestStoppingCancelsInFlightCalls(t *testing.T) {
	t.Parallel()

	inflight, shutdown := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- cancelWith(inflight)(t.Context(), "/poll", nil, nil, nil,
			func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				close(started)
				<-ctx.Done() // a long poll

				return ctx.Err()
			})
	}()

	<-started
	shutdown()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight call outlived the shutdown")
	}
}
