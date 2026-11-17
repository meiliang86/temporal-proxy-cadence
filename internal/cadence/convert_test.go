package cadence

import (
	"context"
	"errors"
	"testing"
	"time"

	gogo "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.uber.org/yarpc/encoding/protobuf"
	"go.uber.org/yarpc/yarpcerrors"
	"google.golang.org/protobuf/types/known/durationpb"
)

func gogoDur(d time.Duration) *gogo.Duration { return gogo.DurationProto(d) }

func TestPayloadsRoundTripByteForByte(t *testing.T) {
	t.Parallel()

	for _, data := range [][]byte{
		[]byte("\"hello\"\n42\n"),
		{0x00, 0xff, 0x10},
	} {
		p := toPayloads(&apiv1.Payload{Data: data})
		require.Len(t, p.Payloads, 1)
		require.Equal(t, encodingBinary, string(p.Payloads[0].Metadata[metadataEncoding]))
		require.Equal(t, data, fromPayloads(p).Data)
	}
}

func TestEmptyPayloadStaysAbsent(t *testing.T) {
	t.Parallel()

	require.Nil(t, toPayloads(nil))
	require.Nil(t, toPayloads(&apiv1.Payload{}))
	require.Nil(t, fromPayloads(nil))
	require.Nil(t, fromPayloads(&commonpb.Payloads{Payloads: []*commonpb.Payload{
		{Metadata: map[string][]byte{metadataEncoding: []byte(encodingNull)}},
	}}))
}

func TestTemporalJSONPayloadsJoinOnePerLine(t *testing.T) {
	t.Parallel()

	got := fromPayloads(&commonpb.Payloads{Payloads: []*commonpb.Payload{
		jsonPayload([]byte(`"a"`)),
		jsonPayload([]byte("1\n")),
	}})

	require.Equal(t, "\"a\"\n1\n", string(got.Data))
}

func TestApplicationFailureRoundTrips(t *testing.T) {
	t.Parallel()

	in := &apiv1.Failure{Reason: "bad-input", Details: []byte(`"x"`)}
	f := toFailure(in)

	require.Equal(t, "bad-input", f.GetApplicationFailureInfo().GetType(),
		"the reason is the type Temporal matches non-retryable error types against")
	require.Equal(t, in, fromFailure(f))
}

func TestServiceRaisedFailuresUseCadenceReasons(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		in   *failurepb.Failure
		want *apiv1.Failure
	}{
		"timeout": {
			in: &failurepb.Failure{FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType:          enumspb.TIMEOUT_TYPE_HEARTBEAT,
				LastHeartbeatDetails: toPayloads(&apiv1.Payload{Data: []byte("7")}),
			}}},
			want: &apiv1.Failure{Reason: "cadenceInternal:Timeout HEARTBEAT", Details: []byte("7")},
		},
		"canceled": {
			in:   &failurepb.Failure{FailureInfo: &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{}}},
			want: &apiv1.Failure{Reason: "cadenceInternal:Canceled"},
		},
		"server": {
			in:   &failurepb.Failure{Message: "workflow task failed", FailureInfo: &failurepb.Failure_ServerFailureInfo{}},
			want: &apiv1.Failure{Reason: "cadenceInternal:Generic", Details: []byte("workflow task failed")},
		},
		"wrapped activity failure unwraps to its cause": {
			in: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ActivityFailureInfo{ActivityFailureInfo: &failurepb.ActivityFailureInfo{}},
				Cause:       toFailure(&apiv1.Failure{Reason: "r"}),
			},
			want: &apiv1.Failure{Reason: "r"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, fromFailure(tt.in))
		})
	}
}

func TestAttemptsShiftBetweenZeroAndOneBased(t *testing.T) {
	t.Parallel()

	require.Equal(t, int32(1), toAttempt(0))
	require.Equal(t, int32(0), fromAttempt(1))
	require.Equal(t, int32(2), fromAttempt(3))
	require.Equal(t, int32(0), fromAttempt(0), "an unset Temporal attempt is the first")
}

func TestActivityTimeouts(t *testing.T) {
	t.Parallel()

	t.Run("without a retry policy every timeout is kept", func(t *testing.T) {
		t.Parallel()

		stc, sts, stcl := activityTimeouts(gogoDur(time.Minute), gogoDur(10*time.Second), gogoDur(30*time.Second), nil)
		require.Equal(t, time.Minute, stc.AsDuration())
		require.Equal(t, 10*time.Second, sts.AsDuration())
		require.Equal(t, 30*time.Second, stcl.AsDuration())
	})

	t.Run("with a retry policy the expiration bounds all attempts", func(t *testing.T) {
		t.Parallel()

		retry := &apiv1.RetryPolicy{ExpirationInterval: gogoDur(time.Hour)}
		stc, sts, stcl := activityTimeouts(gogoDur(time.Minute), gogoDur(2*time.Minute), nil, retry)
		require.Equal(t, time.Hour, stc.AsDuration())
		require.Equal(t, time.Minute, sts.AsDuration(), "capped by the per-attempt schedule-to-close")
		require.Equal(t, time.Minute, stcl.AsDuration(), "an absent start-to-close takes the per-attempt bound")
	})
}

func TestWorkflowTimeouts(t *testing.T) {
	t.Parallel()

	retry := &apiv1.RetryPolicy{ExpirationInterval: gogoDur(time.Hour)}

	exec, run := workflowTimeouts(gogoDur(time.Minute), retry, "")
	require.Equal(t, time.Hour, exec.AsDuration())
	require.Equal(t, time.Minute, run.AsDuration())

	exec, _ = workflowTimeouts(gogoDur(time.Minute), retry, "@every 1m")
	require.Nil(t, exec, "an execution timeout would also end the cron schedule")
}

func TestActivityWithoutRetryPolicyGetsOneAttempt(t *testing.T) {
	t.Parallel()

	require.Equal(t, int32(1), activityRetryPolicy(nil).MaximumAttempts)
	require.Equal(t, []string{"r"}, activityRetryPolicy(&apiv1.RetryPolicy{NonRetryableErrorReasons: []string{"r"}}).NonRetryableErrorTypes)
}

func TestControlRoundTrips(t *testing.T) {
	t.Parallel()

	for _, b := range [][]byte{nil, []byte("plain"), {0xff, 0xfe}, []byte(controlPrefix + "looks encoded")} {
		require.Equal(t, b, fromControl(toControl(b)))
	}

	require.Equal(t, "plain", toControl([]byte("plain")), "readable control stays readable")
}

func TestMarkerDetailsRoundTrip(t *testing.T) {
	t.Parallel()

	in := &apiv1.Payload{Data: []byte("v2")}
	require.Equal(t, in, fromMarkerDetails(toMarkerDetails(in)))
	require.Nil(t, toMarkerDetails(nil))
}

func TestChangeVersionSearchAttributeIsRenamed(t *testing.T) {
	t.Parallel()

	in := &apiv1.SearchAttributes{IndexedFields: map[string]*apiv1.Payload{
		cadenceChangeVersion: {Data: []byte(`["c-1"]`)},
		"CustomIntField":     {Data: []byte("1")},
	}}

	out := toSearchAttributes(in)
	require.Contains(t, out.IndexedFields, temporalChangeVersion)
	require.NotContains(t, out.IndexedFields, cadenceChangeVersion)
	require.Equal(t, in, fromSearchAttributes(out))
}

func TestTaskTokenRoundTrip(t *testing.T) {
	t.Parallel()

	in := &taskToken{Temporal: []byte{1, 2, 3}, Namespace: "ns", WorkflowID: "w", RunID: "r", TaskQueue: "tq", ActivityID: "a"}

	got, err := decodeToken(in.encode())
	require.NoError(t, err)
	require.Equal(t, in, got)
}

func TestForeignTaskTokensAreRejected(t *testing.T) {
	t.Parallel()

	for _, b := range [][]byte{nil, []byte("temporal-token"), tokenMagic, append(append([]byte{}, tokenMagic...), 0xff)} {
		_, err := decodeToken(b)
		require.ErrorIs(t, err, errBadToken)
	}

	_, err := decodeToken((&taskToken{Temporal: []byte{1}}).encode())
	require.ErrorIs(t, err, errBadToken, "a token must name its Namespace")
}

func TestRewriteQuery(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		`WorkflowID = "abc"`:                                             `WorkflowId = 'abc'`,
		`RunID = 'r' AND WorkflowType = "t"`:                             `RunId = 'r' AND WorkflowType = 't'`,
		`TaskList = 'tl'`:                                                `TaskQueue = 'tl'`,
		`CloseTime = missing`:                                            `ExecutionStatus = 'Running'`,
		`CloseTime != missing`:                                           `ExecutionStatus != 'Running'`,
		`CloseStatus = 0`:                                                `ExecutionStatus = 'Completed'`,
		`CloseStatus = "TIMED_OUT"`:                                      `ExecutionStatus = 'TimedOut'`,
		`CloseStatus = continued_as_new`:                                 `ExecutionStatus = 'ContinuedAsNew'`,
		`StartTime > 1700000000000000000`:                                `StartTime > '2023-11-14T22:13:20Z'`,
		`StartTime BETWEEN 0 AND 1000000000`:                             `StartTime BETWEEN '1970-01-01T00:00:00Z' AND '1970-01-01T00:00:01Z'`,
		`CustomIntField = 42 AND CustomKeywordField = 'k'`:               `CustomIntField = 42 AND CustomKeywordField = 'k'`,
		`CustomIntField > -5`:                                            `CustomIntField > -5`,
		`CadenceChangeVersion = 'c-1'`:                                   `TemporalChangeVersion = 'c-1'`,
		`WorkflowType = 'it''s'`:                                         `WorkflowType = 'it''s'`,
		`(WorkflowID = 'a' OR WorkflowID = 'b') ORDER BY StartTime DESC`: `(WorkflowId = 'a' OR WorkflowId = 'b') ORDER BY StartTime DESC`,
		``: ``,
	}

	for in, want := range tests {
		got, err := rewriteQuery(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}

	_, err := rewriteQuery(`WorkflowID = 'unterminated`)
	require.Error(t, err)
}

func TestErrorsCarryTheDetailsCadenceClientsCheck(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		in      error
		code    yarpcerrors.Code
		details any
	}{
		"not found": {
			in: serviceerror.NewNotFound("gone"), code: yarpcerrors.CodeNotFound, details: &apiv1.EntityNotExistsError{},
		},
		"already started": {
			in:      serviceerror.NewWorkflowExecutionAlreadyStarted("dup", "req", "run"),
			code:    yarpcerrors.CodeAlreadyExists,
			details: &apiv1.WorkflowExecutionAlreadyStartedError{StartRequestId: "req", RunId: "run"},
		},
		"namespace exists": {
			in: serviceerror.NewNamespaceAlreadyExists("x"), code: yarpcerrors.CodeAlreadyExists, details: &apiv1.DomainAlreadyExistsError{},
		},
		"query failed": {
			in: serviceerror.NewQueryFailed("x"), code: yarpcerrors.CodeInvalidArgument, details: &apiv1.QueryFailedError{},
		},
		"busy": {
			in:      serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "slow down"),
			code:    yarpcerrors.CodeResourceExhausted,
			details: &apiv1.ServiceBusyError{Reason: "RpsLimit"},
		},
		"bad request": {in: serviceerror.NewInvalidArgument("x"), code: yarpcerrors.CodeInvalidArgument},
		"deadline":    {in: context.DeadlineExceeded, code: yarpcerrors.CodeDeadlineExceeded},
		"bad token":   {in: errBadToken, code: yarpcerrors.CodeInvalidArgument},
		"other":       {in: errors.New("x"), code: yarpcerrors.CodeInternal},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := toCadenceError(serviceerror.ToStatus(tt.in).Err())
			if _, ok := tt.in.(serviceerror.ServiceError); !ok {
				err = toCadenceError(tt.in)
			}

			require.Equal(t, tt.code, yarpcerrors.FromError(err).Code())

			if tt.details != nil {
				got := protobuf.GetErrorDetails(err)
				require.Len(t, got, 1)
				require.Equal(t, tt.details, got[0])
			}
		})
	}
}

func TestCancelRequestedResolvesTheActivityID(t *testing.T) {
	t.Parallel()

	scheduled := &historypb.HistoryEvent{
		EventId:   5,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{ActivityId: "in-page"},
		},
	}
	cancel := func(id int64) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{
			EventId:   id + 10,
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{
				ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{ScheduledEventId: id},
			},
		}
	}

	var resolved []int64
	conv := newHistoryConverter("ns", func(_ context.Context, id int64) (string, error) {
		resolved = append(resolved, id)
		return "from-history", nil
	})

	out, err := conv.events(t.Context(), []*historypb.HistoryEvent{scheduled, cancel(5), cancel(3)})
	require.NoError(t, err)
	require.Len(t, out, 3)
	require.Equal(t, "in-page", out[1].GetActivityTaskCancelRequestedEventAttributes().ActivityId)
	require.Equal(t, "from-history", out[2].GetActivityTaskCancelRequestedEventAttributes().ActivityId)
	require.Equal(t, []int64{3}, resolved, "only an event missing from the page is looked up")
}

func TestEventsWithoutACadenceCounterpartAreDropped(t *testing.T) {
	t.Parallel()

	out, err := newHistoryConverter("ns", nil).events(t.Context(), []*historypb.HistoryEvent{{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{
			WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{},
		},
	}})
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestDecisions(t *testing.T) {
	t.Parallel()

	resolve := func(_ context.Context, id string) (int64, bool, error) {
		return 9, id == "pending", nil
	}

	cmds, err := toCommands(t.Context(), []*apiv1.Decision{
		{Attributes: &apiv1.Decision_RequestCancelActivityTaskDecisionAttributes{
			RequestCancelActivityTaskDecisionAttributes: &apiv1.RequestCancelActivityTaskDecisionAttributes{ActivityId: "pending"},
		}},
		{Attributes: &apiv1.Decision_RequestCancelActivityTaskDecisionAttributes{
			RequestCancelActivityTaskDecisionAttributes: &apiv1.RequestCancelActivityTaskDecisionAttributes{ActivityId: "closed"},
		}},
		{Attributes: &apiv1.Decision_StartTimerDecisionAttributes{
			StartTimerDecisionAttributes: &apiv1.StartTimerDecisionAttributes{TimerId: "t", StartToFireTimeout: gogoDur(time.Second)},
		}},
	}, resolve)
	require.NoError(t, err)
	require.Len(t, cmds, 2, "cancelling an Activity that already closed is dropped")
	require.Equal(t, int64(9), cmds[0].GetRequestCancelActivityTaskCommandAttributes().ScheduledEventId)
	require.Equal(t, durationpb.New(time.Second), cmds[1].GetStartTimerCommandAttributes().StartToFireTimeout)

	_, err = toCommands(t.Context(), []*apiv1.Decision{{Attributes: &apiv1.Decision_AcquireSemaphoreDecisionAttributes{
		AcquireSemaphoreDecisionAttributes: &apiv1.AcquireSemaphoreDecisionAttributes{},
	}}}, resolve)
	require.Equal(t, yarpcerrors.CodeUnimplemented, yarpcerrors.FromError(err).Code())
}

func TestEventsWithoutANamespaceNameTheWorkflowsOwn(t *testing.T) {
	t.Parallel()

	signal := func(ns string) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{
			EventId:   5,
			EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
			Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
				SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
					Namespace: ns,
				},
			},
		}
	}

	out, err := newHistoryConverter("orders", nil).events(t.Context(), []*historypb.HistoryEvent{signal(""), signal("billing")})
	require.NoError(t, err)
	require.Equal(t, "orders", out[0].GetSignalExternalWorkflowExecutionInitiatedEventAttributes().Domain,
		"Cadence replay compares this with the decision's Domain")
	require.Equal(t, "billing", out[1].GetSignalExternalWorkflowExecutionInitiatedEventAttributes().Domain)
}
