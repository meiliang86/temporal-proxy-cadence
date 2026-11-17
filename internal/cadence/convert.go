package cadence

import (
	"bytes"
	"time"

	gogo "github.com/gogo/protobuf/types"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	metadataEncoding = "encoding"

	// encodingBinary labels a Cadence payload stored on Temporal. A Cadence
	// payload is an opaque blob whose layout belongs to the Worker's data
	// converter, so it is never reinterpreted, only carried.
	encodingBinary = "binary/plain"
	encodingJSON   = "json/plain"
	encodingNull   = "binary/null"

	// markerDetailsKey is where a Cadence marker's single details blob lives in
	// a Temporal marker's details map.
	markerDetailsKey = "data"

	// failureSource marks failures this frontend built from a Cadence reason.
	failureSource = "CadenceProxy"
)

// --- time ---

func toDuration(d *gogo.Duration) *durationpb.Duration {
	if d == nil {
		return nil
	}

	return &durationpb.Duration{Seconds: d.Seconds, Nanos: d.Nanos}
}

func fromDuration(d *durationpb.Duration) *gogo.Duration {
	if d == nil {
		return nil
	}

	return &gogo.Duration{Seconds: d.Seconds, Nanos: d.Nanos}
}

func goDuration(d *gogo.Duration) time.Duration {
	if d == nil {
		return 0
	}

	return time.Duration(d.Seconds)*time.Second + time.Duration(d.Nanos)
}

func durationOf(d time.Duration) *durationpb.Duration {
	if d <= 0 {
		return nil
	}

	return durationpb.New(d)
}

func toTimestamp(t *gogo.Timestamp) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}

	return &timestamppb.Timestamp{Seconds: t.Seconds, Nanos: t.Nanos}
}

func fromTimestamp(t *timestamppb.Timestamp) *gogo.Timestamp {
	if t == nil {
		return nil
	}

	return &gogo.Timestamp{Seconds: t.Seconds, Nanos: t.Nanos}
}

// --- payloads ---

// toPayloads stores a Cadence payload as a single Temporal payload. An absent
// or empty payload stays absent, which is how both systems say "no value".
func toPayloads(p *apiv1.Payload) *commonpb.Payloads {
	if p == nil || len(p.Data) == 0 {
		return nil
	}

	return &commonpb.Payloads{Payloads: []*commonpb.Payload{binaryPayload(p.Data)}}
}

func binaryPayload(data []byte) *commonpb.Payload {
	return &commonpb.Payload{
		Metadata: map[string][]byte{metadataEncoding: []byte(encodingBinary)},
		Data:     data,
	}
}

func jsonPayload(data []byte) *commonpb.Payload {
	return &commonpb.Payload{
		Metadata: map[string][]byte{metadataEncoding: []byte(encodingJSON)},
		Data:     data,
	}
}

// fromPayloads returns the Cadence blob behind Temporal payloads. A single
// payload is returned byte for byte, which is the round trip for everything a
// Cadence SDK wrote. Several payloads come from a Temporal SDK; json/plain ones
// are joined one per line, the layout the Cadence Go SDK's JSON data converter
// decodes multiple values from.
func fromPayloads(p *commonpb.Payloads) *apiv1.Payload {
	data := payloadsData(p)
	if data == nil {
		return nil
	}

	return &apiv1.Payload{Data: data}
}

func payloadsData(p *commonpb.Payloads) []byte {
	if p == nil || len(p.Payloads) == 0 {
		return nil
	}

	if len(p.Payloads) == 1 {
		return payloadData(p.Payloads[0])
	}

	var buf bytes.Buffer
	for _, pl := range p.Payloads {
		data := payloadData(pl)
		buf.Write(data)
		if string(pl.GetMetadata()[metadataEncoding]) == encodingJSON && !bytes.HasSuffix(data, []byte("\n")) {
			buf.WriteByte('\n')
		}
	}

	return buf.Bytes()
}

func payloadData(p *commonpb.Payload) []byte {
	if p == nil || string(p.GetMetadata()[metadataEncoding]) == encodingNull {
		return nil
	}

	return p.Data
}

func toPayloadMap(fields map[string]*apiv1.Payload, encode func([]byte) *commonpb.Payload) map[string]*commonpb.Payload {
	if len(fields) == 0 {
		return nil
	}

	out := make(map[string]*commonpb.Payload, len(fields))
	for k, v := range fields {
		out[k] = encode(v.GetData())
	}

	return out
}

func fromPayloadMap(fields map[string]*commonpb.Payload) map[string]*apiv1.Payload {
	if len(fields) == 0 {
		return nil
	}

	out := make(map[string]*apiv1.Payload, len(fields))
	for k, v := range fields {
		out[k] = &apiv1.Payload{Data: v.GetData()}
	}

	return out
}

func toHeader(h *apiv1.Header) *commonpb.Header {
	if h == nil || len(h.Fields) == 0 {
		return nil
	}

	return &commonpb.Header{Fields: toPayloadMap(h.Fields, binaryPayload)}
}

func fromHeader(h *commonpb.Header) *apiv1.Header {
	if h == nil || len(h.Fields) == 0 {
		return nil
	}

	return &apiv1.Header{Fields: fromPayloadMap(h.Fields)}
}

// toMemo labels memo values json/plain: both Cadence SDKs JSON-encode them, so
// the label lets Temporal tooling display them while the bytes stay untouched.
func toMemo(m *apiv1.Memo) *commonpb.Memo {
	if m == nil || len(m.Fields) == 0 {
		return nil
	}

	return &commonpb.Memo{Fields: toPayloadMap(m.Fields, jsonPayload)}
}

func fromMemo(m *commonpb.Memo) *apiv1.Memo {
	if m == nil || len(m.Fields) == 0 {
		return nil
	}

	return &apiv1.Memo{Fields: fromPayloadMap(m.Fields)}
}

// toSearchAttributes labels values json/plain. Cadence SDKs send search
// attribute values JSON-encoded, which is exactly what Temporal validates a
// json/plain search attribute against the registered type with.
//
// Cadence's change-version attribute is renamed onto Temporal's: the Cadence Go
// SDK's GetVersion upserts CadenceChangeVersion, which no Temporal Namespace
// defines, while every one defines TemporalChangeVersion with the same
// "changeID-version" keyword list.
func toSearchAttributes(s *apiv1.SearchAttributes) *commonpb.SearchAttributes {
	if s == nil || len(s.IndexedFields) == 0 {
		return nil
	}

	out := toPayloadMap(s.IndexedFields, jsonPayload)
	if v, ok := out[cadenceChangeVersion]; ok {
		delete(out, cadenceChangeVersion)
		out[temporalChangeVersion] = v
	}

	return &commonpb.SearchAttributes{IndexedFields: out}
}

func fromSearchAttributes(s *commonpb.SearchAttributes) *apiv1.SearchAttributes {
	if s == nil || len(s.IndexedFields) == 0 {
		return nil
	}

	out := fromPayloadMap(s.IndexedFields)
	if v, ok := out[temporalChangeVersion]; ok {
		delete(out, temporalChangeVersion)
		out[cadenceChangeVersion] = v
	}

	return &apiv1.SearchAttributes{IndexedFields: out}
}

const (
	cadenceChangeVersion  = "CadenceChangeVersion"
	temporalChangeVersion = "TemporalChangeVersion"
)

// --- identifiers ---

func toExecution(e *apiv1.WorkflowExecution) *commonpb.WorkflowExecution {
	if e == nil {
		return nil
	}

	return &commonpb.WorkflowExecution{WorkflowId: e.WorkflowId, RunId: e.RunId}
}

func fromExecution(e *commonpb.WorkflowExecution) *apiv1.WorkflowExecution {
	if e == nil {
		return nil
	}

	return &apiv1.WorkflowExecution{WorkflowId: e.WorkflowId, RunId: e.RunId}
}

func toWorkflowType(t *apiv1.WorkflowType) *commonpb.WorkflowType {
	if t == nil {
		return nil
	}

	return &commonpb.WorkflowType{Name: t.Name}
}

func fromWorkflowType(t *commonpb.WorkflowType) *apiv1.WorkflowType {
	if t == nil {
		return nil
	}

	return &apiv1.WorkflowType{Name: t.Name}
}

func toActivityType(t *apiv1.ActivityType) *commonpb.ActivityType {
	if t == nil {
		return nil
	}

	return &commonpb.ActivityType{Name: t.Name}
}

func fromActivityType(t *commonpb.ActivityType) *apiv1.ActivityType {
	if t == nil {
		return nil
	}

	return &apiv1.ActivityType{Name: t.Name}
}

// toTaskQueue converts a Task List. normalName is the Workflow's normal Task
// Queue, which Temporal wants alongside a sticky one; it may be empty.
func toTaskQueue(t *apiv1.TaskList, normalName string) *taskqueuepb.TaskQueue {
	if t == nil {
		return nil
	}

	kind := enumspb.TASK_QUEUE_KIND_NORMAL
	if t.Kind == apiv1.TaskListKind_TASK_LIST_KIND_STICKY {
		kind = enumspb.TASK_QUEUE_KIND_STICKY
	} else {
		normalName = ""
	}

	return &taskqueuepb.TaskQueue{Name: t.Name, Kind: kind, NormalName: normalName}
}

func fromTaskQueue(t *taskqueuepb.TaskQueue) *apiv1.TaskList {
	if t == nil {
		return nil
	}

	kind := apiv1.TaskListKind_TASK_LIST_KIND_NORMAL
	if t.Kind == enumspb.TASK_QUEUE_KIND_STICKY {
		kind = apiv1.TaskListKind_TASK_LIST_KIND_STICKY
	}

	return &apiv1.TaskList{Name: t.Name, Kind: kind}
}

// toAttempt and fromAttempt shift between Cadence's 0-based and Temporal's
// 1-based attempt counters.
func toAttempt(a int32) int32 { return a + 1 }

func fromAttempt(a int32) int32 {
	if a <= 0 {
		return 0
	}

	return a - 1
}

// --- retry policy ---

// toRetryPolicy maps a Cadence retry policy. Cadence's expiration interval has
// no counterpart on the policy itself; callers fold it into the timeout that
// bounds every attempt (see [activityTimeouts] and [workflowTimeouts]). Jitter
// has none at all and is dropped.
func toRetryPolicy(p *apiv1.RetryPolicy) *commonpb.RetryPolicy {
	if p == nil {
		return nil
	}

	return &commonpb.RetryPolicy{
		InitialInterval:        toDuration(p.InitialInterval),
		BackoffCoefficient:     p.BackoffCoefficient,
		MaximumInterval:        toDuration(p.MaximumInterval),
		MaximumAttempts:        p.MaximumAttempts,
		NonRetryableErrorTypes: p.NonRetryableErrorReasons,
	}
}

func fromRetryPolicy(p *commonpb.RetryPolicy, expiration *durationpb.Duration) *apiv1.RetryPolicy {
	if p == nil {
		return nil
	}

	return &apiv1.RetryPolicy{
		InitialInterval:          fromDuration(p.InitialInterval),
		BackoffCoefficient:       p.BackoffCoefficient,
		MaximumInterval:          fromDuration(p.MaximumInterval),
		MaximumAttempts:          p.MaximumAttempts,
		NonRetryableErrorReasons: p.NonRetryableErrorTypes,
		ExpirationInterval:       fromDuration(expiration),
	}
}

// activityTimeouts maps an Activity's timeouts. In Cadence every timeout,
// schedule-to-close included, applies to one attempt, and a retry policy's
// expiration interval bounds them all; in Temporal schedule-to-close bounds
// every attempt together. So with a retry policy, Temporal's schedule-to-close
// is the expiration interval (unbounded when there is none), and Cadence's
// per-attempt schedule-to-close caps the other two per-attempt timeouts.
func activityTimeouts(
	scheduleToClose, scheduleToStart, startToClose *gogo.Duration,
	retry *apiv1.RetryPolicy,
) (stc, sts, stcl *durationpb.Duration) {
	if retry == nil {
		return toDuration(scheduleToClose), toDuration(scheduleToStart), toDuration(startToClose)
	}

	perAttempt := goDuration(scheduleToClose)
	capped := func(d *gogo.Duration) *durationpb.Duration {
		v := goDuration(d)
		if perAttempt > 0 && (v <= 0 || v > perAttempt) {
			v = perAttempt
		}

		return durationOf(v)
	}

	return toDuration(retry.ExpirationInterval), capped(scheduleToStart), capped(startToClose)
}

// workflowTimeouts maps a Workflow's timeouts. Cadence's execution timeout is
// per run, which is Temporal's run timeout. A retry policy's expiration
// interval bounds every retry, which is Temporal's execution timeout, except
// under a cron schedule, where Temporal's execution timeout would also end the
// schedule.
func workflowTimeouts(execution *gogo.Duration, retry *apiv1.RetryPolicy, cron string) (exec, run *durationpb.Duration) {
	run = toDuration(execution)
	if retry != nil && cron == "" && goDuration(retry.ExpirationInterval) > 0 {
		exec = toDuration(retry.ExpirationInterval)
	}

	return exec, run
}

// --- failures ---

// toFailure carries a Cadence reason and details as an ApplicationFailure
// whose type is the reason. Temporal matches non-retryable error types against
// that type, so a Cadence retry policy's non-retryable reasons keep working.
func toFailure(f *apiv1.Failure) *failurepb.Failure {
	if f == nil {
		return nil
	}

	return &failurepb.Failure{
		Message: f.Reason,
		Source:  failureSource,
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:    f.Reason,
			Details: toPayloads(&apiv1.Payload{Data: f.Details}),
		}},
	}
}

// Reasons for failures the Temporal Service raised itself, spelled the way the
// Cadence Go SDK recognises them.
const (
	reasonTimeout  = "cadenceInternal:Timeout"
	reasonCanceled = "cadenceInternal:Canceled"
	reasonGeneric  = "cadenceInternal:Generic"
)

// fromFailure returns a Cadence reason and details for a Temporal failure. An
// ApplicationFailure round trips exactly. Anything else was raised by the
// Temporal Service or a Temporal SDK; its message becomes the reason.
func fromFailure(f *failurepb.Failure) *apiv1.Failure {
	if f == nil {
		return nil
	}

	switch info := f.FailureInfo.(type) {
	case *failurepb.Failure_ApplicationFailureInfo:
		reason := info.ApplicationFailureInfo.GetType()
		if reason == "" {
			reason = f.Message
		}

		return &apiv1.Failure{Reason: reason, Details: payloadsData(info.ApplicationFailureInfo.GetDetails())}
	case *failurepb.Failure_TimeoutFailureInfo:
		return &apiv1.Failure{
			Reason:  reasonTimeout + " " + timeoutTypeName(info.TimeoutFailureInfo.GetTimeoutType()),
			Details: payloadsData(info.TimeoutFailureInfo.GetLastHeartbeatDetails()),
		}
	case *failurepb.Failure_CanceledFailureInfo:
		return &apiv1.Failure{Reason: reasonCanceled, Details: payloadsData(info.CanceledFailureInfo.GetDetails())}
	case *failurepb.Failure_ChildWorkflowExecutionFailureInfo, *failurepb.Failure_ActivityFailureInfo:
		if f.Cause != nil {
			return fromFailure(f.Cause)
		}
	}

	return &apiv1.Failure{Reason: reasonGeneric, Details: []byte(f.Message)}
}

func timeoutTypeName(t enumspb.TimeoutType) string {
	switch t {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		return "START_TO_CLOSE"
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
		return "SCHEDULE_TO_START"
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		return "SCHEDULE_TO_CLOSE"
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		return "HEARTBEAT"
	default:
		return "UNSPECIFIED"
	}
}
