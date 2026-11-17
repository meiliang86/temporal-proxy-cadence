package cadence_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
)

// The Workflows and Activities below are ordinary Cadence code, written
// against the Cadence Go SDK exactly as an application would write them.

// ---- shared state between Activities and the test that drives them ----

var (
	// attempts counts Activity invocations by key.
	attempts   sync.Map // string -> *int32 (guarded by attemptsMu)
	attemptsMu sync.Mutex

	// tokens holds the task tokens of Activities completed asynchronously.
	tokens sync.Map // workflow ID -> []byte

	// cleanups records Activities run while a Workflow was being cancelled.
	cleanups sync.Map // workflow ID -> bool

	// cronRuns records each cron run's count.
	cronRuns sync.Map // workflow ID -> int
)

func bump(key string) int32 {
	attemptsMu.Lock()
	defer attemptsMu.Unlock()

	v, _ := attempts.LoadOrStore(key, new(int32))
	p := v.(*int32)
	*p++

	return *p
}

func count(key string) int32 {
	attemptsMu.Lock()
	defer attemptsMu.Unlock()

	if v, ok := attempts.Load(key); ok {
		return *v.(*int32)
	}

	return 0
}

// ---- context propagation ----

type tenantKey struct{}

const tenantHeader = "tenant"

// propagator carries a tenant from the starting context through the Workflow
// into its Activities, using Cadence headers.
type propagator struct{}

func (propagator) Inject(ctx context.Context, w workflow.HeaderWriter) error {
	if v, ok := ctx.Value(tenantKey{}).(string); ok {
		w.Set(tenantHeader, []byte(v))
	}

	return nil
}

func (propagator) Extract(ctx context.Context, r workflow.HeaderReader) (context.Context, error) {
	err := r.ForEachKey(func(k string, v []byte) error {
		if k == tenantHeader {
			ctx = context.WithValue(ctx, tenantKey{}, string(v))
		}

		return nil
	})

	return ctx, err
}

func (propagator) InjectFromWorkflow(ctx workflow.Context, w workflow.HeaderWriter) error {
	if v, ok := ctx.Value(tenantKey{}).(string); ok {
		w.Set(tenantHeader, []byte(v))
	}

	return nil
}

func (propagator) ExtractToWorkflow(ctx workflow.Context, r workflow.HeaderReader) (workflow.Context, error) {
	err := r.ForEachKey(func(k string, v []byte) error {
		if k == tenantHeader {
			ctx = workflow.WithValue(ctx, tenantKey{}, string(v))
		}

		return nil
	})

	return ctx, err
}

// ---- helpers ----

func activityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	})
}

func fastRetry(maxAttempts int32, nonRetriable ...string) *cadence.RetryPolicy {
	return &cadence.RetryPolicy{
		InitialInterval:          100 * time.Millisecond,
		BackoffCoefficient:       1,
		MaximumInterval:          time.Second,
		ExpirationInterval:       time.Minute,
		MaximumAttempts:          maxAttempts,
		NonRetriableErrorReasons: nonRetriable,
	}
}

// ---- Activities ----

func greetActivity(_ context.Context, name string) (string, error) {
	return "Hello, " + name + "!", nil
}

type order struct {
	ID    string
	Items []string
	Total float64
}

func orderActivity(_ context.Context, o order, discount float64, note string) (order, error) {
	o.Total *= 1 - discount
	o.Items = append(o.Items, note)

	return o, nil
}

// flakyActivity fails until its attempt (0-based, as Cadence counts) reaches
// failures, then reports the attempt it succeeded on.
func flakyActivity(ctx context.Context, failures int32) (int32, error) {
	attempt := activity.GetInfo(ctx).Attempt
	if attempt < failures {
		return 0, fmt.Errorf("transient failure on attempt %d", attempt)
	}

	return attempt, nil
}

func badInputActivity(ctx context.Context) error {
	bump(activity.GetInfo(ctx).WorkflowExecution.ID)
	return cadence.NewCustomError("bad-input", "field x is required")
}

func boomActivity(ctx context.Context) error {
	bump(activity.GetInfo(ctx).WorkflowExecution.ID)
	return errors.New("boom")
}

func slowActivity(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// heartbeatActivity heartbeats progress on its first attempt and then stalls
// past its heartbeat timeout; the retry resumes from the recorded progress.
func heartbeatActivity(ctx context.Context) (int, error) {
	if activity.HasHeartbeatDetails(ctx) {
		var progress int
		if err := activity.GetHeartbeatDetails(ctx, &progress); err != nil {
			return 0, err
		}

		return progress, nil
	}

	for i := 1; i <= 3; i++ {
		activity.RecordHeartbeat(ctx, i)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(10 * time.Second)

	return 0, errors.New("should have timed out")
}

func stallActivity(ctx context.Context) error {
	activity.RecordHeartbeat(ctx, "last words")
	time.Sleep(10 * time.Second)

	return nil
}

// cancellableActivity heartbeats until it is cancelled.
func cancellableActivity(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
			activity.RecordHeartbeat(ctx)
		}
	}
}

func cleanupActivity(ctx context.Context) error {
	cleanups.Store(activity.GetInfo(ctx).WorkflowExecution.ID, true)
	return nil
}

func asyncActivity(ctx context.Context) (string, error) {
	info := activity.GetInfo(ctx)
	if info.ActivityID != "by-id" {
		tokens.Store(info.WorkflowExecution.ID, info.TaskToken)
	}

	return "", activity.ErrResultPending
}

func tenantActivity(ctx context.Context) (string, error) {
	v, _ := ctx.Value(tenantKey{}).(string)
	return v, nil
}

func sessionActivity(ctx context.Context) (string, error) {
	return activity.GetInfo(ctx).TaskList, nil
}

func recordCronActivity(ctx context.Context, n int) error {
	cronRuns.Store(activity.GetInfo(ctx).WorkflowExecution.ID, n)
	return nil
}

// ---- Workflows ----

func greetWorkflow(ctx workflow.Context, name string) (string, error) {
	var out string
	err := workflow.ExecuteActivity(activityCtx(ctx), greetActivity, name).Get(ctx, &out)

	return out, err
}

func orderWorkflow(ctx workflow.Context, o order, discount float64) (order, error) {
	var out order
	err := workflow.ExecuteActivity(activityCtx(ctx), orderActivity, o, discount, "gift-wrap").Get(ctx, &out)

	return out, err
}

func retryWorkflow(ctx workflow.Context, failures int32) (int32, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		RetryPolicy:            fastRetry(10),
	})

	var attempt int32
	err := workflow.ExecuteActivity(ctx, flakyActivity, failures).Get(ctx, &attempt)

	return attempt, err
}

// nonRetryableWorkflow lets the Activity's custom error escape unchanged.
func nonRetryableWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		RetryPolicy:            fastRetry(10, "bad-input"),
	})

	return workflow.ExecuteActivity(ctx, badInputActivity).Get(ctx, nil)
}

// noRetryWorkflow schedules a failing Activity without a retry policy, which
// in Cadence means a single attempt.
func noRetryWorkflow(ctx workflow.Context) (string, error) {
	err := workflow.ExecuteActivity(activityCtx(ctx), boomActivity).Get(ctx, nil)

	if generic, ok := errors.AsType[*workflow.GenericError](err); ok {
		return generic.Error(), nil
	}

	return "", fmt.Errorf("expected a generic error, got %T: %w", err, err)
}

func timeoutWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Second,
	})

	err := workflow.ExecuteActivity(ctx, slowActivity, 10*time.Second).Get(ctx, nil)

	if te, ok := errors.AsType[*workflow.TimeoutError](err); ok {
		return te.TimeoutType().String(), nil
	}

	return "", fmt.Errorf("expected a timeout, got %T: %w", err, err)
}

func heartbeatWorkflow(ctx workflow.Context) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       time.Second,
		RetryPolicy:            fastRetry(3),
	})

	var progress int
	err := workflow.ExecuteActivity(ctx, heartbeatActivity).Get(ctx, &progress)

	return progress, err
}

// heartbeatTimeoutWorkflow reports the timeout type and the last heartbeat
// details of an Activity that stopped heartbeating.
func heartbeatTimeoutWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       time.Second,
	})

	err := workflow.ExecuteActivity(ctx, stallActivity).Get(ctx, nil)

	var te *workflow.TimeoutError
	if !errors.As(err, &te) {
		return "", fmt.Errorf("expected a timeout, got %T: %w", err, err)
	}

	var details string
	if te.HasDetails() {
		if err := te.Details(&details); err != nil {
			return "", err
		}
	}

	return te.TimeoutType().String() + "/" + details, nil
}

func cancelActivityWorkflow(ctx workflow.Context) (string, error) {
	actCtx, cancel := workflow.WithCancel(ctx)
	actCtx = workflow.WithActivityOptions(actCtx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       5 * time.Second,
		WaitForCancellation:    true,
	})

	f := workflow.ExecuteActivity(actCtx, cancellableActivity)

	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}

	cancel()

	err := f.Get(ctx, nil)
	if cadence.IsCanceledError(err) {
		return "activity canceled", nil
	}

	return "", fmt.Errorf("expected cancellation, got %T: %w", err, err)
}

func timerWorkflow(ctx workflow.Context) (string, error) {
	start := workflow.Now(ctx)
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}

	slept := workflow.Now(ctx).Sub(start)

	tctx, cancel := workflow.WithCancel(ctx)
	timer := workflow.NewTimer(tctx, time.Hour)

	_ = workflow.Sleep(ctx, 100*time.Millisecond)
	cancel()

	if err := timer.Get(ctx, nil); !cadence.IsCanceledError(err) {
		return "", fmt.Errorf("expected the timer to be canceled, got %v", err)
	}

	return fmt.Sprintf("slept>=1s:%v timer-canceled", slept >= time.Second), nil
}

func signalSumWorkflow(ctx workflow.Context, n int) (int, error) {
	ch := workflow.GetSignalChannel(ctx, "add")
	sum := 0

	for range n {
		var v int
		ch.Receive(ctx, &v)
		sum += v
	}

	return sum, nil
}

func queryWorkflow(ctx workflow.Context) (string, error) {
	state := "waiting"

	if err := workflow.SetQueryHandler(ctx, "state", func(prefix string) (string, error) {
		return prefix + state, nil
	}); err != nil {
		return "", err
	}

	workflow.GetSignalChannel(ctx, "done").Receive(ctx, nil)
	state = "finished"

	return state, nil
}

func childWorkflow(ctx workflow.Context, name string, fail bool) (string, error) {
	if fail {
		return "", cadence.NewCustomError("child-failed", name)
	}

	return greetWorkflow(ctx, name)
}

func parentWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		ExecutionStartToCloseTimeout: time.Minute,
		TaskStartToCloseTimeout:      10 * time.Second,
	})

	var ok string
	if err := workflow.ExecuteChildWorkflow(ctx, childWorkflow, "child", false).Get(ctx, &ok); err != nil {
		return "", err
	}

	err := workflow.ExecuteChildWorkflow(ctx, childWorkflow, "second", true).Get(ctx, nil)

	var ce *cadence.CustomError
	if !errors.As(err, &ce) {
		return "", fmt.Errorf("expected the child's custom error, got %T: %w", err, err)
	}

	var details string
	_ = ce.Details(&details)

	return ok + " | " + ce.Reason() + ":" + details, nil
}

func continueAsNewWorkflow(ctx workflow.Context, n, acc int) (int, error) {
	if n == 0 {
		return acc, nil
	}

	return 0, workflow.NewContinueAsNewError(ctx, continueAsNewWorkflow, n-1, acc+n)
}

// cancelWorkflow waits to be cancelled, then runs a cleanup Activity on a
// disconnected context, the way Cadence code handles cancellation.
func cancelWorkflow(ctx workflow.Context) error {
	err := workflow.Sleep(ctx, time.Hour)
	if cadence.IsCanceledError(err) {
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		if cerr := workflow.ExecuteActivity(activityCtx(dctx), cleanupActivity).Get(dctx, nil); cerr != nil {
			return cerr
		}
	}

	return err
}

func sideEffectWorkflow(ctx workflow.Context) (string, error) {
	var first, again string

	if err := workflow.SideEffect(ctx, func(workflow.Context) any {
		return fmt.Sprintf("random-%d", time.Now().UnixNano())
	}).Get(&first); err != nil {
		return "", err
	}

	if err := workflow.MutableSideEffect(ctx, "config", func(workflow.Context) any {
		return "v1"
	}, func(a, b any) bool { return a == b }).Get(&again); err != nil {
		return "", err
	}

	// A timer forces a second decision task, so the markers are replayed.
	if err := workflow.Sleep(ctx, 100*time.Millisecond); err != nil {
		return "", err
	}

	return first + "/" + again, nil
}

func versionWorkflow(ctx workflow.Context) (string, error) {
	v := workflow.GetVersion(ctx, "greeting-change", workflow.DefaultVersion, 2)

	name := "old"
	if v == 2 {
		name = "new"
	}

	return greetWorkflow(ctx, fmt.Sprintf("%s-v%d", name, v))
}

func localActivityWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{ScheduleToCloseTimeout: 10 * time.Second})

	var out string
	err := workflow.ExecuteLocalActivity(ctx, greetActivity, "local").Get(ctx, &out)

	return out, err
}

func cronWorkflow(ctx workflow.Context) (int, error) {
	n := 1
	if workflow.HasLastCompletionResult(ctx) {
		var last int
		if err := workflow.GetLastCompletionResult(ctx, &last); err != nil {
			return 0, err
		}

		n = last + 1
	}

	if err := workflow.ExecuteActivity(activityCtx(ctx), recordCronActivity, n).Get(ctx, nil); err != nil {
		return 0, err
	}

	return n, nil
}

// searchAttributesWorkflow upserts a search attribute and waits to be
// signalled, so the test can find it through visibility while it runs.
func searchAttributesWorkflow(ctx workflow.Context, keyword string) (string, error) {
	if err := workflow.UpsertSearchAttributes(ctx, map[string]any{"CustomKeywordField": keyword}); err != nil {
		return "", err
	}

	workflow.GetSignalChannel(ctx, "done").Receive(ctx, nil)

	return keyword, nil
}

func resetWorkflow(ctx workflow.Context) (string, error) {
	var greeting string
	if err := workflow.ExecuteActivity(activityCtx(ctx), greetActivity, "reset").Get(ctx, &greeting); err != nil {
		return "", err
	}

	var suffix string
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, &suffix)

	return greeting + suffix, nil
}

func asyncCompletionWorkflow(ctx workflow.Context) (string, error) {
	var byToken, byID string
	if err := workflow.ExecuteActivity(activityCtx(ctx), asyncActivity).Get(ctx, &byToken); err != nil {
		return "", err
	}

	idCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "by-id",
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	})
	if err := workflow.ExecuteActivity(idCtx, asyncActivity).Get(ctx, &byID); err != nil {
		return "", err
	}

	return byToken + "+" + byID, nil
}

// externalWorkflow signals one Workflow and cancels another by ID.
func externalWorkflow(ctx workflow.Context, signalID, cancelID string) error {
	if err := workflow.SignalExternalWorkflow(ctx, signalID, "", "add", 5).Get(ctx, nil); err != nil {
		return err
	}

	return workflow.RequestCancelExternalWorkflow(ctx, cancelID, "").Get(ctx, nil)
}

func sessionWorkflow(ctx workflow.Context) (string, error) {
	sctx, err := workflow.CreateSession(ctx, &workflow.SessionOptions{
		CreationTimeout:  time.Minute,
		ExecutionTimeout: time.Minute,
	})
	if err != nil {
		return "", err
	}
	defer workflow.CompleteSession(sctx)

	var a, b string
	if err := workflow.ExecuteActivity(activityCtx(sctx), sessionActivity).Get(sctx, &a); err != nil {
		return "", err
	}

	if err := workflow.ExecuteActivity(activityCtx(sctx), sessionActivity).Get(sctx, &b); err != nil {
		return "", err
	}

	return fmt.Sprintf("same-host:%v", a == b), nil
}

func tenantWorkflow(ctx workflow.Context) (string, error) {
	fromWorkflow, _ := ctx.Value(tenantKey{}).(string)

	var fromActivity string
	if err := workflow.ExecuteActivity(activityCtx(ctx), tenantActivity).Get(ctx, &fromActivity); err != nil {
		return "", err
	}

	return fromWorkflow + "/" + fromActivity, nil
}

// workflowRetryWorkflow fails its first attempt; the Workflow retry policy
// runs it again.
func workflowRetryWorkflow(ctx workflow.Context) (int32, error) {
	attempt := workflow.GetInfo(ctx).Attempt
	if attempt == 0 {
		return 0, cadence.NewCustomError("first-attempt")
	}

	return attempt, nil
}

func failingWorkflow(workflow.Context) error {
	return cadence.NewCustomError("order-rejected", "out of stock", 3)
}

// longWorkflow does a little of everything across many decision tasks, for a
// Worker without sticky execution, which replays the full history each time.
func longWorkflow(ctx workflow.Context) (string, error) {
	out := ""
	for i := range 3 {
		var s string
		if err := workflow.ExecuteActivity(activityCtx(ctx), greetActivity, fmt.Sprint(i)).Get(ctx, &s); err != nil {
			return "", err
		}

		out += s

		if err := workflow.Sleep(ctx, 50*time.Millisecond); err != nil {
			return "", err
		}
	}

	var sig string
	workflow.GetSignalChannel(ctx, "go").Receive(ctx, &sig)

	return out + sig, nil
}

var (
	allWorkflows = []any{
		greetWorkflow, orderWorkflow, retryWorkflow, nonRetryableWorkflow, noRetryWorkflow,
		timeoutWorkflow, heartbeatWorkflow, heartbeatTimeoutWorkflow, cancelActivityWorkflow,
		timerWorkflow, signalSumWorkflow, queryWorkflow, childWorkflow, parentWorkflow,
		continueAsNewWorkflow, cancelWorkflow, sideEffectWorkflow, versionWorkflow,
		localActivityWorkflow, cronWorkflow, searchAttributesWorkflow, resetWorkflow,
		asyncCompletionWorkflow, externalWorkflow, sessionWorkflow, tenantWorkflow,
		workflowRetryWorkflow, failingWorkflow, longWorkflow,
	}
	allActivities = []any{
		greetActivity, orderActivity, flakyActivity, badInputActivity, boomActivity,
		slowActivity, heartbeatActivity, stallActivity, cancellableActivity, cleanupActivity,
		asyncActivity, tenantActivity, sessionActivity, recordCronActivity,
	}
)

func registerAll(w worker.Worker) {
	for _, wf := range allWorkflows {
		w.RegisterWorkflow(wf)
	}

	for _, a := range allActivities {
		w.RegisterActivity(a)
	}
}

func registerReplayer(r worker.WorkflowReplayer) {
	for _, wf := range allWorkflows {
		r.RegisterWorkflow(wf)
	}
}
