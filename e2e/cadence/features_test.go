package cadence_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Every case runs over both transports a Cadence SDK may use.

func TestCadenceOverGRPC(t *testing.T) {
	t.Parallel()
	runSuite(t, transportGRPC)
}

func TestCadenceOverTChannel(t *testing.T) {
	t.Parallel()
	runSuite(t, transportTChannel)
}

type suiteCase struct {
	name string
	fn   func(e *env)
}

var suite = []suiteCase{
	{"ActivityResult", testActivityResult},
	{"StructsAndMultipleArguments", testStructs},
	{"ActivityRetryCountsAttemptsFromZero", testActivityRetry},
	{"NonRetriableErrorReason", testNonRetriable},
	{"ActivityWithoutRetryPolicyRunsOnce", testNoRetryByDefault},
	{"ActivityStartToCloseTimeout", testActivityTimeout},
	{"HeartbeatDetailsSurviveRetry", testHeartbeatDetails},
	{"HeartbeatTimeout", testHeartbeatTimeout},
	{"ActivityCancellation", testActivityCancellation},
	{"Timers", testTimers},
	{"Signals", testSignals},
	{"SignalWithStart", testSignalWithStart},
	{"Queries", testQueries},
	{"ChildWorkflows", testChildWorkflows},
	{"ContinueAsNew", testContinueAsNew},
	{"WorkflowCancellation", testWorkflowCancellation},
	{"SideEffects", testSideEffects},
	{"Versioning", testVersioning},
	{"LocalActivity", testLocalActivity},
	{"CronSchedule", testCron},
	{"SearchAttributesMemoAndVisibility", testSearchAttributes},
	{"ListOpenClosedAndCount", testListFilters},
	{"Terminate", testTerminate},
	{"Reset", testReset},
	{"AsyncActivityCompletion", testAsyncCompletion},
	{"ExternalSignalAndCancel", testExternal},
	{"Sessions", testSessions},
	{"ContextPropagation", testContextPropagation},
	{"WorkflowRetryPolicy", testWorkflowRetry},
	{"WorkflowFailure", testWorkflowFailure},
	{"DelayedStart", testDelayStart},
	{"TypedErrors", testTypedErrors},
	{"Domains", testDomains},
	{"DescribeTaskList", testDescribeTaskList},
	{"NonStickyWorkerReplaysFullHistory", testNonSticky},
	{"PagedHistory", testPagedHistory},
}

func runSuite(t *testing.T, tr transport) {
	base := newEnv(t, tr)

	for _, c := range suite {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			e := *base
			e.t = t
			c.fn(&e)
		})
	}
}

func testActivityResult(e *env) {
	var out string
	run, err := e.run(&out, greetWorkflow, "Cadence")
	require.NoError(e.t, err)
	require.Equal(e.t, "Hello, Cadence!", out)
	e.replay(run)
}

func testStructs(e *env) {
	var out order
	run, err := e.run(&out, orderWorkflow, order{ID: "o-1", Items: []string{"book"}, Total: 100}, 0.25)
	require.NoError(e.t, err)
	require.Equal(e.t, order{ID: "o-1", Items: []string{"book", "gift-wrap"}, Total: 75}, out)
	e.replay(run)
}

func testActivityRetry(e *env) {
	var attempt int32
	run, err := e.run(&attempt, retryWorkflow, int32(2))
	require.NoError(e.t, err)
	require.Equal(e.t, int32(2), attempt, "the third attempt is attempt 2 in Cadence")
	e.replay(run)
}

func testNonRetriable(e *env) {
	run, err := e.run(nil, nonRetryableWorkflow)

	var ce *cadence.CustomError
	require.ErrorAs(e.t, err, &ce)
	require.Equal(e.t, "bad-input", ce.Reason())

	var details string
	require.NoError(e.t, ce.Details(&details))
	require.Equal(e.t, "field x is required", details)
	require.Equal(e.t, int32(1), count(run.GetID()), "a non-retriable reason must not be retried")
	e.replay(run)
}

func testNoRetryByDefault(e *env) {
	var msg string
	run, err := e.run(&msg, noRetryWorkflow)
	require.NoError(e.t, err)
	require.Equal(e.t, "boom", msg)
	require.Equal(e.t, int32(1), count(run.GetID()))
}

func testActivityTimeout(e *env) {
	var tt string
	run, err := e.run(&tt, timeoutWorkflow)
	require.NoError(e.t, err)
	require.Equal(e.t, shared.TimeoutTypeStartToClose.String(), tt)
	e.replay(run)
}

func testHeartbeatDetails(e *env) {
	var progress int
	run, err := e.run(&progress, heartbeatWorkflow)
	require.NoError(e.t, err)
	require.Equal(e.t, 3, progress)
	e.replay(run)
}

func testHeartbeatTimeout(e *env) {
	var out string
	_, err := e.run(&out, heartbeatTimeoutWorkflow)
	require.NoError(e.t, err)
	require.Equal(e.t, shared.TimeoutTypeHeartbeat.String()+"/last words", out)
}

func testActivityCancellation(e *env) {
	var out string
	run, err := e.run(&out, cancelActivityWorkflow)
	require.NoError(e.t, err)
	require.Equal(e.t, "activity canceled", out)
	e.replay(run)
}

func testTimers(e *env) {
	var out string
	run, err := e.run(&out, timerWorkflow)
	require.NoError(e.t, err)
	require.Equal(e.t, "slept>=1s:true timer-canceled", out)
	e.replay(run)
}

func testSignals(e *env) {
	run, err := e.client.ExecuteWorkflow(e.ctx(), e.options(), signalSumWorkflow, 3)
	require.NoError(e.t, err)

	for _, v := range []int{1, 2, 3} {
		require.NoError(e.t, e.client.SignalWorkflow(e.ctx(), run.GetID(), "", "add", v))
	}

	var sum int
	require.NoError(e.t, run.Get(e.ctx(), &sum))
	require.Equal(e.t, 6, sum)
	e.replay(run)
}

func testSignalWithStart(e *env) {
	opts := e.options()

	exec, err := e.client.SignalWithStartWorkflow(e.ctx(), opts.ID, "add", 10, opts, signalSumWorkflow, 2)
	require.NoError(e.t, err)

	// A second signal-with-start reaches the running Workflow.
	again, err := e.client.SignalWithStartWorkflow(e.ctx(), opts.ID, "add", 20, opts, signalSumWorkflow, 2)
	require.NoError(e.t, err)
	require.Equal(e.t, exec.RunID, again.RunID)

	var sum int
	require.NoError(e.t, e.client.GetWorkflow(e.ctx(), exec.ID, exec.RunID).Get(e.ctx(), &sum))
	require.Equal(e.t, 30, sum)
}

func testQueries(e *env) {
	run, err := e.client.ExecuteWorkflow(e.ctx(), e.options(), queryWorkflow)
	require.NoError(e.t, err)

	query := func() string {
		v, err := e.client.QueryWorkflow(e.ctx(), run.GetID(), "", "state", "state=")
		require.NoError(e.t, err)

		var s string
		require.NoError(e.t, v.Get(&s))

		return s
	}

	require.Equal(e.t, "state=waiting", query())

	trace, err := e.client.QueryWorkflow(e.ctx(), run.GetID(), "", "__stack_trace")
	require.NoError(e.t, err)

	var stack string
	require.NoError(e.t, trace.Get(&stack))
	require.Contains(e.t, stack, "queryWorkflow")

	require.NoError(e.t, e.client.SignalWorkflow(e.ctx(), run.GetID(), "", "done", nil))
	require.NoError(e.t, run.Get(e.ctx(), nil))

	// A closed Workflow is queried by replaying its full history.
	require.Equal(e.t, "state=finished", query())
	e.replay(run)
}

func testChildWorkflows(e *env) {
	var out string
	run, err := e.run(&out, parentWorkflow)
	require.NoError(e.t, err)
	require.Equal(e.t, "Hello, child! | child-failed:second", out)
	e.replay(run)
}

func testContinueAsNew(e *env) {
	var out int
	run, err := e.run(&out, continueAsNewWorkflow, 4, 0)
	require.NoError(e.t, err)
	require.Equal(e.t, 10, out)

	// The client followed the chain to its last run.
	desc, err := e.client.DescribeWorkflowExecution(e.ctx(), run.GetID(), "")
	require.NoError(e.t, err)
	require.Equal(e.t, shared.WorkflowExecutionCloseStatusCompleted, desc.WorkflowExecutionInfo.GetCloseStatus())
	require.NotEqual(e.t, run.GetRunID(), desc.WorkflowExecutionInfo.Execution.GetRunId())
}

func testWorkflowCancellation(e *env) {
	run, err := e.client.ExecuteWorkflow(e.ctx(), e.options(), cancelWorkflow)
	require.NoError(e.t, err)

	e.eventually(func() bool {
		desc, err := e.client.DescribeWorkflowExecution(e.ctx(), run.GetID(), "")
		return err == nil && desc.GetWorkflowExecutionInfo().GetHistoryLength() >= 5
	}, "the Workflow starts its timer")

	require.NoError(e.t, e.client.CancelWorkflow(e.ctx(), run.GetID(), ""))

	err = run.Get(e.ctx(), nil)
	require.True(e.t, cadence.IsCanceledError(err), "got %T: %v", err, err)

	_, cleaned := cleanups.Load(run.GetID())
	require.True(e.t, cleaned, "the cleanup Activity ran on the disconnected context")
	e.replay(run)
}

func testSideEffects(e *env) {
	var out string
	run, err := e.run(&out, sideEffectWorkflow)
	require.NoError(e.t, err)
	require.True(e.t, strings.HasPrefix(out, "random-"), out)
	require.True(e.t, strings.HasSuffix(out, "/v1"), out)
	e.replay(run)
}

func testVersioning(e *env) {
	var out string
	run, err := e.run(&out, versionWorkflow)
	require.NoError(e.t, err)
	require.Equal(e.t, "Hello, new-v2!", out)
	e.replay(run)
}

func testLocalActivity(e *env) {
	var out string
	run, err := e.run(&out, localActivityWorkflow)
	require.NoError(e.t, err)
	require.Equal(e.t, "Hello, local!", out)
	e.replay(run)
}

func testCron(e *env) {
	opts := e.options()
	opts.CronSchedule = "@every 1s"

	run, err := e.client.StartWorkflow(e.ctx(), opts, cronWorkflow)
	require.NoError(e.t, err)

	e.eventually(func() bool {
		n, ok := cronRuns.Load(run.ID)
		return ok && n.(int) >= 3
	}, "each cron run sees the last run's result")

	require.NoError(e.t, e.client.TerminateWorkflow(e.ctx(), run.ID, "", "done", nil))
}

func testSearchAttributes(e *env) {
	keyword := "kw-" + uuid.NewString()
	opts := e.options()
	opts.Memo = map[string]any{"owner": "alice"}
	opts.SearchAttributes = map[string]any{"CustomIntField": 42}

	run, err := e.client.ExecuteWorkflow(e.ctx(), opts, searchAttributesWorkflow, keyword)
	require.NoError(e.t, err)

	query := fmt.Sprintf("CustomKeywordField = '%s' AND CustomIntField = 42 AND CloseTime = missing", keyword)

	var found *shared.WorkflowExecutionInfo
	e.eventually(func() bool {
		resp, err := e.client.ListWorkflow(e.ctx(), &shared.ListWorkflowExecutionsRequest{Domain: new(domain), Query: &query})
		if err != nil || len(resp.Executions) != 1 {
			return false
		}

		found = resp.Executions[0]

		return true
	}, "the upserted search attribute is queryable")

	require.Equal(e.t, run.GetID(), found.Execution.GetWorkflowId())
	require.Equal(e.t, `"alice"`, strings.TrimSpace(string(found.Memo.Fields["owner"])))
	require.Equal(e.t, "42", strings.TrimSpace(string(found.SearchAttributes.IndexedFields["CustomIntField"])))

	n, err := e.client.CountWorkflow(e.ctx(), &shared.CountWorkflowExecutionsRequest{Domain: new(domain), Query: &query})
	require.NoError(e.t, err)
	require.Equal(e.t, int64(1), n.GetCount())

	desc, err := e.client.DescribeWorkflowExecution(e.ctx(), run.GetID(), "")
	require.NoError(e.t, err)
	require.Contains(e.t, string(desc.WorkflowExecutionInfo.Memo.Fields["owner"]), "alice")

	attrs, err := e.client.GetSearchAttributes(e.ctx())
	require.NoError(e.t, err)
	require.Equal(e.t, shared.IndexedValueTypeKeyword, attrs.Keys["CustomKeywordField"])

	require.NoError(e.t, e.client.SignalWorkflow(e.ctx(), run.GetID(), "", "done", nil))
	require.NoError(e.t, run.Get(e.ctx(), nil))
	e.replay(run)
}

func testListFilters(e *env) {
	var out string
	run, err := e.run(&out, greetWorkflow, "listed")
	require.NoError(e.t, err)

	open, err := e.client.ExecuteWorkflow(e.ctx(), e.options(), signalSumWorkflow, 1)
	require.NoError(e.t, err)

	earliest := time.Now().Add(-time.Hour).UnixNano()
	latest := time.Now().Add(time.Hour).UnixNano()

	e.eventually(func() bool {
		resp, err := e.client.ListOpenWorkflow(e.ctx(), &shared.ListOpenWorkflowExecutionsRequest{
			Domain:          new(domain),
			StartTimeFilter: &shared.StartTimeFilter{EarliestTime: &earliest, LatestTime: &latest},
			ExecutionFilter: &shared.WorkflowExecutionFilter{WorkflowId: new(open.GetID())},
		})

		return err == nil && len(resp.Executions) == 1 && resp.Executions[0].CloseStatus == nil
	}, "the running Workflow is listed as open")

	e.eventually(func() bool {
		resp, err := e.client.ListClosedWorkflow(e.ctx(), &shared.ListClosedWorkflowExecutionsRequest{
			Domain:          new(domain),
			StartTimeFilter: &shared.StartTimeFilter{EarliestTime: &earliest, LatestTime: &latest},
			ExecutionFilter: &shared.WorkflowExecutionFilter{WorkflowId: new(run.GetID())},
		})

		return err == nil && len(resp.Executions) == 1 &&
			resp.Executions[0].GetCloseStatus() == shared.WorkflowExecutionCloseStatusCompleted
	}, "the completed Workflow is listed as closed")

	// Cadence query syntax: WorkflowID, nanosecond times, numeric CloseStatus.
	q := fmt.Sprintf("WorkflowID = %q AND StartTime > %d AND CloseStatus = 0", run.GetID(), earliest)
	e.eventually(func() bool {
		n, err := e.client.CountWorkflow(e.ctx(), &shared.CountWorkflowExecutionsRequest{Domain: new(domain), Query: &q})
		return err == nil && n.GetCount() == 1
	}, "a query in Cadence syntax counts the Workflow")

	require.NoError(e.t, e.client.SignalWorkflow(e.ctx(), open.GetID(), "", "add", 1))
	require.NoError(e.t, open.Get(e.ctx(), nil))
}

func testTerminate(e *env) {
	run, err := e.client.ExecuteWorkflow(e.ctx(), e.options(), signalSumWorkflow, 1)
	require.NoError(e.t, err)

	require.NoError(e.t, e.client.TerminateWorkflow(e.ctx(), run.GetID(), "", "operator request", []byte("details")))

	err = run.Get(e.ctx(), nil)
	require.True(e.t, cadence.IsTerminatedError(err), "got %T: %v", err, err)

	desc, err := e.client.DescribeWorkflowExecution(e.ctx(), run.GetID(), run.GetRunID())
	require.NoError(e.t, err)
	require.Equal(e.t, shared.WorkflowExecutionCloseStatusTerminated, desc.WorkflowExecutionInfo.GetCloseStatus())
}

func testReset(e *env) {
	run, err := e.client.ExecuteWorkflow(e.ctx(), e.options(), resetWorkflow)
	require.NoError(e.t, err)

	// Wait for the decision that follows the Activity, then reset to the first
	// decision, which re-runs the Activity in a new run.
	var resetTo int64
	e.eventually(func() bool {
		it := e.client.GetWorkflowHistory(e.ctx(), run.GetID(), run.GetRunID(), false, shared.HistoryEventFilterTypeAllEvent)

		var completed []int64
		for it.HasNext() {
			ev, err := it.Next()
			if err != nil {
				return false
			}

			if ev.GetEventType() == shared.EventTypeDecisionTaskCompleted {
				completed = append(completed, ev.GetEventId())
			}
		}

		if len(completed) < 2 {
			return false
		}

		resetTo = completed[0]

		return true
	}, "the Workflow completes two decisions")

	resp, err := e.client.ResetWorkflow(e.ctx(), &shared.ResetWorkflowExecutionRequest{
		Domain:                new(domain),
		WorkflowExecution:     &shared.WorkflowExecution{WorkflowId: new(run.GetID()), RunId: new(run.GetRunID())},
		Reason:                new("e2e"),
		DecisionFinishEventId: &resetTo,
		RequestId:             new(uuid.NewString()),
	})
	require.NoError(e.t, err)
	require.NotEqual(e.t, run.GetRunID(), resp.GetRunId())

	require.NoError(e.t, e.client.SignalWorkflow(e.ctx(), run.GetID(), resp.GetRunId(), "finish", " (after reset)"))

	var out string
	require.NoError(e.t, e.client.GetWorkflow(e.ctx(), run.GetID(), resp.GetRunId()).Get(e.ctx(), &out))
	require.Equal(e.t, "Hello, reset! (after reset)", out)
}

func testAsyncCompletion(e *env) {
	run, err := e.client.ExecuteWorkflow(e.ctx(), e.options(), asyncCompletionWorkflow)
	require.NoError(e.t, err)

	var token []byte
	e.eventually(func() bool {
		v, ok := tokens.Load(run.GetID())
		if ok {
			token = v.([]byte)
		}

		return ok
	}, "the Activity hands out its task token")

	require.NoError(e.t, e.client.RecordActivityHeartbeat(e.ctx(), token, "still working"))
	require.NoError(e.t, e.client.CompleteActivity(e.ctx(), token, "by-token", nil))

	e.eventually(func() bool {
		err := e.client.CompleteActivityByID(e.ctx(), domain, run.GetID(), "", "by-id", "by-id", nil)
		return err == nil
	}, "the second Activity is scheduled and completed by ID")

	var out string
	require.NoError(e.t, run.Get(e.ctx(), &out))
	require.Equal(e.t, "by-token+by-id", out)
	e.replay(run)
}

func testExternal(e *env) {
	target, err := e.client.ExecuteWorkflow(e.ctx(), e.options(), signalSumWorkflow, 1)
	require.NoError(e.t, err)

	victim, err := e.client.ExecuteWorkflow(e.ctx(), e.options(), cancelWorkflow)
	require.NoError(e.t, err)

	run, err := e.run(nil, externalWorkflow, target.GetID(), victim.GetID())
	require.NoError(e.t, err)

	var sum int
	require.NoError(e.t, target.Get(e.ctx(), &sum))
	require.Equal(e.t, 5, sum)

	err = victim.Get(e.ctx(), nil)
	require.True(e.t, cadence.IsCanceledError(err), "got %T: %v", err, err)
	e.replay(run)
}

func testSessions(e *env) {
	var out string
	run, err := e.run(&out, sessionWorkflow)
	require.NoError(e.t, err)
	require.Equal(e.t, "same-host:true", out)
	e.replay(run)
}

func testContextPropagation(e *env) {
	ctx := contextWithTenant(e)

	run, err := e.client.ExecuteWorkflow(ctx, e.options(), tenantWorkflow)
	require.NoError(e.t, err)

	var out string
	require.NoError(e.t, run.Get(e.ctx(), &out))
	require.Equal(e.t, "acme/acme", out)
}

func testWorkflowRetry(e *env) {
	opts := e.options()
	opts.RetryPolicy = fastRetry(3)

	run, err := e.client.ExecuteWorkflow(e.ctx(), opts, workflowRetryWorkflow)
	require.NoError(e.t, err)

	var attempt int32
	require.NoError(e.t, run.Get(e.ctx(), &attempt))
	require.Equal(e.t, int32(1), attempt)
}

func testWorkflowFailure(e *env) {
	_, err := e.run(nil, failingWorkflow)

	var ce *cadence.CustomError
	require.ErrorAs(e.t, err, &ce)
	require.Equal(e.t, "order-rejected", ce.Reason())

	var (
		msg string
		n   int
	)
	require.NoError(e.t, ce.Details(&msg, &n))
	require.Equal(e.t, "out of stock", msg)
	require.Equal(e.t, 3, n)
}

func testDelayStart(e *env) {
	opts := e.options()
	opts.DelayStart = 2 * time.Second

	start := time.Now()
	run, err := e.client.ExecuteWorkflow(e.ctx(), opts, greetWorkflow, "later")
	require.NoError(e.t, err)

	require.NoError(e.t, run.Get(e.ctx(), nil))
	require.GreaterOrEqual(e.t, time.Since(start), 2*time.Second)
}

func testTypedErrors(e *env) {
	opts := e.options()
	opts.WorkflowIDReusePolicy = client.WorkflowIDReusePolicyRejectDuplicate

	run, err := e.client.ExecuteWorkflow(e.ctx(), opts, signalSumWorkflow, 1)
	require.NoError(e.t, err)

	_, err = e.client.StartWorkflow(e.ctx(), opts, signalSumWorkflow, 1)

	var started *shared.WorkflowExecutionAlreadyStartedError
	require.ErrorAs(e.t, err, &started)

	_, err = e.client.DescribeWorkflowExecution(e.ctx(), "no-such-"+uuid.NewString(), "")

	var missing *shared.EntityNotExistsError
	require.ErrorAs(e.t, err, &missing)

	_, err = e.client.QueryWorkflow(e.ctx(), run.GetID(), "", "no-such-query")

	var failed *shared.QueryFailedError
	require.ErrorAs(e.t, err, &failed)

	require.NoError(e.t, e.client.SignalWorkflow(e.ctx(), run.GetID(), "", "add", 1))
	require.NoError(e.t, run.Get(e.ctx(), nil))

	err = e.client.SignalWorkflow(e.ctx(), run.GetID(), run.GetRunID(), "add", 1)
	require.ErrorAs(e.t, err, &missing, "signalling a closed Workflow")
}

func testDomains(e *env) {
	dc := client.NewDomainClient(e.service, &client.Options{})
	name := "cadence-e2e-" + uuid.NewString()[:8]
	retention := int32(3)

	require.NoError(e.t, dc.Register(e.ctx(), &shared.RegisterDomainRequest{
		Name:                                   &name,
		Description:                            new("registered by a Cadence client"),
		OwnerEmail:                             new("owner@example.com"),
		WorkflowExecutionRetentionPeriodInDays: &retention,
		Data:                                   map[string]string{"team": "payments"},
	}))

	desc, err := dc.Describe(e.ctx(), name)
	require.NoError(e.t, err)
	require.Equal(e.t, name, desc.DomainInfo.GetName())
	require.Equal(e.t, shared.DomainStatusRegistered, desc.DomainInfo.GetStatus())
	require.Equal(e.t, "payments", desc.DomainInfo.Data["team"])
	require.Equal(e.t, retention, desc.Configuration.GetWorkflowExecutionRetentionPeriodInDays())

	require.NoError(e.t, dc.Update(e.ctx(), &shared.UpdateDomainRequest{
		Name:        &name,
		UpdatedInfo: &shared.UpdateDomainInfo{Description: new("updated")},
	}))

	desc, err = dc.Describe(e.ctx(), name)
	require.NoError(e.t, err)
	require.Equal(e.t, "updated", desc.DomainInfo.GetDescription())

	err = dc.Register(e.ctx(), &shared.RegisterDomainRequest{Name: &name, WorkflowExecutionRetentionPeriodInDays: &retention})

	var exists *shared.DomainAlreadyExistsError
	require.ErrorAs(e.t, err, &exists)

	require.NoError(e.t, e.service.DeprecateDomain(e.ctx(), &shared.DeprecateDomainRequest{Name: &name}))

	desc, err = dc.Describe(e.ctx(), name)
	require.NoError(e.t, err)
	require.Equal(e.t, shared.DomainStatusDeprecated, desc.DomainInfo.GetStatus())

	_, err = dc.Describe(e.ctx(), "no-such-domain-"+uuid.NewString())

	var missing *shared.EntityNotExistsError
	require.ErrorAs(e.t, err, &missing)
}

func testDescribeTaskList(e *env) {
	e.eventually(func() bool {
		resp, err := e.client.DescribeTaskList(e.ctx(), e.taskList, shared.TaskListTypeDecision)
		return err == nil && len(resp.Pollers) > 0
	}, "the Worker polls its Task List")
}

// nonStickyWorker starts a Worker that never caches Workflows, so every
// decision task carries, and replays, the full translated history.
func nonStickyWorker(e *env) client.StartWorkflowOptions {
	taskList := e.taskList + "-nonsticky-" + uuid.NewString()[:8]

	w, err := worker.NewV2(e.service, domain, taskList, worker.Options{
		Logger:                 zap.NewNop(),
		DisableStickyExecution: true,
	})
	require.NoError(e.t, err)
	registerAll(w)
	require.NoError(e.t, w.Start())
	e.t.Cleanup(w.Stop)

	opts := e.options()
	opts.TaskList = taskList

	return opts
}

func testNonSticky(e *env) {
	opts := nonStickyWorker(e)

	run, err := e.client.ExecuteWorkflow(e.ctx(), opts, longWorkflow)
	require.NoError(e.t, err)

	require.NoError(e.t, e.client.SignalWorkflow(e.ctx(), run.GetID(), "", "go", "!"))

	var out string
	require.NoError(e.t, run.Get(e.ctx(), &out))
	require.Equal(e.t, "Hello, 0!Hello, 1!Hello, 2!!", out)
	e.replay(run)
}

// testPagedHistory builds a history longer than one page and has a Worker
// without a cache replay it, so the history is fetched page by page.
func testPagedHistory(e *env) {
	const signals = 1200

	opts := nonStickyWorker(e)

	run, err := e.client.ExecuteWorkflow(e.ctx(), opts, signalSumWorkflow, signals)
	require.NoError(e.t, err)

	var g errgroup.Group
	g.SetLimit(32)

	for range signals {
		g.Go(func() error { return e.client.SignalWorkflow(e.ctx(), run.GetID(), "", "add", 1) })
	}

	require.NoError(e.t, g.Wait())

	var sum int
	require.NoError(e.t, run.Get(e.ctx(), &sum))
	require.Equal(e.t, signals, sum)

	desc, err := e.client.DescribeWorkflowExecution(e.ctx(), run.GetID(), run.GetRunID())
	require.NoError(e.t, err)
	require.Greater(e.t, desc.WorkflowExecutionInfo.GetHistoryLength(), int64(signals))
	e.replay(run)
}

//go:fix inline

func contextWithTenant(e *env) context.Context {
	return context.WithValue(e.ctx(), tenantKey{}, "acme")
}
