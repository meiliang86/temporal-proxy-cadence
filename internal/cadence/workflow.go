package cadence

import (
	"context"
	"math/rand/v2"
	"time"

	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// supportedClientVersions is what GetClusterInfo reports. The frontend accepts
// every Cadence SDK version, so these are the ceilings Cadence itself reports.
var supportedClientVersions = &apiv1.SupportedClientVersions{GoSdk: "1.7.0", JavaSdk: "1.5.0"}

// startDelay folds Cadence's three ways of delaying a first run (a fixed delay,
// a random jitter, and a first-run time) into Temporal's single start delay.
func startDelay(req *apiv1.StartWorkflowExecutionRequest) *durationpb.Duration {
	d := goDuration(req.DelayStart)

	if j := goDuration(req.JitterStart); j > 0 {
		d += rand.N(j) //nolint:gosec // Jitter, not security.
	}

	if t := req.FirstRunAt; t != nil {
		if until := time.Until(time.Unix(t.Seconds, int64(t.Nanos))); until > d {
			d = until
		}
	}

	return durationOf(d)
}

func toStartRequest(req *apiv1.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionRequest {
	reuse, conflict := toReusePolicy(req.WorkflowIdReusePolicy)
	exec, run := workflowTimeouts(req.ExecutionStartToCloseTimeout, req.RetryPolicy, req.CronSchedule)

	return &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                req.Domain,
		WorkflowId:               req.WorkflowId,
		WorkflowType:             toWorkflowType(req.WorkflowType),
		TaskQueue:                toTaskQueue(req.TaskList, ""),
		Input:                    toPayloads(req.Input),
		WorkflowExecutionTimeout: exec,
		WorkflowRunTimeout:       run,
		WorkflowTaskTimeout:      toDuration(req.TaskStartToCloseTimeout),
		Identity:                 req.Identity,
		RequestId:                req.RequestId,
		WorkflowIdReusePolicy:    reuse,
		WorkflowIdConflictPolicy: conflict,
		RetryPolicy:              toRetryPolicy(req.RetryPolicy),
		CronSchedule:             req.CronSchedule,
		Memo:                     toMemo(req.Memo),
		SearchAttributes:         toSearchAttributes(req.SearchAttributes),
		Header:                   toHeader(req.Header),
		WorkflowStartDelay:       startDelay(req),
	}
}

// StartWorkflowExecution starts a Workflow.
func (h *Handler) StartWorkflowExecution(
	ctx context.Context,
	req *apiv1.StartWorkflowExecutionRequest,
) (*apiv1.StartWorkflowExecutionResponse, error) {
	resp, err := h.workflow.StartWorkflowExecution(ctx, toStartRequest(req))
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.StartWorkflowExecutionResponse{RunId: resp.RunId}, nil
}

// StartWorkflowExecutionAsync starts a Workflow synchronously: Temporal has no
// start queue. Like Cadence's queue, it reports a duplicate start as success.
func (h *Handler) StartWorkflowExecutionAsync(
	ctx context.Context,
	req *apiv1.StartWorkflowExecutionAsyncRequest,
) (*apiv1.StartWorkflowExecutionAsyncResponse, error) {
	_, err := h.workflow.StartWorkflowExecution(ctx, toStartRequest(req.GetRequest()))
	if err != nil && !isAlreadyStarted(err) {
		return nil, toCadenceError(err)
	}

	return &apiv1.StartWorkflowExecutionAsyncResponse{}, nil
}

func isAlreadyStarted(err error) bool {
	_, ok := serviceerror.FromStatus(status.Convert(err)).(*serviceerror.WorkflowExecutionAlreadyStarted)
	return ok
}

// SignalWorkflowExecution signals a Workflow.
func (h *Handler) SignalWorkflowExecution(
	ctx context.Context,
	req *apiv1.SignalWorkflowExecutionRequest,
) (*apiv1.SignalWorkflowExecutionResponse, error) {
	_, err := h.workflow.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         req.Domain,
		WorkflowExecution: toExecution(req.WorkflowExecution),
		SignalName:        req.SignalName,
		Input:             toPayloads(req.SignalInput),
		Identity:          req.Identity,
		RequestId:         req.RequestId,
		Control:           toControl(req.Control), //nolint:staticcheck // Cadence still carries control.
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.SignalWorkflowExecutionResponse{}, nil
}

func toSignalWithStartRequest(req *apiv1.SignalWithStartWorkflowExecutionRequest) *workflowservice.SignalWithStartWorkflowExecutionRequest {
	s := toStartRequest(req.GetStartRequest())

	return &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:                s.Namespace,
		WorkflowId:               s.WorkflowId,
		WorkflowType:             s.WorkflowType,
		TaskQueue:                s.TaskQueue,
		Input:                    s.Input,
		WorkflowExecutionTimeout: s.WorkflowExecutionTimeout,
		WorkflowRunTimeout:       s.WorkflowRunTimeout,
		WorkflowTaskTimeout:      s.WorkflowTaskTimeout,
		Identity:                 s.Identity,
		RequestId:                s.RequestId,
		WorkflowIdReusePolicy:    s.WorkflowIdReusePolicy,
		WorkflowIdConflictPolicy: s.WorkflowIdConflictPolicy,
		SignalName:               req.SignalName,
		SignalInput:              toPayloads(req.SignalInput),
		Control:                  toControl(req.Control), //nolint:staticcheck // Cadence still carries control.
		RetryPolicy:              s.RetryPolicy,
		CronSchedule:             s.CronSchedule,
		Memo:                     s.Memo,
		SearchAttributes:         s.SearchAttributes,
		Header:                   s.Header,
		WorkflowStartDelay:       s.WorkflowStartDelay,
	}
}

// SignalWithStartWorkflowExecution signals a Workflow, starting it first if it
// is not running.
func (h *Handler) SignalWithStartWorkflowExecution(
	ctx context.Context,
	req *apiv1.SignalWithStartWorkflowExecutionRequest,
) (*apiv1.SignalWithStartWorkflowExecutionResponse, error) {
	resp, err := h.workflow.SignalWithStartWorkflowExecution(ctx, toSignalWithStartRequest(req))
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.SignalWithStartWorkflowExecutionResponse{RunId: resp.RunId}, nil
}

// SignalWithStartWorkflowExecutionAsync signals with start synchronously.
func (h *Handler) SignalWithStartWorkflowExecutionAsync(
	ctx context.Context,
	req *apiv1.SignalWithStartWorkflowExecutionAsyncRequest,
) (*apiv1.SignalWithStartWorkflowExecutionAsyncResponse, error) {
	if _, err := h.workflow.SignalWithStartWorkflowExecution(ctx, toSignalWithStartRequest(req.GetRequest())); err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.SignalWithStartWorkflowExecutionAsyncResponse{}, nil
}

// ResetWorkflowExecution resets a Workflow to a decision task.
func (h *Handler) ResetWorkflowExecution(
	ctx context.Context,
	req *apiv1.ResetWorkflowExecutionRequest,
) (*apiv1.ResetWorkflowExecutionResponse, error) {
	reapply := enumspb.RESET_REAPPLY_TYPE_SIGNAL
	if req.SkipSignalReapply {
		reapply = enumspb.RESET_REAPPLY_TYPE_NONE
	}

	resp, err := h.workflow.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 req.Domain,
		WorkflowExecution:         toExecution(req.WorkflowExecution),
		Reason:                    req.Reason,
		WorkflowTaskFinishEventId: req.DecisionFinishEventId,
		RequestId:                 req.RequestId,
		ResetReapplyType:          reapply, //nolint:staticcheck // The replacement option set has no "none".
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.ResetWorkflowExecutionResponse{RunId: resp.RunId}, nil
}

// RequestCancelWorkflowExecution requests a Workflow's cancellation.
func (h *Handler) RequestCancelWorkflowExecution(
	ctx context.Context,
	req *apiv1.RequestCancelWorkflowExecutionRequest,
) (*apiv1.RequestCancelWorkflowExecutionResponse, error) {
	_, err := h.workflow.RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace:           req.Domain,
		WorkflowExecution:   toExecution(req.WorkflowExecution),
		Identity:            req.Identity,
		RequestId:           req.RequestId,
		FirstExecutionRunId: req.FirstExecutionRunId,
		Reason:              req.Cause,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RequestCancelWorkflowExecutionResponse{}, nil
}

// TerminateWorkflowExecution terminates a Workflow.
func (h *Handler) TerminateWorkflowExecution(
	ctx context.Context,
	req *apiv1.TerminateWorkflowExecutionRequest,
) (*apiv1.TerminateWorkflowExecutionResponse, error) {
	_, err := h.workflow.TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:           req.Domain,
		WorkflowExecution:   toExecution(req.WorkflowExecution),
		Reason:              req.Reason,
		Details:             toPayloads(req.Details),
		Identity:            req.Identity,
		FirstExecutionRunId: req.FirstExecutionRunId,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.TerminateWorkflowExecutionResponse{}, nil
}

// DescribeWorkflowExecution describes a Workflow's configuration and pending
// work.
func (h *Handler) DescribeWorkflowExecution(
	ctx context.Context,
	req *apiv1.DescribeWorkflowExecutionRequest,
) (*apiv1.DescribeWorkflowExecutionResponse, error) {
	resp, err := h.workflow.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: req.Domain,
		Execution: toExecution(req.WorkflowExecution),
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	out := &apiv1.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: fromExecutionInfo(resp.WorkflowExecutionInfo),
	}

	if c := resp.ExecutionConfig; c != nil {
		out.ExecutionConfiguration = &apiv1.WorkflowExecutionConfiguration{
			TaskList:                     fromTaskQueue(c.TaskQueue),
			ExecutionStartToCloseTimeout: fromDuration(c.WorkflowRunTimeout),
			TaskStartToCloseTimeout:      fromDuration(c.DefaultWorkflowTaskTimeout),
		}
	}

	for _, a := range resp.PendingActivities {
		out.PendingActivities = append(out.PendingActivities, &apiv1.PendingActivityInfo{
			ActivityId:            a.ActivityId,
			ActivityType:          fromActivityType(a.ActivityType),
			State:                 fromPendingActivityState(a.State),
			HeartbeatDetails:      fromPayloads(a.HeartbeatDetails),
			LastHeartbeatTime:     fromTimestamp(a.LastHeartbeatTime),
			LastStartedTime:       fromTimestamp(a.LastStartedTime),
			Attempt:               fromAttempt(a.Attempt),
			MaximumAttempts:       a.MaximumAttempts,
			ScheduledTime:         fromTimestamp(a.ScheduledTime),
			ExpirationTime:        fromTimestamp(a.ExpirationTime),
			LastFailure:           fromFailure(a.LastFailure),
			LastWorkerIdentity:    a.LastWorkerIdentity,
			StartedWorkerIdentity: a.LastWorkerIdentity,
		})
	}

	for _, c := range resp.PendingChildren {
		out.PendingChildren = append(out.PendingChildren, &apiv1.PendingChildExecutionInfo{
			WorkflowExecution: &apiv1.WorkflowExecution{WorkflowId: c.WorkflowId, RunId: c.RunId},
			WorkflowTypeName:  c.WorkflowTypeName,
			InitiatedId:       c.InitiatedId,
			ParentClosePolicy: fromParentClosePolicy(c.ParentClosePolicy),
			Domain:            req.Domain,
		})
	}

	if t := resp.PendingWorkflowTask; t != nil {
		out.PendingDecision = &apiv1.PendingDecisionInfo{
			State:                 fromPendingWorkflowTaskState(t.State),
			ScheduledTime:         fromTimestamp(t.ScheduledTime),
			StartedTime:           fromTimestamp(t.StartedTime),
			Attempt:               fromAttempt(t.Attempt),
			OriginalScheduledTime: fromTimestamp(t.OriginalScheduledTime),
		}
	}

	return out, nil
}

// QueryWorkflow queries a Workflow. Temporal answers every query with the
// consistency Cadence calls strong, so the requested level needs no mapping.
func (h *Handler) QueryWorkflow(ctx context.Context, req *apiv1.QueryWorkflowRequest) (*apiv1.QueryWorkflowResponse, error) {
	resp, err := h.workflow.QueryWorkflow(ctx, &workflowservice.QueryWorkflowRequest{
		Namespace: req.Domain,
		Execution: toExecution(req.WorkflowExecution),
		Query: &querypb.WorkflowQuery{
			QueryType: req.GetQuery().GetQueryType(),
			QueryArgs: toPayloads(req.GetQuery().GetQueryArgs()),
		},
		QueryRejectCondition: toQueryRejectCondition(req.QueryRejectCondition),
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	out := &apiv1.QueryWorkflowResponse{QueryResult: fromPayloads(resp.QueryResult)}
	if r := resp.QueryRejected; r != nil {
		closeStatus, _ := fromExecutionStatus(r.Status)
		out.QueryRejected = &apiv1.QueryRejected{CloseStatus: closeStatus}
	}

	return out, nil
}

// DescribeTaskList lists a Task Queue's pollers.
func (h *Handler) DescribeTaskList(ctx context.Context, req *apiv1.DescribeTaskListRequest) (*apiv1.DescribeTaskListResponse, error) {
	resp, err := h.workflow.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     req.Domain,
		TaskQueue:     toTaskQueue(req.TaskList, ""),
		TaskQueueType: toTaskQueueType(req.TaskListType),
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	out := &apiv1.DescribeTaskListResponse{TaskList: req.TaskList}
	for _, p := range resp.Pollers {
		out.Pollers = append(out.Pollers, &apiv1.PollerInfo{
			LastAccessTime: fromTimestamp(p.LastAccessTime),
			Identity:       p.Identity,
			RatePerSecond:  p.RatePerSecond,
		})
	}

	if s := resp.TaskQueueStatus; s != nil { //nolint:staticcheck // Cadence still reports it.
		out.TaskListStatus = &apiv1.TaskListStatus{
			BacklogCountHint: s.BacklogCountHint,
			ReadLevel:        s.ReadLevel,
			AckLevel:         s.AckLevel,
			RatePerSecond:    s.RatePerSecond,
		}
	}

	return out, nil
}

// GetClusterInfo reports the Cadence SDK versions the frontend accepts.
func (h *Handler) GetClusterInfo(context.Context, *apiv1.GetClusterInfoRequest) (*apiv1.GetClusterInfoResponse, error) {
	return &apiv1.GetClusterInfoResponse{SupportedClientVersions: supportedClientVersions}, nil
}

// GetWorkflowExecutionHistory returns, or long-polls for, a page of history.
func (h *Handler) GetWorkflowExecutionHistory(
	ctx context.Context,
	req *apiv1.GetWorkflowExecutionHistoryRequest,
) (*apiv1.GetWorkflowExecutionHistoryResponse, error) {
	filter := enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
	if req.HistoryEventFilterType == apiv1.EventFilterType_EVENT_FILTER_TYPE_CLOSE_EVENT {
		filter = enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
	}

	exec := toExecution(req.WorkflowExecution)

	resp, err := h.workflow.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:              req.Domain,
		Execution:              exec,
		MaximumPageSize:        req.PageSize,
		NextPageToken:          req.NextPageToken,
		WaitNewEvent:           req.WaitForNewEvent,
		HistoryEventFilterType: filter,
		SkipArchival:           req.SkipArchival,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	history, err := newHistoryConverter(req.Domain, h.activityIDResolver(req.Domain, exec)).history(ctx, resp.History)
	if err != nil {
		return nil, toCadenceError(err)
	}

	if history == nil {
		history = &apiv1.History{}
	}

	return &apiv1.GetWorkflowExecutionHistoryResponse{
		History:       history,
		NextPageToken: resp.NextPageToken,
		Archived:      resp.Archived,
	}, nil
}

// RestartWorkflowExecution is not supported.
func (h *Handler) RestartWorkflowExecution(
	context.Context, *apiv1.RestartWorkflowExecutionRequest,
) (*apiv1.RestartWorkflowExecutionResponse, error) {
	return nil, unimplemented("RestartWorkflowExecution")
}

// GetTaskListsByDomain is not supported: Temporal cannot list Task Queues.
func (h *Handler) GetTaskListsByDomain(
	context.Context, *apiv1.GetTaskListsByDomainRequest,
) (*apiv1.GetTaskListsByDomainResponse, error) {
	return nil, unimplemented("GetTaskListsByDomain")
}

// ListTaskListPartitions reports no partitions; Temporal does not expose them.
func (h *Handler) ListTaskListPartitions(
	context.Context, *apiv1.ListTaskListPartitionsRequest,
) (*apiv1.ListTaskListPartitionsResponse, error) {
	return &apiv1.ListTaskListPartitionsResponse{}, nil
}

// RefreshWorkflowTasks is not supported.
func (h *Handler) RefreshWorkflowTasks(
	context.Context, *apiv1.RefreshWorkflowTasksRequest,
) (*apiv1.RefreshWorkflowTasksResponse, error) {
	return nil, unimplemented("RefreshWorkflowTasks")
}

// DiagnoseWorkflowExecution is not supported.
func (h *Handler) DiagnoseWorkflowExecution(
	context.Context, *apiv1.DiagnoseWorkflowExecutionRequest,
) (*apiv1.DiagnoseWorkflowExecutionResponse, error) {
	return nil, unimplemented("DiagnoseWorkflowExecution")
}

// ExportWorkflowExecution is not supported.
func (h *Handler) ExportWorkflowExecution(
	context.Context, *apiv1.ExportWorkflowExecutionRequest,
) (*apiv1.ExportWorkflowExecutionResponse, error) {
	return nil, unimplemented("ExportWorkflowExecution")
}
