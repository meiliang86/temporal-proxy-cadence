package cadence

import (
	"context"

	gogo "github.com/gogo/protobuf/types"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	querypb "go.temporal.io/api/query/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// PollForDecisionTask long-polls a workflow Task Queue.
func (h *Handler) PollForDecisionTask(ctx context.Context, req *apiv1.PollForDecisionTaskRequest) (*apiv1.PollForDecisionTaskResponse, error) {
	resp, err := h.workflow.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:      req.Domain,
		TaskQueue:      toTaskQueue(req.TaskList, ""),
		Identity:       req.Identity,
		BinaryChecksum: req.BinaryChecksum, //nolint:staticcheck // Cadence still sends it.
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	out, err := h.decisionTask(ctx, req.Domain, resp)
	if err != nil {
		return nil, toCadenceError(err)
	}

	return out, nil
}

// decisionTask converts a polled workflow task. An empty poll (no token) is an
// empty response, as on Cadence.
func (h *Handler) decisionTask(
	ctx context.Context,
	namespace string,
	resp *workflowservice.PollWorkflowTaskQueueResponse,
) (*apiv1.PollForDecisionTaskResponse, error) {
	if resp == nil || len(resp.TaskToken) == 0 {
		return &apiv1.PollForDecisionTaskResponse{}, nil
	}

	exec := resp.WorkflowExecution
	conv := newHistoryConverter(namespace, h.activityIDResolver(namespace, exec))

	history, err := conv.history(ctx, resp.History)
	if err != nil {
		return nil, err
	}

	token := &taskToken{
		Temporal:   resp.TaskToken,
		Namespace:  namespace,
		WorkflowID: exec.GetWorkflowId(),
		RunID:      exec.GetRunId(),
		TaskQueue:  resp.GetWorkflowExecutionTaskQueue().GetName(),
	}

	out := &apiv1.PollForDecisionTaskResponse{
		TaskToken:                 token.encode(),
		WorkflowExecution:         fromExecution(exec),
		WorkflowType:              fromWorkflowType(resp.WorkflowType),
		PreviousStartedEventId:    &gogo.Int64Value{Value: resp.PreviousStartedEventId},
		StartedEventId:            resp.StartedEventId,
		Attempt:                   int64(fromAttempt(resp.Attempt)),
		BacklogCountHint:          resp.BacklogCountHint,
		History:                   history,
		NextPageToken:             resp.NextPageToken,
		Query:                     fromQuery(resp.Query),
		WorkflowExecutionTaskList: fromTaskQueue(resp.WorkflowExecutionTaskQueue),
		ScheduledTime:             fromTimestamp(resp.ScheduledTime),
		StartedTime:               fromTimestamp(resp.StartedTime),
	}

	switch {
	case resp.StartedEventId > 0:
		out.NextEventId = resp.StartedEventId + 1
	case len(resp.NextPageToken) == 0 && len(resp.GetHistory().GetEvents()) > 0:
		events := resp.History.Events
		out.NextEventId = events[len(events)-1].EventId + 1
	}

	if len(resp.Queries) > 0 {
		out.Queries = make(map[string]*apiv1.WorkflowQuery, len(resp.Queries))
		for k, q := range resp.Queries {
			out.Queries[k] = fromQuery(q)
		}
	}

	return out, nil
}

func fromQuery(q *querypb.WorkflowQuery) *apiv1.WorkflowQuery {
	if q == nil {
		return nil
	}

	return &apiv1.WorkflowQuery{QueryType: q.QueryType, QueryArgs: fromPayloads(q.QueryArgs)}
}

func toQueryResult(r *apiv1.WorkflowQueryResult) *querypb.WorkflowQueryResult {
	if r == nil {
		return nil
	}

	out := &querypb.WorkflowQueryResult{
		ResultType:   toQueryResultType(r.ResultType),
		Answer:       toPayloads(r.Answer),
		ErrorMessage: r.ErrorMessage,
	}

	if out.ResultType == enumspb.QUERY_RESULT_TYPE_FAILED {
		out.Failure = &failurepb.Failure{Message: r.ErrorMessage, Source: failureSource}
	}

	return out
}

// RespondDecisionTaskCompleted completes a workflow task with the Worker's
// decisions.
func (h *Handler) RespondDecisionTaskCompleted(
	ctx context.Context,
	req *apiv1.RespondDecisionTaskCompletedRequest,
) (*apiv1.RespondDecisionTaskCompletedResponse, error) {
	tok, err := decodeToken(req.TaskToken)
	if err != nil {
		return nil, toCadenceError(err)
	}

	exec := &commonpb.WorkflowExecution{WorkflowId: tok.WorkflowID, RunId: tok.RunID}

	commands, err := toCommands(ctx, req.Decisions, h.scheduledEventResolver(tok.Namespace, exec))
	if err != nil {
		return nil, toCadenceError(err)
	}

	treq := &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken:                  tok.Temporal,
		Commands:                   commands,
		Identity:                   req.Identity,
		ReturnNewWorkflowTask:      req.ReturnNewDecisionTask,
		ForceCreateNewWorkflowTask: req.ForceCreateNewDecisionTask,
		BinaryChecksum:             req.BinaryChecksum, //nolint:staticcheck // Cadence still sends it.
		Namespace:                  tok.Namespace,
	}

	if s := req.StickyAttributes; s != nil && s.WorkerTaskList != nil {
		treq.StickyAttributes = &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue: &taskqueuepb.TaskQueue{
				Name:       s.WorkerTaskList.Name,
				Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
				NormalName: tok.TaskQueue,
			},
			ScheduleToStartTimeout: toDuration(s.ScheduleToStartTimeout),
		}
	}

	if len(req.QueryResults) > 0 {
		treq.QueryResults = make(map[string]*querypb.WorkflowQueryResult, len(req.QueryResults))
		for k, r := range req.QueryResults {
			treq.QueryResults[k] = toQueryResult(r)
		}
	}

	resp, err := h.workflow.RespondWorkflowTaskCompleted(ctx, treq)
	if err != nil {
		return nil, toCadenceError(err)
	}

	out := &apiv1.RespondDecisionTaskCompletedResponse{}

	if resp.WorkflowTask != nil {
		if out.DecisionTask, err = h.decisionTask(ctx, tok.Namespace, resp.WorkflowTask); err != nil {
			return nil, toCadenceError(err)
		}
	}

	// Activities the decision asked to run locally come back as eager activity
	// tasks; the Worker already holds everything else about them.
	if len(resp.ActivityTasks) > 0 {
		out.ActivitiesToDispatchLocally = make(map[string]*apiv1.ActivityLocalDispatchInfo, len(resp.ActivityTasks))
		for _, t := range resp.ActivityTasks {
			out.ActivitiesToDispatchLocally[t.ActivityId] = &apiv1.ActivityLocalDispatchInfo{
				ActivityId:                 t.ActivityId,
				ScheduledTime:              fromTimestamp(t.ScheduledTime),
				StartedTime:                fromTimestamp(t.StartedTime),
				ScheduledTimeOfThisAttempt: fromTimestamp(t.CurrentAttemptScheduledTime),
				TaskToken:                  activityToken(tok.Namespace, t).encode(),
			}
		}
	}

	return out, nil
}

// RespondDecisionTaskFailed fails a workflow task.
func (h *Handler) RespondDecisionTaskFailed(
	ctx context.Context,
	req *apiv1.RespondDecisionTaskFailedRequest,
) (*apiv1.RespondDecisionTaskFailedResponse, error) {
	tok, err := decodeToken(req.TaskToken)
	if err != nil {
		return nil, toCadenceError(err)
	}

	_, err = h.workflow.RespondWorkflowTaskFailed(ctx, &workflowservice.RespondWorkflowTaskFailedRequest{
		TaskToken: tok.Temporal,
		Cause:     toDecisionFailedCause(req.Cause),
		Failure: &failurepb.Failure{
			Message: string(req.GetDetails().GetData()),
			Source:  failureSource,
		},
		Identity:       req.Identity,
		BinaryChecksum: req.BinaryChecksum, //nolint:staticcheck // Cadence still sends it.
		Namespace:      tok.Namespace,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RespondDecisionTaskFailedResponse{}, nil
}

// PollForActivityTask long-polls an activity Task Queue.
func (h *Handler) PollForActivityTask(ctx context.Context, req *apiv1.PollForActivityTaskRequest) (*apiv1.PollForActivityTaskResponse, error) {
	treq := &workflowservice.PollActivityTaskQueueRequest{
		Namespace: req.Domain,
		TaskQueue: toTaskQueue(req.TaskList, ""),
		Identity:  req.Identity,
	}

	if m := req.GetTaskListMetadata().GetMaxTasksPerSecond(); m != nil {
		treq.TaskQueueMetadata = &taskqueuepb.TaskQueueMetadata{MaxTasksPerSecond: wrapperspb.Double(m.Value)}
	}

	resp, err := h.workflow.PollActivityTaskQueue(ctx, treq)
	if err != nil {
		return nil, toCadenceError(err)
	}

	if len(resp.TaskToken) == 0 {
		return &apiv1.PollForActivityTaskResponse{}, nil
	}

	return &apiv1.PollForActivityTaskResponse{
		TaskToken:                  activityToken(req.Domain, resp).encode(),
		WorkflowExecution:          fromExecution(resp.WorkflowExecution),
		ActivityId:                 resp.ActivityId,
		ActivityType:               fromActivityType(resp.ActivityType),
		Input:                      fromPayloads(resp.Input),
		ScheduledTime:              fromTimestamp(resp.ScheduledTime),
		StartedTime:                fromTimestamp(resp.StartedTime),
		ScheduleToCloseTimeout:     fromDuration(resp.ScheduleToCloseTimeout),
		StartToCloseTimeout:        fromDuration(resp.StartToCloseTimeout),
		HeartbeatTimeout:           fromDuration(resp.HeartbeatTimeout),
		Attempt:                    fromAttempt(resp.Attempt),
		ScheduledTimeOfThisAttempt: fromTimestamp(resp.CurrentAttemptScheduledTime),
		HeartbeatDetails:           fromPayloads(resp.HeartbeatDetails),
		WorkflowType:               fromWorkflowType(resp.WorkflowType),
		WorkflowDomain:             resp.WorkflowNamespace,
		Header:                     fromHeader(resp.Header),
	}, nil
}

func activityToken(namespace string, t *workflowservice.PollActivityTaskQueueResponse) *taskToken {
	return &taskToken{
		Temporal:   t.TaskToken,
		Namespace:  namespace,
		WorkflowID: t.GetWorkflowExecution().GetWorkflowId(),
		RunID:      t.GetWorkflowExecution().GetRunId(),
		ActivityID: t.ActivityId,
	}
}

// RespondActivityTaskCompleted completes an Activity by task token.
func (h *Handler) RespondActivityTaskCompleted(
	ctx context.Context,
	req *apiv1.RespondActivityTaskCompletedRequest,
) (*apiv1.RespondActivityTaskCompletedResponse, error) {
	tok, err := decodeToken(req.TaskToken)
	if err != nil {
		return nil, toCadenceError(err)
	}

	_, err = h.workflow.RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		TaskToken: tok.Temporal,
		Result:    toPayloads(req.Result),
		Identity:  req.Identity,
		Namespace: tok.Namespace,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RespondActivityTaskCompletedResponse{}, nil
}

// RespondActivityTaskCompletedByID completes an Activity by its ID.
func (h *Handler) RespondActivityTaskCompletedByID(
	ctx context.Context,
	req *apiv1.RespondActivityTaskCompletedByIDRequest,
) (*apiv1.RespondActivityTaskCompletedByIDResponse, error) {
	_, err := h.workflow.RespondActivityTaskCompletedById(ctx, &workflowservice.RespondActivityTaskCompletedByIdRequest{
		Namespace:  req.Domain,
		WorkflowId: req.GetWorkflowExecution().GetWorkflowId(),
		RunId:      req.GetWorkflowExecution().GetRunId(),
		ActivityId: req.ActivityId,
		Result:     toPayloads(req.Result),
		Identity:   req.Identity,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RespondActivityTaskCompletedByIDResponse{}, nil
}

// activityFailure is [toFailure] plus the retry delay a Cadence Worker may ask
// for.
func activityFailure(f *apiv1.Failure) *failurepb.Failure {
	out := toFailure(f)
	if d := f.GetOptions().GetNextRetryInterval(); d != nil && out != nil {
		out.GetApplicationFailureInfo().NextRetryDelay = toDuration(d)
	}

	return out
}

// RespondActivityTaskFailed fails an Activity by task token.
func (h *Handler) RespondActivityTaskFailed(
	ctx context.Context,
	req *apiv1.RespondActivityTaskFailedRequest,
) (*apiv1.RespondActivityTaskFailedResponse, error) {
	tok, err := decodeToken(req.TaskToken)
	if err != nil {
		return nil, toCadenceError(err)
	}

	_, err = h.workflow.RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken:            tok.Temporal,
		Failure:              activityFailure(req.Failure),
		Identity:             req.Identity,
		Namespace:            tok.Namespace,
		LastHeartbeatDetails: toPayloads(req.HeartbeatDetails),
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RespondActivityTaskFailedResponse{}, nil
}

// RespondActivityTaskFailedByID fails an Activity by its ID.
func (h *Handler) RespondActivityTaskFailedByID(
	ctx context.Context,
	req *apiv1.RespondActivityTaskFailedByIDRequest,
) (*apiv1.RespondActivityTaskFailedByIDResponse, error) {
	_, err := h.workflow.RespondActivityTaskFailedById(ctx, &workflowservice.RespondActivityTaskFailedByIdRequest{
		Namespace:            req.Domain,
		WorkflowId:           req.GetWorkflowExecution().GetWorkflowId(),
		RunId:                req.GetWorkflowExecution().GetRunId(),
		ActivityId:           req.ActivityId,
		Failure:              activityFailure(req.Failure),
		Identity:             req.Identity,
		LastHeartbeatDetails: toPayloads(req.HeartbeatDetails),
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RespondActivityTaskFailedByIDResponse{}, nil
}

// RespondActivityTaskCanceled acknowledges an Activity's cancellation by task
// token.
func (h *Handler) RespondActivityTaskCanceled(
	ctx context.Context,
	req *apiv1.RespondActivityTaskCanceledRequest,
) (*apiv1.RespondActivityTaskCanceledResponse, error) {
	tok, err := decodeToken(req.TaskToken)
	if err != nil {
		return nil, toCadenceError(err)
	}

	_, err = h.workflow.RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
		TaskToken: tok.Temporal,
		Details:   toPayloads(req.Details),
		Identity:  req.Identity,
		Namespace: tok.Namespace,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RespondActivityTaskCanceledResponse{}, nil
}

// RespondActivityTaskCanceledByID acknowledges an Activity's cancellation by
// its ID.
func (h *Handler) RespondActivityTaskCanceledByID(
	ctx context.Context,
	req *apiv1.RespondActivityTaskCanceledByIDRequest,
) (*apiv1.RespondActivityTaskCanceledByIDResponse, error) {
	_, err := h.workflow.RespondActivityTaskCanceledById(ctx, &workflowservice.RespondActivityTaskCanceledByIdRequest{
		Namespace:  req.Domain,
		WorkflowId: req.GetWorkflowExecution().GetWorkflowId(),
		RunId:      req.GetWorkflowExecution().GetRunId(),
		ActivityId: req.ActivityId,
		Details:    toPayloads(req.Details),
		Identity:   req.Identity,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RespondActivityTaskCanceledByIDResponse{}, nil
}

// RecordActivityTaskHeartbeat records a heartbeat by task token.
func (h *Handler) RecordActivityTaskHeartbeat(
	ctx context.Context,
	req *apiv1.RecordActivityTaskHeartbeatRequest,
) (*apiv1.RecordActivityTaskHeartbeatResponse, error) {
	tok, err := decodeToken(req.TaskToken)
	if err != nil {
		return nil, toCadenceError(err)
	}

	resp, err := h.workflow.RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
		TaskToken: tok.Temporal,
		Details:   toPayloads(req.Details),
		Identity:  req.Identity,
		Namespace: tok.Namespace,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RecordActivityTaskHeartbeatResponse{CancelRequested: resp.CancelRequested}, nil
}

// RecordActivityTaskHeartbeatByID records a heartbeat by Activity ID.
func (h *Handler) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	req *apiv1.RecordActivityTaskHeartbeatByIDRequest,
) (*apiv1.RecordActivityTaskHeartbeatByIDResponse, error) {
	resp, err := h.workflow.RecordActivityTaskHeartbeatById(ctx, &workflowservice.RecordActivityTaskHeartbeatByIdRequest{
		Namespace:  req.Domain,
		WorkflowId: req.GetWorkflowExecution().GetWorkflowId(),
		RunId:      req.GetWorkflowExecution().GetRunId(),
		ActivityId: req.ActivityId,
		Details:    toPayloads(req.Details),
		Identity:   req.Identity,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RecordActivityTaskHeartbeatByIDResponse{CancelRequested: resp.CancelRequested}, nil
}

// RespondQueryTaskCompleted answers a legacy query task.
func (h *Handler) RespondQueryTaskCompleted(
	ctx context.Context,
	req *apiv1.RespondQueryTaskCompletedRequest,
) (*apiv1.RespondQueryTaskCompletedResponse, error) {
	tok, err := decodeToken(req.TaskToken)
	if err != nil {
		return nil, toCadenceError(err)
	}

	r := toQueryResult(req.Result)

	_, err = h.workflow.RespondQueryTaskCompleted(ctx, &workflowservice.RespondQueryTaskCompletedRequest{
		TaskToken:     tok.Temporal,
		CompletedType: r.GetResultType(),
		QueryResult:   r.GetAnswer(),
		ErrorMessage:  r.GetErrorMessage(),
		Namespace:     tok.Namespace,
		Failure:       r.GetFailure(),
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RespondQueryTaskCompletedResponse{}, nil
}

// ResetStickyTaskList moves a Workflow off its Worker's sticky Task Queue.
func (h *Handler) ResetStickyTaskList(
	ctx context.Context,
	req *apiv1.ResetStickyTaskListRequest,
) (*apiv1.ResetStickyTaskListResponse, error) {
	_, err := h.workflow.ResetStickyTaskQueue(ctx, &workflowservice.ResetStickyTaskQueueRequest{
		Namespace: req.Domain,
		Execution: toExecution(req.WorkflowExecution),
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.ResetStickyTaskListResponse{}, nil
}
