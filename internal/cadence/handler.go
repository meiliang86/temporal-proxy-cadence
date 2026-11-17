package cadence

import (
	"context"

	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/yarpc/yarpcerrors"

	"go.temporal.io/proxy/pkg/logger"
)

// Handler implements the Cadence proto API (the uber.cadence.api.v1 Domain,
// Workflow, Worker, Visibility, and Meta services) on top of a Temporal
// WorkflowService. Every method translates its request, makes the matching
// Temporal call, and translates the response or error back.
type Handler struct {
	workflow workflowservice.WorkflowServiceClient
	operator operatorservice.OperatorServiceClient
	log      logger.Logger
}

var (
	_ apiv1.DomainAPIYARPCServer     = (*Handler)(nil)
	_ apiv1.WorkflowAPIYARPCServer   = (*Handler)(nil)
	_ apiv1.WorkerAPIYARPCServer     = (*Handler)(nil)
	_ apiv1.VisibilityAPIYARPCServer = (*Handler)(nil)
	_ apiv1.MetaAPIYARPCServer       = (*Handler)(nil)
)

// NewHandler returns a Handler that calls workflow, and operator for the few
// Cadence calls only the OperatorService can answer.
func NewHandler(
	workflow workflowservice.WorkflowServiceClient,
	operator operatorservice.OperatorServiceClient,
	log logger.Logger,
) *Handler {
	if log == nil {
		log = logger.NewNoopLogger()
	}

	return &Handler{workflow: workflow, operator: operator, log: log}
}

// unimplemented is returned by Cadence calls with no Temporal counterpart.
func unimplemented(method string) error {
	return yarpcerrors.UnimplementedErrorf("%s has no Temporal counterpart, so the Cadence frontend does not support it", method)
}

// historyPages walks a Workflow Execution's history from the start, a page at
// a time, until visit returns false or the history ends.
func (h *Handler) historyPages(
	ctx context.Context,
	namespace string,
	exec *commonpb.WorkflowExecution,
	visit func([]*historypb.HistoryEvent) bool,
) error {
	var token []byte

	for {
		resp, err := h.workflow.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:     namespace,
			Execution:     exec,
			NextPageToken: token,
		})
		if err != nil {
			return err
		}

		if !visit(resp.GetHistory().GetEvents()) || len(resp.NextPageToken) == 0 {
			return nil
		}

		token = resp.NextPageToken
	}
}

// activityIDResolver returns a resolver that looks an ActivityTaskScheduled
// event up in exec's history.
func (h *Handler) activityIDResolver(namespace string, exec *commonpb.WorkflowExecution) activityIDResolver {
	return func(ctx context.Context, scheduledEventID int64) (string, error) {
		var id string

		err := h.historyPages(ctx, namespace, exec, func(events []*historypb.HistoryEvent) bool {
			for _, e := range events {
				if e.EventId == scheduledEventID {
					id = e.GetActivityTaskScheduledEventAttributes().GetActivityId()
					return false
				}

				if e.EventId > scheduledEventID {
					return false
				}
			}

			return true
		})

		return id, err
	}
}

// scheduledEventResolver returns a resolver that finds the pending Activity
// with a given ID in exec's history: the latest ActivityTaskScheduled event
// with that ID that no later event closed.
func (h *Handler) scheduledEventResolver(namespace string, exec *commonpb.WorkflowExecution) scheduledEventResolver {
	var pending map[string]int64

	return func(ctx context.Context, activityID string) (int64, bool, error) {
		if pending == nil {
			pending = map[string]int64{}
			ids := map[int64]string{}

			err := h.historyPages(ctx, namespace, exec, func(events []*historypb.HistoryEvent) bool {
				for _, e := range events {
					if a := e.GetActivityTaskScheduledEventAttributes(); a != nil {
						pending[a.ActivityId] = e.EventId
						ids[e.EventId] = a.ActivityId

						continue
					}

					if sched := closedActivity(e); sched != 0 {
						if id, ok := ids[sched]; ok && pending[id] == sched {
							delete(pending, id)
						}
					}
				}

				return true
			})
			if err != nil {
				pending = nil
				return 0, false, err
			}
		}

		id, ok := pending[activityID]

		return id, ok, nil
	}
}

// closedActivity returns the scheduled event ID of the Activity e closes, or 0.
func closedActivity(e *historypb.HistoryEvent) int64 {
	switch e.EventType {
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		return e.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		return e.GetActivityTaskFailedEventAttributes().GetScheduledEventId()
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		return e.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId()
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		return e.GetActivityTaskCanceledEventAttributes().GetScheduledEventId()
	default:
		return 0
	}
}

// Health answers the Cadence MetaAPI health check.
func (h *Handler) Health(context.Context, *apiv1.HealthRequest) (*apiv1.HealthResponse, error) {
	return &apiv1.HealthResponse{Ok: true, Message: "OK"}, nil
}
