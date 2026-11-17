package cadence

import (
	"context"
	"encoding/base64"
	"strings"
	"unicode/utf8"

	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
)

// activityIDResolver finds the Activity ID of the ActivityTaskScheduled event
// with the given ID, for when that event is not in the page being converted.
type activityIDResolver func(ctx context.Context, scheduledEventID int64) (string, error)

// historyConverter turns Temporal history events into Cadence ones. Every event
// maps one to one except ActivityTaskCancelRequested, which Cadence keys by
// Activity ID and Temporal by scheduled event ID. The converter remembers the
// Activity ID of every ActivityTaskScheduled event it sees, and asks resolve
// for one it has not, which happens with sticky (partial) history.
//
// Temporal events Cadence has no counterpart for (Updates, Nexus operations,
// property modifications) are dropped. A Cadence Worker never issues the
// commands behind them.
//
// An event that names no Namespace (Temporal leaves it empty on a command that
// targets the Workflow's own, and the proxy sends every command that way) is
// reported in namespace, the Workflow's own: Cadence replay compares the
// Domain on the event with the one on the decision.
type historyConverter struct {
	namespace   string
	activityIDs map[int64]string
	resolve     activityIDResolver
}

func newHistoryConverter(namespace string, resolve activityIDResolver) *historyConverter {
	return &historyConverter{namespace: namespace, activityIDs: map[int64]string{}, resolve: resolve}
}

// domain is the Domain an event names: its own Namespace, or the Workflow's.
func (c *historyConverter) domain(namespace string) string {
	if namespace == "" {
		return c.namespace
	}

	return namespace
}

func (c *historyConverter) history(ctx context.Context, h *historypb.History) (*apiv1.History, error) {
	if h == nil {
		return nil, nil
	}

	events, err := c.events(ctx, h.Events)
	if err != nil {
		return nil, err
	}

	return &apiv1.History{Events: events}, nil
}

func (c *historyConverter) events(ctx context.Context, in []*historypb.HistoryEvent) ([]*apiv1.HistoryEvent, error) {
	out := make([]*apiv1.HistoryEvent, 0, len(in))
	for _, e := range in {
		ev, err := c.event(ctx, e)
		if err != nil {
			return nil, err
		}

		if ev != nil {
			out = append(out, ev)
		}
	}

	return out, nil
}

func (c *historyConverter) activityID(ctx context.Context, scheduledEventID int64) (string, error) {
	if id, ok := c.activityIDs[scheduledEventID]; ok {
		return id, nil
	}

	if c.resolve == nil {
		return "", nil
	}

	id, err := c.resolve(ctx, scheduledEventID)
	if err != nil {
		return "", err
	}

	c.activityIDs[scheduledEventID] = id

	return id, nil
}

//nolint:gocyclo,maintidx // One case per event type; splitting it would only scatter the mapping.
func (c *historyConverter) event(ctx context.Context, e *historypb.HistoryEvent) (*apiv1.HistoryEvent, error) {
	out := &apiv1.HistoryEvent{
		EventId:   e.EventId,
		EventTime: fromTimestamp(e.EventTime),
		Version:   e.Version,
		TaskId:    e.TaskId,
	}

	switch a := e.Attributes.(type) {
	case *historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes:
		out.Attributes = &apiv1.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: fromStarted(a.WorkflowExecutionStartedEventAttributes),
		}
	case *historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes:
		x := a.WorkflowExecutionCompletedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &apiv1.WorkflowExecutionCompletedEventAttributes{
				Result:                       fromPayloads(x.Result),
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
			},
		}
	case *historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes:
		x := a.WorkflowExecutionFailedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &apiv1.WorkflowExecutionFailedEventAttributes{
				Failure:                      fromFailure(x.Failure),
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
			},
		}
	case *historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes:
		out.Attributes = &apiv1.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
			WorkflowExecutionTimedOutEventAttributes: &apiv1.WorkflowExecutionTimedOutEventAttributes{
				TimeoutType: apiv1.TimeoutType_TIMEOUT_TYPE_START_TO_CLOSE,
			},
		}
	case *historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes:
		x := a.WorkflowTaskScheduledEventAttributes
		out.Attributes = &apiv1.HistoryEvent_DecisionTaskScheduledEventAttributes{
			DecisionTaskScheduledEventAttributes: &apiv1.DecisionTaskScheduledEventAttributes{
				TaskList:            fromTaskQueue(x.TaskQueue),
				StartToCloseTimeout: fromDuration(x.StartToCloseTimeout),
				Attempt:             fromAttempt(x.Attempt),
			},
		}
	case *historypb.HistoryEvent_WorkflowTaskStartedEventAttributes:
		x := a.WorkflowTaskStartedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_DecisionTaskStartedEventAttributes{
			DecisionTaskStartedEventAttributes: &apiv1.DecisionTaskStartedEventAttributes{
				ScheduledEventId: x.ScheduledEventId,
				Identity:         x.Identity,
				RequestId:        x.RequestId,
			},
		}
	case *historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes:
		x := a.WorkflowTaskCompletedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_DecisionTaskCompletedEventAttributes{
			DecisionTaskCompletedEventAttributes: &apiv1.DecisionTaskCompletedEventAttributes{
				ScheduledEventId: x.ScheduledEventId,
				StartedEventId:   x.StartedEventId,
				Identity:         x.Identity,
				BinaryChecksum:   x.BinaryChecksum, //nolint:staticcheck // Cadence still keys reset points by it.
			},
		}
	case *historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes:
		x := a.WorkflowTaskTimedOutEventAttributes
		out.Attributes = &apiv1.HistoryEvent_DecisionTaskTimedOutEventAttributes{
			DecisionTaskTimedOutEventAttributes: &apiv1.DecisionTaskTimedOutEventAttributes{
				ScheduledEventId: x.ScheduledEventId,
				StartedEventId:   x.StartedEventId,
				TimeoutType:      fromTimeoutType(x.TimeoutType),
				Cause:            apiv1.DecisionTaskTimedOutCause_DECISION_TASK_TIMED_OUT_CAUSE_TIMEOUT,
			},
		}
	case *historypb.HistoryEvent_WorkflowTaskFailedEventAttributes:
		x := a.WorkflowTaskFailedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_DecisionTaskFailedEventAttributes{
			DecisionTaskFailedEventAttributes: &apiv1.DecisionTaskFailedEventAttributes{
				ScheduledEventId: x.ScheduledEventId,
				StartedEventId:   x.StartedEventId,
				Cause:            fromDecisionFailedCause(x.Cause),
				Failure:          fromFailure(x.Failure),
				Identity:         x.Identity,
				BaseRunId:        x.BaseRunId,
				NewRunId:         x.NewRunId,
				ForkEventVersion: x.ForkEventVersion,
				BinaryChecksum:   x.BinaryChecksum, //nolint:staticcheck // Cadence still keys reset points by it.
			},
		}
	case *historypb.HistoryEvent_ActivityTaskScheduledEventAttributes:
		x := a.ActivityTaskScheduledEventAttributes
		c.activityIDs[e.EventId] = x.ActivityId
		out.Attributes = &apiv1.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &apiv1.ActivityTaskScheduledEventAttributes{
				ActivityId:                   x.ActivityId,
				ActivityType:                 fromActivityType(x.ActivityType),
				TaskList:                     fromTaskQueue(x.TaskQueue),
				Input:                        fromPayloads(x.Input),
				ScheduleToCloseTimeout:       fromDuration(x.ScheduleToCloseTimeout),
				ScheduleToStartTimeout:       fromDuration(x.ScheduleToStartTimeout),
				StartToCloseTimeout:          fromDuration(x.StartToCloseTimeout),
				HeartbeatTimeout:             fromDuration(x.HeartbeatTimeout),
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
				RetryPolicy:                  fromRetryPolicy(x.RetryPolicy, x.ScheduleToCloseTimeout),
				Header:                       fromHeader(x.Header),
			},
		}
	case *historypb.HistoryEvent_ActivityTaskStartedEventAttributes:
		x := a.ActivityTaskStartedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &apiv1.ActivityTaskStartedEventAttributes{
				ScheduledEventId: x.ScheduledEventId,
				Identity:         x.Identity,
				RequestId:        x.RequestId,
				Attempt:          fromAttempt(x.Attempt),
				LastFailure:      fromFailure(x.LastFailure),
			},
		}
	case *historypb.HistoryEvent_ActivityTaskCompletedEventAttributes:
		x := a.ActivityTaskCompletedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &apiv1.ActivityTaskCompletedEventAttributes{
				Result:           fromPayloads(x.Result),
				ScheduledEventId: x.ScheduledEventId,
				StartedEventId:   x.StartedEventId,
				Identity:         x.Identity,
			},
		}
	case *historypb.HistoryEvent_ActivityTaskFailedEventAttributes:
		x := a.ActivityTaskFailedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ActivityTaskFailedEventAttributes{
			ActivityTaskFailedEventAttributes: &apiv1.ActivityTaskFailedEventAttributes{
				Failure:          fromFailure(x.Failure),
				ScheduledEventId: x.ScheduledEventId,
				StartedEventId:   x.StartedEventId,
				Identity:         x.Identity,
			},
		}
	case *historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes:
		x := a.ActivityTaskTimedOutEventAttributes
		attrs := &apiv1.ActivityTaskTimedOutEventAttributes{
			ScheduledEventId: x.ScheduledEventId,
			StartedEventId:   x.StartedEventId,
			TimeoutType:      apiv1.TimeoutType_TIMEOUT_TYPE_START_TO_CLOSE,
		}

		if info := x.GetFailure().GetTimeoutFailureInfo(); info != nil {
			attrs.TimeoutType = fromTimeoutType(info.TimeoutType)
			attrs.Details = fromPayloads(info.LastHeartbeatDetails)
		}

		if cause := x.GetFailure().GetCause(); cause != nil {
			attrs.LastFailure = fromFailure(cause)
		}

		out.Attributes = &apiv1.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: attrs}
	case *historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes:
		x := a.ActivityTaskCancelRequestedEventAttributes

		id, err := c.activityID(ctx, x.ScheduledEventId)
		if err != nil {
			return nil, err
		}

		out.Attributes = &apiv1.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{
			ActivityTaskCancelRequestedEventAttributes: &apiv1.ActivityTaskCancelRequestedEventAttributes{
				ActivityId:                   id,
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
			},
		}
	case *historypb.HistoryEvent_ActivityTaskCanceledEventAttributes:
		x := a.ActivityTaskCanceledEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ActivityTaskCanceledEventAttributes{
			ActivityTaskCanceledEventAttributes: &apiv1.ActivityTaskCanceledEventAttributes{
				Details:                      fromPayloads(x.Details),
				LatestCancelRequestedEventId: x.LatestCancelRequestedEventId,
				ScheduledEventId:             x.ScheduledEventId,
				StartedEventId:               x.StartedEventId,
				Identity:                     x.Identity,
			},
		}
	case *historypb.HistoryEvent_TimerStartedEventAttributes:
		x := a.TimerStartedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_TimerStartedEventAttributes{
			TimerStartedEventAttributes: &apiv1.TimerStartedEventAttributes{
				TimerId:                      x.TimerId,
				StartToFireTimeout:           fromDuration(x.StartToFireTimeout),
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
			},
		}
	case *historypb.HistoryEvent_TimerFiredEventAttributes:
		x := a.TimerFiredEventAttributes
		out.Attributes = &apiv1.HistoryEvent_TimerFiredEventAttributes{
			TimerFiredEventAttributes: &apiv1.TimerFiredEventAttributes{
				TimerId:        x.TimerId,
				StartedEventId: x.StartedEventId,
			},
		}
	case *historypb.HistoryEvent_TimerCanceledEventAttributes:
		x := a.TimerCanceledEventAttributes
		out.Attributes = &apiv1.HistoryEvent_TimerCanceledEventAttributes{
			TimerCanceledEventAttributes: &apiv1.TimerCanceledEventAttributes{
				TimerId:                      x.TimerId,
				StartedEventId:               x.StartedEventId,
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
				Identity:                     x.Identity,
			},
		}
	case *historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes:
		x := a.WorkflowExecutionCancelRequestedEventAttributes
		attrs := &apiv1.WorkflowExecutionCancelRequestedEventAttributes{Cause: x.Cause, Identity: x.Identity}
		if x.ExternalWorkflowExecution != nil {
			attrs.ExternalExecutionInfo = &apiv1.ExternalExecutionInfo{
				WorkflowExecution: fromExecution(x.ExternalWorkflowExecution),
				InitiatedId:       x.ExternalInitiatedEventId,
			}
		}

		out.Attributes = &apiv1.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
			WorkflowExecutionCancelRequestedEventAttributes: attrs,
		}
	case *historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes:
		x := a.WorkflowExecutionCanceledEventAttributes
		out.Attributes = &apiv1.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
			WorkflowExecutionCanceledEventAttributes: &apiv1.WorkflowExecutionCanceledEventAttributes{
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
				Details:                      fromPayloads(x.Details),
			},
		}
	case *historypb.HistoryEvent_MarkerRecordedEventAttributes:
		x := a.MarkerRecordedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_MarkerRecordedEventAttributes{
			MarkerRecordedEventAttributes: &apiv1.MarkerRecordedEventAttributes{
				MarkerName:                   x.MarkerName,
				Details:                      fromMarkerDetails(x.Details),
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
				Header:                       fromHeader(x.Header),
			},
		}
	case *historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes:
		x := a.WorkflowExecutionSignaledEventAttributes
		out.Attributes = &apiv1.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &apiv1.WorkflowExecutionSignaledEventAttributes{
				SignalName: x.SignalName,
				Input:      fromPayloads(x.Input),
				Identity:   x.Identity,
			},
		}
	case *historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes:
		x := a.WorkflowExecutionTerminatedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &apiv1.WorkflowExecutionTerminatedEventAttributes{
				Reason:   x.Reason,
				Details:  fromPayloads(x.Details),
				Identity: x.Identity,
			},
		}
	case *historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes:
		x := a.WorkflowExecutionContinuedAsNewEventAttributes
		out.Attributes = &apiv1.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
			WorkflowExecutionContinuedAsNewEventAttributes: &apiv1.WorkflowExecutionContinuedAsNewEventAttributes{
				NewExecutionRunId:            x.NewExecutionRunId,
				WorkflowType:                 fromWorkflowType(x.WorkflowType),
				TaskList:                     fromTaskQueue(x.TaskQueue),
				Input:                        fromPayloads(x.Input),
				ExecutionStartToCloseTimeout: fromDuration(x.WorkflowRunTimeout),
				TaskStartToCloseTimeout:      fromDuration(x.WorkflowTaskTimeout),
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
				BackoffStartInterval:         fromDuration(x.BackoffStartInterval),
				Initiator:                    fromInitiator(x.Initiator),
				Failure:                      fromFailure(x.Failure), //nolint:staticcheck // Cadence still reports it.
				LastCompletionResult:         fromPayloads(x.LastCompletionResult),
				Header:                       fromHeader(x.Header),
				Memo:                         fromMemo(x.Memo),
				SearchAttributes:             fromSearchAttributes(x.SearchAttributes),
			},
		}
	case *historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes:
		x := a.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &apiv1.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
				Domain:                       c.domain(x.Namespace),
				WorkflowExecution:            fromExecution(x.WorkflowExecution),
				Control:                      fromControl(x.Control), //nolint:staticcheck // Cadence still carries control.
				ChildWorkflowOnly:            x.ChildWorkflowOnly,
			},
		}
	case *historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes:
		x := a.RequestCancelExternalWorkflowExecutionFailedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			RequestCancelExternalWorkflowExecutionFailedEventAttributes: &apiv1.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
				Cause:                        fromCancelExternalCause(x.Cause),
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
				Domain:                       c.domain(x.Namespace),
				WorkflowExecution:            fromExecution(x.WorkflowExecution),
				InitiatedEventId:             x.InitiatedEventId,
				Control:                      fromControl(x.Control), //nolint:staticcheck // Cadence still carries control.
			},
		}
	case *historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes:
		x := a.ExternalWorkflowExecutionCancelRequestedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{
			ExternalWorkflowExecutionCancelRequestedEventAttributes: &apiv1.ExternalWorkflowExecutionCancelRequestedEventAttributes{
				InitiatedEventId:  x.InitiatedEventId,
				Domain:            c.domain(x.Namespace),
				WorkflowExecution: fromExecution(x.WorkflowExecution),
			},
		}
	case *historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes:
		x := a.StartChildWorkflowExecutionInitiatedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
			StartChildWorkflowExecutionInitiatedEventAttributes: &apiv1.StartChildWorkflowExecutionInitiatedEventAttributes{
				Domain:                       c.domain(x.Namespace),
				WorkflowId:                   x.WorkflowId,
				WorkflowType:                 fromWorkflowType(x.WorkflowType),
				TaskList:                     fromTaskQueue(x.TaskQueue),
				Input:                        fromPayloads(x.Input),
				ExecutionStartToCloseTimeout: fromDuration(x.WorkflowRunTimeout),
				TaskStartToCloseTimeout:      fromDuration(x.WorkflowTaskTimeout),
				ParentClosePolicy:            fromParentClosePolicy(x.ParentClosePolicy),
				Control:                      fromControl(x.Control), //nolint:staticcheck // Cadence still carries control.
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
				WorkflowIdReusePolicy:        fromReusePolicy(x.WorkflowIdReusePolicy),
				RetryPolicy:                  fromRetryPolicy(x.RetryPolicy, x.WorkflowExecutionTimeout),
				CronSchedule:                 x.CronSchedule,
				Header:                       fromHeader(x.Header),
				Memo:                         fromMemo(x.Memo),
				SearchAttributes:             fromSearchAttributes(x.SearchAttributes),
			},
		}
	case *historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes:
		x := a.StartChildWorkflowExecutionFailedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{
			StartChildWorkflowExecutionFailedEventAttributes: &apiv1.StartChildWorkflowExecutionFailedEventAttributes{
				Domain:                       c.domain(x.Namespace),
				WorkflowId:                   x.WorkflowId,
				WorkflowType:                 fromWorkflowType(x.WorkflowType),
				Cause:                        fromChildFailedCause(x.Cause),
				Control:                      fromControl(x.Control), //nolint:staticcheck // Cadence still carries control.
				InitiatedEventId:             x.InitiatedEventId,
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
			},
		}
	case *historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes:
		x := a.ChildWorkflowExecutionStartedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{
			ChildWorkflowExecutionStartedEventAttributes: &apiv1.ChildWorkflowExecutionStartedEventAttributes{
				Domain:            c.domain(x.Namespace),
				WorkflowExecution: fromExecution(x.WorkflowExecution),
				WorkflowType:      fromWorkflowType(x.WorkflowType),
				InitiatedEventId:  x.InitiatedEventId,
				Header:            fromHeader(x.Header),
			},
		}
	case *historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes:
		x := a.ChildWorkflowExecutionCompletedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
			ChildWorkflowExecutionCompletedEventAttributes: &apiv1.ChildWorkflowExecutionCompletedEventAttributes{
				Domain:            c.domain(x.Namespace),
				WorkflowExecution: fromExecution(x.WorkflowExecution),
				WorkflowType:      fromWorkflowType(x.WorkflowType),
				InitiatedEventId:  x.InitiatedEventId,
				StartedEventId:    x.StartedEventId,
				Result:            fromPayloads(x.Result),
			},
		}
	case *historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes:
		x := a.ChildWorkflowExecutionFailedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
			ChildWorkflowExecutionFailedEventAttributes: &apiv1.ChildWorkflowExecutionFailedEventAttributes{
				Domain:            c.domain(x.Namespace),
				WorkflowExecution: fromExecution(x.WorkflowExecution),
				WorkflowType:      fromWorkflowType(x.WorkflowType),
				InitiatedEventId:  x.InitiatedEventId,
				StartedEventId:    x.StartedEventId,
				Failure:           fromFailure(x.Failure),
			},
		}
	case *historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes:
		x := a.ChildWorkflowExecutionCanceledEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{
			ChildWorkflowExecutionCanceledEventAttributes: &apiv1.ChildWorkflowExecutionCanceledEventAttributes{
				Domain:            c.domain(x.Namespace),
				WorkflowExecution: fromExecution(x.WorkflowExecution),
				WorkflowType:      fromWorkflowType(x.WorkflowType),
				InitiatedEventId:  x.InitiatedEventId,
				StartedEventId:    x.StartedEventId,
				Details:           fromPayloads(x.Details),
			},
		}
	case *historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes:
		x := a.ChildWorkflowExecutionTimedOutEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{
			ChildWorkflowExecutionTimedOutEventAttributes: &apiv1.ChildWorkflowExecutionTimedOutEventAttributes{
				Domain:            c.domain(x.Namespace),
				WorkflowExecution: fromExecution(x.WorkflowExecution),
				WorkflowType:      fromWorkflowType(x.WorkflowType),
				InitiatedEventId:  x.InitiatedEventId,
				StartedEventId:    x.StartedEventId,
				TimeoutType:       apiv1.TimeoutType_TIMEOUT_TYPE_START_TO_CLOSE,
			},
		}
	case *historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes:
		x := a.ChildWorkflowExecutionTerminatedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{
			ChildWorkflowExecutionTerminatedEventAttributes: &apiv1.ChildWorkflowExecutionTerminatedEventAttributes{
				Domain:            c.domain(x.Namespace),
				WorkflowExecution: fromExecution(x.WorkflowExecution),
				WorkflowType:      fromWorkflowType(x.WorkflowType),
				InitiatedEventId:  x.InitiatedEventId,
				StartedEventId:    x.StartedEventId,
			},
		}
	case *historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes:
		x := a.SignalExternalWorkflowExecutionInitiatedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
			SignalExternalWorkflowExecutionInitiatedEventAttributes: &apiv1.SignalExternalWorkflowExecutionInitiatedEventAttributes{
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
				Domain:                       c.domain(x.Namespace),
				WorkflowExecution:            fromExecution(x.WorkflowExecution),
				SignalName:                   x.SignalName,
				Input:                        fromPayloads(x.Input),
				Control:                      fromControl(x.Control), //nolint:staticcheck // Cadence still carries control.
				ChildWorkflowOnly:            x.ChildWorkflowOnly,
			},
		}
	case *historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes:
		x := a.SignalExternalWorkflowExecutionFailedEventAttributes
		out.Attributes = &apiv1.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{
			SignalExternalWorkflowExecutionFailedEventAttributes: &apiv1.SignalExternalWorkflowExecutionFailedEventAttributes{
				Cause:                        fromSignalExternalCause(x.Cause),
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
				Domain:                       c.domain(x.Namespace),
				WorkflowExecution:            fromExecution(x.WorkflowExecution),
				InitiatedEventId:             x.InitiatedEventId,
				Control:                      fromControl(x.Control), //nolint:staticcheck // Cadence still carries control.
			},
		}
	case *historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes:
		x := a.ExternalWorkflowExecutionSignaledEventAttributes
		out.Attributes = &apiv1.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{
			ExternalWorkflowExecutionSignaledEventAttributes: &apiv1.ExternalWorkflowExecutionSignaledEventAttributes{
				InitiatedEventId:  x.InitiatedEventId,
				Domain:            c.domain(x.Namespace),
				WorkflowExecution: fromExecution(x.WorkflowExecution),
				Control:           fromControl(x.Control), //nolint:staticcheck // Cadence still carries control.
			},
		}
	case *historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes:
		x := a.UpsertWorkflowSearchAttributesEventAttributes
		out.Attributes = &apiv1.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{
			UpsertWorkflowSearchAttributesEventAttributes: &apiv1.UpsertWorkflowSearchAttributesEventAttributes{
				DecisionTaskCompletedEventId: x.WorkflowTaskCompletedEventId,
				SearchAttributes:             fromSearchAttributes(x.SearchAttributes),
			},
		}
	default:
		return nil, nil
	}

	return out, nil
}

func fromStarted(x *historypb.WorkflowExecutionStartedEventAttributes) *apiv1.WorkflowExecutionStartedEventAttributes {
	out := &apiv1.WorkflowExecutionStartedEventAttributes{
		WorkflowType:                 fromWorkflowType(x.WorkflowType),
		TaskList:                     fromTaskQueue(x.TaskQueue),
		Input:                        fromPayloads(x.Input),
		ExecutionStartToCloseTimeout: fromDuration(x.WorkflowRunTimeout),
		TaskStartToCloseTimeout:      fromDuration(x.WorkflowTaskTimeout),
		ContinuedExecutionRunId:      x.ContinuedExecutionRunId,
		Initiator:                    fromInitiator(x.Initiator),
		ContinuedFailure:             fromFailure(x.ContinuedFailure),
		LastCompletionResult:         fromPayloads(x.LastCompletionResult),
		OriginalExecutionRunId:       x.OriginalExecutionRunId,
		Identity:                     x.Identity,
		FirstExecutionRunId:          x.FirstExecutionRunId,
		RetryPolicy:                  fromRetryPolicy(x.RetryPolicy, x.WorkflowExecutionTimeout),
		Attempt:                      fromAttempt(x.Attempt),
		ExpirationTime:               fromTimestamp(x.WorkflowExecutionExpirationTime),
		CronSchedule:                 x.CronSchedule,
		FirstDecisionTaskBackoff:     fromDuration(x.FirstWorkflowTaskBackoff),
		Memo:                         fromMemo(x.Memo),
		SearchAttributes:             fromSearchAttributes(x.SearchAttributes),
		PrevAutoResetPoints:          fromResetPoints(x.PrevAutoResetPoints),
		Header:                       fromHeader(x.Header),
	}

	if x.ParentWorkflowExecution != nil {
		out.ParentExecutionInfo = &apiv1.ParentExecutionInfo{
			DomainId:          x.ParentWorkflowNamespaceId,
			DomainName:        x.ParentWorkflowNamespace,
			WorkflowExecution: fromExecution(x.ParentWorkflowExecution),
			InitiatedId:       x.ParentInitiatedEventId,
		}
	}

	return out
}

func fromResetPoints(p *workflowpb.ResetPoints) *apiv1.ResetPoints {
	if p == nil {
		return nil
	}

	out := &apiv1.ResetPoints{}
	for _, pt := range p.Points {
		out.Points = append(out.Points, &apiv1.ResetPointInfo{
			BinaryChecksum:           pt.BinaryChecksum, //nolint:staticcheck // Cadence still keys reset points by it.
			RunId:                    pt.RunId,
			FirstDecisionCompletedId: pt.FirstWorkflowTaskCompletedId,
			CreatedTime:              fromTimestamp(pt.CreateTime),
			ExpiringTime:             fromTimestamp(pt.ExpireTime),
			Resettable:               pt.Resettable,
		})
	}

	return out
}

// toMarkerDetails and fromMarkerDetails carry a Cadence marker's single
// details blob under [markerDetailsKey].
func toMarkerDetails(p *apiv1.Payload) map[string]*commonpb.Payloads {
	if p == nil || len(p.Data) == 0 {
		return nil
	}

	return map[string]*commonpb.Payloads{markerDetailsKey: toPayloads(p)}
}

func fromMarkerDetails(d map[string]*commonpb.Payloads) *apiv1.Payload {
	if p, ok := d[markerDetailsKey]; ok {
		return fromPayloads(p)
	}

	return nil
}

// controlPrefix marks a Cadence control blob that was not valid UTF-8 and was
// therefore base64-encoded into Temporal's string control field.
const controlPrefix = "cadence-b64:"

func toControl(b []byte) string {
	if len(b) == 0 {
		return ""
	}

	s := string(b)
	if utf8.ValidString(s) && !strings.HasPrefix(s, controlPrefix) {
		return s
	}

	return controlPrefix + base64.StdEncoding.EncodeToString(b)
}

func fromControl(s string) []byte {
	if s == "" {
		return nil
	}

	if rest, ok := strings.CutPrefix(s, controlPrefix); ok {
		if b, err := base64.StdEncoding.DecodeString(rest); err == nil {
			return b
		}
	}

	return []byte(s)
}
