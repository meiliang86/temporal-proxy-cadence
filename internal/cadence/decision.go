package cadence

import (
	"context"

	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.uber.org/yarpc/yarpcerrors"
)

// scheduledEventResolver finds the scheduled event ID of the pending Activity
// with the given ID, which Temporal names an Activity by when cancelling it.
// ok is false when no pending Activity has that ID.
type scheduledEventResolver func(ctx context.Context, activityID string) (id int64, ok bool, err error)

// toCommands converts a decision task's decisions into Temporal commands. An
// empty Task Queue is left empty: both Services then default it to the
// Workflow's own. A decision's Domain is dropped: Temporal deprecated
// cross-Namespace commands, and a command without one targets the Workflow's
// own Namespace, which is what Cadence code names in practice.
//
//nolint:gocyclo // One case per decision type; splitting it would only scatter the mapping.
func toCommands(
	ctx context.Context,
	decisions []*apiv1.Decision,
	resolve scheduledEventResolver,
) ([]*commandpb.Command, error) {
	out := make([]*commandpb.Command, 0, len(decisions))

	for _, d := range decisions {
		cmd := &commandpb.Command{}

		switch a := d.Attributes.(type) {
		case *apiv1.Decision_ScheduleActivityTaskDecisionAttributes:
			x := a.ScheduleActivityTaskDecisionAttributes
			stc, sts, stcl := activityTimeouts(x.ScheduleToCloseTimeout, x.ScheduleToStartTimeout, x.StartToCloseTimeout, x.RetryPolicy)
			cmd.CommandType = enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK
			cmd.Attributes = &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             x.ActivityId,
					ActivityType:           toActivityType(x.ActivityType),
					TaskQueue:              toTaskQueue(x.TaskList, ""),
					Header:                 toHeader(x.Header),
					Input:                  toPayloads(x.Input),
					ScheduleToCloseTimeout: stc,
					ScheduleToStartTimeout: sts,
					StartToCloseTimeout:    stcl,
					HeartbeatTimeout:       toDuration(x.HeartbeatTimeout),
					RetryPolicy:            activityRetryPolicy(x.RetryPolicy),
					RequestEagerExecution:  x.RequestLocalDispatch,
				},
			}
		case *apiv1.Decision_StartTimerDecisionAttributes:
			x := a.StartTimerDecisionAttributes
			cmd.CommandType = enumspb.COMMAND_TYPE_START_TIMER
			cmd.Attributes = &commandpb.Command_StartTimerCommandAttributes{
				StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            x.TimerId,
					StartToFireTimeout: toDuration(x.StartToFireTimeout),
				},
			}
		case *apiv1.Decision_CompleteWorkflowExecutionDecisionAttributes:
			cmd.CommandType = enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION
			cmd.Attributes = &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: toPayloads(a.CompleteWorkflowExecutionDecisionAttributes.Result),
				},
			}
		case *apiv1.Decision_FailWorkflowExecutionDecisionAttributes:
			cmd.CommandType = enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION
			cmd.Attributes = &commandpb.Command_FailWorkflowExecutionCommandAttributes{
				FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: toFailure(a.FailWorkflowExecutionDecisionAttributes.Failure),
				},
			}
		case *apiv1.Decision_RequestCancelActivityTaskDecisionAttributes:
			activityID := a.RequestCancelActivityTaskDecisionAttributes.ActivityId

			id, ok, err := resolve(ctx, activityID)
			if err != nil {
				return nil, err
			}

			if !ok {
				// The Activity closed before the decision reached the Service. Cadence
				// would record RequestCancelActivityTaskFailed, which Temporal has no
				// event for; the Worker learns of the close from the next task instead.
				continue
			}

			cmd.CommandType = enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK
			cmd.Attributes = &commandpb.Command_RequestCancelActivityTaskCommandAttributes{
				RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
					ScheduledEventId: id,
				},
			}
		case *apiv1.Decision_CancelTimerDecisionAttributes:
			cmd.CommandType = enumspb.COMMAND_TYPE_CANCEL_TIMER
			cmd.Attributes = &commandpb.Command_CancelTimerCommandAttributes{
				CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
					TimerId: a.CancelTimerDecisionAttributes.TimerId,
				},
			}
		case *apiv1.Decision_CancelWorkflowExecutionDecisionAttributes:
			cmd.CommandType = enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION
			cmd.Attributes = &commandpb.Command_CancelWorkflowExecutionCommandAttributes{
				CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{
					Details: toPayloads(a.CancelWorkflowExecutionDecisionAttributes.Details),
				},
			}
		case *apiv1.Decision_RequestCancelExternalWorkflowExecutionDecisionAttributes:
			x := a.RequestCancelExternalWorkflowExecutionDecisionAttributes
			cmd.CommandType = enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION
			cmd.Attributes = &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{
				RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
					WorkflowId:        x.GetWorkflowExecution().GetWorkflowId(),
					RunId:             x.GetWorkflowExecution().GetRunId(),
					Control:           toControl(x.Control), //nolint:staticcheck // Cadence still carries control.
					ChildWorkflowOnly: x.ChildWorkflowOnly,
				},
			}
		case *apiv1.Decision_RecordMarkerDecisionAttributes:
			x := a.RecordMarkerDecisionAttributes
			cmd.CommandType = enumspb.COMMAND_TYPE_RECORD_MARKER
			cmd.Attributes = &commandpb.Command_RecordMarkerCommandAttributes{
				RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: x.MarkerName,
					Details:    toMarkerDetails(x.Details),
					Header:     toHeader(x.Header),
				},
			}
		case *apiv1.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes:
			x := a.ContinueAsNewWorkflowExecutionDecisionAttributes
			cmd.CommandType = enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION
			cmd.Attributes = &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
				ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType:         toWorkflowType(x.WorkflowType),
					TaskQueue:            toTaskQueue(x.TaskList, ""),
					Input:                toPayloads(x.Input),
					WorkflowRunTimeout:   toDuration(x.ExecutionStartToCloseTimeout),
					WorkflowTaskTimeout:  toDuration(x.TaskStartToCloseTimeout),
					BackoffStartInterval: toDuration(x.BackoffStartInterval),
					RetryPolicy:          toRetryPolicy(x.RetryPolicy),
					Initiator:            toInitiator(x.Initiator),
					Failure:              toFailure(x.Failure),
					LastCompletionResult: toPayloads(x.LastCompletionResult),
					CronSchedule:         x.CronSchedule,
					Header:               toHeader(x.Header),
					Memo:                 toMemo(x.Memo),
					SearchAttributes:     toSearchAttributes(x.SearchAttributes),
				},
			}
		case *apiv1.Decision_StartChildWorkflowExecutionDecisionAttributes:
			x := a.StartChildWorkflowExecutionDecisionAttributes
			reuse, _ := toReusePolicy(x.WorkflowIdReusePolicy)
			exec, run := workflowTimeouts(x.ExecutionStartToCloseTimeout, x.RetryPolicy, x.CronSchedule)
			cmd.CommandType = enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
			cmd.Attributes = &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
				StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					WorkflowId:               x.WorkflowId,
					WorkflowType:             toWorkflowType(x.WorkflowType),
					TaskQueue:                toTaskQueue(x.TaskList, ""),
					Input:                    toPayloads(x.Input),
					WorkflowExecutionTimeout: exec,
					WorkflowRunTimeout:       run,
					WorkflowTaskTimeout:      toDuration(x.TaskStartToCloseTimeout),
					ParentClosePolicy:        toParentClosePolicy(x.ParentClosePolicy),
					Control:                  toControl(x.Control),
					WorkflowIdReusePolicy:    reuse,
					RetryPolicy:              toRetryPolicy(x.RetryPolicy),
					CronSchedule:             x.CronSchedule,
					Header:                   toHeader(x.Header),
					Memo:                     toMemo(x.Memo),
					SearchAttributes:         toSearchAttributes(x.SearchAttributes),
				},
			}
		case *apiv1.Decision_SignalExternalWorkflowExecutionDecisionAttributes:
			x := a.SignalExternalWorkflowExecutionDecisionAttributes
			cmd.CommandType = enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION
			cmd.Attributes = &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{
				SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
					Execution:         toExecution(x.WorkflowExecution),
					SignalName:        x.SignalName,
					Input:             toPayloads(x.Input),
					Control:           toControl(x.Control), //nolint:staticcheck // Cadence still carries control.
					ChildWorkflowOnly: x.ChildWorkflowOnly,
				},
			}
		case *apiv1.Decision_UpsertWorkflowSearchAttributesDecisionAttributes:
			cmd.CommandType = enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES
			cmd.Attributes = &commandpb.Command_UpsertWorkflowSearchAttributesCommandAttributes{
				UpsertWorkflowSearchAttributesCommandAttributes: &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{
					SearchAttributes: toSearchAttributes(a.UpsertWorkflowSearchAttributesDecisionAttributes.SearchAttributes),
				},
			}
		default:
			return nil, yarpcerrors.UnimplementedErrorf("decision %T has no Temporal counterpart", d.Attributes)
		}

		out = append(out, cmd)
	}

	return out, nil
}

// activityRetryPolicy is [toRetryPolicy] for an Activity. Temporal retries an
// Activity by default while Cadence does not, so an Activity scheduled without
// a policy gets one that allows a single attempt.
func activityRetryPolicy(p *apiv1.RetryPolicy) *commonpb.RetryPolicy {
	if p == nil {
		return &commonpb.RetryPolicy{MaximumAttempts: 1}
	}

	return toRetryPolicy(p)
}
