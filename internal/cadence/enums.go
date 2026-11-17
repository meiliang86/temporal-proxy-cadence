package cadence

import (
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

// Cadence and Temporal share most enum values by name but not by number, so
// every mapping is spelled out; a value with no counterpart maps to the zero
// (unspecified) value of the other side.

func toReusePolicy(p apiv1.WorkflowIdReusePolicy) (enumspb.WorkflowIdReusePolicy, enumspb.WorkflowIdConflictPolicy) {
	switch p {
	case apiv1.WorkflowIdReusePolicy_WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY:
		return enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY, enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED
	case apiv1.WorkflowIdReusePolicy_WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE:
		return enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED
	case apiv1.WorkflowIdReusePolicy_WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE:
		return enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED
	case apiv1.WorkflowIdReusePolicy_WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING:
		// Temporal splits "terminate if running" into a conflict policy.
		return enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
	default:
		return enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED
	}
}

func fromReusePolicy(p enumspb.WorkflowIdReusePolicy) apiv1.WorkflowIdReusePolicy {
	switch p {
	case enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY:
		return apiv1.WorkflowIdReusePolicy_WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	case enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE:
		return apiv1.WorkflowIdReusePolicy_WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	case enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE:
		return apiv1.WorkflowIdReusePolicy_WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	case enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING: //nolint:staticcheck // Older histories still carry it.
		return apiv1.WorkflowIdReusePolicy_WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
	default:
		return apiv1.WorkflowIdReusePolicy_WORKFLOW_ID_REUSE_POLICY_INVALID
	}
}

func toParentClosePolicy(p apiv1.ParentClosePolicy) enumspb.ParentClosePolicy {
	switch p {
	case apiv1.ParentClosePolicy_PARENT_CLOSE_POLICY_ABANDON:
		return enumspb.PARENT_CLOSE_POLICY_ABANDON
	case apiv1.ParentClosePolicy_PARENT_CLOSE_POLICY_REQUEST_CANCEL:
		return enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL
	case apiv1.ParentClosePolicy_PARENT_CLOSE_POLICY_TERMINATE:
		return enumspb.PARENT_CLOSE_POLICY_TERMINATE
	default:
		return enumspb.PARENT_CLOSE_POLICY_UNSPECIFIED
	}
}

func fromParentClosePolicy(p enumspb.ParentClosePolicy) apiv1.ParentClosePolicy {
	switch p {
	case enumspb.PARENT_CLOSE_POLICY_ABANDON:
		return apiv1.ParentClosePolicy_PARENT_CLOSE_POLICY_ABANDON
	case enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL:
		return apiv1.ParentClosePolicy_PARENT_CLOSE_POLICY_REQUEST_CANCEL
	case enumspb.PARENT_CLOSE_POLICY_TERMINATE:
		return apiv1.ParentClosePolicy_PARENT_CLOSE_POLICY_TERMINATE
	default:
		return apiv1.ParentClosePolicy_PARENT_CLOSE_POLICY_INVALID
	}
}

func toInitiator(i apiv1.ContinueAsNewInitiator) enumspb.ContinueAsNewInitiator {
	switch i {
	case apiv1.ContinueAsNewInitiator_CONTINUE_AS_NEW_INITIATOR_DECIDER:
		return enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW
	case apiv1.ContinueAsNewInitiator_CONTINUE_AS_NEW_INITIATOR_RETRY_POLICY:
		return enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY
	case apiv1.ContinueAsNewInitiator_CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE:
		return enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE
	default:
		return enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED
	}
}

func fromInitiator(i enumspb.ContinueAsNewInitiator) apiv1.ContinueAsNewInitiator {
	switch i {
	case enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW:
		return apiv1.ContinueAsNewInitiator_CONTINUE_AS_NEW_INITIATOR_DECIDER
	case enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY:
		return apiv1.ContinueAsNewInitiator_CONTINUE_AS_NEW_INITIATOR_RETRY_POLICY
	case enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE:
		return apiv1.ContinueAsNewInitiator_CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE
	default:
		return apiv1.ContinueAsNewInitiator_CONTINUE_AS_NEW_INITIATOR_INVALID
	}
}

// fromTimeoutType relies on both enums numbering the four timeout types alike.
func fromTimeoutType(t enumspb.TimeoutType) apiv1.TimeoutType {
	switch t {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		return apiv1.TimeoutType_TIMEOUT_TYPE_START_TO_CLOSE
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
		return apiv1.TimeoutType_TIMEOUT_TYPE_SCHEDULE_TO_START
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		return apiv1.TimeoutType_TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		return apiv1.TimeoutType_TIMEOUT_TYPE_HEARTBEAT
	default:
		return apiv1.TimeoutType_TIMEOUT_TYPE_INVALID
	}
}

// decisionFailedCauses pairs causes that exist on both sides under the same
// meaning. It is read in both directions.
var decisionFailedCauses = map[apiv1.DecisionTaskFailedCause]enumspb.WorkflowTaskFailedCause{
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_UNHANDLED_DECISION:                                        enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES:                          enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES:                    enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_START_TIMER_ATTRIBUTES:                                enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_TIMER_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_CANCEL_TIMER_ATTRIBUTES:                               enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_TIMER_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_RECORD_MARKER_ATTRIBUTES:                              enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_RECORD_MARKER_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_COMPLETE_WORKFLOW_EXECUTION_ATTRIBUTES:                enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_COMPLETE_WORKFLOW_EXECUTION_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES:                    enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES:                  enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_ATTRIBUTES: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES:                            enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_START_TIMER_DUPLICATE_ID:                                  enumspb.WORKFLOW_TASK_FAILED_CAUSE_START_TIMER_DUPLICATE_ID,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_RESET_STICKY_TASK_LIST:                                    enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_STICKY_TASK_QUEUE,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE:                         enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES:                  enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES:                      enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_FORCE_CLOSE_DECISION:                                      enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_FAILOVER_CLOSE_DECISION:                                   enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_SIGNAL_INPUT_SIZE:                                     enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_INPUT_SIZE,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_RESET_WORKFLOW:                                            enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_BINARY:                                                enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_BINARY,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_SCHEDULE_ACTIVITY_DUPLICATE_ID:                            enumspb.WORKFLOW_TASK_FAILED_CAUSE_SCHEDULE_ACTIVITY_DUPLICATE_ID,
	apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES:                                     enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES,
}

func toDecisionFailedCause(c apiv1.DecisionTaskFailedCause) enumspb.WorkflowTaskFailedCause {
	if v, ok := decisionFailedCauses[c]; ok {
		return v
	}

	return enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
}

func fromDecisionFailedCause(c enumspb.WorkflowTaskFailedCause) apiv1.DecisionTaskFailedCause {
	for k, v := range decisionFailedCauses {
		if v == c {
			return k
		}
	}

	// Causes Temporal added later (non-deterministic error, pending limits,
	// and so on) surface as an unhandled Worker failure.
	return apiv1.DecisionTaskFailedCause_DECISION_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
}

func fromExecutionStatus(s enumspb.WorkflowExecutionStatus) (apiv1.WorkflowExecutionCloseStatus, apiv1.WorkflowExecutionStatus) {
	switch s {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_INVALID,
			apiv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_STARTED
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_COMPLETED,
			apiv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_COMPLETED
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_FAILED,
			apiv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_FAILED
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_CANCELED,
			apiv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_CANCELED
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_TERMINATED,
			apiv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_TERMINATED
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_CONTINUED_AS_NEW,
			apiv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_TIMED_OUT,
			apiv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_TIMED_OUT
	default:
		return apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_INVALID,
			apiv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_INVALID
	}
}

// closeStatusNames are the Temporal visibility spellings of each Cadence close
// status, used when rewriting list filters and queries.
var closeStatusNames = map[apiv1.WorkflowExecutionCloseStatus]string{
	apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_COMPLETED:        "Completed",
	apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_FAILED:           "Failed",
	apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_CANCELED:         "Canceled",
	apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_TERMINATED:       "Terminated",
	apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_CONTINUED_AS_NEW: "ContinuedAsNew",
	apiv1.WorkflowExecutionCloseStatus_WORKFLOW_EXECUTION_CLOSE_STATUS_TIMED_OUT:        "TimedOut",
}

func fromCancelExternalCause(c enumspb.CancelExternalWorkflowExecutionFailedCause) apiv1.CancelExternalWorkflowExecutionFailedCause {
	if c == enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND {
		return apiv1.CancelExternalWorkflowExecutionFailedCause_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION
	}

	return apiv1.CancelExternalWorkflowExecutionFailedCause_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_INVALID
}

func fromSignalExternalCause(c enumspb.SignalExternalWorkflowExecutionFailedCause) apiv1.SignalExternalWorkflowExecutionFailedCause {
	if c == enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND {
		return apiv1.SignalExternalWorkflowExecutionFailedCause_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION
	}

	return apiv1.SignalExternalWorkflowExecutionFailedCause_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_INVALID
}

func fromChildFailedCause(c enumspb.StartChildWorkflowExecutionFailedCause) apiv1.ChildWorkflowExecutionFailedCause {
	if c == enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS {
		return apiv1.ChildWorkflowExecutionFailedCause_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_RUNNING
	}

	return apiv1.ChildWorkflowExecutionFailedCause_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_INVALID
}

func toQueryRejectCondition(c apiv1.QueryRejectCondition) enumspb.QueryRejectCondition {
	switch c {
	case apiv1.QueryRejectCondition_QUERY_REJECT_CONDITION_NOT_OPEN:
		return enumspb.QUERY_REJECT_CONDITION_NOT_OPEN
	case apiv1.QueryRejectCondition_QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY:
		return enumspb.QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY
	default:
		return enumspb.QUERY_REJECT_CONDITION_NONE
	}
}

func toQueryResultType(t apiv1.QueryResultType) enumspb.QueryResultType {
	switch t {
	case apiv1.QueryResultType_QUERY_RESULT_TYPE_ANSWERED:
		return enumspb.QUERY_RESULT_TYPE_ANSWERED
	case apiv1.QueryResultType_QUERY_RESULT_TYPE_FAILED:
		return enumspb.QUERY_RESULT_TYPE_FAILED
	default:
		return enumspb.QUERY_RESULT_TYPE_UNSPECIFIED
	}
}

func fromIndexedValueType(t enumspb.IndexedValueType) apiv1.IndexedValueType {
	switch t {
	case enumspb.INDEXED_VALUE_TYPE_TEXT:
		return apiv1.IndexedValueType_INDEXED_VALUE_TYPE_STRING
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
		return apiv1.IndexedValueType_INDEXED_VALUE_TYPE_KEYWORD
	case enumspb.INDEXED_VALUE_TYPE_INT:
		return apiv1.IndexedValueType_INDEXED_VALUE_TYPE_INT
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		return apiv1.IndexedValueType_INDEXED_VALUE_TYPE_DOUBLE
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		return apiv1.IndexedValueType_INDEXED_VALUE_TYPE_BOOL
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		return apiv1.IndexedValueType_INDEXED_VALUE_TYPE_DATETIME
	default:
		return apiv1.IndexedValueType_INDEXED_VALUE_TYPE_INVALID
	}
}

func toArchivalState(s apiv1.ArchivalStatus) enumspb.ArchivalState {
	switch s {
	case apiv1.ArchivalStatus_ARCHIVAL_STATUS_ENABLED:
		return enumspb.ARCHIVAL_STATE_ENABLED
	case apiv1.ArchivalStatus_ARCHIVAL_STATUS_DISABLED:
		return enumspb.ARCHIVAL_STATE_DISABLED
	default:
		return enumspb.ARCHIVAL_STATE_UNSPECIFIED
	}
}

func fromArchivalState(s enumspb.ArchivalState) apiv1.ArchivalStatus {
	switch s {
	case enumspb.ARCHIVAL_STATE_ENABLED:
		return apiv1.ArchivalStatus_ARCHIVAL_STATUS_ENABLED
	case enumspb.ARCHIVAL_STATE_DISABLED:
		return apiv1.ArchivalStatus_ARCHIVAL_STATUS_DISABLED
	default:
		return apiv1.ArchivalStatus_ARCHIVAL_STATUS_INVALID
	}
}

func fromNamespaceState(s enumspb.NamespaceState) apiv1.DomainStatus {
	switch s {
	case enumspb.NAMESPACE_STATE_REGISTERED:
		return apiv1.DomainStatus_DOMAIN_STATUS_REGISTERED
	case enumspb.NAMESPACE_STATE_DEPRECATED:
		return apiv1.DomainStatus_DOMAIN_STATUS_DEPRECATED
	case enumspb.NAMESPACE_STATE_DELETED:
		return apiv1.DomainStatus_DOMAIN_STATUS_DELETED
	default:
		return apiv1.DomainStatus_DOMAIN_STATUS_INVALID
	}
}

func toTaskQueueType(t apiv1.TaskListType) enumspb.TaskQueueType {
	if t == apiv1.TaskListType_TASK_LIST_TYPE_ACTIVITY {
		return enumspb.TASK_QUEUE_TYPE_ACTIVITY
	}

	return enumspb.TASK_QUEUE_TYPE_WORKFLOW
}

func fromPendingActivityState(s enumspb.PendingActivityState) apiv1.PendingActivityState {
	switch s {
	case enumspb.PENDING_ACTIVITY_STATE_SCHEDULED:
		return apiv1.PendingActivityState_PENDING_ACTIVITY_STATE_SCHEDULED
	case enumspb.PENDING_ACTIVITY_STATE_STARTED:
		return apiv1.PendingActivityState_PENDING_ACTIVITY_STATE_STARTED
	case enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED:
		return apiv1.PendingActivityState_PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	default:
		return apiv1.PendingActivityState_PENDING_ACTIVITY_STATE_INVALID
	}
}

func fromPendingWorkflowTaskState(s enumspb.PendingWorkflowTaskState) apiv1.PendingDecisionState {
	switch s {
	case enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED:
		return apiv1.PendingDecisionState_PENDING_DECISION_STATE_SCHEDULED
	case enumspb.PENDING_WORKFLOW_TASK_STATE_STARTED:
		return apiv1.PendingDecisionState_PENDING_DECISION_STATE_STARTED
	default:
		return apiv1.PendingDecisionState_PENDING_DECISION_STATE_INVALID
	}
}
