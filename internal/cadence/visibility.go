package cadence

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"time"

	gogo "github.com/gogo/protobuf/types"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func fromExecutionInfo(i *workflowpb.WorkflowExecutionInfo) *apiv1.WorkflowExecutionInfo {
	if i == nil {
		return nil
	}

	closeStatus, status := fromExecutionStatus(i.Status)
	out := &apiv1.WorkflowExecutionInfo{
		WorkflowExecution: fromExecution(i.Execution),
		Type:              fromWorkflowType(i.Type),
		StartTime:         fromTimestamp(i.StartTime),
		CloseTime:         fromTimestamp(i.CloseTime),
		CloseStatus:       closeStatus,
		ExecutionStatus:   status,
		HistoryLength:     i.HistoryLength,
		ExecutionTime:     fromTimestamp(i.ExecutionTime),
		Memo:              fromMemo(i.Memo),
		SearchAttributes:  fromSearchAttributes(i.SearchAttributes),
		AutoResetPoints:   fromResetPoints(i.AutoResetPoints),
		TaskList:          i.TaskQueue,
		TaskListInfo:      &apiv1.TaskList{Name: i.TaskQueue, Kind: apiv1.TaskListKind_TASK_LIST_KIND_NORMAL},
	}

	if i.ParentExecution != nil {
		out.ParentExecutionInfo = &apiv1.ParentExecutionInfo{
			DomainId:          i.ParentNamespaceId,
			WorkflowExecution: fromExecution(i.ParentExecution),
		}
	}

	return out
}

func fromExecutionInfos(in []*workflowpb.WorkflowExecutionInfo) []*apiv1.WorkflowExecutionInfo {
	out := make([]*apiv1.WorkflowExecutionInfo, 0, len(in))
	for _, i := range in {
		out = append(out, fromExecutionInfo(i))
	}

	return out
}

// list runs a Temporal visibility query and converts the page.
func (h *Handler) list(
	ctx context.Context,
	namespace string,
	pageSize int32,
	token []byte,
	query string,
) ([]*apiv1.WorkflowExecutionInfo, []byte, error) {
	resp, err := h.workflow.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     namespace,
		PageSize:      pageSize,
		NextPageToken: token,
		Query:         query,
	})
	if err != nil {
		return nil, nil, toCadenceError(err)
	}

	return fromExecutionInfos(resp.Executions), resp.NextPageToken, nil
}

// ListWorkflowExecutions lists Workflows matching a Cadence visibility query.
func (h *Handler) ListWorkflowExecutions(
	ctx context.Context,
	req *apiv1.ListWorkflowExecutionsRequest,
) (*apiv1.ListWorkflowExecutionsResponse, error) {
	q, err := rewriteQuery(req.Query)
	if err != nil {
		return nil, toCadenceError(errBadQuery(err))
	}

	execs, token, err := h.list(ctx, req.Domain, req.PageSize, req.NextPageToken, q)
	if err != nil {
		return nil, err
	}

	return &apiv1.ListWorkflowExecutionsResponse{Executions: execs, NextPageToken: token}, nil
}

// ScanWorkflowExecutions lists without ordering guarantees, which a plain list
// satisfies.
func (h *Handler) ScanWorkflowExecutions(
	ctx context.Context,
	req *apiv1.ScanWorkflowExecutionsRequest,
) (*apiv1.ScanWorkflowExecutionsResponse, error) {
	q, err := rewriteQuery(req.Query)
	if err != nil {
		return nil, toCadenceError(errBadQuery(err))
	}

	execs, token, err := h.list(ctx, req.Domain, req.PageSize, req.NextPageToken, q)
	if err != nil {
		return nil, err
	}

	return &apiv1.ScanWorkflowExecutionsResponse{Executions: execs, NextPageToken: token}, nil
}

// ListArchivedWorkflowExecutions is not supported.
func (h *Handler) ListArchivedWorkflowExecutions(
	context.Context, *apiv1.ListArchivedWorkflowExecutionsRequest,
) (*apiv1.ListArchivedWorkflowExecutionsResponse, error) {
	return nil, unimplemented("ListArchivedWorkflowExecutions")
}

// ListOpenWorkflowExecutions lists running Workflows, filtered by start time
// and optionally by Workflow ID or type.
func (h *Handler) ListOpenWorkflowExecutions(
	ctx context.Context,
	req *apiv1.ListOpenWorkflowExecutionsRequest,
) (*apiv1.ListOpenWorkflowExecutionsResponse, error) {
	clauses := []string{"ExecutionStatus = 'Running'"}
	clauses = append(clauses, timeClauses("StartTime", req.StartTimeFilter)...)
	clauses = append(clauses, filterClauses(req.GetExecutionFilter(), req.GetTypeFilter())...)

	execs, token, err := h.list(ctx, req.Domain, req.PageSize, req.NextPageToken, strings.Join(clauses, " AND "))
	if err != nil {
		return nil, err
	}

	return &apiv1.ListOpenWorkflowExecutionsResponse{Executions: execs, NextPageToken: token}, nil
}

// ListClosedWorkflowExecutions lists closed Workflows, filtered by close time
// (Cadence's time filter on this call selects by close time) and optionally by
// Workflow ID, type, or close status.
func (h *Handler) ListClosedWorkflowExecutions(
	ctx context.Context,
	req *apiv1.ListClosedWorkflowExecutionsRequest,
) (*apiv1.ListClosedWorkflowExecutionsResponse, error) {
	clauses := []string{"ExecutionStatus != 'Running'"}
	clauses = append(clauses, timeClauses("CloseTime", req.StartTimeFilter)...)
	clauses = append(clauses, filterClauses(req.GetExecutionFilter(), req.GetTypeFilter())...)

	if f := req.GetStatusFilter(); f != nil {
		if name, ok := closeStatusNames[f.Status]; ok {
			clauses = append(clauses, fmt.Sprintf("ExecutionStatus = '%s'", name))
		}
	}

	execs, token, err := h.list(ctx, req.Domain, req.PageSize, req.NextPageToken, strings.Join(clauses, " AND "))
	if err != nil {
		return nil, err
	}

	return &apiv1.ListClosedWorkflowExecutionsResponse{Executions: execs, NextPageToken: token}, nil
}

func timeClauses(attr string, f *apiv1.StartTimeFilter) []string {
	var out []string

	if t := f.GetEarliestTime(); t != nil && (t.Seconds != 0 || t.Nanos != 0) {
		out = append(out, fmt.Sprintf("%s >= '%s'", attr, formatTime(t)))
	}

	if t := f.GetLatestTime(); t != nil && (t.Seconds != 0 || t.Nanos != 0) {
		out = append(out, fmt.Sprintf("%s <= '%s'", attr, formatTime(t)))
	}

	return out
}

func formatTime(t *gogo.Timestamp) string {
	return time.Unix(t.Seconds, int64(t.Nanos)).UTC().Format(time.RFC3339Nano)
}

func filterClauses(exec *apiv1.WorkflowExecutionFilter, typ *apiv1.WorkflowTypeFilter) []string {
	var out []string

	if exec != nil {
		if exec.WorkflowId != "" {
			out = append(out, fmt.Sprintf("WorkflowId = %s", quote(exec.WorkflowId)))
		}

		if exec.RunId != "" {
			out = append(out, fmt.Sprintf("RunId = %s", quote(exec.RunId)))
		}
	}

	if typ != nil && typ.Name != "" {
		out = append(out, fmt.Sprintf("WorkflowType = %s", quote(typ.Name)))
	}

	return out
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// CountWorkflowExecutions counts Workflows matching a Cadence visibility query.
func (h *Handler) CountWorkflowExecutions(
	ctx context.Context,
	req *apiv1.CountWorkflowExecutionsRequest,
) (*apiv1.CountWorkflowExecutionsResponse, error) {
	q, err := rewriteQuery(req.Query)
	if err != nil {
		return nil, toCadenceError(errBadQuery(err))
	}

	resp, err := h.workflow.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: req.Domain,
		Query:     q,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.CountWorkflowExecutionsResponse{Count: resp.Count}, nil
}

// GetSearchAttributes lists the search attributes Cadence code can use. A
// Cadence cluster defines them cluster-wide and the call names no Domain, while
// Temporal defines custom attributes per Namespace, so the answer is the system
// attributes plus every Namespace's custom ones. Types Cadence cannot express
// are left out: the Cadence client rejects an unknown type outright.
func (h *Handler) GetSearchAttributes(ctx context.Context, _ *apiv1.GetSearchAttributesRequest) (*apiv1.GetSearchAttributesResponse, error) {
	resp, err := h.workflow.GetSearchAttributes(ctx, &workflowservice.GetSearchAttributesRequest{}) //nolint:staticcheck // Cadence's call has no Namespace to scope it by.
	if err != nil {
		return nil, toCadenceError(err)
	}

	keys := resp.Keys

	if h.operator != nil {
		custom, err := h.customSearchAttributes(ctx)
		if err != nil {
			return nil, toCadenceError(err)
		}

		maps.Copy(keys, custom)
	}

	out := &apiv1.GetSearchAttributesResponse{Keys: make(map[string]apiv1.IndexedValueType, len(keys))}
	for k, v := range keys {
		if t := fromIndexedValueType(v); t != apiv1.IndexedValueType_INDEXED_VALUE_TYPE_INVALID {
			out.Keys[k] = t
		}
	}

	if v, ok := out.Keys[temporalChangeVersion]; ok {
		out.Keys[cadenceChangeVersion] = v
	}

	return out, nil
}

// customSearchAttributes unions the custom search attributes of every
// Namespace.
func (h *Handler) customSearchAttributes(ctx context.Context) (map[string]enumspb.IndexedValueType, error) {
	out := map[string]enumspb.IndexedValueType{}

	var token []byte

	for {
		page, err := h.workflow.ListNamespaces(ctx, &workflowservice.ListNamespacesRequest{NextPageToken: token})
		if err != nil {
			return nil, err
		}

		for _, ns := range page.Namespaces {
			attrs, err := h.operator.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{
				Namespace: ns.GetNamespaceInfo().GetName(),
			})
			if err != nil {
				return nil, err
			}

			maps.Copy(out, attrs.CustomAttributes)
		}

		if token = page.NextPageToken; len(token) == 0 {
			return out, nil
		}
	}
}
