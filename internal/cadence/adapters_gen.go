// Code generated from the go.uber.org/cadence and cadence-idl interfaces. DO NOT EDIT.
// The interface assertions below fail to compile if either module changes the
// method sets, which is the signal to regenerate.

package cadence

import (
	"context"

	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceserver"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/yarpc"
)

// localDomainClient calls a DomainAPI server in process, for the Thrift adapter.
type localDomainClient struct{ s apiv1.DomainAPIYARPCServer }

var _ apiv1.DomainAPIYARPCClient = localDomainClient{}

func (c localDomainClient) RegisterDomain(ctx context.Context, req *apiv1.RegisterDomainRequest, _ ...yarpc.CallOption) (*apiv1.RegisterDomainResponse, error) {
	return c.s.RegisterDomain(ctx, req)
}

func (c localDomainClient) DescribeDomain(ctx context.Context, req *apiv1.DescribeDomainRequest, _ ...yarpc.CallOption) (*apiv1.DescribeDomainResponse, error) {
	return c.s.DescribeDomain(ctx, req)
}

func (c localDomainClient) ListDomains(ctx context.Context, req *apiv1.ListDomainsRequest, _ ...yarpc.CallOption) (*apiv1.ListDomainsResponse, error) {
	return c.s.ListDomains(ctx, req)
}

func (c localDomainClient) UpdateDomain(ctx context.Context, req *apiv1.UpdateDomainRequest, _ ...yarpc.CallOption) (*apiv1.UpdateDomainResponse, error) {
	return c.s.UpdateDomain(ctx, req)
}

func (c localDomainClient) FailoverDomain(ctx context.Context, req *apiv1.FailoverDomainRequest, _ ...yarpc.CallOption) (*apiv1.FailoverDomainResponse, error) {
	return c.s.FailoverDomain(ctx, req)
}

func (c localDomainClient) DeprecateDomain(ctx context.Context, req *apiv1.DeprecateDomainRequest, _ ...yarpc.CallOption) (*apiv1.DeprecateDomainResponse, error) {
	return c.s.DeprecateDomain(ctx, req)
}

func (c localDomainClient) DeleteDomain(ctx context.Context, req *apiv1.DeleteDomainRequest, _ ...yarpc.CallOption) (*apiv1.DeleteDomainResponse, error) {
	return c.s.DeleteDomain(ctx, req)
}

func (c localDomainClient) ListFailoverHistory(ctx context.Context, req *apiv1.ListFailoverHistoryRequest, _ ...yarpc.CallOption) (*apiv1.ListFailoverHistoryResponse, error) {
	return c.s.ListFailoverHistory(ctx, req)
}

// localWorkflowClient calls a WorkflowAPI server in process, for the Thrift adapter.
type localWorkflowClient struct{ s apiv1.WorkflowAPIYARPCServer }

var _ apiv1.WorkflowAPIYARPCClient = localWorkflowClient{}

func (c localWorkflowClient) RestartWorkflowExecution(ctx context.Context, req *apiv1.RestartWorkflowExecutionRequest, _ ...yarpc.CallOption) (*apiv1.RestartWorkflowExecutionResponse, error) {
	return c.s.RestartWorkflowExecution(ctx, req)
}

func (c localWorkflowClient) StartWorkflowExecution(ctx context.Context, req *apiv1.StartWorkflowExecutionRequest, _ ...yarpc.CallOption) (*apiv1.StartWorkflowExecutionResponse, error) {
	return c.s.StartWorkflowExecution(ctx, req)
}

func (c localWorkflowClient) StartWorkflowExecutionAsync(ctx context.Context, req *apiv1.StartWorkflowExecutionAsyncRequest, _ ...yarpc.CallOption) (*apiv1.StartWorkflowExecutionAsyncResponse, error) {
	return c.s.StartWorkflowExecutionAsync(ctx, req)
}

func (c localWorkflowClient) SignalWorkflowExecution(ctx context.Context, req *apiv1.SignalWorkflowExecutionRequest, _ ...yarpc.CallOption) (*apiv1.SignalWorkflowExecutionResponse, error) {
	return c.s.SignalWorkflowExecution(ctx, req)
}

func (c localWorkflowClient) SignalWithStartWorkflowExecution(ctx context.Context, req *apiv1.SignalWithStartWorkflowExecutionRequest, _ ...yarpc.CallOption) (*apiv1.SignalWithStartWorkflowExecutionResponse, error) {
	return c.s.SignalWithStartWorkflowExecution(ctx, req)
}

func (c localWorkflowClient) SignalWithStartWorkflowExecutionAsync(ctx context.Context, req *apiv1.SignalWithStartWorkflowExecutionAsyncRequest, _ ...yarpc.CallOption) (*apiv1.SignalWithStartWorkflowExecutionAsyncResponse, error) {
	return c.s.SignalWithStartWorkflowExecutionAsync(ctx, req)
}

func (c localWorkflowClient) ResetWorkflowExecution(ctx context.Context, req *apiv1.ResetWorkflowExecutionRequest, _ ...yarpc.CallOption) (*apiv1.ResetWorkflowExecutionResponse, error) {
	return c.s.ResetWorkflowExecution(ctx, req)
}

func (c localWorkflowClient) RequestCancelWorkflowExecution(ctx context.Context, req *apiv1.RequestCancelWorkflowExecutionRequest, _ ...yarpc.CallOption) (*apiv1.RequestCancelWorkflowExecutionResponse, error) {
	return c.s.RequestCancelWorkflowExecution(ctx, req)
}

func (c localWorkflowClient) TerminateWorkflowExecution(ctx context.Context, req *apiv1.TerminateWorkflowExecutionRequest, _ ...yarpc.CallOption) (*apiv1.TerminateWorkflowExecutionResponse, error) {
	return c.s.TerminateWorkflowExecution(ctx, req)
}

func (c localWorkflowClient) DescribeWorkflowExecution(ctx context.Context, req *apiv1.DescribeWorkflowExecutionRequest, _ ...yarpc.CallOption) (*apiv1.DescribeWorkflowExecutionResponse, error) {
	return c.s.DescribeWorkflowExecution(ctx, req)
}

func (c localWorkflowClient) QueryWorkflow(ctx context.Context, req *apiv1.QueryWorkflowRequest, _ ...yarpc.CallOption) (*apiv1.QueryWorkflowResponse, error) {
	return c.s.QueryWorkflow(ctx, req)
}

func (c localWorkflowClient) DescribeTaskList(ctx context.Context, req *apiv1.DescribeTaskListRequest, _ ...yarpc.CallOption) (*apiv1.DescribeTaskListResponse, error) {
	return c.s.DescribeTaskList(ctx, req)
}

func (c localWorkflowClient) GetTaskListsByDomain(ctx context.Context, req *apiv1.GetTaskListsByDomainRequest, _ ...yarpc.CallOption) (*apiv1.GetTaskListsByDomainResponse, error) {
	return c.s.GetTaskListsByDomain(ctx, req)
}

func (c localWorkflowClient) ListTaskListPartitions(ctx context.Context, req *apiv1.ListTaskListPartitionsRequest, _ ...yarpc.CallOption) (*apiv1.ListTaskListPartitionsResponse, error) {
	return c.s.ListTaskListPartitions(ctx, req)
}

func (c localWorkflowClient) GetClusterInfo(ctx context.Context, req *apiv1.GetClusterInfoRequest, _ ...yarpc.CallOption) (*apiv1.GetClusterInfoResponse, error) {
	return c.s.GetClusterInfo(ctx, req)
}

func (c localWorkflowClient) GetWorkflowExecutionHistory(ctx context.Context, req *apiv1.GetWorkflowExecutionHistoryRequest, _ ...yarpc.CallOption) (*apiv1.GetWorkflowExecutionHistoryResponse, error) {
	return c.s.GetWorkflowExecutionHistory(ctx, req)
}

func (c localWorkflowClient) RefreshWorkflowTasks(ctx context.Context, req *apiv1.RefreshWorkflowTasksRequest, _ ...yarpc.CallOption) (*apiv1.RefreshWorkflowTasksResponse, error) {
	return c.s.RefreshWorkflowTasks(ctx, req)
}

func (c localWorkflowClient) DiagnoseWorkflowExecution(ctx context.Context, req *apiv1.DiagnoseWorkflowExecutionRequest, _ ...yarpc.CallOption) (*apiv1.DiagnoseWorkflowExecutionResponse, error) {
	return c.s.DiagnoseWorkflowExecution(ctx, req)
}

func (c localWorkflowClient) ExportWorkflowExecution(ctx context.Context, req *apiv1.ExportWorkflowExecutionRequest, _ ...yarpc.CallOption) (*apiv1.ExportWorkflowExecutionResponse, error) {
	return c.s.ExportWorkflowExecution(ctx, req)
}

// localWorkerClient calls a WorkerAPI server in process, for the Thrift adapter.
type localWorkerClient struct{ s apiv1.WorkerAPIYARPCServer }

var _ apiv1.WorkerAPIYARPCClient = localWorkerClient{}

func (c localWorkerClient) PollForDecisionTask(ctx context.Context, req *apiv1.PollForDecisionTaskRequest, _ ...yarpc.CallOption) (*apiv1.PollForDecisionTaskResponse, error) {
	return c.s.PollForDecisionTask(ctx, req)
}

func (c localWorkerClient) RespondDecisionTaskCompleted(ctx context.Context, req *apiv1.RespondDecisionTaskCompletedRequest, _ ...yarpc.CallOption) (*apiv1.RespondDecisionTaskCompletedResponse, error) {
	return c.s.RespondDecisionTaskCompleted(ctx, req)
}

func (c localWorkerClient) RespondDecisionTaskFailed(ctx context.Context, req *apiv1.RespondDecisionTaskFailedRequest, _ ...yarpc.CallOption) (*apiv1.RespondDecisionTaskFailedResponse, error) {
	return c.s.RespondDecisionTaskFailed(ctx, req)
}

func (c localWorkerClient) PollForActivityTask(ctx context.Context, req *apiv1.PollForActivityTaskRequest, _ ...yarpc.CallOption) (*apiv1.PollForActivityTaskResponse, error) {
	return c.s.PollForActivityTask(ctx, req)
}

func (c localWorkerClient) RespondActivityTaskCompleted(ctx context.Context, req *apiv1.RespondActivityTaskCompletedRequest, _ ...yarpc.CallOption) (*apiv1.RespondActivityTaskCompletedResponse, error) {
	return c.s.RespondActivityTaskCompleted(ctx, req)
}

func (c localWorkerClient) RespondActivityTaskCompletedByID(ctx context.Context, req *apiv1.RespondActivityTaskCompletedByIDRequest, _ ...yarpc.CallOption) (*apiv1.RespondActivityTaskCompletedByIDResponse, error) {
	return c.s.RespondActivityTaskCompletedByID(ctx, req)
}

func (c localWorkerClient) RespondActivityTaskFailed(ctx context.Context, req *apiv1.RespondActivityTaskFailedRequest, _ ...yarpc.CallOption) (*apiv1.RespondActivityTaskFailedResponse, error) {
	return c.s.RespondActivityTaskFailed(ctx, req)
}

func (c localWorkerClient) RespondActivityTaskFailedByID(ctx context.Context, req *apiv1.RespondActivityTaskFailedByIDRequest, _ ...yarpc.CallOption) (*apiv1.RespondActivityTaskFailedByIDResponse, error) {
	return c.s.RespondActivityTaskFailedByID(ctx, req)
}

func (c localWorkerClient) RespondActivityTaskCanceled(ctx context.Context, req *apiv1.RespondActivityTaskCanceledRequest, _ ...yarpc.CallOption) (*apiv1.RespondActivityTaskCanceledResponse, error) {
	return c.s.RespondActivityTaskCanceled(ctx, req)
}

func (c localWorkerClient) RespondActivityTaskCanceledByID(ctx context.Context, req *apiv1.RespondActivityTaskCanceledByIDRequest, _ ...yarpc.CallOption) (*apiv1.RespondActivityTaskCanceledByIDResponse, error) {
	return c.s.RespondActivityTaskCanceledByID(ctx, req)
}

func (c localWorkerClient) RecordActivityTaskHeartbeat(ctx context.Context, req *apiv1.RecordActivityTaskHeartbeatRequest, _ ...yarpc.CallOption) (*apiv1.RecordActivityTaskHeartbeatResponse, error) {
	return c.s.RecordActivityTaskHeartbeat(ctx, req)
}

func (c localWorkerClient) RecordActivityTaskHeartbeatByID(ctx context.Context, req *apiv1.RecordActivityTaskHeartbeatByIDRequest, _ ...yarpc.CallOption) (*apiv1.RecordActivityTaskHeartbeatByIDResponse, error) {
	return c.s.RecordActivityTaskHeartbeatByID(ctx, req)
}

func (c localWorkerClient) RespondQueryTaskCompleted(ctx context.Context, req *apiv1.RespondQueryTaskCompletedRequest, _ ...yarpc.CallOption) (*apiv1.RespondQueryTaskCompletedResponse, error) {
	return c.s.RespondQueryTaskCompleted(ctx, req)
}

func (c localWorkerClient) ResetStickyTaskList(ctx context.Context, req *apiv1.ResetStickyTaskListRequest, _ ...yarpc.CallOption) (*apiv1.ResetStickyTaskListResponse, error) {
	return c.s.ResetStickyTaskList(ctx, req)
}

// localVisibilityClient calls a VisibilityAPI server in process, for the Thrift adapter.
type localVisibilityClient struct {
	s apiv1.VisibilityAPIYARPCServer
}

var _ apiv1.VisibilityAPIYARPCClient = localVisibilityClient{}

func (c localVisibilityClient) ListWorkflowExecutions(ctx context.Context, req *apiv1.ListWorkflowExecutionsRequest, _ ...yarpc.CallOption) (*apiv1.ListWorkflowExecutionsResponse, error) {
	return c.s.ListWorkflowExecutions(ctx, req)
}

func (c localVisibilityClient) ListOpenWorkflowExecutions(ctx context.Context, req *apiv1.ListOpenWorkflowExecutionsRequest, _ ...yarpc.CallOption) (*apiv1.ListOpenWorkflowExecutionsResponse, error) {
	return c.s.ListOpenWorkflowExecutions(ctx, req)
}

func (c localVisibilityClient) ListClosedWorkflowExecutions(ctx context.Context, req *apiv1.ListClosedWorkflowExecutionsRequest, _ ...yarpc.CallOption) (*apiv1.ListClosedWorkflowExecutionsResponse, error) {
	return c.s.ListClosedWorkflowExecutions(ctx, req)
}

func (c localVisibilityClient) ListArchivedWorkflowExecutions(ctx context.Context, req *apiv1.ListArchivedWorkflowExecutionsRequest, _ ...yarpc.CallOption) (*apiv1.ListArchivedWorkflowExecutionsResponse, error) {
	return c.s.ListArchivedWorkflowExecutions(ctx, req)
}

func (c localVisibilityClient) ScanWorkflowExecutions(ctx context.Context, req *apiv1.ScanWorkflowExecutionsRequest, _ ...yarpc.CallOption) (*apiv1.ScanWorkflowExecutionsResponse, error) {
	return c.s.ScanWorkflowExecutions(ctx, req)
}

func (c localVisibilityClient) CountWorkflowExecutions(ctx context.Context, req *apiv1.CountWorkflowExecutionsRequest, _ ...yarpc.CallOption) (*apiv1.CountWorkflowExecutionsResponse, error) {
	return c.s.CountWorkflowExecutions(ctx, req)
}

func (c localVisibilityClient) GetSearchAttributes(ctx context.Context, req *apiv1.GetSearchAttributesRequest, _ ...yarpc.CallOption) (*apiv1.GetSearchAttributesResponse, error) {
	return c.s.GetSearchAttributes(ctx, req)
}

// unsupportedScheduleClient answers every Cadence schedule call as unimplemented:
// Cadence schedules have no Temporal counterpart this frontend maps.
type unsupportedScheduleClient struct{}

var _ apiv1.ScheduleAPIYARPCClient = unsupportedScheduleClient{}

func (unsupportedScheduleClient) CreateSchedule(context.Context, *apiv1.CreateScheduleRequest, ...yarpc.CallOption) (*apiv1.CreateScheduleResponse, error) {
	return nil, unimplemented("CreateSchedule")
}

func (unsupportedScheduleClient) DescribeSchedule(context.Context, *apiv1.DescribeScheduleRequest, ...yarpc.CallOption) (*apiv1.DescribeScheduleResponse, error) {
	return nil, unimplemented("DescribeSchedule")
}

func (unsupportedScheduleClient) ListSchedules(context.Context, *apiv1.ListSchedulesRequest, ...yarpc.CallOption) (*apiv1.ListSchedulesResponse, error) {
	return nil, unimplemented("ListSchedules")
}

func (unsupportedScheduleClient) DeleteSchedule(context.Context, *apiv1.DeleteScheduleRequest, ...yarpc.CallOption) (*apiv1.DeleteScheduleResponse, error) {
	return nil, unimplemented("DeleteSchedule")
}

func (unsupportedScheduleClient) PauseSchedule(context.Context, *apiv1.PauseScheduleRequest, ...yarpc.CallOption) (*apiv1.PauseScheduleResponse, error) {
	return nil, unimplemented("PauseSchedule")
}

func (unsupportedScheduleClient) UnpauseSchedule(context.Context, *apiv1.UnpauseScheduleRequest, ...yarpc.CallOption) (*apiv1.UnpauseScheduleResponse, error) {
	return nil, unimplemented("UnpauseSchedule")
}

func (unsupportedScheduleClient) BackfillSchedule(context.Context, *apiv1.BackfillScheduleRequest, ...yarpc.CallOption) (*apiv1.BackfillScheduleResponse, error) {
	return nil, unimplemented("BackfillSchedule")
}

func (unsupportedScheduleClient) UpdateSchedule(context.Context, *apiv1.UpdateScheduleRequest, ...yarpc.CallOption) (*apiv1.UpdateScheduleResponse, error) {
	return nil, unimplemented("UpdateSchedule")
}

// thriftServer serves the Thrift WorkflowService by calling the Cadence
// client's Thrift-to-proto adapter, dropping the call options a server never
// receives.
type thriftServer struct {
	c workflowserviceclient.Interface
}

var _ workflowserviceserver.Interface = thriftServer{}

func (t thriftServer) BackfillSchedule(ctx context.Context, Request *shared.BackfillScheduleRequest) (*shared.BackfillScheduleResponse, error) {
	return t.c.BackfillSchedule(ctx, Request)
}

func (t thriftServer) CountWorkflowExecutions(ctx context.Context, CountRequest *shared.CountWorkflowExecutionsRequest) (*shared.CountWorkflowExecutionsResponse, error) {
	return t.c.CountWorkflowExecutions(ctx, CountRequest)
}

func (t thriftServer) CreateSchedule(ctx context.Context, Request *shared.CreateScheduleRequest) (*shared.CreateScheduleResponse, error) {
	return t.c.CreateSchedule(ctx, Request)
}

func (t thriftServer) DeleteDomain(ctx context.Context, DeleteRequest *shared.DeleteDomainRequest) error {
	return t.c.DeleteDomain(ctx, DeleteRequest)
}

func (t thriftServer) DeleteSchedule(ctx context.Context, Request *shared.DeleteScheduleRequest) (*shared.DeleteScheduleResponse, error) {
	return t.c.DeleteSchedule(ctx, Request)
}

func (t thriftServer) DeprecateDomain(ctx context.Context, DeprecateRequest *shared.DeprecateDomainRequest) error {
	return t.c.DeprecateDomain(ctx, DeprecateRequest)
}

func (t thriftServer) DescribeDomain(ctx context.Context, DescribeRequest *shared.DescribeDomainRequest) (*shared.DescribeDomainResponse, error) {
	return t.c.DescribeDomain(ctx, DescribeRequest)
}

func (t thriftServer) DescribeSchedule(ctx context.Context, Request *shared.DescribeScheduleRequest) (*shared.DescribeScheduleResponse, error) {
	return t.c.DescribeSchedule(ctx, Request)
}

func (t thriftServer) DescribeTaskList(ctx context.Context, Request *shared.DescribeTaskListRequest) (*shared.DescribeTaskListResponse, error) {
	return t.c.DescribeTaskList(ctx, Request)
}

func (t thriftServer) DescribeWorkflowExecution(ctx context.Context, DescribeRequest *shared.DescribeWorkflowExecutionRequest) (*shared.DescribeWorkflowExecutionResponse, error) {
	return t.c.DescribeWorkflowExecution(ctx, DescribeRequest)
}

func (t thriftServer) DiagnoseWorkflowExecution(ctx context.Context, DiagnoseRequest *shared.DiagnoseWorkflowExecutionRequest) (*shared.DiagnoseWorkflowExecutionResponse, error) {
	return t.c.DiagnoseWorkflowExecution(ctx, DiagnoseRequest)
}

func (t thriftServer) FailoverDomain(ctx context.Context, FailoverRequest *shared.FailoverDomainRequest) (*shared.FailoverDomainResponse, error) {
	return t.c.FailoverDomain(ctx, FailoverRequest)
}

func (t thriftServer) GetClusterInfo(ctx context.Context) (*shared.ClusterInfo, error) {
	return t.c.GetClusterInfo(ctx)
}

func (t thriftServer) GetSearchAttributes(ctx context.Context) (*shared.GetSearchAttributesResponse, error) {
	return t.c.GetSearchAttributes(ctx)
}

func (t thriftServer) GetTaskListsByDomain(ctx context.Context, Request *shared.GetTaskListsByDomainRequest) (*shared.GetTaskListsByDomainResponse, error) {
	return t.c.GetTaskListsByDomain(ctx, Request)
}

func (t thriftServer) GetWorkflowExecutionHistory(ctx context.Context, GetRequest *shared.GetWorkflowExecutionHistoryRequest) (*shared.GetWorkflowExecutionHistoryResponse, error) {
	return t.c.GetWorkflowExecutionHistory(ctx, GetRequest)
}

func (t thriftServer) ListArchivedWorkflowExecutions(ctx context.Context, ListRequest *shared.ListArchivedWorkflowExecutionsRequest) (*shared.ListArchivedWorkflowExecutionsResponse, error) {
	return t.c.ListArchivedWorkflowExecutions(ctx, ListRequest)
}

func (t thriftServer) ListClosedWorkflowExecutions(ctx context.Context, ListRequest *shared.ListClosedWorkflowExecutionsRequest) (*shared.ListClosedWorkflowExecutionsResponse, error) {
	return t.c.ListClosedWorkflowExecutions(ctx, ListRequest)
}

func (t thriftServer) ListDomains(ctx context.Context, ListRequest *shared.ListDomainsRequest) (*shared.ListDomainsResponse, error) {
	return t.c.ListDomains(ctx, ListRequest)
}

func (t thriftServer) ListFailoverHistory(ctx context.Context, ListRequest *shared.ListFailoverHistoryRequest) (*shared.ListFailoverHistoryResponse, error) {
	return t.c.ListFailoverHistory(ctx, ListRequest)
}

func (t thriftServer) ListOpenWorkflowExecutions(ctx context.Context, ListRequest *shared.ListOpenWorkflowExecutionsRequest) (*shared.ListOpenWorkflowExecutionsResponse, error) {
	return t.c.ListOpenWorkflowExecutions(ctx, ListRequest)
}

func (t thriftServer) ListSchedules(ctx context.Context, Request *shared.ListSchedulesRequest) (*shared.ListSchedulesResponse, error) {
	return t.c.ListSchedules(ctx, Request)
}

func (t thriftServer) ListTaskListPartitions(ctx context.Context, Request *shared.ListTaskListPartitionsRequest) (*shared.ListTaskListPartitionsResponse, error) {
	return t.c.ListTaskListPartitions(ctx, Request)
}

func (t thriftServer) ListWorkflowExecutions(ctx context.Context, ListRequest *shared.ListWorkflowExecutionsRequest) (*shared.ListWorkflowExecutionsResponse, error) {
	return t.c.ListWorkflowExecutions(ctx, ListRequest)
}

func (t thriftServer) PauseSchedule(ctx context.Context, Request *shared.PauseScheduleRequest) (*shared.PauseScheduleResponse, error) {
	return t.c.PauseSchedule(ctx, Request)
}

func (t thriftServer) PollForActivityTask(ctx context.Context, PollRequest *shared.PollForActivityTaskRequest) (*shared.PollForActivityTaskResponse, error) {
	return t.c.PollForActivityTask(ctx, PollRequest)
}

func (t thriftServer) PollForDecisionTask(ctx context.Context, PollRequest *shared.PollForDecisionTaskRequest) (*shared.PollForDecisionTaskResponse, error) {
	return t.c.PollForDecisionTask(ctx, PollRequest)
}

func (t thriftServer) QueryWorkflow(ctx context.Context, QueryRequest *shared.QueryWorkflowRequest) (*shared.QueryWorkflowResponse, error) {
	return t.c.QueryWorkflow(ctx, QueryRequest)
}

func (t thriftServer) RecordActivityTaskHeartbeat(ctx context.Context, HeartbeatRequest *shared.RecordActivityTaskHeartbeatRequest) (*shared.RecordActivityTaskHeartbeatResponse, error) {
	return t.c.RecordActivityTaskHeartbeat(ctx, HeartbeatRequest)
}

func (t thriftServer) RecordActivityTaskHeartbeatByID(ctx context.Context, HeartbeatRequest *shared.RecordActivityTaskHeartbeatByIDRequest) (*shared.RecordActivityTaskHeartbeatResponse, error) {
	return t.c.RecordActivityTaskHeartbeatByID(ctx, HeartbeatRequest)
}

func (t thriftServer) RefreshWorkflowTasks(ctx context.Context, Request *shared.RefreshWorkflowTasksRequest) error {
	return t.c.RefreshWorkflowTasks(ctx, Request)
}

func (t thriftServer) RegisterDomain(ctx context.Context, RegisterRequest *shared.RegisterDomainRequest) error {
	return t.c.RegisterDomain(ctx, RegisterRequest)
}

func (t thriftServer) RequestCancelWorkflowExecution(ctx context.Context, CancelRequest *shared.RequestCancelWorkflowExecutionRequest) error {
	return t.c.RequestCancelWorkflowExecution(ctx, CancelRequest)
}

func (t thriftServer) ResetStickyTaskList(ctx context.Context, ResetRequest *shared.ResetStickyTaskListRequest) (*shared.ResetStickyTaskListResponse, error) {
	return t.c.ResetStickyTaskList(ctx, ResetRequest)
}

func (t thriftServer) ResetWorkflowExecution(ctx context.Context, ResetRequest *shared.ResetWorkflowExecutionRequest) (*shared.ResetWorkflowExecutionResponse, error) {
	return t.c.ResetWorkflowExecution(ctx, ResetRequest)
}

func (t thriftServer) RespondActivityTaskCanceled(ctx context.Context, CanceledRequest *shared.RespondActivityTaskCanceledRequest) error {
	return t.c.RespondActivityTaskCanceled(ctx, CanceledRequest)
}

func (t thriftServer) RespondActivityTaskCanceledByID(ctx context.Context, CanceledRequest *shared.RespondActivityTaskCanceledByIDRequest) error {
	return t.c.RespondActivityTaskCanceledByID(ctx, CanceledRequest)
}

func (t thriftServer) RespondActivityTaskCompleted(ctx context.Context, CompleteRequest *shared.RespondActivityTaskCompletedRequest) error {
	return t.c.RespondActivityTaskCompleted(ctx, CompleteRequest)
}

func (t thriftServer) RespondActivityTaskCompletedByID(ctx context.Context, CompleteRequest *shared.RespondActivityTaskCompletedByIDRequest) error {
	return t.c.RespondActivityTaskCompletedByID(ctx, CompleteRequest)
}

func (t thriftServer) RespondActivityTaskFailed(ctx context.Context, FailRequest *shared.RespondActivityTaskFailedRequest) error {
	return t.c.RespondActivityTaskFailed(ctx, FailRequest)
}

func (t thriftServer) RespondActivityTaskFailedByID(ctx context.Context, FailRequest *shared.RespondActivityTaskFailedByIDRequest) error {
	return t.c.RespondActivityTaskFailedByID(ctx, FailRequest)
}

func (t thriftServer) RespondDecisionTaskCompleted(ctx context.Context, CompleteRequest *shared.RespondDecisionTaskCompletedRequest) (*shared.RespondDecisionTaskCompletedResponse, error) {
	return t.c.RespondDecisionTaskCompleted(ctx, CompleteRequest)
}

func (t thriftServer) RespondDecisionTaskFailed(ctx context.Context, FailedRequest *shared.RespondDecisionTaskFailedRequest) error {
	return t.c.RespondDecisionTaskFailed(ctx, FailedRequest)
}

func (t thriftServer) RespondQueryTaskCompleted(ctx context.Context, CompleteRequest *shared.RespondQueryTaskCompletedRequest) error {
	return t.c.RespondQueryTaskCompleted(ctx, CompleteRequest)
}

func (t thriftServer) RestartWorkflowExecution(ctx context.Context, RestartRequest *shared.RestartWorkflowExecutionRequest) (*shared.RestartWorkflowExecutionResponse, error) {
	return t.c.RestartWorkflowExecution(ctx, RestartRequest)
}

func (t thriftServer) ScanWorkflowExecutions(ctx context.Context, ListRequest *shared.ListWorkflowExecutionsRequest) (*shared.ListWorkflowExecutionsResponse, error) {
	return t.c.ScanWorkflowExecutions(ctx, ListRequest)
}

func (t thriftServer) SignalWithStartWorkflowExecution(ctx context.Context, SignalWithStartRequest *shared.SignalWithStartWorkflowExecutionRequest) (*shared.StartWorkflowExecutionResponse, error) {
	return t.c.SignalWithStartWorkflowExecution(ctx, SignalWithStartRequest)
}

func (t thriftServer) SignalWithStartWorkflowExecutionAsync(ctx context.Context, SignalWithStartRequest *shared.SignalWithStartWorkflowExecutionAsyncRequest) (*shared.SignalWithStartWorkflowExecutionAsyncResponse, error) {
	return t.c.SignalWithStartWorkflowExecutionAsync(ctx, SignalWithStartRequest)
}

func (t thriftServer) SignalWorkflowExecution(ctx context.Context, SignalRequest *shared.SignalWorkflowExecutionRequest) error {
	return t.c.SignalWorkflowExecution(ctx, SignalRequest)
}

func (t thriftServer) StartWorkflowExecution(ctx context.Context, StartRequest *shared.StartWorkflowExecutionRequest) (*shared.StartWorkflowExecutionResponse, error) {
	return t.c.StartWorkflowExecution(ctx, StartRequest)
}

func (t thriftServer) StartWorkflowExecutionAsync(ctx context.Context, StartRequest *shared.StartWorkflowExecutionAsyncRequest) (*shared.StartWorkflowExecutionAsyncResponse, error) {
	return t.c.StartWorkflowExecutionAsync(ctx, StartRequest)
}

func (t thriftServer) TerminateWorkflowExecution(ctx context.Context, TerminateRequest *shared.TerminateWorkflowExecutionRequest) error {
	return t.c.TerminateWorkflowExecution(ctx, TerminateRequest)
}

func (t thriftServer) UnpauseSchedule(ctx context.Context, Request *shared.UnpauseScheduleRequest) (*shared.UnpauseScheduleResponse, error) {
	return t.c.UnpauseSchedule(ctx, Request)
}

func (t thriftServer) UpdateDomain(ctx context.Context, UpdateRequest *shared.UpdateDomainRequest) (*shared.UpdateDomainResponse, error) {
	return t.c.UpdateDomain(ctx, UpdateRequest)
}

func (t thriftServer) UpdateSchedule(ctx context.Context, Request *shared.UpdateScheduleRequest) (*shared.UpdateScheduleResponse, error) {
	return t.c.UpdateSchedule(ctx, Request)
}
