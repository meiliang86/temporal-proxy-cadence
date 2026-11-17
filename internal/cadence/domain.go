package cadence

import (
	"context"
	"time"

	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// minRetention is the shortest retention a Temporal Service accepts by
// default. Cadence allows shorter, down to none at all, so a Domain registered
// with less gets this.
const minRetention = 24 * time.Hour

func toRetention(d time.Duration) *durationpb.Duration {
	return durationpb.New(max(d, minRetention))
}

func fromNamespace(r *workflowservice.DescribeNamespaceResponse) *apiv1.Domain {
	if r == nil {
		return nil
	}

	info, cfg, repl := r.NamespaceInfo, r.Config, r.ReplicationConfig
	out := &apiv1.Domain{
		Id:                               info.GetId(),
		Name:                             info.GetName(),
		Status:                           fromNamespaceState(info.GetState()),
		Description:                      info.GetDescription(),
		OwnerEmail:                       info.GetOwnerEmail(),
		Data:                             info.GetData(),
		WorkflowExecutionRetentionPeriod: fromDuration(cfg.GetWorkflowExecutionRetentionTtl()),
		HistoryArchivalStatus:            fromArchivalState(cfg.GetHistoryArchivalState()),
		HistoryArchivalUri:               cfg.GetHistoryArchivalUri(),
		VisibilityArchivalStatus:         fromArchivalState(cfg.GetVisibilityArchivalState()),
		VisibilityArchivalUri:            cfg.GetVisibilityArchivalUri(),
		ActiveClusterName:                repl.GetActiveClusterName(),
		FailoverVersion:                  r.FailoverVersion,
		IsGlobalDomain:                   r.IsGlobalNamespace,
	}

	for _, c := range repl.GetClusters() {
		out.Clusters = append(out.Clusters, &apiv1.ClusterReplicationConfiguration{ClusterName: c.ClusterName})
	}

	if bins := cfg.GetBadBinaries().GetBinaries(); len(bins) > 0 {
		out.BadBinaries = &apiv1.BadBinaries{Binaries: make(map[string]*apiv1.BadBinaryInfo, len(bins))}
		for k, b := range bins {
			out.BadBinaries.Binaries[k] = &apiv1.BadBinaryInfo{
				Reason:      b.Reason,
				Operator:    b.Operator,
				CreatedTime: fromTimestamp(b.CreateTime),
			}
		}
	}

	return out
}

func toClusters(in []*apiv1.ClusterReplicationConfiguration) []*replicationpb.ClusterReplicationConfig {
	var out []*replicationpb.ClusterReplicationConfig
	for _, c := range in {
		out = append(out, &replicationpb.ClusterReplicationConfig{ClusterName: c.ClusterName})
	}

	return out
}

// RegisterDomain registers a Namespace.
func (h *Handler) RegisterDomain(ctx context.Context, req *apiv1.RegisterDomainRequest) (*apiv1.RegisterDomainResponse, error) {
	_, err := h.workflow.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        req.Name,
		Description:                      req.Description,
		OwnerEmail:                       req.OwnerEmail,
		WorkflowExecutionRetentionPeriod: toRetention(goDuration(req.WorkflowExecutionRetentionPeriod)),
		Clusters:                         toClusters(req.Clusters),
		ActiveClusterName:                req.ActiveClusterName,
		Data:                             req.Data,
		SecurityToken:                    req.SecurityToken,
		IsGlobalNamespace:                req.IsGlobalDomain,
		HistoryArchivalState:             toArchivalState(req.HistoryArchivalStatus),
		HistoryArchivalUri:               req.HistoryArchivalUri,
		VisibilityArchivalState:          toArchivalState(req.VisibilityArchivalStatus),
		VisibilityArchivalUri:            req.VisibilityArchivalUri,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.RegisterDomainResponse{}, nil
}

// DescribeDomain describes a Namespace by name or ID.
func (h *Handler) DescribeDomain(ctx context.Context, req *apiv1.DescribeDomainRequest) (*apiv1.DescribeDomainResponse, error) {
	resp, err := h.workflow.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: req.GetName(),
		Id:        req.GetId(),
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.DescribeDomainResponse{Domain: fromNamespace(resp)}, nil
}

// ListDomains lists Namespaces.
func (h *Handler) ListDomains(ctx context.Context, req *apiv1.ListDomainsRequest) (*apiv1.ListDomainsResponse, error) {
	resp, err := h.workflow.ListNamespaces(ctx, &workflowservice.ListNamespacesRequest{
		PageSize:      req.PageSize,
		NextPageToken: req.NextPageToken,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	out := &apiv1.ListDomainsResponse{NextPageToken: resp.NextPageToken}
	for _, n := range resp.Namespaces {
		out.Domains = append(out.Domains, fromNamespace(n))
	}

	return out, nil
}

// UpdateDomain updates the Namespace fields named by the request's update mask.
func (h *Handler) UpdateDomain(ctx context.Context, req *apiv1.UpdateDomainRequest) (*apiv1.UpdateDomainResponse, error) {
	treq := &workflowservice.UpdateNamespaceRequest{
		Namespace:       req.Name,
		SecurityToken:   req.SecurityToken,
		DeleteBadBinary: req.DeleteBadBinary,
	}

	info := &namespacepb.UpdateNamespaceInfo{}
	cfg := &namespacepb.NamespaceConfig{}

	var setInfo, setCfg bool

	for _, path := range req.GetUpdateMask().GetPaths() {
		switch path {
		case "description":
			info.Description, setInfo = req.Description, true
		case "owner_email":
			info.OwnerEmail, setInfo = req.OwnerEmail, true
		case "data":
			info.Data, setInfo = req.Data, true
		case "workflow_execution_retention_period":
			cfg.WorkflowExecutionRetentionTtl, setCfg = toRetention(goDuration(req.WorkflowExecutionRetentionPeriod)), true
		case "bad_binaries":
			cfg.BadBinaries, setCfg = toBadBinaries(req.BadBinaries), true
		case "history_archival_status":
			cfg.HistoryArchivalState, setCfg = toArchivalState(req.HistoryArchivalStatus), true
		case "history_archival_uri":
			cfg.HistoryArchivalUri, setCfg = req.HistoryArchivalUri, true
		case "visibility_archival_status":
			cfg.VisibilityArchivalState, setCfg = toArchivalState(req.VisibilityArchivalStatus), true
		case "visibility_archival_uri":
			cfg.VisibilityArchivalUri, setCfg = req.VisibilityArchivalUri, true
		case "active_cluster_name", "clusters":
			treq.ReplicationConfig = &replicationpb.NamespaceReplicationConfig{
				ActiveClusterName: req.ActiveClusterName,
				Clusters:          toClusters(req.Clusters),
			}
		}
	}

	if setInfo {
		treq.UpdateInfo = info
	}

	if setCfg {
		treq.Config = cfg
	}

	resp, err := h.workflow.UpdateNamespace(ctx, treq)
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.UpdateDomainResponse{Domain: fromNamespace(&workflowservice.DescribeNamespaceResponse{
		NamespaceInfo:     resp.NamespaceInfo,
		Config:            resp.Config,
		ReplicationConfig: resp.ReplicationConfig,
		FailoverVersion:   resp.FailoverVersion,
		IsGlobalNamespace: resp.IsGlobalNamespace,
	})}, nil
}

func toBadBinaries(b *apiv1.BadBinaries) *namespacepb.BadBinaries {
	if b == nil {
		return nil
	}

	out := &namespacepb.BadBinaries{Binaries: make(map[string]*namespacepb.BadBinaryInfo, len(b.Binaries))}
	for k, v := range b.Binaries {
		out.Binaries[k] = &namespacepb.BadBinaryInfo{
			Reason:     v.Reason,
			Operator:   v.Operator,
			CreateTime: toTimestamp(v.CreatedTime),
		}
	}

	return out
}

// DeprecateDomain deprecates a Namespace.
func (h *Handler) DeprecateDomain(ctx context.Context, req *apiv1.DeprecateDomainRequest) (*apiv1.DeprecateDomainResponse, error) {
	//nolint:staticcheck // The Cadence call has no other counterpart.
	_, err := h.workflow.DeprecateNamespace(ctx, &workflowservice.DeprecateNamespaceRequest{
		Namespace:     req.Name,
		SecurityToken: req.SecurityToken,
	})
	if err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.DeprecateDomainResponse{}, nil
}

// DeleteDomain deletes a Namespace through the OperatorService.
func (h *Handler) DeleteDomain(ctx context.Context, req *apiv1.DeleteDomainRequest) (*apiv1.DeleteDomainResponse, error) {
	if h.operator == nil {
		return nil, unimplemented("DeleteDomain")
	}

	if _, err := h.operator.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{Namespace: req.Name}); err != nil {
		return nil, toCadenceError(err)
	}

	return &apiv1.DeleteDomainResponse{}, nil
}

// FailoverDomain is not supported; fail over a Temporal Namespace with
// Temporal's own tooling.
func (h *Handler) FailoverDomain(context.Context, *apiv1.FailoverDomainRequest) (*apiv1.FailoverDomainResponse, error) {
	return nil, unimplemented("FailoverDomain")
}

// ListFailoverHistory is not supported.
func (h *Handler) ListFailoverHistory(
	context.Context, *apiv1.ListFailoverHistoryRequest,
) (*apiv1.ListFailoverHistoryResponse, error) {
	return nil, unimplemented("ListFailoverHistory")
}
