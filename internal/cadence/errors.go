package cadence

import (
	"context"
	"errors"

	gogoproto "github.com/gogo/protobuf/proto"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"go.temporal.io/api/serviceerror"
	"go.uber.org/yarpc/encoding/protobuf"
	"go.uber.org/yarpc/yarpcerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toCadenceError translates an error from the Temporal Service into the YARPC
// error a Cadence frontend would have returned: the same code, and for the
// errors a Cadence SDK branches on, the typed details it looks for (an
// EntityNotExistsError, a WorkflowExecutionAlreadyStartedError, and so on).
// The Cadence client turns those back into its own error types, so code that
// checks for them keeps working.
func toCadenceError(err error) error {
	if err == nil {
		return nil
	}

	if yarpcerrors.IsStatus(err) {
		return err
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return yarpcerrors.DeadlineExceededErrorf("%v", err)
	case errors.Is(err, context.Canceled):
		return yarpcerrors.CancelledErrorf("%v", err)
	case errors.Is(err, errBadToken):
		return yarpcerrors.InvalidArgumentErrorf("%v", err)
	}

	st, ok := status.FromError(err)
	if !ok {
		return yarpcerrors.InternalErrorf("%v", err)
	}

	msg := st.Message()

	switch e := serviceerror.FromStatus(st).(type) {
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		return withDetails(yarpcerrors.CodeNotFound, msg, &apiv1.EntityNotExistsError{})
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		return withDetails(yarpcerrors.CodeAlreadyExists, msg, &apiv1.WorkflowExecutionAlreadyStartedError{
			StartRequestId: e.StartRequestId,
			RunId:          e.RunId,
		})
	case *serviceerror.NamespaceAlreadyExists:
		return withDetails(yarpcerrors.CodeAlreadyExists, msg, &apiv1.DomainAlreadyExistsError{})
	case *serviceerror.CancellationAlreadyRequested:
		return withDetails(yarpcerrors.CodeAlreadyExists, msg, &apiv1.CancellationAlreadyRequestedError{})
	case *serviceerror.QueryFailed:
		return withDetails(yarpcerrors.CodeInvalidArgument, msg, &apiv1.QueryFailedError{})
	case *serviceerror.NamespaceNotActive:
		return withDetails(yarpcerrors.CodeFailedPrecondition, msg, &apiv1.DomainNotActiveError{
			Domain:         e.Namespace,
			CurrentCluster: e.CurrentCluster,
			ActiveCluster:  e.ActiveCluster,
		})
	case *serviceerror.ClientVersionNotSupported:
		return withDetails(yarpcerrors.CodeFailedPrecondition, msg, &apiv1.ClientVersionNotSupportedError{
			FeatureVersion:    e.ClientVersion,
			ClientImpl:        e.ClientName,
			SupportedVersions: e.SupportedVersions,
		})
	case *serviceerror.ResourceExhausted:
		return withDetails(yarpcerrors.CodeResourceExhausted, msg, &apiv1.ServiceBusyError{Reason: e.Cause.String()})
	}

	return yarpcerrors.Newf(yarpcCode(st.Code()), "%s", msg)
}

func withDetails(code yarpcerrors.Code, msg string, details gogoproto.Message) error {
	return protobuf.NewError(code, msg, protobuf.WithErrorDetails(details))
}

// yarpcCode maps a gRPC code to YARPC's, which numbers them identically.
func yarpcCode(c codes.Code) yarpcerrors.Code {
	switch c {
	case codes.OK:
		return yarpcerrors.CodeOK
	case codes.Canceled:
		return yarpcerrors.CodeCancelled
	case codes.InvalidArgument:
		return yarpcerrors.CodeInvalidArgument
	case codes.DeadlineExceeded:
		return yarpcerrors.CodeDeadlineExceeded
	case codes.NotFound:
		return yarpcerrors.CodeNotFound
	case codes.AlreadyExists:
		return yarpcerrors.CodeAlreadyExists
	case codes.PermissionDenied:
		return yarpcerrors.CodePermissionDenied
	case codes.ResourceExhausted:
		return yarpcerrors.CodeResourceExhausted
	case codes.FailedPrecondition:
		return yarpcerrors.CodeFailedPrecondition
	case codes.Aborted:
		return yarpcerrors.CodeAborted
	case codes.OutOfRange:
		return yarpcerrors.CodeOutOfRange
	case codes.Unimplemented:
		return yarpcerrors.CodeUnimplemented
	case codes.Internal:
		return yarpcerrors.CodeInternal
	case codes.Unavailable:
		return yarpcerrors.CodeUnavailable
	case codes.DataLoss:
		return yarpcerrors.CodeDataLoss
	case codes.Unauthenticated:
		return yarpcerrors.CodeUnauthenticated
	default:
		return yarpcerrors.CodeUnknown
	}
}
