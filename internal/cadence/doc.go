// Package cadence serves the Cadence frontend API and translates every call onto
// the Temporal WorkflowService, so that Cadence Clients and Workers run against
// a Temporal Service without code changes.
//
// The frontend speaks both transports a Cadence SDK may use: gRPC (the
// uber.cadence.api.v1 proto API over YARPC) and TChannel (the Thrift
// WorkflowService). Both converge on one set of proto handlers; the Thrift side
// reaches them through the Cadence client's own Thrift-to-proto adapter, so the
// two cannot drift apart.
//
// Translation is stateless. The few facts a later call needs but a Cadence
// request does not carry (the Namespace and Workflow Execution behind a task
// token, and the normal Task Queue behind a sticky one) travel inside the task
// token handed to the Worker; see [taskToken].
//
// The non-obvious mappings, each documented where it happens:
//
//   - Payloads: a Cadence payload is one opaque blob; it becomes a single
//     Temporal Payload (binary/plain) and is returned byte for byte. Payloads a
//     Temporal SDK wrote as json/plain are joined with newlines, which is how
//     the Cadence Go SDK's JSON data converter lays out multiple arguments.
//   - Failures: a Cadence reason and details become an ApplicationFailure whose
//     type is the reason, so a retry policy's non-retryable reasons keep working
//     as Temporal non-retryable error types.
//   - Attempts: Cadence counts attempts from 0, Temporal from 1.
//   - Markers: a Cadence marker's details are one blob; they are stored under
//     the "data" key of the Temporal marker's details map.
//   - Activity cancellation: Cadence names the Activity by ID while Temporal
//     names it by its scheduled event, so both directions resolve one from the
//     other through the Workflow's history.
package cadence
