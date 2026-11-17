package cadence

import (
	"bytes"
	"errors"

	"google.golang.org/protobuf/encoding/protowire"
)

// tokenMagic prefixes every task token this frontend issues, so one it did not
// issue (a Temporal token, or garbage) is rejected rather than misread.
var tokenMagic = []byte{0xca, 0xde, 0x01}

var errBadToken = errors.New("task token was not issued by this Cadence frontend")

// taskToken wraps the Temporal task token handed to a Cadence Worker together
// with the facts a later call needs but a Cadence request does not carry. A
// Cadence Respond* request names no Domain, while Temporal requires the
// Namespace on every call; a sticky Task Queue needs its Workflow's normal Task
// Queue; and resolving an Activity ID to its scheduled event needs the Workflow
// Execution. It is encoded as protobuf wire fields after [tokenMagic].
type taskToken struct {
	Temporal   []byte
	Namespace  string
	WorkflowID string
	RunID      string
	// TaskQueue is the Workflow's normal Task Queue, for a workflow task.
	TaskQueue string
	// ActivityID names the Activity, for an activity task.
	ActivityID string
}

const (
	tokenFieldTemporal protowire.Number = iota + 1
	tokenFieldNamespace
	tokenFieldWorkflowID
	tokenFieldRunID
	tokenFieldTaskQueue
	tokenFieldActivityID
)

func (t *taskToken) encode() []byte {
	if t == nil || len(t.Temporal) == 0 {
		return nil
	}

	b := append([]byte(nil), tokenMagic...)
	b = protowire.AppendTag(b, tokenFieldTemporal, protowire.BytesType)
	b = protowire.AppendBytes(b, t.Temporal)

	for _, f := range []struct {
		n protowire.Number
		v string
	}{
		{tokenFieldNamespace, t.Namespace},
		{tokenFieldWorkflowID, t.WorkflowID},
		{tokenFieldRunID, t.RunID},
		{tokenFieldTaskQueue, t.TaskQueue},
		{tokenFieldActivityID, t.ActivityID},
	} {
		if f.v == "" {
			continue
		}

		b = protowire.AppendTag(b, f.n, protowire.BytesType)
		b = protowire.AppendString(b, f.v)
	}

	return b
}

func decodeToken(b []byte) (*taskToken, error) {
	if !bytes.HasPrefix(b, tokenMagic) {
		return nil, errBadToken
	}

	b = b[len(tokenMagic):]
	t := &taskToken{}

	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || typ != protowire.BytesType {
			return nil, errBadToken
		}

		b = b[n:]

		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return nil, errBadToken
		}

		b = b[n:]

		switch num {
		case tokenFieldTemporal:
			t.Temporal = append([]byte(nil), v...)
		case tokenFieldNamespace:
			t.Namespace = string(v)
		case tokenFieldWorkflowID:
			t.WorkflowID = string(v)
		case tokenFieldRunID:
			t.RunID = string(v)
		case tokenFieldTaskQueue:
			t.TaskQueue = string(v)
		case tokenFieldActivityID:
			t.ActivityID = string(v)
		}
	}

	if len(t.Temporal) == 0 || t.Namespace == "" {
		return nil, errBadToken
	}

	return t, nil
}
