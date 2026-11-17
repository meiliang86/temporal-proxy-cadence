package cadence

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"go.uber.org/yarpc/yarpcerrors"
)

// Cadence and Temporal list filters share a SQL-like grammar but differ in the
// names of the system attributes, in how a close status and a time are
// written, and in how an open Workflow is selected. rewriteQuery bridges those
// differences token by token and leaves everything else, custom search
// attributes included, as written.

// renamedAttributes maps Cadence system attribute names onto Temporal's.
var renamedAttributes = map[string]string{
	"workflowid":    "WorkflowId",
	"runid":         "RunId",
	"workflowtype":  "WorkflowType",
	"tasklist":      "TaskQueue",
	"starttime":     "StartTime",
	"closetime":     "CloseTime",
	"executiontime": "ExecutionTime",
	"historylength": "HistoryLength",
	"closestatus":   "ExecutionStatus",

	"cadencechangeversion": temporalChangeVersion,
}

var timeAttributes = map[string]bool{"StartTime": true, "CloseTime": true, "ExecutionTime": true}

// cadenceCloseStatuses spells each Cadence close status, by its Thrift number
// and its name, the way Temporal visibility does.
var cadenceCloseStatuses = map[string]string{
	"0": "Completed", "completed": "Completed",
	"1": "Failed", "failed": "Failed",
	"2": "Canceled", "canceled": "Canceled",
	"3": "Terminated", "terminated": "Terminated",
	"4": "ContinuedAsNew", "continued_as_new": "ContinuedAsNew", "continuedasnew": "ContinuedAsNew",
	"5": "TimedOut", "timed_out": "TimedOut", "timedout": "TimedOut",
}

type tokenKind int

const (
	tokSpace tokenKind = iota
	tokIdent
	tokString
	tokNumber
	tokOp
	tokPunct
)

type token struct {
	kind tokenKind
	text string
}

// rewriteQuery translates a Cadence visibility query into a Temporal one.
func rewriteQuery(q string) (string, error) {
	toks, err := tokenize(q)
	if err != nil {
		return "", err
	}

	var (
		out     strings.Builder
		attr    string // the system attribute the current comparison is about
		between bool   // inside "x BETWEEN a AND b", whose AND is not a conjunction
	)

	for i := 0; i < len(toks); i++ {
		t := toks[i]

		switch t.kind {
		case tokIdent:
			lower := strings.ToLower(t.text)

			switch lower {
			case "and":
				if between {
					between = false
				} else {
					attr = ""
				}

				out.WriteString(t.text)

				continue
			case "or", "not", "in", "is", "null", "order", "by", "asc", "desc":
				if lower == "or" {
					attr = ""
				}

				out.WriteString(t.text)

				continue
			case "between":
				between = true
				out.WriteString(t.text)

				continue
			}

			// "CloseTime = missing" is Cadence's way of selecting open Workflows.
			if lower == "closetime" {
				if op, val, n := comparison(toks, i); n > 0 && strings.EqualFold(val.text, "missing") {
					if op.text == "=" {
						out.WriteString("ExecutionStatus = 'Running'")
					} else {
						out.WriteString("ExecutionStatus != 'Running'")
					}

					i += n

					continue
				}
			}

			if name, ok := renamedAttributes[lower]; ok {
				attr = name
				out.WriteString(name)

				continue
			}

			if attr == "ExecutionStatus" {
				// A bare enum name, such as CloseStatus = COMPLETED.
				out.WriteString(closeStatusLiteral(t.text))

				continue
			}

			attr = ""
			out.WriteString(t.text)
		case tokString, tokNumber:
			out.WriteString(literal(attr, t))
		default:
			out.WriteString(t.text)
		}
	}

	return out.String(), nil
}

// comparison reads the operator and value after the identifier at i, skipping
// whitespace, and returns how many tokens past i they span (0 if absent).
func comparison(toks []token, i int) (op, val token, n int) {
	j := i + 1
	skip := func() {
		for j < len(toks) && toks[j].kind == tokSpace {
			j++
		}
	}

	skip()
	if j >= len(toks) || toks[j].kind != tokOp {
		return op, val, 0
	}

	op = toks[j]
	j++

	skip()
	if j >= len(toks) {
		return op, val, 0
	}

	return op, toks[j], j - i
}

// literal rewrites a value compared against attr.
func literal(attr string, t token) string {
	switch {
	case attr == "ExecutionStatus":
		return closeStatusLiteral(unquote(t))
	case timeAttributes[attr] && t.kind == tokNumber:
		// Cadence takes a time as Unix nanoseconds; Temporal wants RFC 3339.
		if n, err := strconv.ParseInt(t.text, 10, 64); err == nil {
			return "'" + time.Unix(0, n).UTC().Format(time.RFC3339Nano) + "'"
		}
	}

	if t.kind == tokString {
		return "'" + strings.ReplaceAll(unquote(t), "'", "''") + "'"
	}

	return t.text
}

func closeStatusLiteral(v string) string {
	if s, ok := cadenceCloseStatuses[strings.ToLower(v)]; ok {
		return "'" + s + "'"
	}

	return "'" + v + "'"
}

func unquote(t token) string {
	if t.kind != tokString || len(t.text) < 2 {
		return t.text
	}

	q := t.text[0]
	body := t.text[1 : len(t.text)-1]

	return strings.ReplaceAll(body, string([]byte{q, q}), string(q))
}

func tokenize(q string) ([]token, error) {
	var toks []token

	rs := []rune(q)
	for i := 0; i < len(rs); {
		r := rs[i]
		start := i

		switch {
		case unicode.IsSpace(r):
			for i < len(rs) && unicode.IsSpace(rs[i]) {
				i++
			}

			toks = append(toks, token{tokSpace, string(rs[start:i])})
		case r == '\'' || r == '"' || r == '`':
			i++
			for {
				if i >= len(rs) {
					return nil, fmt.Errorf("unterminated string in query at offset %d", start)
				}

				if rs[i] == r {
					if i+1 < len(rs) && rs[i+1] == r {
						i += 2
						continue
					}

					i++

					break
				}

				i++
			}

			if r == '`' {
				// A backquoted identifier.
				toks = append(toks, token{tokIdent, string(rs[start+1 : i-1])})
			} else {
				toks = append(toks, token{tokString, string(rs[start:i])})
			}
		case unicode.IsDigit(r) || (r == '-' && i+1 < len(rs) && unicode.IsDigit(rs[i+1]) && precedesValue(toks)):
			i++
			for i < len(rs) && (unicode.IsDigit(rs[i]) || rs[i] == '.' || rs[i] == 'e' || rs[i] == 'E') {
				i++
			}

			toks = append(toks, token{tokNumber, string(rs[start:i])})
		case unicode.IsLetter(r) || r == '_':
			for i < len(rs) && (unicode.IsLetter(rs[i]) || unicode.IsDigit(rs[i]) || rs[i] == '_' || rs[i] == '.') {
				i++
			}

			toks = append(toks, token{tokIdent, string(rs[start:i])})
		case strings.ContainsRune("=<>!", r):
			for i < len(rs) && strings.ContainsRune("=<>!", rs[i]) {
				i++
			}

			toks = append(toks, token{tokOp, string(rs[start:i])})
		default:
			i++
			toks = append(toks, token{tokPunct, string(r)})
		}
	}

	return toks, nil
}

// precedesValue reports whether the last significant token is one a value can
// follow, so a leading '-' is a sign rather than an operator.
func precedesValue(toks []token) bool {
	for _, tok := range slices.Backward(toks) {
		switch tok.kind {
		case tokSpace:
			continue
		case tokOp, tokPunct:
			return true
		case tokIdent:
			l := strings.ToLower(tok.text)
			return l == "and" || l == "between" || l == "in"
		default:
			return false
		}
	}

	return true
}

func errBadQuery(err error) error {
	return yarpcerrors.InvalidArgumentErrorf("invalid visibility query: %v", err)
}
