package eddy

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
)

// settings are the per-kind scheduling choices made where a Kind or Table is
// declared, so the API (which inserts jobs) and the controller (which works
// them) agree.
type settings struct {
	priority int // River priority 1-4; 0 means the default
	workers  int // size of the kind's own queue; 0 shares the main queue
}

// SchedulingOption is accepted by both NewKind and NewTable: see Priority
// and MaxWorkers.
type SchedulingOption interface {
	KindOption
	TableOption
}

// KindOption configures NewKind.
type KindOption interface{ applyKind(*settings) }

type schedulingOption func(*settings)

func (f schedulingOption) applyKind(s *settings)      { f(s) }
func (f schedulingOption) applyTable(o *tableOptions) { f(&o.set) }

// Priority sets how urgently this kind's reconciles are picked up relative
// to other kinds sharing a queue: 1 (the default) is most urgent, 4 least.
// Use a higher number for bulk or background kinds.
func Priority(n int) SchedulingOption {
	if n < 1 || n > 4 {
		panic(fmt.Sprintf("eddy: Priority(%d): must be between 1 and 4", n))
	}
	return schedulingOption(func(s *settings) { s.priority = n })
}

// MaxWorkers caps how many reconciles of this kind run at once, by giving
// the kind a queue of its own, separate from the shared one sized by
// WithWorkers. A slow or noisy kind then cannot crowd out the others, and
// the others cannot starve it.
func MaxWorkers(n int) SchedulingOption {
	if n < 1 {
		panic(fmt.Sprintf("eddy: MaxWorkers(%d): need at least 1", n))
	}
	return schedulingOption(func(s *settings) { s.workers = n })
}

// Kind names a type of object that eddy reconciles, together with the Go type
// of its identifier. It is the layer under Table, for objects that are not
// one row of one table. Declare one per resource type and share it between
// the API (which enqueues) and the controller (which handles):
//
//	var Apps = eddy.NewKind[uuid.UUID]("app")
//
// ID must round-trip through encoding/json; uuid.UUID, int64 and string all
// do.
type Kind[ID any] struct {
	name string
	set  settings
}

// NewKind returns the Kind called name. Names must be unique within a
// controller, and must not be empty.
func NewKind[ID any](name string, opts ...KindOption) Kind[ID] {
	if name == "" {
		panic("eddy: NewKind: empty name")
	}
	k := Kind[ID]{name: name}
	for _, opt := range opts {
		opt.applyKind(&k.set)
	}
	return k
}

// Name returns the kind's name.
func (k Kind[ID]) Name() string { return k.name }

// Of returns a reference to the object of this kind identified by id, ready
// to enqueue. It panics if id cannot be encoded as JSON, which for the usual
// identifier types never happens.
func (k Kind[ID]) Of(id ID) Ref {
	raw, err := json.Marshal(id)
	if err != nil {
		panic(fmt.Sprintf("eddy: %s.Of: encode id: %v", k.name, err))
	}
	return Ref{kind: k.name, id: raw, set: k.set}
}

// Ref points at one object: a kind name and an encoded identifier. Build one
// with Kind.Of or Table.Of. The zero Ref is invalid.
type Ref struct {
	kind    string
	id      json.RawMessage
	deleted bool
	set     settings
}

// Kind returns the referenced object's kind name.
func (r Ref) Kind() string { return r.kind }

// ID returns the object's identifier as JSON text, e.g. 42 or "acme", for
// logs, metrics labels and span attributes.
func (r Ref) ID() string { return string(r.id) }

// Deleted reports whether the reference is to a deleted table row, whose
// reconcile is a teardown. It is set only on the Ref middleware receives.
func (r Ref) Deleted() bool { return r.deleted }

// String formats the reference as kind/id, for logs.
func (r Ref) String() string { return r.kind + "/" + string(r.id) }

func (r Ref) valid() bool { return r.kind != "" && len(r.id) > 0 }

// maxQueueName is River's limit on queue names.
const maxQueueName = 64

// queueFor returns the queue a kind's jobs go to: the base queue, or the
// kind's own when it has MaxWorkers.
func queueFor(base, kind string, set settings) string {
	if set.workers == 0 {
		return base
	}
	// River allows lower-case letters and digits, joined by single "_" or "-".
	var b strings.Builder
	sep := true // no leading separator
	for _, c := range strings.ToLower(kind) {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			b.WriteRune(c)
			sep = false
		} else if !sep {
			b.WriteByte('_')
			sep = true
		}
	}
	name := base + "-" + strings.TrimRight(b.String(), "_")
	if strings.HasSuffix(name, "-") || len(name) > maxQueueName {
		// Nothing usable in the kind name, or too long: fall back to a hash.
		h := fnv.New32a()
		_, _ = h.Write([]byte(kind)) // fnv never fails
		suffix := fmt.Sprintf("-%08x", h.Sum32())
		name = strings.TrimRight(name[:min(len(name), maxQueueName-len(suffix))], "-_") + suffix
	}
	return name
}
