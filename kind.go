package eddy

import (
	"encoding/json"
	"fmt"
)

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
}

// NewKind returns the Kind called name. Names must be unique within a
// controller, and must not be empty.
func NewKind[ID any](name string) Kind[ID] {
	if name == "" {
		panic("eddy: NewKind: empty name")
	}
	return Kind[ID]{name: name}
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
	return Ref{kind: k.name, id: raw}
}

// Ref points at one object: a kind name and an encoded identifier. Build one
// with Kind.Of. The zero Ref is invalid.
type Ref struct {
	kind    string
	id      json.RawMessage
	deleted bool
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
