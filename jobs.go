package eddy

import (
	"encoding/json"
)

// reconcileJob is the River job behind every enqueue. It normally carries
// only which object to look at; the row is the source of truth. Gone is the
// exception: a deleted row's last state, since there is no row left to load.
type reconcileJob struct {
	Object string          `json:"kind"`
	ID     json.RawMessage `json:"id"`
	Gone   json.RawMessage `json:"gone,omitempty"`
}

func (reconcileJob) Kind() string { return "eddy.reconcile" }

func (j reconcileJob) ref() Ref { return Ref{kind: j.Object, id: j.ID, deleted: j.Gone != nil} }

// resyncJob asks the leader to enqueue every object of one kind.
type resyncJob struct {
	Object string `json:"kind"`
}

func (resyncJob) Kind() string { return "eddy.resync" }
