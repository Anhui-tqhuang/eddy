package eddy

import (
	"encoding/json"

	"github.com/riverqueue/river"
)

// reconcileJob is the River job behind every enqueue. It normally carries
// only which object to look at; the row is the source of truth. Gone is the
// exception: a deleted row's last state, since there is no row left to load.
type reconcileJob struct {
	Object string          `json:"kind"`
	ID     json.RawMessage `json:"id"`
	Gone   json.RawMessage `json:"gone,omitempty"`

	set settings // where and how urgently to enqueue; not stored
}

func (reconcileJob) Kind() string { return "eddy.reconcile" }

func (j reconcileJob) ref() Ref { return Ref{kind: j.Object, id: j.ID, deleted: j.Gone != nil} }

// insertOpts routes the job to its kind's queue at its kind's priority.
func (j reconcileJob) insertOpts(baseQueue string) *river.InsertOpts {
	return &river.InsertOpts{Queue: queueFor(baseQueue, j.Object, j.set), Priority: j.set.priority}
}

// resyncJob asks the leader to enqueue every object of one kind.
type resyncJob struct {
	Object string `json:"kind"`
}

func (resyncJob) Kind() string { return "eddy.resync" }
