// Package eddy runs reconcile loops with Postgres as the source of truth, the
// queue and the leader election.
//
// A Table maps a Go struct to a database table. Open returns a DB, which is
// used like a pgx pool; each Insert, Update, Delete and Exec through a Table
// enqueues the touched rows' reconciles in the same transaction. DB.Run is
// the controller: run it on any number of replicas, and the one that holds a
// Postgres advisory lock works the jobs on River. It loads each row, calls
// the function registered with Reconcile, and saves the status fields that
// function set; deleted rows go to the function registered with Teardown.
//
// An object has at most one waiting reconcile job: a write or a resync uses the one that
// waits (writes bring it forward), and a Retry gives way to a job that already waits. A
// write while the object's reconcile runs still enqueues one more.
//
// Kind and Handle are the smaller core underneath, for objects that are not
// one row of one table.
package eddy
