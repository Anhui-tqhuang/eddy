// Package eddy runs reconcile loops with Postgres as the source of truth, the
// queue and the leader election.
//
// A Table maps a Go struct to a database table. The API writes rows through
// it with a DB, which is used like a pgx pool; each Insert, Update and Delete
// enqueues the row's reconcile in the same transaction. A Controller, run on
// any number of replicas, elects one leader through a Postgres advisory lock.
// The leader works the jobs on River: it loads each row, calls the function
// registered with Reconcile, and saves the status fields that function set.
//
// Kind, Handle and Enqueuer are the smaller core underneath, for objects
// that are not one row of one table.
package eddy
