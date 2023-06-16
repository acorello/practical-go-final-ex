package pool

import (
	"log"

	"golang.org/x/exp/maps"
)

// this datasource is used to pull data to feed workers
type DataSource[T any] interface {
	Next() T
	HasNext() bool
}

type WorkerReport uint

const (
	MoveOn WorkerReport = iota
	AbortBatch
	Retry
)

/*
A worker is a client provided function which is given a sequence number of `data T`, useful to combine the results on the client side, and the `data T` to be processed.

If retry the data is submitted again with the same sequence number.
*/
type Worker[T any] func(n SequenceNumber, data T) WorkerReport

type SequenceNumber = uint

type jobReport struct {
	SequenceNumber
	WorkerReport
}

type job[T any] struct {
	SequenceNumber
	data T
}

type nod struct{}

type jobMap[T any] map[SequenceNumber]job[T]

type set[T comparable] map[T]nod

// given a DataSource
// assigns a sequence number to each new piece of data
// keeps track of the data being returned until it's flagged as Done()
// if flagged as Retry() it returns the data again before consuming more
//
// It's up to he Worker to decide if an how many times to retry a job.
// The worker can also decide  to abort the whole computation.
type jobsTracker[T any] struct {
	ds DataSource[T]
	// incremented each time we call ds.Next()
	counter SequenceNumber
	// contains all jobs returned by Next() but not yet Done()
	inProgress jobMap[T]
	// the subset of `inProgress` jobs marked as Retry()
	retrySet set[SequenceNumber]
}

// The sequence number of the WIP job we want to retry.
//
// Panics if job not in WIP.
func (jobs *jobsTracker[T]) Retry(n SequenceNumber) {
	jobs.requireJobInProgress(n)
	jobs.retrySet[n] = nod{}
}

func (jobs *jobsTracker[T]) Done(n SequenceNumber) {
	jobs.requireJobInProgress(n)
	delete(jobs.inProgress, n)
	delete(jobs.retrySet, n)
}

func (jobs *jobsTracker[T]) Clear() {
	maps.Clear(jobs.inProgress)
	maps.Clear(jobs.retrySet)
}

func (jobs *jobsTracker[T]) HasNext() bool {
	return len(jobs.retrySet) > 0 || jobs.ds.HasNext()
}

func (jobs *jobsTracker[T]) HasWIP() bool {
	return len(jobs.inProgress) > 0
}

// Return a job from the Retry Queue or the next job from the underlying data-source.
// Returned job is stored in the WIP collection.
// Returned job-sequence is accessible on jobsTracker.lastJob
func (jobs *jobsTracker[T]) Next() (j job[T]) {
	if n, found := pop(jobs.retrySet); found {
		j = jobs.inProgress[n]
	} else if jobs.ds.HasNext() {
		jobs.counter += 1
		j.SequenceNumber = jobs.counter
		j.data = jobs.ds.Next()
		jobs.inProgress[j.SequenceNumber] = j
	}
	return j
}

func (jobs *jobsTracker[T]) requireJobInProgress(n SequenceNumber) {
	if _, found := jobs.inProgress[n]; !found {
		log.Panicf("Job Number %d not found in WIP: %v", n, maps.Keys(jobs.inProgress))
	}
}

func newJobsTracker[T any](workersPoolSize uint8, dataSource DataSource[T]) jobsTracker[T] {
	return jobsTracker[T]{
		ds: dataSource,

		retrySet:   make(set[SequenceNumber]),
		inProgress: make(jobMap[T], workersPoolSize),
	}
}

// if workersPoolSize is 0 than it's automatically decided.
func Process[T any](worker Worker[T], workersPoolSize uint8, dataSource DataSource[T]) {
	if workersPoolSize == 0 {
		workersPoolSize = 4
	}

	jobsTracker := newJobsTracker[T](workersPoolSize, dataSource)

	workersChannel := make(chan job[T])
	workersChannelCopy := workersChannel
	reportsChannel := make(chan jobReport)
	startWorker := func(workerId uint8) {
		for job := range workersChannel {
			log.Printf("Worker %d on job %d", workerId, job.SequenceNumber)
			workerReport := worker(job.SequenceNumber, job.data)
			reportsChannel <- jobReport{
				SequenceNumber: job.SequenceNumber,
				WorkerReport:   workerReport,
			}
		}
	}

	for wI := workersPoolSize; wI > 0; wI-- {
		go startWorker(wI)
	}

feedingLoop:
	for jobsTracker.HasNext() || jobsTracker.HasWIP() {
		select {
		// ❓ what if I'm calling something provided by the client that puts this co-routine to sleep within the Next() method and not because the workersChannel is saturated?
		case workersChannel <- jobsTracker.Next():
			if !jobsTracker.HasNext() {
				// disable this ‹case›, otherwise we'll feed zero(job)
				workersChannel = nil
			}
		case report := <-reportsChannel:
			switch report.WorkerReport {
			case AbortBatch:
				break feedingLoop
			case MoveOn:
				jobsTracker.Done(report.SequenceNumber)
			case Retry:
				jobsTracker.Retry(report.SequenceNumber)
				if jobsTracker.HasNext() && workersChannel == nil {
					workersChannel = workersChannelCopy
				}
			}
		}
	}
	// CLEAN-UP
	close(workersChannelCopy)
	jobsTracker.Clear()
}

func pop(m set[SequenceNumber]) (SequenceNumber, bool) {
	for n := range m {
		delete(m, n)
		return n, true
	}
	return 0, false
}
