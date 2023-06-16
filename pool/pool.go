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

type report struct {
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

type jobsTracker[T any] struct {
	ds DataSource[T]

	lastJob    SequenceNumber
	retryQueue set[SequenceNumber]
	wip        jobMap[T]
}

func (s *jobsTracker[T]) Retry(n SequenceNumber) {
	s.retryQueue[n] = nod{}
}

func (s *jobsTracker[T]) Done(n SequenceNumber) {
	delete(s.retryQueue, n)
	delete(s.wip, n)
}

func (s *jobsTracker[T]) Clear() {
	maps.Clear(s.retryQueue)
	maps.Clear(s.wip)
}

func (s *jobsTracker[T]) HasNext() bool {
	return len(s.retryQueue) > 0 || s.ds.HasNext()
}

func (s *jobsTracker[T]) HasWIP() bool {
	return len(s.wip) > 0
}

// Return a job from the Retry Queue or the next job from the underlying data-source.
// Returned job is stored in the WIP collection.
// Returned job-sequence is accessible on jobsTracker.lastJob
func (s *jobsTracker[T]) Next() job[T] {
	var j job[T]
	if n, found := pop(s.retryQueue); found {
		s.lastJob = n
		j = s.wip[n]
	} else if s.ds.HasNext() {
		s.lastJob += 1
		j.SequenceNumber = s.lastJob
		j.data = s.ds.Next()
		s.wip[s.lastJob] = j
	}
	return j
}

func newJobsTracker[T any](workersPoolSize uint8, dataSource DataSource[T]) jobsTracker[T] {
	return jobsTracker[T]{
		ds:         dataSource,
		retryQueue: make(set[SequenceNumber]),
		wip:        make(jobMap[T], workersPoolSize),
	}
}

// Size is 0 than it's automatically decided.
func Process[T any](worker Worker[T], workersPoolSize uint8, dataSource DataSource[T]) {
	if !dataSource.HasNext() {
		log.Printf("Data Source is empty")
		return
	}
	jobsTracker := newJobsTracker[T](workersPoolSize, dataSource)

	workersChannel := make(chan job[T])
	workersChannelCopy := workersChannel
	reportsChannel := make(chan report)

	for wI := workersPoolSize; wI > 0; wI-- {
		go func(workerId uint8) {
			for job := range workersChannel {
				log.Printf("Worker %d on %d", workerId, job.SequenceNumber)
				reportsChannel <- report{
					SequenceNumber: job.SequenceNumber,
					WorkerReport:   worker(job.SequenceNumber, job.data),
				}
			}
		}(wI)
	}

feedingLoop:
	for jobsTracker.HasNext() || jobsTracker.HasWIP() {
		select {
		// what if I'm calling something provided by the client that puts this co-routine to sleep within the Next() method and not because the workersChannel is saturated?
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
