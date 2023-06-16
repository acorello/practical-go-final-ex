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

type jobsTracker[T any] struct {
	ds DataSource[T]

	// counter exclusively incremented each time we get new data from ds
	counter SequenceNumber
	// the number of the last job we returned: maybe from ds.Next() or from the retrySet
	lastJob SequenceNumber

	inProgress jobMap[T]
	// retrySet is always a subset of wip
	retrySet set[SequenceNumber]
}

func (s *jobsTracker[T]) panicIfJobNotInProgress(n SequenceNumber) {
	if _, found := s.inProgress[n]; !found {
		log.Panicf("Job Number %d not found in WIP: %v", n, maps.Keys(s.inProgress))
	}
}

// The sequence number of the WIP job we want to retry.
//
// Panics if job not in WIP.
func (s *jobsTracker[T]) Retry(n SequenceNumber) {
	s.panicIfJobNotInProgress(n)
	s.retrySet[n] = nod{}
}

func (s *jobsTracker[T]) Done(n SequenceNumber) {
	s.panicIfJobNotInProgress(n)
	delete(s.inProgress, n)
	delete(s.retrySet, n)
}

func (s *jobsTracker[T]) Clear() {
	maps.Clear(s.retrySet)
	maps.Clear(s.inProgress)
}

func (s *jobsTracker[T]) HasNext() bool {
	return len(s.retrySet) > 0 || s.ds.HasNext()
}

func (s *jobsTracker[T]) HasWIP() bool {
	return len(s.inProgress) > 0
}

// Return a job from the Retry Queue or the next job from the underlying data-source.
// Returned job is stored in the WIP collection.
// Returned job-sequence is accessible on jobsTracker.lastJob
func (s *jobsTracker[T]) Next() (j job[T]) {
	if n, found := pop(s.retrySet); found {
		j = s.inProgress[n]
	} else if s.ds.HasNext() {
		s.counter += 1
		j.SequenceNumber = s.counter
		j.data = s.ds.Next()
		s.inProgress[j.SequenceNumber] = j
	}
	// if we got neither we're using the zero value
	s.lastJob = j.SequenceNumber
	return j
}

func newJobsTracker[T any](workersPoolSize uint8, dataSource DataSource[T]) jobsTracker[T] {
	return jobsTracker[T]{
		ds: dataSource,

		retrySet:   make(set[SequenceNumber]),
		inProgress: make(jobMap[T], workersPoolSize),
	}
}

// Size is 0 than it's automatically decided.
func Process[T any](worker Worker[T], workersPoolSize uint8, dataSource DataSource[T]) {
	jobsTracker := newJobsTracker[T](workersPoolSize, dataSource)

	workersChannel := make(chan job[T])
	workersChannelCopy := workersChannel
	reportsChannel := make(chan jobReport)

	for wI := workersPoolSize; wI > 0; wI-- {
		go func(workerId uint8) {
			for job := range workersChannel {
				log.Printf("Worker %d on %d", workerId, job.SequenceNumber)
				reportsChannel <- jobReport{
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
