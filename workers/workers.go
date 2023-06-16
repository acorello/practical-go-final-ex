/*
Coordinator for a pool of concurrent workers

Two ways to submit the data to be worked on:
  - pass a data stream at instantiation time and run the pool until provider is exhausted
		note: there is always a `Data-Collection -> Data-Stream`
  - call `pool.WorkOn(Job)` in an externally controlled iterator chunking logic (or chunks of work) and worker logic are external dependencies.

In the first case, It seems obvious to me that the initialization of is non-blocking and the client will invoke another method to get the result. Would I want to use a channel to signal the work is done (like in context.Context.Done() ?) or expose a Wait() method?

Should the combination of the different results be left to the Worker implementation or be managed by us? Definitely a client side concern: we know nothing about how to combine the results.


Error could be used to signal a the pool should abort execution `workers.FatalError`.

worker :: T -> Report

type Report = Report JobSequence error

If so the Worker should have a sequence number of the Job in case it needs to map out of order processes to an ordered sequence.

Regardless of how we feed the pool: pass a data-source (at initialization time, or to a method) and let the pool PULL data; there should be a way to notify completion. On the other hand, liveness is known by virtue of invoking the workers? If the client knows how many jobs it will generate it can also keep track of how many jobs have been submitted and hence report progress.

*/

package workers

import (
	"golang.org/x/exp/maps"
)

type JobSeqNumber uint

type Job[T any] struct {
	Data T
	JobSeqNumber
}

type Report struct {
	Error error
	JobSeqNumber
}

type Worker[T any] func(Job[T]) Report

type pool[T any] struct {
	size uint8
	do   Worker[T]

	workersChannel chan Job[T]
	reportsChannel chan Report

	jobs    map[JobSeqNumber]Job[T]
	reports []Report
}

type Source[T any] interface {
	PoolSize() uint8
	Worker() Worker[T]
}

func NewPool[T any](size uint8) pool[T] {
	return pool[T]{
		size: size,

		workersChannel: make(chan Job[T]),
		reportsChannel: make(chan Report),
	}
}

func (p *pool[T]) hasJobs() bool {
	return len(p.jobs) > 0
}

func (p *pool[T]) checkIn(r Report) {
	// aborts on first error to keep things simple
	p.reports = append(p.reports, r)
	if r.Error != nil {
		// TODO: we may want to report that some jobs have not been attempted
		maps.Clear(p.jobs)
	}
}

func (I *pool[T]) checkOutJob() (job Job[T]) {
	for k, v := range I.jobs {
		delete(I.jobs, k)
		job = v
		break
	}
	return
}

// I assumed jobs is a property of the 'pool', but it's should be rather part of a specific execution. And so the worker! And so the channels, as they have to be data-specific!
func (my *pool[T]) Run() {
	for n := my.size; n > 0; n-- {
		go func() {
			for job := range my.workersChannel {
				my.reportsChannel <- my.do(job)
			}
		}()
	}
	// how do I structure this code?
	// `nextTask` procedure has to take a data structure
	_workersChannel := my.workersChannel
	wip := 0
	for my.hasJobs() || wip > 0 {
		// QUESTION: if I wrapped the writing to the channel in a procedure, could I still use it as case expression?
		select {
		case my.workersChannel <- my.checkOutJob():
			wip += 1
			if !my.hasJobs() {
				// prevent feeding zero(Job) into my.workersChannel
				my.workersChannel = nil
			}
		case report := <-my.reportsChannel:
			wip -= 1
			my.checkIn(report)
			if my.hasJobs() && my.workersChannel == nil {
				my.workersChannel = _workersChannel
			}
			// if 'all workers are busy' and 'reportsChannel is empty' and '!hasWork()'
			// select blocks and will unblock when new reports arrive.
		}
	}
	close(my.workersChannel)
	// if halted close and drain reports channel
}
