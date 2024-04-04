package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"runtime"
	"sync"
	"time"
)

// TaskStatus represents the status of the current task in the processing pipeline
type TaskStatus int

const (
	// TaskStatusQueued means this task is yet to be processed
	TaskStatusQueued TaskStatus = iota

	// TaskStatusExecuting - the task is currently executing
	TaskStatusExecuting

	// TaskStatusSuccessful - the task has been successfully processed
	TaskStatusSuccessful

	// TaskStatusFailed - the task processing failed for some reason
	TaskStatusFailed
)

// A Task in the task-processing pipeline
type Task struct {
	// The id of the task
	id int

	// createdAt, when this task was created
	createdAt time.Time

	// executedAt, when this task started executing
	executedAt time.Time

	// sourceTimestamp, some piece of information (probably from an external source)
	// Currently, this is just a timestamp
	sourceTimestamp string

	// A message for this task, indicating successful execution or an error description
	message string

	// The status of the task in the processing pipeline
	status TaskStatus
}

// newTask creates a new Task with the specified id
func newTask(id int) Task {
	createdAt := time.Now()

	return Task{
		id:              id,
		createdAt:       createdAt,
		sourceTimestamp: createdAt.Format(time.RFC3339),
	}
}

// isQueued returns whether the task is queued
func (task *Task) isQueued() bool {
	return task.status == TaskStatusQueued
}

// isExecuting returns whether the task is already executing
func (task *Task) isExecuting() bool {
	return task.status == TaskStatusExecuting
}

// setStatusExecuting, updates the task status and executedAt time
func (task *Task) setStatusExecuting() {
	task.status = TaskStatusExecuting
	task.executedAt = time.Now()
}

// isSuccessful returns whether the task was successfully executed
func (task *Task) isSuccessful() bool {
	return task.status == TaskStatusSuccessful
}

// setStatusSuccessful, updates the task status
func (task *Task) setStatusSuccessful() {
	task.status = TaskStatusSuccessful
}

// isFailed returns whether the task has failed to process
func (task *Task) isFailed() bool {
	return task.status == TaskStatusFailed
}

// setStatusFailed, updates the task status
func (task *Task) setStatusFailed() {
	task.status = TaskStatusFailed
}

// setMessage gives this task the specified message
func (task *Task) setMessage(message string) {
	task.message = message
}

// TaskProducer generates tasks and sends them to a channel for further processing
type TaskProducer struct {
	// taskChan is where the tasks will be sent
	taskChan chan<- Task
}

// newTaskProducer, creates a TaskProducer with the given output channel
func newTaskProducer(taskChan chan<- Task) TaskProducer {
	return TaskProducer{
		taskChan: taskChan,
	}
}

// execute starts generating Task objects.
// This method also waits on the given context, for finalization
func (taskProducer *TaskProducer) execute(ctx context.Context) {
	log.Info("TaskProducer thread started")

outer:
	for {
		select {
		// if context is cancelled, return
		case <-ctx.Done():
			break outer

		default:
			// create task
			taskId := int(time.Now().Unix())
			task := newTask(taskId)

			// Decide whether this task is doomed to fail...
			randomInt := time.Now().Nanosecond()
			if runtime.GOOS == `windows` {
				// We have to divide by 100 here because Windows doesn't provide a clock with nanosecond-precision.
				randomInt /= 100
			}
			if randomInt%2 > 0 {
				task.sourceTimestamp = "Fucked Up Beyond All Recognition"
			}

			// send to channel
			taskProducer.taskChan <- task
		}
	}

	log.Info("TaskProducer thread done")

	// Close the channel to indicate there will be no more new tasks
	close(taskProducer.taskChan)
}

const (
	// TaskSourceTimeThreshold denotes the allowed "freshness" of tasks when processing
	TaskSourceTimeThreshold time.Duration = 20 * time.Second
)

// A TaskProcessor reads tasks from its input channel, processes them, and writes them to its output channel
type TaskProcessor struct {
	// Read tasks from inputChan
	inputChan <-chan Task

	// Write tasks to outputChan
	outputChan chan<- Task
}

// newTaskProcessor creates a TaskProcessor with the given input and output channels
func newTaskProcessor(
	inputChan <-chan Task,
	outputChan chan<- Task,
) TaskProcessor {
	return TaskProcessor{
		inputChan:  inputChan,
		outputChan: outputChan,
	}
}

func (taskProcessor *TaskProcessor) execute() {
	log.Info("TaskProcessor thread started")

outer:
	for {
		select {
		case task, ok := <-taskProcessor.inputChan:
			if !ok {
				// If there are no more tasks to process, terminate
				break outer
			}

			if !task.isQueued() {
				// Ignore tasks which are not in the "queued" status
				taskProcessor.outputChan <- task
				continue
			}

			// flag the task as "executing"
			task.setStatusExecuting()

			// Attempt to parse the Task's sourceTimestamp
			sourceTime, err := time.Parse(time.RFC3339, task.sourceTimestamp)
			if err != nil {
				// mark the Task as failed, and save the error message
				task.setStatusFailed()
				task.setMessage(err.Error())
				taskProcessor.outputChan <- task
				continue
			}

			// Check if the task is "fresh" enough for processing
			if sourceTime.After(time.Now().Add(-TaskSourceTimeThreshold)) {
				// simulate some "heavy" task processing...
				time.Sleep(150 * time.Millisecond)

				// mark the Task as successful, and save a nice message
				task.setStatusSuccessful()
				task.setMessage("task succeeded :)")
				taskProcessor.outputChan <- task
				continue
			} else {
				// mark the Task as failed, and save a reason message
				task.setStatusFailed()
				task.setMessage("task too old :(")
				taskProcessor.outputChan <- task
				continue
			}
		}
	}

	log.Info("TaskProcessor thread done")

	// Close the output channel to signify that there will be no more tasks
	close(taskProcessor.outputChan)
}

// TaskFilter sends successful tasks to its output channel, and failed tasks' messages to its error channel.
type TaskFilter struct {
	// read tasks from inputChan
	inputChan <-chan Task

	// write successful tasks to outputChan
	outputChan chan<- Task

	// write error messages of failed tasks to errorChan
	errorChan chan<- error
}

// newTaskFilter creates a TaskFilter with the given input, output and error channels
func newTaskFilter(
	inputChan <-chan Task,
	outputChan chan<- Task,
	errorChan chan<- error,
) TaskFilter {
	return TaskFilter{
		inputChan:  inputChan,
		outputChan: outputChan,
		errorChan:  errorChan,
	}
}

// execute runs the TaskFilter
func (taskFilter *TaskFilter) execute() {
	log.Info("TaskFilter thread started")

outer:
	for {
		select {
		case task, ok := <-taskFilter.inputChan:
			if !ok {
				// if there are no more tasks, terminate
				break outer
			}

			// If the task is successful, write it to the output channel
			if task.isSuccessful() {
				taskFilter.outputChan <- task
				continue
			}

			// If the task is failed, write a message to the error channel
			if task.isFailed() {
				err := fmt.Errorf(
					"task id %d, time %q, error %q",
					task.id,
					task.sourceTimestamp,
					task.message,
				)
				taskFilter.errorChan <- err
				continue
			}

			// Ignore tasks with other statuses
			continue
		}
	}

	log.Info("TaskFilter thread done")

	// Close the channels to denote there will be no more tasks or errors sent
	close(taskFilter.outputChan)
	close(taskFilter.errorChan)
}

// TaskStorage accumulates successful tasks and errors from its respective input channels
type TaskStorage struct {
	taskChan  <-chan Task
	errorChan <-chan error
	tasks     map[int]Task
	errors    []error
}

// newTaskStorage creates a TaskStorage with the given task and error channels
func newTaskStorage(taskChan <-chan Task, errorChan <-chan error) TaskStorage {
	return TaskStorage{
		taskChan:  taskChan,
		errorChan: errorChan,
		tasks:     map[int]Task{},
		errors:    []error{},
	}
}

// addTask saves the given task
func (taskStorage *TaskStorage) addTask(task Task) {
	taskStorage.tasks[task.id] = task
}

// addError saves the given error
func (taskStorage *TaskStorage) addError(err error) {
	taskStorage.errors = append(taskStorage.errors, err)
}

// execute starts the TaskStorage accumulation process
func (taskStorage *TaskStorage) execute() {
	log.Info("TaskStorage thread started")

	// some flags denoting channel statuses
	taskChanOpen := true
	errorChanOpen := true

outer:
	for {
		// If both channels are closed, terminate
		if !taskChanOpen && !errorChanOpen {
			break outer
		}

		select {
		case task, ok := <-taskStorage.taskChan:
			taskChanOpen = ok
			if ok {
				// If we have read a task, save it
				taskStorage.addTask(task)
			}

		case err, ok := <-taskStorage.errorChan:
			errorChanOpen = ok
			if ok {
				// If we have read an error, save it
				taskStorage.addError(err)
			}
		}
	}

	log.Info("TaskStorage thread done")
}

func init() {
	// Configure logging
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	log.Info("Starting task executor...")

	// create context for the TaskProducer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create wait group
	wg := sync.WaitGroup{}

	// create a Task queue for processing (with a maximum queue length of 10 tasks)
	taskChan := make(chan Task, 10)

	// Spawn the TaskProducer
	wg.Add(1)
	go func() {
		defer wg.Done()

		taskProducer := newTaskProducer(taskChan)
		taskProducer.execute(ctx)
	}()

	// create a queue with already processed Tasks
	processedTaskChan := make(chan Task, 10)

	// Spawn the TaskProcessor
	wg.Add(1)
	go func() {
		defer wg.Done()

		taskProcessor := newTaskProcessor(taskChan, processedTaskChan)
		taskProcessor.execute()
	}()

	// Create channels for successful tasks and errors of failed tasks
	successfulTaskChan := make(chan Task)
	errorChan := make(chan error)

	// Spawn the TaskFilter
	wg.Add(1)
	go func() {
		defer wg.Done()

		taskFilter := newTaskFilter(processedTaskChan, successfulTaskChan, errorChan)
		taskFilter.execute()
	}()

	// Spawn the TaskStorage
	wg.Add(1)
	go func() {
		defer wg.Done()

		taskStorage := newTaskStorage(successfulTaskChan, errorChan)
		taskStorage.execute()

		// Print the accumulated data, after its all been processed
		log.Debug("Printing task execution summary...")

		for _, err := range taskStorage.errors {
			log.Error(err.Error())
		}

		for _, task := range taskStorage.tasks {
			log.Infof("Task id %d completed", task.id)
		}
	}()

	// Spawn a timer which will cancel the TaskProducer after a timeout
	wg.Add(1)
	go func() {
		defer wg.Done()

		// cancel context after 3 seconds
		log.Debug("Sleep started")
		time.Sleep(3 * time.Second)
		log.Debug("Sleep done")

		// Stop the TaskProducer
		cancel()
		log.Debug("Cancelled context")
	}()

	// wait for all remaining goroutines to finish
	log.Debug("Main thread waiting started")
	wg.Wait()
	log.Debug("Main thread waiting done")

	log.Info("Exiting")
}
