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

type TaskStatus int

const (
	TaskStatusQueued TaskStatus = iota
	TaskStatusExecuting
	TaskStatusSuccessful
	TaskStatusFailed
)

type Task struct {
	id              int
	createdAt       time.Time
	executedAt      time.Time
	sourceTimestamp string
	message         string
	status          TaskStatus
}

func newTask(id int) Task {
	createdAt := time.Now()

	return Task{
		id:              id,
		createdAt:       createdAt,
		sourceTimestamp: createdAt.Format(time.RFC3339),
	}
}

func (task *Task) isQueued() bool {
	return task.status == TaskStatusQueued
}

func (task *Task) isExecuting() bool {
	return task.status == TaskStatusExecuting
}

func (task *Task) setStatusExecuting() {
	task.status = TaskStatusExecuting
	task.executedAt = time.Now()
}

func (task *Task) isSuccessful() bool {
	return task.status == TaskStatusSuccessful
}

func (task *Task) setStatusSuccessful() {
	task.status = TaskStatusSuccessful
}

func (task *Task) isFailed() bool {
	return task.status == TaskStatusFailed
}

func (task *Task) setStatusFailed() {
	task.status = TaskStatusFailed
}

func (task *Task) setMessage(message string) {
	task.message = message
}

type TaskProducer struct {
	taskChan chan<- Task
}

func newTaskProducer(taskChan chan<- Task) TaskProducer {
	return TaskProducer{
		taskChan: taskChan,
	}
}

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
	close(taskProducer.taskChan)
}

const (
	TaskSourceTimeThreshold time.Duration = 20 * time.Second
)

type TaskProcessor struct {
	inputChan  <-chan Task
	outputChan chan<- Task
}

func newTaskProcessor(
	inputChan <-chan Task,
	outputChan chan<- Task,
) TaskProcessor {
	return TaskProcessor{
		inputChan:  inputChan,
		outputChan: outputChan,
	}
}

func (taskProcessor *TaskProcessor) execute(ctx context.Context) {
	log.Info("TaskProcessor thread started")

outer:
	for {
		select {
		// if context is cancelled, return
		case <-ctx.Done():
			break outer

		case task, ok := <-taskProcessor.inputChan:
			if !ok {
				break outer
			}

			if !task.isQueued() {
				taskProcessor.outputChan <- task
				continue
			}

			task.setStatusExecuting()

			sourceTime, err := time.Parse(time.RFC3339, task.sourceTimestamp)
			if err != nil {
				task.setStatusFailed()
				task.setMessage(err.Error())
				taskProcessor.outputChan <- task
				continue
			}

			if sourceTime.After(time.Now().Add(-TaskSourceTimeThreshold)) {
				// simulate some "heavy" task processing...
				time.Sleep(150 * time.Millisecond)

				task.setStatusSuccessful()
				task.setMessage("task succeeded :)")
				taskProcessor.outputChan <- task
				continue
			} else {
				task.setStatusFailed()
				task.setMessage("task too old :(")
				taskProcessor.outputChan <- task
				continue
			}
		}
	}

	log.Info("TaskProcessor thread done")
	close(taskProcessor.outputChan)
}

type TaskFilter struct {
	inputChan  <-chan Task
	outputChan chan<- Task
	errorChan  chan<- error
}

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

func (taskFilter *TaskFilter) execute(ctx context.Context) {
	log.Info("TaskFilter thread started")

outer:
	for {
		select {
		// if context is cancelled, return
		case <-ctx.Done():
			break outer

		case task, ok := <-taskFilter.inputChan:
			if !ok {
				break outer
			}

			if task.isSuccessful() {
				taskFilter.outputChan <- task
				continue
			}

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
	close(taskFilter.outputChan)
	close(taskFilter.errorChan)
}

type TaskStorage struct {
	taskChan  <-chan Task
	errorChan <-chan error
	tasks     map[int]Task
	errors    []error
}

func newTaskStorage(taskChan <-chan Task, errorChan <-chan error) TaskStorage {
	return TaskStorage{
		taskChan:  taskChan,
		errorChan: errorChan,
		tasks:     map[int]Task{},
		errors:    []error{},
	}
}

func (taskStorage *TaskStorage) addTask(task Task) {
	taskStorage.tasks[task.id] = task
}

func (taskStorage *TaskStorage) addError(err error) {
	taskStorage.errors = append(taskStorage.errors, err)
}

func (taskStorage *TaskStorage) execute(ctx context.Context) {
	log.Info("TaskStorage thread started")

	taskChanOpen := true
	errorChanOpen := true

outer:
	for {
		if !taskChanOpen && !errorChanOpen {
			break outer
		}

		select {
		// if context is cancelled, return
		case <-ctx.Done():
			break outer

		case task, ok := <-taskStorage.taskChan:
			taskChanOpen = ok
			if ok {
				taskStorage.addTask(task)
			}

		case err, ok := <-taskStorage.errorChan:
			errorChanOpen = ok
			if ok {
				taskStorage.addError(err)
			}
		}
	}

	log.Info("TaskStorage thread done")
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	log.Info("Starting task executor...")

	// create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create wait group
	wg := sync.WaitGroup{}

	taskChan := make(chan Task, 10)

	wg.Add(1)
	go func() {
		defer wg.Done()

		taskProducer := newTaskProducer(taskChan)
		taskProducer.execute(ctx)
	}()

	processedTaskChan := make(chan Task, 10)

	wg.Add(1)
	go func() {
		defer wg.Done()

		taskProcessor := newTaskProcessor(taskChan, processedTaskChan)
		taskProcessor.execute(ctx)
	}()

	successfulTaskChan := make(chan Task)
	errorChan := make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()

		taskFilter := newTaskFilter(processedTaskChan, successfulTaskChan, errorChan)
		taskFilter.execute(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		taskStorage := newTaskStorage(successfulTaskChan, errorChan)
		taskStorage.execute(ctx)

		log.Debug("Printing task execution summary...")

		for _, err := range taskStorage.errors {
			log.Error(err.Error())
		}

		for _, task := range taskStorage.tasks {
			log.Infof("Task id %d completed", task.id)
		}
	}()

	// cancel context after 3 seconds
	log.Debug("Main thread sleep started")
	time.Sleep(3 * time.Second)
	log.Debug("Main thread sleep done")

	cancel()
	log.Debug("Main thread cancelled context")

	// wait for goroutines to finish
	log.Debug("Main thread waiting started")
	wg.Wait()
	log.Debug("Main thread waiting done")

	log.Info("Exiting")
}
