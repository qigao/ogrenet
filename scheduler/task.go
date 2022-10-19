package scheduler

// Task represents a generic task.
type Task interface {
	Do() error
}

// TaskFunc is a wrapper for task function.
type TaskFunc func() error

// Do is the Task interface implementation for type TaskFunc.
func (t TaskFunc) Do() error {
	return t()
}
