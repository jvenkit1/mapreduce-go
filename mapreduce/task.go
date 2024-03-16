package mapreduce

type TaskType int
type TaskStatus int

const (
	MapTask    TaskType = iota // Constant MapTask is created of type TaskType and value iota (0)
	ReduceTask                 // Constant ReduceTask is created of type TaskType and value 1
)

const (
	Waiting TaskStatus = iota
	InProgress
	Completed
	Failed
)

type Task struct {
	taskType   TaskType
	taskStatus TaskStatus
	files      []string
	numReduce  int // Number of Reduce tasks. 1 if the task is a "map" task
}
