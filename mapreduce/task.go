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
	Type   TaskType
	Status TaskStatus
	File   string
	Index  int
}
