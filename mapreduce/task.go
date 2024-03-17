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
	opName           string
	taskType         TaskType
	taskStatus       TaskStatus
	currentTaskIndex int
	file             string
	numIntermediate  int // Number of intermediate files to be generated / used
}

func NewTask(opName string, taskType TaskType, file string, currentTaskIndex, numIntermediate int) Task {
	return Task{
		opName:           opName,
		taskType:         taskType,
		taskStatus:       Waiting,
		file:             file,
		currentTaskIndex: currentTaskIndex,
		numIntermediate:  numIntermediate,
	}
}
