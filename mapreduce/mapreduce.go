package mapreduce

// Struct defines the Job config and the metadata stored by the leader node
type JobConfig struct {
	Mapper     Mapper
	Reducer    Reduce
	InputPath  string
	OutputPath string
}

type MapReduce interface {
	RunJob(config JobConfig) error
}
