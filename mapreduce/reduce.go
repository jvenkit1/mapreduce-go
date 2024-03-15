package mapreduce

// Interface defining the structure of "reduce" function
type Reducer interface {
	Reduce(key string, values []string) string
}

// struct used to implement the "reduce()" function
type reduceWorker struct {
	reducer Reducer
}

// Generate a new object of type reduceWorker
func newReduceWorker(reducer Reducer) *reduceWorker {
	return &reduceWorker{
		reducer: reducer,
	}
}

// Perfoms the reduce operation.
// The client is expected to implement their own "reduce" function for their use case
func (worker *reduceWorker) performReduce(key string, values []string) string {
	return worker.reducer.Reduce(key, values)
}
