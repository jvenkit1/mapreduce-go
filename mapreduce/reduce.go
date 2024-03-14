package mapreduce

// Interface defining the structure of "reduce" function
type Reduce interface {
	Reduce(key string, values []string) string
}
