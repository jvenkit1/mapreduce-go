package mapreduce

// Interface defining the structure of the "map" function
type Mapper interface {
	Map(key, value string) []KeyValue
}

// Represents the "intermediate" key Value data generated by the Map function
type KeyValue struct {
	Key   string
	Value string
}

// struct is used to implement the Map() function
type mapWorker struct {
	mapper Mapper
}

// Generate a new object of type mapWorker
func newMapWorker(mapper Mapper) *mapWorker {
	return &mapWorker{
		mapper: mapper,
	}
}

// Parses the input data and invokes the map function
// Arguments input1 and input2 represent the fileName and the contents of the file
func (worker *mapWorker) performMap(input1, input2 string) []KeyValue {
	return worker.mapper.Map(input1, input2)
}
