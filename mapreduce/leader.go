package mapreduce

import (
	"net"
	"sync"
)

/*
Leader's duties:

1. Split a task into "M" map and "R" reduce tasks
2. Schedule a task to a particular node
3. Monitor status of the task
4. Frequently poll and obtain the status of a task
	* Design an HTTP Server for this
*/

type Leader struct {
	address         string       // Address to listen on for follower registration and communication.
	mapTasks        []string     // Filenames of the input files to be processed.
	reduceTasks     int          // The number of reduce tasks. Mentioned as "R" in the MapReduce paper.
	followers       []string     // Registered follower addresses.
	followerChannel chan string  // Channel for receiving available followers.
	tasksDone       chan bool    // Channel to signal task completion.
	lock            sync.Mutex   // Mutex to protect shared access to Leader's state.
	listener        net.Listener // Listener object for RPC communications
}

// Returns a new Leader node object
func newLeader(address string, mapTasks []string, reduceTasks int) *Leader {
	return &Leader{
		address:         address,
		mapTasks:        mapTasks,
		reduceTasks:     reduceTasks,
		followers:       make([]string, 0),
		followerChannel: make(chan string),
		tasksDone:       make(chan bool),
	}
}

// Register a new follower node with the leader
func (leader *Leader) registerNewFollower(followerAddress string) {
	leader.lock.Lock()
	defer leader.lock.Unlock()

	leader.followers = append(leader.followers, followerAddress)
}

func (leader *Leader) sequentialExecution(jobName string, files []string, numReduce int,
	mapFunction Mapper, reduceFunction Reducer) {
	leader = newLeader("leader", files, numReduce)
	mapWorker := newMapWorker(mapFunction)
	// reduceWorker := newReduceWorker(reduceFunction)

	// sequentially run all the map tasks
	for _, _ = range leader.mapTasks {
		// @TODO: FINISH THIS PORTION PROPERLY
		mapWorker.performMap("key", "val")
	}

	// sequentially running the reduce operations
	for i := 0; i < numReduce; i++ {
		// reduceWorker.performReduce("key", "value")
	}
}
