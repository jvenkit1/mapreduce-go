package mapreduce

import (
	"net"
	"sync"

	"github.com/rs/zerolog/log"
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
	sync.Mutex                      // Mutex to protect shared access to Leader's state.
	address           string        // Address that leader listens on
	doneChannel       chan bool     // Channel to signal task completion.
	opName            string        // Currently executing job's name
	mapTasks          []string      // Filenames of the input files to be processed.
	numReduce         int           // The number of reduce tasks. Mentioned as "R" in the MapReduce paper.
	followers         []string      // Registered follower addresses.
	followerAvailable *sync.Cond    // Variable that signifies if a follower is available to take up new tasks
	shutdownChannel   chan struct{} // Channel to signal server shutdown
	listener          net.Listener  // Listener object for RPC communications
}

// Returns a new Leader node object
func (leader *Leader) newLeader(address, opName string, mapTasks []string, numReduce int) *Leader {
	return &Leader{
		address:           address,
		mapTasks:          mapTasks,
		numReduce:         numReduce,
		opName:            opName,
		followers:         make([]string, 0),
		followerAvailable: sync.NewCond(leader),
		doneChannel:       make(chan bool),
	}
}

// Register a new follower node with the leader
// RPC method that is invoked by a follower
func (leader *Leader) registerNewFollower(followerAddress string) {
	leader.Lock()
	defer leader.Unlock()

	log.Info().Msgf("Registering follower %s", followerAddress)
	leader.followers = append(leader.followers, followerAddress)

	// Broadcast to fn() that there is a new follower available
	leader.followerAvailable.Broadcast()
}

func (leader *Leader) assignTasks(ch chan string) {
	index := 0

	for {
		leader.Lock()
		if len(leader.followers) > 1 {
			followerAddr := leader.followers[index]
			go func() {
				ch <- followerAddr
			}()

			index += 1
		} else {
			// There aren't enough followers available to take up tasks. Wait until there are enough followers
			leader.followerAvailable.Wait()
		}
		leader.Unlock()
	}
}

// registeredFollowers contains all availale followers to which we can schedule "map" or "reduce" jobs
func (leader *Leader) scheduleJob(opName string, tasks []string, numReduce int, taskType TaskType, registeredFollowers chan string) {
	var numTasks int // number of parallel tasks to be run.
	var numFiles int // contains the number of intermediate files. Used when reading input during reduce phase and during writing output during map phase

	switch taskType {
	case MapTask:
		{
			numTasks = len(tasks)
			numFiles = numReduce
		}
	case ReduceTask:
		{
			numFiles = len(tasks)
			numTasks = numReduce
		}
	}

	log.Info().Msgf("Scheduling task %v for operation %v", taskType, opName)

	// Channel to store the number of tasks. Each go routine scheduling the tasks can read from this channel instead of reading from the tasks list
	tasksChannel := make(chan int, numTasks)
	for i := 0; i < numTasks; i += 1 {
		tasksChannel <- i
	}

	for len(tasksChannel) > 0 {
		switch taskType {
		case MapTask:
			{
				leader.processTask(opName, tasks, numReduce, MapTask, registeredFollowers, tasksChannel, len(tasksChannel), numFiles)
			}
		case ReduceTask:
			{
				leader.processTask(opName, tasks, len(tasksChannel), ReduceTask, registeredFollowers, tasksChannel, len(tasksChannel), numFiles)
			}
		}
	}
	log.Info().Msgf("Scheduled %v for operation %v", taskType, opName)
}

func (leader *Leader) invokeAndProcess(registeredFollowers chan string, trackerChannel chan int, tasksChannel chan int, currentTask Task, index int) {
	address := <-registeredFollowers // wait till we get a new follower available

	response := Call(address, "Follower.Perform", currentTask, nil)

	if response {
		trackerChannel <- 1
		registeredFollowers <- address // Making the current follower available again
	} else {
		trackerChannel <- 0
		tasksChannel <- index // We need to redo the current task
	}
}

func (leader *Leader) processTask(opName string, tasks []string, numReduce int, taskType TaskType, registeredFollowers chan string, tasksChannel chan int, numTasks int, numIntermediate int) {
	/*
	* This function will actually distribute the tasks to the correct follower node
	*
	 */

	trackerChannel := make(chan int) // channel tracks the completion status of a particular task

	for len(tasksChannel) > 0 {
		index := <-tasksChannel

		switch taskType {
		case MapTask:
			{
				mapTask := NewTask(opName, MapTask, tasks[index], index, numIntermediate)
				go leader.invokeAndProcess(registeredFollowers, trackerChannel, tasksChannel, mapTask, index)
			}

		case ReduceTask:
			{
				reduceTask := NewTask(opName, ReduceTask, "", index, numIntermediate)
				go leader.invokeAndProcess(registeredFollowers, trackerChannel, tasksChannel, reduceTask, index)
			}
		}
	}

	// Verify all processes have completed
	// Loop over the num of Tasks and wait for a value from the channel "trackerChannel"
	// We only finish once we receive "numTasks" confirmations
	for process := 0; process < numTasks; process += 1 {
		<-trackerChannel
	}
}

func (leader *Leader) executeMapPhase() {
	ch := make(chan string)
	go leader.assignTasks(ch)

	leader.scheduleJob(leader.opName, leader.mapTasks, leader.numReduce, MapTask, ch)
}

func (leader *Leader) executeReducePhase() {
	ch := make(chan string)
	go leader.assignTasks(ch)

	leader.scheduleJob(leader.opName, nil, leader.numReduce, ReduceTask, ch)
}

func (leader *Leader) setupLeader(address, jobName string, inputFiles []string, numReduce int) {
	leader = leader.newLeader(address, jobName, inputFiles, numReduce)

	leader.SetupRPCServer()

	go func() {
		go leader.executeMapPhase()

		go leader.executeReducePhase()
	}()

	// TODO:Setup a clean function as well
}

// func (leader *Leader) sequentialExecution(jobName string, files []string, numReduce int,
// 	mapFunction Mapper, reduceFunction Reducer) {
// 	leader = newLeader("leader", files, numReduce)
// 	mapWorker := newMapWorker(mapFunction)
// 	// reduceWorker := newReduceWorker(reduceFunction)

// 	// sequentially run all the map tasks
// 	for _, _ = range leader.mapTasks {
// 		// @TODO: FINISH THIS PORTION PROPERLY
// 		mapWorker.performMap("key", "val")
// 	}

// 	// sequentially running the reduce operations
// 	for i := 0; i < numReduce; i++ {
// 		// reduceWorker.performReduce("key", "value")
// 	}
// }
