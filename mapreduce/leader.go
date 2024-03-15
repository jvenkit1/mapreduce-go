package mapreduce

import "sync"

/*
Each MapReduce operation must obey the following constructs:

Information stored by the leader node:

Node Identity:
1. Identity of the follower machine (for non-idle tasks)

Job Identity:
1. State of the job

MapReduce operation structure
*/

type Leader struct {
	address         string      // Address to listen on for follower registration and communication.
	mapTasks        []string    // Filenames of the input files to be processed.
	reduceTasks     int         // The number of reduce tasks. Mentioned as "R" in the MapReduce paper.
	followers       []string    // Registered follower addresses.
	followerChannel chan string // Channel for receiving available followers.
	tasksDone       chan bool   // Channel to signal task completion.
	lock            sync.Mutex  // Mutex to protect shared access to Leader's state.
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
