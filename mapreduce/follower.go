package mapreduce

import (
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type Parallelism struct {
	sync.Mutex

	currentlyExecuting int64
	maxExecuting       int64
}

type Follower struct {
	sync.Mutex

	followerName        string
	mapFunction         func(string, string) []KeyValue
	reduceFunction      func(string, []string) string
	concurrentTasks     int // number of parallel tasks being processing by the follower node
	numTasksExecuted    int
	shutDownAfterNTasks int // Limit set when shutting down. The follower node will exit after completeting "shutDownAfterNTasks" number of tasks
	listener            net.Listener
	parallelism         *Parallelism
}

// Sets up and registers a new follower node with the leader
func (follower *Follower) Register(leaderAddress string) {
	registerArgs := new(RegisterArgs)
	registerArgs.followerAddress = follower.followerName

	response := Call(leaderAddress, "Leader.Register", registerArgs, new(struct{}))

	if !response {
		log.Error().Msgf("Unable to register follower %v with the leader %v", follower.followerName, leaderAddress)
	}
}

// Shuts down a follower post activity completion
func (follower *Follower) ShutdownFollower(_ *struct{}, shutdownReply *ShutdownArgs) error {
	log.Info().Msgf("Shutting down follower node %v", follower.followerName)
	follower.Lock()
	defer follower.Unlock()

	shutdownReply.numTasks = follower.numTasksExecuted
	follower.shutDownAfterNTasks = 1
	return nil
}

func (follower *Follower) PerformTask(taskArg *Task, _ *struct{}) error {
	log.Info().Msgf("[%v] Performing task %v:%d", follower.followerName, taskArg.taskType, taskArg.currentTaskIndex)

	follower.Lock()
	follower.numTasksExecuted += 1
	follower.concurrentTasks += 1
	numConcurrentTasks := follower.concurrentTasks
	follower.Unlock()

	if numConcurrentTasks > 1 {
		log.Fatal().Msgf("Multiple tasks assigned to the node %s", follower.followerName)
	}

	pause := false

	if follower.parallelism != nil {
		follower.parallelism.Lock()

		follower.parallelism.currentlyExecuting += 1

		if follower.parallelism.currentlyExecuting > follower.parallelism.maxExecuting {
			follower.parallelism.maxExecuting = follower.parallelism.currentlyExecuting
		}

		if follower.parallelism.maxExecuting < 2 {
			pause = true
		}

		follower.parallelism.Unlock()
	}

	if pause {
		time.Sleep(time.Second)
	}

	switch taskArg.taskType {
	case MapTask:
		{
			// invoke Map Task
			log.Info().Msgf("Invoking map Task at the follower node %s", follower.followerName)
		}
	case ReduceTask:
		{
			// invoke Reduce task
			log.Info().Msgf("Invoking reduce Task at the follower node %s", follower.followerName)
		}
	}

	return nil
}

func Run(leaderAddress, followerName string,
	mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string,
	shutDownAfterNTasks int, parallelism *Parallelism) {
	log.Info().Msgf("Running follower node %s", followerName)

	follower := Follower{
		followerName:        followerName,
		mapFunction:         mapFunction,
		reduceFunction:      reduceFunction,
		shutDownAfterNTasks: shutDownAfterNTasks,
		parallelism:         parallelism,
	}

	rpcServer := rpc.NewServer()

	rpcServer.Register(follower)
	os.Remove(followerName)

	listener, err := net.Listen("unix", followerName)
	if err != nil {
		log.Error().Err(err).Msgf("Follower %s unable to setup rpc", followerName)
	}

	follower.listener = listener
	follower.Register(leaderAddress)

	for {
		follower.Lock()
		if follower.shutDownAfterNTasks == 0 {
			follower.Unlock()
			break
		}
		follower.Unlock()

		connection, err := follower.listener.Accept()
		if err != nil {
			follower.Lock()
			follower.shutDownAfterNTasks -= 1
			follower.Unlock()
			go rpcServer.ServeConn(connection)
		} else {
			break
		}
	}
	follower.listener.Close()
	log.Info().Msgf("Exiting followerNode %s", follower.followerName)
}
