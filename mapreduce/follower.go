package mapreduce

import (
	"net"
	"sync"

	"github.com/rs/zerolog/log"
)

type Parallelism struct {
	sync.Mutex

	now int64
	max int64
}

type Follower struct {
	sync.Mutex

	followerName        string
	mapFunction         func(string, string) []KeyValue
	reduceFunction      func(string, []string) string
	numTasksExecuted    int
	shutDownAfterNTasks int // Limit set when shutting down. The follower node will exit after completeting "shutDownAfterNTasks" number of tasks
	listener            net.Listener
	parallelism         *Parallelism
}

func (follower *Follower) register(leaderAddress string) {
	registerArgs := new(RegisterArgs)
	registerArgs.followerAddress = follower.followerName

	response := Call(leaderAddress, "Leader.Register", registerArgs, new(struct{}))

	if !response {
		log.Error().Msgf("Unable to register follower %v with the leader %v", follower.followerName, leaderAddress)
	}
}

func (follower *Follower) shutdownFollower(_ *struct{}, shutdownReply *ShutdownArgs) error {
	log.Info().Msgf("Shutting down follower node %v", follower.followerName)
	follower.Lock()
	defer follower.Unlock()

	shutdownReply.numTasks = follower.numTasksExecuted
	follower.shutDownAfterNTasks = 1
	return nil
}
