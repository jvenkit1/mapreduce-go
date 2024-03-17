package mapreduce

import (
	"net"
	"net/rpc"

	"github.com/rs/zerolog/log"
)

// Used by a follower when registering with the leader
type RegisterArgs struct {
	followerAddress string
}

type ShutdownArgs struct {
	numTasks int
}

func (leader *Leader) SetupRPCServer() {
	rpcServer := rpc.NewServer()
	rpcServer.Register(leader)

	listener, err := net.Listen("unix", leader.address)
	if err != nil {
		log.Fatal().Err(err).Msgf("Unable to register leader node at address: %s", leader.address)
	}

	leader.listener = listener

	go func() {
		// Setting up an asynchronous loop to accept new connections.
		// Whenever a new connection is accepted, it starts a new goRoutine to serve RPC requests
		for {
			conn, err := listener.Accept()
			if err != nil {
				break
				// Check if the listener is closed
			}
			go rpcServer.ServeConn(conn)
		}
	}()
}

func (leader *Leader) ShutdownRPCServer() {
	leader.Lock()
	defer leader.Unlock()

	// close(leader.shutdown) // closes the shutdown channel owned by leader

	for _, f := range leader.followers {
		go leader.ShutdownFollowerNode(f)
	}

	if leader.listener != nil {
		leader.listener.Close()
	}

}

// Shutdown a follower node given its address
func (leader *Leader) ShutdownFollowerNode(address string) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Fatal().Err(err).Msgf("Failed to dial follower %s", address)
	}

	defer client.Close()

	args := ShutdownArgs{}

	// The follower node can update the "reply" struct value as required.
	// Kept for extensibility and can consider removing
	var reply struct{}

	err = client.Call("Follower Shutdown", args, &reply)
	if err != nil {
		log.Fatal().Err(err).Msgf("Unable to shutdown follower %s", address)
	}
}

func Call(address string, rpcName string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", address)
	if err != nil {
		log.Error().Err(err).Msgf("Unable to dial server at %v", address)
		return false
	}

	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err != nil {
		log.Error().Err(err).Msgf("Unable to make the actual rpc call for operation: %v", rpcName)
		return false
	}

	return true
}
