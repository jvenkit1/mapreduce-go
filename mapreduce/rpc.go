package mapreduce

import (
	"log"
	"net"
	"net/rpc"
)

type ShutdownArgs struct {
}

func (leader *Leader) setupRPCServer() {
	rpcServer := rpc.NewServer()
	rpcServer.Register(leader)

	listener, err := net.Listen("unix", leader.address)
	if err != nil {
		log.Fatalf("Unable to register leader node at address: %s, error=%s", leader.address, err)
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

func (leader *Leader) shutdownRPCServer() {
	leader.lock.Lock()
	defer leader.lock.Unlock()

	// close(leader.shutdown) // closes the shutdown channel owned by leader

	for _, f := range leader.followers {
		go leader.shutdownFollowerNode(f)
	}

	if leader.listener != nil {
		leader.listener.Close()
	}

}

// Shutdown a follower node given its address
func (leader *Leader) shutdownFollowerNode(address string) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Fatalf("Failed to dial follower %s with error %s", address, err)
	}

	defer client.Close()

	args := ShutdownArgs{}

	// The follower node can update the "reply" struct value as required.
	// Kept for extensibility and can consider removing
	var reply struct{}

	err = client.Call("Follower Shutdown", args, &reply)
	if err != nil {
		log.Fatalf("Unable to shutdown follower %s with error %s", address, err)
	}
}
