package main

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"

	"github.com/Ryan-Ficklin/distributed_btc_simulation/shared"

	//"strings"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"sync"
	"time"
	//"encoding/hex"
)

const (
	MAX_NODES = 8
	X_TIME    = 100
	Y_TIME    = 200
	//Z_TIME_MAX   = 100
	//Z_TIME_MIN   = 10
	ELECTION_MAX = 3000
	ELECTION_MIN = 1500
	LEADER_HB    = 500 // intervals for leader HB
)

var (
	self_node     shared.Node
	self_mutex    sync.Mutex
	votesReceived int
	private_key   ecdsa.PrivateKey
	wg            = &sync.WaitGroup{}
	lastRecvHB    time.Time // last time a HB was recv from Leader
)

func main() {
	rand.Seed(time.Now().UnixNano())
	//Z_TIME := rand.Intn(Z_TIME_MAX-Z_TIME_MIN) + Z_TIME_MIN

	// Connect to RPC server
	server, _ := rpc.DialHTTP("tcp", "localhost:9005")

	args := os.Args[1:]

	// Get ID from command line argument
	if len(args) == 0 {
		fmt.Println("No args given")
		return
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("Found Error", err)
	}

	private_key, err := ecdsa.GenerateKey(elliptic.P256(), nil)
	pk := private_key.Public()
	pk_bytes, _ := x509.MarshalPKIXPublicKey(pk)
	// proper error handling

	currTime := calcTime()
	// Construct self
	self_node = shared.Node{
		ID: id, PubKey: pk_bytes, Hbcounter: 0, Time: currTime, Alive: true,
	}
	lastRecvHB = time.Now()

	var self_node_response shared.Node // Allocate space for a response to overwrite this

	// Add node with input ID
	if err := server.Call("Membership.Add", self_node, &self_node_response); err != nil {
		fmt.Println("Error:2 Membership.Add()", err)
	} else {
		fmt.Printf("Success: Node created with id= %d\n", id)
	}

	neighbors := self_node.InitializeNeighbors(id)
	fmt.Println("Neighbors:", neighbors)

	membership := shared.NewMembership()
	membership.Add(self_node, &self_node)

	sendMessage(server, neighbors[0], *membership)

	// Gossip HB protocol
	time.AfterFunc(time.Millisecond*X_TIME, func() { updateHeartbeat(server, &membership, id) })
	time.AfterFunc(time.Millisecond*Y_TIME, func() { shareMembershipTables(server, neighbors, &membership, id) })

	wg.Add(1)
	wg.Wait()
}

// ~~~~~ BobbyCoin ~~~~~

// ~~~ RPCs for RAFT elections ~~~

func printStatus(membership **shared.Membership) {
	for {
		time.Sleep(1 * time.Second)
		printMembership(**membership)
		//print("Hello!\n")
	}
}

// ~~~~~~~~ GOSSIP HB PROTOCOL ~~~~~~~~~~~~

// Send the current membership table to a neighboring node with the provided ID
func sendMessage(server *rpc.Client, id int, membership shared.Membership) {
	req := shared.Request{
		ID:    id,
		Table: membership,
	}

	var reply bool

	// send message by adding to the requests
	err := server.Call("Requests.Add", req, &reply)
	if err != nil {
		fmt.Println("Message send failed", err)
	}
}

// Read incoming messages from other nodes
func readMessages(server *rpc.Client, id int) *shared.Membership {
	incoming := shared.NewMembership()

	// receive any sent messages
	err := server.Call("Requests.Listen", id, incoming)
	if err != nil {
		fmt.Println("Request read failed", err)
	}

	return incoming
}

func calcTime() time.Time {
	return time.Now()
}

// Gossip protocol, updating my own heartbeat
func updateHeartbeat(server *rpc.Client, membership **shared.Membership, id int) {
	// stop if dead
	if !self_node.Alive {
		return
	}

	// Incremement (still alive)
	self_node.Hbcounter++
	// update time after heartbeat
	self_node.Time = calcTime()

	// local update (requires locking)
	self_mutex.Lock()
	(**membership).Update(self_node, &self_node)
	//printMembership(**membership)
	//fmt.Printf("My leader is: %d\n", self_node.ElectionState.IDVotedFor)
	self_mutex.Unlock()

	// Schedule the next HB increment for the next X duration
	time.AfterFunc(time.Millisecond*X_TIME, func() { updateHeartbeat(server, membership, id) })
}

// Gossip protocol sharing my membership table
func shareMembershipTables(server *rpc.Client, neighbors [3]int, membership **shared.Membership, id int) {
	// stop if dead
	if !self_node.Alive {
		return
	}

	self_mutex.Lock()
	// send my table to my neighbors
	//neighbor := neighbors[rand.Intn(len(neighbors))]
	for _, neighbor := range neighbors {
		sendMessage(server, neighbor, **membership)
	}
	self_mutex.Unlock()

	// look for messages sent from neighbors
	self_mutex.Lock()
	// received requests
	(*membership) = shared.CombineTables(*membership, readMessages(server, id))
	self_mutex.Unlock()

	// schedule the next gossip
	time.AfterFunc(time.Millisecond*Y_TIME, func() { shareMembershipTables(server, neighbors, membership, id) })
}

// only for killing nodes at random
func runAfterZ(server *rpc.Client, id int) {
	fmt.Printf("NODE %d IS NOW DEAD\n", id)
	self_node.Alive = false
	wg.Done()
	return
}

func printMembership(m shared.Membership) {
	for _, val := range m.Members {
		status := "is Alive"
		if !val.Alive {
			status = "is Dead"
		}
		fmt.Printf("Node %d has hb %d, time %s and %s, role %d term %d leader %d\n",
			val.ID,
			val.Hbcounter,
			val.Time.Format("15:04:05"),
			status)
	}
	fmt.Println("")
}
