package main

import (
	"bytes"
	"fmt"

	"math/rand"
	"net/rpc"
	"os"
	"strconv"

	"github.com/Ryan-Ficklin/distributed_btc_simulation/shared"

	//"strings"
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
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
	DIFFICULTY   = 0x0000007FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
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

// provide a user interface to give instructions for this computing node
func user() {

}

// given a recipient public key and value
// find transactions on my block chain where my PK gets enough coins
// create new TX struct giving value to recipient and sign it with my PK
// add to my list of utx
func create_TX(recipient []byte, value int) {
	// find coins from which to give
	block_id, out_idx := collect_coins(value)
	block := find_block(block_id)

	if block_id == nil {
		return
	}

	// give our coins to our recipient
	output := []shared.TX_Output{
		shared.TX_Output{
			Value:  value,
			PubKey: recipient,
		}}

	// give left over coins back to myself
	if block.TX.Output[out_idx].Value > value {
		output = append(output, shared.TX_Output{
			Value:  block.TX.Output[out_idx].Value - value,
			PubKey: self_node.PubKey,
		})
	}

	input := shared.TX_Input{
		Block_ID: block_id,
		N:        out_idx,
	}

	// sign this input
	in_encoding, _ := json.Marshal(input)
	out_encoding, _ := json.Marshal(output)
	encoding := append(in_encoding, out_encoding...)
	hash := sha256.Sum256(encoding)
	sig, _ := ecdsa.SignASN1(nil, &private_key, hash[:])

	new_tx := shared.Transaction{
		Signature: sig,
		Input:     input,
		Output:    output,
	}

	self_mutex.Lock()
	// add my new transaction to my list of unverified transactions
	self_node.UTX = append(self_node.UTX, new_tx)
	self_mutex.Unlock()

}

// given a value, find which blocks on my blockchain have that or more coins
// return block id and output index of block
func collect_coins(value int) ([]byte, int) {
	var block_id []byte = nil
	var out_n int = -1

	// loop through all blocks
	for index, block := range self_node.Blockchain {
		// find tx of mine with >= value
		for out_index, out := range block.TX.Output {
			// found one
			if out.Value >= value && bytes.Equal(out.PubKey, self_node.PubKey) {
				// set the returns for now
				block_id = block.Block_ID
				out_n = out_index

				// check for double spend
				for j := index + 1; j < len(self_node.Blockchain); j++ {
					// this out index of this blockchain is stale
					if self_node.Blockchain[j].TX.Input.N == out_n ||
						bytes.Equal(block_id, self_node.Blockchain[j].Block_ID) {
						// reset values -- not valid bc stale
						block_id = nil
						out_n = -1
					}
				}
				// this transation was not double spent
				if block_id != nil {
					return block_id, out_n
				}
			}
		}
	}
	return block_id, out_n
}

// given a block id, return the associated block from the block chain
func find_block(id []byte) shared.Block {
	for _, block := range self_node.Blockchain { // maybe replace _ with index?
		if bytes.Equal(block.Block_ID, id) {
			return block
		}
	}
	return shared.Block{} // not sure if this is what we want...? but couldn't just return nil
}

// TODO
func validate_transaction(tx shared.Transaction) {

}

// TODO
func validate_block(block shared.Block) {

}

// TODO
func mine() {

}

// given a transaction and a previous block ID, compute the proof of work
// necessary to mint this block
func compute_pow(tx shared.Transaction, prev_id []byte, difficulty uint64) ([]byte, []byte) {
	// make space for our nonce
	nonce := make([]byte, 32)

	for {
		// stop if you are trying to mine on an old block
		if !bytes.Equal(self_node.Blockchain[len(self_node.Blockchain)-1].Block_ID, prev_id) {
			return nil, nil
		}
		// get a random 32 byte nonce value
		crand.Read(nonce)
		// compute sha256 to check for leading 0s
		encoding, _ := json.Marshal(tx)
		encoding = append(encoding, prev_id...)
		encoding = append(encoding, nonce...)
		hash := sha256.Sum256(encoding)

		// check that the integer representation of our hash has the proper amt
		// of leading 0s
		attempt := binary.BigEndian.Uint64(hash[:])
		if attempt < difficulty {
			return hash[:], nonce
		}
	}
}

// helper for continuously printing status
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

	// TODO
	// look for longest block chain of neighbors, add/subtract any new/spent transactions

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
