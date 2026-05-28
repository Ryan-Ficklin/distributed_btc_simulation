package main

import (
	"fmt"
	"github.com/Ryan-Ficklin/CSC569_lab4/shared"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	//"strings"
	"sync"
	"time"
)

const (
	MAX_NODES  = 8
	X_TIME     = 100
	Y_TIME     = 200
	Z_TIME_MAX = 100
	Z_TIME_MIN = 10
  ELECTION_MAX = 3000
  ELECTION_MIN = 1500
  LEADER_HB = 500 // intervals for leader HB
)
var (
  self_node shared.Node
  self_mutex sync.Mutex
  votesReceived int
  wg = &sync.WaitGroup{}
  lastRecvHB time.Time // last time a HB was recv from Leader
)

func main() {
	rand.Seed(time.Now().UnixNano())
	Z_TIME := rand.Intn(Z_TIME_MAX - Z_TIME_MIN) + Z_TIME_MIN

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

	fmt.Println("Node", id, "will fail after", Z_TIME, "seconds")

	currTime := calcTime()
	// Construct self
	self_node = shared.Node{
    ID: id, Hbcounter: 0, Time: currTime, Alive: true, 
    ElectionState: shared.RAFT {
      Role: shared.Follower, 
      CurrentTerm: 0, 
      IDVotedFor: 0, // 0 means no one, >0 indicates the ID for which was voted
    },
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

  // RAFT election goroutines 
  // I want these seperate from the HB protocol
  go processBallots(server, &membership)
  //go printStatus(&membership)

  // Gossip HB protocol
	time.AfterFunc(time.Millisecond*X_TIME, func() { updateHeartbeat(server, &membership, id) })
	time.AfterFunc(time.Millisecond*Y_TIME, func() { shareMembershipTables(server, neighbors, &membership, id) })

	wg.Add(1)
	wg.Wait()
}

// ~~~ RPCs for RAFT elections ~~~

func printStatus(membership **shared.Membership) {
  for {
    time.Sleep(1*time.Second)
    printMembership(**membership)
    //print("Hello!\n")
  }
}


func processBallots(server *rpc.Client, membership **shared.Membership) {
  // poll aysnchronously for as long as the client lives
  for {
    // frequent polling
    time.Sleep(50 * time.Millisecond)

    if !self_node.Alive {
      return // stop polling if dead
    }
    
    var inbox []shared.RaftMessage
    err := server.Call("Ballots.Listen", self_node.ID, &inbox)
    if err != nil || len(inbox) == 0 {
      continue // no new ballot messages, go back to polling 
    }
     
    self_mutex.Lock()
    // for everyone who sent me a message 
    for _, msg := range inbox {
      // case 0: we are outdated 
      if msg.Term > self_node.ElectionState.CurrentTerm {
        self_node.ElectionState.CurrentTerm = msg.Term
        self_node.ElectionState.IDVotedFor = 0
        self_node.ElectionState.Role = shared.Follower
      }
      
      // case 1: someone is requesting a vote from me  
      if msg.IsRequest { 
        grant := false
        // grant vote only if we have not already for this term 
        if msg.Term >= self_node.ElectionState.CurrentTerm &&
           (self_node.ElectionState.IDVotedFor == 0 || self_node.ElectionState.IDVotedFor == msg.From) {
          
          grant = true
          self_node.ElectionState.IDVotedFor = msg.From
          self_node.ElectionState.CurrentTerm = msg.Term
          fmt.Printf("FOLLOWER: I am voting for %d as leader for term %d\n", msg.From, msg.Term)
          // give candidate some time to win the election 
          // otherwise I'll grant that vote and then spin up another election
          lastRecvHB = time.Now()
        }
        
        // send our grant (or deny) vote to the requestor 
        var reply bool
        server.Call("Ballots.Add", shared.RaftMessage {
          From:        self_node.ID,
          To:          msg.From,
          Term:        self_node.ElectionState.CurrentTerm,
          IsRequest:   false, // is response
          VoteGranted: grant,
        }, &reply)
      // case 2: someone is replying to one of my vote requests 
      } else if !msg.IsRequest && 
                !msg.IsHeartbeat && 
                self_node.ElectionState.Role == shared.Candidate { 
        // increment my current votes if I am a candidate and the reply I got was current 
        // and the reply I got was a granted Vote
        if msg.Term == self_node.ElectionState.CurrentTerm && msg.VoteGranted { 
          votesReceived++
          // check if I became the leader
          if votesReceived > MAX_NODES/2 {
            fmt.Printf("LEADER: Node %d is leader for term %d\n", self_node.ID, self_node.ElectionState.CurrentTerm)
            self_node.ElectionState.Role = shared.Leader
            // update my local membership table to show I am the leader
            (**membership).Update(self_node, &self_node)
          }
        }
      }
    }
    self_mutex.Unlock()
  }
}

// follower is leaderless and starts an election
func startElection(server *rpc.Client, timeout time.Duration) {
  if time.Since(lastRecvHB) < timeout {
    return 
  }
  
  // begin my candidacy
  self_node.ElectionState.CurrentTerm++
  self_node.ElectionState.Role = shared.Candidate
  self_node.ElectionState.IDVotedFor = self_node.ID // vote for self 
  votesReceived = 1 // one vote (from myself) 
  // reset timer so one node does not spam elections
  lastRecvHB = time.Now()

  // helpful shorthand 
  term := self_node.ElectionState.CurrentTerm
  // request to all other nodes that they vote for me 
  for i := 1; i <= MAX_NODES; i++ {
    if i == self_node.ID { continue } // do not message self
    var reply bool
    server.Call("Ballots.Add", shared.RaftMessage{
      From: self_node.ID,
      To:   i,
      Term: term,
      IsRequest: true,
      IsHeartbeat: false,
    }, &reply)
  }
}


// ~~~~~~~~ GOSSIP HB PROTOCOL ~~~~~~~~~~~~

// Send the current membership table to a neighboring node with the provided ID
func sendMessage(server *rpc.Client, id int, membership shared.Membership) {
  req := shared.Request{
    ID: id,
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

  leaderId, _ := leaderAlive(**membership)
  if leaderId == -1 { 
    // each node has a random timeout between 150 - 300 ms which is the amt 
    // of time each follower has to wait until becoming candidate
    timeout := time.Duration(rand.Intn(ELECTION_MAX-ELECTION_MIN+1)+ELECTION_MIN) * time.Millisecond

    self_mutex.Lock()
    if self_node.ElectionState.Role != shared.Leader && time.Since(lastRecvHB) > timeout {
      time.AfterFunc(timeout, func () { startElection(server, timeout) })
    }
    self_mutex.Unlock()
  } else {
    lastRecvHB = time.Now()
  }

  // schedule the next gossip
  time.AfterFunc(time.Millisecond*Y_TIME, func() { shareMembershipTables(server, neighbors, membership, id) })
}

// returns leaderID (of highest term leader) if leader is alive, -1 otherwise
func leaderAlive(membership shared.Membership) (int, int) {
  // default vals
  id := -1
  term := -1

  for _, mem := range membership.Members {
    if mem.Alive && 
       mem.ElectionState.Role == shared.Leader && 
       mem.ElectionState.CurrentTerm > term {
      
      id = mem.ID
      term = mem.ElectionState.CurrentTerm
    }
  }
  return id, term
}

// only for killing nodes at random
func runAfterZ(server *rpc.Client, id int) {
  fmt.Printf("NODE %d IS NOW DEAD\n", id)
  self_node.Alive = false 
  wg.Done() 
  return
}

func printMembership(m shared.Membership){
	for _, val := range m.Members {
		status := "is Alive"
		if !val.Alive {
			status = "is Dead"
		}
		fmt.Printf("Node %d has hb %d, time %s and %s, role %d term %d leader %d\n", 
      val.ID, 
      val.Hbcounter, 
      val.Time.Format("15:04:05"),
      status,
      val.ElectionState.Role, 
      val.ElectionState.CurrentTerm,
      val.ElectionState.IDVotedFor)
	}
	fmt.Println("")
}
