package shared

import (
	"math/rand"
	"time"
  "sync"
  "fmt"
  "crypto/ecdsa"
)

const (
	MAX_NODES = 8
)

// raft role: leader, follower, candidate
type RaftRole int 
const (
  Follower  RaftRole = iota
  Candidate RaftRole = iota
  Leader    RaftRole = iota
)

// Node struct represents a computing node.
type Node struct {
  ID        int
	PubKey    ecdsa.PublicKey
	Hbcounter int
	Time      time.Time
	Alive     bool
  // parts of the node included for RAFT
  //ElectionState RAFT
}

type RAFT struct {
  Role        RaftRole
  CurrentTerm int
  IDVotedFor  int
}

// initialize 3 neighbors to reduce cases wherein nodes get stuck in a 
// cycle without communication to the rest of the system 
func (n Node) InitializeNeighbors(id int) [3]int {
	neighbor1 := RandInt()
	for neighbor1 == id {
		neighbor1 = RandInt()
	}
	neighbor2 := RandInt()
	for neighbor1 == neighbor2 || neighbor2 == id {
		neighbor2 = RandInt()
	}
	neighbor3 := RandInt()
  // can't be the same as id, n1, or n2
	for neighbor1 == neighbor3 || neighbor2 == neighbor3 || neighbor3 == id {
		neighbor3 = RandInt()
	}
	return [3]int{neighbor1, neighbor2, neighbor3}
}

func RandInt() int {
	//rand.Seed(time.Now().UnixNano())
	return rand.Intn(MAX_NODES-1+1) + 1
}

/*---------------*/

// Membership struct represents participanting nodes
type Membership struct {
	mutex sync.RWMutex
  Members map[int]Node
}

// Returns a new instance of a Membership (pointer).
func NewMembership() *Membership {
	return &Membership{
		Members: make(map[int]Node),
	}
}

// Adds a node to the membership list.
func (m *Membership) Add(payload Node, reply *Node) error {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  
  m.Members[payload.ID] = payload
  return nil
}

// Updates a node in the membership list.
func (m *Membership) Update(payload Node, reply *Node) error {
  m.mutex.Lock()
  defer m.mutex.Unlock()
 
 m.Members[payload.ID] = payload
 return nil
}

// Returns a node with specific ID.
func (m *Membership) Get(payload int, reply *Node) error {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  
  *reply = m.Members[payload]
  return nil
}

/*---------------*/

// Request struct represents a new message request to a client
type Request struct {
	ID    int
	Table Membership
}

// Requests struct represents pending message requests
type Requests struct {
	mutex sync.RWMutex
  Pending map[int][]Membership
}

// Returns a new instance of a Reques.
func NewRequests() *Requests {
  return &Requests{
    Pending: make(map[int][]Membership),
  }
}

// private membership cloning function 
// to update and pass around Membership structs without mutex duplication
func cloneMembership(mem Membership) Membership {
  // make the clone 
  clone := Membership {
    Members : make(map[int]Node, len(mem.Members)),
  }

  // populate the clone nodes
  for id, node := range mem.Members {
    clone.Members[id] = node
  }
  
  return clone
}

// Adds a new message request to the pending list
func (req *Requests) Add(payload Request, reply *bool) error {
  req.mutex.Lock()
  defer req.mutex.Unlock()
 
  // get the payload's message
  msg := cloneMembership(payload.Table)

  // add to the list of pending requests (I previously overwrote with each new request)
  // this should serve more as an actual "pending" list now 
  req.Pending[payload.ID] = append(req.Pending[payload.ID], msg)

  *reply = true
  return nil
}

// Listens to communication from neighboring nodes.
func (req *Requests) Listen(ID int, reply *Membership) error {
  req.mutex.Lock()
  
  // get all the pending membership table requests
  // copy instead of just point to the struct, because we are going to delete it
  inbox := append([]Membership(nil), req.Pending[ID]...)
  // clear inbox 
  delete(req.Pending, ID)

  req.mutex.Unlock()
  
  reply.Members = make(map[int]Node)

  // for each member that has sent us a message
  for _, table := range inbox {
    // for each node in the current table's membership list
    for id, node := range table.Members {
      existing, exists := reply.Members[id]

      // combine membership tables of current messages 
      // to reflect the most up to date state of the whole system
      // But I don't want the failure detection logic to happen 
      // prior to the node receiving it's messages, so Im not calling CombineTables
      if !exists || // not present
         node.Hbcounter > existing.Hbcounter ||  // existing hb is out of date
         (node.Hbcounter == existing.Hbcounter && node.Time.After(existing.Time)) { // hb is same but more recent
        reply.Members[id] = node
      }
    }
  }


  return nil
}

func CombineTables(table1 *Membership, table2 *Membership) *Membership {
  combined := NewMembership()
  
  // lock these tables s.t. we can read from them without updates occuring
  // mid-read
  table1.mutex.Lock()
  defer table1.mutex.Unlock()
  table2.mutex.Lock()
  defer table2.mutex.Unlock()
  
  // add all of table 1 to the combined list
  for id, member := range table1.Members {
    combined.Members[id] = member
  }

  // iterate through table2 and add or update members only when table2 is 
  // more recent
  timeout := 1 * time.Second
  for id, member := range table2.Members {
    maybeMember, exists := combined.Members[id]
    // add/update with table2's member if there isnt a corresponding node in
    // table1 
    if !exists {
      member.Time = time.Now()
      combined.Members[id] = member
    //if table2 has a larger heartbeat
    } else if member.Hbcounter > maybeMember.Hbcounter {
      // we have seen the hb increase, so we can set the node as alive again
      // i.e. maybe it came back to life
      member.Alive = true
      member.Time = time.Now()
      combined.Members[id] = member
    // finding dead nodes (if node is not already dead):
    // 1) member exists in both
    // 2) Heartbeat has not been updated
    // 3) time since the member has been updated in both tables is longer than 
    //    some tuned arbitrary timeout (I chose 3 seconds after some testing)
    } else if exists && 
              maybeMember.Alive && 
              member.Hbcounter == maybeMember.Hbcounter && 
              (time.Since(maybeMember.Time) > timeout && time.Since(member.Time) > timeout) {
              //(member.Time.Sub(maybeMember.Time).Abs() > 3*time.Second) { 
      member.Alive = false
      combined.Members[id] = member
      fmt.Printf("Member %d has been found dead\n", member.ID)
    } 
  }
  
  return combined
}

/* ~~~~~~~~~~~~~ */

type RaftMessage struct {
  From        int
  To          int
  Term        int
  IsHeartbeat bool // True for HeartBeat
  IsRequest   bool // True for Request Vote, False for reply (grant/deny)
  VoteGranted bool // only matters when IsHeartbeat/IsRequest is False
}

type Ballots struct {
  mutex sync.RWMutex
  BallotBox map[int][]RaftMessage
}

func NewBallots() *Ballots {
  return &Ballots{
      BallotBox: make(map[int][]RaftMessage),
  }
}

// add a RaftMessage to our BallotBox
func (bal *Ballots) Add(payload RaftMessage, reply *bool) error {
  bal.mutex.Lock()
  defer bal.mutex.Unlock()

  bal.BallotBox[payload.To] = append(bal.BallotBox[payload.To], payload)
  *reply = true
  return nil
}

// return all the RaftMessages in the given node ID's ballotBox 
func (bal *Ballots) Listen(ID int, reply *[]RaftMessage) error {
  bal.mutex.Lock()
  defer bal.mutex.Unlock()

  *reply = append([]RaftMessage(nil), bal.BallotBox[ID]...)
  delete(bal.BallotBox, ID) // clear inbox after reading 
  return nil
}

/* ~~~~~~~~~~~ */ 
 
