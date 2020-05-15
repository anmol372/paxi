package avalanche

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

//send to k nodes k = 60 % of c.N()
// alpha = 0.6
//global variables
var slotID float64
var sampleSize int
var totalNodes int
var consensus int
var alpha int
var expRep int

// entry in log
type entry struct {
	//ballot  paxi.Ballot
	command     paxi.Command
	commit      bool
	request     *paxi.Request
	timestamp   time.Time
	query       uint64 //paxi.Value
	result      int
	recvReplies int
	FromClient  bool
}

// Avalanche instance
type Avalanche struct {
	paxi.Node

	config []paxi.ID
	N      paxi.Config
	log    map[float64]*entry // log ordered by slot
	//logR     map[int]*RequestSlot // log ordered by slot for receiving requests
	//ballot   paxi.Ballot // highest ballot number
	requests []*paxi.Request
	slot     float64 // highest slot number
	execute  float64
	//quorum   		*paxi.Quorum    // phase 1 quorum
	Color int //nodes current color
}

// NewAvalanche creates new avalanche instance
func NewAvalanche(n paxi.Node, options ...func(*Avalanche)) *Avalanche {
	totalNodes = len(paxi.GetConfig().Addrs)
	alpha = 60      //60%
	sampleSize = 60 //60%
	expRep = int(math.Ceil(float64(totalNodes*sampleSize) / (100)))
	//provide random seed
	rand.Seed(time.Now().UnixNano())
	//0-red, 1-blue, 2-undeclared
	//Intn(n) -> [0,n)
	randomColor := rand.Intn(3)
	p := &Avalanche{
		Node:     n,
		log:      make(map[float64]*entry, paxi.GetConfig().BufferSize),
		slot:     -1.0,
		execute:  0.0,
		requests: make([]*paxi.Request, 0),
		Color:    randomColor,
	}
	slotID, _ = strconv.ParseFloat(string(p.ID()), 8)
	fmt.Printf("\ncolor %v\ntotalNodes %v\n", p.Color, totalNodes)
	p.slot += slotID
	for _, opt := range options {
		opt(p)
	}
	return p
}

// HandleRequest handles request and start query
//Query()
func (a *Avalanche) HandleRequest(r paxi.Request) {
	log.Debugf("\n<---R----HandleRequest----R------>\n")
	log.Debugf("Sender ID %v\n", r.NodeID)
	fmt.Printf("in HandleRequest with %v\n", r)
	//request can come to any server and must be sent
	//p.logR[s].active = true
	q, _ := binary.Uvarint(r.Command.Value)
	fmt.Printf("Query for: %v\n", int(q))
	a.slot = a.slot + 1
	a.log[a.slot] = &entry{
		command:     r.Command,
		request:     &r,
		timestamp:   time.Now(),
		query:       q, //r.Command.Value
		result:      0,
		recvReplies: 0,
		FromClient:  true,
	}
	fmt.Printf("Entry to log -> Key: %v, Value: %v\n", a.slot, a.log[a.slot])
	//if color '0' add 1 else if '1' sub 1
	/*if a.color == 0 {
		a.log[a.slot].result++
	} else if a.color == 1 {
		a.log[a.slot].result--
	}*/
	if a.Color == 2 {
		a.Color = int(q)
	}
	//add iteration no parameter, add for loop
	//for loop array of timers(if consensus, ok else resend for iteration)
	//big timer, for (m+1)*small_timer, for slush give last result,
	m := Query{
		ID:         a.ID(),
		Color:      q,
		Sender:     a.ID(),
		MID:        a.slot, //message ID
		FromClient: true,
	}
	fmt.Printf("now multicasting randomly: %v\n", m)
	a.MulticastRandom(sampleSize, m)
}

// HandleQuery recieved by other servers
// handle query with for...
//Slush Loop
func (a *Avalanche) HandleQuery(m Query) {
	fmt.Printf("in slush loop %v , mid: %v\n", m, m.MID)
	color := m.Color
	//if a.color == 0 || a.color == 1 {
	if a.Color == 0 || a.Color == 1 {
		color = uint64(a.Color)
	}
	r := Reply{
		Sender:   a.ID(),
		Color:    color,
		MID:      m.MID,
		Reciever: m.ID,
	}
	fmt.Printf("Query Reply %v\n", r)
	a.Send(r.Reciever, r)
	//} else
	if a.Color == 2 {
		a.Color = int(m.Color)
		//create entry in local log and send to sampleSize random nodes
		//log entry
		a.log[m.MID] = &entry{
			timestamp:   time.Now(),
			query:       m.Color,
			result:      0,
			recvReplies: 0,
			FromClient:  false,
		}
		//message query
		mq := Query{
			ID:         m.ID,
			Color:      m.Color,
			Sender:     a.ID(),
			MID:        m.MID, //message ID
			FromClient: false,
		}
		a.MulticastRandom(sampleSize, mq)
	}
}

//HandleReply reply from queries
//Slush loop Reply processing
func (a *Avalanche) HandleReply(m Reply) {
	fmt.Printf("recv reply for mid %v", m.MID)
	var s string
	//recieve reply
	rColor := m.Color
	rSlot := m.MID
	rQuery := 0
	if rColor == 0 {
		rQuery = 1
	} else if rColor == 1 {
		rQuery = -1
	}
	/*entry, error := a.log[rSlot]
	if !error {
		fmt.Printf("entry doesn't exist\n")
	} else {
		fmt.Printf("entry: %v\nquery: %v\n", entry, rQuery)
	}*/
	//->update log appropriately
	a.log[rSlot].result += rQuery
	a.log[rSlot].recvReplies++
	//->take actions based on log
	//->if consensus is reached reply to client
	if a.log[rSlot].recvReplies >= expRep {
		if a.log[rSlot].result != 0 {
			fmt.Printf("Consenseus reached entry:%v\n decision: %v", a.log[rSlot], a.log[rSlot].result)
			//flip color according to outcome
			if a.log[rSlot].result > 0 {
				a.Color = 0
			} else {
				a.Color = 1
			}
			s = "Consenseus reached, decision:" + strconv.Itoa(a.log[rSlot].result)

		} else {
			fmt.Printf("No Consensus\nAll decision recvd\nRetry\n")
			s = "No Consensus. All decision recvd. Retry\n"
		}
		reply := paxi.Reply{
			Command:    a.log[rSlot].command,
			Value:      []byte(s),
			Properties: make(map[string]string),
		}
		reply.Properties[HTTPHeaderSlot] = fmt.Sprintf("%f", a.execute)
		reply.Properties[HTTPHeaderExecute] = fmt.Sprintf("%f", a.execute)
		if a.log[rSlot].FromClient == true {
			a.log[rSlot].request.Reply(reply)
		}
		a.log[rSlot].request = nil
		a.execute++
	}
}
