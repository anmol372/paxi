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
var beta int
var expRep int
var loopRound int //loop size
//var redConfidence int
//var blueConfidence int

// entry in log
type entry struct {
	//ballot  paxi.Ballot
	command        paxi.Command
	commit         bool
	request        *paxi.Request
	timestamp      time.Time
	query          uint64 //paxi.Value
	result         int
	red            []int
	blue           []int
	perQueryResult []int
	perIterRecvd   []int
	recvReplies    int
	FromClient     bool
	CompletedIter  int
	q              Query
	//recv        int
	//redInternal  int
	//blueInternal int
}

// Avalanche instance
type Avalanche struct {
	paxi.Node

	config         []paxi.ID
	N              paxi.Config
	log            map[float64]*entry // log ordered by slot
	requests       []*paxi.Request
	slot           float64 // highest slot number
	execute        float64
	Color          int //nodes current color
	RedConfidence  int //confidence in red
	BlueConfidence int //confidence in blue
}

// NewAvalanche creates new avalanche instance
func NewAvalanche(n paxi.Node, options ...func(*Avalanche)) *Avalanche {
	totalNodes = len(paxi.GetConfig().Addrs)
	alpha = 60      //60%
	sampleSize = 60 //60%
	loopRound = 5
	beta = 3
	expRep = int(math.Ceil(float64(totalNodes*sampleSize) / (100)))
	//provide random seed
	rand.Seed(time.Now().UnixNano())
	//0-red, 1-blue, 2-undeclared
	//Intn(n) -> [0,n)
	randomColor := rand.Intn(3)
	p := &Avalanche{
		Node:           n,
		log:            make(map[float64]*entry, paxi.GetConfig().BufferSize),
		slot:           -1.0,
		execute:        0.0,
		requests:       make([]*paxi.Request, 0),
		Color:          randomColor,
		RedConfidence:  0,
		BlueConfidence: 0,
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
	rd := make([]int, loopRound+1)
	bl := make([]int, loopRound+1)
	pr := make([]int, loopRound+1)
	pir := make([]int, loopRound+1)
	a.log[a.slot] = &entry{
		command:   r.Command,
		request:   &r,
		timestamp: time.Now(),
		query:     q, //r.Command.Value
		//result:      0,
		red:            rd,
		blue:           bl,
		perQueryResult: pr,
		perIterRecvd:   pir,
		recvReplies:    expRep,
		FromClient:     true,
		CompletedIter:  0,
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
		Iter:       0,
	}

	a.log[a.slot].q = m

	fmt.Printf("now multicasting randomly: %v\n", m)
	//a.MulticastRandom(sampleSize, 0, m)
	//for i := 0; i < loopRound; i++ {
	//	m.Iter = i
	a.MulticastRandom(sampleSize, 0, m)
	//	time.Sleep(10 * time.Millisecond)
	//}
}

// HandleQuery recieved by other servers
//TODO handle query with for...,
// internal request with timer
//Slush Loop
func (a *Avalanche) HandleQuery(m Query) {
	fmt.Printf("in slush loop %v , mid: %v\n", m, m.MID)
	//reply to sender
	color := m.Color
	//if a.color == 0 || a.color == 1 {
	if a.Color == 0 || a.Color == 1 {
		color = uint64(a.Color)
	}
	r := Reply{
		Sender:     a.ID(),
		Color:      color,
		MID:        m.MID,
		Reciever:   m.ID,
		Iter:       m.Iter,
		FromClient: m.FromClient,
	}
	fmt.Printf("Query Reply %v\n", r)
	a.Send(r.Reciever, r)
	//} else
	//if color is undecided, send query to get reply
	if a.Color == 2 {
		a.Color = int(m.Color)
		//create entry in local log and send to sampleSize random nodes
		//log entry
		rd := make([]int, loopRound+1)
		bl := make([]int, loopRound+1)
		pr := make([]int, loopRound+1)
		pir := make([]int, loopRound+1)
		a.log[m.MID] = &entry{
			timestamp: time.Now(),
			query:     m.Color,
			//result:       0,
			recvReplies:    expRep,
			red:            rd,
			blue:           bl,
			perQueryResult: pr,
			perIterRecvd:   pir,
			//recv:        0,
			FromClient: false,
			//redInternal:  0,
			//blueInternal: 0,
		}
		//message query
		mq := Query{
			ID:         m.ID,
			Color:      m.Color,
			Sender:     a.ID(),
			MID:        m.MID, //message ID
			FromClient: false,
			Iter:       0,
		}

		a.log[m.MID].q = mq
		//for i := 0; i < loopRound; i++ {
		//mq.Iter = i
		a.MulticastRandom(sampleSize, 0, mq)
		//time.Sleep(10 * time.Millisecond)
		//}
	}
}

//HandleReply reply from queries
//todo: handle reply with loop
//Slush loop Reply processing
func (a *Avalanche) HandleReply(m Reply) {
	fmt.Printf("recv reply for mid %v", m.MID)
	//var s string
	//recieve reply
	//rColor := m.Color
	//rSlot := m.MID
	/*rQuery := 0
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
	//a.log[rSlot].result += rQuery
	//a.log[rSlot].recv++

	//if m.FromClient == false {
	//	fmt.Printf("Reply for internal query")
	a.handleReply(m)
	//}

	//if m.FromClient == true {
	//	fmt.Printf("Reply for internal query")
	//a.handleMainReply(m)
	//}

	/*
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
			//seperate func based on timeouts or if response complete
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
	*/
}

func (a *Avalanche) handleReply(m Reply) {

	rColor := m.Color
	rSlot := m.MID
	rIter := m.Iter
	flip := false

	entry, error := a.log[rSlot]
	if !error {
		fmt.Printf("entry doesn't exist\n")
	} else {
		fmt.Printf("entry: %v\nquery: %v\n", entry, m.Color)
		fmt.Printf("slot: %v, iter: %v\n\n", rSlot, rIter)
	}

	a.log[rSlot].perIterRecvd[rIter]++

	if rColor == 0 {
		a.log[rSlot].red[rIter]++
	} else if rColor == 1 {
		a.log[rSlot].blue[rIter]++
	}

	if a.log[rSlot].perIterRecvd[rIter] >= a.log[rSlot].recvReplies {
		if a.log[rSlot].red[rIter] > a.log[rSlot].blue[rIter] {
			//a.Color = 0
			a.log[rSlot].perQueryResult[rIter] = 0
			a.RedConfidence++
		} else if a.log[rSlot].blue[rIter] > a.log[rSlot].red[rIter] {
			//a.Color = 1
			a.log[rSlot].perQueryResult[rIter] = 1
			a.BlueConfidence++
		}
		//fmt.Printf("Consenseus reached, decision: %v", a.Color)
		a.log[rSlot].CompletedIter = rIter

		if rIter == 0 && a.log[rSlot].perQueryResult[rIter] != a.Color {
			flip = true
		}

		//if iter > 0 check confidence and flip
		if rIter > 0 {
			fmt.Printf("Set Color According to Confidence\n")
			/*if a.RedConfidence > a.BlueConfidence {
				a.Color = 0
			} else if a.BlueConfidence > a.RedConfidence {
				a.Color = 1
			}*/
			a.checkConfidence()
		}

		//} //end of current iter

		if flip == false && rIter == 0 {
			a.checkConfidence()
			a.log[rSlot].result = a.Color
			//if m.FromClient == false {

			//} else  if
			if m.FromClient == true {
				//send reply
				a.replyToClient(rSlot)
			}
			return
		}

		//done with m rounds after Oth initial round
		if rIter > loopRound {

			//may need to re-eval confidence
			a.checkConfidence()
			a.log[rSlot].result = a.Color

			if m.FromClient == true {
				//send reply
				a.replyToClient(rSlot)
			}
			return
		} //else {
		////continue query loop
		a.sendQuery(m)
		//}
	}

}

func (a *Avalanche) sendQuery(m Reply) {

	fmt.Printf("\n\nsend query for iter: %d\n\n", m.Iter+1)

	rSlot := m.MID
	rIter := m.Iter

	a.log[rSlot].q.Iter = rIter + 1
	mq := a.log[rSlot].q

	mq.Iter = m.Iter + 1

	a.MulticastRandom(sampleSize, 0, mq)

}

func (a *Avalanche) checkConfidence() {
	if a.RedConfidence > a.BlueConfidence {
		a.Color = 0
	} else if a.BlueConfidence > a.RedConfidence {
		a.Color = 1
	}
}

func (a *Avalanche) replyToClient(rSlot float64) {
	s := "Consenseus reached, decision:" + strconv.Itoa(a.log[rSlot].result)
	reply := paxi.Reply{
		Command:    a.log[rSlot].command,
		Value:      []byte(s),
		Properties: make(map[string]string),
	}
	reply.Properties[HTTPHeaderSlot] = fmt.Sprintf("%f", a.execute)
	reply.Properties[HTTPHeaderExecute] = fmt.Sprintf("%f", a.execute)

	a.log[rSlot].request.Reply(reply)
	a.log[rSlot].request = nil
	a.execute++

}
