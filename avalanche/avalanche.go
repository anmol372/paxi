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

//InitSlot is 1st slot of server
var InitSlot float64

//MSlot is max slot no in server right now
var MSlot float64

var sampleSize int
var totalNodes int
var consensus int
var alpha int
var beta int
var expRep int
var loopRound int //loop size
//var clientQ0timer = make(map[float64]*time.Timer)

//var redConfidence int
//var blueConfidence int

// entry in log
type entry struct {
	//ballot  paxi.Ballot
	command         paxi.Command
	commit          bool
	request         *paxi.Request
	timestamp       time.Time
	query           uint64 //paxi.Value
	result          int
	Red             []int
	Blue            []int
	PerQueryResult  []int
	PerIterRecvd    []int
	RecvReplies     int
	FromClient      bool
	CompletedIter   int
	Q               Query
	Replied         bool
	RedConsecutive  int
	BlueConsecutive int
	RedConfidence   int //confidence in red
	BlueConfidence  int //confidence in blue
}

// Avalanche instance
type Avalanche struct {
	paxi.Node

	config   []paxi.ID
	N        paxi.Config
	log      map[float64]*entry // log ordered by slot
	requests []*paxi.Request
	slot     float64 // highest slot number
	execute  float64
	Color    int //nodes current color
	//RedConfidence  int //confidence in red
	//BlueConfidence int //confidence in blue
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
		Node:     n,
		log:      make(map[float64]*entry, paxi.GetConfig().BufferSize),
		slot:     -1.0,
		execute:  0.0,
		requests: make([]*paxi.Request, 0),
		Color:    randomColor,
		//RedConfidence:  0,
		//BlueConfidence: 0,
	}
	InitSlot, _ = strconv.ParseFloat(string(p.ID()), 8)
	fmt.Printf("\ncolor %v\ntotalNodes %v\n", p.Color, totalNodes)
	p.slot += InitSlot
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
	MSlot = a.slot
	/*Rd := make([]int, loopRound+1)
	Bl := make([]int, loopRound+1)
	Pr := make([]int, loopRound+1)
	Pir := make([]int, loopRound+1)*/
	var Rd []int
	var Bl []int
	var Pr []int
	var Pir []int
	a.log[a.slot] = &entry{
		command:   r.Command,
		request:   &r,
		timestamp: time.Now(),
		query:     q, //r.Command.Value
		//result:      0,
		Red:             Rd,
		Blue:            Bl,
		PerQueryResult:  Pr,
		PerIterRecvd:    Pir,
		RecvReplies:     expRep,
		FromClient:      true,
		CompletedIter:   0,
		Replied:         false,
		RedConsecutive:  0,
		BlueConsecutive: 0,
		RedConfidence:   0,
		BlueConfidence:  0,
	}
	a.log[a.slot].Red = append(a.log[a.slot].Red, 0)
	a.log[a.slot].Blue = append(a.log[a.slot].Blue, 0)
	a.log[a.slot].PerQueryResult = append(a.log[a.slot].PerQueryResult, 0)
	a.log[a.slot].PerIterRecvd = append(a.log[a.slot].PerIterRecvd, 0)

	fmt.Printf("iter=%d len=%d cap=%d %v\n", 0, len(a.log[a.slot].Red), cap(a.log[a.slot].Red), a.log[a.slot].Red)

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

	a.log[a.slot].Q = m

	/*req := Request{
		slot:            m.MID,  //float64
		mIter:           m.Iter, // int
		recvdReplies:    0,      // int
		expectedReplies: expRep, //int
		q:               m,      //Query
	}
	fmt.Printf("recv: %v, expected: %v\n", a.log[a.slot].PerIterRecvd, expRep)
	RecvFrom <- req*/

	//msg := " send to channel successful"
	//select {
	//case
	//Trial <- msg
	//fmt.Println("sent message", msg)
	//default:
	//fmt.Println("no message sent")
	//}
	fmt.Printf("now multicasting randomly: %v\n", m)
	a.MulticastRandom(sampleSize, 0, m)

	//time.Sleep(10 * time.Millisecond)
	//fmt.Println("Awake now")
	//}
	/*go func() {
		<-clientQ0timer[a.slot].C
		fmt.Printf("Hello %v", a.slot)

	}()*/
}

// HandleQuery recieved by other servers
//TODO handle query with for...,
// internal request with timer
//Slush Loop
func (a *Avalanche) HandleQuery(m Query) {
	fmt.Printf("in slush loop, mid: %v from\n", m.MID)
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
	fmt.Printf("Query Reply %v\n", r.Reciever)
	a.Send(r.Reciever, r)
	//} else
	//if color is undecided, send query to get reply
	if a.Color == 2 {
		a.Color = int(m.Color)
		//create entry in local log and send to sampleSize random nodes
		//log entry
		/*
			Rd := make([]int, loopRound+1)
			Bl := make([]int, loopRound+1)
			Pr := make([]int, loopRound+1)
			Pir := make([]int, loopRound+1)
		*/
		var Rd []int
		var Bl []int
		var Pr []int
		var Pir []int
		a.log[m.MID] = &entry{
			timestamp: time.Now(),
			query:     m.Color,
			//result:       0,
			RecvReplies:    expRep,
			Red:            Rd,
			Blue:           Bl,
			PerQueryResult: Pr,
			PerIterRecvd:   Pir,
			//recv:        0,
			FromClient: false,
			Replied:    false,
			//redInternal:  0,
			//blueInternal: 0,
			RedConsecutive:  0,
			BlueConsecutive: 0,
			RedConfidence:   0,
			BlueConfidence:  0,
		}
		a.log[m.MID].Red = append(a.log[m.MID].Red, 0)
		a.log[m.MID].Blue = append(a.log[m.MID].Blue, 0)
		a.log[m.MID].PerQueryResult = append(a.log[m.MID].PerQueryResult, 0)
		a.log[m.MID].PerIterRecvd = append(a.log[m.MID].PerIterRecvd, 0)

		fmt.Printf("iter=%d len=%d cap=%d %v\n", 0, len(a.log[m.MID].Red), cap(a.log[m.MID].Red), a.log[m.MID].Red)
		//message query
		mq := Query{
			ID:         m.ID,
			Color:      m.Color,
			Sender:     a.ID(),
			MID:        m.MID, //message ID
			FromClient: false,
			Iter:       0,
		}

		a.log[m.MID].Q = mq
		//for i := 0; i < loopRound; i++ {
		//mq.Iter = i
		/*
			req := Request{
				slot:            m.MID,  //float64
				mIter:           m.Iter, // int
				recvdReplies:    0,      // int
				expectedReplies: expRep, //int
				q:               mq,     //Query
			}
			fmt.Printf("recv: %v, expected: %v\n", a.log[m.MID].PerIterRecvd, expRep)
			RecvFrom <- req
		*/

		fmt.Printf("now multicasting randomly: %v\n", m)
		a.MulticastRandom(sampleSize, 0, mq)
		//time.Sleep(10 * time.Millisecond)
		//}
	}
}

//HandleReply reply from queries
//todo: handle reply with loop
//Slush loop Reply processing
func (a *Avalanche) HandleReply(m Reply) {
	fmt.Printf("recv reply for mid %v\n", m.MID)
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
	//flip := false

	/*entry, error := a.log[rSlot]
	if !error {
		fmt.Printf("entry doesn't exist\n")
	} else {
		fmt.Printf("entry: %v\nquery: %v\n", entry, m.Color)
		fmt.Printf("\nslot: %v, iter: %v lenofArrays: %d\n\n", rSlot, rIter, len(a.log[rSlot].perIterRecvd))
	}*/

	a.log[rSlot].PerIterRecvd[rIter]++

	if rColor == 0 {
		a.log[rSlot].Red[rIter]++
	} else if rColor == 1 {
		a.log[rSlot].Blue[rIter]++
	}

	if a.log[rSlot].PerIterRecvd[rIter] >= a.log[rSlot].RecvReplies {
		if a.log[rSlot].Red[rIter] > a.log[rSlot].Blue[rIter] {
			//a.Color = 0
			a.log[rSlot].PerQueryResult[rIter] = 0
			a.log[rSlot].RedConfidence++
			a.log[rSlot].RedConsecutive++
			a.log[rSlot].BlueConsecutive = 0
		} else if a.log[rSlot].Blue[rIter] > a.log[rSlot].Red[rIter] {
			//a.Color = 1
			a.log[rSlot].PerQueryResult[rIter] = 1
			a.log[rSlot].BlueConfidence++
			a.log[rSlot].RedConsecutive = 0
			a.log[rSlot].BlueConsecutive++
		}
		//fmt.Printf("Consenseus reached, decision: %v", a.Color)
		a.log[rSlot].CompletedIter = rIter

		/*if rIter == 0 && a.log[rSlot].PerQueryResult[rIter] != a.Color {
			flip = true
		}
		*/
		//if iter > 0 check confidence and flip
		//if rIter > 0 {
		fmt.Printf("Set Color According to Confidence\n")
		a.checkConfidence(rSlot)
		//}

		//} //end of current iter

		/*if flip == false && rIter == 0 {
			//defer wg.Done()
			a.checkConfidence(rSlot)
			a.log[rSlot].result = a.Color
			//if m.FromClient == false {

			//} else  if
			if m.FromClient == true {
				//send reply
				a.replyToClient(rSlot)
			}
			return
		}*/

		//done with m rounds after Oth initial round
		//if rIter >= loopRound {
		if (a.log[rSlot].RedConsecutive == 3 && a.log[rSlot].BlueConsecutive == 0) || (a.log[rSlot].RedConsecutive == 0 && a.log[rSlot].BlueConsecutive == 3) {
			//defer wg.Done()
			//may need to re-eval confidence
			a.checkConfidence(rSlot)
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

	a.log[rSlot].Red = append(a.log[rSlot].Red, 0)
	a.log[rSlot].Blue = append(a.log[rSlot].Blue, 0)
	a.log[rSlot].PerQueryResult = append(a.log[rSlot].PerQueryResult, 0)
	a.log[rSlot].PerIterRecvd = append(a.log[rSlot].PerIterRecvd, 0)

	fmt.Printf("iter=%d len=%d cap=%d %v\n", rIter+1, len(a.log[rSlot].Red), cap(a.log[rSlot].Red), a.log[rSlot].Red)

	a.log[rSlot].Q.Iter = rIter + 1
	mq := a.log[rSlot].Q

	mq.Iter = m.Iter + 1

	/*if m.FromClient == true {
		wg.Add(1)
		req := Request{
			slot:            mq.MID,  //float64
			mIter:           mq.Iter, // int
			recvdReplies:    0,       // int
			expectedReplies: expRep,  //int
			q:               mq,      //Query
		}
		fmt.Printf("recv: %v, expected: %v\n", a.log[rSlot].PerIterRecvd, expRep)
		RecvFrom <- req
	}*/

	fmt.Printf("now multicasting randomly: %v\n", m)

	a.MulticastRandom(sampleSize, 0, mq)

}

func (a *Avalanche) checkConfidence(rSlot float64) {
	if a.log[rSlot].RedConfidence > a.log[rSlot].BlueConfidence {
		a.Color = 0
	} else if a.log[rSlot].BlueConfidence > a.log[rSlot].RedConfidence {
		a.Color = 1
	}
}

func (a *Avalanche) replyToClient(rSlot float64) {

	s := "Consenseus reached, decision:" + strconv.Itoa(a.log[rSlot].result)
	fmt.Printf("reply to client \n[%v]\n", s)
	fmt.Printf("perQueryIter: %v", a.log[rSlot].PerIterRecvd)
	reply := paxi.Reply{
		Command:    a.log[rSlot].command,
		Value:      []byte(s),
		Properties: make(map[string]string),
	}
	reply.Properties[HTTPHeaderSlot] = fmt.Sprintf("%f", a.execute)
	reply.Properties[HTTPHeaderExecute] = fmt.Sprintf("%f", a.execute)
	if a.log[rSlot].Replied == false {
		a.log[rSlot].request.Reply(reply)
		a.log[rSlot].request = nil
		a.execute++
		a.log[rSlot].Replied = true
		//close(RecvFrom)
	}

}
