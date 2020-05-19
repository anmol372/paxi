package avalanche

import (
	"fmt"
	"sync"
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// Replica for one Avalanche instance
type Replica struct {
	paxi.Node
	*Avalanche
	//mux sync.Mutex
}

type Request struct {
	slot            float64
	mIter           int
	recvdReplies    int
	expectedReplies int
	q               Query
}

var RecvFrom chan Request
var wg sync.WaitGroup

//var index int64

//var SendTo chan int

//var Trial chan string

const (
	HTTPHeaderSlot = "Slot"
	//HTTPHeaderBallot     = "Ballot"
	HTTPHeaderExecute = "Execute"
	//HTTPHeaderInProgress = "Inprogress"
)

// NewReplica generates new Avalanche replica
func NewReplica(id paxi.ID) *Replica {
	log.Debugf("Starting Replica with ID : %v\n", id)
	//fmt.Printf("Starting Replica with ID : %v\n", id)
	//new: returns a pointer to zero init replica struct
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Avalanche = NewAvalanche(r)

	//add msg interfaces to replica struct
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(Query{}, r.HandleQuery)
	r.Register(Reply{}, r.HandleReply)
	//r.Register()

	//RecvFrom = make(chan Request)
	//SendTo = make(chan int)
	//go r.maintainTimer()
	//index++

	return r
}

//recieves request from paxi
func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	fmt.Printf("Replica %s received %v\n", r.ID(), m)
	if m.Command.IsWrite() {
		/*//provide random seed
		//rand.Seed(time.Now().UnixNano())
		//0-red, 1-blue
		//randomColor := rand.Intn(1)
		//bs := make([]byte, 4)
		// 0: 00000000 1: 01000000
		//binary.LittleEndian.PutUint32(bs, uint32(randomColor))
		//m.Command.Value = bs
		fmt.Printf("manipulated value %v\n, color: %v and sent to handle request\n", m, m.Command.Value)
		*/
		//go r.checkTimeout()
		//wg.Add(2)
		//go r.maintainTimer()
		//go
		r.Avalanche.HandleRequest(m)

	}
	//ignored get in benchmark.go
	if m.Command.IsRead() {
		return
	}
	//select {}
}

func (r *Replica) maintainTimer() {
	fmt.Println("hello")
	RecvFrom = make(chan Request)
	for {
		//fmt.Printf("$")
		select {
		case req := <-RecvFrom:
			//wg.Add(1)
			//go
			r.startTimer(req)
			//default:
		}
		//fmt.Printf("!")
	}
	//fmt.Println("broke free now what")
}

func (r *Replica) startTimer(req Request) {
	//defer wg.Done()
	fmt.Printf("starting timer for slot: %v, iter: %v\n", req.slot, req.mIter)
	time.Sleep(1000 * time.Millisecond)
	for {
		//fmt.Printf("From start timer after timeout \n:::::::\n [%v]", r.Avalanche.log[req.slot].PerIterRecvd[req.mIter])
		if _, ok := r.Avalanche.log[req.slot]; ok {
			//fmt.Println("116")
			if r.Avalanche.log[req.slot].PerIterRecvd[req.mIter] < req.expectedReplies {

				send := req.expectedReplies - r.Avalanche.log[req.slot].PerIterRecvd[req.mIter]
				fmt.Println("needs some resends")
				r.Avalanche.MulticastRandom(0, send, req.q)
				time.Sleep(100 * time.Millisecond)
			} else if r.Avalanche.log[req.slot].PerIterRecvd[req.mIter] >= req.expectedReplies {
				//return
				fmt.Println("in else startTimer")
				//time.Sleep(24 * time.Hour)
				break
			}
		} else {
			fmt.Printf("Entry %v gone", req.slot)
			//return
			break
		}
	}
	//fmt.Println("return")

}
