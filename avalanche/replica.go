package avalanche

import (
	"fmt"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// Replica for one Avalanche instance
type Replica struct {
	paxi.Node
	*Avalanche
	//mux sync.Mutex
}

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

	//return replica
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
		r.Avalanche.HandleRequest(m)
	}
	//ignored get in benchmark.go
	if m.Command.IsRead() {
		return
	}
}
