package avalanche

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(Query{})
	gob.Register(Reply{})
}

//Query message
type Query struct {
	//Ballot paxi.Ballot
	ID paxi.ID
	//Request paxi.Request
	//Command paxi.Command
	Color      uint64
	Sender     paxi.ID
	MID        float64 //message ID
	FromClient bool    //query from client = true, from server = false
	//Slot   int
}

func (m Query) String() string {
	return fmt.Sprintf("\nQuery from ID %v, with Color %v and mid(slot) %v\n", m.ID, int(m.Color), m.MID)
}

//Reply message for query
type Reply struct {
	Sender   paxi.ID
	Reciever paxi.ID
	Color    uint64
	MID      float64
}

func (r Reply) String() string {
	return fmt.Sprintf("\nReply from ID %v, with Color %v to Reciever %v, mid(slot) %v\n", r.Sender, int(r.Color), r.Reciever, r.MID)
}
