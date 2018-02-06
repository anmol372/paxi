package async_paxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(P1a{})
	gob.Register(P1b{})
	gob.Register(P2a{})
	gob.Register(P2b{})
}

// P1a prepare message
type P1a struct {
	Ballot paxi.Ballot
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v}", m.Ballot)
}

type CommandBallot struct {
	Command paxi.Command
	Ballot  paxi.Ballot
}

func (cb CommandBallot) String() string {
	return fmt.Sprintf("c=%v b=%v", cb.Command, cb.Ballot)
}

// P1b promise message
type P1b struct {
	Ballot paxi.Ballot
	ID     paxi.ID               // from node id
	Log    map[int]CommandBallot // uncommitted logs
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v, log=%v}", m.Ballot, m.Log)
}

// P2a accept message
type P2a struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v, s=%d, c=%v}", m.Ballot, m.Slot, m.Command)
}

// P2b accepted message
type P2b struct {
	Ballot paxi.Ballot
	ID     paxi.ID // from node id
	Slot   int
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v, s=%d}", m.Ballot, m.Slot)
}