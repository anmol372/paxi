package paxi

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ailidani/paxi/log"
)

//NoDuplicateSends maintains hash to not resend to same nodes innMulticastRandom
//var NoDuplicateSends = make(map[interface{}]map[ID]int)
var NoDuplicateSends = make(map[string]map[ID]int)

// Socket integrates all networking interface and fault injections
type Socket interface {

	// Send put message to outbound queue
	Send(to ID, m interface{})

	// MulticastZone send msg to all nodes in the same site
	MulticastZone(zone int, m interface{})

	// MulticastQuorum sends msg to random number of nodes
	MulticastQuorum(quorum int, m interface{})

	//if numNodes provided sends m to random numNodes else sends to percent of nodes provided
	//MulticastRandom sends msg to random nodes
	MulticastRandom(nodePercent int, numNodes int, mident string, m interface{})

	// Broadcast send to all peers
	Broadcast(m interface{})

	// Recv receives a message
	Recv() interface{}

	Close()

	// Fault injection
	Drop(id ID, t int)             // drops every message send to ID last for t seconds
	Slow(id ID, d int, t int)      // delays every message send to ID for d ms and last for t seconds
	Flaky(id ID, p float64, t int) // drop message by chance p for t seconds
	Crash(t int)                   // node crash for t seconds
}

type socket struct {
	id        ID
	addresses map[ID]string
	nodes     map[ID]Transport

	crash bool
	drop  map[ID]bool
	slow  map[ID]int
	flaky map[ID]float64

	lock sync.RWMutex // locking map nodes
}

// NewSocket return Socket interface instance given self ID, node list, transport and codec name
func NewSocket(id ID, addrs map[ID]string) Socket {
	socket := &socket{
		id:        id,
		addresses: addrs,
		nodes:     make(map[ID]Transport),
		crash:     false,
		drop:      make(map[ID]bool),
		slow:      make(map[ID]int),
		flaky:     make(map[ID]float64),
	}

	socket.nodes[id] = NewTransport(addrs[id])
	socket.nodes[id].Listen()

	return socket
}

func (s *socket) Send(to ID, m interface{}) {
	log.Debugf("node %s send message %+v to %v", s.id, m, to)

	if s.crash {
		return
	}

	if s.drop[to] {
		return
	}

	if p, ok := s.flaky[to]; ok && p > 0 {
		if rand.Float64() < p {
			return
		}
	}

	s.lock.RLock()
	t, exists := s.nodes[to]
	s.lock.RUnlock()
	if !exists {
		s.lock.RLock()
		address, ok := s.addresses[to]
		s.lock.RUnlock()
		if !ok {
			log.Errorf("socket does not have address of node %s", to)
			return
		}
		t = NewTransport(address)
		err := Retry(t.Dial, 100, time.Duration(50)*time.Millisecond)
		if err != nil {
			panic(err)
		}
		s.lock.Lock()
		s.nodes[to] = t
		s.lock.Unlock()
	}

	if delay, ok := s.slow[to]; ok && delay > 0 {
		timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
		go func() {
			<-timer.C
			t.Send(m)
		}()
		return
	}

	t.Send(m)
}

func (s *socket) Recv() interface{} {
	s.lock.RLock()
	t := s.nodes[s.id]
	s.lock.RUnlock()
	for {
		m := t.Recv()
		if !s.crash {
			return m
		}
	}
}

func (s *socket) MulticastZone(zone int, m interface{}) {
	//log.Debugf("node %s broadcasting message %+v in zone %d", s.id, m, zone)
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		if id.Zone() == zone {
			s.Send(id, m)
		}
	}
}

func (s *socket) MulticastQuorum(quorum int, m interface{}) {
	//log.Debugf("node %s multicasting message %+v for %d nodes", s.id, m, quorum)
	i := 0
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		s.Send(id, m)
		i++
		if i == quorum {
			break
		}
	}
}

func (s *socket) MulticastRandom(nodePercent int, numNodes int, mident string, m interface{}) {
	//messageID := m.
	fmt.Printf("\nin multicast random\nm:%v\n", m)
	totalNodes := len(s.addresses)
	nodes := totalNodes * nodePercent / 100
	fmt.Printf("total nodes: %v, send to: %v or %v\n", totalNodes, nodes, numNodes)
	if totalNodes < 3 {
		s.Broadcast(m)
	}
	randMap := make(map[int]int)
	addressMap := make(map[int]ID)
	j := 0
	for id := range s.addresses {
		if id != s.id {
			addressMap[j] = id
			j++
		}
	}
	//provide random seed
	rand.Seed(time.Now().UnixNano())
	if numNodes > 0 {
		nodes = numNodes
	}
	i := 0
	/*
		if _, ok := NoDuplicateSends[m]; !ok {
			NoDuplicateSends[m] = make(map[ID]int)
		}
		for i < nodes {
			randomnumber := rand.Intn(totalNodes)
			if _, ok := randMap[randomnumber]; !ok {
				randMap[randomnumber] = randomnumber
				id, okk := addressMap[randomnumber]
				if okk {
					if _, okkk := NoDuplicateSends[m][id]; !okkk {
						fmt.Printf("sending to node %v\n", id)
						s.Send(id, m)
						NoDuplicateSends[m][id] = 1
						i++
					} else if len(NoDuplicateSends[m]) == len(addressMap) {
						fmt.Printf("Sent to all nodes\n")
						return
					}
				}
			}
		}
	*/
	if _, ok := NoDuplicateSends[mident]; !ok {
		NoDuplicateSends[mident] = make(map[ID]int)
	}
	for i < nodes {
		randomnumber := rand.Intn(totalNodes)
		if _, ok := randMap[randomnumber]; !ok {
			randMap[randomnumber] = randomnumber
			id, okk := addressMap[randomnumber]
			if okk {
				if _, okkk := NoDuplicateSends[mident][id]; !okkk {
					fmt.Printf("sending to node %v for identifier:%s\n", id, mident)
					NoDuplicateSends[mident][id] = 1
					i++
					s.Send(id, m)
				} else if len(NoDuplicateSends[mident]) == len(addressMap) {
					fmt.Printf("Sent to all nodes\n")
					return
				}
			}
		}
	}
	fmt.Printf("done sending\n")
}

func (s *socket) Broadcast(m interface{}) {
	//log.Debugf("node %s broadcasting message %+v", s.id, m)
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		s.Send(id, m)
	}
}

func (s *socket) Close() {
	for _, t := range s.nodes {
		t.Close()
	}
}

func (s *socket) Drop(id ID, t int) {
	s.drop[id] = true
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.drop[id] = false
	}()
}

func (s *socket) Slow(id ID, delay int, t int) {
	s.slow[id] = delay
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.slow[id] = 0
	}()
}

func (s *socket) Flaky(id ID, p float64, t int) {
	s.flaky[id] = p
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.flaky[id] = 0
	}()
}

func (s *socket) Crash(t int) {
	s.crash = true
	if t > 0 {
		timer := time.NewTimer(time.Duration(t) * time.Second)
		go func() {
			<-timer.C
			s.crash = false
		}()
	}
}
