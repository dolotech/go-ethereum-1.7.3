package main

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/nat"

	"github.com/golang/glog"
	"unsafe"
	"sync/atomic"
	"crypto/sha1"
	"github.com/ethereum/go-ethereum/rlp"
	"errors"
	"flag"
)


const keysCount = 32

var keys = [keysCount]string{
	"d49dcf37238dc8a7aac57dc61b9fee68f0a97f062968978b9fafa7d1033d03a9",
	"73fd6143c48e80ed3c56ea159fe7494a0b6b393a392227b422f4c3e8f1b54f98",
	"119dd32adb1daa7a4c7bf77f847fb28730785aa92947edf42fdd997b54de40dc",
	"deeda8709dea935bb772248a3144dea449ffcc13e8e5a1fd4ef20ce4e9c87837",
	"5bd208a079633befa349441bdfdc4d85ba9bd56081525008380a63ac38a407cf",
	"1d27fb4912002d58a2a42a50c97edb05c1b3dffc665dbaa42df1fe8d3d95c9b5",
	"15def52800c9d6b8ca6f3066b7767a76afc7b611786c1276165fbc61636afb68",
	"51be6ab4b2dc89f251ff2ace10f3c1cc65d6855f3e083f91f6ff8efdfd28b48c",
	"ef1ef7441bf3c6419b162f05da6037474664f198b58db7315a6f4de52414b4a0",
	"09bdf6985aabc696dc1fbeb5381aebd7a6421727343872eb2fadfc6d82486fd9",
	"15d811bf2e01f99a224cdc91d0cf76cea08e8c67905c16fee9725c9be71185c4",
	"2f83e45cf1baaea779789f755b7da72d8857aeebff19362dd9af31d3c9d14620",
	"73f04e34ac6532b19c2aae8f8e52f38df1ac8f5cd10369f92325b9b0494b0590",
	"1e2e07b69e5025537fb73770f483dc8d64f84ae3403775ef61cd36e3faf162c1",
	"8963d9bbb3911aac6d30388c786756b1c423c4fbbc95d1f96ddbddf39809e43a",
	"0422da85abc48249270b45d8de38a4cc3c02032ede1fcf0864a51092d58a2f1f",
	"8ae5c15b0e8c7cade201fdc149831aa9b11ff626a7ffd27188886cc108ad0fa8",
	"acd8f5a71d4aecfcb9ad00d32aa4bcf2a602939b6a9dd071bab443154184f805",
	"a285a922125a7481600782ad69debfbcdb0316c1e97c267aff29ef50001ec045",
	"28fd4eee78c6cd4bf78f39f8ab30c32c67c24a6223baa40e6f9c9a0e1de7cef5",
	"c5cca0c9e6f043b288c6f1aef448ab59132dab3e453671af5d0752961f013fc7",
	"46df99b051838cb6f8d1b73f232af516886bd8c4d0ee07af9a0a033c391380fd",
	"c6a06a53cbaadbb432884f36155c8f3244e244881b5ee3e92e974cfa166d793f",
	"783b90c75c63dc72e2f8d11b6f1b4de54d63825330ec76ee8db34f06b38ea211",
	"9450038f10ca2c097a8013e5121b36b422b95b04892232f930a29292d9935611",
	"e215e6246ed1cfdcf7310d4d8cdbe370f0d6a8371e4eb1089e2ae05c0e1bc10f",
	"487110939ed9d64ebbc1f300adeab358bc58875faf4ca64990fbd7fe03b78f2b",
	"824a70ea76ac81366da1d4f4ac39de851c8ac49dca456bb3f0a186ceefa269a5",
	"ba8f34fa40945560d1006a328fe70c42e35cc3d1017e72d26864cd0d1b150f15",
	"30a5dfcfd144997f428901ea88a43c8d176b19c79dde54cc58eea001aa3d246c",
	"de59f7183aca39aa245ce66a05245fecfc7e2c75884184b52b27734a4a58efa2",
	"92629e2ff5f0cb4f5f08fffe0f64492024d36f045b901efb271674b801095c5a",
}


const (
	nodesCount      = 7
	mesgsCount      = 30
	maxPeersPerNode = nodesCount/2 + 1
	ringSize        = 1000
)

const (
	statusCode           = 0
	messagesCode         = 1
	NumberOfMessageCodes = 255
)

const (
	ProtocolName       = "cht"
	ProtocolVersion    = uint64(1)
	ProtocolVersionStr = "1.0"

	batchLength      = 10
	broadcastTimeout = 100 * time.Millisecond
)

const TTL = 60

type Message struct {
	Room      string `json:"room"`
	Nickname  string `json:"nickname"`
	Text      string `json:"text"`
	Timestamp int64  `json:"timestamp,omitempty"`
}

type message struct {
	body []byte

	cDeathtime int64
	cHash      *Hash
	peerID     []byte
	index      int64
}

type testNode struct {
	no     int
	id     *ecdsa.PrivateKey
	server *p2p.Server
	board  *board
	quit   chan struct{}
	hashes map[Hash]struct{}
	mu     sync.Mutex
}

type nodes []*testNode

type TSF struct {
	mu sync.Mutex
	m  map[Hash]int64
}
type Hash [20]byte
type board struct {
	index int64
	ring  []unsafe.Pointer
	tsf   *TSF
}
type peer struct {
	board *board
	p2    *p2p.Peer
	rw    p2p.MsgReadWriter
	tsf   *TSF
}
type S struct {
	Hash
	string
}

func main() {
	flag.Parse()
	ns := setup(nodesCount, ringSize, maxPeersPerNode)
	glog.Infoln("%d nodes started", len(ns))

	hs := make([]S, mesgsCount*nodesCount)

	ns.checkpoint(0)

	glog.Infoln("SENDING MESSAGES")

	var wg sync.WaitGroup
	for _, n := range ns {
		wg.Add(1)
		go func(n *testNode) {
			for i := 0; i < mesgsCount; i++ {
				s := fmt.Sprintf("test message %d via node %d", i, n.no)
				h, err := n.send("", s)
				if err != nil {
					glog.Fatal(err)
				}
				hs[n.no*mesgsCount+i] = S{h, s}
			}
			wg.Done()
		}(n)
	}
	wg.Wait()

	glog.Infoln("CHECKING PROPAGATION FOR MESSAGES")

	var count int

CheckPropagation:
	for _, n := range ns {
	WaitingFor10secMax:
		for i := 0; i < 10; i++ {
			count = 0
			for _, v := range hs {
				if !n.has(v.Hash) {
					count++
				}
			}
			if count == 0 {
				break WaitingFor10secMax
			}
			glog.Infof("Waiting for node %d, it missed %d messages", n.no, count)
			<-time.After(time.Second)
		}
		if count != 0 {
			break CheckPropagation
		}
	}

	ns.stop()

	if count != 0 {
		var bf bytes.Buffer
		bf.WriteString("-\n")
		for _, n := range ns {
			count = 0
			for _, v := range hs {
				if !n.has(v.Hash) {
					count++
				}
			}
			bf.WriteString(fmt.Sprintf("node %d lost %d of %d messages\n", n.no, count, len(hs)))
			for _, v := range hs {
				if !n.has(v.Hash) {
					bf.WriteString(fmt.Sprintf("      %v: %v\n", v.Hash, v.string))
				}
			}
			iid := discover.PubkeyID(&n.id.PublicKey)
			bf.WriteString(fmt.Sprintf("ring (%v):\n", hex.EncodeToString(iid[:8])))
			m, i := n.board.get(0)
			for m != nil {
				mesg, _ := m.open()
				peer := "self"
				if m.peerID != nil {
					peer = hex.EncodeToString(m.peerID[:8])
				}
				bf.WriteString(fmt.Sprintf(" %3d, %v: %v, %v\n", i-1, m.hash(), mesg.Text, peer))
				m, i = n.board.get(i)
			}
		}
		glog.Fatalf(bf.String())
	}
}

func newTSF() *TSF {
	return &TSF{
		m: make(map[Hash]int64),
	}
}

func (tsf *TSF) pass(m *message) bool {
	dt := m.deathTime()
	hash := m.hash()

	tsf.mu.Lock()
	_, exists := tsf.m[hash]
	if !exists {
		tsf.m[hash] = dt
	}
	tsf.mu.Unlock()

	return !exists
}

func (tsf *TSF) passDT(m *message, now int64) bool {
	dt := m.deathTime()
	if dt <= now {
		return false
	}

	return tsf.pass(m)
}

func (tsf *TSF) clean() {
	t := time.Now().Unix()
	tsf.mu.Lock()
	for k, dt := range tsf.m {
		if dt <= t {
			delete(tsf.m, k)
		}
	}
	tsf.mu.Unlock()
}

func (tsf *TSF) expire(quit chan struct{}) {
	t := time.NewTicker(TTL * time.Second)
	for {
		if done2(quit, t.C) {
			return
		}
		tsf.clean()
	}
}

func newBoard(ringSize int) *board {
	return &board{
		ring: make([]unsafe.Pointer, ringSize),
		tsf:  newTSF(),
	}
}

func (brd *board) get(index int64) (*message, int64) {
	L := int64(len(brd.ring))

	for {
		head := int64(0)
		tail := atomic.LoadInt64(&brd.index)
		if tail > L {
			head = tail - L
		}
		if index < head {
			index = head
		}
		if index >= tail {
			return nil, index
		}
		p := &brd.ring[index%L]
		m := (*message)(atomic.LoadPointer(p))
		if m == nil || m.index < index {
			return nil, index
		}
		if m.index == index {
			return m, index + 1
		}
	}
}

func (brd *board) put(m *message) {

	if !brd.tsf.pass(m) {
		return
	}

	L := int64(len(brd.ring))
	tail := atomic.AddInt64(&brd.index, 1)
	m.index = tail - 1
	p := &brd.ring[m.index%L]
	atomic.StorePointer(p, unsafe.Pointer(m))
}

func (brd *board) expire(quit chan struct{}) {
	brd.tsf.expire(quit)
}

func setup(nodesCount int, boardRingSize int, maxPeersPerNode int) (ns nodes) {
	var err error
	ip := net.IPv4(127, 0, 0, 1)
	port0 := 30999

	if nodesCount > keysCount {
		glog.Fatalf("to many nodes")
	}

	ns = make(nodes, 0, nodesCount)

	for i := 0; i < nodesCount; i++ {
		var node testNode

		node.no = i
		node.id, err = crypto.HexToECDSA(keys[i])
		if err != nil {
			glog.Fatalf("failed convert the key: %s", keys[i])
		}

		port := port0 + i
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		name := common.MakeName("chat-go", "1.0")
		var peers []*discover.Node
		if i > 0 {
			peerNodeID := ns[i-1].id
			peerPort := uint16(port - 1)
			peerNode := discover.PubkeyID(&peerNodeID.PublicKey)
			peer := discover.NewNode(peerNode, ip, peerPort, peerPort)
			peers = append(peers, peer)
		}

		node.board = newBoard(boardRingSize)

		node.server = &p2p.Server{
			Config: p2p.Config{
				PrivateKey:     node.id,
				MaxPeers:       maxPeersPerNode,
				Name:           name,
				Protocols:      protocols(node.board, 100000),
				ListenAddr:     addr,
				NAT:            nat.Any(),
				BootstrapNodes: peers,
				StaticNodes:    peers,
				TrustedNodes:   peers,
			},
		}

		ns = append(ns, &node)
	}

	ec := make(chan error, nodesCount)
	ns[0].start(ec)
	for i := 1; i < nodesCount; i++ {
		go ns[i].start(ec)
	}

	for i := 0; i < nodesCount; i++ {
		err := <-ec
		if err != nil {
			glog.Fatal(err)
		}
	}

	return
}

func (n *testNode) has(h Hash) bool {
	n.mu.Lock()
	_, ok := n.hashes[h]
	n.mu.Unlock()
	return ok
}

func (n *testNode) watch() {
	delay := time.NewTicker(100 * time.Millisecond)
	var index int64
	var m *message
	for {
		if done2(n.quit, delay.C) {
			return
		}
		m, index = n.board.get(index)
		for m != nil {
			glog.Infoln("watch", "hash", m.hash())
			if mesg, err := m.open(); err != nil {
				glog.Infoln("mesg open error", "m", m, "err", err)
			} else {
				n.mu.Lock()
				n.hashes[mesg.Hash()] = struct{}{}
				n.mu.Unlock()
			}

			if done(n.quit) {
				return
			}
			m, index = n.board.get(index)
		}
	}
}

func (ns nodes) checkpoint(no int) {
	glog.Infoln("CHECKPOINT %d", no)

	h, err := ns[0].send("", fmt.Sprintf("CHECKPOINT %d", no))
	if err != nil {
		glog.Fatal(err)
	}

	for j, n := range ns {
		succeeded := false
	WaitForEndMesg:
		for i := 0; i < 100; i++ {
			if _, ok := n.hashes[h]; ok {
				succeeded = true
				break WaitForEndMesg
			}
			<-time.After(100 * time.Millisecond)
		}
		if !succeeded {
			glog.Fatalf("node %d has not recived checkpoint message", j)
		}
	}
}

func (n *testNode) start(ec chan error) {
	err := n.server.Start()
	if err != nil {
		ec <- fmt.Errorf("failed to start the server %d: %v", n.no, err)
		return
	}
	n.quit = make(chan struct{})
	n.hashes = make(map[Hash]struct{})
	go n.watch()
	go n.board.expire(n.quit)
	ec <- nil
}

func (n *testNode) stop() {
	n.server.Stop()
	close(n.quit)
}

func (ns nodes) stop() {
	for i := 0; i < len(ns); i++ {
		n := ns[i]
		if n != nil {
			n.stop()
		}
	}
}

func (n *testNode) send(room, text string) (Hash, error) {
	m := &message{}
	if err := m.seal(&Message{Room: room, Text: text}); err != nil {
		return Hash{}, err
	}
	n.board.put(m)
	return m.hash(), nil
}

func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

func (mesg *Message) EqualNoStamp(a *Message) bool {
	return mesg.Room == a.Room &&
		mesg.Nickname == a.Nickname &&
		mesg.Text == a.Text
}

func (mesg *Message) Hash() Hash {
	var m message
	m.encode(mesg, mesg.Timestamp)
	return m.hash()
}

func (m *message) expired() bool {
	return m.deathTime() <= time.Now().Unix()
}

func (m *message) validate() error {
	return nil
}

func (m *message) deathTime() int64 {
	if m.cDeathtime == 0 {
		ts, _ := m.fetchTimestamp()
		m.cDeathtime = ts + TTL
	}
	return m.cDeathtime
}

func (m *message) hash() Hash {
	if m.cHash == nil {
		h := Hash(sha1.Sum(m.body))
		m.cHash = &h
	}
	return *m.cHash
}

func (m *message) encode(mesg *Message, timestamp int64) error {
	var l byte
	var l2 uint16
	var b bytes.Buffer

	for n := 0; n < 8; n++ {
		b.WriteByte(byte(timestamp >> (uint(n) * 8)))
	}

	l = byte(len(mesg.Room))
	b.WriteByte(l)
	if l > 0 {
		b.Write([]byte(mesg.Room)[:l])
	}

	l = byte(len(mesg.Nickname))
	b.WriteByte(l)
	if l > 0 {
		b.Write([]byte(mesg.Nickname)[:l])
	}

	l2 = uint16(len(mesg.Text))
	b.WriteByte(byte(l2))
	b.WriteByte(byte(l2 >> 8))
	if l2 > 0 {
		b.Write([]byte(mesg.Text)[:l2])
	}

	m.body = b.Bytes()
	return nil
}

func (m *message) seal(mesg *Message) error {
	t := time.Now().Unix()
	return m.encode(mesg, t)
}

var badMessageError = errors.New("bad message")

func (m *message) fetchTimestamp() (int64, error) {
	var ts int64
	b := m.body
	if len(b) < 8 {
		return 0, badMessageError
	}
	for n := 0; n < 8; n++ {
		ts |= int64(b[n]) << (uint(n) * 8)
	}
	return ts, nil
}

func (m *message) open() (*Message, error) {
	var l int
	var b []byte
	var err error

	mesg := &Message{}

	mesg.Timestamp, err = m.fetchTimestamp()
	if err != nil {
		return nil, err
	}

	b = m.body[8:]

	l = int(b[0])
	if len(b) < l+1 {
		return nil, badMessageError
	}
	mesg.Room = string(b[1: l+1])
	b = b[l+1:]

	l = int(b[0])
	if len(b) < l+1 {
		return nil, badMessageError
	}
	mesg.Nickname = string(b[1: l+1])
	b = b[l+1:]

	l = int(b[0]) + (int(b[1]) << 8)
	if len(b) < l+1 {
		return nil, badMessageError
	}
	mesg.Text = string(b[2: l+2])

	return mesg, nil
}

func protocols(brd *board, p2pMaxPkgSize uint32) []p2p.Protocol {
	return []p2p.Protocol{
		{
			Name:    ProtocolName,
			Version: uint(ProtocolVersion),
			Length:  NumberOfMessageCodes,
			NodeInfo: func() interface{} {
				return map[string]interface{}{
					"version":        ProtocolVersionStr,
					"maxMessageSize": p2pMaxPkgSize,
				}
			},
			Run: func(p2 *p2p.Peer, mrw p2p.MsgReadWriter) error {
				return (&peer{brd, p2, mrw, newTSF()}).loop(p2pMaxPkgSize)
			},
		},
	}
}

func (p *peer) ID() []byte {
	id := p.p2.ID()
	return id[:]
}

func (p *peer) broadcast(quit chan struct{}) {
	t := time.NewTicker(broadcastTimeout)
	var index int64
	batch := make([][]byte, batchLength)
	for {
		if done2(quit, t.C) {
			return
		}

		now := time.Now().Unix()
		n := 0
		m, i := p.board.get(index)
	FillBatch:
		for m != nil {
			if done(quit) {
				return
			}
			if p.tsf.passDT(m, now) {
				batch[n] = m.body
				n++
			}
			if n == batchLength {
				break FillBatch
			}
			m, i = p.board.get(i)
		}

		if n != 0 {
			if err := p2p.Send(p.rw, messagesCode, batch[:n]); err != nil {
				glog.Infoln("failed to send messages", "peer", p.ID, "err", err)
			} else {
				index = i
			}
		}
	}
}

func (p *peer) handshake() error {
	ec := make(chan error, 1)
	go func() {
		ec <- p2p.SendItems(p.rw, statusCode, ProtocolVersion)
	}()

	pkt, err := p.rw.ReadMsg()
	if err != nil {
		return fmt.Errorf("peer [%x] failed to read packet: %v", p.ID(), err)
	}
	if pkt.Code != statusCode {
		return fmt.Errorf("peer [%x] sent packet %x before status packet", p.ID(), pkt.Code)
	}
	s := rlp.NewStream(pkt.Payload, uint64(pkt.Size))
	_, err = s.List()
	if err != nil {
		return fmt.Errorf("peer [%x] sent bad status message: %v", p.ID(), err)
	}
	peerVersion, err := s.Uint()
	if err != nil {
		return fmt.Errorf("peer [%x] sent bad status message (unable to decode version): %v", p.ID(), err)
	}
	if peerVersion != ProtocolVersion {
		return fmt.Errorf("peer [%x]: protocol version mismatch %d != %d", p.ID(), peerVersion, ProtocolVersion)
	}
	if err := <-ec; err != nil {
		return fmt.Errorf("peer [%x] failed to send status packet: %v", p.ID(), err)
	}

	return nil
}

func (p *peer) loop(p2pMaxPkgSize uint32) error {
	if err := p.handshake(); err != nil {
		glog.Errorln("handshake failed", "peer", p.ID(), "err", err)
		return err
	}

	quit := make(chan struct{})
	go p.broadcast(quit)
	go p.tsf.expire(quit)
	defer close(quit)

	for {
		pkt, err := p.rw.ReadMsg()
		if err != nil {
			if err.Error() != "EOF" {
				glog.Infoln("message loop", "peer", p.ID(), "err", err)
			}
			return err
		}
		if pkt.Size > p2pMaxPkgSize {
			glog.Infoln("oversized packet received", "peer", p.ID())
			return errors.New("oversized packet received")
		}

		switch pkt.Code {
		case statusCode:
			glog.Infoln("unxepected status packet received", "peer", p.ID())

		case messagesCode:
			var bs [][]byte

			if err := pkt.Decode(&bs); err != nil {
				glog.Errorln("failed to decode messages, peer will be disconnected", "peer", p.ID(), "err", err)
				return errors.New("invalid messages")
			}

			for _, b := range bs {
				m := &message{body: b, peerID: p.p2.ID().Bytes()}
				if err := m.validate(); err != nil {
					glog.Errorln("bad message received, peer will be disconnected", "peer", p.ID(), "err", err)
					return errors.New("invalid message")
				}
				now := time.Now().Unix()
				if p.tsf.passDT(m, now) {
					p.board.put(m)
				}
			}
		}
	}
}
func done(q chan struct{}) bool {
	select {
	case <-q:
		return true
	default:
		return false
	}
}

func done2(q chan struct{}, t <-chan time.Time) bool {
	select {
	case <-q:
		return true
	case <-t:
		return false
	}
}
