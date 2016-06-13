package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"math"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

import "log"

import "syscall"

import "sync/atomic"

import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	//iota为行号，从0开始，枚举值下面默认的和上面一样为iota+1，这样这里的四个值为1,2,3
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
	Empty
)

const (
	InitialValue = -1
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here
	majoritySize int
	instances    map[int]*InstanceState
	doneSeqs     []int
	maxSeq       int
	minSeq       int
}

type InstanceState struct {
	promisedN string
	acceptN   string
	acceptV   interface{}
	status    Fate
}

type PrepareArgs struct {
	Pid         int
	ProposalNum string
}

type PrepareReply struct {
	Promised bool
	AcceptN  string
	AcceptV  interface{}
}

type AcceptArgs struct {
	Pid         int
	ProposalNum string
	value       interface{}
}

type AcceptReply struct {
	Accepted bool
}

type DecidedArgs struct {
	Pid         int
	ProposalNum string
	Value       interface{}
}

type DecidedReply struct {
	Done int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//Prepare 方法
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if _, exist := px.instances[args.Pid]; exist {
		if px.instances[args.Pid].promisedN < args.ProposalNum {
			px.instances[args.Pid].promisedN = args.ProposalNum
		} else {
			//拒绝
			return nil
		}
	} else {
		px.instances[args.Pid] = &InstanceState{
			promisedN: "",
			acceptN:   "",
			acceptV:   nil,
			status:    0,
		}
	}
	px.instances[args.Pid].promisedN = args.ProposalNum
	reply.Promised = true
	reply.AcceptN = px.instances[args.Pid].acceptN
	reply.AcceptV = px.instances[args.Pid].acceptV
	return nil
}

//Accept 函数，接受大于等于自己的承诺值
func (px *Paxos) Accept(args AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	_, exist := px.instances[args.Pid]
	if !exist {
		px.instances[args.Pid] = &InstanceState{
			promisedN: "",
			acceptN:   "",
			acceptV:   nil,
			status:    0,
		}
	}
	if args.ProposalNum >= px.instances[args.Pid].promisedN {
		px.instances[args.Pid].promisedN = args.ProposalNum
		px.instances[args.Pid].acceptN = args.ProposalNum
		px.instances[args.Pid].acceptV = args.value
		px.instances[args.Pid].status = Pending
		reply.Accepted = true
	} else {
		reply.Accepted = false
	}
	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if _, exist := px.instances[args.Pid]; exist {
		px.instances[args.Pid].acceptN = args.ProposalNum
		px.instances[args.Pid].acceptV = args.Value
		px.instances[args.Pid].status = Decided
	} else {
		px.instances[args.Pid] = &InstanceState{
			promisedN: "",
			acceptN:   args.ProposalNum,
			acceptV:   args.Value,
			status:    Decided,
		}
	}
	//搞清楚这里
	if args.Pid > px.maxSeq {
		px.maxSeq = args.Pid
	}
	reply.Done = px.doneSeqs[px.me]
	return nil
}

func genereteIncreasingNum(me int) string {
	return strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.Itoa(me)
}

//选择出peers中的大多数
func (px *Paxos) selectMajority() []string {
	majority := make(map[int]bool)
	answer := make([]string, 0, px.majoritySize)
	size := len(px.peers)
	i := 0
	for i < px.majoritySize {
		randNum := rand.Intn(size)
		if _, exist := majority[randNum]; exist {
			continue
		} else {
			answer = append(answer, px.peers[randNum])
			majority[randNum] = true
			i++
		}
	}
	return answer
}

func (px *Paxos) sendPrepare(acceptors []string, seq int, proposalNum string, v interface{}) (int, interface{}) {
	preparedServers := 0
	args := &PrepareArgs{seq, proposalNum}
	maxValue := v
	maxSeq := ""
	for i, server := range acceptors {
		var reply PrepareReply
		ret := false
		if i != px.me {
			ret = call(server, "Paxos.Prepare", args, &reply)
		} else {
			ret = px.Prepare(args, &reply) == nil
		}
		if ret && reply.Promised {
			if reply.AcceptN > maxSeq {
				maxSeq = reply.AcceptN
				maxValue = reply.AcceptV
			}
			preparedServers++
		}
	}
	return preparedServers, maxValue
}
func (px *Paxos) sendAccept(acceptors []string, pid int, proposalNum string, maxValue interface{}) int {
	acceptedServers := 0
	acceptArgs := &AcceptArgs{pid, proposalNum, maxValue}
	for i, server := range acceptors {
		var acceptReply AcceptReply
		ret := false
		if i != px.me {
			ret = call(server, "Paxos.Accept", acceptArgs, &acceptReply)
		} else {
			ret = px.Accept(acceptArgs, &acceptReply) == nil
		}
		if ret && acceptReply.Accepted {
			acceptedServers++
		}
	}
	return acceptedServers
}

//func (px *Paxos) sendAccept(acceptors []string, pid int, proposalNum string, maxValue interface{}) int {
//	acceptedServers := 0
//	acceptArgs := &AcceptArgs{pid, proposalNum, maxValue}
//	for i, server := range acceptors {
//		var acceptReply AcceptReply
//		ret := false
//		if i != px.me {
//			ret = call(server, "Paxos.Accept", acceptArgs, &acceptReply)
//		} else {
//			ret = px.Accept(acceptArgs, &acceptReply) == nil
//		}
//		if ret && acceptReply.Accepted {
//			acceptedServers++
//		}
//	}
//	return acceptedServers
//}

func (px *Paxos) sendDecided(pid int, proposalNum string, maxValue interface{}) {
	decide_args := &DecidedArgs{pid, proposalNum, maxValue}
	allDecided := false
	minDone := math.MaxInt32
	dones := make([]int, len(px.peers))
	for !allDecided {
		allDecided = true
		for i, server := range px.peers {
			var reply DecidedReply
			ret := false
			if i != px.me {
				ret = call(server, "Paxos.Decided", decide_args, &reply)
			} else {
				ret = px.Decided(decide_args, &reply) == nil
			}
			if !ret {
				allDecided = false
			} else {
				if reply.Done < minDone {
					minDone = reply.Done
				}
				dones[i] = reply.Done
			}
		}
		if !allDecided {
			time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
		}
	}
	if minDone != InitialValue {
		px.mu.Lock()
		defer px.mu.Unlock()
		px.doneSeqs = dones
		for key, _ := range px.instances {
			if key <= minDone {
				delete(px.instances, key)
			}
		}
		px.minSeq = minDone + 1
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go func() {
		if seq < px.minSeq {
			return
		}
		for true {
			//prepare 阶段
			proposalNum := genereteIncreasingNum(px.me)
			acceptors := px.selectMajority()
			preparedServers, maxValue := px.sendPrepare(acceptors, seq, proposalNum, v)

			//accept 阶段
			if preparedServers == px.majoritySize {
				acceptedServers := px.sendAccept(acceptors, seq, proposalNum, maxValue)
				//decided 阶段
				if acceptedServers == px.majoritySize {
					px.sendDecided(seq, proposalNum, maxValue)
					break
				} else {
					time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
			}
		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	px.doneSeqs[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	minDone := math.MaxInt32
	for _, value := range px.doneSeqs {
		if value < minDone {
			minDone = value
		}
	}
	if minDone >= px.minSeq {
		for key, _ := range px.instances {
			if key <= minDone {
				delete(px.instances, key)
			}
		}
		px.minSeq = minDone + 1
	}
	return px.minSeq
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	px.mu.Unlock()
	minSeq := px.Min()
	if seq < minSeq {
		return Forgotten, nil
	}
	if instance, ok := px.instances[seq]; ok {
		return instance.status, instance.acceptV
	} else {
		return Empty, nil
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.majoritySize = len(px.peers)/2 + 1
	px.instances = make(map[int]*InstanceState)
	px.doneSeqs = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		px.doneSeqs[i] = InitialValue
	}
	px.minSeq = 0
	px.maxSeq = InitialValue
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
