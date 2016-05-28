package pbservice

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"net/rpc"
	"time"
	"viewservice"
)

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	me   string
	view viewservice.View
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.me = me
	view, ok := ck.vs.Get()
	if !ok {
		//fatal等于print之后调用os.Exit(1)
		log.Fatal("client从viewservice中获取view失败")
	}
	ck.view = view
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	//log.Print(srv + "be called")
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//主机正在往备份中拷贝，所以要不停地试探，直到得到反馈
func (ck *Clerk) Get(key string) string {

	// Your code here.
	args := &GetArgs{key}
	var reply GetReply

	ok := false
	for !ok {
		ok = call(ck.view.Primary, "PBServer.Get", args, &reply)
		if !ok || reply.Err == ErrWrongServer {
			time.Sleep(viewservice.PingInterval)
			view, ok1 := ck.vs.Get()
			if !ok1 {
				log.Printf("客户端 %s 从viewservice获取view失败", ck.me)
				continue
			}
			ck.view = view
			ok = false
		}
	}

	return reply.Value

}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	args := &PutAppendArgs{key, value, nrand(), ck.me, op}
	var reply PutAppendReply
	ok := false
	for !ok {
		ok = call(ck.vs.Primary(), "PBServer.PutAppend", args, &reply)
		if !ok || reply.Err == ErrWrongServer {
			time.Sleep(viewservice.PingInterval)
			view, ok1 := ck.vs.Get()
			if !ok1 {
				log.Printf("客户端 %s 从viewservice获取view失败", ck.me)
				continue
			}
			ck.view = view
			ok = false
		}
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
