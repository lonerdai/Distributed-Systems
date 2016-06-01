package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "strconv"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view viewservice.View

	data           map[string]string
	request_number map[string]int

	//使用读写锁，读操作之间不互斥，写操作与读操作或其他写操作互斥
	rw_mutex sync.RWMutex
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	//只需要使用读锁
	pb.rw_mutex.RLock()
	if pb.me == pb.view.Primary {
		value, ok := pb.data[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		//if the server isn't primary, reject the request
		reply.Err = ErrWrongServer
	}

	defer pb.rw_mutex.RUnlock()
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	//used for at-most-once
	//用十进制来表示serailNumber
	request_key := strconv.FormatInt(args.SerailNumber, 10)
	pb.rw_mutex.Lock()
	value, ok := pb.request_number[request_key]
	if !ok || value != 1 {
		//primary deal with the request
		if pb.me == pb.view.Primary {
			if args.Key == "" {
				reply.Err = ErrEmptyKey
			}
			pb.request_number[request_key] = 0
			//transmit the request to the backup
			args.Client = pb.me
			var backup_reply PutAppendReply
			backup_ok := false
			if pb.view.Backup != "" {
				backup_ok = call(pb.view.Backup, "PBServer.PutAppend", args, &backup_reply)
			} else {
				backup_ok = true
			}
			//if backup return false, maybe the backup is alive but
			//a network error occurred, or the backup is dead. so
			//wait ping interval to get the latest view, and then retry
			for !backup_ok {
				time.Sleep(viewservice.PingInterval)
				if pb.view.Primary != pb.me {
					backup_ok = false
					break
				} else if pb.view.Backup != "" {
					backup_ok = call(pb.view.Backup, "PBServer.PutAppend", args, &backup_reply)
				} else {
					backup_ok = true
				}
			}
			if backup_ok {
				if _, exist := pb.data[args.Key]; args.Type == AppendTag && exist {
					pb.data[args.Key] += args.Value
				} else {
					pb.data[args.Key] = args.Value
				}
				pb.request_number[request_key] = 1
				reply.Err = OK
			} else {
				reply.Err = ErrWrongServer
			}
		} else if pb.me == pb.view.Backup && args.Client == pb.view.Primary {
			//backup deal with the request transmitted from primary
			if _, exist := pb.data[args.Key]; args.Type == AppendTag && exist {
				pb.data[args.Key] += args.Value
			} else {
				pb.data[args.Key] = args.Value
			}
			pb.request_number[request_key] = 1
			reply.Err = OK
		} else {
			//if the server isn't primary, reject the request
			reply.Err = ErrWrongServer
		}
	} else {
		reply.Err = ErrDuplicateKey
	}
	defer pb.rw_mutex.Unlock()
	return nil
}

//同步备份
//Synchronize the backup when it first became the backup
func (pb *PBServer) SynBackup(args *PBSynArgs, reply *PBSynReply) error {

	// Your code here.
	pb.mu.Lock()
	if args.Primary == pb.view.Primary {
		pb.rw_mutex.Lock()
		pb.data = args.Data
		pb.rw_mutex.Unlock()
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}
	defer pb.mu.Unlock()

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	reply_view, err := pb.vs.Ping(pb.view.Viewnum)
	if err == nil {
		//同步备份机，（在保存view中的备份几与返回view中的backup不一致的情况下）
		if pb.me == reply_view.Primary && reply_view.Backup != "" && reply_view.Backup != pb.view.Backup {
			args := &PBSynArgs{pb.data, pb.view.Primary}
			var reply PBSynReply
			ok := call(reply_view.Backup, "PBServer.SynBackup", args, &reply)
			if !ok || reply.Err != OK {
				log.Fatal("Error : Syn Backup failed\n")
			}
		}
	}
	pb.view = reply_view
	defer pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.data = make(map[string]string)
	pb.request_number = make(map[string]int)
	pb.view = viewservice.View{0, "", ""}

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
