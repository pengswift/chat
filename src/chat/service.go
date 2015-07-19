package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

import (
	"github.com/boltdb/bolt"
	"golang.org/x/net/context"
	"gopkg.in/vmihailenco/msgpack.v2"
)

import (
	log "github.com/pengswift/gamelibs/nsq-logger"
	"github.com/pengswift/go-pubsub"
)

import (
	. "proto"
)

const (
	SERVICE = "[CHAT]"
)

const (
	BOLTDB_FILE    = "/data/CHAT.DAT" // db文件名
	BOLTDB_BUCKET  = "EPS"
	MAX_QUEUE_SIZE = 128 // num of message kept
	PENDING_SIZE   = 65536
	CHECK_INTERVAL = time.Minute
)

var (
	OK                   = &Chat_Nil{}
	ERROR_ALREADY_EXISTS = errors.New("id already exists")
	ERROR_NOT_EXISTS     = errors.New("id not exists")
)

type EndPoint struct {
	inbox      []Chat_Message //聊天消息
	ps         *pubsub.PubSub //订阅发布对象
	sync.Mutex                //锁
}

//推入消息
func (ep *EndPoint) Push(msg *Chat_Message) {
	ep.Lock()
	defer ep.Unlock()
	//如果消息盒子长度>最大队列长度, 移除第一个消息
	if len(ep.inbox) > MAX_QUEUE_SIZE {
		ep.inbox = append(ep.inbox[1:], *msg)
	} else {
		ep.inbox = append(ep.inbox, *msg)
	}
}

//读消息
func (ep *EndPoint) Read() []Chat_Message {
	ep.Lock()
	defer ep.Unlock()
	//copy传递
	return append([]Chat_Message(nil), ep.inbox...)
}

func NewEndPoint() *EndPoint {
	u := &EndPoint{}
	//初始化订阅器
	u.ps = pubsub.New()
	return u
}

//用server包装EndPoint
type server struct {
	eps          map[uint64]*EndPoint //存放每个id对应的消息集合
	pending      chan uint64          //存储已有消息的id
	sync.RWMutex                      //读写锁
}

func (s *server) init() {
	s.eps = make(map[uint64]*EndPoint)
	s.pending = make(chan uint64, PENDING_SIZE)
	s.restore()             //从数据库加载已存数据
	go s.persistence_task() //执行定时任务
}

//通过id来读取消息集合
func (s *server) read_ep(id uint64) *EndPoint {
	s.RLock()
	defer s.RUnlock()
	return s.eps[id]
}

//订阅消息, 通过Chat_Id
func (s *server) Subscribe(p *Chat_Id, stream ChatService_SubscribeServer) error {
	die := make(chan bool)
	//包装消息
	f := pubsub.NewWrap(func(msg *Chat_Message) {
		//发送message
		if err := stream.Send(msg); err != nil {
			// 出错时, 取消订阅
			close(die)
		}
	})

	log.Tracef("new subscriber: %p", f)

	//读取消息盒子
	ep := s.read_ep(p.Id)
	if ep == nil {
		log.Errorf("canot find endpoint %v", p)
		return ERROR_NOT_EXISTS
	}

	//订阅消息
	ep.ps.Sub(f)
	defer func() {
		ep.ps.Leave(f)
	}()

	<-die
	return nil
}

//通过Chat_Id读取消息
func (s *server) Read(p *Chat_Id, stream ChatService_ReadServer) error {
	ep := s.read_ep(p.Id)
	if ep == nil {
		log.Errorf("canot find endpoint %v", p)
		return ERROR_NOT_EXISTS
	}

	//如果读到消息
	msgs := ep.Read()
	for k := range msgs {
		//发送消息
		if err := stream.Send(&msgs[k]); err != nil {
			return err
		}
	}
	return nil
}

//向聊天通道发送消息
func (s *server) Send(ctx context.Context, msg *Chat_Message) (*Chat_Nil, error) {
	//判断该通道是否存在
	ep := s.read_ep(msg.Id)
	if ep == nil {
		return nil, ERROR_NOT_EXISTS
	}

	//发布消息
	ep.ps.Pub(msg)
	//加入已发送消息盒子
	ep.Push(msg)
	//标记需要存储
	s.pending <- msg.Id
	return OK, nil
}

//注册消息通道
func (s *server) Reg(ctx context.Context, p *Chat_Id) (*Chat_Nil, error) {
	s.Lock()
	defer s.Unlock()
	//通过id读取消息盒子
	ep := s.eps[p.Id]
	//判断是否已注册
	if ep != nil {
		log.Errorf("id already exists:%v", p.Id)
		return nil, ERROR_ALREADY_EXISTS
	}

	//一个消息id绑定一个endpoint
	s.eps[p.Id] = NewEndPoint()
	//标记要保存的id
	s.pending <- p.Id
	return OK, nil
}

//持久化任务
func (s *server) persistence_task() {
	//一分钟执行一次
	timer := time.After(CHECK_INTERVAL)
	db := s.open_db()

	//定义改变的id集合
	changes := make(map[uint64]bool)

	//信号监听
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	for {
		select {
		//s.pending传入变更的id
		case key := <-s.pending:
			changes[key] = true
		case <-timer:
			//一分钟后写入
			s.dump(db, changes)
			log.Infof("perisisted %v endpoints:", len(changes))
			//清空
			changes = make(map[uint64]bool)
			//重新计时
			timer = time.After(CHECK_INTERVAL)
			//当接受关闭信号时
		case nr := <-sig:
			//写入数据
			s.dump(db, changes)
			//关闭
			db.Close()
			log.Info(nr)
			os.Exit(0)
		}
	}
}

//打开数据库
func (s *server) open_db() *bolt.DB {
	db, err := bolt.Open(BOLTDB_FILE, 0600, nil)
	if err != nil {
		log.Critical(err)
		os.Exit(-1)
	}

	//创建表
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(BOLTDB_BUCKET))
		if err != nil {
			log.Criticalf("create bucket: %s", err)
			os.Exit(-1)
		}
		return nil
	})
	return db
}

//写入文件
func (s *server) dump(db *bolt.DB, changes map[uint64]bool) {
	//写入已经更改的id
	for k := range changes {
		//读取message
		ep := s.read_ep(k)
		if ep == nil {
			log.Errorf("canot find endpoint %v", k)
			continue
		}

		//序列化数据
		bin, err := msgpack.Marshal(ep.Read())
		if err != nil {
			log.Critical("canot marshal:", err)
			continue
		}

		//更新表
		db.Update(func(tx *bolt.Tx) error {
			//拿到表句柄
			b := tx.Bucket([]byte(BOLTDB_BUCKET))
			//存入数据, k-v形式
			err := b.Put([]byte(fmt.Sprint(k)), bin)
			return err
		})
	}
}

//加载数据
func (s *server) restore() {
	db := s.open_db()
	defer db.Close()
	count := 0
	//查询
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET))
		b.ForEach(func(k, v []byte) error {
			//查询每个用户对应消息记录集合，将其反序列化成ChatMessage[]对象
			var msg []Chat_Message
			err := msgpack.Unmarshal(v, &msg)
			if err != nil {
				log.Critical("chat data corrupted:", err)
				os.Exit(-1)
			}
			//id转成uint64
			id, err := strconv.ParseUint(string(k), 0, 64)
			if err != nil {
				log.Critical("chat data corrupted:", err)
				os.Exit(-1)
			}
			//用NewEndPoint包装msg
			ep := NewEndPoint()
			ep.inbox = msg
			//用过eps包装ep
			s.eps[id] = ep
			count++
			return nil
		})
		return nil
	})
	log.Infof("restored %v chats", count)
}
