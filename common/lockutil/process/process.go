package process

import (
	"os"
	"io/ioutil"
	"strconv"
	"github.com/pister/yfs/common/fileutil"
	"net"
	"fmt"
	"time"
	lg "github.com/pister/yfs/log"
	"github.com/pister/yfs/common/lockutil"
)

// 注意：该锁不适合高并发情况!
// process mutex lock base on socket port

const requestMessage = "needlock"
const responseMessage = "locked"

const defaultLockPort = 10500

var log lg.Logger
var inProcessLocker lockutil.TryLocker

func init() {
	l, err := lg.NewLogger("net")
	if err != nil {
		panic(err)
	}
	log = l
	inProcessLocker = lockutil.NewTryLocker()
}

type SocketTryLocker struct {
	fileName string
	port     int
	listener net.Listener
}

func OpenLocker(fileName string) *SocketTryLocker {
	locker := new(SocketTryLocker)
	locker.fileName = fileName
	locker.port = defaultLockPort
	return locker
}

func (locker *SocketTryLocker) Lock() {
	for {
		if locker.TryLock() {
			return
		}
		time.Sleep(1 * time.Second)
	}
}

func (locker *SocketTryLocker) Unlock() {
	if locker.listener != nil {
		locker.listener.Close()
		fileutil.DeleteFile(locker.fileName)
		locker.listener = nil
	}
}

func (locker *SocketTryLocker) TryLock() bool {
	if !inProcessLocker.TryLock() {
		return false
	}
	defer inProcessLocker.Unlock()

	success, err := locker.mayGetLock()
	if err != nil {
		panic(err)
	}
	if !success {
		return false
	}
	success, err = locker.lockIt()
	if err != nil {
		panic(err)
	}
	return success
}

func (locker *SocketTryLocker) lockIt() (bool, error) {
	var listener net.Listener = nil
	var err error = nil
	var port = locker.port
	for ; port < locker.port+100; port++ {
		listener, err = net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			break
		}
		got, err := locker.mayGetLock()
		if err != nil {
			panic(err)
		}
		if !got {
			return false, nil
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return false, err
	}
	err = ioutil.WriteFile(locker.fileName, []byte(fmt.Sprintf("%d", port)), 0666)
	if err != nil {
		return false, err
	}
	locker.listener = listener
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				// ignore msg
				// log.Info("accept error, %s", err)
				time.Sleep(1 * time.Second)
				continue
			}
			// start a new goroutine to handle
			// the new connection.
			go func() {
				defer c.Close()
				buf := make([]byte, len(requestMessage))
				err := fileutil.ReadFully(c, buf)
				if err != nil {
					log.Info("read error, %s", err)
					return
				}
				if string(buf) != requestMessage {
					log.Info("request data is not match, but here send normal response")
				}
				err = fileutil.WriteFully(c, []byte(responseMessage))
				if err != nil {
					log.Info("send error, %s", err)
				}
			}()
		}
	}()

	return true, nil
}

func (locker *SocketTryLocker) mayGetLock() (bool, error) {
	exist, err := fileutil.PathExists(locker.fileName)
	if err != nil {
		return false, err
	}
	if !exist {
		// not locked
		locker.port = defaultLockPort
		return true, nil
	}
	file, err := os.Open(locker.fileName)
	if err != nil {
		return false, err
	}
	defer file.Close()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return false, err
	}
	port, err := strconv.Atoi(string(data))
	if err != nil {
		return false, err
	}
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 1*time.Second)
	if err != nil {
		// connect fail
		// not locked
		locker.port = defaultLockPort
		return true, nil
	}
	defer conn.Close()
	deadline := time.Now().Add(3 * time.Second)
	conn.SetWriteDeadline(deadline)
	if err := fileutil.WriteFully(conn, []byte(requestMessage)); err != nil {
		return false, err
	}
	buf := make([]byte, len(responseMessage))
	if err := fileutil.ReadFully(conn, buf); err != nil {
		return false, err
	}
	if string(buf) == responseMessage {
		// has locked
		return false, nil
	} else {
		locker.port = port + 1
		return true, nil
	}

}
