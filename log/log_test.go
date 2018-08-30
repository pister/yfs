package log

import (
	"testing"
	"path/filepath"
	"os"
	"fmt"
	"github.com/pister/yfs/common/ioutil"
	_ "time"
	"time"
)

func TestNewLogger(t *testing.T) {
	logger, err := NewLogger("foo")
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("hello")
	logger.Info("hello: %s", " the naming.")
}

func TestWalk(t *testing.T) {
	filepath.Walk("/Users/songlihuang/temp/temp1", func(path string, info os.FileInfo, err error) error {
		fmt.Println(path)
		return nil
	})
}

func TestTryGetBackupLogFile(t *testing.T) {
	tp1 := tryGetBackupLogFile("/Users/temp1/abc/ybfs-log.log_2018-12-30_11_32_11")
	if tp1 == nil {
		t.Fatal("must not be nil")
	}
	t.Log(tp1)
	tp1 = tryGetBackupLogFile("/Users/temp1/abc/ybfs-log.log_2018-12")
	if tp1 != nil {
		t.Fatal("must be nil")
	}
	tp1 = tryGetBackupLogFile("/Users/temp1/abc/ybfs-log.log")
	if tp1 != nil {
		t.Fatal("must be nil")
	}
}

func TestTryDeleteOldestFiles(t *testing.T) {
	path := "/Users/songlihuang/temp/temp2/test-logs"
	ioutil.WriteDataToFile(path+"/ybfs-log.log_2018-12-30_11_55_20", []byte("hello"))
	ioutil.WriteDataToFile(path+"/ybfs-log.log_2018-12-14_11_32_12", []byte("hello"))
	ioutil.WriteDataToFile(path+"/ybfs-log.log_2018-12-14_11_22_12", []byte("hello"))
	ioutil.WriteDataToFile(path+"/ybfs-log.log_2018-12-30_11_32_11", []byte("hello"))
	ioutil.WriteDataToFile(path+"/ybfs-log.log_2018-12-11_11_32_11", []byte("hello"))
	ioutil.WriteDataToFile(path+"/ybfs-log.log_2018-12-30_11_32_13", []byte("hello"))
	ct := tryDeleteOldestFiles(path, 3)
	fmt.Println(ct)
}

func printLine(logger Logger, groupName string, loop int) {
	for i := 0; i < loop; i++ {
		logger.Info("[%s] hello jack...: %d", groupName, i)
	}
}

func TestNewLogger2(t *testing.T) {
	logger, err := NewLogger("foo")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		go printLine(logger, fmt.Sprintf("group_%02d",i), 10000)
	}

	time.Sleep(1000 * time.Second)
}
