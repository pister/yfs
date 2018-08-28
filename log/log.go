package log

import (
	"log"
	"os"
	"encoding/json"
	"io"
	"github.com/pister/yfs/sysio"
	"fmt"
	"sync"
	"time"
	"path/filepath"
	"strings"
	"regexp"
	"sort"
)

type LoggerFactory struct {
}

type Logger interface {
	Info(format string, args ...interface{})
}

type LoggerImpl struct {
	log    *log.Logger
	file   *os.File
	config *LoggerConfiguration
	mutex  sync.Mutex
}

func (logger *LoggerImpl) Info(format string, args ...interface{}) {
	logger.mutex.Lock()
	defer logger.mutex.Unlock()

	logger.log.Printf(format, args...)
	fi, err := logger.file.Stat()
	if err == nil {
		logger.tryRoll(fi.Size())
	}
}

func backupFile(config *LoggerConfiguration) bool {
	currentFileName := config.currentFileName()
	baseBackupFileName := fmt.Sprintf("%s_%s", currentFileName, time.Now().Format(backNameFormat))
	err := os.Rename(currentFileName, baseBackupFileName)
	if err == nil {
		return true
	}
	for i := 1; i <= 10; i++ {
		tryBackupFileName := fmt.Sprintf("%s_%d", baseBackupFileName, i)
		err := os.Rename(currentFileName, tryBackupFileName)
		if err == nil {
			return true
		}
	}
	// try 10 times. backup fail.
	return false
}

type timePath struct {
	path string
	time *time.Time
}

func tryGetBackupLogFile(path string) (*timePath) {
	parts := strings.Split(path, "/")
	name := parts[len(parts)-1]
	if name == logFileName {
		return nil
	}
	if !backupLogFileNamePattern.MatchString(name) {
		return nil
	}
	timePart := name[len(logFileName)+1:]
	ts, err := time.Parse(backNameFormat, timePart)
	if err != nil {
		return nil
	}
	ret := new(timePath)
	ret.path = path
	ret.time = &ts
	return ret
}

type timePathList []*timePath

func (c timePathList) Len() int {
	return len(c)
}

func (c timePathList) Less(i, j int) bool {
	return c[i].time.Unix() < c[j].time.Unix()
}

func (c timePathList) Swap(i, j int) {
	temp := c[j]
	c[i] = c[j]
	c[j] = temp
}

func tryDeleteOldestFiles(path string, rollingSize int) int {
	timePaths := make([]*timePath, 0, 64)
	filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		tp := tryGetBackupLogFile(path)
		if tp != nil {
			timePaths = append(timePaths, tp)
		}
		return nil
	})
	successCount := 0
	if len(timePaths) > rollingSize {
		sort.Sort(timePathList(timePaths))
		for i := 0; i < len(timePaths)-rollingSize; i++ {
			err := os.Remove(timePaths[i].path)
			if err == nil {
				successCount += 1
			}
		}
	}
	return successCount
}

func (logger *LoggerImpl) tryRoll(size int64) error {
	if logger.config.OutputPath == stdoutPath {
		return nil
	}
	if size < logger.config.LogFileSize {
		return nil
	}

	// close old file
	logger.file.Close()
	// delete oldest files
	tryDeleteOldestFiles(logger.config.OutputPath, logger.config.Rolling)
	// rename old file
	backupFile(logger.config)
	// create new file
	f, err := defaultConfiguration.openLogFile()
	if err != nil {
		return err
	}
	logger.file = f
	logger.log.SetOutput(f)

	return err
}

type LoggerConfiguration struct {
	OutputPath  string
	Level       string
	Rolling     int
	LogFileSize int64
}

var backupLogFileNamePattern *regexp.Regexp
var defaultConfiguration *LoggerConfiguration

const (
	stdoutPath     = "<std>"
	logFileName    = "ybfs-log.log"
	backNameFormat = "2006-01-02_15_04_05"
)

func loadLogConfigFromFile() *LoggerConfiguration {
	file, err := os.Open("log-config.json")
	if err != nil {
		return nil
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	config := new(LoggerConfiguration)
	if err := decoder.Decode(config); err != nil {
		return nil
	}
	return config
}

func init() {
	var err error
	backupLogFileNamePattern, err = regexp.Compile(`ybfs-log\.log_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}_\d{2}`)
	if err != nil {
		panic(err)
	}

	defaultConfiguration = loadLogConfigFromFile()
	if defaultConfiguration == nil {
		defaultConfiguration = new(LoggerConfiguration)
		defaultConfiguration.OutputPath = stdoutPath
		defaultConfiguration.Level = "Info"
		defaultConfiguration.Rolling = 0
	}
	if len(defaultConfiguration.OutputPath) == 0 {
		defaultConfiguration.OutputPath = stdoutPath
	}
	if defaultConfiguration.OutputPath != stdoutPath {
		if defaultConfiguration.LogFileSize <= 0 {
			defaultConfiguration.LogFileSize = 100 * 1024 * 1024
		}
		if defaultConfiguration.Rolling < 1 {
			defaultConfiguration.Rolling = 20
		}
	}
}

func (cf *LoggerConfiguration) ensureLogDirExist() error {
	if stdoutPath == cf.OutputPath {
		return nil
	}
	exists, err := sysio.PathExists(cf.OutputPath)
	if err != nil {
		return nil
	}
	// if exist, check it is a normal file or a directory
	if exists {
		fi, err := os.Stat(cf.OutputPath)
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			return fmt.Errorf("%s is an exist normal file, can not be use to log directory", cf.OutputPath)
		}
	} else {
		if err = os.MkdirAll(cf.OutputPath, os.ModeDir|os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}

func (cf *LoggerConfiguration) currentFileName() string {
	return fmt.Sprintf("%s%c%s", cf.OutputPath, os.PathSeparator, logFileName)
}

func (cf *LoggerConfiguration) openLogFile() (*os.File, error) {
	fs, err := os.OpenFile(cf.currentFileName(), os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)
	return fs, err
}

func NewLogger(name string) (Logger, error) {
	var writer io.Writer
	logger := new(LoggerImpl)
	if stdoutPath == defaultConfiguration.OutputPath {
		writer = os.Stdout
	} else {
		if err := defaultConfiguration.ensureLogDirExist(); err != nil {
			return nil, err
		}
		f, err := defaultConfiguration.openLogFile()
		if err != nil {
			return nil, err
		}
		logger.file = f
		writer = f
	}
	logger.log = log.New(writer, name+" ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	logger.config = defaultConfiguration
	return logger, nil
}
