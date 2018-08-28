package sysio

import (
	"os"
	"io/ioutil"
	"fmt"
)

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func WriteDataToFile(path string, data []byte) error {
	return ioutil.WriteFile(path, data, 0644)
}

func MkDirs(dir string) error {
	fi, err := os.Stat(dir)
	if err == nil || !os.IsNotExist(err) {
		if fi.IsDir() {
			return nil
		}
		return fmt.Errorf("%s is a normal file", dir)
	}
	return os.MkdirAll(dir, os.ModeDir|os.ModePerm)
}
