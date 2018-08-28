package sysio

import (
	"os"
	"io/ioutil"
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