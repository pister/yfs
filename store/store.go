package store

import (
	"github.com/pister/yfs/name"
)

type Store interface {
	Add(data []byte) (name.Name, error)
	Delete(name []byte) error
	Get(name []byte) ([]byte, error)
	Exist(name []byte) (bool, error)
}
