package bloom

import (
	"testing"
	"fmt"
)

func TestNewHashMaker(t *testing.T) {
	maker := NewHashMaker()
	hashes := maker.hash([]byte("hello world2"))
	fmt.Println(hashes)
}

func TestBloomFilter(t *testing.T) {
	filter := NewUnsafeBloomFilter(1024 * 10)
	filter.Add([]byte("hello world"))
	filter.Add([]byte("hello world0"))
	filter.Add([]byte("hello world2"))
	filter.Add([]byte("hello world10"))
	filter.Add([]byte("hello world11"))
	if !filter.Hit([]byte("hello world")) {
		t.Fatal("error")
	}
	if !filter.Hit([]byte("hello world0")) {
		t.Fatal("error")
	}
	if !filter.Hit([]byte("hello world2")) {
		t.Fatal("error")
	}
	if !filter.Hit([]byte("hello world10")) {
		t.Fatal("error")
	}
	if !filter.Hit([]byte("hello world11")) {
		t.Fatal("error")
	}
	if filter.Hit([]byte("hello world1")) {
		t.Fatal("error")
	}
}
