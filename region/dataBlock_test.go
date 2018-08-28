package block

import (
	"testing"
	"fmt"
)

func TestSumHash(t *testing.T) {
	fmt.Println(sumHash([]byte("abcdefgh")))
	fmt.Println(sumHash([]byte("abcdef1234")))
	fmt.Println(sumHash([]byte("abcdef123")))
	fmt.Println(sumHash([]byte("abcdef1234")))
	fmt.Println(sumHash([]byte("abcdef1235")))
}
